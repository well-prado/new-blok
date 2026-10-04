//go:build windows

package runtime

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// processOwner is the worker process tree the supervisor started. Windows has
// no SIGTERM, and terminating only the direct child would orphan anything it
// launched (a .cmd wrapper's node.exe, for example). The child is therefore
// placed in a job object that kills every process in it when the job closes:
// stop and kill terminate the whole tree, and release closes the job, so
// descendants that outlived the child are ended too, even if the supervisor
// itself dies. Breakaway is not allowed: a descendant cannot leave the job.
//
// The worker is created suspended and joins the job before its first
// instruction runs (see start), so nothing it launches can be created outside
// the job: a process joins a job at creation only if its parent is already in
// it.
//
// Windows offers no request to exit, so stop terminates the tree at once. The
// supervisor calls it after the drain has been acknowledged or its cleanup
// budget has run out; abort handlers in the worker do not run.
type processOwner struct {
	mu      sync.Mutex
	job     windows.Handle // zero once released
	process *os.Process

	// Test seams, nil in production: beforeAssign runs after the suspended
	// process exists and before it joins the job; assign replaces
	// AssignProcessToJobObject.
	beforeAssign func()
	assign       func(job, process windows.Handle) error
}

func newProcessOwner() (*processOwner, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		_ = windows.CloseHandle(job)
		return nil, err
	}
	return &processOwner{job: job}, nil
}

// isolateWorker gives the worker its own hidden console, so a Ctrl+C or
// Ctrl+Break aimed at the deployment's console does not also reach the
// worker and cut its calls while the host is still draining.
func isolateWorker(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_NO_WINDOW
}

// start launches cmd and places it in the job before any of its code runs.
// This is the order Microsoft documents for AssignProcessToJobObject: create
// the process suspended, assign it, then resume it. Starting it running and
// assigning it afterwards would let it create children outside the job in
// between, and those would escape stop, kill and release.
//
// If the process cannot be assigned or resumed it is terminated before any of
// its code has run: it has created nothing, so ending it ends everything. The
// returned error then wraps errProcessOwnership.
func (o *processOwner) start(cmd *exec.Cmd) error {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	if err := cmd.Start(); err != nil {
		return err
	}
	if err := o.own(cmd); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return fmt.Errorf("%w: %w", errProcessOwnership, err)
	}
	return nil
}

func (o *processOwner) own(cmd *exec.Cmd) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.process = cmd.Process
	if o.beforeAssign != nil {
		o.beforeAssign()
	}
	// The os.Process keeps its own handle open, so the PID cannot be reused
	// before it is opened here.
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(process)
	assign := windows.AssignProcessToJobObject
	if o.assign != nil {
		assign = o.assign
	}
	if err := assign(o.job, process); err != nil {
		return err
	}
	return resumeSuspended(uint32(cmd.Process.Pid))
}

// resumeSuspended resumes the initial thread of a process created with
// CREATE_SUSPENDED. os/exec closes the thread handle CreateProcess returns, so
// the thread is found by its owner. A process created suspended has exactly
// one thread until it is resumed (the loader's threads start after); finding
// none, or more than one (a tool injecting a thread, say), fails closed. The
// process is held open by its os.Process, but the thread ID is not reserved:
// the suspend count check below rejects a thread that is not the suspended
// initial one.
func resumeSuspended(pid uint32) error {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return err
	}
	defer windows.CloseHandle(snapshot)
	var threads []uint32
	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID == pid {
			threads = append(threads, entry.ThreadID)
		}
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return err
	}
	if len(threads) != 1 {
		return fmt.Errorf("suspended worker has %d threads, want 1", len(threads))
	}
	thread, err := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, threads[0])
	if err != nil {
		return err
	}
	defer windows.CloseHandle(thread)
	previous, err := windows.ResumeThread(thread)
	if err != nil {
		return err
	}
	if previous != 1 {
		return fmt.Errorf("worker thread suspend count was %d, want 1", previous)
	}
	return nil
}

func (o *processOwner) stop() { o.kill() }

func (o *processOwner) kill() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.job == 0 {
		// Released: the handle value may already belong to another job.
		return
	}
	if windows.TerminateJobObject(o.job, 1) != nil && o.process != nil {
		_ = o.process.Kill()
	}
}

func (o *processOwner) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.job != 0 {
		_ = windows.CloseHandle(o.job)
		o.job = 0
	}
}
