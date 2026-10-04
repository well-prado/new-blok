//go:build windows

package runtime

import (
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
// Windows offers no request to exit, so stop terminates the tree at once. The
// supervisor calls it after the drain has been acknowledged or its cleanup
// budget has run out; abort handlers in the worker do not run.
type processOwner struct {
	mu      sync.Mutex
	job     windows.Handle // zero once released
	process *os.Process
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

func (o *processOwner) adopt(cmd *exec.Cmd) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.process = cmd.Process
	// The os.Process keeps its own handle open, so the PID cannot be reused
	// before it is opened here.
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(process)
	return windows.AssignProcessToJobObject(o.job, process)
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
