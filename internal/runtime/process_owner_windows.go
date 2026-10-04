//go:build windows

package runtime

import (
	"os/exec"
	"unsafe"

	"golang.org/x/sys/windows"
)

// ownedProcess is the worker process tree the supervisor started. Windows has
// no SIGTERM, and terminating only the direct child would orphan anything it
// launched (a .cmd wrapper's node.exe, for example). The child is therefore
// placed in a job object that kills every process in it when the job closes:
// stop and kill terminate the whole tree, and release reaps descendants that
// outlived the child, even if the supervisor itself dies.
//
// stop runs only after the worker has drained or the cleanup budget expired,
// so terminating the tree at once matches what SIGTERM does on POSIX: the
// worker has no admitted work left to finish.
type ownedProcess struct {
	cmd *exec.Cmd
	job windows.Handle
}

func ownProcess(cmd *exec.Cmd) (ownedProcess, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return ownedProcess{}, err
	}
	limits := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	limits.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(job, windows.JobObjectExtendedLimitInformation, uintptr(unsafe.Pointer(&limits)), uint32(unsafe.Sizeof(limits))); err != nil {
		_ = windows.CloseHandle(job)
		return ownedProcess{}, err
	}
	// The os.Process keeps its own handle open, so the PID cannot be reused
	// before it is opened here.
	process, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(cmd.Process.Pid))
	if err != nil {
		_ = windows.CloseHandle(job)
		return ownedProcess{}, err
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(job, process); err != nil {
		_ = windows.CloseHandle(job)
		return ownedProcess{}, err
	}
	return ownedProcess{cmd: cmd, job: job}, nil
}

func (o ownedProcess) stop() { o.kill() }

func (o ownedProcess) kill() {
	if windows.TerminateJobObject(o.job, 1) != nil {
		_ = o.cmd.Process.Kill()
	}
}

func (o ownedProcess) release() { _ = windows.CloseHandle(o.job) }
