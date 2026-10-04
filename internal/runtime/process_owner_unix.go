//go:build !windows

package runtime

import (
	"os"
	"os/exec"
	"sync"
	"syscall"
)

// processOwner is the worker process the supervisor started. On POSIX
// systems stop asks it to exit with SIGTERM and kill ends it outright.
type processOwner struct {
	mu      sync.Mutex
	process *os.Process
}

func newProcessOwner() (*processOwner, error) { return &processOwner{}, nil }

// isolateWorker has nothing to do on POSIX: signals reach only the process
// they are sent to.
func isolateWorker(*exec.Cmd) {}

// start launches cmd. Signals reach only the process they are sent to, so
// there is nothing to establish before it runs.
func (o *processOwner) start(cmd *exec.Cmd) error {
	if err := cmd.Start(); err != nil {
		return err
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.process = cmd.Process
	return nil
}

func (o *processOwner) stop() { o.signal(syscall.SIGTERM) }
func (o *processOwner) kill() { o.signal(syscall.SIGKILL) }

func (o *processOwner) signal(s os.Signal) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.process != nil {
		_ = o.process.Signal(s)
	}
}

// release forgets the process once it has been reaped.
func (o *processOwner) release() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.process = nil
}
