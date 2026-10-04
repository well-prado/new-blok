//go:build !windows

package runtime

import (
	"os/exec"
	"syscall"
)

// ownedProcess is the worker process the supervisor started. On POSIX
// systems stop asks it to exit with SIGTERM and kill ends it outright.
type ownedProcess struct{ cmd *exec.Cmd }

func ownProcess(cmd *exec.Cmd) (ownedProcess, error) { return ownedProcess{cmd: cmd}, nil }

func (o ownedProcess) stop()    { _ = o.cmd.Process.Signal(syscall.SIGTERM) }
func (o ownedProcess) kill()    { _ = o.cmd.Process.Kill() }
func (o ownedProcess) release() {}
