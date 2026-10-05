//go:build !windows

package devtool

import (
	"os"
	"os/exec"
	"syscall"
)

// isolate starts the go command in its own process group, so an interrupt
// reaches it and every test binary it started, and nothing else.
func isolate(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func interruptGroup(process *os.Process) error {
	return syscall.Kill(-process.Pid, syscall.SIGINT)
}

func killGroup(process *os.Process) error {
	return syscall.Kill(-process.Pid, syscall.SIGKILL)
}
