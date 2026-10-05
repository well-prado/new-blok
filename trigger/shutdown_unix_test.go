//go:build !windows

package trigger_test

import (
	"os/exec"
	"syscall"
)

func prepareShutdownChild(*exec.Cmd) {}

// requestShutdown asks a selected binary to drain the way a POSIX host does.
func requestShutdown(cmd *exec.Cmd) error { return cmd.Process.Signal(syscall.SIGTERM) }
