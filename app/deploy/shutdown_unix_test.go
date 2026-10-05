//go:build !windows

package deploy

import (
	"os"
	"os/exec"
	"syscall"
)

// shutdownSignal is how a deployment's host asks it to stop: SIGTERM.
var shutdownSignal os.Signal = syscall.SIGTERM

func prepareShutdownChild(*exec.Cmd) {}

func requestShutdown(cmd *exec.Cmd) error { return cmd.Process.Signal(syscall.SIGTERM) }
