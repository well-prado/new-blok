//go:build !windows

package cluster

import (
	"os"
	"syscall"
)

func suspendProcess(process *os.Process) error { return process.Signal(syscall.SIGSTOP) }

func resumeProcess(process *os.Process) error { return process.Signal(syscall.SIGCONT) }
