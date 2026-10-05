//go:build windows

package devtool

import (
	"os"
	"os/exec"
)

// isolate has no process-group equivalent here. Windows behaviour of check
// and test cancellation is unverified (Windows track, #156/#157): the go
// command is killed outright and a test binary it started may outlive it.
func isolate(*exec.Cmd) {}

func interruptGroup(process *os.Process) error { return process.Kill() }

func killGroup(process *os.Process) error { return process.Kill() }
