//go:build windows

package devtool

import (
	"os"
	"os/exec"
)

// isolate has no process-group equivalent here. Windows behaviour of check
// and test cancellation is unverified (Windows track, #156/#157): the go
// command is killed outright, a test binary it started may outlive it, and
// nothing stops either if blok itself is killed (a job object would).
func isolate(*exec.Cmd) {}

func interruptGroup(process *os.Process) error { return process.Kill() }

func killGroup(process *os.Process) error { return process.Kill() }

// guardShell is unused on Windows, which has no guard.
var guardShell = ""

// processGuard is absent on Windows; see isolate.
type processGuard struct{}

func startGuard(*os.Process) (*processGuard, error) { return &processGuard{}, nil }

func (*processGuard) release() {}
