//go:build windows

package main

import (
	"os"
	"syscall"
)

// toolSignals are the signals Windows delivers to a console program. Their
// effect on check and test is unverified (#156, #157).
var toolSignals = []os.Signal{os.Interrupt, syscall.SIGTERM}

func ignoreBrokenPipe() {}

func catchBrokenPipe() func() { return func() {} }
