//go:build !windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

// toolSignals stop check, test and inspect: an interrupt, a terminate, a
// lost terminal and a quit all end the go command's process group and write
// the partial report, instead of leaving it running.
var toolSignals = []os.Signal{os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT}

// ignoreBrokenPipe makes a write to a closed pipe (blok check --json | head)
// fail with EPIPE, which the CLI reports as exit 4, instead of killing the
// process with SIGPIPE.
func ignoreBrokenPipe() { signal.Ignore(syscall.SIGPIPE) }
