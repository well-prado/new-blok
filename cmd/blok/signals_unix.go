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

// catchBrokenPipe makes a write to a closed pipe fail with EPIPE for blok
// dev (exit 4) without ignoring SIGPIPE: an ignored signal stays ignored in
// the application blok dev starts, a caught one is reset to its default.
// The returned function stops catching it.
func catchBrokenPipe() func() {
	caught := make(chan os.Signal, 1)
	signal.Notify(caught, syscall.SIGPIPE)
	return func() { signal.Stop(caught) }
}
