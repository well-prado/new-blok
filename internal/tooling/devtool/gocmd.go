package devtool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Bounds on a Go toolchain subprocess.
const (
	// InterruptGrace is how long a go command may take to stop after it is
	// interrupted before its whole process group is killed.
	InterruptGrace = 5 * time.Second
	// maxLineBytes bounds one line of go command output; the rest of a longer
	// line is dropped.
	maxLineBytes = 1 << 20
	// pipeDrain bounds how long output is still copied after the go command
	// exits, in case a descendant kept the pipe open.
	pipeDrain = 2 * time.Second
)

// goCommand is one go invocation.
type goCommand struct {
	binary string
	dir    string
	args   []string
	// env is the caller's environment; nil means os.Environ(). goEnv pins
	// what may not come from it.
	env            []string
	stdout, stderr func([]byte)
	// remove is a directory the guard removes after killing the go
	// command's group if blok dies (blok dev's build directory, which the
	// go command may still be writing when the session guard removes it).
	remove string
}

// goRun is the outcome of one go command.
type goRun struct {
	exitCode int
}

// pinnedEnv are the go settings blok decides itself. The caller's
// environment (and its go env file, which environment variables override)
// cannot make check or test download a toolchain or a module, rewrite
// go.mod or go.sum, follow a go.work, or inject flags such as -toolexec:
//
//   - GOTOOLCHAIN=local: run the installed go, never fetch another one;
//   - GOPROXY=off: never reach a module proxy or VCS host, so a module
//     missing from the cache is a diagnostic, not a download (this also
//     disables checksum-database lookups);
//   - GOFLAGS=-mod=readonly (or -mod=vendor with vendor/modules.txt):
//     never edit go.mod or go.sum; this replaces the caller's GOFLAGS;
//   - GOWORK=off: build the project's own module, not a workspace.
func goEnv(base []string, dir string) []string {
	if base == nil {
		base = os.Environ()
	}
	mode := "-mod=readonly"
	if info, err := os.Lstat(filepath.Join(dir, "vendor", "modules.txt")); err == nil && info.Mode().IsRegular() {
		mode = "-mod=vendor"
	}
	pinned := []string{"GOTOOLCHAIN=local", "GOPROXY=off", "GOFLAGS=" + mode, "GOWORK=off"}
	env := make([]string, 0, len(base)+len(pinned))
	for _, entry := range base {
		key, _, _ := strings.Cut(entry, "=")
		keep := true
		for _, setting := range pinned {
			name, _, _ := strings.Cut(setting, "=")
			keep = keep && !strings.EqualFold(key, name)
		}
		if keep {
			env = append(env, entry)
		}
	}
	return append(env, pinned...)
}

// runGo runs the go command and hands each output line to the callbacks,
// which are called from two goroutines. The command runs in its own process
// group, which blok stops in every way blok itself can stop:
//
//   - cancellation interrupts the group, then kills it after InterruptGrace;
//   - a panic in a callback kills the group before runGo re-panics;
//   - if blok dies outright (SIGKILL included), a guard process holding the
//     other end of a pipe sees it close and kills the group (POSIX only).
//
// runGo returns errToolUnavailable when the command or its guard cannot
// start.
func runGo(ctx context.Context, spec goCommand) (goRun, error) {
	if err := ctx.Err(); err != nil {
		return goRun{}, err
	}
	command := exec.Command(spec.binary, spec.args...)
	command.Dir = spec.dir
	command.Env = goEnv(spec.env, spec.dir)
	stdout, stderr := &lineWriter{emit: spec.stdout}, &lineWriter{emit: spec.stderr}
	command.Stdout, command.Stderr = stdout, stderr
	command.WaitDelay = pipeDrain
	isolate(command)
	// Set before Start: the copy goroutines that call the writers start
	// inside it, after command.Process is assigned.
	kill := sync.OnceFunc(func() { _ = killGroup(command.Process) })
	stdout.onPanic, stderr.onPanic = kill, kill
	if err := command.Start(); err != nil {
		return goRun{}, &startError{err: err}
	}
	guard, err := startGuard(command.Process, spec.remove)
	if err != nil {
		_ = killGroup(command.Process)
		_ = command.Wait()
		return goRun{}, &startError{err: fmt.Errorf("start the process guard: %w", err), guard: true}
	}
	defer guard.release()

	exited := make(chan struct{})
	var watcher sync.WaitGroup
	watcher.Go(func() {
		select {
		case <-exited:
			return
		case <-ctx.Done():
		}
		_ = interruptGroup(command.Process)
		timer := time.NewTimer(InterruptGrace)
		defer timer.Stop()
		select {
		case <-exited:
		case <-timer.C:
			kill()
		}
	})
	err = command.Wait()
	close(exited)
	watcher.Wait()
	stdout.flush()
	stderr.flush()
	for _, writer := range []*lineWriter{stdout, stderr} {
		if writer.panicked != nil {
			panic(writer.panicked)
		}
	}
	run := goRun{exitCode: command.ProcessState.ExitCode()}
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) && !errors.Is(err, exec.ErrWaitDelay) {
		return run, err
	}
	return run, nil
}

// startError is a go command, or its process guard, that could not be
// started. Either way the command does not run (exit 3).
type startError struct {
	err   error
	guard bool
}

func (e *startError) Error() string        { return e.err.Error() }
func (e *startError) Unwrap() error        { return e.err }
func (e *startError) Is(target error) bool { return target == errToolUnavailable }

// lineWriter splits a stream into bounded lines. The slice it emits is
// reused; a callback copies what it keeps. A panicking callback stops the
// process group at once and is re-raised by runGo after the command exits.
type lineWriter struct {
	emit     func([]byte)
	pending  []byte
	onPanic  func()
	panicked any
}

func (w *lineWriter) Write(data []byte) (int, error) {
	written := len(data)
	for len(data) > 0 {
		newline := bytes.IndexByte(data, '\n')
		chunk := data
		if newline >= 0 {
			chunk = data[:newline]
		}
		if room := maxLineBytes - len(w.pending); room > 0 {
			w.pending = append(w.pending, chunk[:min(len(chunk), room)]...)
		}
		if newline < 0 {
			break
		}
		w.line()
		data = data[newline+1:]
	}
	return written, nil
}

func (w *lineWriter) line() {
	defer func() { w.pending = w.pending[:0] }()
	if w.emit == nil || w.panicked != nil {
		return
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			w.panicked = recovered
			if w.onPanic != nil {
				w.onPanic()
			}
		}
	}()
	w.emit(bytes.TrimRight(w.pending, "\r"))
}

func (w *lineWriter) flush() {
	if len(w.pending) > 0 {
		w.line()
	}
}
