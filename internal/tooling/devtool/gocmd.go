package devtool

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
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

// goRun is the outcome of one go command.
type goRun struct {
	exitCode int
}

// runGo runs the go command in dir and hands each output line to the given
// callbacks, which are called from two goroutines. On cancellation the
// command's process group is interrupted, then killed after InterruptGrace,
// so neither go nor a test binary it started outlives this call. runGo
// returns errToolUnavailable when the command cannot be started.
func runGo(ctx context.Context, goBinary, dir string, args []string, stdout, stderr func([]byte)) (goRun, error) {
	if err := ctx.Err(); err != nil {
		return goRun{}, err
	}
	command := exec.Command(goBinary, args...)
	command.Dir = dir
	command.Env = os.Environ()
	command.Stdout = &lineWriter{emit: stdout}
	command.Stderr = &lineWriter{emit: stderr}
	command.WaitDelay = pipeDrain
	isolate(command)
	if err := command.Start(); err != nil {
		return goRun{}, &startError{err: err}
	}
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
			_ = killGroup(command.Process)
		}
	})
	err := command.Wait()
	close(exited)
	watcher.Wait()
	command.Stdout.(*lineWriter).flush()
	command.Stderr.(*lineWriter).flush()
	run := goRun{exitCode: command.ProcessState.ExitCode()}
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) && !errors.Is(err, exec.ErrWaitDelay) {
		return run, err
	}
	return run, nil
}

// startError is a go command that could not be started.
type startError struct{ err error }

func (e *startError) Error() string        { return e.err.Error() }
func (e *startError) Unwrap() error        { return e.err }
func (e *startError) Is(target error) bool { return target == errToolUnavailable }

// lineWriter splits a stream into bounded lines. The slice it emits is
// reused; a callback copies what it keeps.
type lineWriter struct {
	emit    func([]byte)
	pending []byte
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
	if w.emit != nil {
		w.emit(bytes.TrimRight(w.pending, "\r"))
	}
	w.pending = w.pending[:0]
}

func (w *lineWriter) flush() {
	if len(w.pending) > 0 {
		w.line()
	}
}
