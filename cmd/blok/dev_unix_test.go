//go:build !windows

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/devtool"
)

// devRun is one real blok dev process on a real scaffolded application.
type devRun struct {
	command *exec.Cmd
	stdout  *lineCollector
	stderr  bytes.Buffer
	done    chan struct{}
	address string
}

// lineCollector keeps every line a stream wrote and lets a test wait for one.
type lineCollector struct {
	mu    sync.Mutex
	lines []string
}

func (c *lineCollector) read(stream io.Reader) {
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		c.mu.Lock()
		c.lines = append(c.lines, scanner.Text())
		c.mu.Unlock()
	}
}

func (c *lineCollector) all() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.lines...)
}

func (c *lineCollector) wait(t *testing.T, match func(string) bool) string {
	t.Helper()
	for deadline := time.Now().Add(4 * time.Minute); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		for _, line := range c.all() {
			if match(line) {
				return line
			}
		}
	}
	t.Fatalf("no matching line in:\n%s", strings.Join(c.all(), "\n"))
	return ""
}

// startDev runs blok dev in its own process group, as a shell would run a
// foreground job, so a test can deliver Ctrl+C to that group alone.
func startDevProcess(t *testing.T, dir string, args ...string) *devRun {
	t.Helper()
	run := &devRun{stdout: &lineCollector{}, done: make(chan struct{})}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	run.address = listener.Addr().String()
	listener.Close()
	run.command = exec.Command(blokBinary(t), append([]string{"dev"}, args...)...)
	run.command.Dir = dir
	run.command.Env = append(os.Environ(), "ADDR="+run.address)
	run.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	run.command.Stderr = &run.stderr
	stdout, err := run.command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := run.command.Start(); err != nil {
		t.Fatal(err)
	}
	copied := make(chan struct{})
	go func() { run.stdout.read(stdout); close(copied) }()
	go func() { <-copied; _ = run.command.Wait(); close(run.done) }()
	t.Cleanup(func() {
		_ = run.command.Process.Kill()
		// An application that outlived blok dev holds the test's pipes;
		// end it so a failure is reported instead of hanging.
		for _, pid := range run.apps() {
			_ = syscall.Kill(-pid, syscall.SIGKILL)
		}
		select {
		case <-run.done:
		case <-time.After(time.Minute):
			t.Error("blok dev's output never closed")
		}
	})
	return run
}

// apps lists the pids of every app-started event so far.
func (r *devRun) apps() []int {
	var pids []int
	for _, line := range r.stdout.all() {
		var event devtool.DevEvent
		if json.Unmarshal([]byte(line), &event) == nil && event.Event == devtool.EventAppStarted {
			pids = append(pids, event.PID)
		}
	}
	return pids
}

// started waits for the application to start and answer a request, and
// returns its pid.
func (r *devRun) started(t *testing.T, build int) int {
	t.Helper()
	line := r.stdout.wait(t, func(line string) bool {
		var event devtool.DevEvent
		return json.Unmarshal([]byte(line), &event) == nil && event.Event == devtool.EventAppStarted && event.Build == build
	})
	var event devtool.DevEvent
	_ = json.Unmarshal([]byte(line), &event)
	r.serving(t)
	return event.PID
}

// serving waits until the application answers a quote.
func (r *devRun) serving(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		response, err := http.Post("http://"+r.address+"/quotes", "application/json", strings.NewReader(`{"sku":"coffee","quantity":2}`))
		if err == nil {
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatalf("status %d", response.StatusCode)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
}

func (r *devRun) exit(t *testing.T, within time.Duration) int {
	t.Helper()
	select {
	case <-r.done:
	case <-time.After(within):
		t.Fatalf("blok dev did not exit within %s, or a surviving process holds its output open\n%s", within, strings.Join(r.stdout.all(), "\n"))
	}
	return r.command.ProcessState.ExitCode()
}

func assertGroupGone(t *testing.T, group int) {
	t.Helper()
	assertGone(t, group)
	for deadline := time.Now().Add(10 * time.Second); syscall.Kill(-group, 0) == nil; time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("the application's process group %d outlived blok dev", group)
		}
	}
}

// TestDevSignalsLeaveNothingRunning stops a real blok dev five ways while
// the application serves: an interrupt to blok, Ctrl+C at a terminal (an
// interrupt to blok's whole foreground process group), SIGTERM, SIGHUP (a
// lost terminal) and SIGKILL, which runs no blok code at all. Each time the
// application and its whole process group are gone afterwards; every way
// but SIGKILL exits 130 with a final stopped event, after stopping the
// application gracefully. macOS and Linux only; Windows is unverified
// (#156/#157).
func TestDevSignalsLeaveNothingRunning(t *testing.T) {
	for _, test := range []struct {
		name   string
		group  bool
		signal syscall.Signal
	}{
		{"sigint", false, syscall.SIGINT},
		{"ctrl-c", true, syscall.SIGINT},
		{"sigterm", false, syscall.SIGTERM},
		{"sighup", false, syscall.SIGHUP},
		{"sigkill", false, syscall.SIGKILL},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := startDevProcess(t, shopProject(t, "classic"), "--json")
			app := run.started(t, 1)
			// blok dev starts the application by the absolute path of its
			// build, inside blok dev's private build directory.
			executable, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(app)).Output()
			if err != nil {
				t.Fatal(err)
			}
			builds := filepath.Dir(filepath.Dir(strings.Fields(string(executable))[0]))
			if !strings.HasPrefix(filepath.Base(builds), "blok-dev-") {
				t.Fatalf("unexpected build directory %s", builds)
			}
			target := run.command.Process.Pid
			if test.group {
				target = -target
			}
			if err := syscall.Kill(target, test.signal); err != nil {
				t.Fatal(err)
			}
			code := run.exit(t, time.Minute)
			assertGroupGone(t, app)
			// However blok dev ended, its executables are gone too: removed
			// by blok dev itself, or by the guard after SIGKILL.
			for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
				if _, err := os.Stat(builds); errors.Is(err, os.ErrNotExist) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("build directory %s outlived blok dev", builds)
				}
			}
			lines := run.stdout.all()
			if test.signal == syscall.SIGKILL {
				if code != -1 {
					t.Fatalf("exit %d after SIGKILL", code)
				}
				return
			}
			last := lines[len(lines)-1]
			if code != devtool.ExitInterrupted || last != `{"version":"blok-dev/v1","event":"stopped","exitCode":130}` {
				t.Fatalf("exit %d, last event %s", code, last)
			}
			var stopped devtool.DevEvent
			if err := json.Unmarshal([]byte(lines[len(lines)-2]), &stopped); err != nil || stopped.Event != devtool.EventAppStopped || stopped.Status != "exit status 0" || len(stopped.Diagnostics) != 0 {
				t.Fatalf("the application was not stopped gracefully: %s", lines[len(lines)-2])
			}
		})
	}
}

// TestDevSecondInterruptForcesTheApplicationDown: an application that
// ignores SIGTERM holds blok dev for its 10s grace after one Ctrl+C; a
// second Ctrl+C kills it at once.
func TestDevSecondInterruptForcesTheApplicationDown(t *testing.T) {
	dir := shopProject(t, "unified")
	main := filepath.Join(dir, "cmd", "shop", "main.go")
	source, err := os.ReadFile(main)
	if err != nil {
		t.Fatal(err)
	}
	edited := strings.Replace(string(source), "signal.Notify(signals, os.Interrupt, syscall.SIGTERM)", "signal.Notify(signals, os.Interrupt)\n\tsignal.Ignore(syscall.SIGTERM)", 1)
	if err := os.WriteFile(main, []byte(edited), 0o644); err != nil {
		t.Fatal(err)
	}
	run := startDevProcess(t, dir, "--json")
	app := run.started(t, 1)
	group := -run.command.Process.Pid
	if err := syscall.Kill(group, syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	if !running(app) {
		t.Fatal("the application ignoring SIGTERM stopped before the second interrupt")
	}
	begin := time.Now()
	if err := syscall.Kill(group, syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	if code := run.exit(t, 5*time.Second); code != devtool.ExitInterrupted {
		t.Fatalf("exit %d", code)
	}
	if took := time.Since(begin); took > 3*time.Second {
		t.Fatalf("the second interrupt took %s", took)
	}
	assertGroupGone(t, app)
}

// TestDevHumanStream: without --json, events are "blok dev: …" lines on
// standard output, and the application's own output reaches the terminal
// unchanged.
func TestDevHumanStream(t *testing.T) {
	run := startDevProcess(t, shopProject(t, "classic"))
	run.stdout.wait(t, func(line string) bool {
		return strings.HasPrefix(line, "blok dev: started build 1 (generation 1, pid ")
	})
	run.serving(t)
	if err := syscall.Kill(run.command.Process.Pid, syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	if code := run.exit(t, time.Minute); code != devtool.ExitInterrupted {
		t.Fatalf("exit %d", code)
	}
	lines := run.stdout.all()
	want := []string{"blok dev: watching 9 files", "blok dev: build 1 started", "blok dev: build 1 succeeded"}
	if len(lines) < 6 || fmt.Sprint(lines[:3]) != fmt.Sprint(want) || !strings.HasPrefix(lines[len(lines)-2], "blok dev: stopped build 1 (exit status 0)") || lines[len(lines)-1] != "blok dev: stopped, exit code 130" {
		t.Fatalf("stream:\n%s", strings.Join(lines, "\n"))
	}
	if !strings.Contains(run.stderr.String(), "listening on "+run.address) {
		t.Fatalf("application output not passed through: %q", run.stderr.String())
	}
}

// TestDevUsageAndOutputFailures: bad arguments exit 2 with one stderr line
// and start nothing; --help exits 0; an event stream written to a closed
// pipe exits 4 instead of dying of SIGPIPE, without building anything.
func TestDevUsageAndOutputFailures(t *testing.T) {
	dir := t.TempDir()
	for _, args := range [][]string{{"dev", "--bogus"}, {"dev", "a", "b"}, {"dev", "--package"}} {
		got := runBlok(t, dir, nil, args...)
		if got.exit != devtool.ExitUsage || got.stdout != "" || strings.Count(got.stderr, "\n") != 1 || !strings.HasPrefix(got.stderr, "blok dev: ") {
			t.Fatalf("%v: exit=%d stdout=%q stderr=%q", args, got.exit, got.stdout, got.stderr)
		}
	}
	if got := runBlok(t, dir, nil, "dev", "--help"); got.exit != 0 || !strings.HasPrefix(got.stdout, "Usage: blok dev ") {
		t.Fatalf("help: %+v", got)
	}
	if got := runBlok(t, dir, nil, "help"); !strings.Contains(got.stdout, "\n  dev       ") {
		t.Fatalf("blok help does not list dev: %q", got.stdout)
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	defer writer.Close()
	project := shopProject(t, "unified")
	command := exec.Command(blokBinary(t), "dev", "--json", project)
	var stderr bytes.Buffer
	command.Stdout, command.Stderr = writer, &stderr
	if err := command.Run(); err != nil && !errors.As(err, new(*exec.ExitError)) {
		t.Fatal(err)
	}
	if code := command.ProcessState.ExitCode(); code != devtool.ExitOutput || !strings.HasPrefix(stderr.String(), "blok dev: write output: ") {
		t.Fatalf("exit=%d (%v) stderr=%q", code, command.ProcessState, stderr.String())
	}
}
