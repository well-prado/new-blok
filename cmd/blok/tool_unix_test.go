//go:build !windows

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/devtool"
)

// TestToolInterruptFlushesAndExits130 delivers a real signal to a blok test
// process while one of the application's tests is running, five ways: an
// interrupt to blok alone, Ctrl+C at a terminal (an interrupt to blok's
// whole foreground process group), SIGTERM, SIGHUP (a lost terminal) and
// SIGQUIT. Each time blok writes one
// complete JSON report holding the test that finished, marks the running
// one incomplete, exits 130 with nothing on stderr, and leaves no test
// binary behind. macOS and Linux only; Windows is unverified (#156/#157).
func TestToolInterruptFlushesAndExits130(t *testing.T) {
	for _, test := range []struct {
		name   string
		signal func(pid int) error
	}{
		{"sigint-to-process", func(pid int) error { return syscall.Kill(pid, syscall.SIGINT) }},
		{"ctrl-c-to-process-group", func(pid int) error { return syscall.Kill(-pid, syscall.SIGINT) }},
		{"sigterm-to-process", func(pid int) error { return syscall.Kill(pid, syscall.SIGTERM) }},
		{"sighup-to-process", func(pid int) error { return syscall.Kill(pid, syscall.SIGHUP) }},
		{"sigquit-to-process", func(pid int) error { return syscall.Kill(pid, syscall.SIGQUIT) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			run := startSlowTest(t)
			command, stdout, stderr, done := run.command, run.stdout, run.stderr, run.done
			signaled := time.Now()
			if err := test.signal(command.Process.Pid); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(devtool.InterruptGrace + 30*time.Second):
				t.Fatal("blok did not exit after the signal")
			}
			if code := command.ProcessState.ExitCode(); code != devtool.ExitInterrupted || stderr.Len() != 0 {
				t.Fatalf("exit=%d stderr=%q", code, stderr.String())
			}
			// The forwarded interrupt stops go test; the SIGKILL after the
			// grace is only a backstop.
			if elapsed := time.Since(signaled); elapsed >= devtool.InterruptGrace {
				t.Fatalf("exit took %s, not less than the %s grace", elapsed, devtool.InterruptGrace)
			}
			var report devtool.Report
			if err := json.Unmarshal(stdout.Bytes(), &report); err != nil {
				t.Fatalf("the report was not flushed whole: %v\n%s", err, stdout.String())
			}
			statuses := map[string]string{}
			for _, item := range report.Tests.Packages {
				for _, record := range item.Tests {
					statuses[record.Name] = record.Status
				}
			}
			if report.Status != devtool.StatusInterrupted || report.ExitCode != devtool.ExitInterrupted || statuses["TestQuoteOverHTTP"] != devtool.TestPass || statuses["TestSlow"] != devtool.TestIncomplete {
				t.Fatalf("report=%s", stdout.String())
			}
			assertGone(t, run.pid)
		})
	}
}

type slowRun struct {
	command        *exec.Cmd
	stdout, stderr *bytes.Buffer
	done           chan error
	pid            int
}

// startSlowTest starts blok test --json, in its own process group as a
// shell starts a foreground job, on an application whose second test
// sleeps, and returns once that test binary is running. Cleanup kills
// blok's group and the test binary's group (go's), so a failing test
// leaves nothing behind.
func startSlowTest(t *testing.T) slowRun {
	t.Helper()
	dir := shopProject(t, "classic")
	marker := filepath.Join(t.TempDir(), "pid")
	slow := "package app\n\nimport (\n\t\"os\"\n\t\"strconv\"\n\t\"testing\"\n\t\"time\"\n)\n\nfunc TestSlow(t *testing.T) {\n\t_ = os.WriteFile(" + strconv.Quote(marker) + ", []byte(strconv.Itoa(os.Getpid())), 0o644)\n\ttime.Sleep(10 * time.Minute)\n}\n"
	if err := os.WriteFile(filepath.Join(dir, "internal", "app", "slow_test.go"), []byte(slow), 0o644); err != nil {
		t.Fatal(err)
	}
	run := slowRun{command: exec.Command(blokBinary(t), "test", "--json"), stdout: &bytes.Buffer{}, stderr: &bytes.Buffer{}, done: make(chan error, 1)}
	run.command.Dir = dir
	run.command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	run.command.Stdout, run.command.Stderr = run.stdout, run.stderr
	if err := run.command.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { run.done <- run.command.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(-run.command.Process.Pid, syscall.SIGKILL)
		if run.pid > 0 {
			if group, err := syscall.Getpgid(run.pid); err == nil && group > 1 {
				_ = syscall.Kill(-group, syscall.SIGKILL)
			}
		}
	})
	for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(50 * time.Millisecond) {
		if data, err := os.ReadFile(marker); err == nil {
			if pid, err := strconv.Atoi(string(data)); err == nil && pid > 0 {
				run.pid = pid
				return run
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the slow test never started\n%s\n%s", run.stdout.String(), run.stderr.String())
		}
	}
}

func assertGone(t *testing.T, pid int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); running(pid); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			stat, _ := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
			t.Fatalf("process %d outlived blok (stat: %s)", pid, stat)
		}
	}
}

// running reports whether pid is a live process. A zombie has exited: it
// waits only for its parent to reap it, which in a container whose init is
// a plain shell may never happen, so Linux's /proc state Z counts as gone.
func running(pid int) bool {
	if syscall.Kill(pid, 0) != nil {
		return false
	}
	stat, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return true
	}
	// The state follows the parenthesised command name.
	fields := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
	return len(fields) == 0 || fields[0] != "Z"
}

// groupRunning reports whether any live process is in the process group.
func groupRunning(group int) bool {
	if syscall.Kill(-group, 0) != nil {
		return false
	}
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return true
	}
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if member, err := syscall.Getpgid(pid); err == nil && member == group && running(pid) {
			return true
		}
	}
	return false
}

// TestToolDeathKillsTheGoCommand: SIGKILL gives blok no chance to run any
// code, yet the go command and the test binary it started stop, because
// the pipe guard sees blok's end of its pipe close.
func TestToolDeathKillsTheGoCommand(t *testing.T) {
	run := startSlowTest(t)
	group, err := syscall.Getpgid(run.pid)
	if err != nil || group == run.command.Process.Pid {
		t.Fatalf("the test binary is not in its own go group: group=%d err=%v", group, err)
	}
	if err := syscall.Kill(run.command.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	<-run.done
	assertGone(t, run.pid)
	for deadline := time.Now().Add(10 * time.Second); groupRunning(group); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("go's process group %d outlived blok", group)
		}
	}
}

// TestToolClosedPipeExitsFour: blok inspect --json | head, with head
// already gone, exits 4 and says so, instead of dying of SIGPIPE.
func TestToolClosedPipeExitsFour(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	_ = reader.Close()
	defer writer.Close()
	command := exec.Command(blokBinary(t), "inspect", "--json", t.TempDir())
	var stderr bytes.Buffer
	command.Stdout, command.Stderr = writer, &stderr
	_ = command.Run()
	if code := command.ProcessState.ExitCode(); code != devtool.ExitOutput || !strings.Contains(stderr.String(), "write output: ") {
		t.Fatalf("exit=%d (%v) stderr=%q", code, command.ProcessState, stderr.String())
	}
}
