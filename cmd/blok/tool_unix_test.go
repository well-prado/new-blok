//go:build !windows

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/devtool"
)

// TestToolInterruptFlushesAndExits130 delivers a real signal to a blok test
// process while one of the application's tests is running, three ways: an
// interrupt to blok alone, Ctrl+C at a terminal (an interrupt to blok's
// whole foreground process group) and SIGTERM. Each time blok writes one
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
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := shopProject(t, "classic")
			marker := filepath.Join(t.TempDir(), "pid")
			slow := "package app\n\nimport (\n\t\"os\"\n\t\"strconv\"\n\t\"testing\"\n\t\"time\"\n)\n\nfunc TestSlow(t *testing.T) {\n\t_ = os.WriteFile(" + strconv.Quote(marker) + ", []byte(strconv.Itoa(os.Getpid())), 0o644)\n\ttime.Sleep(10 * time.Minute)\n}\n"
			if err := os.WriteFile(filepath.Join(dir, "internal", "app", "slow_test.go"), []byte(slow), 0o644); err != nil {
				t.Fatal(err)
			}
			command := exec.Command(blokBinary(t), "test", "--json")
			command.Dir = dir
			// Its own process group, as a shell gives a foreground job.
			command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			var stdout, stderr bytes.Buffer
			command.Stdout, command.Stderr = &stdout, &stderr
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- command.Wait() }()
			var pidText []byte
			for deadline := time.Now().Add(3 * time.Minute); ; time.Sleep(50 * time.Millisecond) {
				var err error
				if pidText, err = os.ReadFile(marker); err == nil && len(pidText) > 0 {
					break
				}
				if time.Now().After(deadline) {
					_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
					t.Fatalf("the slow test never started\n%s\n%s", stdout.String(), stderr.String())
				}
			}
			signaled := time.Now()
			if err := test.signal(command.Process.Pid); err != nil {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(devtool.InterruptGrace + 30*time.Second):
				_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
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
			pid, _ := strconv.Atoi(string(pidText))
			for deadline := time.Now().Add(10 * time.Second); syscall.Kill(pid, 0) == nil; time.Sleep(50 * time.Millisecond) {
				if time.Now().After(deadline) {
					_ = syscall.Kill(pid, syscall.SIGKILL)
					t.Fatalf("test binary %d outlived blok", pid)
				}
			}
		})
	}
}
