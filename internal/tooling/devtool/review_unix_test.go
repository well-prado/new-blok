//go:build !windows

package devtool

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fakeProject is the smallest valid project: blok.json and go.mod. Tests
// that pair it with fakeGo exercise how a report folds the go command's
// output, never in place of running real tests (TestFixtures does that).
func fakeProject(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	for name, content := range map[string]string{
		"go.mod":    "module example.com/fake\n\ngo 1.27.0\n",
		"blok.json": `{"name":"fake","module":"example.com/fake","runtime":"go","layout":"classic","triggers":["http"]}` + "\n",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// fakeGo writes a /bin/sh script standing in for the go command.
func fakeGo(t *testing.T, script string) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "go")
	if err := os.WriteFile(name, []byte("#!/bin/sh\n"+script), 0o755); err != nil {
		t.Fatal(err)
	}
	return name
}

func waitForPID(t *testing.T, marker string) int {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if data, err := os.ReadFile(marker); err == nil {
			if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil && pid > 0 {
				return pid
			}
		}
	}
	t.Fatal("the fake go command never started its child")
	return 0
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

// TestStderrIsRedactedAndProgressIgnored: go's standard error is folded
// into diagnostics through the redaction boundary, a PEM block and a bearer
// token never reach the report, and "go: downloading" progress is not a
// problem.
func TestStderrIsRedactedAndProgressIgnored(t *testing.T) {
	dir := fakeProject(t)
	leaky := fakeGo(t, `echo 'go: downloading example.org/x v1.0.0' >&2
echo 'go: example.org/x@v1.0.0: reading https://proxy.invalid: Authorization: Bearer fixture0123456789abcdefghijkl' >&2
echo '-----BEGIN RSA PRIVATE KEY-----' >&2
echo 'MIIEpAIBAAKCAQEA0Z3VS5JJcds3xfn/ygWyF8PbnGy0AHB7MaGlDQe6G1XzXc5a' >&2
echo '-----END RSA PRIVATE KEY-----' >&2
exit 1
`)
	for _, report := range []Report{Check(context.Background(), Options{Root: dir, Go: leaky}), Test(context.Background(), TestOptions{Options: Options{Root: dir, Go: leaky}})} {
		encoded := encode(t, report)
		if report.ExitCode != ExitFindings {
			t.Fatalf("%s: %s", report.Command, encoded)
		}
		for _, secret := range []string{"fixture0123456789", "MIIEpAIBAAKCAQEA0Z3VS5JJ", "BEGIN RSA"} {
			if bytes.Contains(encoded, []byte(secret)) {
				t.Fatalf("%s leaked %q:\n%s", report.Command, secret, encoded)
			}
		}
	}
	quiet := fakeGo(t, `echo 'go: downloading example.org/x v1.0.0' >&2
echo '{"Action":"pass","Package":"example.com/fake","Test":"TestOne"}'
exit 0
`)
	if report := Check(context.Background(), Options{Root: dir, Go: quiet}); report.ExitCode != ExitOK {
		t.Fatalf("progress was reported as a problem: %s", encode(t, report))
	}
	if report := Test(context.Background(), TestOptions{Options: Options{Root: dir, Go: quiet}}); report.ExitCode != ExitOK {
		t.Fatalf("progress was reported as a problem: %s", encode(t, report))
	}
}

// TestTestsAreBounded: past MaxTests the report keeps MaxTests tests and
// says it was truncated.
func TestTestsAreBounded(t *testing.T) {
	dir := fakeProject(t)
	many := fakeGo(t, fmt.Sprintf(`awk 'BEGIN { for (i = 0; i < %d; i++) printf "{\"Action\":\"pass\",\"Package\":\"example.com/fake\",\"Test\":\"Test%%d\"}\n", i }'
`, MaxTests+5))
	report := Test(context.Background(), TestOptions{Options: Options{Root: dir, Go: many}})
	if report.Tests == nil || report.Tests.Passed != MaxTests || !report.Truncated {
		t.Fatalf("passed=%v truncated=%v", report.Tests, report.Truncated)
	}
}

// TestGraceKillsAGroupThatIgnoresInterrupts: a go command whose process
// group ignores SIGINT is killed once InterruptGrace has passed.
func TestGraceKillsAGroupThatIgnoresInterrupts(t *testing.T) {
	dir := fakeProject(t)
	marker := filepath.Join(t.TempDir(), "pid")
	stubborn := fakeGo(t, `trap '' INT
sleep 600 &
echo $! > `+marker+`
wait
`)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan Report, 1)
	go func() { done <- Test(ctx, TestOptions{Options: Options{Root: dir, Go: stubborn}}) }()
	child := waitForPID(t, marker)
	t.Cleanup(func() { _ = syscall.Kill(child, syscall.SIGKILL) })
	canceled := time.Now()
	cancel()
	select {
	case report := <-done:
		elapsed := time.Since(canceled)
		if elapsed < InterruptGrace-100*time.Millisecond || report.ExitCode != ExitInterrupted {
			t.Fatalf("returned after %s with exit %d", elapsed, report.ExitCode)
		}
	case <-time.After(InterruptGrace + 20*time.Second):
		t.Fatal("the group was never killed")
	}
	for deadline := time.Now().Add(5 * time.Second); alive(child); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("child %d survived the grace kill", child)
		}
	}
}

// TestPanicKillsTheGroup: a panic while go output is being read kills the
// go command's process group at once and then reaches the caller.
func TestPanicKillsTheGroup(t *testing.T) {
	dir := fakeProject(t)
	marker := filepath.Join(t.TempDir(), "pid")
	slow := fakeGo(t, `sleep 600 &
echo $! > `+marker+`
echo '{"Action":"run","Package":"example.com/fake","Test":"TestSlow"}'
wait
`)
	var child int
	started := time.Now()
	func() {
		defer func() {
			if recovered := recover(); recovered != "fixture panic" {
				t.Fatalf("recovered %v", recovered)
			}
		}()
		Test(context.Background(), TestOptions{Options: Options{Root: dir, Go: slow, onLine: func(stream string, _ []byte) {
			if stream == "stdout" {
				child = waitForPID(t, marker)
				panic("fixture panic")
			}
		}}})
		t.Fatal("the panic did not propagate")
	}()
	if elapsed := time.Since(started); elapsed >= InterruptGrace {
		t.Fatalf("took %s", elapsed)
	}
	for deadline := time.Now().Add(5 * time.Second); alive(child); time.Sleep(20 * time.Millisecond) {
		if time.Now().After(deadline) {
			_ = syscall.Kill(child, syscall.SIGKILL)
			t.Fatalf("child %d survived the panic", child)
		}
	}
}
