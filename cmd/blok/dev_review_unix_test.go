//go:build !windows

package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/devtool"
)

// TestDevNohupKeepsRunningOnHangup (review R6): started as nohup blok dev &
// starts it, with SIGHUP and SIGINT ignored, blok dev keeps them ignored:
// a hangup or an interrupt does not stop it, SIGTERM still does.
func TestDevNohupKeepsRunningOnHangup(t *testing.T) {
	dir := shopProject(t, "classic")
	run := startDevCommand(t, dir, nil, "/bin/sh", "-c", `trap "" HUP INT; exec "$0" "$@"`, blokBinary(t), "dev", "--json")
	app := run.started(t, 1)
	for _, signal := range []syscall.Signal{syscall.SIGHUP, syscall.SIGINT} {
		if err := syscall.Kill(run.command.Process.Pid, signal); err != nil {
			t.Fatal(err)
		}
		select {
		case <-run.done:
			t.Fatalf("blok dev, started with %s ignored, exited on it: %v\n%s", signal, run.command.ProcessState, strings.Join(run.stdout.all(), "\n"))
		case <-time.After(time.Second):
		}
		if !running(app) {
			t.Fatalf("the application stopped on an ignored %s", signal)
		}
		run.serving(t)
	}
	if err := syscall.Kill(run.command.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if code := run.exit(t, time.Minute); code != devtool.ExitInterrupted {
		t.Fatalf("exit %d after SIGTERM", code)
	}
	assertGroupGone(t, app)
}

// TestDevKilledLeavesNoBuilds (review R9): SIGKILL removes blok dev's
// private build directory however it is caught: while the first build
// runs, when no application (and so no application guard) exists yet, and
// while idle after a failed build, when neither the go command nor an
// application is running. The session's own guard removes it, and the go
// command's guard kills the build.
func TestDevKilledLeavesNoBuilds(t *testing.T) {
	for _, idle := range []bool{false, true} {
		t.Run(map[bool]string{false: "during-the-first-build", true: "idle-after-a-failed-build"}[idle], func(t *testing.T) {
			dir := t.TempDir()
			for name, content := range map[string]string{
				"go.mod":           "module example.com/fake\n\ngo 1.27.0\n",
				"blok.json":        `{"name":"fake","module":"example.com/fake","runtime":"go","layout":"classic","triggers":["http"]}` + "\n",
				"cmd/fake/main.go": "package main\n\nfunc main() {}\n",
			} {
				if err := os.MkdirAll(filepath.Dir(filepath.Join(dir, name)), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			bin, temp, marker := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "go-pid")
			script := "#!/bin/sh\necho $$ > " + strconv.Quote(marker) + "\nexec sleep 300\n"
			if idle {
				script = "#!/bin/sh\necho $$ > " + strconv.Quote(marker) + "\necho 'cmd/fake/main.go:3:1: syntax error: fixture failure' >&2\nexit 1\n"
			}
			if err := os.WriteFile(filepath.Join(bin, "go"), []byte(script), 0o755); err != nil {
				t.Fatal(err)
			}
			run := startDevCommand(t, dir, []string{"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "TMPDIR=" + temp}, blokBinary(t), "dev", "--json")
			var goPID int
			for deadline := time.Now().Add(time.Minute); goPID == 0; time.Sleep(20 * time.Millisecond) {
				data, _ := os.ReadFile(marker)
				goPID, _ = strconv.Atoi(strings.TrimSpace(string(data)))
				if time.Now().After(deadline) {
					t.Fatalf("the build never started\n%s", strings.Join(run.stdout.all(), "\n"))
				}
			}
			if idle {
				run.stdout.wait(t, func(line string) bool { return strings.Contains(line, `"event":"build-failed"`) })
				assertGone(t, goPID)
			}
			builds, _ := filepath.Glob(filepath.Join(temp, "blok-dev-*"))
			if len(builds) != 1 {
				t.Fatalf("build directories %v", builds)
			}
			if err := syscall.Kill(run.command.Process.Pid, syscall.SIGKILL); err != nil {
				t.Fatal(err)
			}
			run.exit(t, time.Minute)
			assertGone(t, goPID)
			for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
				if _, err := os.Stat(builds[0]); os.IsNotExist(err) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("build directory %s outlived a SIGKILLed blok dev", builds[0])
				}
			}
		})
	}
}
