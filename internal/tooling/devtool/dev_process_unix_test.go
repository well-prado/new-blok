//go:build !windows

package devtool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// shellApp is a project whose go command is a fake that "builds" a /bin/sh
// application with the given body, for testing blok dev's process
// handling quickly, alongside the real builds of TestDevFixtures. The body
// can write to $MARKERS.
func shellApp(t *testing.T, body string) (DevOptions, string) {
	t.Helper()
	dir := fakeProject(t)
	writeFiles(t, dir, map[string]string{"cmd/fake/main.go": "package main\n\nfunc main() {}\n"})
	markers := t.TempDir()
	script := `[ "$1" = build ] && [ "$2" = -o ] || exit 2
cat > "$3" <<'APP'
#!/bin/sh
echo "$0" > "$MARKERS/executable"
` + body + `
APP
chmod +x "$3"
`
	options := DevOptions{Options: Options{Root: dir, Go: fakeGo(t, script)}, AppEnv: append(os.Environ(), "MARKERS="+markers)}
	return options, markers
}

func readPID(t *testing.T, name string) int {
	t.Helper()
	var pid int
	waitFor(t, filepath.Base(name), func() bool {
		data, err := os.ReadFile(name)
		pid, _ = strconv.Atoi(strings.TrimSpace(string(data)))
		return err == nil && pid > 0
	})
	return pid
}

func gone(t *testing.T, pids ...int) {
	t.Helper()
	for _, pid := range pids {
		waitFor(t, "process "+strconv.Itoa(pid)+" to end", func() bool { return !alive(pid) })
	}
}

// TestDevSweepsDescendants: an application that exits on SIGTERM but
// leaves a child behind (a worker it failed to stop) does not leave it
// running: blok dev kills the application's whole process group.
func TestDevSweepsDescendants(t *testing.T) {
	options, markers := shellApp(t, `sleep 300 &
echo $! > "$MARKERS/child"
trap 'exit 0' TERM
wait`)
	session := startDev(t, options)
	started := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted }, "start")
	child := readPID(t, filepath.Join(markers, "child"))
	if group, err := syscall.Getpgid(child); err != nil || group != started.PID {
		t.Fatalf("child %d is in group %d (%v), want the application's %d", child, group, err, started.PID)
	}
	events := session.stop()
	gone(t, started.PID, child)
	assertNoProcesses(t, events, child)
	if executable := strings.TrimSpace(string(readFile(t, filepath.Join(markers, "executable")))); executable == "" {
		t.Fatal("no executable recorded")
	} else if _, err := os.Stat(filepath.Dir(executable)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("build directory %s outlived blok dev (%v)", filepath.Dir(executable), err)
	}
}

// TestDevForceKillsAtOnce: an application that ignores SIGTERM is killed
// as soon as Force receives (a second Ctrl+C), not after StopGrace.
func TestDevForceKillsAtOnce(t *testing.T) {
	options, markers := shellApp(t, `trap '' TERM
echo $$ > "$MARKERS/ready"
while :; do sleep 1; done`)
	force := make(chan struct{}, 1)
	options.Force, options.StopGrace = force, time.Hour
	session := startDev(t, options)
	started := session.await(func(e DevEvent) bool { return e.Event == EventAppStarted }, "start")
	readPID(t, filepath.Join(markers, "ready"))
	session.cancel()
	time.Sleep(300 * time.Millisecond)
	if !alive(started.PID) {
		t.Fatal("the application ignoring SIGTERM is already gone before force")
	}
	begin := time.Now()
	force <- struct{}{}
	events := session.stop()
	if took := time.Since(begin); took > 5*time.Second {
		t.Fatalf("force took %s", took)
	}
	assertNoProcesses(t, events)
	stopped := events[len(events)-2]
	if stopped.Event != EventAppStopped || len(stopped.Diagnostics) != 1 || stopped.Diagnostics[0].Code != "dev_app_stop_timeout" {
		t.Fatalf("stop event %+v", stopped.DevEvent)
	}
}

// TestDevPanicKillsTheApplication: a panic inside blok dev still kills the
// application's group and removes the builds before it propagates (the CLI
// turns it into exit 3).
func TestDevPanicKillsTheApplication(t *testing.T) {
	options, markers := shellApp(t, `sleep 300 &
echo $! > "$MARKERS/child"
wait`)
	var pid int
	options.Emit = func(event DevEvent) error {
		if event.Event == EventAppStarted {
			pid = event.PID
			readPID(t, filepath.Join(markers, "child"))
			panic("fixture panic")
		}
		return nil
	}
	func() {
		defer func() {
			if recovered := recover(); recovered != "fixture panic" {
				t.Fatalf("recovered %v", recovered)
			}
		}()
		_, _ = Dev(context.Background(), options)
	}()
	child := readPID(t, filepath.Join(markers, "child"))
	gone(t, pid, child)
	if syscall.Kill(-pid, 0) == nil {
		t.Fatalf("group %d survived the panic", pid)
	}
	executable := strings.TrimSpace(string(readFile(t, filepath.Join(markers, "executable"))))
	if _, err := os.Stat(filepath.Dir(executable)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("build directory survived the panic: %v", err)
	}
}

// TestDevOutputFailureStopsEverything: an event that cannot be written
// stops blok dev with ExitOutput and the application with it.
func TestDevOutputFailureStopsEverything(t *testing.T) {
	options, _ := shellApp(t, `trap 'exit 0' TERM
sleep 300 & wait`)
	var pid int
	options.Emit = func(event DevEvent) error {
		if event.Event == EventAppStarted {
			pid = event.PID
			return errors.New("disk full")
		}
		return nil
	}
	done := make(chan struct{})
	var code int
	var err error
	go func() { code, err = Dev(context.Background(), options); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Minute):
		t.Fatal("blok dev kept running after its output failed")
	}
	if code != ExitOutput || err == nil || err.Error() != "disk full" {
		t.Fatalf("code=%d err=%v", code, err)
	}
	gone(t, pid)
}

// TestDevCleanExitWaitsForAChange: an application that exits 0 on its own
// is not restarted; the next change rebuilds and starts it.
func TestDevCleanExitWaitsForAChange(t *testing.T) {
	options, _ := shellApp(t, `exit 0`)
	session := startDev(t, options)
	exited := session.await(func(e DevEvent) bool { return e.Event == EventAppExited }, "exit")
	if exited.Status != "exit status 0" || len(exited.Diagnostics) != 0 {
		t.Fatalf("exit %+v", exited.DevEvent)
	}
	time.Sleep(time.Second)
	writeFiles(t, options.Root, map[string]string{"cmd/fake/main.go": "package main\n\nfunc main() { _ = 1 }\n"})
	session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 2 }, "rebuild")
	events := session.stop()
	starts, restarts := 0, 0
	for _, event := range events {
		switch event.Event {
		case EventAppStarted:
			starts++
		case EventRestartScheduled:
			restarts++
		}
	}
	if starts != 2 || restarts != 0 {
		t.Fatalf("starts=%d restarts=%d", starts, restarts)
	}
}

// TestDevStartFailures: an unreadable project exits 1, a missing go
// command and a missing process guard exit 3, each with its diagnostic.
func TestDevStartFailures(t *testing.T) {
	missing := DevOptions{Options: Options{Root: filepath.Join(t.TempDir(), "absent")}}
	noGo, _ := shellApp(t, "exit 0")
	noGo.Go = filepath.Join(t.TempDir(), "go")
	noGuard, _ := shellApp(t, "exit 0")
	for _, test := range []struct {
		name    string
		options DevOptions
		guard   string
		code    int
		problem string
	}{
		{"unreadable", missing, guardShell, ExitFindings, "project_unreadable"},
		{"no go", noGo, guardShell, ExitTool, "go_toolchain_unavailable"},
		{"no guard", noGuard, filepath.Join(t.TempDir(), "sh"), ExitTool, "process_guard_unavailable"},
	} {
		t.Run(test.name, func(t *testing.T) {
			saved := guardShell
			guardShell = test.guard
			defer func() { guardShell = saved }()
			var codes []string
			test.options.Emit = func(event DevEvent) error {
				for _, problem := range event.Diagnostics {
					codes = append(codes, problem.Code)
				}
				return nil
			}
			code, err := Dev(context.Background(), test.options)
			if code != test.code || err != nil || strings.Join(codes, ",") != test.problem {
				t.Fatalf("code=%d err=%v diagnostics=%v", code, err, codes)
			}
		})
	}
}
