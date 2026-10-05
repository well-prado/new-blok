//go:build !windows

package devtool

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/layout"
	"github.com/well-prado/new-blok/observe/redact"
)

// heldApp is shellApp whose fake go command can be held or failed per
// build from the test: while $CONTROL/hold-build-N exists, build N waits
// for $CONTROL/release-build-N; while $CONTROL/fail-build-N exists, build N
// fails with a compile error.
func heldApp(t *testing.T, body string) (DevOptions, string, string) {
	t.Helper()
	dir := fakeProject(t)
	writeFiles(t, dir, map[string]string{"cmd/fake/main.go": "package main\n\nfunc main() {}\n"})
	markers, control := t.TempDir(), t.TempDir()
	script := `[ "$1" = build ] && [ "$2" = -o ] || exit 2
d=${3%/*}; n=${d##*/}
if [ -e "` + control + `/hold-$n" ]; then
  while [ ! -e "` + control + `/release-$n" ]; do sleep 0.02; done
fi
if [ -e "` + control + `/fail-$n" ]; then
  echo "cmd/fake/main.go:3:1: syntax error: fixture failure" >&2
  exit 1
fi
cat > "$3" <<'APP'
#!/bin/sh
echo "$0" > "$MARKERS/executable-$BLOK_DEV_GENERATION"
` + body + `
APP
chmod +x "$3"
`
	options := DevOptions{Options: Options{Root: dir, Go: fakeGo(t, script)}, AppEnv: append(os.Environ(), "MARKERS="+markers)}
	return options, markers, control
}

func touch(t *testing.T, name string) {
	t.Helper()
	if err := os.WriteFile(name, nil, 0o644); err != nil {
		t.Fatal(err)
	}
}

func editMain(t *testing.T, root string, n int) {
	t.Helper()
	writeFiles(t, root, map[string]string{"cmd/fake/main.go": fmt.Sprintf("package main\n\nfunc main() { _ = %d }\n", n)})
}

func eventsOf(events []timedEvent, name string, build int) []timedEvent {
	var found []timedEvent
	for _, event := range events {
		if event.Event == name && (build == 0 || event.Build == build) {
			found = append(found, event)
		}
	}
	return found
}

// TestDevStopDuringReplaceStartsNothing (review R1): a stop requested while
// blok dev is replacing the application (here: while the old one drains
// on SIGTERM, right after build-succeeded) starts nothing. The new build
// is never launched only to be killed.
func TestDevStopDuringReplaceStartsNothing(t *testing.T) {
	options, markers, _ := heldApp(t, `trap 'sleep 2; exit 0' TERM
echo $$ > "$MARKERS/ready-$BLOK_DEV_GENERATION"
sleep 300 &
wait`)
	session := startDev(t, options)
	session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")
	readPID(t, filepath.Join(markers, "ready-1"))
	editMain(t, options.Root, 2)
	session.await(func(e DevEvent) bool { return e.Event == EventBuildSucceeded && e.Build == 2 }, "build 2 succeeded")
	// The old application now drains for 2 s; the stop lands inside it.
	events := session.stop()
	if started := eventsOf(events, EventAppStarted, 2); len(started) != 0 {
		t.Fatalf("build 2 was started after the stop was requested\n%s", session.dump())
	}
	stopped := eventsOf(events, EventAppStopped, 0)
	if len(stopped) != 1 || stopped[0].Build != 1 || stopped[0].Status != "exit status 0" || len(stopped[0].Diagnostics) != 0 {
		t.Fatalf("stops %+v\n%s", stopped, session.dump())
	}
	if last := events[len(events)-1]; last.Event != EventStopped || last.ExitCode == nil || *last.ExitCode != ExitInterrupted {
		t.Fatalf("last event %+v", last.DevEvent)
	}
	assertNoProcesses(t, events)
}

// TestDevCrashDuringBuildIsAnExit (review R5): an application that dies
// while a build runs is reported as what it is, app-exited with
// dev_app_exited and its output tail, never as app-stopped. When the build
// succeeds the replacement starts with no restart scheduled for the dead
// one; when it fails, build-failed says nothing is running and the dead
// build restarts with backoff.
func TestDevCrashDuringBuildIsAnExit(t *testing.T) {
	for _, fails := range []bool{false, true} {
		t.Run(fmt.Sprintf("build-fails=%v", fails), func(t *testing.T) {
			options, markers, control := heldApp(t, `echo $$ > "$MARKERS/ready-$BLOK_DEV_GENERATION"
if [ "$BLOK_DEV_GENERATION" = 1 ]; then
  while [ ! -e "$MARKERS/crash" ]; do sleep 0.02; done
  echo "fixture: crashing mid-build" >&2
  exit 3
fi
trap 'exit 0' TERM
sleep 300 &
wait`)
			options.Backoff = 100 * time.Millisecond
			session := startDev(t, options)
			session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")
			pid := readPID(t, filepath.Join(markers, "ready-1"))
			touch(t, filepath.Join(control, "hold-build-2"))
			if fails {
				touch(t, filepath.Join(control, "fail-build-2"))
			}
			editMain(t, options.Root, 2)
			session.await(func(e DevEvent) bool { return e.Event == EventBuildStarted && e.Build == 2 }, "build 2 started")
			touch(t, filepath.Join(markers, "crash"))
			// Reaped: the application is gone before the build ends.
			gone(t, pid)
			touch(t, filepath.Join(control, "release-build-2"))
			if fails {
				failed := session.await(func(e DevEvent) bool { return e.Event == EventBuildFailed && e.Build == 2 }, "build 2 failed")
				if failed.Running != 0 {
					t.Fatalf("build-failed claims build %d keeps serving, but it crashed\n%s", failed.Running, session.dump())
				}
				session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 && e.Generation == 2 }, "build 1 restarted")
			} else {
				session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 2 }, "build 2 started")
			}
			events := session.stop()
			exited := eventsOf(events, EventAppExited, 1)
			if len(exited) != 1 || exited[0].Status != "exit status 3" || len(exited[0].Diagnostics) != 1 || exited[0].Diagnostics[0].Code != "dev_app_exited" || !strings.Contains(strings.Join(exited[0].Output, "\n"), "fixture: crashing mid-build") {
				t.Fatalf("the crash was not reported as app-exited: %+v\n%s", exited, session.dump())
			}
			for _, stop := range eventsOf(events, EventAppStopped, 1) {
				if stop.Generation == 1 {
					t.Fatalf("the crashed application was reported as stopped by blok dev: %+v\n%s", stop.DevEvent, session.dump())
				}
			}
			restarts := eventsOf(events, EventRestartScheduled, 1)
			if fails && len(restarts) != 1 || !fails && len(restarts) != 0 {
				t.Fatalf("restarts %+v (build fails: %v)\n%s", restarts, fails, session.dump())
			}
			assertNoProcesses(t, events)
		})
	}
}

// TestDevFirstScanOverTheBoundIsFatal (review R2): a project past the
// watch bounds at the first scan cannot be watched at all, so blok dev
// exits 1 with layout_limit_exceeded instead of running an application it
// would never rebuild.
func TestDevFirstScanOverTheBoundIsFatal(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	saved := watchBounds
	watchBounds.files = 2
	defer func() { watchBounds = saved }()
	var events []DevEvent
	options.Emit = func(event DevEvent) error { events = append(events, event); return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	code, err := Dev(ctx, options)
	var names []string
	for _, event := range events {
		names = append(names, event.Event)
	}
	if code != ExitFindings || err != nil || fmt.Sprint(names) != "[watch-failed stopped]" || len(events[0].Diagnostics) != 1 || events[0].Diagnostics[0].Code != layout.CodeLimitExceeded {
		t.Fatalf("code=%d err=%v events=%v %+v", code, err, names, events)
	}
}

// TestDevRefusesLinkedPackageDirectories (review R3): a symbolic link to a
// directory inside the module (internal/shared -> a directory outside the
// project) would be compiled by go build but never watched, so the build
// is refused with dev_symlink_unwatched; a link inside a skipped directory
// is not the module's and is ignored.
func TestDevRefusesLinkedPackageDirectories(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	outside := t.TempDir()
	writeFiles(t, outside, map[string]string{"lib.go": "package shared\n"})
	if err := os.MkdirAll(filepath.Join(options.Root, "internal"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(options.Root, "internal", "shared")); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(options.Root, "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(options.Root, "testdata", "linked")); err != nil {
		t.Fatal(err)
	}
	session := startDev(t, options)
	first := session.await(func(e DevEvent) bool {
		return (e.Event == EventBuildFailed || e.Event == EventBuildSucceeded) && e.Build == 1
	}, "build 1 to finish")
	if first.Event != EventBuildFailed || len(first.Diagnostics) != 1 || first.Diagnostics[0].Code != "dev_symlink_unwatched" || first.Diagnostics[0].Source != "internal/shared" {
		t.Fatalf("a linked package directory was built: %+v\n%s", first.DevEvent, session.dump())
	}
	if err := os.Remove(filepath.Join(options.Root, "internal", "shared")); err != nil {
		t.Fatal(err)
	}
	session.await(func(e DevEvent) bool { return e.Event == EventBuildSucceeded && e.Build == 2 }, "the build after removing the link")
	session.stop()
}

// TestDevDiagnosticsAreRedactedAtEmit (review R8): credentials in a build's
// diagnostics never reach the event stream, whatever produced them; the
// redaction at emit is the boundary.
func TestDevDiagnosticsAreRedactedAtEmit(t *testing.T) {
	options, _, control := heldApp(t, `exit 0`)
	touch(t, filepath.Join(control, "fail-build-1"))
	secrets := []string{"hunter2", "AKIAIOSFODNN7EXAMPLE", "ghp_0123456789abcdefghijABCDEFGHIJ012345"}
	leaky := fakeGo(t, `echo "cmd/fake/main.go:3:1: undefined: password=hunter2" >&2
echo "cmd/fake/main.go:4:1: cannot use AKIAIOSFODNN7EXAMPLE (untyped string constant) as int value" >&2
echo "cmd/fake/main.go:5:1: invalid token ghp_0123456789abcdefghijABCDEFGHIJ012345" >&2
exit 1
`)
	options.Go = leaky
	session := startDev(t, options)
	failed := session.await(func(e DevEvent) bool { return e.Event == EventBuildFailed && e.Build == 1 }, "build 1 failed")
	session.stop()
	var encoded bytes.Buffer
	if err := WriteDevJSON(&encoded, failed.DevEvent); err != nil {
		t.Fatal(err)
	}
	if len(failed.Diagnostics) == 0 {
		t.Fatalf("no diagnostics: %s", encoded.String())
	}
	for _, secret := range secrets {
		if strings.Contains(encoded.String(), secret) {
			t.Fatalf("build-failed leaked %q:\n%s", secret, encoded.String())
		}
	}
}

// TestDevChangedPathsAreRedactedAndCapped (review R8): a changed event
// counts every changed path, lists only the first MaxChangedListed in
// order, and passes each listed path through observe/redact.
func TestDevChangedPathsAreRedactedAndCapped(t *testing.T) {
	options, _, _ := heldApp(t, `trap 'exit 0' TERM
sleep 300 &
wait`)
	session := startDev(t, options)
	session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")
	const secretName = "internal/aa/ghp_0123456789abcdefghijABCDEFGHIJ012345.go"
	if redact.String(secretName) != redact.Marker {
		t.Fatalf("fixture path %q is not credential-shaped to observe/redact", secretName)
	}
	files := map[string]string{secretName: "package aa\n"}
	var names []string
	for index := range MaxChangedListed + 5 {
		name := fmt.Sprintf("internal/many/f%02d.go", index)
		files[name] = "package many\n"
		names = append(names, name)
	}
	writeFiles(t, options.Root, files)
	changed := session.await(func(e DevEvent) bool { return e.Event == EventChanged }, "changed")
	session.stop()
	sort.Strings(names)
	want := append([]string{redact.Marker}, names[:MaxChangedListed-1]...)
	if changed.Files != MaxChangedListed+6 || fmt.Sprint(changed.Changed) != fmt.Sprint(want) {
		t.Fatalf("changed files=%d listed=%v\nwant files=%d listed=%v", changed.Files, changed.Changed, MaxChangedListed+6, want)
	}
}

// TestScanRefusesLinkedDirectories (review R3): a link to a directory that
// is not skipped is recorded and refused, like a linked source file; one
// under a skipped directory is not read at all.
func TestScanRefusesLinkedDirectories(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	writeFiles(t, root, map[string]string{"go.mod": "module example.com/x\n", "main.go": "package main\n", "vendor/keep.txt": ""})
	writeFiles(t, outside, map[string]string{"lib.go": "package lib\n", "notes.txt": ""})
	for link, target := range map[string]string{"internal/shared": outside, "vendor/linked": outside, "README.md": filepath.Join(outside, "notes.txt")} {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, link)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(root, filepath.FromSlash(link))); err != nil {
			t.Fatal(err)
		}
	}
	scan, problem, err := scanProject(context.Background(), root)
	if err != nil || problem != nil {
		t.Fatal(err, problem)
	}
	links := scan.links()
	if len(links) != 1 || links[0].Code != "dev_symlink_unwatched" || links[0].Source != "internal/shared" {
		t.Fatalf("links %+v (scan %v)", links, scan)
	}
}

// TestChangesSeeMetadataPreservingEdits (review R4): an edit that keeps a
// file's size and restores its modification time (touch -r), and a
// replacement renamed over the file with the same size and modification
// time (cp -p, rsync -a, tar -x), are both changes. The inode and the
// status-change time see what size and mtime cannot.
func TestChangesSeeMetadataPreservingEdits(t *testing.T) {
	root := t.TempDir()
	writeFiles(t, root, map[string]string{"go.mod": "module x\n", "a.go": "package a // one\n", "b.go": "package a // one\n"})
	before, _, _ := scanProject(context.Background(), root)
	time.Sleep(20 * time.Millisecond)
	for _, name := range []string{"a.go", "b.go"} {
		target := filepath.Join(root, name)
		info, err := os.Stat(target)
		if err != nil {
			t.Fatal(err)
		}
		edited := target
		if name == "b.go" {
			edited = filepath.Join(root, ".b.go.tmp")
		}
		if err := os.WriteFile(edited, []byte("package a // two\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(edited, info.ModTime(), info.ModTime()); err != nil {
			t.Fatal(err)
		}
		if edited != target {
			if err := os.Rename(edited, target); err != nil {
				t.Fatal(err)
			}
		}
		if after, err := os.Stat(target); err != nil || after.Size() != info.Size() || !after.ModTime().Equal(info.ModTime()) {
			t.Fatalf("%s: the edit did not keep size and mtime (%v)", name, err)
		}
	}
	after, _, _ := scanProject(context.Background(), root)
	if got := changes(before, after); fmt.Sprint(got) != "[a.go b.go]" {
		t.Fatalf("changes %v, want [a.go b.go]", got)
	}
}
