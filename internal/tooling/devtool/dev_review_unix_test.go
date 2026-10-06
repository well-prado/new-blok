//go:build !windows

package devtool

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
// project) that the application imports would be compiled by go build but
// never watched, so the build is refused with dev_symlink_unwatched; a link
// inside a skipped directory that nothing imports is ignored. Replacing the
// link with the directory itself builds.
func TestDevRefusesLinkedPackageDirectories(t *testing.T) {
	options, _, _ := heldApp(t, `exit 0`)
	outside := t.TempDir()
	writeFiles(t, outside, map[string]string{"lib.go": "package shared\n"})
	writeFiles(t, options.Root, map[string]string{"cmd/fake/main.go": mainImporting("internal/shared")})
	linkAll(t, options.Root, map[string]string{"internal/shared": outside, "testdata/linked": outside})
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
	writeFiles(t, options.Root, map[string]string{"internal/shared/lib.go": "package shared\n"})
	session.await(func(e DevEvent) bool { return e.Event == EventBuildSucceeded && e.Build == 2 }, "the build after replacing the link")
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

// TestScanRefusesLinkedDirectories (review R3, round 2): a link the build
// reads a package through, to files the watcher does not see, is refused
// whatever its name: one leaving the project, and one into a skipped
// directory or a nested module. A link inside the project to a watched
// directory is harmless; a link nothing builds through (a file link, a
// directory nothing imports) is recorded, so retargeting it rebuilds, but
// not refused; a link inside a skipped directory, or named with a leading
// dot (no import path can name it), is not read at all.
func TestScanRefusesLinkedDirectories(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	writeFiles(t, root, map[string]string{
		"go.mod": "module example.com/x\n", "vendor/keep.txt": "",
		"cmd/app/main.go":    "package main\n\nimport (\n\t_ \"example.com/x/internal/shared\"\n\t_ \"example.com/x/internal/fixture\"\n\t_ \"example.com/x/internal/sub\"\n\t_ \"example.com/x/internal/alias\"\n)\n",
		"internal/real/r.go": "package real\n", "docs/notes.md": "", "testdata/pkg/p.go": "package pkg\n",
		"nested/go.mod": "module example.com/nested\n", "nested/lib/l.go": "package lib\n",
	})
	writeFiles(t, outside, map[string]string{"lib.go": "package lib\n", "notes.txt": ""})
	linkAll(t, root, map[string]string{
		"internal/shared":  outside,                                // refused: leaves the project
		"internal/fixture": filepath.Join(root, "testdata", "pkg"), // refused: into a skipped directory
		"internal/sub":     "../nested/lib",                        // refused: into a nested module
		"internal/alias":   "real",                                 // harmless: a watched directory
		"internal/unused":  outside,                                // not refused: nothing imports it
		"NOTES.txt":        filepath.Join(outside, "notes.txt"),    // not refused: nothing builds it
		"README.md":        "docs/notes.md",                        // not refused: nothing builds it
		"vendor/linked":    outside,                                // not read: in a skipped directory
		".env":             filepath.Join(outside, "notes.txt"),    // not read: hidden
	})
	scan, problem, err := scanProject(context.Background(), root)
	if err != nil || problem != nil {
		t.Fatal(err, problem)
	}
	for _, name := range []string{"internal/shared", "internal/unused", "NOTES.txt", "README.md"} {
		if _, ok := scan[name]; !ok {
			t.Fatalf("link %s not recorded: %v", name, scan)
		}
	}
	for _, name := range []string{"vendor/linked", ".env"} {
		if _, ok := scan[name]; ok {
			t.Fatalf("link %s recorded", name)
		}
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	found, err := scan.buildReads(context.Background(), resolved, "example.com/x", "cmd/app")
	if err != nil {
		t.Fatal(err)
	}
	var refused []string
	for _, link := range found {
		if link.Code != "dev_symlink_unwatched" {
			t.Fatalf("found %+v", found)
		}
		refused = append(refused, link.Source+": "+link.Actual)
	}
	want := []string{
		"internal/fixture: a symbolic link into testdata, which blok dev does not watch; the build reads package internal/fixture through it",
		"internal/shared: a symbolic link leading outside the project; the build reads package internal/shared through it",
		"internal/sub: a symbolic link into the nested module nested, which blok dev does not watch; the build reads package internal/sub through it",
	}
	if fmt.Sprint(refused) != fmt.Sprint(want) {
		t.Fatalf("refused links\n%q\nwant\n%q", refused, want)
	}
	// Removing a refused link is a change, so the build after it runs.
	if err := os.Remove(filepath.Join(root, "internal", "shared")); err != nil {
		t.Fatal(err)
	}
	after, _, _ := scanProject(context.Background(), root)
	if got := changes(scan, after); fmt.Sprint(got) != "[internal/shared]" {
		t.Fatalf("changes %v", got)
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

// observedScan is one scan the loop reported to scanObserver.
type observedScan struct {
	at     time.Time // when it finished
	took   time.Duration
	failed bool
}

// observeScans records every scan of the sessions started after it, until
// the test ends.
func observeScans(t *testing.T) func() []observedScan {
	t.Helper()
	var mu sync.Mutex
	var scans []observedScan
	saved := scanObserver
	scanObserver = func(took time.Duration, failed bool) {
		mu.Lock()
		defer mu.Unlock()
		scans = append(scans, observedScan{time.Now(), took, failed})
	}
	// Registered before the session's own cleanup, so it runs after the
	// session has stopped.
	t.Cleanup(func() { scanObserver = saved })
	return func() []observedScan {
		mu.Lock()
		defer mu.Unlock()
		return append([]observedScan(nil), scans...)
	}
}

// TestDevWatchFailureBacksOff (review R2): a project that grows past the
// watch bounds mid-session is reported once, the running application keeps
// serving, and the failing scans back off, doubling from Poll up to
// MaxScanBackoff, instead of rescanning the over-bound tree at full speed.
// Once the project is back within bounds the watcher recovers, scans at
// Poll again, and the next edit rebuilds.
func TestDevWatchFailureBacksOff(t *testing.T) {
	options, _, _ := heldApp(t, `trap 'exit 0' TERM
sleep 300 &
wait`)
	options.Poll = 50 * time.Millisecond
	saved := watchBounds
	watchBounds.files = 6 // blok.json, go.mod, cmd/fake/main.go and room to edit
	t.Cleanup(func() { watchBounds = saved })
	scans := observeScans(t)
	session := startDev(t, options)
	session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")
	files := map[string]string{}
	for index := range 6 {
		files[fmt.Sprintf("internal/many/f%d.go", index)] = "package many\n"
	}
	writeFiles(t, options.Root, files)
	failed := session.await(func(e DevEvent) bool { return e.Event == EventWatchFailed }, "the watch to fail")
	if len(failed.Diagnostics) != 1 || failed.Diagnostics[0].Code != layout.CodeLimitExceeded || failed.Running != 1 {
		t.Fatalf("watch-failed %+v\n%s", failed.DevEvent, session.dump())
	}
	time.Sleep(4 * time.Second)
	var failures []observedScan
	for _, scan := range scans() {
		if scan.failed {
			failures = append(failures, scan)
		}
	}
	// At Poll (50 ms) four seconds hold about 80 scans; backing off from
	// 100 ms they hold at most 6 (0.1+0.2+0.4+0.8+1.6 s, then 3.2 s).
	if len(failures) < 3 || len(failures) > 7 {
		t.Fatalf("%d failed scans in about 4 s, want 3 to 7 (backing off)", len(failures))
	}
	for index := 2; index < len(failures); index++ {
		previous, gap := failures[index-1].at.Sub(failures[index-2].at), failures[index].at.Sub(failures[index-1].at)
		if gap < previous*3/2 && gap < MaxScanBackoff {
			t.Fatalf("failed scans %s apart after %s: not backing off\n%v", gap, previous, failures)
		}
	}
	if started := eventsOf(session.snapshot(), EventWatchFailed, 0); len(started) != 1 {
		t.Fatalf("watch-failed reported %d times, want once\n%s", len(started), session.dump())
	}
	for name := range files {
		if err := os.Remove(filepath.Join(options.Root, filepath.FromSlash(name))); err != nil {
			t.Fatal(err)
		}
	}
	var recovered int
	waitFor(t, "a successful scan", func() bool {
		all := scans()
		recovered = len(all) - 1
		return !all[recovered].failed
	})
	// The first successful scan ends the backoff: the interval is Poll
	// (50 ms) again, so 1.5 s holds many scans, not the one or none that
	// a backoff stuck at 3.2–10 s would.
	time.Sleep(1500 * time.Millisecond)
	if after := len(scans()) - 1 - recovered; after < 8 {
		t.Fatalf("%d scans in 1.5 s after the recovery, want at least 8 (back at Poll)\n%v", after, scans()[recovered:])
	}
	editMain(t, options.Root, 2)
	session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 2 }, "build 2 after the recovery")
	assertNoProcesses(t, session.stop())
}

// TestDevPollAdaptsToScanCost (review R2): the pause after each scan is at
// least ScanDuty times what the scan took, so on a project slow to scan
// the watcher uses at most about 1/ScanDuty of a CPU instead of scanning
// back to back.
func TestDevPollAdaptsToScanCost(t *testing.T) {
	options, _, _ := heldApp(t, `trap 'exit 0' TERM
sleep 300 &
wait`)
	// 10,000 entries nothing watches, so each scan costs real stats.
	for group := range 10 {
		dir := filepath.Join(options.Root, "assets", fmt.Sprintf("g%02d", group))
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for index := range 1000 {
			if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("a%04d.txt", index)), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	options.Poll = 5 * time.Millisecond
	scans := observeScans(t)
	session := startDev(t, options)
	session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")
	// However slow the machine (-race), wait for enough scans to compare.
	waitFor(t, "8 scans", func() bool { return len(scans()) >= 8 })
	observed := scans()
	session.stop()
	var busy time.Duration
	for index := 1; index < len(observed); index++ {
		previous, next := observed[index-1], observed[index]
		if previous.took*ScanDuty <= options.Poll {
			t.Fatalf("a scan took %s, too fast for this test to tell ScanDuty from Poll", previous.took)
		}
		// The next scan starts no sooner than ScanDuty times the previous
		// one's duration after it ended.
		if idle := next.at.Sub(previous.at) - next.took; idle < previous.took*ScanDuty-time.Millisecond {
			t.Fatalf("scan %d started %s after one that took %s; want at least %s", index, idle, previous.took, previous.took*ScanDuty)
		}
		busy += next.took
	}
	window := observed[len(observed)-1].at.Sub(observed[0].at)
	if share := float64(busy) / float64(window); share > 1.5/ScanDuty {
		t.Fatalf("the watcher scanned %.0f%% of the time, want at most about %d%%", 100*share, 100/ScanDuty)
	}
}

// TestDevKeepsRecentBuildsAndTheResumableOne (review R7): replaced builds'
// executables stay on disk until the session ends, the KeptBuilds most
// recent earlier ones plus the newest earlier one that did not refuse its
// durable state; every refusal (exit status 65) is reported as
// dev_durable_incompatible, is not restarted, and resumes with that build.
// When the session ends, all of them are removed.
func TestDevKeepsRecentBuildsAndTheResumableOne(t *testing.T) {
	// Builds 1 and 2 serve; every later one refuses its durable state.
	options, markers, _ := heldApp(t, `case "$0" in
*/build-1/*|*/build-2/*) ;;
*) echo "retained journal incompatible" >&2; exit 65 ;;
esac
trap 'exit 0' TERM
sleep 300 &
wait`)
	options.Args = []string{"--flag", "it's quoted"}
	session := startDev(t, options)
	session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == 1 }, "build 1 started")
	var buildDir string
	waitFor(t, "build 1's executable path", func() bool {
		data, err := os.ReadFile(filepath.Join(markers, "executable-1"))
		buildDir = filepath.Dir(filepath.Dir(strings.TrimSpace(string(data))))
		return err == nil && strings.HasSuffix(strings.TrimSpace(string(data)), "/build-1/fake")
	})
	const last = KeptBuilds + 4
	for build := 2; build <= last; build++ {
		editMain(t, options.Root, build)
		session.await(func(e DevEvent) bool { return e.Event == EventAppStarted && e.Build == build }, fmt.Sprintf("build %d started", build))
		if build < 3 {
			continue
		}
		refused := session.await(func(e DevEvent) bool { return e.Event == EventAppExited && e.Build == build }, fmt.Sprintf("build %d refusing", build))
		want := "cd " + options.Root + " && " + filepath.Join(buildDir, "build-2", "fake") + ` --flag 'it'\''s quoted'`
		if refused.Status != "exit status 65" || len(refused.Diagnostics) != 1 || refused.Diagnostics[0].Code != "dev_durable_incompatible" || refused.Resume != want {
			t.Fatalf("build %d: %+v\nwant resume %s\n%s", build, refused.DevEvent, want, session.dump())
		}
	}
	time.Sleep(time.Second)
	var kept []string
	entries, err := os.ReadDir(buildDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		kept = append(kept, entry.Name())
	}
	sort.Strings(kept)
	// Build 9 is current; 4–8 are the five most recent earlier ones; 2 is
	// the newest that did not refuse.
	if want := "[build-2 build-4 build-5 build-6 build-7 build-8 build-9]"; fmt.Sprint(kept) != want {
		t.Fatalf("kept %v, want %s", kept, want)
	}
	events := session.stop()
	if restarts := eventsOf(events, EventRestartScheduled, 0); len(restarts) != 0 {
		t.Fatalf("a refusal was restarted: %+v", restarts)
	}
	if _, err := os.Stat(buildDir); !os.IsNotExist(err) {
		t.Fatalf("the build directory outlived the session: %v", err)
	}
	assertNoProcesses(t, events)
}

// stoppingLoop is a devLoop that records its events, for driving stopApp
// directly.
func stoppingLoop(t *testing.T) (*devLoop, *[]DevEvent) {
	t.Helper()
	var events []DevEvent
	options := DevOptions{StopGrace: 5 * time.Second, Emit: func(event DevEvent) error {
		events = append(events, event)
		return nil
	}}
	options.defaults()
	loop := &devLoop{options: options, ctx: context.Background(), backoff: options.Backoff}
	t.Cleanup(loop.cancelRestart)
	return loop, &events
}

// exitingApp is a script that exits with status 3.
func exitingApp(t *testing.T) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "app")
	if err := os.WriteFile(name, []byte("#!/bin/sh\nexit 3\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return name
}

// assertExitedNotStopped: an application that had exited before stopApp
// asked it to is reported as an exit (app-exited, dev_app_exited), never as
// a stop, and is not restarted: stopApp is replacing it.
func assertExitedNotStopped(t *testing.T, loop *devLoop, events []DevEvent) {
	t.Helper()
	if len(events) != 1 || events[0].Event != EventAppExited || events[0].Status != "exit status 3" || len(events[0].Diagnostics) != 1 || events[0].Diagnostics[0].Code != "dev_app_exited" {
		t.Fatalf("events %+v, want one app-exited (exit status 3, dev_app_exited)", events)
	}
	if loop.app != nil || loop.restart != nil {
		t.Fatalf("after stopApp: app %v, restart scheduled %v", loop.app != nil, loop.restart != nil)
	}
}

// TestStopAppAfterTheApplicationExited (review R round 2): stopApp's two
// already-exited paths. The exit was seen before stopApp asked (the exited
// channel is closed), or the process was reaped but its exited channel not
// yet closed, so SIGTERM finds no process (os.ErrProcessDone).
func TestStopAppAfterTheApplicationExited(t *testing.T) {
	t.Run("seen before the stop", func(t *testing.T) {
		loop, events := stoppingLoop(t)
		app, err := startApp(exitingApp(t), t.TempDir(), "", loop.options, 1, 1)
		if err != nil {
			t.Fatal(err)
		}
		<-app.exited
		loop.app = app
		loop.stopApp()
		assertExitedNotStopped(t, loop, *events)
	})
	t.Run("reaped while the stop asks", func(t *testing.T) {
		loop, events := stoppingLoop(t)
		command := exec.Command(exitingApp(t))
		isolate(command)
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		guard, err := startGuard(nil, "")
		if err != nil {
			t.Fatal(err)
		}
		_ = command.Wait()
		app := &appProcess{cmd: command, guard: guard, build: 1, generation: 1, started: time.Now(), exited: make(chan struct{}), tail: &outputTail{}}
		time.AfterFunc(100*time.Millisecond, func() { close(app.exited) })
		loop.app = app
		loop.stopApp()
		assertExitedNotStopped(t, loop, *events)
	})
}
