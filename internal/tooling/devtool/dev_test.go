package devtool

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/layout"
	"github.com/well-prado/new-blok/observe/redact"
)

// TestWatchedFiles pins which project files restart the application.
func TestWatchedFiles(t *testing.T) {
	for name, want := range map[string]bool{
		"blok.json": true, "go.mod": true, "go.sum": true,
		"internal/app/app.go": true, "cmd/shop/main.go": true, "workflows/quotes/quotes.go": true,
		"runtimes/go/nodes/quote/quote.go": true, "nodes/go/quote/types.go": true,
		// A foreign node's descriptor and sources belong to its node.
		"runtimes/node/nodes/slow/node.json": true, "nodes/node/slow/index.mjs": true,
		// Go test files are not built into the application, in a node or
		// anywhere else.
		"runtimes/go/nodes/quote/quote_test.go": false, "internal/app/app_test.go": false,
		"README.md": false, "internal/app/notes.txt": false, "blok.json.bak": false,
		// "nodes" deeper than the root is an ordinary directory.
		"internal/nodes/x/y/readme.md": false,
	} {
		if got := watchedFile(name); got != want {
			t.Errorf("watchedFile(%q) = %v, want %v", name, got, want)
		}
	}
}

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		target := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// TestScanStaysInsideTheProject: skipped directories and nested modules are
// not read, a linked directory is not entered, and a linked source file is
// recorded as a link, never stat-ed through.
func TestScanStaysInsideTheProject(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	writeFiles(t, root, map[string]string{
		"go.mod": "module example.com/x\n", "blok.json": "{}", "main.go": "package main\n",
		"vendor/v/v.go": "package v\n", "testdata/t.go": "package t\n", ".hidden/h.go": "package h\n",
		"_skip/s.go": "package s\n", "web/node_modules/m/m.go": "package m\n",
		"nested/go.mod": "module example.com/nested\n", "nested/n.go": "package n\n",
	})
	writeFiles(t, outside, map[string]string{"far.go": "package far\n", "dir/inner.go": "package inner\n"})
	if runtime.GOOS != "windows" {
		if err := os.Symlink(filepath.Join(outside, "far.go"), filepath.Join(root, "linked.go")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.Join(outside, "dir"), filepath.Join(root, "linkdir")); err != nil {
			t.Fatal(err)
		}
	}
	scan, problem, err := scanProject(context.Background(), root)
	if err != nil || problem != nil {
		t.Fatal(err, problem)
	}
	want := []string{"blok.json", "go.mod", "main.go"}
	if runtime.GOOS != "windows" {
		want = append(want, "linked.go")
	}
	var got []string
	for name := range scan {
		got = append(got, name)
	}
	if fmt.Sprint(sortedCopy(got)) != fmt.Sprint(sortedCopy(want)) {
		t.Fatalf("watched %v, want %v", sortedCopy(got), want)
	}
	if runtime.GOOS != "windows" {
		links := scan.links()
		if len(links) != 1 || links[0].Code != "dev_symlink_unwatched" || links[0].Source != "linked.go" {
			t.Fatalf("links %+v", links)
		}
		// Editing the file the link points to is not a change.
		before := scan
		writeFiles(t, outside, map[string]string{"far.go": "package far\n\nconst changed = 1\n"})
		after, _, _ := scanProject(context.Background(), root)
		if changed := changes(before, after); len(changed) != 0 {
			t.Fatalf("an edit outside the root was seen: %v", changed)
		}
	}
}

func sortedCopy(values []string) []string {
	result := append([]string(nil), values...)
	for i := range result {
		for j := i + 1; j < len(result); j++ {
			if result[j] < result[i] {
				result[i], result[j] = result[j], result[i]
			}
		}
	}
	return result
}

// TestScanIsBounded: past either bound a scan fails with
// layout_limit_exceeded instead of reading on.
func TestScanIsBounded(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{}
	for index := range 6 {
		files[fmt.Sprintf("p%d/f.go", index)] = "package p\n"
	}
	writeFiles(t, root, files)
	saved := watchBounds
	defer func() { watchBounds = saved }()
	for _, bounds := range []struct{ files, entries int }{{5, 1000}, {1000, 8}} {
		watchBounds = bounds
		scan, problem, err := scanProject(context.Background(), root)
		if err != nil || problem == nil || problem.Code != layout.CodeLimitExceeded || scan != nil {
			t.Fatalf("bounds %+v: scan=%v problem=%+v err=%v", bounds, scan, problem, err)
		}
	}
	watchBounds = saved
	if _, problem, err := scanProject(context.Background(), root); err != nil || problem != nil {
		t.Fatal(err, problem)
	}
}

// TestChangesSeeEditsAdditionsDeletionsAndReplacements.
func TestChangesSeeEditsAdditionsDeletionsAndReplacements(t *testing.T) {
	root := t.TempDir()
	writeFiles(t, root, map[string]string{"go.mod": "module x\n", "a.go": "package a\n", "b.go": "package a\n", "c.go": "package a\n"})
	before, _, _ := scanProject(context.Background(), root)
	time.Sleep(10 * time.Millisecond)
	writeFiles(t, root, map[string]string{"a.go": "package a // edited\n", "d.go": "package a\n"})
	if err := os.Remove(filepath.Join(root, "b.go")); err != nil {
		t.Fatal(err)
	}
	// An editor's save: write a new file and rename it over the old one.
	writeFiles(t, root, map[string]string{".c.go.swp": "package a // saved\n"})
	if err := os.Rename(filepath.Join(root, ".c.go.swp"), filepath.Join(root, "c.go")); err != nil {
		t.Fatal(err)
	}
	after, _, _ := scanProject(context.Background(), root)
	if got := changes(before, after); fmt.Sprint(got) != "[a.go b.go c.go d.go]" {
		t.Fatalf("changes %v", got)
	}
	if got := changes(after, after); len(got) != 0 {
		t.Fatalf("no-op changes %v", got)
	}
}

// TestBackoffDoublesToItsBoundAndResets: restarts wait Backoff, doubling to
// MaxBackoff; a run that lasted StableAfter starts the sequence again.
func TestBackoffDoublesToItsBoundAndResets(t *testing.T) {
	var delays []int64
	options := DevOptions{Backoff: 100 * time.Millisecond, MaxBackoff: 800 * time.Millisecond, StableAfter: time.Second, Emit: func(event DevEvent) error {
		delays = append(delays, event.DelayMS)
		return nil
	}}
	options.defaults()
	loop := &devLoop{options: options, backoff: options.Backoff}
	for range 6 {
		loop.scheduleRestart(0)
	}
	loop.scheduleRestart(time.Second)
	loop.scheduleRestart(0)
	loop.cancelRestart()
	if fmt.Sprint(delays) != "[100 200 400 800 800 800 100 200]" {
		t.Fatalf("delays %v", delays)
	}
}

// TestDevDefaultsAreBounded: every zero duration takes a positive default,
// and MaxBackoff never falls below Backoff.
func TestDevDefaultsAreBounded(t *testing.T) {
	options := DevOptions{Backoff: time.Minute}
	options.defaults()
	for name, value := range map[string]time.Duration{"poll": options.Poll, "quiet": options.Quiet, "max wait": options.MaxWait, "grace": options.StopGrace, "stable": options.StableAfter} {
		if value <= 0 {
			t.Errorf("%s default %s", name, value)
		}
	}
	if options.MaxBackoff != time.Minute {
		t.Errorf("max backoff %s below backoff", options.MaxBackoff)
	}
	if DefaultStopGrace <= InterruptGrace || DefaultMaxBackoff < DefaultBackoff || DefaultMaxWait < DefaultQuiet {
		t.Error("inconsistent defaults")
	}
}

// TestOutputTailIsBoundedAndRedacted: the tail keeps the last
// MaxOutputTail lines, each cut at maxTailLineBytes, and passes the
// redaction boundary as a block.
func TestOutputTailIsBoundedAndRedacted(t *testing.T) {
	tail := &outputTail{}
	for index := range MaxOutputTail + 5 {
		fmt.Fprintf(tail, "line %d\n", index)
	}
	fmt.Fprint(tail, strings.Repeat("x", 3*maxTailLineBytes))
	lines := tail.lines()
	if len(lines) != MaxOutputTail || lines[0] != "line 6" || len(lines[MaxOutputTail-1]) != maxTailLineBytes {
		t.Fatalf("tail %d lines, first %q", len(lines), lines[0])
	}
	secret := &outputTail{}
	fmt.Fprint(secret, "starting\nAuthorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789\n")
	joined := strings.Join(secret.lines(), "\n")
	if strings.Contains(joined, "abcdefghij") || !strings.Contains(joined, redact.MessageMarker) {
		t.Fatalf("credential not redacted: %q", joined)
	}
}

// TestWithEnvReplacesBlokSettings: the caller cannot choose the generation.
func TestWithEnvReplacesBlokSettings(t *testing.T) {
	env := withEnv([]string{"A=1", EnvDevGeneration + "=99", "blok_dev=0"}, EnvDev+"=1", EnvDevGeneration+"=3")
	if fmt.Sprint(env) != "[A=1 BLOK_DEV=1 BLOK_DEV_GENERATION=3]" {
		t.Fatalf("env %v", env)
	}
}

// TestMainPackageMustBeInsideTheRoot.
func TestMainPackageMustBeInsideTheRoot(t *testing.T) {
	root := t.TempDir()
	writeFiles(t, root, map[string]string{"cmd/app/main.go": "package main\n", "file.go": "package x\n"})
	loop := &devLoop{root: root}
	for requested, want := range map[string]string{
		"./cmd/app": "", "cmd/app": "", "": "dev_main_package_missing", "../x": "dev_main_package_missing",
		"/abs": "dev_main_package_missing", "./cmd/../cmd/app": "dev_main_package_missing", "./cmd/none": "dev_main_package_missing",
		"./file.go": "dev_main_package_missing",
	} {
		loop.options.Package = requested
		_, problem := loop.mainPackage(Workspace{})
		got := ""
		if problem != nil {
			got = problem.Code
		}
		if got != want {
			t.Errorf("package %q: %q, want %q", requested, got, want)
		}
	}
}
