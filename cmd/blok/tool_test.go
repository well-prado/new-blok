package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/internal/scaffold"
	"github.com/well-prado/new-blok/internal/tooling/devtool"
)

var updateCLI = flag.Bool("update", false, "rewrite testdata/tooling/golden CLI files")

var (
	baseMu    sync.Mutex
	baseDirs  = map[string]string{}
	binary    string
	binaryErr error
	buildOnce sync.Once
	cleanups  []string
)

func TestMain(m *testing.M) {
	flag.Parse()
	code := m.Run()
	for _, dir := range cleanups {
		_ = os.RemoveAll(dir)
	}
	os.Exit(code)
}

// shopProject returns a fresh copy of a real scaffolded application,
// tidied once per layout per test binary.
func shopProject(t *testing.T, layout string) string {
	t.Helper()
	if testing.Short() {
		t.Skip("creates and type-checks real applications")
	}
	baseMu.Lock()
	base, ok := baseDirs[layout]
	if !ok {
		parent, err := os.MkdirTemp("", "blok-cli-"+layout+"-")
		if err != nil {
			baseMu.Unlock()
			t.Fatal(err)
		}
		cleanups = append(cleanups, parent)
		base = filepath.Join(parent, "shop")
		if _, err := scaffold.Create(scaffold.Options{Directory: base, Module: "example.com/shop", Name: "shop", Layout: layout, Framework: scaffold.Framework{Dir: repoRoot(t)}}); err != nil {
			baseMu.Unlock()
			t.Fatal(err)
		}
		tidy := exec.Command("go", "mod", "tidy")
		tidy.Dir = base
		if output, err := tidy.CombinedOutput(); err != nil {
			baseMu.Unlock()
			t.Fatalf("go mod tidy: %v\n%s", err, output)
		}
		baseDirs[layout] = base
	}
	baseMu.Unlock()
	dir := filepath.Join(t.TempDir(), "shop")
	err := filepath.WalkDir(base, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(base, name)
		if entry.IsDir() {
			return os.MkdirAll(filepath.Join(dir, relative), 0o755)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, relative), data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// blokBinary builds this command once, for tests that need a real process.
func blokBinary(t *testing.T) string {
	t.Helper()
	buildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "blok-bin-")
		if err != nil {
			binaryErr = err
			return
		}
		cleanups = append(cleanups, dir)
		binary = filepath.Join(dir, "blok")
		if runtime.GOOS == "windows" {
			binary += ".exe"
		}
		build := exec.Command("go", "build", "-o", binary, ".")
		if output, err := build.CombinedOutput(); err != nil {
			binaryErr = errors.New(string(output))
		}
	})
	if binaryErr != nil {
		t.Fatal(binaryErr)
	}
	return binary
}

type streams struct {
	stdout, stderr string
	exit           int
}

func runBlok(t *testing.T, dir string, env []string, args ...string) streams {
	t.Helper()
	command := exec.Command(blokBinary(t), args...)
	command.Dir = dir
	command.Env = append(os.Environ(), env...)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	err := command.Run()
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		t.Fatal(err)
	}
	return streams{stdout: stdout.String(), stderr: stderr.String(), exit: command.ProcessState.ExitCode()}
}

func goldenText(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join(repoRoot(t), "testdata", "tooling", "golden", name)
	if *updateCLI {
		if err := os.WriteFile(path, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("%s differs:\n%s", name, got)
	}
}

// TestToolUsageErrors: bad arguments exit 2, print one line on stderr and
// write no report.
func TestToolUsageErrors(t *testing.T) {
	for _, test := range []struct {
		args   []string
		stderr string
	}{
		{[]string{"check", "--bogus"}, "blok check: flag provided but not defined: -bogus; run blok check --help\n"},
		{[]string{"check", "a", "b"}, "blok check: expected at most one project directory; run blok check --help\n"},
		{[]string{"test", "--run"}, "blok test: flag needs an argument: -run; run blok test --help\n"},
		{[]string{"inspect", "--fields", "nodes,secrets"}, "blok inspect: unknown inspect field \"secrets\"; choose from nodes,workflows,triggers,descriptions,sources,tests,examples,schemas; run blok inspect --help\n"},
	} {
		t.Run(strings.Join(test.args, " "), func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := execute(test.args, &stdout, &stderr, strings.NewReader("")); code != devtool.ExitUsage {
				t.Fatalf("exit=%d", code)
			}
			if stdout.Len() != 0 || stderr.String() != test.stderr {
				t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
	for _, command := range []string{"check", "test", "inspect"} {
		var stdout, stderr bytes.Buffer
		if code := execute([]string{command, "--help"}, &stdout, &stderr, strings.NewReader("")); code != 0 || !strings.HasPrefix(stdout.String(), "Usage: blok "+command) || stderr.Len() != 0 {
			t.Fatalf("%s --help: exit=%d stdout=%q stderr=%q", command, code, stdout.String(), stderr.String())
		}
	}
}

// TestToolStreamsAndExitCodes pins stdout, stderr and the exit code of real
// blok processes, and requires the CLI's JSON to be byte-for-byte the API's
// report for the same project.
func TestToolStreamsAndExitCodes(t *testing.T) {
	passing := shopProject(t, "classic")
	broken := shopProject(t, "classic")
	quotes := filepath.Join(broken, "workflows", "quotes", "quotes.go")
	data, err := os.ReadFile(quotes)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(quotes, bytes.Replace(data, []byte("TotalCents()"), []byte("TotalCentz()"), 1), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		golden string
		dir    string
		args   []string
		exit   int
	}{
		{"cli-check-passed.txt", passing, []string{"check"}, 0},
		{"cli-check-failed.txt", broken, []string{"check"}, 1},
		{"cli-test-failed.txt", broken, []string{"test"}, 1},
		{"cli-inspect.txt", passing, []string{"inspect"}, 0},
	} {
		t.Run(test.golden, func(t *testing.T) {
			got := runBlok(t, test.dir, nil, test.args...)
			if got.exit != test.exit || got.stderr != "" {
				t.Fatalf("exit=%d stderr=%q stdout=%s", got.exit, got.stderr, got.stdout)
			}
			goldenText(t, test.golden, got.stdout)
		})
	}
	for _, test := range []struct {
		dir     string
		command string
		api     func(string) devtool.Report
	}{
		{passing, "check", func(dir string) devtool.Report {
			return devtool.Check(context.Background(), devtool.Options{Root: dir})
		}},
		{broken, "check", func(dir string) devtool.Report {
			return devtool.Check(context.Background(), devtool.Options{Root: dir})
		}},
		{broken, "test", func(dir string) devtool.Report {
			return devtool.Test(context.Background(), devtool.TestOptions{Options: devtool.Options{Root: dir}})
		}},
		{passing, "inspect", func(dir string) devtool.Report {
			return devtool.Inspect(context.Background(), devtool.InspectOptions{Options: devtool.Options{Root: dir}})
		}},
	} {
		got := runBlok(t, test.dir, nil, test.command, "--json")
		var want bytes.Buffer
		report := test.api(test.dir)
		if err := devtool.WriteJSON(&want, report); err != nil {
			t.Fatal(err)
		}
		if got.stdout != want.String() || got.exit != report.ExitCode || got.stderr != "" {
			t.Fatalf("%s --json: exit=%d (api %d) stderr=%q\ncli=%s\napi=%s", test.command, got.exit, report.ExitCode, got.stderr, got.stdout, want.String())
		}
	}
	// The broken project's CLI JSON is also the API golden.
	got := runBlok(t, broken, nil, "check", "--json")
	want, err := os.ReadFile(filepath.Join(repoRoot(t), "testdata", "tooling", "golden", "check-compile-error.classic.json"))
	if err != nil || got.stdout != string(want) {
		t.Fatalf("CLI JSON differs from the API golden: err=%v\n%s", err, got.stdout)
	}
}

// TestToolWithoutGoExitsThree: with no go command on PATH, check reports a
// tool error (exit 3), not a project failure and not success.
func TestToolWithoutGoExitsThree(t *testing.T) {
	dir := shopProject(t, "unified")
	got := runBlok(t, dir, []string{"PATH=" + t.TempDir()}, "check", "--json")
	if got.exit != devtool.ExitTool || got.stderr != "" || !strings.Contains(got.stdout, `"status": "error"`) || !strings.Contains(got.stdout, `"code": "go_toolchain_unavailable"`) {
		t.Fatalf("exit=%d stderr=%q stdout=%s", got.exit, got.stderr, got.stdout)
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }

// TestToolWriteFailureExitsFour: a report that cannot be written exits 4
// and says so on stderr, in the API and in a real process whose standard
// output refuses writes.
func TestToolWriteFailureExitsFour(t *testing.T) {
	dir := t.TempDir()
	var stderr bytes.Buffer
	if code := execute([]string{"inspect", "--json", dir}, brokenWriter{}, &stderr, strings.NewReader("")); code != devtool.ExitOutput {
		t.Fatalf("exit=%d", code)
	}
	if stderr.String() != "blok inspect: write output: broken pipe\n" {
		t.Fatalf("stderr=%q", stderr.String())
	}

	readOnly, err := os.Create(filepath.Join(t.TempDir(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	readOnly.Close()
	readOnly, err = os.Open(readOnly.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	command := exec.Command(blokBinary(t), "inspect", "--json", dir)
	var processErr bytes.Buffer
	command.Stdout, command.Stderr = readOnly, &processErr
	_ = command.Run()
	if code := command.ProcessState.ExitCode(); code != devtool.ExitOutput || !strings.HasPrefix(processErr.String(), "blok inspect: write output: ") {
		t.Fatalf("exit=%d stderr=%q", code, processErr.String())
	}
}
