package devtool

import (
	"context"
	"encoding/json"
	"errors"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/observe/redact"
)

// Bounds on what blok test retains from go test.
const (
	// MaxTestOutputLines bounds the output kept per test or package.
	MaxTestOutputLines = 64
	// MaxTests bounds the tests one report lists.
	MaxTests = 10000
)

// Test outcomes.
const (
	TestPass = "pass"
	TestFail = "fail"
	TestSkip = "skip"
	// TestIncomplete is a test that started and had not finished when go
	// test stopped, as after an interrupt.
	TestIncomplete = "incomplete"
	// PackageBuildFailed is a package whose test binary did not build.
	PackageBuildFailed = "build-failed"
	// PackageNoTests is a package without test files.
	PackageNoTests = "no-tests"
)

// TestOptions select which tests blok test runs.
type TestOptions struct {
	Options
	// Run is go test's -run pattern; empty runs every test.
	Run string
	// Race runs the tests with the race detector.
	Race bool
}

// TestResult is the outcome of go test, without timings, so the same run
// produces the same document.
type TestResult struct {
	Packages   []PackageResult `json:"packages"`
	Passed     int             `json:"passed"`
	Failed     int             `json:"failed"`
	Skipped    int             `json:"skipped"`
	Incomplete int             `json:"incomplete"`
}

type PackageResult struct {
	ImportPath string       `json:"importPath"`
	Dir        string       `json:"dir,omitempty"`
	Status     string       `json:"status"`
	Tests      []TestRecord `json:"tests,omitempty"`
}

type TestRecord struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	// Source is the first file:line the test's failure output names.
	Source string `json:"source,omitempty"`
	// Output is the test's own output, bounded and redacted, kept only for
	// a test that did not pass.
	Output []string `json:"output,omitempty"`
}

// testEvent is one test2json event.
type testEvent struct {
	Action      string `json:"Action"`
	Package     string `json:"Package"`
	ImportPath  string `json:"ImportPath"`
	Test        string `json:"Test"`
	Output      string `json:"Output"`
	OutputType  string `json:"OutputType"`
	FailedBuild string `json:"FailedBuild"`
}

// testLocation matches the file:line prefix testing adds to t.Log/t.Error.
var testLocation = regexp.MustCompile(`^\s+([^\s:]+\.go):(\d+): (.*)$`)

// Test runs the project's tests with go test -json and reports every
// package, test and failure. It never substitutes its own execution for go
// test. A context cancellation interrupts go test's process group; the
// report then holds every result go test produced before it stopped.
func Test(ctx context.Context, options TestOptions) (report Report) {
	report = Report{Command: "test"}
	found := &diagnostics{}
	toolFailure := false
	defer func() { finish(&report, found, ctx, toolFailure) }()

	workspace, problems, err := options.source().Load(ctx, options.Root)
	report.Project = workspace.project()
	if err != nil {
		if ctx.Err() == nil {
			found.add(diagnostic.Diagnostic{Code: "project_unreadable", Actual: errorText(err), Expected: "a readable project directory", Remediation: "run blok from a readable application directory", Message: "the project could not be read"})
		}
		return report
	}
	for _, problem := range problems {
		found.add(problem)
	}
	if workspace.Module == "" {
		return report
	}
	args := []string{"test", "-json"}
	if options.Race {
		args = append(args, "-race")
	}
	if options.Run != "" {
		args = append(args, "-run", options.Run)
	}
	args = append(args, "./...")

	collector := newTestCollector(workspace)
	toolchain := &goDiagnostics{root: workspace.Root}
	var stderr []string
	run, err := runGo(ctx, goCommand{
		binary: options.goBinary(), dir: workspace.Root, args: args, env: options.Env,
		stdout: func(line []byte) {
			options.observe("stdout", line)
			collector.line(line)
		},
		stderr: func(line []byte) {
			options.observe("stderr", line)
			text := string(line)
			toolchain.line(text)
			if strings.TrimSpace(text) != "" && len(stderr) < MaxTestOutputLines {
				stderr = append(stderr, strings.TrimSpace(relativeText(text, workspace.Root)))
			}
		},
	})
	if errors.Is(err, errToolUnavailable) {
		found.add(toolUnavailable(options.goBinary(), err))
		toolFailure = true
		return report
	}
	if err != nil && ctx.Err() == nil {
		found.add(toolchainError(err.Error()))
	}
	result, failures := collector.result(ctx)
	failures = dedupe(append(failures, toolchain.result()...))
	found.truncated = found.truncated || collector.truncated
	report.Tests = &result
	for _, failure := range failures {
		found.add(failure)
	}
	if ctx.Err() == nil && run.exitCode != 0 && len(failures) == 0 {
		text := "go test exited with status " + strconv.Itoa(run.exitCode)
		if len(stderr) > 0 {
			text = strings.Join(redactLines(stderr), "; ")
		}
		found.add(toolchainError(text))
	}
	if ctx.Err() == nil && run.exitCode == 0 && result.Passed+result.Failed+result.Skipped == 0 {
		found.add(diagnostic.Diagnostic{Code: "no_tests_ran", Expected: "at least one test", Actual: "0 tests", Remediation: "add a test for the application, or widen the -run pattern", Message: "go test ran no tests, so nothing was verified"})
	}
	return report
}

type testState struct {
	status string
	output []string
	source string
}

type packageState struct {
	status string
	dir    string
	tests  map[string]*testState
	output []string
}

// testCollector folds go test's event stream. It is fed from one goroutine.
type testCollector struct {
	workspace Workspace
	packages  map[string]*packageState
	build     *goDiagnostics
	tests     int
	truncated bool
}

func newTestCollector(workspace Workspace) *testCollector {
	return &testCollector{workspace: workspace, packages: map[string]*packageState{}, build: &goDiagnostics{root: workspace.Root}}
}

func (c *testCollector) pkg(importPath string) *packageState {
	state := c.packages[importPath]
	if state == nil {
		dir := ""
		if importPath == c.workspace.Module {
			dir = "."
		} else if rest, ok := strings.CutPrefix(importPath, c.workspace.Module+"/"); ok {
			dir = rest
		}
		state = &packageState{dir: dir, tests: map[string]*testState{}}
		c.packages[importPath] = state
	}
	return state
}

func (c *testCollector) line(line []byte) {
	var event testEvent
	if json.Unmarshal(line, &event) != nil {
		return
	}
	switch event.Action {
	case "build-output":
		c.build.line(event.Output)
		return
	case "build-fail":
		return
	}
	if event.Package == "" {
		return
	}
	state := c.pkg(event.Package)
	if event.Test == "" {
		switch event.Action {
		case "output":
			if strings.Contains(event.Output, "[no test files]") {
				state.status = PackageNoTests
			}
			state.output = appendBounded(state.output, event.Output, c.workspace.Root)
		case "pass", "fail", "skip":
			if state.status == PackageNoTests {
				return
			}
			state.status = event.Action
			if event.FailedBuild != "" {
				state.status = PackageBuildFailed
			}
		}
		return
	}
	test := state.tests[event.Test]
	if test == nil {
		if c.tests >= MaxTests {
			c.truncated = true
			return
		}
		c.tests++
		test = &testState{status: TestIncomplete}
		state.tests[event.Test] = test
	}
	switch event.Action {
	case "pass", "fail", "skip":
		test.status = event.Action
	case "output":
		if event.OutputType == "frame" {
			return
		}
		if test.source == "" {
			if match := testLocation.FindStringSubmatch(strings.TrimRight(event.Output, "\n")); match != nil {
				test.source = path.Join(state.dir, match[1]) + ":" + match[2]
			}
		}
		test.output = appendBounded(test.output, event.Output, c.workspace.Root)
	}
}

// appendBounded keeps raw output; it is redacted as a whole by result.
func appendBounded(lines []string, output, root string) []string {
	text := strings.TrimRight(output, "\n")
	if strings.TrimSpace(text) == "" || len(lines) >= MaxTestOutputLines {
		return lines
	}
	return append(lines, relativeText(text, root))
}

// result builds the sorted result and one diagnostic per failure: a compile
// error, a failing leaf test, or a package that failed outside any test.
func (c *testCollector) result(ctx context.Context) (TestResult, []diagnostic.Diagnostic) {
	var result TestResult
	failures := dedupe(c.build.result())
	paths := make([]string, 0, len(c.packages))
	for importPath := range c.packages {
		paths = append(paths, importPath)
	}
	sort.Strings(paths)
	for _, importPath := range paths {
		state := c.packages[importPath]
		status := state.status
		if status == "" {
			status = TestIncomplete
		}
		item := PackageResult{ImportPath: redact.String(importPath), Dir: redact.String(state.dir), Status: status}
		names := make([]string, 0, len(state.tests))
		for name := range state.tests {
			names = append(names, name)
		}
		sort.Strings(names)
		failedTests := 0
		for _, name := range names {
			test := state.tests[name]
			output := redactLines(test.output)
			record := TestRecord{Name: redact.String(name), Status: test.status, Source: redact.String(test.source)}
			if test.status != TestPass {
				record.Output = output
			}
			item.Tests = append(item.Tests, record)
			switch test.status {
			case TestPass:
				result.Passed++
			case TestFail:
				result.Failed++
				failedTests++
				if !failedChild(state.tests, name) {
					failures = append(failures, testFailure(item.ImportPath, record.Name, record.Source, output))
				}
			case TestSkip:
				result.Skipped++
			default:
				result.Incomplete++
			}
		}
		if status == TestFail && failedTests == 0 && ctx.Err() == nil {
			failures = append(failures, diagnostic.Diagnostic{Code: "test_package_failed", Source: item.Dir, Expected: "the package's tests to finish", Actual: lastLines(redactLines(state.output), 3), Remediation: "fix what stopped the test binary (a panic, TestMain exit or timeout) and rerun blok test", Message: "package " + item.ImportPath + " failed outside any test"})
		}
		result.Packages = append(result.Packages, item)
	}
	return result, failures
}

func failedChild(tests map[string]*testState, name string) bool {
	for other, test := range tests {
		if test.status == TestFail && strings.HasPrefix(other, name+"/") {
			return true
		}
	}
	return false
}

// testFailure describes a failed leaf test from its already redacted name,
// source and output.
func testFailure(importPath, name, source string, output []string) diagnostic.Diagnostic {
	// The first located message, with the continuation lines testing
	// indents beneath it.
	actual, located := "", false
	for _, line := range output {
		match := testLocation.FindStringSubmatch(line)
		if located && match != nil {
			break
		}
		if match != nil {
			actual, located = match[3], true
		} else if located {
			actual += " " + strings.TrimSpace(line)
		}
	}
	if actual == "" {
		actual = lastLines(output, 1)
	}
	return diagnostic.Diagnostic{Code: "test_failed", Source: source, Expected: "pass", Actual: actual, Remediation: "fix the failing assertion or the code under test, then rerun blok test", Message: "test " + name + " in " + importPath + " failed"}
}

func lastLines(lines []string, count int) string {
	if len(lines) > count {
		lines = lines[len(lines)-count:]
	}
	trimmed := make([]string, len(lines))
	for index, line := range lines {
		trimmed[index] = strings.TrimSpace(line)
	}
	return strings.Join(trimmed, "; ")
}
