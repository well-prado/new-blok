package devtool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/generate"
	"github.com/well-prado/new-blok/internal/scaffold"
)

var update = flag.Bool("update", false, "rewrite testdata/tooling/golden")

const fixtureModule = "example.com/shop"

type fixtureFile struct {
	Cases []fixtureCase `json:"cases"`
}

type fixtureCase struct {
	ID       string        `json:"id"`
	Command  string        `json:"command"`
	Fields   string        `json:"fields"`
	Layouts  []string      `json:"layouts"`
	Golden   bool          `json:"golden"`
	Edits    []fixtureEdit `json:"edits"`
	Expected struct {
		ExitCode      int    `json:"exitCode"`
		Status        Status `json:"status"`
		OutputRecords int    `json:"outputRecords"`
		ErrorRecords  int    `json:"errorRecords"`
		EffectRecords int    `json:"effectRecords"`
		Redacted      *int   `json:"redacted"`
		Unresolved    []struct {
			Kind   string `json:"kind"`
			Source string `json:"source"`
		} `json:"unresolved"`
		Diagnostics []struct {
			Code         string `json:"code"`
			Source       string `json:"source"`
			SourcePrefix string `json:"sourcePrefix"`
			Step         string `json:"step"`
			Field        string `json:"field"`
		} `json:"diagnostics"`
	} `json:"expected"`
}

type fixtureEdit struct {
	File    string    `json:"file"`
	Replace [2]string `json:"replace"`
	Append  string    `json:"append"`
	Write   *string   `json:"write"`
	Delete  bool      `json:"delete"`
}

func repoRoot(t testing.TB) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}

// placeholders are the layout-dependent paths a fixture names.
func placeholders(layout string) *strings.Replacer {
	node := scaffold.NodeDir(layout, "quote")
	return strings.NewReplacer("{module}", fixtureModule, "{node}", node, "{nodes}", path.Dir(node))
}

var (
	baseOnce sync.Map // layout -> *baseProject
)

type baseProject struct {
	once sync.Once
	dir  string
	err  error
}

// scaffolded returns a fresh copy of a real application created by the
// scaffold for layout, tidied once per test binary.
func scaffolded(t *testing.T, layout string) string {
	t.Helper()
	if testing.Short() {
		t.Skip("creates and type-checks real applications")
	}
	value, _ := baseOnce.LoadOrStore(layout, &baseProject{})
	base := value.(*baseProject)
	base.once.Do(func() {
		dir, err := os.MkdirTemp("", "blok-devtool-"+layout+"-")
		if err != nil {
			base.err = err
			return
		}
		base.dir = filepath.Join(dir, "shop")
		if _, err := scaffold.Create(scaffold.Options{Directory: base.dir, Module: fixtureModule, Name: "shop", Layout: layout, Framework: scaffold.Framework{Dir: repoRoot(t)}}); err != nil {
			base.err = err
			return
		}
		tidy := exec.Command("go", "mod", "tidy")
		tidy.Dir = base.dir
		if output, err := tidy.CombinedOutput(); err != nil {
			base.err = fmt.Errorf("go mod tidy: %v\n%s", err, output)
		}
	})
	if base.err != nil {
		t.Fatal(base.err)
	}
	dir := filepath.Join(t.TempDir(), "shop")
	copyTree(t, base.dir, dir)
	return dir
}

func TestMain(m *testing.M) {
	flag.Parse()
	code := m.Run()
	baseOnce.Range(func(_, value any) bool {
		if base := value.(*baseProject); base.dir != "" {
			_ = os.RemoveAll(filepath.Dir(base.dir))
		}
		return true
	})
	os.Exit(code)
}

func copyTree(t *testing.T, from, to string) {
	t.Helper()
	err := filepath.WalkDir(from, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(from, name)
		target := filepath.Join(to, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func applyEdits(t *testing.T, dir string, edits []fixtureEdit, names *strings.Replacer) {
	t.Helper()
	for _, edit := range edits {
		name := filepath.Join(dir, filepath.FromSlash(names.Replace(edit.File)))
		switch {
		case edit.Delete:
			if err := os.Remove(name); err != nil {
				t.Fatal(err)
			}
		case edit.Write != nil:
			if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, []byte(names.Replace(*edit.Write)), 0o644); err != nil {
				t.Fatal(err)
			}
		default:
			data, err := os.ReadFile(name)
			if err != nil {
				t.Fatal(err)
			}
			text := string(data)
			if edit.Replace[0] != "" {
				if !strings.Contains(text, edit.Replace[0]) {
					t.Fatalf("edit %s: %q not found", edit.File, edit.Replace[0])
				}
				text = strings.Replace(text, edit.Replace[0], edit.Replace[1], 1)
			}
			text += edit.Append
			if err := os.WriteFile(name, []byte(text), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// treeDigest maps every project file to its content digest.
func treeDigest(t *testing.T, dir string) map[string][32]byte {
	t.Helper()
	tree := map[string][32]byte{}
	err := filepath.WalkDir(dir, func(name string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() {
			return err
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(dir, name)
		tree[filepath.ToSlash(relative)] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return tree
}

func effects(before, after map[string][32]byte) int {
	changed := 0
	for name, digest := range after {
		if previous, ok := before[name]; !ok || previous != digest {
			changed++
		}
	}
	for name := range before {
		if _, ok := after[name]; !ok {
			changed++
		}
	}
	return changed
}

func runCommand(ctx context.Context, command, dir, fields string) Report {
	options := Options{Root: dir}
	switch command {
	case "check":
		return Check(ctx, options)
	case "test":
		return Test(ctx, TestOptions{Options: options})
	default:
		selected, err := ParseFields(fields)
		if err != nil {
			panic(err)
		}
		return Inspect(ctx, InspectOptions{Options: options, Fields: selected})
	}
}

func outputRecords(report Report) int {
	switch {
	case report.Catalog != nil:
		return len(report.Catalog.Nodes) + len(report.Catalog.Workflows) + len(report.Catalog.Triggers)
	case report.Tests != nil:
		return report.Tests.Passed
	}
	passed := 0
	for _, check := range report.Checks {
		if check.State == CheckPassed {
			passed++
		}
	}
	return passed
}

func loadFixtures(t *testing.T) []fixtureCase {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repoRoot(t), "testdata", "tooling", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file fixtureFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	return file.Cases
}

// TestFixtures runs every predeclared case against a real scaffolded
// application in each listed layout and compares the report with its
// declared exit code, status, diagnostics and record counts. Golden cases
// also run twice and must produce byte-identical JSON matching the golden.
func TestFixtures(t *testing.T) {
	for _, item := range loadFixtures(t) {
		for _, layout := range item.Layouts {
			t.Run(item.ID+"/"+layout, func(t *testing.T) {
				t.Parallel()
				names := placeholders(layout)
				dir := scaffolded(t, layout)
				applyEdits(t, dir, item.Edits, names)
				before := treeDigest(t, dir)
				report := runCommand(context.Background(), item.Command, dir, item.Fields)
				encoded := encode(t, report)
				if got := effects(before, treeDigest(t, dir)); got != item.Expected.EffectRecords {
					t.Errorf("effects=%d want %d", got, item.Expected.EffectRecords)
				}
				if report.Version != Version || report.Command != item.Command {
					t.Errorf("version=%q command=%q", report.Version, report.Command)
				}
				if report.ExitCode != item.Expected.ExitCode || report.Status != item.Expected.Status {
					t.Errorf("exit=%d status=%s want %d %s\n%s", report.ExitCode, report.Status, item.Expected.ExitCode, item.Expected.Status, encoded)
				}
				if got := len(report.Diagnostics); got != item.Expected.ErrorRecords || got != len(item.Expected.Diagnostics) {
					t.Errorf("diagnostics=%d want %d\n%s", got, item.Expected.ErrorRecords, encoded)
				}
				if got := outputRecords(report); got != item.Expected.OutputRecords {
					t.Errorf("output records=%d want %d\n%s", got, item.Expected.OutputRecords, encoded)
				}
				for index, want := range item.Expected.Diagnostics {
					if index >= len(report.Diagnostics) {
						break
					}
					got := report.Diagnostics[index]
					if err := got.Validate(); err != nil {
						t.Errorf("diagnostic %d breaks the #28 contract: %v", index, err)
					}
					if got.Code != want.Code ||
						(want.Source != "" && got.Source != names.Replace(want.Source)) ||
						(want.SourcePrefix != "" && !strings.HasPrefix(got.Source, names.Replace(want.SourcePrefix))) ||
						(want.Step != "" && got.Step != want.Step) || (want.Field != "" && got.Field != want.Field) {
						t.Errorf("diagnostic %d = %+v, want %+v", index, got, want)
					}
				}
				if item.Expected.Redacted != nil && (report.Catalog == nil || report.Catalog.Redacted != *item.Expected.Redacted) {
					t.Errorf("redacted mismatch, want %d\n%s", *item.Expected.Redacted, encoded)
				}
				for _, want := range item.Expected.Unresolved {
					found := false
					for _, got := range report.Catalog.Unresolved {
						found = found || (got.Kind == want.Kind && got.Source == names.Replace(want.Source))
					}
					if !found {
						t.Errorf("unresolved %+v missing\n%s", want, encoded)
					}
				}
				if !item.Golden {
					return
				}
				again := encode(t, runCommand(context.Background(), item.Command, dir, item.Fields))
				if !bytes.Equal(encoded, again) {
					t.Errorf("two runs differ:\n%s\n---\n%s", encoded, again)
				}
				golden := filepath.Join(repoRoot(t), "testdata", "tooling", "golden", item.ID+"."+layout+".json")
				if *update {
					if err := os.WriteFile(golden, encoded, 0o644); err != nil {
						t.Fatal(err)
					}
				}
				want, err := os.ReadFile(golden)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(encoded, want) {
					t.Errorf("report differs from %s:\n%s", golden, encoded)
				}
			})
		}
	}
}

func encode(t *testing.T, report Report) []byte {
	t.Helper()
	var out bytes.Buffer
	if err := WriteJSON(&out, report); err != nil {
		t.Fatal(err)
	}
	return out.Bytes()
}

// TestCheckAndTestAgree: a compile error is the same diagnostic whether
// blok check's go vet or blok test's go test found it, and a duplicate step
// id check reports statically is rejected by flow.Define when the
// application's own test builds the workflow.
func TestCheckAndTestAgree(t *testing.T) {
	dir := scaffolded(t, "classic")
	applyEdits(t, dir, []fixtureEdit{{File: "workflows/quotes/quotes.go", Replace: [2]string{"TotalCents()", "TotalCentz()"}}}, placeholders("classic"))
	checked, tested := Check(context.Background(), Options{Root: dir}), Test(context.Background(), TestOptions{Options: Options{Root: dir}})
	if len(checked.Diagnostics) != 1 || len(tested.Diagnostics) != 1 || checked.Diagnostics[0] != tested.Diagnostics[0] {
		t.Fatalf("check=%+v\ntest=%+v", checked.Diagnostics, tested.Diagnostics)
	}

	dir = scaffolded(t, "classic")
	applyEdits(t, dir, []fixtureEdit{{File: "workflows/quotes/quotes.go", Replace: [2]string{"\t\treturn quote.OutputFields(result).TotalCents()", "\t\t_ = flow.Call(builder, \"calculate\", calculate, input)\n\t\treturn quote.OutputFields(result).TotalCents()"}}}, placeholders("classic"))
	checked, tested = Check(context.Background(), Options{Root: dir}), Test(context.Background(), TestOptions{Options: Options{Root: dir}})
	if len(checked.Diagnostics) != 1 || checked.Diagnostics[0].Code != "workflow_step_id_duplicate" {
		t.Fatalf("check=%+v", checked.Diagnostics)
	}
	if len(tested.Diagnostics) != 1 || tested.Diagnostics[0].Code != "test_failed" || !strings.Contains(tested.Diagnostics[0].Actual, "flow: duplicate instruction id calculate") {
		t.Fatalf("test=%+v", tested.Diagnostics)
	}
}

// TestRepairWithOneDiagnostic: each broken application carries exactly one
// diagnostic, and doing what it says — and nothing else — makes check pass.
func TestRepairWithOneDiagnostic(t *testing.T) {
	for _, layout := range []string{"classic", "unified"} {
		t.Run(layout, func(t *testing.T) {
			t.Parallel()
			names := placeholders(layout)
			dir := scaffolded(t, layout)
			// Stale bindings: the remediation is a command.
			applyEdits(t, dir, []fixtureEdit{{File: "{node}/types.go", Append: "\n// Extra is new.\ntype Extra struct {\n\tNote string `json:\"note\"`\n}\n"}}, names)
			report := Check(context.Background(), Options{Root: dir})
			if len(report.Diagnostics) != 1 || report.Diagnostics[0].Remediation != "run blok generate" {
				t.Fatalf("diagnostics=%+v", report.Diagnostics)
			}
			bindings := report.Diagnostics[0].Source
			types, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(names.Replace("{node}/types.go"))))
			if err != nil {
				t.Fatal(err)
			}
			generated, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(bindings)))
			if err != nil {
				t.Fatal(err)
			}
			if regenerated := regenerate(t, types); bytes.Equal(regenerated, generated) {
				t.Fatal("the stale bindings already match")
			} else if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(bindings)), regenerated, 0o644); err != nil {
				t.Fatal(err)
			}
			if report := Check(context.Background(), Options{Root: dir}); report.ExitCode != ExitOK {
				t.Fatalf("after blok generate: %+v", report.Diagnostics)
			}

			// A compile error: the diagnostic's position and actual text
			// locate the one token to change.
			applyEdits(t, dir, []fixtureEdit{{File: "workflows/quotes/quotes.go", Replace: [2]string{"TotalCents()", "TotalCentz()"}}}, names)
			report = Check(context.Background(), Options{Root: dir})
			if len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != "go_compile_error" {
				t.Fatalf("diagnostics=%+v", report.Diagnostics)
			}
			repairAt(t, dir, report.Diagnostics[0], "TotalCentz", "TotalCents")
			if report := Check(context.Background(), Options{Root: dir}); report.ExitCode != ExitOK {
				t.Fatalf("after repair: %+v", report.Diagnostics)
			}

			// A forbidden node import: the diagnostic names the import line.
			applyEdits(t, dir, []fixtureEdit{{File: "{nodes}/tax/tax.go", Write: ptr("package tax\n\nimport \"{module}/{node}\"\n\nfunc Rate(input quote.Input) int { return input.Quantity }\n")}}, names)
			report = Check(context.Background(), Options{Root: dir})
			if len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != "node_import_forbidden" {
				t.Fatalf("diagnostics=%+v", report.Diagnostics)
			}
			file, _, _ := strings.Cut(report.Diagnostics[0].Source, ":")
			if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(file)), []byte("package tax\n\n// Input is the tax node's own input.\ntype Input struct{ Quantity int }\n\nfunc Rate(input Input) int { return input.Quantity }\n"), 0o644); err != nil {
				t.Fatal(err)
			}
			if report := Check(context.Background(), Options{Root: dir}); report.ExitCode != ExitOK {
				t.Fatalf("after repair: %+v", report.Diagnostics)
			}
		})
	}
}

func ptr(value string) *string { return &value }

// regenerate is what blok generate writes for a types file.
func regenerate(t *testing.T, types []byte) []byte {
	t.Helper()
	generated, err := generate.Source(types, generate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	return generated
}

// repairAt replaces old with replacement on the diagnostic's line only.
func repairAt(t *testing.T, dir string, item diagnostic.Diagnostic, old, replacement string) {
	t.Helper()
	parts := strings.Split(item.Source, ":")
	line, err := strconv.Atoi(parts[1])
	if err != nil || !strings.Contains(item.Actual, old) {
		t.Fatalf("diagnostic %+v does not locate %q", item, old)
	}
	name := filepath.Join(dir, filepath.FromSlash(parts[0]))
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(string(data), "\n")
	lines[line-1] = strings.Replace(lines[line-1], old, replacement, 1)
	if err := os.WriteFile(name, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestInterruptFlushesPartialResults cancels blok test while a test is
// running: the report keeps the tests that finished, marks the running one
// incomplete, says it was interrupted, exits 130, and the test binary does
// not outlive the call.
func TestInterruptFlushesPartialResults(t *testing.T) {
	dir := scaffolded(t, "classic")
	marker := filepath.Join(t.TempDir(), "pid")
	applyEdits(t, dir, []fixtureEdit{{File: "internal/app/slow_test.go", Write: ptr("package app\n\nimport (\n\t\"os\"\n\t\"strconv\"\n\t\"testing\"\n\t\"time\"\n)\n\nfunc TestSlow(t *testing.T) {\n\t_ = os.WriteFile(" + strconv.Quote(marker) + ", []byte(strconv.Itoa(os.Getpid())), 0o644)\n\ttime.Sleep(10 * time.Minute)\n}\n")}}, placeholders("classic"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	canceled := make(chan time.Time, 1)
	go func() {
		for deadline := time.Now().Add(3 * time.Minute); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
			if _, err := os.Stat(marker); err == nil {
				canceled <- time.Now()
				cancel()
				return
			}
		}
	}()
	report := Test(ctx, TestOptions{Options: Options{Root: dir}})
	returned := time.Now()
	pidText, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("the slow test never started: %v\n%s", err, encode(t, report))
	}
	if report.Status != StatusInterrupted || report.ExitCode != ExitInterrupted {
		t.Fatalf("status=%s exit=%d", report.Status, report.ExitCode)
	}
	// The interrupt itself stops go test and its test binary; the SIGKILL
	// after InterruptGrace is only a backstop.
	if elapsed := returned.Sub(<-canceled); elapsed >= InterruptGrace {
		t.Fatalf("stopping took %s, not less than the %s grace", elapsed, InterruptGrace)
	}
	statuses := map[string]string{}
	for _, item := range report.Tests.Packages {
		for _, test := range item.Tests {
			statuses[test.Name] = test.Status
		}
	}
	if statuses["TestQuoteOverHTTP"] != TestPass || statuses["TestSlow"] != TestIncomplete {
		t.Fatalf("statuses=%v", statuses)
	}
	if last := report.Diagnostics[len(report.Diagnostics)-1]; last.Code != "interrupted" {
		t.Fatalf("diagnostics=%+v", report.Diagnostics)
	}
	pid, _ := strconv.Atoi(string(pidText))
	assertGone(t, pid)
}

// assertGone waits briefly for a process to disappear.
func assertGone(t *testing.T, pid int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if process, err := os.FindProcess(pid); err != nil || process.Signal(syscall.Signal(0)) != nil {
			return
		}
	}
	t.Fatalf("process %d outlived the interrupted command", pid)
}

// TestCanceledBeforeStartRunsNothing: a context canceled before check runs
// starts no go command and still produces an interrupted report.
func TestCanceledBeforeStartRunsNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	missing := filepath.Join(t.TempDir(), "go-must-not-run")
	report := Check(ctx, Options{Root: t.TempDir(), Go: missing})
	if report.Status != StatusInterrupted || report.ExitCode != ExitInterrupted {
		t.Fatalf("report=%+v", report)
	}
	for _, check := range report.Checks {
		if check.State == CheckPassed {
			t.Fatalf("check %s passed after cancellation", check.Name)
		}
	}
}

// TestMissingToolchainIsAToolError: when go cannot start, check and test
// exit 3 with go_toolchain_unavailable, never 0 and never a project failure.
func TestMissingToolchainIsAToolError(t *testing.T) {
	dir := scaffolded(t, "unified")
	missing := filepath.Join(t.TempDir(), "no-such-go")
	for _, report := range []Report{Check(context.Background(), Options{Root: dir, Go: missing}), Test(context.Background(), TestOptions{Options: Options{Root: dir, Go: missing}})} {
		if report.ExitCode != ExitTool || report.Status != StatusError {
			t.Fatalf("%s: exit=%d status=%s", report.Command, report.ExitCode, report.Status)
		}
		if len(report.Diagnostics) != 1 || report.Diagnostics[0].Code != "go_toolchain_unavailable" {
			t.Fatalf("%s: %+v", report.Command, report.Diagnostics)
		}
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestWritersReportFailure(t *testing.T) {
	report := Report{Command: "check", Status: StatusPassed, Version: Version, Diagnostics: []diagnostic.Diagnostic{}}
	if err := WriteJSON(failingWriter{}, report); err == nil {
		t.Fatal("WriteJSON hid a write failure")
	}
	if err := WriteHuman(failingWriter{}, report); err == nil {
		t.Fatal("WriteHuman hid a write failure")
	}
}

func TestParseFields(t *testing.T) {
	fields, err := ParseFields("")
	if err != nil || strings.Join(fields, ",") != strings.Join(DefaultFields, ",") {
		t.Fatalf("default=%v err=%v", fields, err)
	}
	for _, field := range fields {
		if field == FieldSchemas {
			t.Fatal("schemas are projected by default")
		}
	}
	if fields, err := ParseFields("schemas, nodes"); err != nil || strings.Join(fields, ",") != "nodes,schemas" {
		t.Fatalf("fields=%v err=%v", fields, err)
	}
	if _, err := ParseFields("nodes,secrets"); err == nil {
		t.Fatal("an unknown field was accepted")
	}
}

// TestFieldPolicy: an unselected field is absent, and the schema of a
// scaffolded node appears only when schemas are selected.
func TestFieldPolicy(t *testing.T) {
	dir := scaffolded(t, "classic")
	full := Inspect(context.Background(), InspectOptions{Options: Options{Root: dir}})
	if node := full.Catalog.Nodes[0]; node.OutputSchema != nil || node.Source == "" || len(node.Tests) == 0 {
		t.Fatalf("default projection=%+v", node)
	}
	narrow := Inspect(context.Background(), InspectOptions{Options: Options{Root: dir}, Fields: []string{FieldNodes, FieldSchemas}})
	node := narrow.Catalog.Nodes[0]
	if node.OutputSchema == nil || node.Source != "" || node.Tests != nil || node.Description != "" || narrow.Catalog.Workflows != nil || narrow.Catalog.Triggers != nil {
		t.Fatalf("narrow projection=%+v", narrow.Catalog)
	}
}

// TestDirectorySourceStaysInsideRoot: a symbolic link to code outside the
// project is never read.
func TestDirectorySourceStaysInsideRoot(t *testing.T) {
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "leak.go"), []byte("package leak\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "go.mod"), []byte("module example.com/confined\n\ngo 1.27.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if err := os.Symlink(filepath.Join(outside, "leak.go"), filepath.Join(root, "leak.go")); err != nil {
		t.Fatal(err)
	}
	workspace, _, err := DirectorySource{}.Load(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(workspace.Packages) != 0 {
		t.Fatalf("read outside the root: %+v", workspace.Packages)
	}
}

// TestStaticStepRules covers what the static workflow reader decides and
// what it refuses to guess.
func TestStaticStepRules(t *testing.T) {
	source := `package flows

import "github.com/well-prado/new-blok/flow"

func build(ok bool) {
	_, _ = flow.Define(flow.Spec{Name: "w", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[int]) flow.Ref[int] {
		flow.Parallel(b, "fan", func(arm *flow.ArmBuilder) { flow.ArmCall(arm, "a", n, in) }, func(arm *flow.ArmBuilder) { flow.ArmCall(arm, "a", n, in) })
		if ok {
			flow.Call(b, "maybe", n, in)
		} else {
			flow.Call(b, "maybe", n, in)
		}
		helper := func() { flow.Call(b, "hidden", n, in) }
		_ = helper
		return flow.Call(b, "out", n, in)
	})
}
`
	root := t.TempDir()
	for name, content := range map[string]string{"go.mod": "module example.com/flows\n\ngo 1.27.0\n", "flows.go": source} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	workspace, _, err := DirectorySource{}.Load(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseWorkspace(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	workflows := parsed.extract().workflows
	if len(workflows) != 1 {
		t.Fatalf("workflows=%+v", workflows)
	}
	var ids []string
	for _, step := range workflows[0].steps {
		ids = append(ids, step.id)
	}
	sort.Strings(ids)
	if strings.Join(ids, ",") != "a,a,fan,maybe,maybe,out" || len(workflows[0].unresolved) != 1 {
		t.Fatalf("ids=%v unresolved=%v", ids, workflows[0].unresolved)
	}
	problems := parsed.stepDiagnostics(workflows[0])
	// The arms of one Parallel both run, so "a" is a duplicate; the two
	// "maybe" calls sit under Go control flow and are not compared.
	if len(problems) != 1 || problems[0].Code != "workflow_step_id_duplicate" || problems[0].Step != "a" {
		t.Fatalf("problems=%+v", problems)
	}
}
