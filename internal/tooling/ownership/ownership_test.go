package ownership

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// fixtureDir holds the synthetic import-graph fixtures (testdata/imports).
var fixtureDir = filepath.Join("..", "..", "..", "testdata", "imports")

// A fixture is a txtar-style archive: a description, then "-- path --"
// sections. Two sections are directives, not project files:
//
//	@expect  {"nodes": [[dir, status], …], "diagnostics": [[code, source], …],
//	          "units": {dir: count}} or {"layout": [[code, source], …]} when
//	         discovery itself fails; units, when given, pins how many units
//	         each node's analysis read, so a pass is never over an empty graph
//	@links   one "link -> target" per line, created as symbolic links
type fixture struct {
	name     string
	files    map[string]string
	links    [][2]string
	expected expectation
}

type expectation struct {
	Nodes       [][2]string    `json:"nodes"`
	Diagnostics [][2]string    `json:"diagnostics"`
	Layout      [][2]string    `json:"layout"`
	Units       map[string]int `json:"units"`
}

func loadFixture(t *testing.T, file string) fixture {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	f := fixture{name: strings.TrimSuffix(filepath.Base(file), ".txtar"), files: map[string]string{}}
	var current string
	var body strings.Builder
	flush := func() {
		switch content := body.String(); {
		case current == "":
		case current == "@expect":
			decoder := json.NewDecoder(strings.NewReader(content))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&f.expected); err != nil {
				t.Fatalf("%s: @expect: %v", file, err)
			}
		case current == "@links":
			for _, line := range strings.Split(strings.TrimSpace(content), "\n") {
				link, target, ok := strings.Cut(line, " -> ")
				if !ok {
					t.Fatalf("%s: bad link line %q", file, line)
				}
				f.links = append(f.links, [2]string{strings.TrimSpace(link), strings.TrimSpace(target)})
			}
		default:
			f.files[current] = content
		}
		body.Reset()
	}
	for _, line := range strings.SplitAfter(string(data), "\n") {
		trimmed := strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(trimmed, "-- ") && strings.HasSuffix(trimmed, " --") && len(trimmed) > 6 {
			flush()
			current = strings.TrimSuffix(strings.TrimPrefix(trimmed, "-- "), " --")
			continue
		}
		if current != "" {
			body.WriteString(line)
		}
	}
	flush()
	return f
}

func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		target := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func materialize(t *testing.T, f fixture) string {
	t.Helper()
	root := filepath.Join(t.TempDir(), "project")
	writeFiles(t, root, f.files)
	for _, link := range f.links {
		if runtime.GOOS == "windows" {
			t.Skip("symbolic link fixtures are not exercised on Windows (ADR 0025 limits)")
		}
		target := filepath.Join(root, filepath.FromSlash(link[0]))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(filepath.FromSlash(link[1]), target); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

type outcome struct {
	nodes       [][2]string
	diagnostics [][2]string
	layout      [][2]string
	report      *Report
}

func check(t *testing.T, root string) outcome {
	t.Helper()
	report, err := CheckDir(root)
	if err != nil {
		var layoutErr *layout.Error
		if !errors.As(err, &layoutErr) {
			t.Fatalf("check failed outside the diagnostic contract: %v", err)
		}
		var got [][2]string
		for _, d := range layoutErr.Diagnostics {
			got = append(got, [2]string{d.Code, d.Source})
		}
		return outcome{layout: got}
	}
	var out outcome
	out.report = report
	for _, node := range report.Nodes {
		out.nodes = append(out.nodes, [2]string{node.Dir, node.Status})
	}
	for _, d := range report.Diagnostics {
		if err := d.Validate(); err != nil {
			t.Fatalf("diagnostic violates the contract: %v (%+v)", err, d)
		}
		if Class(d.Code) == "" {
			t.Fatalf("diagnostic code %s has no class", d.Code)
		}
		if strings.Contains(d.Source+d.Message+d.Actual+d.Expected+d.Field, root) || strings.HasPrefix(d.Source, "..") {
			t.Fatalf("diagnostic leaks a machine path or leaves the root: %+v", d)
		}
		out.diagnostics = append(out.diagnostics, [2]string{d.Code, d.Source})
	}
	if report.Verified() != (len(out.diagnostics) == 0) {
		t.Fatalf("Verified()=%v with %d diagnostics", report.Verified(), len(out.diagnostics))
	}
	if (report.Err() == nil) != report.Verified() {
		t.Fatalf("Err()=%v disagrees with Verified()=%v", report.Err(), report.Verified())
	}
	return out
}

func fixtureFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(fixtureDir, "*.txtar"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatal("no fixtures")
	}
	return files
}

type counts struct {
	Runtime       string `json:"runtime"`
	OutputRecords int    `json:"outputRecords"`
	ErrorRecords  int    `json:"errorRecords"`
	EffectRecords int    `json:"effectRecords"`
}

type provenance struct {
	Source      string            `json:"source"`
	License     string            `json:"license"`
	PrivateData bool              `json:"privateData"`
	Expected    map[string]counts `json:"expected"`
}

func readProvenance(t *testing.T) provenance {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixtureDir, "provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p provenance
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&p); err != nil {
		t.Fatal(err)
	}
	if p.PrivateData || p.License == "" {
		t.Fatal("provenance must declare synthetic, licensed fixtures")
	}
	return p
}

// TestFixtures runs every fixture against its predeclared outcome: each
// node's status and the exact sorted [code, source] diagnostics, plus the
// output/error/effect counts declared in provenance.json.
func TestFixtures(t *testing.T) {
	p := readProvenance(t)
	seen := map[string]bool{}
	for _, file := range fixtureFiles(t) {
		f := loadFixture(t, file)
		seen[f.name] = true
		t.Run(f.name, func(t *testing.T) {
			got := check(t, materialize(t, f))
			want := f.expected
			if want.Layout != nil {
				if !slices.Equal(got.layout, want.Layout) {
					t.Fatalf("layout diagnostics=%v\nwant               %v", got.layout, want.Layout)
				}
			} else {
				if got.layout != nil {
					t.Fatalf("discovery failed: %v", got.layout)
				}
				if !slices.Equal(got.nodes, want.Nodes) {
					t.Fatalf("nodes=%v\nwant  %v", got.nodes, want.Nodes)
				}
				if !slices.Equal(got.diagnostics, want.Diagnostics) {
					t.Fatalf("diagnostics=%v\nwant        %v", got.diagnostics, want.Diagnostics)
				}
				for _, node := range got.report.Nodes {
					if units, pinned := want.Units[node.Dir]; pinned && units != node.Units {
						t.Errorf("node %s read %d units; want %d", node.Dir, node.Units, units)
					}
				}
			}
			declared, ok := p.Expected[f.name]
			if !ok {
				t.Fatalf("provenance.json has no expected counts for %s", f.name)
			}
			actual := declared
			actual.OutputRecords = len(got.nodes)
			actual.ErrorRecords = len(got.diagnostics) + len(got.layout)
			actual.EffectRecords = 0 // the check has no effect path; see TestCheckNeverExecutesSource
			if actual != declared {
				t.Fatalf("counts=%+v; provenance declares %+v", actual, declared)
			}
		})
	}
	for name := range p.Expected {
		if !seen[name] {
			t.Errorf("provenance.json declares %s, which has no fixture", name)
		}
	}
}

// TestCheckIsDeterministic: the same project yields the same report bytes.
func TestCheckIsDeterministic(t *testing.T) {
	f := loadFixture(t, filepath.Join(fixtureDir, "node-transitive.txtar"))
	var first []byte
	for range 3 {
		report, err := CheckDir(materialize(t, f))
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		if first == nil {
			first = encoded
		} else if string(first) != string(encoded) {
			t.Fatalf("report changed between runs:\n%s\n%s", first, encoded)
		}
	}
}

// TestDiagnosticCodesAreClassified: every code has a class and a
// remediation, and the classes partition violations from unverified.
func TestDiagnosticCodesAreClassified(t *testing.T) {
	for code, class := range classes {
		if class != ClassViolation && class != ClassUnverified {
			t.Errorf("%s has class %q", code, class)
		}
		if remediations[code] == "" {
			t.Errorf("%s has no remediation", code)
		}
	}
	for _, code := range []string{CodeDynamicImport, CodeUnsupportedForm, CodeImportUnresolved} {
		if Class(code) == ClassViolation {
			t.Errorf("%s must leave a node unverified, not claim a verdict", code)
		}
	}
}
