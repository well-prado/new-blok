package layout

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

// A case is a txtar-style archive: a description, then "-- path --"
// sections. Sections named @expect, @links and @outside/<path> are
// directives, not project files:
//
//	@expect          {"catalog": …, "digest": …} or {"diagnostics": [[code, source], …]}
//	@links           one "link -> target" per line; $OUTSIDE is a directory
//	                 outside the project root, $PARENT the root's resolved
//	                 parent and $ROOT the resolved root
//	@outside/<path>  a file written under $OUTSIDE
type fixtureCase struct {
	name     string
	files    map[string]string
	outside  map[string]string
	links    [][2]string
	expected expectation
}

type expectation struct {
	Catalog     json.RawMessage `json:"catalog"`
	Digest      string          `json:"digest"`
	Diagnostics [][2]string     `json:"diagnostics"`
}

func loadCase(t *testing.T, file string) fixtureCase {
	t.Helper()
	data, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	c := fixtureCase{name: strings.TrimSuffix(filepath.Base(file), ".txtar"), files: map[string]string{}, outside: map[string]string{}}
	var current string
	var body strings.Builder
	flush := func() {
		if current == "" {
			return
		}
		content := body.String()
		switch {
		case current == "@expect":
			if err := json.Unmarshal([]byte(content), &c.expected); err != nil {
				t.Fatalf("%s: @expect: %v", file, err)
			}
		case current == "@links":
			for _, line := range strings.Split(strings.TrimSpace(content), "\n") {
				link, target, ok := strings.Cut(line, " -> ")
				if !ok {
					t.Fatalf("%s: bad link line %q", file, line)
				}
				c.links = append(c.links, [2]string{strings.TrimSpace(link), strings.TrimSpace(target)})
			}
		case strings.HasPrefix(current, "@outside/"):
			c.outside[strings.TrimPrefix(current, "@outside/")] = content
		default:
			c.files[current] = content
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
	return c
}

// materialize writes the case to a fresh project root and returns it.
func materialize(t *testing.T, c fixtureCase) string {
	t.Helper()
	base := t.TempDir()
	root, outside := filepath.Join(base, "project"), filepath.Join(base, "outside")
	write := func(dir string, files map[string]string) {
		for rel, content := range files {
			target := filepath.Join(dir, filepath.FromSlash(rel))
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(target, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	write(root, c.files)
	write(outside, c.outside)
	for _, link := range c.links {
		path := filepath.Join(root, filepath.FromSlash(link[0]))
		parent, err := filepath.EvalSymlinks(base)
		if err != nil {
			t.Fatal(err)
		}
		resolvedRoot := filepath.Join(parent, "project")
		target := strings.NewReplacer("$OUTSIDE", filepath.ToSlash(outside), "$PARENT", filepath.ToSlash(parent), "$ROOT", filepath.ToSlash(resolvedRoot)).Replace(link[1])
		target = filepath.FromSlash(target)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, path); err != nil {
			if runtime.GOOS == "windows" {
				t.Skipf("symlinks unavailable: %v", err)
			}
			t.Fatal(err)
		}
	}
	return root
}

type outcome struct {
	catalog     []byte
	digest      string
	diagnostics [][2]string
}

func discoverCase(t *testing.T, root string) outcome {
	t.Helper()
	project, err := Discover(root)
	if err != nil {
		var layoutErr *Error
		if !errors.As(err, &layoutErr) {
			t.Fatalf("discovery failed outside the diagnostic contract: %v", err)
		}
		var got [][2]string
		for _, d := range layoutErr.Diagnostics {
			if err := d.Validate(); err != nil {
				t.Fatalf("diagnostic violates the contract: %v (%+v)", err, d)
			}
			if strings.Contains(d.Source+d.Message+d.Actual+d.Expected, root) {
				t.Fatalf("diagnostic leaks a machine path: %+v", d)
			}
			got = append(got, [2]string{d.Code, d.Source})
		}
		return outcome{diagnostics: got}
	}
	catalog := project.Catalog()
	canonical, err := catalog.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := catalog.Digest()
	if err != nil {
		t.Fatal(err)
	}
	return outcome{catalog: canonical, digest: digest}
}

func caseFiles(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("testdata", "cases", "*.txtar"))
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(files)
	if len(files) == 0 {
		t.Fatal("no fixtures")
	}
	return files
}

// TestSyntheticCases runs every fixture against its predeclared outcome:
// the exact catalog and digest, or the exact sorted diagnostics.
func TestSyntheticCases(t *testing.T) {
	provenance := readProvenance(t)
	for _, file := range caseFiles(t) {
		c := loadCase(t, file)
		t.Run(c.name, func(t *testing.T) {
			got := discoverCase(t, materialize(t, c))
			want := c.expected
			switch {
			case want.Diagnostics != nil:
				if got.diagnostics == nil {
					t.Fatalf("discovery succeeded with %s; want diagnostics %v", got.catalog, want.Diagnostics)
				}
				if !equalPairs(got.diagnostics, want.Diagnostics) {
					t.Fatalf("diagnostics=%v\nwant        %v", got.diagnostics, want.Diagnostics)
				}
			default:
				if got.diagnostics != nil {
					t.Fatalf("diagnostics=%v; want catalog %s", got.diagnostics, want.Catalog)
				}
				var compact bytes.Buffer
				if err := json.Compact(&compact, want.Catalog); err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(got.catalog, compact.Bytes()) {
					t.Fatalf("catalog=%s\nwant    %s", got.catalog, compact.Bytes())
				}
				if got.digest != want.Digest {
					t.Fatalf("digest=%s; want %s", got.digest, want.Digest)
				}
			}
			counts, ok := provenance.Expected[c.name]
			if !ok {
				t.Fatalf("testdata/provenance.json has no expected counts for %s", c.name)
			}
			var catalog Catalog
			if got.catalog != nil {
				if err := json.Unmarshal(got.catalog, &catalog); err != nil {
					t.Fatal(err)
				}
			}
			actual := counts
			actual.OutputRecords = len(catalog.Nodes) + len(catalog.Workflows)
			actual.ErrorRecords = len(got.diagnostics)
			actual.EffectRecords = 0 // discovery has no effect path; see TestDiscoveryNeverExecutesSource
			if actual != counts {
				t.Fatalf("counts=%+v; provenance declares %+v", actual, counts)
			}
		})
	}
}

func equalPairs(a, b [][2]string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

type counts struct {
	OutputRecords int `json:"outputRecords"`
	ErrorRecords  int `json:"errorRecords"`
	EffectRecords int `json:"effectRecords"`
}

type provenanceFile struct {
	Source      string            `json:"source"`
	License     string            `json:"license"`
	PrivateData bool              `json:"privateData"`
	Expected    map[string]counts `json:"expected"`
}

func readProvenance(t *testing.T) provenanceFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	var p provenanceFile
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatal(err)
	}
	if p.Source == "" || p.License != "Apache-2.0" || p.PrivateData {
		t.Fatalf("invalid fixture provenance: %+v", p)
	}
	return p
}

// TestFixtureInventoryIsComplete keeps provenance.json and the cases in
// step: a case without declared counts, or counts without a case, fails.
func TestFixtureInventoryIsComplete(t *testing.T) {
	provenance := readProvenance(t)
	seen := map[string]bool{}
	for _, file := range caseFiles(t) {
		seen[strings.TrimSuffix(filepath.Base(file), ".txtar")] = true
	}
	for name := range provenance.Expected {
		if !seen[name] {
			t.Errorf("provenance.json declares %s, which has no case", name)
		}
	}
	if len(seen) != len(provenance.Expected) {
		t.Errorf("%d cases, %d declared", len(seen), len(provenance.Expected))
	}
}

// TestLayoutsProduceTheSameCatalog: each layout pair is one application
// written in both layouts, with different directory names. Their canonical
// catalogs, and so their digests, are byte-identical.
func TestLayoutsProduceTheSameCatalog(t *testing.T) {
	for _, pair := range [][2]string{{"classic-shop", "unified-shop"}} {
		var outcomes [2]outcome
		for i, name := range pair {
			outcomes[i] = discoverCase(t, materialize(t, loadCase(t, filepath.Join("testdata", "cases", name+".txtar"))))
			if outcomes[i].diagnostics != nil {
				t.Fatalf("%s: %v", name, outcomes[i].diagnostics)
			}
		}
		if !bytes.Equal(outcomes[0].catalog, outcomes[1].catalog) || outcomes[0].digest != outcomes[1].digest {
			t.Fatalf("%s and %s differ:\n%s %s\n%s %s", pair[0], pair[1], outcomes[0].catalog, outcomes[0].digest, outcomes[1].catalog, outcomes[1].digest)
		}
	}
}
