package layout

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// writeTree writes files under root.
func writeTree(t *testing.T, root string, files map[string]string) {
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

func baseProject(layout string) map[string]string {
	return map[string]string{
		"blok.json": `{"name":"shop","module":"example.com/shop","runtime":"go","layout":"` + layout + `","triggers":["http"],"types":""}`,
		"go.mod":    "module example.com/shop\n",
	}
}

func codes(t *testing.T, err error) []string {
	t.Helper()
	layoutErr, ok := err.(*Error)
	if !ok {
		t.Fatalf("err=%v; want *Error", err)
	}
	return layoutErr.Codes()
}

// TestManifestValidationIsBounded (review H1): validation work is linear.
// A blok.json at the 1 MiB file bound listing ~100k workflow paths is
// refused by the path cap with one diagnostic, before any overlap work; the
// maximum accepted list of fully nested paths yields one overlap
// diagnostic per path, not one per pair. The time bound is a smoke check
// generous enough for -race on a shared CPU; the previous quadratic check
// took 55 s for 40k entries without -race.
func TestManifestValidationIsBounded(t *testing.T) {
	var many strings.Builder
	many.WriteString(`{"name":"a","layout":"classic","workflows":[`)
	for i := 0; many.Len() < MaxFileBytes-64; i++ {
		if i > 0 {
			many.WriteByte(',')
		}
		fmt.Fprintf(&many, `"w/%d"`, i)
	}
	many.WriteString(`]}`)
	nested := make([]string, MaxWorkflowPaths)
	for i := range nested {
		nested[i] = strconv.Quote(strings.Repeat("d/", i) + "d")
	}
	deep := `{"name":"a","layout":"classic","workflows":[` + strings.Join(nested, ",") + `]}`
	for _, test := range []struct {
		name, manifest string
		diagnostics    int
	}{
		{"100k entries", many.String(), 1},
		{"nested maximum", deep, MaxWorkflowPaths - 1},
	} {
		start := time.Now()
		_, diagnostics := ParseManifest([]byte(test.manifest))
		elapsed := time.Since(start)
		if len(diagnostics) != test.diagnostics {
			t.Fatalf("%s: %d diagnostics; want %d", test.name, len(diagnostics), test.diagnostics)
		}
		if elapsed > 20*time.Second {
			t.Fatalf("%s: validation took %v", test.name, elapsed)
		}
		t.Logf("%s: %d bytes validated in %v", test.name, len(test.manifest), elapsed)
	}
	if _, diagnostics := ParseManifest([]byte(`{"name":"a","layout":"classic","workflows":["a","a-b","a/c","a"]}`)); len(diagnostics) != 2 {
		t.Fatalf("overlap with interleaved siblings: %v; want a/c inside a, and a twice", diagnostics)
	}
}

// heapPeak samples live heap objects while fn runs and returns the peak
// rise over the starting value.
func heapPeak(fn func()) uint64 {
	runtime.GC()
	sample := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
	read := func() uint64 { metrics.Read(sample); return sample[0].Value.Uint64() }
	base := read()
	var peak atomic.Uint64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			if value := read(); value > peak.Load() {
				peak.Store(value)
			}
			select {
			case <-done:
				return
			case <-time.After(time.Millisecond):
			}
		}
	})
	fn()
	close(done)
	wg.Wait()
	if p := peak.Load(); p > base {
		return p - base
	}
	return 0
}

// TestDiscoveryMemoryIsBounded (review H2): a node of sixty 1 MiB files of
// integer literals - each a large syntax tree - stops at the per-node read
// budget, and discovery never holds more than one file's syntax at a time.
// Retaining every tree reached gigabytes.
func TestDiscoveryMemoryIsBounded(t *testing.T) {
	root := t.TempDir()
	files := baseProject(Unified)
	files["nodes/go/big/node.go"] = "package big\n\nimport \"github.com/well-prado/new-blok/node\"\n\nvar Node = node.MustDefine(\"shop/big\", \"1.0.0\", nil)\n"
	body := "package big\n\nvar X = []int{" + strings.Repeat("1,", (MaxFileBytes-64)/2) + "}\n"
	for i := range 60 {
		files[fmt.Sprintf("nodes/go/big/f%02d.go", i)] = body
	}
	writeTree(t, root, files)
	var err error
	peak := heapPeak(func() { _, err = Discover(root) })
	if got := codes(t, err); !slices.Equal(got, []string{CodeLimitExceeded}) || err.(*Error).Diagnostics[0].Expected != strconv.Itoa(MaxNodeBytes) {
		t.Fatalf("diagnostics=%+v; want one per-node budget limit", err)
	}
	const bound = 256 << 20
	t.Logf("peak heap rise %d MiB (bound %d MiB)", peak>>20, bound>>20)
	if peak > bound {
		t.Fatalf("peak heap rose %d MiB; bound %d MiB", peak>>20, bound>>20)
	}
}

// TestDiscoveryReadBudget: the total read budget stops a project whose
// nodes each stay under the per-node budget.
func TestDiscoveryReadBudget(t *testing.T) {
	root := t.TempDir()
	files := baseProject(Unified)
	comment := "package n\n// " + strings.Repeat("x", MaxFileBytes-32) + "\n"
	nodes := MaxTotalBytes/(MaxNodeBytes-MaxFileBytes) + 1
	for n := range nodes {
		dir := fmt.Sprintf("nodes/go/n%d", n)
		files[dir+"/node.go"] = fmt.Sprintf("package n\n\nimport \"github.com/well-prado/new-blok/node\"\n\nvar Node = node.MustDefine(\"shop/n%d\", \"1.0.0\", nil)\n", n)
		for f := range MaxNodeBytes/MaxFileBytes - 1 {
			files[fmt.Sprintf("%s/c%d.go", dir, f)] = comment
		}
	}
	writeTree(t, root, files)
	_, err := Discover(root)
	if got := codes(t, err); !slices.Equal(got, []string{CodeLimitExceeded}) || err.(*Error).Diagnostics[0].Expected != strconv.Itoa(MaxTotalBytes) {
		t.Fatalf("diagnostics=%+v; want one total budget limit", err)
	}
}

// TestDirectoryBounds (review L5): directories count toward the entry
// bound (exercised at a reduced bound), and one directory's listing is
// bounded.
func TestDirectoryBounds(t *testing.T) {
	t.Run("listing", func(t *testing.T) {
		root := t.TempDir()
		files := baseProject(Unified)
		files["nodes/go/wide/node.go"] = "package wide\n"
		writeTree(t, root, files)
		for i := range MaxDirEntries {
			if err := os.WriteFile(filepath.Join(root, "nodes", "go", "wide", fmt.Sprintf("e%05d.txt", i)), nil, 0o644); err != nil {
				t.Fatal(err)
			}
		}
		_, err := Discover(root)
		if got := codes(t, err); !slices.Contains(got, CodeLimitExceeded) {
			t.Fatalf("codes=%v; want a listing limit", got)
		}
	})
	t.Run("directories count", func(t *testing.T) {
		root := t.TempDir()
		files := baseProject(Unified)
		files["nodes/go/deep/node.go"] = "package deep\n\nimport \"github.com/well-prado/new-blok/node\"\n\nvar Node = node.MustDefine(\"shop/deep\", \"1.0.0\", nil)\n"
		writeTree(t, root, files)
		const limit = 300
		per := limit/3 + 1
		for group := range 3 {
			for i := range per {
				if err := os.MkdirAll(filepath.Join(root, "nodes", "go", "deep", fmt.Sprintf("g%d", group), fmt.Sprintf("d%04d", i)), 0o755); err != nil {
					t.Fatal(err)
				}
			}
		}
		if _, err := discoverWith(root, limit*2); err != nil {
			t.Fatalf("under the bound: %v", err)
		}
		_, err := discoverWith(root, limit)
		if got := codes(t, err); !slices.Equal(got, []string{CodeLimitExceeded}) {
			t.Fatalf("codes=%v; want the entry limit from directories alone", got)
		}
	})
}

// TestParseErrorsEchoNoContent (review L4): a syntax error names a line,
// never the token, so a file's content cannot leak into a diagnostic.
func TestParseErrorsEchoNoContent(t *testing.T) {
	root := t.TempDir()
	files := baseProject(Unified)
	files["nodes/go/bad/bad.go"] = "package bad\n\nfunc SECRETTOKEN {\n"
	writeTree(t, root, files)
	_, err := Discover(root)
	layoutErr := err.(*Error)
	if len(layoutErr.Diagnostics) != 1 || layoutErr.Diagnostics[0].Code != CodeParseFailed || layoutErr.Diagnostics[0].Message != "Go syntax error at line 3" {
		t.Fatalf("diagnostics=%+v", layoutErr.Diagnostics)
	}
}

// TestDiscoveryNeverExecutesSource is structural evidence for "no source
// execution" and "no effects" (review L1): discovery's own code imports no
// package that starts processes or loads code, calls no process-starting
// or file-writing function by any receiver, opens nothing for writing, and
// uses no linkname. golang.org/x/sys/unix is confined to the file guard
// and to the two names it needs. Fixtures carry an init that panics; it
// never runs because nothing compiles it, and this guard keeps it that way.
func TestDiscoveryNeverExecutesSource(t *testing.T) {
	forbiddenImports := map[string]bool{"os/exec": true, "plugin": true, "go/build": true, "go/importer": true, "go/types": true, "golang.org/x/tools/go/packages": true, "syscall": true, "unsafe": true, "golang.org/x/sys/windows": true, "reflect": true}
	forbiddenNames := map[string]bool{}
	for _, name := range strings.Fields("StartProcess FindProcess Command CommandContext Exec ForkExec Syscall Syscall6 RawSyscall Create CreateTemp WriteFile Remove RemoveAll Rename Mkdir MkdirAll MkdirTemp Symlink Link Chmod Chown Lchown Chtimes Chdir Truncate Setenv Unsetenv Pipe Open Getenv LookupEnv") {
		forbiddenNames[name] = true
	}
	forbiddenFlags := map[string]bool{"O_WRONLY": true, "O_RDWR": true, "O_CREATE": true, "O_TRUNC": true, "O_APPEND": true, "O_EXCL": true}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		unixName := ""
		for _, spec := range parsed.Imports {
			value, _ := strconv.Unquote(spec.Path.Value)
			if forbiddenImports[value] {
				t.Errorf("%s imports %s; discovery must only read files", file, value)
			}
			if value == "golang.org/x/sys/unix" {
				if !strings.HasPrefix(file, "fileguard_") {
					t.Errorf("%s imports golang.org/x/sys/unix outside the file guard", file)
				}
				unixName = "unix"
			}
		}
		for _, group := range parsed.Comments {
			for _, comment := range group.List {
				if strings.HasPrefix(comment.Text, "//go:linkname") {
					t.Errorf("%s uses go:linkname", file)
				}
			}
		}
		ast.Inspect(parsed, func(n ast.Node) bool {
			selector, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			// d.root.Open is a read-only Root method; os.Open and every
			// other Open are refused so all reads go through OpenFile with
			// the guard's read-only flags.
			if forbiddenNames[selector.Sel.Name] || forbiddenFlags[selector.Sel.Name] {
				if !(selector.Sel.Name == "Open" && isRootSelector(selector)) {
					t.Errorf("%s uses %s", file, selector.Sel.Name)
				}
			}
			if ident, ok := selector.X.(*ast.Ident); ok && unixName != "" && ident.Name == unixName && selector.Sel.Name != "O_NONBLOCK" && selector.Sel.Name != "Fstat" && selector.Sel.Name != "Stat_t" {
				t.Errorf("%s uses unix.%s", file, selector.Sel.Name)
			}
			return true
		})
		checked++
	}
	if checked < 6 {
		t.Fatalf("checked %d files; the guard is not seeing the package", checked)
	}
}

// isRootSelector matches d.root.Open and opened.Open-style calls on the
// *os.Root held by the discoverer.
func isRootSelector(selector *ast.SelectorExpr) bool {
	inner, ok := selector.X.(*ast.SelectorExpr)
	return ok && inner.Sel.Name == "root"
}

// TestRefusalsNameTheirReason: the build-constrained and shadowing refusals
// say why, so the author can fix the right thing.
func TestRefusalsNameTheirReason(t *testing.T) {
	for file, want := range map[string]string{
		"descriptor-build-constraints.txtar":  "declared in a build-constrained file",
		"descriptor-scope.txtar":              "must use string literals",
		"descriptor-duplicate-constant.txtar": "is declared 2 times",
	} {
		c := loadCase(t, filepath.Join("testdata", "cases", file))
		_, err := Discover(materialize(t, c))
		layoutErr, ok := err.(*Error)
		if !ok {
			t.Fatalf("%s: %v", file, err)
		}
		found := false
		for _, d := range layoutErr.Diagnostics {
			found = found || strings.Contains(d.Message, want)
		}
		if !found {
			t.Fatalf("%s: no diagnostic says %q: %+v", file, want, layoutErr.Diagnostics)
		}
	}
}
