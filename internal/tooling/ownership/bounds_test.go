package ownership

import (
	"fmt"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/diagnostic"
	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// TestBoundsLeaveNodesUnverified: a directory listing past the entry bound
// and a file past the size bound are reported, and the node that reaches
// them is unverified, never verified.
func TestBoundsLeaveNodesUnverified(t *testing.T) {
	files := map[string]string{
		"blok.json":                `{"name":"shop","module":"example.com/shop","runtime":"go","layout":"unified","triggers":["http"],"types":""}`,
		"go.mod":                   "module example.com/shop\n",
		"nodes/nodejs/a/node.json": `{"name":"shop/a","version":"1.0.0","runtime":"nodejs"}`,
		"nodes/nodejs/a/index.ts":  "import { x } from \"../../../wide/x.js\";\nimport { y } from \"../../../big.js\";\nexport const a = [x, y];\n",
		"big.js":                   "// " + strings.Repeat("x", layout.MaxFileBytes) + "\n",
		"nodes/nodejs/b/node.json": `{"name":"shop/b","version":"1.0.0","runtime":"nodejs"}`,
		"nodes/nodejs/b/index.ts":  "export const b = 1;\n",
	}
	for i := range layout.MaxDirEntries + 1 {
		files[fmt.Sprintf("wide/f%05d.js", i)] = ""
	}
	root := filepath.Join(t.TempDir(), "project")
	writeFiles(t, root, files)
	got := check(t, root)
	want := [][2]string{{CodeLimitExceeded, "big.js"}, {CodeLimitExceeded, "nodes/nodejs/a/index.ts:1"}}
	if !slices.Equal(got.diagnostics, want) {
		t.Fatalf("diagnostics=%v; want %v", got.diagnostics, want)
	}
	if !slices.Equal(got.nodes, [][2]string{{"nodes/nodejs/a", StatusUnverified}, {"nodes/nodejs/b", StatusVerified}}) {
		t.Fatalf("nodes=%v", got.nodes)
	}
}

// measure runs CheckDir and returns the report, wall time and bytes
// allocated.
func measure(t *testing.T, root string) (*Report, time.Duration, uint64) {
	t.Helper()
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	start := time.Now()
	report, err := CheckDir(root)
	elapsed := time.Since(start)
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatal(err)
	}
	return report, elapsed, after.TotalAlloc - before.TotalAlloc
}

func status(report *Report, dir string) string {
	for _, node := range report.Nodes {
		if node.Dir == dir {
			return node.Status
		}
	}
	return ""
}

// TestResolutionTimeIsBounded (Review R, PR #314): a package.json whose
// exports array repeats one target 100,000 times (~0.9 MiB), required 4,000
// times from one file and once from each of 50 directories, and a diamond
// of tsconfig extends (9 levels of 8 configs, each extending all 8 of the
// next), were 2m55s/119 GB and 8.3 s/2.6 GB. Resolution is now cached per
// directory and specifier, exports matches per package and key, and each
// tsconfig is loaded once. The time bound is a generous smoke check for
// -race on a shared CPU; the allocation bound is the real assertion.
func TestResolutionTimeIsBounded(t *testing.T) {
	base := map[string]string{
		"blok.json":                `{"name":"shop","module":"example.com/shop","runtime":"go","layout":"unified","triggers":["http"],"types":""}`,
		"go.mod":                   "module example.com/shop\n",
		"nodes/nodejs/a/node.json": `{"name":"shop/a","version":"1.0.0","runtime":"nodejs"}`,
		"nodes/nodejs/b/node.json": `{"name":"shop/b","version":"1.0.0","runtime":"nodejs"}`,
		"nodes/nodejs/b/index.js":  "module.exports = 1;\n",
	}
	bomb := map[string]string{
		"package.json":               `{"workspaces":["packages/*"]}`,
		"packages/bomb/x.js":         "module.exports = 1;\n",
		"packages/bomb/package.json": `{"name":"bomb","exports":{".":[` + strings.TrimSuffix(strings.Repeat(`"./x.js",`, 100000), ",") + `]}}`,
		"nodes/nodejs/a/index.js":    strings.Repeat("require(\"bomb\");\n", 4000),
	}
	for k, v := range base {
		bomb[k] = v
	}
	for i := range 50 {
		bomb[fmt.Sprintf("nodes/nodejs/a/d%02d/index.js", i)] = "require(\"bomb\");\n"
	}
	root := filepath.Join(t.TempDir(), "bomb")
	writeFiles(t, root, bomb)
	report, elapsed, allocated := measure(t, root)
	if status(report, "nodes/nodejs/a") != StatusVerified || elapsed > 30*time.Second || allocated > 1<<30 {
		t.Fatalf("exports bomb: status=%s elapsed=%v allocated=%d MiB diags=%v", status(report, "nodes/nodejs/a"), elapsed, allocated>>20, report.Diagnostics)
	}

	diamond := map[string]string{"nodes/nodejs/a/index.ts": "import x from \"lodash\";\nexport default x;\n", "nodes/nodejs/a/tsconfig.json": `{"extends":"../../../cfg/c0_0.json"}`}
	for k, v := range base {
		diamond[k] = v
	}
	const width = 8
	for level := range 9 {
		for i := range width {
			var bases []string
			for j := 0; level < 8 && j < width; j++ {
				bases = append(bases, fmt.Sprintf(`"./c%d_%d.json"`, level+1, j))
			}
			diamond[fmt.Sprintf("cfg/c%d_%d.json", level, i)] = `{"extends":[` + strings.Join(bases, ",") + `]}`
		}
	}
	root = filepath.Join(t.TempDir(), "diamond")
	writeFiles(t, root, diamond)
	report, elapsed, allocated = measure(t, root)
	// Nine levels exceed the extends depth bound: the node fails closed.
	if status(report, "nodes/nodejs/a") != StatusUnverified || elapsed > 30*time.Second || allocated > 256<<20 {
		t.Fatalf("extends diamond: status=%s elapsed=%v allocated=%d MiB", status(report, "nodes/nodejs/a"), elapsed, allocated>>20)
	}
}

// TestWorkBudgetFailsClosed: resolution work is counted in path elements
// looked up, not wall-clock time; past the budget every lookup is a bound
// overrun and the node is unverified, never verified.
func TestWorkBudgetFailsClosed(t *testing.T) {
	saved := maxWork
	t.Cleanup(func() { maxWork = saved })
	files := map[string]string{
		"blok.json":                `{"name":"shop","module":"example.com/shop","runtime":"go","layout":"unified","triggers":["http"],"types":""}`,
		"go.mod":                   "module example.com/shop\n",
		"nodes/nodejs/a/node.json": `{"name":"shop/a","version":"1.0.0","runtime":"nodejs"}`,
		"nodes/nodejs/a/index.js":  "require(\"./one.js\");\nrequire(\"./two.js\");\n",
		"nodes/nodejs/a/one.js":    "module.exports = 1;\n",
		"nodes/nodejs/a/two.js":    "module.exports = 2;\n",
	}
	root := filepath.Join(t.TempDir(), "project")
	writeFiles(t, root, files)
	maxWork = 1 << 30
	if report, err := CheckDir(root); err != nil || !report.Verified() {
		t.Fatalf("control: %v %+v", err, report)
	}
	maxWork = 40
	report, err := CheckDir(root)
	if err != nil {
		t.Fatal(err)
	}
	if status(report, "nodes/nodejs/a") != StatusUnverified || !slices.ContainsFunc(report.Diagnostics, func(d diagnostic.Diagnostic) bool { return d.Code == CodeLimitExceeded }) {
		t.Fatalf("an exhausted work budget must leave the node unverified: %+v", report)
	}
}
