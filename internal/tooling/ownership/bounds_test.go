package ownership

import (
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"

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
