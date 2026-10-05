package layout

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func TestCollisionsFoldLetterCase(t *testing.T) {
	got := Collisions([]string{"nodes/go/quote/a.go", "nodes/go/Quote/a.go", "nodes/go/quote/A.go", "nodes/go/quote/b.go", "nodes/go/quote/a.go"})
	want := [][2]string{{"nodes/go/Quote/a.go", "nodes/go/quote/A.go"}, {"nodes/go/Quote/a.go", "nodes/go/quote/a.go"}}
	if !slices.Equal(got, want) {
		t.Fatalf("collisions=%v; want %v", got, want)
	}
	if got := Collisions([]string{"a", "b", "a"}); len(got) != 0 {
		t.Fatalf("a repeated identical path is not a collision: %v", got)
	}
}

// TestCaseCollisionOnDisk needs a case-sensitive file system, where both
// spellings can exist; on a case-insensitive one they cannot be created,
// which is the hazard the diagnostic prevents.
func TestCaseCollisionOnDisk(t *testing.T) {
	c := loadCase(t, filepath.Join("testdata", "cases", "duplicate-identity.txtar"))
	c.files = map[string]string{
		"blok.json":                        c.files["blok.json"],
		"go.mod":                           c.files["go.mod"],
		"runtimes/go/nodes/quote/quote.go": "package quote\n\nimport \"github.com/well-prado/new-blok/node\"\n\nvar Node = node.MustDefine(\"shop/quote\", \"1.0.0\", nil)\n",
		"runtimes/go/nodes/quote/QUOTE.go": "package quote\n",
	}
	root := materialize(t, c)
	if entries, _ := os.ReadDir(filepath.Join(root, "runtimes", "go", "nodes", "quote")); len(entries) != 2 {
		t.Skip("case-insensitive file system: two spellings cannot coexist here; TestCollisionsFoldLetterCase covers detection")
	}
	got := discoverCase(t, root)
	want := [][2]string{{CodePathCollision, "runtimes/go/nodes/quote/quote.go"}}
	if !equalPairs(got.diagnostics, want) {
		t.Fatalf("diagnostics=%v; want %v", got.diagnostics, want)
	}
}

func TestDiscoveryIsDeterministic(t *testing.T) {
	c := loadCase(t, filepath.Join("testdata", "cases", "descriptor-failures.txtar"))
	first := discoverCase(t, materialize(t, c))
	for range 3 {
		if again := discoverCase(t, materialize(t, c)); !equalPairs(first.diagnostics, again.diagnostics) {
			t.Fatalf("diagnostics changed between runs: %v vs %v", first.diagnostics, again.diagnostics)
		}
	}
}

func TestBounds(t *testing.T) {
	c := loadCase(t, filepath.Join("testdata", "cases", "duplicate-identity.txtar"))
	delete(c.files, "runtimes/go/nodes/b/b.go")
	deep := "runtimes/go/nodes/a"
	for range MaxDepth + 2 {
		deep += "/d"
	}
	c.files[deep+"/x.go"] = "package d\n"
	c.files["runtimes/go/nodes/a/big.go"] = "package a\n// " + strings.Repeat("x", MaxFileBytes) + "\n"
	got := discoverCase(t, materialize(t, c))
	codes := 0
	for _, d := range got.diagnostics {
		if d[0] == CodeLimitExceeded {
			codes++
		}
	}
	if codes != 2 {
		t.Fatalf("diagnostics=%v; want a depth and a size limit", got.diagnostics)
	}
}

func TestErrorTextAndLoadManifest(t *testing.T) {
	root := t.TempDir()
	_, err := LoadManifest(root)
	var layoutErr *Error
	if !errors.As(err, &layoutErr) || !slices.Equal(layoutErr.Codes(), []string{CodeManifestMissing}) {
		t.Fatalf("err=%v", err)
	}
	if err := os.WriteFile(filepath.Join(root, ManifestFile), []byte(`{"name":"shop","module":"example.com/shop","runtime":"go","layout":"unified","triggers":["http"],"types":"nodes/go/quote/types.go"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	manifest, err := LoadManifest(root)
	if err != nil || manifest.Types != "nodes/go/quote/types.go" || !slices.Equal(manifest.WorkflowPaths(), []string{DefaultWorkflowPath}) {
		t.Fatalf("manifest=%+v err=%v", manifest, err)
	}
	if text := (&Error{Diagnostics: layoutErr.Diagnostics}).Error(); !strings.HasPrefix(text, "layout: layout_manifest_missing blok.json") {
		t.Fatalf("error text %q", text)
	}
}

func TestNodeRootIsRootAnchored(t *testing.T) {
	for rel, want := range map[string]string{
		"nodes/go/quote":                     "nodes/go/quote",
		"nodes/go/quote/rates/deep":          "nodes/go/quote",
		"runtimes/go/nodes/quote":            "runtimes/go/nodes/quote",
		"runtimes/go/nodes/quote/rates/deep": "runtimes/go/nodes/quote",
		"internal/nodes/go/quote":            "",
		"nodes/go":                           "",
		"runtimes/go/other/quote":            "",
	} {
		if got, _ := NodeRoot(rel); got != want {
			t.Errorf("NodeRoot(%q)=%q; want %q", rel, got, want)
		}
	}
	if NodeDir(Classic, "go", "quote") != "runtimes/go/nodes/quote" || NodeDir(Unified, "nodejs", "fmt") != "nodes/nodejs/fmt" {
		t.Fatal("NodeDir does not follow architecture §3")
	}
}
