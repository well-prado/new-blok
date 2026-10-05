package scaffold

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/internal/tooling/layout"
)

// goldenPath records the byte-exact starter each layout writes. It was
// captured from origin/main before layout discovery (#67) touched the
// scaffold, so a refactor that changes any starter file for an existing
// project turns this test red. Set BLOK_UPDATE_SCAFFOLD_GOLDEN=1 to rewrite
// it deliberately; a rewrite is a reviewed output change, never a fix.
const goldenPath = "testdata/golden.json"

type goldenFile struct {
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
}

func scaffoldDigests(t *testing.T, layoutName string) map[string]goldenFile {
	t.Helper()
	directory, paths := scaffoldShop(t, layoutName)
	files := map[string]goldenFile{}
	for _, path := range paths {
		content, err := os.ReadFile(filepath.Join(directory, filepath.FromSlash(path)))
		if err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(content)
		files[path] = goldenFile{SHA256: hex.EncodeToString(sum[:]), Bytes: len(content)}
	}
	return files
}

func scaffoldShop(t *testing.T, layoutName string) (string, []string) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "shop")
	paths, err := Create(Options{Directory: directory, Module: "example.com/shop", Layout: layoutName, Framework: Framework{Version: "v0.1.0"}})
	if err != nil {
		t.Fatal(err)
	}
	return directory, paths
}

// scaffoldCatalog is what layout discovery reads from a fresh starter in
// either layout: the descriptor identities, never the directories.
const scaffoldCatalog = `{"version":1,"nodes":[{"name":"app/calculate-quote","version":"1.0.0","runtime":"go","files":["bindings_gen.go","quote.go","types.go"]}],"workflows":[{"name":"quote","version":"1.0.0"}]}`

const scaffoldCatalogDigest = "sha256:4bb9149a1605dc2d38950cec15a4d4fd195eaf0574d1c22c47aec4b5556af4af"

// TestDiscoveryReadsBothStartersAsOneCatalog: discovery over the starters
// blok new writes yields one catalog and digest for both layouts.
func TestDiscoveryReadsBothStartersAsOneCatalog(t *testing.T) {
	for _, layoutName := range []string{layout.Classic, layout.Unified} {
		directory, _ := scaffoldShop(t, layoutName)
		project, err := layout.Discover(directory)
		if err != nil {
			t.Fatalf("%s: %v", layoutName, err)
		}
		if len(project.Nodes) != 1 || project.Nodes[0].Dir != NodeDir(layoutName, "quote") {
			t.Fatalf("%s: nodes=%+v", layoutName, project.Nodes)
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
		if string(canonical) != scaffoldCatalog || digest != scaffoldCatalogDigest {
			t.Fatalf("%s: catalog=%s digest=%s\nwant    %s %s", layoutName, canonical, digest, scaffoldCatalog, scaffoldCatalogDigest)
		}
	}
}

func TestScaffoldOutputsMatchGolden(t *testing.T) {
	got := map[string]map[string]goldenFile{}
	for _, layoutName := range []string{"classic", "unified"} {
		got[layoutName] = scaffoldDigests(t, layoutName)
	}
	if os.Getenv("BLOK_UPDATE_SCAFFOLD_GOLDEN") == "1" {
		encoded, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(goldenPath, append(encoded, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatal(err)
	}
	var want map[string]map[string]goldenFile
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	for _, layoutName := range []string{"classic", "unified"} {
		if len(got[layoutName]) != len(want[layoutName]) {
			t.Fatalf("%s: wrote %d files; golden has %d", layoutName, len(got[layoutName]), len(want[layoutName]))
		}
		for path, file := range want[layoutName] {
			if got[layoutName][path] != file {
				t.Errorf("%s %s: got %+v; golden %+v", layoutName, path, got[layoutName][path], file)
			}
		}
	}
}
