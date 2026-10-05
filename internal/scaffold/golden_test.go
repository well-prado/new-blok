package scaffold

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
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

func scaffoldDigests(t *testing.T, layout string) map[string]goldenFile {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "shop")
	paths, err := Create(Options{Directory: directory, Module: "example.com/shop", Layout: layout, Framework: Framework{Version: "v0.1.0"}})
	if err != nil {
		t.Fatal(err)
	}
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

func TestScaffoldOutputsMatchGolden(t *testing.T) {
	got := map[string]map[string]goldenFile{}
	for _, layout := range []string{"classic", "unified"} {
		got[layout] = scaffoldDigests(t, layout)
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
	for _, layout := range []string{"classic", "unified"} {
		if len(got[layout]) != len(want[layout]) {
			t.Fatalf("%s: wrote %d files; golden has %d", layout, len(got[layout]), len(want[layout]))
		}
		for path, file := range want[layout] {
			if got[layout][path] != file {
				t.Errorf("%s %s: got %+v; golden %+v", layout, path, got[layout][path], file)
			}
		}
	}
}
