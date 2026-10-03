package scaffold

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateProducesSortedDeterministicStarter(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "app")
	paths, err := Create(Options{Directory: directory, Module: "example.com/app", Name: "app", Triggers: []string{"http"}})
	if err != nil {
		t.Fatal(err)
	}
	for index := 1; index < len(paths); index++ {
		if paths[index-1] >= paths[index] {
			t.Fatalf("paths are not sorted: %v", paths)
		}
	}
	manifest, err := os.ReadFile(filepath.Join(directory, "blok.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(manifest), `"module": "example.com/app"`) {
		t.Fatalf("unexpected manifest: %s", manifest)
	}
	if _, err := Create(Options{Directory: directory, Module: "example.com/app", Name: "app"}); err == nil {
		t.Fatal("expected existing files to be protected")
	}
}

func TestCreateRejectsUnsupportedSelectionsBeforeWriting(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "app")
	if _, err := Create(Options{Directory: directory, Name: "app", Runtime: "node"}); err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("got %v", err)
	}
	if _, err := os.Stat(directory); !os.IsNotExist(err) {
		t.Fatalf("invalid options created target: %v", err)
	}
}
