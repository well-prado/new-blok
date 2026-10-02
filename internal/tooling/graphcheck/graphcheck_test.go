package graphcheck

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSyntheticGraphs(t *testing.T) {
	cases := []struct{ name, dir, code string }{
		{"good", "good", ""},
		{"direct engine boundary", "bad-engine", "engine_import_forbidden"},
		{"transitive engine boundary", "bad-transitive", "engine_transitive_import_forbidden"},
		{"unified node alias", "bad-node-unified", "node_import_forbidden"},
		{"classic node alias", "bad-node-classic", "node_import_forbidden"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diagnostics, err := Check(filepath.Join("..", "..", "..", "testdata", "graphs", tc.dir))
			if err != nil {
				t.Fatal(err)
			}
			if tc.code == "" {
				if len(diagnostics) != 0 {
					t.Fatalf("unexpected diagnostics: %+v", diagnostics)
				}
				return
			}
			found := false
			for _, d := range diagnostics {
				if d.Code == tc.code {
					found = true
				}
			}
			if !found {
				t.Fatalf("diagnostics=%+v, want %s", diagnostics, tc.code)
			}
		})
	}
}

func TestDiscoveryDoesNotExecuteInit(t *testing.T) {
	if _, err := Analyze(filepath.Join("..", "..", "..", "testdata", "graphs", "good")); err != nil {
		t.Fatal(err)
	}
}

func TestFixtureProvenance(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "graphs", "provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Source      string `json:"source"`
		License     string `json:"license"`
		PrivateData bool   `json:"privateData"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Source == "" || fixture.License != "Apache-2.0" || fixture.PrivateData {
		t.Fatalf("invalid fixture provenance: %+v", fixture)
	}
}
