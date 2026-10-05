package graphcheck

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSyntheticGraphs(t *testing.T) {
	cases := []struct {
		name, dir string
		want      [][2]string // {diagnostic code, package}
	}{
		{"good", "good", nil},
		{"direct engine boundary", "bad-engine", [][2]string{{"engine_import_forbidden", "fixture.test/engine"}}},
		{"transitive engine boundary", "bad-transitive", [][2]string{{"engine_transitive_import_forbidden", "fixture.test/engine"}}},
		{"engine imports a broker client", "bad-engine-broker", [][2]string{{"engine_external_import_forbidden", "fixture.test/engine"}}},
		{"engine reaches a broker client through a module package", "bad-engine-broker-transitive", [][2]string{{"engine_transitive_import_forbidden", "fixture.test/engine"}}},
		{"unified node alias", "bad-node-unified", [][2]string{{"node_import_forbidden", "fixture.test/nodes/go/alpha"}}},
		{"classic node alias", "bad-node-classic", [][2]string{{"node_import_forbidden", "fixture.test/runtimes/go/nodes/alpha"}}},
		{"trigger adapter boundaries", "bad-trigger", [][2]string{
			{"trigger_import_forbidden", "fixture.test/trigger/http"},
			{"trigger_transitive_import_forbidden", "fixture.test/trigger/cron"},
			{"trigger_declaration_missing", "fixture.test/trigger/sse"},
			{"trigger_declaration_missing", "fixture.test/trigger/mcp"},
			{"trigger_declaration_missing", "fixture.test/trigger/pubsub/nats"},
			{"trigger_conformance_missing", "fixture.test/trigger/grpc"},
			{"trigger_conformance_missing", "fixture.test/trigger/websocket"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diagnostics, err := Check(filepath.Join("..", "..", "..", "testdata", "graphs", tc.dir))
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == nil {
				if len(diagnostics) != 0 {
					t.Fatalf("unexpected diagnostics: %+v", diagnostics)
				}
				return
			}
			for _, want := range tc.want {
				found := false
				for _, d := range diagnostics {
					if d.Code == want[0] && d.Package == want[1] {
						found = true
					}
				}
				if !found {
					t.Fatalf("diagnostics=%+v, want %s on %s", diagnostics, want[0], want[1])
				}
			}
			for _, d := range diagnostics {
				if d.Package == "fixture.test/trigger/websocket" && d.Code == "trigger_declaration_missing" || d.Package == "fixture.test/trigger/pubsub/nats" && d.Code == "trigger_conformance_missing" {
					t.Fatalf("false positive %+v: typed Declaration or dot-imported RunTrigger call was not recognized", d)
				}
			}
		})
	}
}

// TestRepositoryGraph applies the same boundaries to this repository: the
// engine reaches no adapter or store, no adapter reaches the interpreter or
// journal, and every trigger/<kind> package declares its behavior and runs
// trigger conformance.
func TestRepositoryGraph(t *testing.T) {
	diagnostics, err := Check(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if len(diagnostics) != 0 {
		t.Fatalf("repository graph diagnostics: %+v", diagnostics)
	}
	graph, err := Analyze(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	adapters := 0
	for path, p := range graph.Packages {
		if p.Source && isTriggerAdapter(graph.Module, path) {
			adapters++
		}
	}
	if adapters < 2 {
		t.Fatalf("found %d trigger adapters; the repository check is not seeing trigger/http and trigger/worker", adapters)
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

// TestNestedNodeFilesShareOneOwner: files nested under one node directory,
// in either layout, belong to that node, so importing them is a same-node
// import, while importing another node still fails (#67). Only a
// root-anchored nodes/ or runtimes/<rt>/nodes/ path is a node: a "nodes"
// element elsewhere, including in the checkout's own absolute path, is an
// ordinary package name.
func TestNestedNodeFilesShareOneOwner(t *testing.T) {
	for _, layout := range [][2]string{{"runtimes/go/nodes/alpha", "runtimes/go/nodes/beta"}, {"nodes/go/alpha", "nodes/go/beta"}} {
		alpha, beta := layout[0], layout[1]
		for _, parent := range []string{"plain", "nodes"} {
			t.Run(alpha+" under "+parent, func(t *testing.T) {
				root := filepath.Join(t.TempDir(), parent, "checkout")
				files := map[string]string{
					"go.mod":                   "module fixture.test\n",
					alpha + "/alpha.go":        "package alpha\n\nimport (\n\t_ \"fixture.test/" + alpha + "/rates\"\n\t_ \"fixture.test/" + beta + "\"\n)\n",
					alpha + "/rates/rates.go":  "package rates\n\nimport _ \"fixture.test/" + alpha + "/rates/deep\"\n",
					alpha + "/rates/deep/d.go": "package deep\n",
					beta + "/beta.go":          "package beta\n",
					"internal/nodes/x/x.go":    "package x\n\nimport _ \"fixture.test/internal/nodes/y\"\n",
					"internal/nodes/y/y.go":    "package y\n",
				}
				for rel, content := range files {
					path := filepath.Join(root, filepath.FromSlash(rel))
					if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
						t.Fatal(err)
					}
					if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
						t.Fatal(err)
					}
				}
				diagnostics, err := Check(root)
				if err != nil {
					t.Fatal(err)
				}
				if len(diagnostics) != 1 || diagnostics[0].Code != "node_import_forbidden" || diagnostics[0].Package != "fixture.test/"+alpha || diagnostics[0].Import != "fixture.test/"+beta {
					t.Fatalf("diagnostics=%+v; want only %s importing %s", diagnostics, alpha, beta)
				}
			})
		}
	}
}
