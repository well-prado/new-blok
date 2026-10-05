package parity

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// oldEngineGate is the explicit opt-in for every test that executes the
// published Blok 2.5.0 packages. Like the repository's other external-runtime
// gates (BLOK_NODE_INTEGRATION_ROOT), it keeps a fresh checkout's
// `go test ./...` independent of an npm install. When the gate is set, the
// pinned toolchain and installed package versions are verified before any
// comparison runs; a mismatch fails instead of measuring a different engine.
const oldEngineGate = "BLOK_PARITY_OLD_ENGINE"

var (
	oldEngineCheck sync.Once
	oldEngineError string
)

func requireOldEngine(t *testing.T) {
	t.Helper()
	if os.Getenv(oldEngineGate) != "1" {
		t.Skipf("explicit current-Blok parity gate: run `npm ci --ignore-scripts` in benchmarks/parity/old-engine, then set %s=1", oldEngineGate)
	}
	oldEngineCheck.Do(func() { oldEngineError = verifyPinnedOldEngine("old-engine") })
	if oldEngineError != "" {
		t.Fatal(oldEngineError)
	}
}

// verifyPinnedOldEngine returns a non-empty reason when the Node runtime or
// any installed direct dependency differs from the exact pins in package.json.
func verifyPinnedOldEngine(root string) string {
	raw, err := os.ReadFile(filepath.Join(root, "package.json"))
	if err != nil {
		return "read old-engine package.json: " + err.Error()
	}
	var manifest struct {
		Engines      map[string]string `json:"engines"`
		Dependencies map[string]string `json:"dependencies"`
	}
	if err := json.Unmarshal(raw, &manifest); err != nil {
		return "decode old-engine package.json: " + err.Error()
	}
	wantNode := manifest.Engines["node"]
	if wantNode == "" || len(manifest.Dependencies) == 0 {
		return "old-engine package.json must pin engines.node and its dependencies"
	}
	output, err := exec.Command("node", "--version").Output()
	if err != nil {
		return "run node --version: " + err.Error()
	}
	if got := strings.TrimPrefix(strings.TrimSpace(string(output)), "v"); got != wantNode {
		return "node " + got + " is not the pinned old-engine runtime " + wantNode
	}
	for name, want := range manifest.Dependencies {
		if strings.ContainsAny(want, "^~<>*| ") {
			return "dependency " + name + " is not pinned to an exact version: " + want
		}
		installed, err := os.ReadFile(filepath.Join(root, "node_modules", filepath.FromSlash(name), "package.json"))
		if err != nil {
			return "pinned dependency " + name + "@" + want + " is not installed; run `npm ci --ignore-scripts` in benchmarks/parity/old-engine"
		}
		var installedManifest struct {
			Version string `json:"version"`
		}
		if err := json.Unmarshal(installed, &installedManifest); err != nil {
			return "decode installed " + name + " manifest: " + err.Error()
		}
		if installedManifest.Version != want {
			return "installed " + name + "@" + installedManifest.Version + " differs from pinned " + want
		}
	}
	return ""
}

func TestVerifyPinnedOldEngineRejectsDrift(t *testing.T) {
	nodeOutput, err := exec.Command("node", "--version").Output()
	if err != nil {
		t.Skip("node is not installed; the drift check is exercised wherever the old-engine gate can run")
	}
	nodeVersion := strings.TrimPrefix(strings.TrimSpace(string(nodeOutput)), "v")
	write := func(t *testing.T, root, path, body string) {
		t.Helper()
		full := filepath.Join(root, filepath.FromSlash(path))
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	manifest := func(nodePin, depPin string) string {
		return `{"engines":{"node":"` + nodePin + `"},"dependencies":{"@blokjs/core":"` + depPin + `"}}`
	}
	for _, test := range []struct {
		name      string
		nodePin   string
		depPin    string
		installed string
		wantOK    bool
	}{
		{name: "exact pins match", nodePin: nodeVersion, depPin: "2.5.0", installed: "2.5.0", wantOK: true},
		{name: "different node runtime", nodePin: "0.0.1", depPin: "2.5.0", installed: "2.5.0"},
		{name: "installed package drift", nodePin: nodeVersion, depPin: "2.5.0", installed: "2.5.1"},
		{name: "range instead of exact pin", nodePin: nodeVersion, depPin: "^2.5.0", installed: "2.5.0"},
		{name: "dependency not installed", nodePin: nodeVersion, depPin: "2.5.0"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			write(t, root, "package.json", manifest(test.nodePin, test.depPin))
			if test.installed != "" {
				write(t, root, "node_modules/@blokjs/core/package.json", `{"version":"`+test.installed+`"}`)
			}
			reason := verifyPinnedOldEngine(root)
			if test.wantOK != (reason == "") {
				t.Fatalf("verifyPinnedOldEngine() = %q, want ok=%t", reason, test.wantOK)
			}
		})
	}
}
