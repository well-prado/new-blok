package policy

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPolicyEvidenceInventory(t *testing.T) {
	raw, err := os.ReadFile("testdata/provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var provenance struct {
		Issue                     int
		Source, License, Decision string
		PrivateData               bool
		Fixtures                  []string
	}
	if err := json.Unmarshal(raw, &provenance); err != nil {
		t.Fatal(err)
	}
	if provenance.Issue != 75 || provenance.Source == "" || provenance.License != "Apache-2.0" || provenance.PrivateData || len(provenance.Fixtures) != 2 {
		t.Fatalf("invalid synthetic provenance: %+v", provenance)
	}
	for _, file := range provenance.Fixtures {
		if file != "adversarial.json" && file != "catalog.json" {
			t.Fatalf("unknown fixture inventory: %s", file)
		}
		raw, err := os.ReadFile(filepath.Join("testdata", file))
		if err != nil {
			t.Fatal(err)
		}
		var fixtures []struct {
			Name, Error                  string
			Effects, Attempts, Published *int
		}
		if err := json.Unmarshal(raw, &fixtures); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		for _, f := range fixtures {
			if f.Name == "" || seen[f.Name] || f.Effects == nil || f.Attempts == nil || f.Published == nil || *f.Effects < 0 || *f.Attempts < 0 || *f.Published < 0 || *f.Published > *f.Attempts {
				t.Fatalf("invalid expected trace: %+v", f)
			}
			if f.Error != "" && f.Error != "stale" && f.Error != "denied" && f.Error != "evidence" {
				t.Fatalf("unknown error expectation: %+v", f)
			}
			seen[f.Name] = true
		}
		if len(seen) < 10 || !seen["accepted"] || !seen["missing"] || !seen["expired"] || !seen["changed-input"] {
			t.Fatalf("missing executable scenarios: %v", seen)
		}
	}
	root := filepath.Join("..", "..")
	adr, err := os.ReadFile(filepath.Join(root, provenance.Decision))
	if err != nil {
		t.Fatal(err)
	}
	for _, term := range []string{"ToolDigest", "BindCatalog", "24-hour", "SIGKILL", "#48", "Review R", "TestCatalogPolicyProviderFixtures", "TestActualNativeNodeCatalogDurablePolicy", "TestProcessKillApprovalDispatchPublication"} {
		if !strings.Contains(string(adr), term) {
			t.Errorf("ADR missing evidence/boundary: %s", term)
		}
	}
	for _, file := range []string{"README.md", "docs/architecture.md"} {
		doc, err := os.ReadFile(filepath.Join(root, file))
		if err != nil || !strings.Contains(string(doc), "0008-durable-tool-policy.md") {
			t.Fatalf("missing public decision link: %s %v", file, err)
		}
	}
	roadmap, err := os.ReadFile(filepath.Join(root, ".github", "roadmap.json"))
	if err != nil {
		t.Fatal(err)
	}
	var plan struct {
		Issues map[string]struct {
			Number       int
			Dependencies []string
			Review       string
		}
	}
	if err := json.Unmarshal(roadmap, &plan); err != nil {
		t.Fatal(err)
	}
	task := plan.Issues["E14-T02"]
	if task.Number != 75 || task.Review != "R" || !sameSet(task.Dependencies, []string{"E14-T01", "E07-T06"}) {
		t.Fatalf("dependency/review drift: %+v", task)
	}
}
