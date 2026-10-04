package migration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

type migrationFixture struct {
	Inventory []contract.NodeDescriptor `json:"inventory"`
	Cases     []struct {
		Name     string          `json:"name"`
		Source   json.RawMessage `json:"source"`
		Expected struct {
			Supported bool   `json:"supported"`
			Workflow  string `json:"workflow,omitempty"`
			Code      string `json:"code,omitempty"`
			Path      string `json:"path,omitempty"`
		} `json:"expected"`
	} `json:"cases"`
	ExportCases []struct {
		Name     string `json:"name"`
		Mutation string `json:"mutation"`
		Code     string `json:"code"`
	} `json:"exportCases"`
}

func TestMigrationFixtures(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "parity", "migration-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixtures migrationFixture
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	inventory := make(map[string]contract.NodeDescriptor, len(fixtures.Inventory))
	for _, descriptor := range fixtures.Inventory {
		inventory[descriptor.ID] = descriptor
	}
	for _, fixture := range fixtures.Cases {
		t.Run(fixture.Name, func(t *testing.T) {
			doc, err := Convert(fixture.Source, inventory)
			if fixture.Expected.Supported {
				if err != nil {
					t.Fatalf("Convert() error = %v", err)
				}
				if doc.Workflow.ID != fixture.Expected.Workflow {
					t.Fatalf("workflow id = %q, want %q", doc.Workflow.ID, fixture.Expected.Workflow)
				}
				if err := doc.Validate(); err != nil {
					t.Fatalf("migrated document is invalid: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("Convert() accepted an unsupported migration")
			}
			var diagnostic Diagnostic
			if !asDiagnostic(err, &diagnostic) {
				t.Fatalf("error %T is not actionable: %v", err, err)
			}
			if diagnostic.Code != fixture.Expected.Code || diagnostic.Path != fixture.Expected.Path || diagnostic.Remediation == "" {
				t.Fatalf("diagnostic = %+v, expected code=%q path=%q and remediation", diagnostic, fixture.Expected.Code, fixture.Expected.Path)
			}
		})
	}
	baseSource := []byte(`{"schemaVersion":"2","name":"order quote","version":"1.0.0","trigger":{"http":{}},"steps":[{"id":"load","use":"catalog","inputs":{"$ref":{"step":"@trigger","path":[]}}},{"id":"total","use":"price","inputs":{"$ref":{"step":"load","path":[]}}}]}`)
	for _, fixture := range fixtures.ExportCases {
		t.Run("export/"+fixture.Name, func(t *testing.T) {
			doc, err := Convert(baseSource, inventory)
			if err != nil {
				t.Fatalf("base conversion: %v", err)
			}
			switch fixture.Mutation {
			case "output-earlier-call":
				doc.Workflow.Instructions[len(doc.Workflow.Instructions)-1].References[0].Step = "load"
			case "call-self-reference":
				call := &doc.Workflow.Instructions[1]
				call.References[0].Step = call.ID
				if err := doc.Validate(); err != nil {
					t.Fatalf("public document validator no longer reproduces the self-reference edge case: %v", err)
				}
			case "extra-call-reference":
				call := &doc.Workflow.Instructions[1]
				call.References = append(call.References, contract.Reference{Step: "load", Path: []string{"sku"}})
			case "multiple-bindings":
				doc.Bindings = append(doc.Bindings, contract.Binding{ID: "second-entry", Kind: "sse", Workflow: doc.Workflow.ID, InputSchema: append(json.RawMessage(nil), doc.Workflow.InputSchema...)})
			case "unreferenced-node":
				doc.Nodes = append(doc.Nodes, contract.NodeDescriptor{ID: "unused", Version: "1.0.0", Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)})
			default:
				t.Fatalf("unknown export mutation %q", fixture.Mutation)
			}
			if _, err := Export(doc); err == nil {
				t.Fatal("Export() silently dropped unsupported document semantics")
			} else {
				var diagnostic Diagnostic
				if !asDiagnostic(err, &diagnostic) || diagnostic.Code != fixture.Code || diagnostic.Remediation == "" {
					t.Fatalf("export diagnostic = %T %v; expected code %q and remediation", err, err, fixture.Code)
				}
			}
		})
	}
}

func TestStructuralMigrationRoundTrips(t *testing.T) {
	fixture := []byte(`{"schemaVersion":"2","name":"order quote","version":"1.0.0","steps":[{"id":"load","use":"catalog","inputs":{"$ref":{"step":"@trigger","path":[]}}},{"id":"total","use":"price-sku","inputs":{"$ref":{"step":"load","path":["sku"]}}}]}`)
	inventory := fixtureInventory()
	first, err := Convert(fixture, inventory)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Export(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Convert(encoded, inventory)
	if err != nil {
		t.Fatalf("round-trip conversion: %v\n%s", err, encoded)
	}
	if len(first.Workflow.Instructions) != len(second.Workflow.Instructions) {
		t.Fatalf("instruction count changed: %d -> %d", len(first.Workflow.Instructions), len(second.Workflow.Instructions))
	}
	for index := range first.Workflow.Instructions {
		left, _ := json.Marshal(first.Workflow.Instructions[index])
		right, _ := json.Marshal(second.Workflow.Instructions[index])
		if string(left) != string(right) {
			t.Fatalf("instruction %d changed: %s -> %s", index, left, right)
		}
	}
	if string(first.Workflow.InputSchema) != string(second.Workflow.InputSchema) || string(first.Workflow.OutputSchema) != string(second.Workflow.OutputSchema) {
		t.Fatalf("schemas changed on round trip")
	}
	if got := second.Workflow.Instructions[1].References[0].Path; len(got) != 1 || got[0] != "sku" {
		t.Fatalf("earlier-step path projection changed on export/convert round trip: %v", got)
	}
}

func TestMigratedWorkflowIDsDoNotCollapseSlugCollisions(t *testing.T) {
	left := migratedWorkflowID("order quote")
	right := migratedWorkflowID("order-quote")
	if left == right || !validID.MatchString(left) || !validID.MatchString(right) || len(left) > 64 || len(right) > 64 {
		t.Fatalf("workflow ids do not safely distinguish source names: %q and %q", left, right)
	}
}

func TestMigrationBoundsCollectionsAndDiagnostics(t *testing.T) {
	if _, err := Convert(make([]byte, maxSourceBytes+1), nil); diagnosticCode(t, err) != "source_too_large" {
		t.Fatalf("oversized source diagnostic = %v", err)
	}
	var oversizedSteps strings.Builder
	oversizedSteps.WriteString(`{"schemaVersion":"2","name":"bounded","version":"1.0.0","steps":[`)
	for index := 0; index <= maxWorkflowStep; index++ {
		if index > 0 {
			oversizedSteps.WriteByte(',')
		}
		oversizedSteps.WriteString(`{"id":"step","use":"node"}`)
	}
	oversizedSteps.WriteString(`]}`)
	if _, err := Convert([]byte(oversizedSteps.String()), nil); diagnosticCode(t, err) != "too_many_steps" {
		t.Fatalf("oversized step collection diagnostic = %v", err)
	}

	tooManyNodes := make(map[string]contract.NodeDescriptor, maxInventory+1)
	for index := 0; index <= maxInventory; index++ {
		tooManyNodes[fmt.Sprintf("node-%d", index)] = contract.NodeDescriptor{}
	}
	if _, err := Convert([]byte(`{}`), tooManyNodes); diagnosticCode(t, err) != "inventory_too_large" {
		t.Fatalf("oversized inventory diagnostic = %v", err)
	}
	if _, err := Convert([]byte(`{"schemaVersion":"2","name":"forward","version":"1.0.0","steps":[{"id":"first","use":"catalog","inputs":{"$ref":{"step":"later","path":[]}}},{"id":"later","use":"price"}]}`), fixtureInventory()); diagnosticCode(t, err) != "forward_or_missing_reference" {
		t.Fatalf("forward-reference diagnostic = %v", err)
	}
	largeSchema := func(value string) json.RawMessage {
		encoded, _ := json.Marshal(map[string]any{"type": "object", "description": strings.Repeat(value, 290_000)})
		return encoded
	}
	largeInventory := map[string]contract.NodeDescriptor{"large": {ID: "large", Version: "1.0.0", Digest: "sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", InputSchema: largeSchema("i"), OutputSchema: largeSchema("o")}}
	largeSource := []byte(`{"schemaVersion":"2","name":"large schema","version":"1.0.0","steps":[{"id":"call","use":"large"}]}`)
	if _, err := Convert(largeSource, largeInventory); diagnosticCode(t, err) != "target_document_too_large" {
		t.Fatalf("oversized converted document diagnostic = %v", err)
	}

	collisionInventory := fixtureInventory()
	collisionInventory["other"] = contract.NodeDescriptor{ID: "catalog", Version: "9.0.0", Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object"}`)}
	collisionSource := []byte(`{"schemaVersion":"2","name":"collision","version":"1.0.0","steps":[{"id":"first","use":"catalog"},{"id":"second","use":"other"}]}`)
	if _, err := Convert(collisionSource, collisionInventory); diagnosticCode(t, err) != "target_node_identity_collision" {
		t.Fatalf("node identity collision diagnostic = %v", err)
	}
	longID := strings.Repeat("x", 100_000)
	largeDiagnosticSource := []byte(`{"schemaVersion":"2","name":"bounded","version":"1.0.0","steps":[{"id":"` + longID + `","use":"catalog"}]}`)
	_, err := Convert(largeDiagnosticSource, fixtureInventory())
	if diagnosticCode(t, err) != "unsupported_step_id" || len(err.Error()) > 512 {
		t.Fatalf("unbounded diagnostic = %d bytes: %v", len(err.Error()), err)
	}
}

func diagnosticCode(t *testing.T, err error) string {
	t.Helper()
	var diagnostic Diagnostic
	if !asDiagnostic(err, &diagnostic) {
		t.Fatalf("error %T is not an actionable migration diagnostic: %v", err, err)
	}
	return diagnostic.Code
}

func fixtureInventory() map[string]contract.NodeDescriptor {
	return map[string]contract.NodeDescriptor{
		"catalog":   {ID: "catalog", Version: "1.0.0", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", InputSchema: json.RawMessage(`{"type":"object"}`), OutputSchema: json.RawMessage(`{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"]}`)},
		"price":     {ID: "price", Version: "1.0.0", Digest: "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", InputSchema: json.RawMessage(`{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"]}`), OutputSchema: json.RawMessage(`{"type":"object","properties":{"totalCents":{"type":"integer"}},"required":["totalCents"]}`)},
		"price-sku": {ID: "price-sku", Version: "1.0.0", Digest: "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc", InputSchema: json.RawMessage(`{"type":"string"}`), OutputSchema: json.RawMessage(`{"type":"object","properties":{"totalCents":{"type":"integer"}},"required":["totalCents"]}`)},
	}
}

func asDiagnostic(err error, target *Diagnostic) bool {
	switch typed := err.(type) {
	case Diagnostic:
		*target = typed
		return true
	case Diagnostics:
		if len(typed) == 0 {
			return false
		}
		*target = typed[0]
		return true
	default:
		return false
	}
}
