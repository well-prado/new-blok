package compile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

func loadDocument(t *testing.T) contract.Document {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "contracts", "valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	document, err := contract.Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	document.Nodes[0].OutputSchema = json.RawMessage(`{"type":"object","properties":{"totalCents":{"type":"integer"}},"required":["totalCents"]}`)
	return document
}

func TestCompileResolvesWholeAndFieldReferencesAndPreservesSource(t *testing.T) {
	document := loadDocument(t)
	document.Workflow.Instructions[1].Source = &contract.SourceSpan{File: "quote.go", StartLine: 12, StartColumn: 3, EndLine: 12, EndColumn: 30}
	result, err := Compile(document)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Program.Instructions) != 2 || result.Program.Instructions[1].Source == nil {
		t.Fatalf("program=%+v", result.Program)
	}
	if len(result.RuntimeValidations) != 0 {
		t.Fatalf("runtime validations=%+v", result.RuntimeValidations)
	}
}

func TestCompileRejectsMissingAndInvalidFields(t *testing.T) {
	document := loadDocument(t)
	document.Workflow.Instructions[1].References[0].Path = []string{"missing"}
	_, err := Compile(document)
	if err == nil || !strings.Contains(err.Error(), "unresolved_field") {
		t.Fatalf("got %v", err)
	}
	document = loadDocument(t)
	document.Workflow.Instructions[1].References[0].Step = "future"
	_, err = Compile(document)
	if err == nil || !strings.Contains(err.Error(), "invalid_reference") {
		t.Fatalf("got %v", err)
	}
}

func TestCompileRejectsIncompatibleTriggerMapping(t *testing.T) {
	document := loadDocument(t)
	document.Bindings[0].InputSchema = json.RawMessage(`{"type":"string"}`)
	_, err := Compile(document)
	if err == nil || !strings.Contains(err.Error(), "incompatible_trigger_mapping") {
		t.Fatalf("got %v", err)
	}
}

func TestCompileRetainsDynamicValidationRequirement(t *testing.T) {
	document := loadDocument(t)
	document.Nodes[0].OutputSchema = json.RawMessage(`{"anyOf":[{"type":"object"},{"type":"null"}]}`)
	result, err := Compile(document)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.RuntimeValidations) != 1 || result.RuntimeValidations[0].Step != "respond" {
		t.Fatalf("runtime validations=%+v", result.RuntimeValidations)
	}
}

func FuzzCompileMalformedReferences(f *testing.F) {
	f.Add("missing")
	f.Fuzz(func(t *testing.T, step string) {
		document := loadDocument(t)
		document.Workflow.Instructions[1].References[0].Step = step
		_, _ = Compile(document)
	})
}
