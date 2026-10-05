package contract

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoldenDocuments(t *testing.T) {
	cases := []struct{ name, file, code string }{
		{"valid", "valid.json", ""},
		{"duplicate IDs", "invalid-duplicate-id.json", "duplicate_id"},
		{"future version", "invalid-future-version.json", "unsupported_version"},
		{"unknown instruction", "invalid-unknown-instruction.json", "unknown_instruction"},
		{"malformed binding", "invalid-binding.json", "invalid_binding"},
		{"self reference", "invalid-self-reference.json", "invalid_reference"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			data, err := os.ReadFile(filepath.Join("..", "testdata", "contracts", tc.file))
			if err != nil {
				t.Fatal(err)
			}
			d, err := Parse(data)
			if tc.code == "" {
				if err != nil {
					t.Fatal(err)
				}
				p, err := d.Compile()
				if err != nil || len(p.Instructions) != 2 {
					t.Fatalf("compile=%+v err=%v", p, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.code) {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
		})
	}
}

func TestDocumentReferencesRequireStrictlyEarlierInstructions(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "contracts", "valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"call", "condition", "output", "wait", "parallel"} {
		for _, target := range []string{"calculate", "respond", "missing", ""} {
			t.Run(kind+"/"+target, func(t *testing.T) {
				d, err := Parse(data)
				if err != nil {
					t.Fatal(err)
				}
				d.Workflow.Instructions[0].Kind = kind
				d.Workflow.Instructions[0].References = []Reference{{Step: target}}
				assertDocumentBoundaryError(t, d, "invalid_reference", "workflow.instructions[0].references")
			})
		}
	}
	for _, kind := range []string{"call", "condition", "output", "wait", "parallel"} {
		t.Run("earlier/"+kind, func(t *testing.T) {
			d, err := Parse(data)
			if err != nil {
				t.Fatal(err)
			}
			d.Workflow.Instructions[1].Kind = kind
			if kind == "call" {
				d.Workflow.Instructions[1].Node = "quote-node"
			}
			encoded, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Parse(encoded); err != nil {
				t.Fatal(err)
			}
			p, err := d.Compile()
			if err != nil || len(p.Instructions) != 2 || len(p.Instructions[1].References) != 1 || p.Instructions[1].References[0].Step != "calculate" || len(p.Instructions[1].References[0].Path) != 1 || p.Instructions[1].References[0].Path[0] != "totalCents" {
				t.Fatalf("earlier reference/path changed: program=%+v err=%v", p, err)
			}
			if _, err := d.Canonical(); err != nil {
				t.Fatal(err)
			}
		})
	}
	t.Run("later instruction self reference", func(t *testing.T) {
		d, err := Parse(data)
		if err != nil {
			t.Fatal(err)
		}
		d.Workflow.Instructions[1].References[0].Step = "respond"
		assertDocumentBoundaryError(t, d, "invalid_reference", "workflow.instructions[1].references")
	})
	t.Run("duplicate id retains diagnostic", func(t *testing.T) {
		d, err := Parse(data)
		if err != nil {
			t.Fatal(err)
		}
		d.Workflow.Instructions[1].ID = "calculate"
		assertDocumentBoundaryError(t, d, "duplicate_id", "workflow.instructions")
	})
}

// Issue #247: "output" is reserved only by flow, whose Lower synthesizes an
// instruction under it. A document names its own output instruction, so a
// call named "output" is valid and only a repeated id collides.
func TestDocumentCallNamedOutputIsValidAndDuplicatesAreRejected(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "contracts", "valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	d.Workflow.Instructions[0].ID = "output"
	d.Workflow.Instructions[1].References[0].Step = "output"
	p, err := d.Compile()
	if err != nil || len(p.Instructions) != 2 || p.Instructions[0].ID != "output" || p.Instructions[1].ID != "respond" {
		t.Fatalf("compile=%+v err=%v", p, err)
	}
	d.Workflow.Instructions[1].ID = "output"
	assertDocumentBoundaryError(t, d, "duplicate_id", "workflow.instructions")
}

func assertDocumentBoundaryError(t *testing.T, d Document, code, path string) {
	t.Helper()
	encoded, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	boundaries := map[string]func() error{
		"Validate":  d.Validate,
		"Parse":     func() error { _, err := Parse(encoded); return err },
		"Compile":   func() error { _, err := d.Compile(); return err },
		"Canonical": func() error { _, err := d.Canonical(); return err },
	}
	for name, boundary := range boundaries {
		var diagnostic *Error
		if err := boundary(); !errors.As(err, &diagnostic) || diagnostic.Code != code || diagnostic.Path != path {
			t.Errorf("%s returned %v; want code=%s path=%s", name, err, code, path)
		}
	}
}

func TestPresenceAndDiscardedOutput(t *testing.T) {
	var d Document
	if err := json.Unmarshal([]byte(`{"version":1,"workflow":{"id":"w","name":"w","version":"1.0.0","digest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","inputSchema":{},"outputSchema":{},"instructions":[{"id":"a","kind":"output"},{"id":"b","kind":"output","output":null}]}}`), &d); err != nil {
		t.Fatal(err)
	}
	if d.Workflow.Instructions[0].Output.Presence != Missing || d.Workflow.Instructions[1].Output.Presence != Null {
		t.Fatalf("presence not preserved: %+v", d.Workflow.Instructions)
	}
}

func TestCanonicalConversionIsDeterministic(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "contracts", "valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	a, err := d.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	b, err := d.Canonical()
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Fatalf("canonical output changed: %s != %s", a, b)
	}
}

func TestTrailingJSONIsRejected(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "contracts", "valid.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(append(data, []byte(" {}")...)); err == nil || !strings.Contains(err.Error(), "trailing_data") {
		t.Fatalf("got %v, want trailing_data", err)
	}
}

func FuzzParseBounded(f *testing.F) {
	data, _ := os.ReadFile(filepath.Join("..", "testdata", "contracts", "valid.json"))
	f.Add(data)
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > MaxDocumentBytes {
			t.Skip()
		}
		_, _ = Parse(input)
	})
}

// ValidID is the exported grammar documents validate ids with; flow checks
// step ids with it (#251). Pin which ids it accepts and that a document
// rejects exactly the others with invalid_id.
func TestValidIDIsTheDocumentIDGrammar(t *testing.T) {
	valid := []string{"a", "z9", "line_items", "line-items", "output", "a" + strings.Repeat("b", 63)}
	invalid := []string{"", "a.b", "A", "Output", "1a", "-a", "_a", "a b", "a/b", "$step", "caf\u00e9", "a" + strings.Repeat("b", 64)}
	for _, id := range valid {
		if !ValidID(id) {
			t.Fatalf("ValidID(%q)=false, want true", id)
		}
		if err := (Document{Version: CurrentVersion, Workflow: Workflow{ID: id}}).Validate(); err != nil && strings.Contains(err.Error(), "workflow.id") {
			t.Fatalf("document rejected workflow id %q: %v", id, err)
		}
	}
	for _, id := range invalid {
		if ValidID(id) {
			t.Fatalf("ValidID(%q)=true, want false", id)
		}
		err := Document{Version: CurrentVersion, Workflow: Workflow{ID: id}}.Validate()
		var diagnostic *Error
		if !errors.As(err, &diagnostic) || diagnostic.Code != "invalid_id" || diagnostic.Path != "workflow.id" {
			t.Fatalf("document workflow id %q: err=%v, want invalid_id at workflow.id", id, err)
		}
	}
}
