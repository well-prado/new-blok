package contract

import (
	"encoding/json"
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
