package schema

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoldenCorpus(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "conformance", "schema-corpus.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name       string          `json:"name"`
		Schema     json.RawMessage `json:"schema"`
		Value      json.RawMessage `json:"value"`
		Error      string          `json:"error"`
		Normalized json.RawMessage `json:"normalized"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			s, err := Parse(tc.Schema)
			if err != nil {
				t.Fatal(err)
			}
			normalized, err := s.Normalize(tc.Value)
			if tc.Error != "" {
				if err == nil || !strings.Contains(err.Error(), tc.Error) {
					t.Fatalf("got %v, want %s", err, tc.Error)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(normalized) != string(tc.Normalized) {
				t.Fatalf("normalized=%s want=%s", normalized, tc.Normalized)
			}
		})
	}
}

func TestInt64WireValueIsString(t *testing.T) {
	s, err := Parse([]byte(`{"type":"integer","wire":"int64-string","minimum":-9223372036854775808,"maximum":9223372036854775807}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.Normalize([]byte(`"9223372036854775807"`))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `"9223372036854775807"` {
		t.Fatalf("got %s", got)
	}
}

func TestNativeAndWireInt64NormalizationMatch(t *testing.T) {
	s, err := Parse([]byte(`{"type":"object","properties":{"total":{"type":"integer","wire":"int64-string"}},"required":["total"],"additionalProperties":false}`))
	if err != nil {
		t.Fatal(err)
	}
	native, err := s.NormalizeValue(map[string]any{"total": int64(9223372036854775807)})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := s.Normalize([]byte(`{"total":"9223372036854775807"}`))
	if err != nil {
		t.Fatal(err)
	}
	nativeJSON, err := json.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	if string(nativeJSON) != string(wire) {
		t.Fatalf("native=%s wire=%s", nativeJSON, wire)
	}
}
func TestAbsentAndNull(t *testing.T) {
	s, err := Parse([]byte(`{"type":"object","properties":{"name":{"type":"string","nullable":true}},"additionalProperties":false}`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Normalize([]byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Normalize([]byte(`{"name":null}`)); err != nil {
		t.Fatal(err)
	}
}
func TestUnprovenCompatibilityFails(t *testing.T) {
	a, _ := Parse([]byte(`{"type":"string"}`))
	b, _ := Parse([]byte(`{"type":"integer"}`))
	if err := Compatible(a, b); err == nil || !strings.Contains(err.Error(), "compatibility_unproven") {
		t.Fatalf("got %v", err)
	}
}
func FuzzNormalizeBounded(f *testing.F) {
	f.Add([]byte(`{"name":"blok"}`))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > MaxPayloadBytes {
			t.Skip()
		}
		s := Schema{Type: "object", Properties: map[string]Schema{"name": {Type: "string"}}, AdditionalProperties: boolPtr(false)}
		_, _ = s.Normalize(input)
	})
}
func boolPtr(v bool) *bool { return &v }

func FuzzNullUnionNormalization(f *testing.F) {
	for _, seed := range []string{`null`, `"blok"`, `true`, `1`, `[]`, `{}`, `"9223372036854775807"`} {
		f.Add([]byte(seed))
	}
	null := Schema{Type: "null"}
	union := Schema{AnyOf: []Schema{{Type: "null"}, {Type: "string"}}}
	ambiguous := Schema{AnyOf: []Schema{{Type: "null"}, {Type: "null"}}}
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > MaxPayloadBytes {
			t.Skip()
		}
		var value any
		if err := json.Unmarshal(input, &value); err != nil {
			return
		}
		_, err := null.Normalize(input)
		if (err == nil) != (value == nil) {
			t.Fatalf("null schema accepted non-null or rejected null: %s, %v", input, err)
		}
		_, isString := value.(string)
		got, err := union.Normalize(input)
		if (err == nil) != (value == nil || isString) {
			t.Fatalf("union result: %s, %v", input, err)
		}
		if err == nil {
			again, err := union.Normalize(got)
			if err != nil || string(again) != string(got) {
				t.Fatalf("normalization not stable: %s -> %s, %v", got, again, err)
			}
		}
		if _, err := ambiguous.Normalize(input); err == nil {
			t.Fatalf("ambiguous null union accepted: %s", input)
		}
	})
}
