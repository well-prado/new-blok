package catalog

import (
	"math"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/schema"
)

var objectSchema = []byte(`{"type":"object","properties":{"name":{"type":"string"},"count":{"type":"integer","wire":"int64-string"}},"required":["name"]}`)

func TestRegistryUsesSameDescriptorRulesForGenericNodes(t *testing.T) {
	r := NewRegistry()
	n := Node{Descriptor: Descriptor{Name: "generic/select", Version: "1.0.0", Description: "select fields", InputSchema: objectSchema, OutputSchema: objectSchema, Deterministic: true}, Execute: func(v any) (any, error) { return v, nil }}
	if err := r.Register(n); err != nil {
		t.Fatal(err)
	}
	if _, ok := r.Lookup("generic/select", "1.0.0"); !ok {
		t.Fatal("registered generic node not found")
	}
	if err := r.Register(n); err == nil {
		t.Fatal("duplicate descriptor accepted")
	}
	bad := n
	bad.Descriptor.Effects = []string{"http:write"}
	bad.Descriptor.Deterministic = true
	bad.Descriptor.Name = "generic/effect"
	if err := r.Register(bad); err == nil {
		t.Fatal("effectful deterministic node accepted")
	}
}

func TestSelectMapAndTemplatePreserveFieldDiagnostics(t *testing.T) {
	input := []byte(`{"name":"coffee","count":2,"nested":{"ok":true}}`)
	selected, err := SelectMap(input, map[string]string{"label": "name", "flag": "nested.ok"})
	if err != nil || string(selected) != `{"flag":true,"label":"coffee"}` {
		t.Fatalf("selected=%s err=%v", selected, err)
	}
	if _, err := SelectMap(input, map[string]string{"x": "missing.value"}); err == nil || !strings.Contains(err.Error(), "missing.value") {
		t.Fatalf("missing field error: %v", err)
	}
	templated, err := Template("{{name}} x {{count}} {{missing}}", input, map[string]string{"missing": "default"})
	if err != nil || templated != "coffee x 2 default" {
		t.Fatalf("template=%q err=%v", templated, err)
	}
}

func TestValidationUsesPortableSchemaAndRejectsUnknownFields(t *testing.T) {
	s, err := schema.Parse(objectSchema)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Validate([]byte(`{"name":"coffee","count":"not-an-int"}`), s); err == nil {
		t.Fatal("invalid integer accepted")
	}
	if _, err := Validate([]byte(`{"name":"coffee","unknown":true}`), s); err == nil {
		t.Fatal("unknown field accepted")
	}
}

func TestExactIntegerOperationsDetectBothDirectionsOfOverflow(t *testing.T) {
	if _, err := AddInt64(math.MaxInt64, 1); err != ErrOverflow {
		t.Fatalf("add overflow: %v", err)
	}
	if _, err := AddInt64(math.MinInt64, -1); err != ErrOverflow {
		t.Fatalf("subtract overflow: %v", err)
	}
	if _, err := MultiplyInt64(math.MaxInt64, 2); err != ErrOverflow {
		t.Fatalf("multiply overflow: %v", err)
	}
	if _, err := MultiplyInt64(math.MinInt64, -1); err != ErrOverflow {
		t.Fatalf("min multiply overflow: %v", err)
	}
	if got, err := MultiplyInt64(1500, 2); err != nil || got != 3000 {
		t.Fatalf("valid money math: %d %v", got, err)
	}
}

func TestCollectionAndTemplateBounds(t *testing.T) {
	fields := make(map[string]string, MaxCollectionItems+1)
	for i := 0; i <= MaxCollectionItems; i++ {
		fields["x"+string(rune(i))] = "name"
	}
	if _, err := SelectMap([]byte(`{"name":"x"}`), fields); err != ErrBounds {
		t.Fatalf("collection bound: %v", err)
	}
	if _, err := Template(strings.Repeat("x", MaxTemplateBytes+1), []byte(`{}`), nil); err != ErrBounds {
		t.Fatalf("template bound: %v", err)
	}
}
