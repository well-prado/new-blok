package catalog

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

func TestBuiltinsArePureAndCustomNodesShareRegistry(t *testing.T) {
	registry := NewRegistry()
	builtins := []node.Any{Validate().Any(), Map().Any(), Template().Any(), AddInteger().Any(), AddMoney().Any()}
	for _, builtin := range builtins {
		if err := registry.Register(builtin); err != nil {
			t.Fatal(err)
		}
		if descriptor := builtin.Descriptor(); !descriptor.Deterministic || len(descriptor.Effects) != 0 {
			t.Fatalf("builtin %s is not truthfully pure: %+v", descriptor.Name, descriptor)
		}
	}
	custom := node.MustDefine("custom/trim", "1.0.0", func(_ context.Context, value string) (string, error) { return strings.TrimSpace(value), nil }, node.Description("trim"), node.Schemas([]byte(`{"type":"string"}`), []byte(`{"type":"string"}`)), node.Pure())
	if err := registry.Register(custom.Any()); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Lookup("custom/trim", "1.0.0"); !ok {
		t.Fatal("custom descriptor was not registered through the shared registry")
	}
}

func TestValidatePreservesFieldDiagnosticsAndDefaults(t *testing.T) {
	input := ValidateInput{
		Schema: json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"},"count":{"type":"integer","default":2}},"required":["name"],"additionalProperties":false}`),
		Value:  json.RawMessage(`{"name":7}`),
	}
	_, err := Validate().Invoke(context.Background(), input)
	if err == nil || !strings.Contains(err.Error(), "$.name") {
		t.Fatalf("error=%v, want field path", err)
	}
	input.Value = json.RawMessage(`{"name":"blok"}`)
	got, err := Validate().Invoke(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Value) != `{"count":2,"name":"blok"}` && string(got.Value) != `{"name":"blok","count":2}` {
		t.Fatalf("normalized=%s", got.Value)
	}
}

func TestMapTemplateBoundsAndImmutability(t *testing.T) {
	mapped, err := Map().Invoke(context.Background(), MapInput{
		Value:  json.RawMessage(`{"customer":{"id":"c-1"},"items":[{"sku":"coffee"}]}`),
		Fields: map[string]string{"id": "customer.id", "sku": "items[0].sku"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(mapped.Value) != `{"id":"c-1","sku":"coffee"}` {
		t.Fatalf("mapped=%s", mapped.Value)
	}
	values := map[string]string{"name": "Blok"}
	rendered, err := Template().Invoke(context.Background(), TemplateInput{Template: "Hello {{name}}", Values: values})
	if err != nil || rendered.Value != "Hello Blok" {
		t.Fatalf("rendered=%+v err=%v", rendered, err)
	}
	values["name"] = "changed"
	if rendered.Value != "Hello Blok" {
		t.Fatal("template output aliases input")
	}
	_, err = Template().Invoke(context.Background(), TemplateInput{Template: strings.Repeat("x", MaxTemplateBytes+1)})
	if err == nil || !strings.Contains(err.Error(), "template_limit") {
		t.Fatalf("err=%v, want bounded template failure", err)
	}
}

func TestExactIntegerAndMoneyOverflow(t *testing.T) {
	for _, definition := range []struct {
		name string
		call func() (IntegerOutput, error)
	}{
		{"integer", func() (IntegerOutput, error) {
			return AddInteger().Invoke(context.Background(), IntegerInput{Left: math.MaxInt64, Right: 1})
		}},
		{"money", func() (IntegerOutput, error) {
			return AddMoney().Invoke(context.Background(), IntegerInput{Left: math.MinInt64, Right: -1})
		}},
	} {
		t.Run(definition.name, func(t *testing.T) {
			if _, err := definition.call(); err == nil || !strings.Contains(err.Error(), "integer_overflow") {
				t.Fatalf("err=%v", err)
			}
		})
	}
	got, err := AddMoney().Invoke(context.Background(), IntegerInput{Left: 1999, Right: 1001})
	if err != nil || got.Value != 3000 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestCatalogNodeComposesStructurallyInWorkflow(t *testing.T) {
	definition, err := flow.Define(flow.Spec{Name: "catalog-example", Version: "1.0.0", Durability: flow.Memory}, func(builder *flow.Builder, input flow.Ref[ValidateInput]) flow.Ref[ValidateOutput] {
		return flow.Call(builder, "validate", Validate(), input)
	})
	if err != nil {
		t.Fatal(err)
	}
	program := definition.Program()
	if len(program.Instructions) != 1 || program.Instructions[0].Node.Name != "catalog/validate" {
		t.Fatalf("program=%+v", program)
	}
	lowered, err := definition.Lower()
	if err != nil {
		t.Fatal(err)
	}
	if len(lowered.Instructions) != 2 || lowered.Instructions[1].Kind != "output" {
		t.Fatalf("lowered=%+v", lowered)
	}
}

func FuzzTemplateBounded(f *testing.F) {
	f.Add("Hello {{name}}", "Blok")
	f.Fuzz(func(t *testing.T, template, name string) {
		if len(template) > MaxTemplateBytes {
			t.Skip()
		}
		_, _ = Template().Invoke(context.Background(), TemplateInput{Template: template, Values: map[string]string{"name": name}})
	})
}
