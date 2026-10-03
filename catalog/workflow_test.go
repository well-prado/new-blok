package catalog

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/flowtest"
	"github.com/well-prado/new-blok/node"
)

func runCatalog[I, O any](t *testing.T, n node.Definition[I, O], input I) flowtest.Result {
	t.Helper()
	wf, err := flow.Define(flow.Spec{Name: "catalog-regression", Version: "1.0.0", Durability: flow.Memory}, func(b *flow.Builder, entry flow.Ref[I]) flow.Ref[O] {
		return flow.Call(b, "operation", n, entry)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := wf.Lower()
	if err != nil {
		t.Fatal(err)
	}
	return flowtest.Run(context.Background(), program, map[string]node.Any{n.Descriptor().Name: n.Any()}, input, flowtest.Options{})
}

func TestValidateFixturesThroughRealWorkflow(t *testing.T) {
	raw, err := os.ReadFile("../testdata/catalog/normalization-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		ID, Error                string
		Schema, Value, Expected  json.RawMessage
		Outputs, Errors, Effects int
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		t.Run(fixture.ID, func(t *testing.T) {
			run := runCatalog(t, Validate(), ValidateInput{Schema: fixture.Schema, Value: fixture.Value})
			outputs, errors := 0, 0
			if run.Err() != nil {
				errors++
				if fixture.Error == "" || !strings.Contains(run.Err().Error(), fixture.Error) {
					t.Fatalf("error=%v, expected=%s", run.Err(), fixture.Error)
				}
				if _, ok := run.State("operation"); ok || run.Response() != nil {
					t.Fatal("failed validation published output")
				}
			} else {
				outputs++
				output, ok := run.Response().(ValidateOutput)
				if !ok || string(output.Value) != string(fixture.Expected) {
					t.Fatalf("output=%+v, expected=%s", run.Response(), fixture.Expected)
				}
				step, ok := run.Step("operation")
				if !ok || !step.Executed || step.Error != nil {
					t.Fatalf("step=%+v", step)
				}
				if _, ok := run.State("operation"); !ok {
					t.Fatal("successful validation did not commit output")
				}
			}
			if outputs != fixture.Outputs || errors != fixture.Errors || fixture.Effects != 0 {
				t.Fatalf("counts=%d/%d/0, expected=%d/%d/%d", outputs, errors, fixture.Outputs, fixture.Errors, fixture.Effects)
			}
		})
	}
}

func TestMapThroughRealWorkflow(t *testing.T) {
	for _, input := range []MapInput{
		{Value: json.RawMessage(`{"name":"blok","n":9223372036854775807,"optional":null}`), Fields: map[string]string{"name": "name", "n": "n", "optional": "optional"}},
		{Value: json.RawMessage(`["blok"]`), Fields: map[string]string{"name": "[0]"}},
	} {
		run := runCatalog(t, Map(), input)
		if !run.OK() {
			t.Fatal(run.Err())
		}
		output := run.Response().(MapOutput)
		if !json.Valid(output.Value) || !strings.Contains(string(output.Value), `"name":"blok"`) {
			t.Fatalf("output=%s", output.Value)
		}
		if len(input.Fields) > 1 && string(output.Value) != `{"n":9223372036854775807,"name":"blok","optional":null}` {
			t.Fatalf("mapping changed exact integer/null: %s", output.Value)
		}
	}
}

func TestTemplateBudgetThroughRealWorkflow(t *testing.T) {
	for _, tc := range []struct {
		name, template, value, expected, error string
	}{
		{"literal-boundary", strings.Repeat("x", MaxTemplateBytes), "", strings.Repeat("x", MaxTemplateBytes), ""},
		{"replacement-boundary", "{{x}}", strings.Repeat("x", MaxTemplateBytes), strings.Repeat("x", MaxTemplateBytes), ""},
		{"repeated-overflow", strings.Repeat("{{x}}", MaxTemplateBytes/5), strings.Repeat("x", MaxTemplateBytes), "", "template_limit"},
		{"literal-plus-replacement", "x{{x}}", strings.Repeat("x", MaxTemplateBytes), "", "template_limit"},
		{"utf8-bytes", "{{x}}{{x}}", strings.Repeat("é", MaxTemplateBytes/2), "", "template_limit"},
		{"missing", "{{missing}}", "", "", "missing_field"},
		{"no-recursive-expansion", "{{x}}", "{{missing}}", "{{missing}}", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := runCatalog(t, Template(), TemplateInput{Template: tc.template, Values: map[string]string{"x": tc.value}})
			if tc.error != "" {
				if run.OK() || !strings.Contains(run.Err().Error(), tc.error) {
					t.Fatalf("error=%v, expected=%s", run.Err(), tc.error)
				}
				if _, ok := run.State("operation"); ok || run.Response() != nil {
					t.Fatal("failed render published output")
				}
			} else if !run.OK() || run.Response().(TemplateOutput).Value != tc.expected {
				t.Fatalf("error=%v, output=%+v", run.Err(), run.Response())
			}
		})
	}
}

func TestTemplateRejectsAmplificationBeforeOutputAllocation(t *testing.T) {
	// These bounded inputs would expand to 8 MiB if rendered before rejection.
	input := TemplateInput{Template: strings.Repeat("{{x}}", 2048), Values: map[string]string{"x": strings.Repeat("x", 4096)}}
	n := Template()
	measurement := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := n.Invoke(context.Background(), input); err == nil {
				b.Fatal("amplification accepted")
			}
		}
	})
	// Includes bounded regexp scanning scratch (larger with race instrumentation),
	// but excludes even a small fraction of the rejected expanded output.
	const rejectionBudget = 4 * MaxTemplateBytes
	if allocated := measurement.AllocedBytesPerOp(); allocated > rejectionBudget {
		t.Fatalf("overflow rejection allocated %d bytes, budget=%d", allocated, rejectionBudget)
	}
}
