package engine_test

import (
	"context"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

// TestStepEventsMarkExternalCalls: every step event of a node that declares
// effects is marked External, and no event of a pure step or of the output
// is (ADR 0022). Exporters count external calls from this mark alone.
func TestStepEventsMarkExternalCalls(t *testing.T) {
	schema := []byte(`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}`)
	pure := node.MustDefine("ext/pure", "1.0.0", func(_ context.Context, in seenInput) (seenInput, error) { return in, nil }, node.Description("pure"), node.Schemas(schema, schema), node.Pure())
	effect := node.MustDefine("ext/charge", "1.0.0", func(_ context.Context, in seenInput) (seenInput, error) { return in, nil }, node.Description("charge"), node.Schemas(schema, schema), node.Effects("payments:charge"))
	program := contract.InternalProgram{WorkflowID: "external", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "validate", Kind: "call", Node: "ext/pure"},
		{Index: 1, ID: "charge", Kind: "call", Node: "ext/charge", References: []contract.Reference{{Step: "validate"}}},
		{Index: 2, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "charge", Path: []string{"value"}}}},
	}}
	recorder := &traceRecorder{}
	runner := engine.New(map[string]node.Any{"ext/pure": pure.Any(), "ext/charge": effect.Any()}).WithObserver(recorder)
	if _, err := runner.RunObserved(context.Background(), program, seenInput{Value: "x"}, invocation()); err != nil {
		t.Fatal(err)
	}
	external := map[string]int{}
	steps := map[string]int{}
	for _, event := range recorder.events {
		if event.StepID == "" {
			if event.External {
				t.Fatalf("run event %s marked external", event.Kind)
			}
			continue
		}
		steps[event.StepID]++
		if event.External {
			external[event.StepID]++
		}
	}
	if external["charge"] != steps["charge"] || external["charge"] != 2 || external["validate"] != 0 || external["respond"] != 0 {
		t.Fatalf("external marks %v of step events %v; want both charge events only", external, steps)
	}
}
