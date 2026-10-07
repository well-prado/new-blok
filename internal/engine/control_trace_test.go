package engine

import (
	"context"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
)

// A step inside an arm is a child span of its construct's span; the
// construct's own span is a child of the run's (Review R round 1 on #379).
func TestArmStepSpansHangOffTheirConstruct(t *testing.T) {
	program := controlProgram(
		contract.InternalInstruction{ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{literal("true")}, Arms: []contract.Arm{
			{Name: "then", Instructions: []contract.InternalInstruction{compare("inner", "eq", literal("1"), literal("1"))}, Output: ptr(ref("inner"))},
			{Name: "else", Output: ptr(literal("false"))},
		}}},
		outputOf("route"),
	)
	log := &eventLog{}
	result, err := New(nil).WithObserver(log).WithTracing(observe.TracePolicy{Ratio: 1}).RunObserved(context.Background(), program, nil, inspection.Invocation{RunID: "run-1", Principal: "p"})
	if err != nil {
		t.Fatal(err)
	}
	spans := map[string]observe.Span{}
	for _, event := range log.events {
		if event.Kind == inspection.StepCompleted {
			spans[event.StepID] = event.Trace
		}
	}
	route, inner := spans["route"], spans["inner"]
	if !route.SpanID.IsValid() || route.Parent != result.Trace.SpanID {
		t.Fatalf("construct span %+v is not a child of the run %+v", route, result.Trace)
	}
	if !inner.SpanID.IsValid() || inner.Parent != route.SpanID {
		t.Fatalf("arm step span %+v is not a child of its construct %+v", inner, route)
	}
}
