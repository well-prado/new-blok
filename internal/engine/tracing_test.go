package engine_test

import (
	"context"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type traceRecorder struct {
	mu     sync.Mutex
	events []inspection.Event
}

func (r *traceRecorder) Observe(event inspection.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

type payloadFree struct{ *traceRecorder }

func (payloadFree) ObservesPayloads() bool { return false }

type seenInput struct {
	Value string `json:"value"`
}

// tracedProgram runs two calls and an output. Each call records the trace
// context its ctx carried and logs once.
func tracedProgram(seen *[]observe.TraceContext, mu *sync.Mutex) (contract.InternalProgram, map[string]node.Any) {
	probe := node.MustDefine("trace/probe", "1.0.0", func(ctx context.Context, in seenInput) (seenInput, error) {
		trace, _ := observe.TraceFrom(ctx)
		mu.Lock()
		*seen = append(*seen, trace)
		mu.Unlock()
		node.Logger(ctx).Info("probe ran")
		return in, nil
	}, node.Description("trace probe"), node.Schemas([]byte(`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}`), []byte(`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}`)), node.Pure())
	program := contract.InternalProgram{WorkflowID: "traced", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "first", Kind: "call", Node: "trace/probe"},
		{Index: 1, ID: "second", Kind: "call", Node: "trace/probe", References: []contract.Reference{{Step: "first"}}},
		{Index: 2, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "second", Path: []string{"value"}}}},
	}}
	return program, map[string]node.Any{"trace/probe": probe.Any()}
}

func invocation() inspection.Invocation {
	return inspection.Invocation{RunID: "run-trace", Principal: "principal-synthetic-secret", Tenant: "tenant-a"}
}

func TestTracedRunAllocatesOneTraceAndHandsEachStepItsSpan(t *testing.T) {
	var seen []observe.TraceContext
	var mu sync.Mutex
	program, nodes := tracedProgram(&seen, &mu)
	recorder := &traceRecorder{}
	interpreter := engine.New(nodes).WithObserver(recorder).WithTracing(observe.TracePolicy{Ratio: 1})
	result, err := interpreter.RunObserved(context.Background(), program, seenInput{Value: "synthetic-payload"}, invocation())
	if err != nil || result.Output != "synthetic-payload" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	run := result.Trace
	if !run.TraceID.IsValid() || !run.SpanID.IsValid() || run.Parent.IsValid() || !run.Sampled() {
		t.Fatalf("run span %+v", run)
	}
	steps := map[string]observe.Span{}
	logs := 0
	for _, event := range recorder.events {
		if event.Tenant != "tenant-a" {
			t.Fatalf("%s lost the tenant: %+v", event.Kind, event)
		}
		if event.Trace.TraceID != run.TraceID {
			t.Fatalf("%s left the trace: %+v", event.Kind, event.Trace)
		}
		switch event.Kind {
		case inspection.RunStarted, inspection.RunCompleted:
			if event.Trace != run {
				t.Fatalf("%s carries %+v, want run span %+v", event.Kind, event.Trace, run)
			}
		case inspection.StepProcessing, inspection.StepCompleted:
			if event.Trace.Parent != run.SpanID || event.Trace.SpanID == run.SpanID {
				t.Fatalf("%s %s is not a child of the run: %+v", event.Kind, event.StepID, event.Trace)
			}
			if previous, ok := steps[event.StepID]; ok && previous != event.Trace {
				t.Fatalf("step %s changed span between processing and completion", event.StepID)
			}
			steps[event.StepID] = event.Trace
		case inspection.StepLog:
			logs++
			if event.Trace != steps[event.StepID] {
				t.Fatalf("log of %s carries %+v, want its step span", event.StepID, event.Trace)
			}
		}
	}
	if len(steps) != 3 || logs != 2 || steps["first"].SpanID == steps["second"].SpanID {
		t.Fatalf("steps=%+v logs=%d", steps, logs)
	}
	if len(seen) != 2 || seen[0] != steps["first"].TraceContext || seen[1] != steps["second"].TraceContext {
		t.Fatalf("nodes saw %+v, want their own step contexts %+v", seen, steps)
	}
}

func TestUntracedObservationCarriesNoSpanAndNodesSeeNone(t *testing.T) {
	var seen []observe.TraceContext
	var mu sync.Mutex
	program, nodes := tracedProgram(&seen, &mu)
	recorder := &traceRecorder{}
	result, err := engine.New(nodes).WithObserver(recorder).RunObserved(context.Background(), program, seenInput{Value: "v"}, invocation())
	if err != nil || result.Trace != (observe.Span{}) {
		t.Fatalf("result trace %+v err=%v", result.Trace, err)
	}
	for _, event := range recorder.events {
		if event.Trace != (observe.Span{}) {
			t.Fatalf("%s carries a span without a policy", event.Kind)
		}
	}
	for _, trace := range seen {
		if trace.Valid() {
			t.Fatal("a node saw a trace context without a policy")
		}
	}
	// Unobserved runs never trace, even with a policy.
	seen = nil
	if _, err := engine.New(nodes).WithTracing(observe.TracePolicy{Ratio: 1}).Run(context.Background(), program, seenInput{Value: "v"}); err != nil {
		t.Fatal(err)
	}
	for _, trace := range seen {
		if trace.Valid() {
			t.Fatal("an unobserved run handed nodes a trace context")
		}
	}
}

func TestChildRunJoinsTheStepThatStartedIt(t *testing.T) {
	var seen []observe.TraceContext
	var mu sync.Mutex
	program, nodes := tracedProgram(&seen, &mu)
	recorder := &traceRecorder{}
	interpreter := engine.New(nodes).WithObserver(recorder).WithTracing(observe.TracePolicy{Ratio: 0.000001})
	parent, err := observe.ParseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatal(err)
	}
	// A run started inside a traced step inherits that step's context.
	result, err := interpreter.RunObserved(observe.WithTrace(context.Background(), parent), program, seenInput{Value: "v"}, invocation())
	if err != nil {
		t.Fatal(err)
	}
	if result.Trace.TraceID != parent.TraceID || result.Trace.Parent != parent.SpanID || !result.Trace.Sampled() {
		t.Fatalf("child run %+v did not join parent %+v (it must follow the parent's decision, not its own ratio)", result.Trace, parent)
	}
	// An explicit trusted parent on the invocation wins over the context.
	explicit, _ := observe.ParseTraceparent("00-11111111111111111111111111111111-2222222222222222-00")
	identity := invocation()
	identity.Trace = explicit
	result, err = interpreter.RunObserved(observe.WithTrace(context.Background(), parent), program, seenInput{Value: "v"}, identity)
	if err != nil || result.Trace.TraceID != explicit.TraceID || result.Trace.Parent != explicit.SpanID || result.Trace.Sampled() {
		t.Fatalf("explicit parent ignored: %+v err=%v", result.Trace, err)
	}
	for _, trace := range seen[len(seen)-2:] {
		if trace.TraceID != explicit.TraceID || trace.Sampled() {
			t.Fatalf("node saw %+v after an unsampled explicit parent", trace)
		}
	}
}

func TestPayloadFreeObserversReceiveNoSerializedPayloads(t *testing.T) {
	var seen []observe.TraceContext
	var mu sync.Mutex
	program, nodes := tracedProgram(&seen, &mu)
	free := payloadFree{&traceRecorder{}}
	if _, err := engine.New(nodes).WithObserver(free).RunObserved(context.Background(), program, seenInput{Value: "synthetic-payload"}, invocation()); err != nil {
		t.Fatal(err)
	}
	full := &traceRecorder{}
	if _, err := engine.New(nodes).WithObserver(full).RunObserved(context.Background(), program, seenInput{Value: "synthetic-payload"}, invocation()); err != nil {
		t.Fatal(err)
	}
	if len(free.events) != len(full.events) {
		t.Fatalf("payload-free observer saw %d events, full saw %d", len(free.events), len(full.events))
	}
	payloads := 0
	for i, event := range free.events {
		if event.Input != nil || event.Output != nil {
			t.Fatalf("%s carried a payload to a payload-free observer", event.Kind)
		}
		if event.Kind != full.events[i].Kind {
			t.Fatalf("event %d kind %s, want %s", i, event.Kind, full.events[i].Kind)
		}
		if full.events[i].Input != nil || full.events[i].Output != nil {
			payloads++
		}
	}
	if payloads == 0 {
		t.Fatal("the default observer received no payloads; the comparison is vacuous")
	}
}

func TestTerminalEmittedByTheBoundaryKeepsTheRunSpan(t *testing.T) {
	var seen []observe.TraceContext
	var mu sync.Mutex
	program, nodes := tracedProgram(&seen, &mu)
	recorder := &traceRecorder{}
	interpreter := engine.New(nodes).WithObserver(recorder).WithTracing(observe.TracePolicy{Ratio: 1})
	result, err := interpreter.RunObservedPending(context.Background(), program, seenInput{Value: "v"}, invocation())
	if err != nil {
		t.Fatal(err)
	}
	interpreter.EmitRunTerminal(invocation(), program.WorkflowID, result, inspection.RunCompleted, "", "")
	last := recorder.events[len(recorder.events)-1]
	if last.Kind != inspection.RunCompleted || last.Trace != result.Trace || last.Tenant != "tenant-a" || !last.Trace.SpanID.IsValid() {
		t.Fatalf("boundary terminal %+v, run span %+v", last, result.Trace)
	}
}
