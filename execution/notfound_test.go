package execution_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/redact"
	"github.com/well-prado/new-blok/trigger"
)

// outcomes records every terminal outcome the runner writes.
type outcomes struct {
	mu    sync.Mutex
	calls []string
}

func (o *outcomes) add(call string) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.calls = append(o.calls, call)
	return nil
}
func (o *outcomes) CompleteRun(context.Context, inspection.Invocation, json.RawMessage) error {
	return o.add("complete")
}
func (o *outcomes) FailRun(_ context.Context, _ inspection.Invocation, code, class string) error {
	return o.add("fail " + code + " " + class)
}
func (o *outcomes) CancelRun(context.Context, inspection.Invocation, string) error {
	return o.add("cancel")
}
func (o *outcomes) MarkRunUncertain(_ context.Context, _ inspection.Invocation, code, class string) error {
	return o.add("uncertain " + code + " " + class)
}

type events struct {
	mu   sync.Mutex
	seen []inspection.Event
}

func (e *events) Observe(event inspection.Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen = append(e.seen, event)
}

type lookup struct {
	ID string `json:"id"`
}

type found struct {
	ID string `json:"id"`
}

// TestNotFoundRunIsTerminalNotRetriedNorUncertain covers #306's retry and
// telemetry rules for the class, through the real engine and runner. A
// not-found node failure, from a pure node and from one that declares
// effects, fails its run once: the node runs once, the terminal outcome is
// FailRun with the code and class (never MarkRunUncertain, which callers must
// reconcile, nor a cancellation), and the step and run events carry the
// class label "not_found", which the inspection projection (#80) keeps and
// the SLO/OTel exporters (#105) accept as a bounded label.
func TestNotFoundRunIsTerminalNotRetriedNorUncertain(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options []node.Option
	}{
		{"pure node", []node.Option{node.Pure()}},
		{"effectful node", []node.Option{node.Effects("db.read")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			invocations := 0
			options := append([]node.Option{node.Description("Synthetic lookup"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))}, tc.options...)
			definition, err := node.Define("records/get", "1.0.0", func(context.Context, lookup) (found, error) {
				invocations++
				return found{}, &node.DomainError{Code: "not_found", Class: node.ClassNotFound, Err: errors.New("synthetic: belongs to another principal")}
			}, options...)
			if err != nil {
				t.Fatal(err)
			}
			workflow, err := flow.Define(flow.Spec{Name: "records-get", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[lookup]) flow.Ref[found] {
				return flow.Call(builder, "get", definition, input)
			})
			if err != nil {
				t.Fatal(err)
			}
			program, err := workflow.Lower()
			if err != nil {
				t.Fatal(err)
			}
			written, observed := &outcomes{}, &events{}
			application, err := app.New(app.Config{RunOutcomes: written, Inspection: observed})
			if err != nil {
				t.Fatal(err)
			}
			if err := application.Start(context.Background()); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
			runner := execution.NewRunner(application, map[string]node.Any{"records/get": definition.Any()})
			_, runErr := runner.Run(context.Background(), program, lookup{ID: "r-alice"}, inspection.Invocation{RunID: "run-1", Principal: "bob", AttemptID: "attempt-1"})
			code, class, ok := trigger.Classify(runErr)
			if !ok || code != "not_found" || class != trigger.ClassNotFound {
				t.Fatalf("run error %v classified %q/%q/%v; want not_found/not_found", runErr, code, class, ok)
			}
			var uncertain interface{ IsUncertain() bool }
			if errors.As(runErr, &uncertain) && uncertain.IsUncertain() {
				t.Fatalf("a not-found failure reads as uncertain: %v", runErr)
			}
			if invocations != 1 || len(written.calls) != 1 || written.calls[0] != "fail not_found not_found" {
				t.Fatalf("invocations=%d outcomes=%v; want one invocation and one FailRun not_found not_found", invocations, written.calls)
			}
			kinds := map[inspection.Kind]string{}
			for _, event := range observed.seen {
				if event.ErrorClass != "" {
					kinds[event.Kind] = event.ErrorClass
				}
			}
			if kinds[inspection.StepFailed] != "not_found" || kinds[inspection.RunFailed] != "not_found" || len(kinds) != 2 {
				t.Fatalf("classified events %v; want step.failed and run.failed with class not_found", kinds)
			}
		})
	}
	if redact.Sensitive(node.ClassNotFound) || !observe.ValidLabel(node.ClassNotFound) {
		t.Fatal("the class label would be redacted or refused as a telemetry label")
	}
	for _, spelled := range []string{node.ClassNotFound, trigger.ClassNotFound} {
		if spelled != "not_found" {
			t.Fatalf("node %q and trigger %q must both spell the wire class not_found", node.ClassNotFound, trigger.ClassNotFound)
		}
	}
}
