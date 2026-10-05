package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/well-prado/new-blok/contract/observe"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/schema"
	"strings"
	"testing"
)

func TestEffectTransportFailurePreservesKnownCauseBeforeContextTimer(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		ctx := context.Background() // Its timer has not observed the deadline.
		failure := transportFailure(ctx, fmt.Errorf("synthetic-sensitive-detail: %w", cause))
		if ctx.Err() != nil || !errors.Is(failure, cause) || !failure.Uncertain || failure.Class != "uncertain" {
			t.Fatalf("lost known cause: %v", failure)
		}
		if strings.Contains(failure.Error(), "synthetic-sensitive-detail") {
			t.Fatal("transport detail leaked")
		}
	}
	failure := transportFailure(context.Background(), errors.New("synthetic-sensitive-detail"))
	if failure.Err != nil || errors.Is(failure, context.DeadlineExceeded) || strings.Contains(failure.Error(), "synthetic-sensitive-detail") {
		t.Fatal("unrelated transport failure was reclassified or leaked")
	}
}

func TestNativeJSONConvertsInt64InsideNestedUnion(t *testing.T) {
	s, err := schema.Parse([]byte(`{"anyOf":[{"type":"object","properties":{"amount":{"type":"integer","wire":"int64-string"}},"required":["amount"]},{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"-9223372036854775808", "9223372036854775807"} {
		wire, err := s.Normalize([]byte(`{"amount":"` + value + `"}`))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := nativeJSON(s, wire)
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Amount int64 `json:"amount"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("typed union decode: %s %v", raw, err)
		}
		if string(raw) != `{"amount":`+value+`}` {
			t.Fatalf("lost exact integer: %s", raw)
		}
	}
}

func TestTracePropagationNeverChangesWhetherACallFits(t *testing.T) {
	parent, err := observe.ParseTraceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if err != nil {
		t.Fatal(err)
	}
	parent.State = "vendor=" + strings.Repeat("v", 200)
	base := contract.Call{CallID: "call", AttemptID: "attempt", Node: "fixture/echo", NodeVersion: "1.0.0", Generation: 1, Input: []byte(`{}`)}

	untraced := base
	propagateTrace(context.Background(), &untraced, contract.MaxFrameBytes)
	if untraced.Traceparent != "" || untraced.Tracestate != "" {
		t.Fatalf("a call without a trace context gained one: %+v", untraced)
	}

	traced := base
	propagateTrace(observe.WithTrace(context.Background(), parent), &traced, contract.MaxFrameBytes)
	if traced.Traceparent != parent.Traceparent() || traced.Tracestate != parent.State {
		t.Fatalf("trace context not propagated: %+v", traced)
	}

	// A call that fits only without the trace context keeps fitting: the
	// optional context is omitted rather than the call refused.
	edge := base
	limit := contract.EncodedCallBytes(edge)
	propagateTrace(observe.WithTrace(context.Background(), parent), &edge, limit)
	if edge.Traceparent != "" || edge.Tracestate != "" || contract.EncodedCallBytes(edge) > limit {
		t.Fatalf("trace context pushed an admissible call over the frame bound: %+v", edge)
	}
}
