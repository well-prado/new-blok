package trigger

import (
	"context"
	"errors"
	"testing"

	"github.com/well-prado/new-blok/contract/observe"
)

const traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestTraceIngressZeroValueExtractsNothing(t *testing.T) {
	traced := observe.TracePolicy{Ratio: 1}
	if parent, ok := (TraceIngress{}).Parent(traced, []string{traceparent}, []string{"a=1"}); ok || parent != (observe.TraceContext{}) {
		t.Fatalf("the zero ingress extracted %+v", parent)
	}
	ctx := context.Background()
	if got := (TraceIngress{}).Context(ctx, traced, []string{traceparent}, nil); got != ctx {
		t.Fatal("the zero ingress changed the context")
	}
	// Honoring the caller's sampling is not opting in to extraction.
	if _, ok := (TraceIngress{Sampling: observe.HonorInboundSampling}).Parent(traced, []string{traceparent}, nil); ok {
		t.Fatal("a sampling policy without Extract extracted")
	}
}

func TestTraceIngressHandsTheParentToTheRunContext(t *testing.T) {
	ingress := TraceIngress{Extract: true, Sampling: observe.HonorInboundSampling}
	ctx := ingress.Context(context.Background(), observe.TracePolicy{Ratio: 1}, []string{traceparent}, []string{"a=1"})
	parent, ok := observe.TraceFrom(ctx)
	if !ok || parent.Traceparent() != traceparent || parent.State != "a=1" {
		t.Fatalf("context carries %+v ok=%v", parent, ok)
	}
	for name, policy := range map[string]observe.TracePolicy{"untraced": {}} {
		if _, ok := observe.TraceFrom(ingress.Context(context.Background(), policy, []string{traceparent}, nil)); ok {
			t.Fatalf("%s application received a trace", name)
		}
	}
	if _, ok := observe.TraceFrom(ingress.Context(context.Background(), observe.TracePolicy{Ratio: 1}, []string{traceparent, traceparent}, nil)); ok {
		t.Fatal("a duplicated traceparent was used")
	}
}

func TestTraceIngressValidateRejectsUndefinedSampling(t *testing.T) {
	var invalid *Error
	if err := (TraceIngress{Extract: true, Sampling: 9}).Validate(); !errors.As(err, &invalid) || invalid.Code != "invalid_trace_ingress" {
		t.Fatalf("err=%v", err)
	}
	for _, sampling := range []observe.InboundSampling{observe.IgnoreInboundSampling, observe.HonorInboundSampling} {
		if err := (TraceIngress{Extract: true, Sampling: sampling}).Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
