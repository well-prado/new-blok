package trigger

import (
	"context"
	"fmt"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/observe/redact"
)

// The W3C Trace Context carrier field names. HTTP header lookups
// canonicalize them; gRPC metadata keys are already lower case.
const (
	TraceparentField = "traceparent"
	TracestateField  = "tracestate"
)

// TraceIngress is an adapter's opt-in policy for an inbound W3C trace
// context (ADR 0020). The zero value extracts nothing: an inbound
// traceparent is caller data, and joining a caller's trace is the
// application's decision.
//
// When Extract is set and the application traces (its TracePolicy is
// enabled), the adapter reads traceparent/tracestate after it has admitted,
// authenticated and validated the work, and hands the context to the run
// as its parent: through the handler's context for in-band work, and with
// the durable submission (Submission.Trace) for queued work. A malformed,
// oversized or duplicated carrier is ignored (observe.ExtractTrace); it
// never refuses work. The context never reaches admission, authorization,
// idempotency keys, payload digests or routing: every one of those is
// decided before it is read, and none reads it.
type TraceIngress struct {
	Extract bool
	// Sampling says whether the inbound sampled flag is honored or the
	// application's own ratio decides (the default).
	Sampling observe.InboundSampling
}

// Validate rejects an undefined sampling policy.
func (t TraceIngress) Validate() error {
	if !t.Sampling.Valid() {
		return &Error{Code: "invalid_trace_ingress", Path: "trace.sampling", Message: fmt.Sprintf("unknown inbound sampling policy %d", t.Sampling)}
	}
	return nil
}

// Parent returns the trace context a run admitted with these carrier values
// joins under policy, or false when extraction is off, the application
// does not trace, or the carrier holds no usable context.
func (t TraceIngress) Parent(policy observe.TracePolicy, traceparent, tracestate []string) (observe.TraceContext, bool) {
	if !t.Extract {
		return observe.TraceContext{}, false
	}
	remote, ok := observe.ExtractTrace(traceparent, tracestate)
	if !ok {
		return observe.TraceContext{}, false
	}
	parent, ok := policy.Inbound(remote, t.Sampling)
	if !ok {
		return observe.TraceContext{}, false
	}
	parent.State = SafeTracestate(parent.State)
	return parent, true
}

// SafeTracestate returns state without any list member the redaction
// boundary (observe/redact, ADR 0021) flags, as written or in an encoded
// form: a caller's tracestate is forwarded to every worker call and to the
// exporter, so a credential in it must not survive admission. Every place
// that stores or hands on a trace context received from outside a run uses
// it (#285 review F1).
func SafeTracestate(state string) string {
	return observe.FilterTracestate(state, redact.Sensitive)
}

// Context returns ctx carrying Parent's result as its active trace context
// (observe.WithTrace), or ctx itself when there is none. A run started with
// that context and no explicit inspection.Invocation.Trace joins it.
func (t TraceIngress) Context(ctx context.Context, policy observe.TracePolicy, traceparent, tracestate []string) context.Context {
	if parent, ok := t.Parent(policy, traceparent, tracestate); ok {
		return observe.WithTrace(ctx, parent)
	}
	return ctx
}
