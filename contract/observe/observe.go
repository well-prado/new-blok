// Package observe is the narrow, transport-neutral observation port the
// engine shares with optional telemetry exporters (ADR 0020).
//
// It defines W3C trace context values, the context carrier that hands a
// step's trace context to the node it invokes, the head-sampling policy an
// application selects, and the bounded label rule exporters apply. It
// imports only the standard library: it has no network, listener, provider
// or exporter code, and nothing registers itself globally. Exporters live
// outside the engine's dependency graph (observe/otel is a separate module).
package observe

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"regexp"
)

const (
	// MaxTracestateBytes bounds the vendor tracestate carried with a trace
	// context. W3C allows 512; the framework keeps half so a worker call's
	// fixed metadata stays small.
	MaxTracestateBytes = 256
	// MaxLabelBytes bounds one telemetry label value.
	MaxLabelBytes    = 64
	traceparentBytes = 55
)

var (
	ErrInvalidTraceContext = errors.New("observe: invalid trace context")
	ErrInvalidTracePolicy  = errors.New("observe: invalid trace policy")
)

// TraceID is a W3C trace id. The zero value is invalid.
type TraceID [16]byte

// SpanID is a W3C parent/span id. The zero value is invalid.
type SpanID [8]byte

func (id TraceID) IsValid() bool  { return id != TraceID{} }
func (id SpanID) IsValid() bool   { return id != SpanID{} }
func (id TraceID) String() string { return hex.EncodeToString(id[:]) }
func (id SpanID) String() string  { return hex.EncodeToString(id[:]) }

// TraceFlags are the W3C trace flags. Only FlagSampled is defined.
type TraceFlags byte

const FlagSampled TraceFlags = 0x01

func (f TraceFlags) Sampled() bool { return f&FlagSampled != 0 }

// TraceContext is one W3C trace context: the identity a callee records as
// its parent. State is the opaque vendor tracestate, at most
// MaxTracestateBytes; it is correlation data and never authority.
type TraceContext struct {
	TraceID TraceID
	SpanID  SpanID
	Flags   TraceFlags
	State   string
}

// Valid reports whether both ids are non-zero and State is well formed.
func (c TraceContext) Valid() bool {
	return c.TraceID.IsValid() && c.SpanID.IsValid() && ValidTracestate(c.State)
}

// Sampled reports whether the trace is recorded.
func (c TraceContext) Sampled() bool { return c.Flags.Sampled() }

// Traceparent formats the W3C version-00 traceparent header value.
func (c TraceContext) Traceparent() string {
	return fmt.Sprintf("00-%s-%s-%02x", c.TraceID, c.SpanID, byte(c.Flags))
}

// ParseTraceparent parses a W3C traceparent value. Version 00 must be
// exactly 55 bytes; a later version may append fields after a '-', which
// are ignored as the specification requires. Version ff, upper-case hex and
// all-zero ids are rejected.
func ParseTraceparent(value string) (TraceContext, error) {
	if len(value) < traceparentBytes || value[2] != '-' || value[35] != '-' || value[52] != '-' {
		return TraceContext{}, ErrInvalidTraceContext
	}
	version, ok := lowerHex(value[0:2])
	if !ok || version[0] == 0xff {
		return TraceContext{}, ErrInvalidTraceContext
	}
	if version[0] == 0 && len(value) != traceparentBytes {
		return TraceContext{}, ErrInvalidTraceContext
	}
	if len(value) > traceparentBytes && value[traceparentBytes] != '-' {
		return TraceContext{}, ErrInvalidTraceContext
	}
	traceID, okTrace := lowerHex(value[3:35])
	spanID, okSpan := lowerHex(value[36:52])
	flags, okFlags := lowerHex(value[53:55])
	if !okTrace || !okSpan || !okFlags {
		return TraceContext{}, ErrInvalidTraceContext
	}
	var out TraceContext
	copy(out.TraceID[:], traceID)
	copy(out.SpanID[:], spanID)
	out.Flags = TraceFlags(flags[0])
	if !out.TraceID.IsValid() || !out.SpanID.IsValid() {
		return TraceContext{}, ErrInvalidTraceContext
	}
	return out, nil
}

func lowerHex(text string) ([]byte, bool) {
	for i := 0; i < len(text); i++ {
		c := text[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return nil, false
		}
	}
	decoded, err := hex.DecodeString(text)
	return decoded, err == nil
}

// ValidTracestate reports whether value is empty or a bounded list of
// printable ASCII members. It checks size and character set, not vendor
// key grammar: tracestate is forwarded, never interpreted.
func ValidTracestate(value string) bool {
	if len(value) > MaxTracestateBytes {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return false
		}
	}
	return true
}

// Span is the trace position an observation describes: its own context and
// the span it is a child of (zero for a root).
type Span struct {
	TraceContext
	Parent SpanID
}

// Child returns a new span in the same trace whose parent is s. It keeps the
// sampling decision and tracestate: one trace has one decision.
func (s Span) Child() Span {
	return Span{TraceContext: TraceContext{TraceID: s.TraceID, SpanID: NewSpanID(), Flags: s.Flags, State: s.State}, Parent: s.SpanID}
}

type traceKey struct{}

// WithTrace returns ctx carrying c as the active trace context. The engine
// sets it on the context of each node it invokes while tracing, so a native
// node, a worker adapter or a child run started from that step can
// propagate it. An invalid context is not stored.
func WithTrace(ctx context.Context, c TraceContext) context.Context {
	if !c.Valid() {
		return ctx
	}
	return context.WithValue(ctx, traceKey{}, c)
}

// TraceFrom returns the active trace context, if any.
func TraceFrom(ctx context.Context) (TraceContext, bool) {
	if ctx == nil {
		return TraceContext{}, false
	}
	c, ok := ctx.Value(traceKey{}).(TraceContext)
	return c, ok && c.Valid()
}

// TracePolicy is the application's head-sampling decision. It is explicit:
// the zero value disables tracing, so no trace context is created or
// propagated and observations carry no span.
//
//   - A run with no parent starts a new trace and is sampled when the trace
//     id falls under Ratio (deterministic, as OpenTelemetry's
//     TraceIDRatioBased: every process applying the same ratio to the same
//     trace id agrees).
//   - A run with a parent (a child run started from a traced step, or a
//     remote parent the application chose to trust) joins that trace and
//     keeps the parent's sampled flag.
//
// Unsampled traces still propagate (flag 00), so downstream workers and
// child runs agree not to record them. Sampling never affects metrics,
// validation, approval, journaling or the run's outcome.
type TracePolicy struct {
	Ratio float64
}

// Validate rejects NaN and ratios outside [0, 1].
func (p TracePolicy) Validate() error {
	if math.IsNaN(p.Ratio) || p.Ratio < 0 || p.Ratio > 1 {
		return fmt.Errorf("%w: ratio %v is outside [0, 1]", ErrInvalidTracePolicy, p.Ratio)
	}
	return nil
}

// Enabled reports whether runs are traced at all.
func (p TracePolicy) Enabled() bool { return p.Validate() == nil && p.Ratio > 0 }

// Samples reports the deterministic root decision for id.
func (p TracePolicy) Samples(id TraceID) bool {
	if !p.Enabled() {
		return false
	}
	if p.Ratio >= 1 {
		return true
	}
	bound := uint64(p.Ratio * (1 << 63))
	return binary.BigEndian.Uint64(id[8:16])>>1 < bound
}

// Root returns the run span for a run whose parent is parent (zero for
// none), or a zero Span when tracing is disabled.
func (p TracePolicy) Root(parent TraceContext) Span {
	if !p.Enabled() {
		return Span{}
	}
	if parent.Valid() {
		return Span{TraceContext: TraceContext{TraceID: parent.TraceID, SpanID: NewSpanID(), Flags: parent.Flags & FlagSampled, State: parent.State}, Parent: parent.SpanID}
	}
	id := NewTraceID()
	var flags TraceFlags
	if p.Samples(id) {
		flags = FlagSampled
	}
	return Span{TraceContext: TraceContext{TraceID: id, SpanID: NewSpanID(), Flags: flags}}
}

// NewTraceID returns a random non-zero trace id. Ids correlate; they are not
// secrets, so a fast cryptographically seeded generator suffices.
func NewTraceID() TraceID {
	for {
		var id TraceID
		binary.BigEndian.PutUint64(id[0:8], rand.Uint64())
		binary.BigEndian.PutUint64(id[8:16], rand.Uint64())
		if id.IsValid() {
			return id
		}
	}
}

// NewSpanID returns a random non-zero span id.
func NewSpanID() SpanID {
	for {
		var id SpanID
		binary.BigEndian.PutUint64(id[:], rand.Uint64())
		if id.IsValid() {
			return id
		}
	}
}

// PayloadObserver is implemented by an inspection observer to say whether it
// reads observation payloads (Event.Input and Event.Output). When every
// selected observer reports false the engine skips serializing them, so a
// production exporter neither pays for nor can leak business payloads.
// Observers that do not implement it are assumed to read payloads.
type PayloadObserver interface {
	ObservesPayloads() bool
}

// ValidLabel reports whether value may be used as a telemetry label: 1 to
// MaxLabelBytes of ASCII letters, digits and '_', '-', '.', '/', ':'. It is
// a shape rule; exporters still bound how many distinct values they keep.
func ValidLabel(value string) bool {
	if value == "" || len(value) > MaxLabelBytes {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.' || c == '/' || c == ':') {
			return false
		}
	}
	return true
}

// sensitiveLogText matches credential-shaped free text. It is the single
// pattern shared by inspection projections, worker logs and telemetry
// exporters; observe/redact adds the structured and encoded layers on top of
// it (ADR 0021). A sensitive key may carry a prefix or suffix
// ("client_secret", "access_token") and may be quoted, as in JSON text.
var sensitiveLogText = regexp.MustCompile(`(?i)([a-z0-9_.-]*(password|passwd|secret|token|authorization|credential|api[_-]?key|private[_-]?key)[a-z0-9_.-]*["']?\s*[:=]\s*\S+|\bbearer\s+\S+|\bAKIA[0-9A-Z]{16}\b|\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b|\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----)`)

// SensitiveText reports whether text contains a credential-shaped fragment.
// It inspects the text as written; observe/redact also inspects encoded forms.
func SensitiveText(text string) bool { return sensitiveLogText.MatchString(text) }

// RedactLogMessage replaces a log message that contains credential-shaped
// text with a fixed marker. Pattern matching cannot find every secret in
// prose; applications keep sensitive values in structured attributes.
func RedactLogMessage(message string) string {
	if sensitiveLogText.MatchString(message) {
		return "[redacted: sensitive-looking log message]"
	}
	return message
}
