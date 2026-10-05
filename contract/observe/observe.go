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
	"strings"
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
	return len(value) <= MaxTracestateBytes && printable(value)
}

func printable(value string) bool {
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

// MaxTraceparentBytes bounds an inbound traceparent value. Version 00 is
// exactly 55 bytes; a later version may append fields, which are read only
// up to this bound.
const MaxTraceparentBytes = 256

// ExtractTrace reads an inbound W3C trace context from the values of a
// carrier's traceparent and tracestate fields, one entry per header line,
// metadata value or message header as received. It never fails: anything
// it cannot use is ignored, so a malformed header can never refuse work.
//
//   - Exactly one traceparent value is used. None, or more than one (W3C
//     defines a single traceparent field), yields no context at all.
//   - The value must be at most MaxTraceparentBytes of printable ASCII,
//     including any fields a later version appends, and parse with
//     ParseTraceparent (version ff, upper-case hex and all-zero ids are
//     rejected).
//   - Exactly one tracestate value that ValidTracestate accepts is kept.
//     More than one, an oversized one or a non-printable one is dropped,
//     and the traceparent is still used, as W3C allows. Values are never
//     merged.
//
// The context is correlation data, never authority.
func ExtractTrace(traceparent, tracestate []string) (TraceContext, bool) {
	if len(traceparent) != 1 || len(traceparent[0]) > MaxTraceparentBytes || !printable(traceparent[0]) {
		return TraceContext{}, false
	}
	parsed, err := ParseTraceparent(traceparent[0])
	if err != nil {
		return TraceContext{}, false
	}
	parsed.Flags &= FlagSampled
	if len(tracestate) == 1 && ValidTracestate(tracestate[0]) {
		parsed.State = tracestate[0]
	}
	return parsed, true
}

// InboundSampling says what an inbound trace context's sampled flag means
// to the run it parents (ADR 0020).
type InboundSampling uint8

const (
	// IgnoreInboundSampling, the zero value, keeps the inbound trace id and
	// parent span but decides sampling locally: the run is sampled with
	// probability Ratio, drawn independently of anything the caller sent,
	// so a forged sampled flag or a crafted trace id cannot raise the
	// sample rate.
	IgnoreInboundSampling InboundSampling = iota
	// HonorInboundSampling keeps the caller's sampled flag, as OpenTelemetry
	// ParentBased sampling does. Choose it only when callers are trusted to
	// decide how much the application records.
	HonorInboundSampling
)

// Valid reports whether s is a defined policy.
func (s InboundSampling) Valid() bool {
	return s == IgnoreInboundSampling || s == HonorInboundSampling
}

// Inbound returns the parent a run admitted with the inbound context remote
// joins, with its sampled flag decided by sampling. It reports false, and
// nothing is joined, when tracing is disabled, remote is not valid or
// sampling is not a defined policy: an application that traces nothing
// propagates nothing it received.
func (p TracePolicy) Inbound(remote TraceContext, sampling InboundSampling) (TraceContext, bool) {
	if !p.Enabled() || !remote.TraceID.IsValid() || !remote.SpanID.IsValid() || !sampling.Valid() {
		return TraceContext{}, false
	}
	if !ValidTracestate(remote.State) {
		remote.State = ""
	}
	switch sampling {
	case HonorInboundSampling:
		remote.Flags &= FlagSampled
	default:
		remote.Flags = 0
		// A fresh random id, never the caller's: Samples is deterministic
		// in its argument, and a caller that chose trace ids under the
		// ratio would otherwise be sampled every time.
		if p.Samples(NewTraceID()) {
			remote.Flags = FlagSampled
		}
	}
	return remote, true
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
// it (ADR 0021). Groups: 1-2 a sensitive key and its value (the key may carry
// a prefix or suffix, "client_secret", "access_token", and may be quoted or
// JSON-escaped, `\"password\":`); 3-4 an XML element; 5-6 a bearer token;
// 7 a credential with an unconditional shape (cloud and provider token
// prefixes, JWTs, URL userinfo, PEM private keys).
var sensitiveLogText = regexp.MustCompile(`(?i)` +
	`((?:password|passwd|pwd|passphrase|secret|token|authorization|credential|cookie|session[_-]?id|api[_-]?key|private[_-]?key)[a-z0-9_.-]*)\\?["']?\s*[:=]\s*\\?["']?((?:bearer\s+|basic\s+)?[^\s"'\\,;&<>]+)` +
	`|<(password|passwd|pwd|passphrase|secret|token|api[_-]?key|private[_-]?key)>\s*([^<\s]+)` +
	`|\b(bearer)\s+([A-Za-z0-9._~+/=-]+)` +
	`|(\bAKIA[0-9A-Z]{16}\b|\beyJ[A-Za-z0-9_-]+\.eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+|\b[a-z][a-z0-9+.-]*://[^/\s:@]+:[^/\s@]+@|-----BEGIN [A-Z ]*PRIVATE KEY-----|\bgh[pousr]_[A-Za-z0-9]{20,}|\bgithub_pat_[A-Za-z0-9_]{20,}|\bsk-[A-Za-z0-9_-]{16,}|\bxox[abprs]-[A-Za-z0-9-]{10,})`)

// sensitiveMarkers is a necessary condition for sensitiveLogText: text that
// contains none of them (case-insensitively) cannot match, so the regular
// expression runs only on the rare text that might.
var sensitiveMarkers = []string{"password", "passwd", "pwd", "passphrase", "secret", "token", "authorization", "credential", "cookie", "session", "api", "private", "bearer", "akia", "eyj", "://", "-----begin", "ghp_", "gho_", "ghu_", "ghs_", "ghr_", "github_pat_", "sk-", "xox"}

// scanCredentials calls accept for each candidate match: the full key (the
// marker with its prefix and suffix, lowercased; "bearer" for a bearer
// token; "" for an unconditional shape), its value, and whether the match
// has an unconditional credential shape. It reports whether accept returned
// true for any.
func scanCredentials(text string, accept func(key, value string, shaped bool) bool) bool {
	lower := strings.ToLower(text)
	found := false
	for _, marker := range sensitiveMarkers {
		if strings.Contains(lower, marker) {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	group := func(m []int, i int) string {
		if m[2*i] < 0 {
			return ""
		}
		return text[m[2*i]:m[2*i+1]]
	}
	for _, m := range sensitiveLogText.FindAllStringSubmatchIndex(text, -1) {
		var ok bool
		switch {
		case m[2] >= 0:
			// The unanchored match starts at the marker; the key's prefix
			// ("access_", "max_") precedes it.
			start := m[2]
			for start > 0 && keyByte(lower[start-1]) {
				start--
			}
			ok = accept(lower[start:m[3]], group(m, 2), false)
		case m[6] >= 0:
			ok = accept(strings.ToLower(group(m, 3)), group(m, 4), false)
		case m[10] >= 0:
			ok = accept("bearer", group(m, 6), false)
		default:
			ok = accept("", group(m, 7), true)
		}
		if ok {
			return true
		}
	}
	return false
}

func keyByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-'
}

// tokenCountNames are the exact (normalized) names of model-token counts and
// limits. Only these are exempt from the "token" marker: a plural key such
// as refreshTokens, accessTokens or csrfTokens holds tokens and stays
// sensitive.
var tokenCountNames = map[string]bool{
	"maxtokens": true, "totaltokens": true, "prompttokens": true, "completiontokens": true,
	"inputtokens": true, "outputtokens": true, "maxoutputtokens": true, "maxinputtokens": true,
	"maxcompletiontokens": true, "cachedtokens": true, "reasoningtokens": true, "numtokens": true,
	"tokensused": true, "usedtokens": true, "tokencount": true, "tokenlimit": true,
	"tokenbudget": true, "tokenusage": true, "maxtokenlimit": true,
}

// TokenCountKey reports whether key is the exact name of a model-token
// count or limit, such as "max_tokens", "tokenLimit" or the last segment of
// a dotted path ("usage.total_tokens"). Case and '_' '-' are ignored.
func TokenCountKey(key string) bool {
	if dot := strings.LastIndexAny(key, "./"); dot >= 0 {
		key = key[dot+1:]
	}
	key = strings.ToLower(strings.NewReplacer("_", "", "-", "").Replace(key))
	return tokenCountNames[key]
}

// SensitiveText reports whether text contains a credential-shaped fragment.
// It is deliberately broad, for redacting projected copies: any value after
// a sensitive key counts, except a plain number after an exact token-count
// name ("max_tokens: 256"). A number after any other token key
// ("access_token=48291736") is still redacted. It inspects the text as written;
// observe/redact also inspects encoded forms.
func SensitiveText(text string) bool {
	return scanCredentials(text, func(key, value string, shaped bool) bool {
		return shaped || !(TokenCountKey(key) && digits(value))
	})
}

// CredentialText reports whether text contains an actual credential value,
// not merely a sensitive word in prose: an unconditional credential shape,
// or a value after a sensitive key or bearer scheme that looks generated (at
// least 8 characters, with both letters and digits). Use it to refuse
// content; use SensitiveText to redact it.
func CredentialText(text string) bool {
	return scanCredentials(text, func(_, value string, shaped bool) bool {
		return shaped || LooksSecret(value)
	})
}

// LooksSecret reports whether a value looks generated rather than written:
// at least 8 characters (after an optional "Bearer "/"Basic " scheme) with
// both a letter and a digit. "the new password" and "256" do not.
func LooksSecret(value string) bool {
	lower := strings.ToLower(value)
	for _, scheme := range []string{"bearer ", "basic "} {
		if strings.HasPrefix(lower, scheme) {
			value = strings.TrimSpace(value[len(scheme):])
		}
	}
	if len(value) < 8 {
		return false
	}
	letter, digit := false, false
	for i := 0; i < len(value); i++ {
		c := value[i]
		letter = letter || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
		digit = digit || c >= '0' && c <= '9'
	}
	return letter && digit
}

func digits(value string) bool {
	if value == "" {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

// RedactLogMessage replaces a log message that contains credential-shaped
// text with a fixed marker. Pattern matching cannot find every secret in
// prose; applications keep sensitive values in structured attributes.
func RedactLogMessage(message string) string {
	if SensitiveText(message) {
		return "[redacted: sensitive-looking log message]"
	}
	return message
}
