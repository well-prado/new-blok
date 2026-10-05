package observe_test

import (
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/observe"
)

// Inbound trace context (#276): extraction from a carrier and the sampling
// policy a run that joins it gets.

func TestExtractTraceUsesOnlyOneWellFormedTraceparent(t *testing.T) {
	other := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	cases := []struct {
		name        string
		traceparent []string
		ok          bool
	}{
		{"one canonical value", []string{validParent}, true},
		{"future version within the bound", []string{"01" + validParent[2:] + "-extra"}, true},
		{"absent", nil, false},
		{"empty list entry", []string{""}, false},
		{"two different values", []string{validParent, other}, false},
		{"the same value twice", []string{validParent, validParent}, false},
		{"comma-joined values", []string{validParent + "," + other}, false},
		{"version ff", []string{"ff" + validParent[2:]}, false},
		{"upper-case hex", []string{strings.ToUpper(validParent)}, false},
		{"zero trace id", []string{"00-00000000000000000000000000000000-00f067aa0ba902b7-01"}, false},
		{"non-ASCII", []string{validParent[:34] + "é-00f067aa0ba902b7-01"}, false},
		{"non-ASCII after a future version", []string{"01" + validParent[2:] + "-é"}, false},
		{"control character after a future version", []string{"01" + validParent[2:] + "-\x7f"}, false},
		{"oversized future version", []string{"01" + validParent[2:] + "-" + strings.Repeat("a", observe.MaxTraceparentBytes)}, false},
		{"leading space", []string{" " + validParent}, false},
	}
	for _, tc := range cases {
		parsed, ok := observe.ExtractTrace(tc.traceparent, nil)
		if ok != tc.ok {
			t.Fatalf("%s: ok=%v, want %v (%+v)", tc.name, ok, tc.ok, parsed)
		}
		if ok && (parsed.TraceID.String() != "4bf92f3577b34da6a3ce929d0e0e4736" || parsed.SpanID.String() != "00f067aa0ba902b7" || !parsed.Sampled() || parsed.State != "") {
			t.Fatalf("%s: parsed %+v", tc.name, parsed)
		}
		if !ok && parsed != (observe.TraceContext{}) {
			t.Fatalf("%s: a refused carrier returned %+v", tc.name, parsed)
		}
	}
}

func TestExtractTraceKeepsOneValidTracestateAndDropsTheRest(t *testing.T) {
	cases := []struct {
		name       string
		tracestate []string
		want       string
	}{
		{"one value", []string{"rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"}, "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"},
		{"absent", nil, ""},
		{"two values are never merged", []string{"a=1", "b=2"}, ""},
		{"over the limit", []string{strings.Repeat("x", observe.MaxTracestateBytes+1)}, ""},
		{"at the limit", []string{strings.Repeat("x", observe.MaxTracestateBytes)}, strings.Repeat("x", observe.MaxTracestateBytes)},
		{"non-ASCII", []string{"vendor=é"}, ""},
		{"control character", []string{"vendor=a\x01"}, ""},
	}
	for _, tc := range cases {
		parsed, ok := observe.ExtractTrace([]string{validParent}, tc.tracestate)
		if !ok || parsed.State != tc.want {
			t.Fatalf("%s: ok=%v state=%q, want the traceparent kept with state %q", tc.name, ok, parsed.State, tc.want)
		}
	}
	if _, ok := observe.ExtractTrace(nil, []string{"a=1"}); ok {
		t.Fatal("a tracestate without a traceparent was used")
	}
}

func TestExtractTraceKeepsOnlyTheSampledFlag(t *testing.T) {
	parsed, ok := observe.ExtractTrace([]string{"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-ff"}, nil)
	if !ok || parsed.Flags != observe.FlagSampled {
		t.Fatalf("flags %x ok=%v", parsed.Flags, ok)
	}
}

func remoteParent(t *testing.T, traceparent string) observe.TraceContext {
	t.Helper()
	parsed, ok := observe.ExtractTrace([]string{traceparent}, []string{"vendor=1"})
	if !ok {
		t.Fatalf("%s did not parse", traceparent)
	}
	return parsed
}

func TestInboundJoinsTheRemoteTraceUnderAnEnabledPolicyOnly(t *testing.T) {
	remote := remoteParent(t, validParent)
	for _, policy := range []observe.TracePolicy{{}, {Ratio: -1}} {
		if parent, ok := policy.Inbound(remote, observe.HonorInboundSampling); ok || parent != (observe.TraceContext{}) {
			t.Fatalf("policy %+v joined %+v", policy, parent)
		}
	}
	if _, ok := (observe.TracePolicy{Ratio: 1}).Inbound(observe.TraceContext{}, observe.HonorInboundSampling); ok {
		t.Fatal("joined a zero context")
	}
	if _, ok := (observe.TracePolicy{Ratio: 1}).Inbound(remote, observe.InboundSampling(7)); ok {
		t.Fatal("an undefined sampling policy joined")
	}
	for _, sampling := range []observe.InboundSampling{observe.IgnoreInboundSampling, observe.HonorInboundSampling} {
		parent, ok := (observe.TracePolicy{Ratio: 1}).Inbound(remote, sampling)
		if !ok || parent.TraceID != remote.TraceID || parent.SpanID != remote.SpanID || parent.State != "vendor=1" {
			t.Fatalf("sampling %d: %+v ok=%v", sampling, parent, ok)
		}
		root := (observe.TracePolicy{Ratio: 1}).Root(parent)
		if root.TraceID != remote.TraceID || root.Parent != remote.SpanID || root.SpanID == remote.SpanID {
			t.Fatalf("sampling %d: the run span %+v is not a child of the inbound span", sampling, root)
		}
	}
	if (observe.InboundSampling(2)).Valid() || !observe.IgnoreInboundSampling.Valid() || !observe.HonorInboundSampling.Valid() {
		t.Fatal("InboundSampling.Valid")
	}
}

func TestHonorKeepsTheCallersDecision(t *testing.T) {
	policy := observe.TracePolicy{Ratio: 0.5}
	for _, flags := range []string{"00", "01"} {
		remote := remoteParent(t, validParent[:53]+flags)
		parent, ok := policy.Inbound(remote, observe.HonorInboundSampling)
		if !ok || parent.Sampled() != remote.Sampled() {
			t.Fatalf("flags %s: honored as sampled=%v", flags, parent.Sampled())
		}
	}
}

// TestIgnoreDecidesLocallyWhateverTheCallerSends: a forged sampled flag,
// with a trace id chosen so the ratio arithmetic would sample it, is sampled
// at the local ratio; an unsampled flag cannot opt out of full sampling.
func TestIgnoreDecidesLocallyWhateverTheCallerSends(t *testing.T) {
	crafted := remoteParent(t, "00-4bf92f3577b34da60000000000000000-00f067aa0ba902b7-01")
	if !(observe.TracePolicy{Ratio: 1e-12}).Samples(crafted.TraceID) {
		t.Fatal("the crafted id is not one the ratio arithmetic samples; the test would be vacuous")
	}
	const draws, ratio = 20000, 0.1
	policy := observe.TracePolicy{Ratio: ratio}
	for name, remote := range map[string]observe.TraceContext{"crafted id": crafted, "random ids": {}} {
		sampled := 0
		for range draws {
			if name == "random ids" {
				remote = observe.TraceContext{TraceID: observe.NewTraceID(), SpanID: observe.NewSpanID(), Flags: observe.FlagSampled}
			}
			parent, ok := policy.Inbound(remote, observe.IgnoreInboundSampling)
			if !ok || parent.TraceID != remote.TraceID {
				t.Fatalf("%s: %+v ok=%v", name, parent, ok)
			}
			if parent.Sampled() {
				sampled++
			}
		}
		// Binomial(20000, 0.1): mean 2000, standard deviation 42.
		if sampled < 1800 || sampled > 2200 {
			t.Fatalf("%s: %d of %d forced-sampled contexts sampled at ratio %v", name, sampled, draws, ratio)
		}
	}
	unsampled := remoteParent(t, validParent[:53]+"00")
	for range 100 {
		if parent, _ := (observe.TracePolicy{Ratio: 1}).Inbound(unsampled, observe.IgnoreInboundSampling); !parent.Sampled() {
			t.Fatal("an inbound 00 flag lowered the local full sampling")
		}
	}
}

// TestExtractTraceDropsCredentialShapedTracestateMembers (#285 review F1):
// tracestate is forwarded to exporters and workers, so a member the shared
// credential pattern flags never survives extraction; the traceparent and
// the other members do.
func TestExtractTraceDropsCredentialShapedTracestateMembers(t *testing.T) {
	cases := []struct{ tracestate, want string }{
		{"token=SYNTHETIC-ts-0001,pw=password:hunter2", ""},
		{"vendor=ok,token=SYNTHETIC-ts-0001,pw=password:hunter2,other=1", "vendor=ok,other=1"},
		{"rojo=00f067aa0ba902b7, api_key=SYNTHETIC-k3y-0002 ,congo=t61rcWkgMzE", "rojo=00f067aa0ba902b7,congo=t61rcWkgMzE"},
		{"vendor=AKIAABCDEFGHIJKLMNOP", ""},
		{"vendor=ok", "vendor=ok"},
	}
	for _, tc := range cases {
		parsed, ok := observe.ExtractTrace([]string{validParent}, []string{tc.tracestate})
		if !ok || parsed.TraceID.String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
			t.Fatalf("%q: the traceparent was not kept: %+v ok=%v", tc.tracestate, parsed, ok)
		}
		if parsed.State != tc.want || strings.Contains(parsed.State, "SYNTHETIC") || strings.Contains(parsed.State, "hunter2") {
			t.Fatalf("%q: state %q, want %q", tc.tracestate, parsed.State, tc.want)
		}
		if observe.SensitiveText(parsed.State) {
			t.Fatalf("%q: extracted state %q is still sensitive", tc.tracestate, parsed.State)
		}
	}
	// A context built elsewhere is cleaned the same way when a run joins it.
	remote, _ := observe.ExtractTrace([]string{validParent}, nil)
	remote.State = "token=SYNTHETIC-ts-0001,vendor=ok"
	if parent, ok := (observe.TracePolicy{Ratio: 1}).Inbound(remote, observe.HonorInboundSampling); !ok || parent.State != "vendor=ok" {
		t.Fatalf("Inbound kept %q (ok=%v)", parent.State, ok)
	}
}

// TestIgnoreSamplingIsIndependentOfTheCallersTraceID (#285 review F4): the
// ignore policy must not apply the ratio arithmetic to the caller's id. An
// id that arithmetic always samples, and one it never samples, must both be
// sampled at the local ratio.
func TestIgnoreSamplingIsIndependentOfTheCallersTraceID(t *testing.T) {
	always := remoteParent(t, "00-4bf92f3577b34da60000000000000000-00f067aa0ba902b7-01")
	never := remoteParent(t, "00-4bf92f3577b34da6ffffffffffffffff-00f067aa0ba902b7-01")
	half := observe.TracePolicy{Ratio: 0.5}
	if !half.Samples(always.TraceID) || half.Samples(never.TraceID) {
		t.Fatal("the fixture ids do not pin the ratio arithmetic; the test would be vacuous")
	}
	for name, remote := range map[string]observe.TraceContext{"always-sampled id": always, "never-sampled id": never} {
		sampled := 0
		const draws = 2000
		for range draws {
			if parent, _ := half.Inbound(remote, observe.IgnoreInboundSampling); parent.Sampled() {
				sampled++
			}
		}
		// Binomial(2000, 0.5): mean 1000, standard deviation 22.
		if sampled < 850 || sampled > 1150 {
			t.Fatalf("%s: %d of %d sampled at ratio 0.5; the decision follows the caller's id", name, sampled, draws)
		}
	}
}

func TestFilterTracestateDropsOnlyFlaggedMembers(t *testing.T) {
	flagged := func(member string) bool { return strings.HasPrefix(member, "bad=") }
	cases := []struct {
		state     string
		sensitive func(string) bool
		want      string
	}{
		{"a=1, b=2", flagged, "a=1, b=2"},
		{"a=1, bad=x ,b=2", flagged, "a=1,b=2"},
		{"bad=x", flagged, ""},
		{"", flagged, ""},
		{strings.Repeat("x", observe.MaxTracestateBytes+1), flagged, ""},
		{"a=1,token=SYNTHETIC-1", nil, "a=1"},
		{"a=1,b=2", func(string) bool { return false }, "a=1,b=2"},
		// Members that are clean apart but flagged together keep nothing.
		{"a=1,b=2", func(s string) bool { return s == "a=1,b=2" }, ""},
	}
	for _, tc := range cases {
		if got := observe.FilterTracestate(tc.state, tc.sensitive); got != tc.want {
			t.Fatalf("FilterTracestate(%q) = %q, want %q", tc.state, got, tc.want)
		}
	}
}
