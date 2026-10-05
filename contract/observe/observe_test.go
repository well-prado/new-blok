package observe_test

import (
	"context"
	"math"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/internal/tooling/graphcheck"
)

const validParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func TestParseTraceparentAcceptsOnlyW3CContexts(t *testing.T) {
	cases := []struct {
		name, value string
		ok          bool
	}{
		{"canonical sampled", validParent, true},
		{"canonical unsampled", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", true},
		{"future version with extra field", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra", true},
		{"version 00 with extra field", validParent + "-extra", false},
		{"future version with junk tail", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01x", false},
		{"forbidden version ff", "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", false},
		{"upper-case hex", strings.ToUpper(validParent), false},
		{"zero trace id", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", false},
		{"zero span id", "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", false},
		{"short", validParent[:54], false},
		{"wrong separator", strings.Replace(validParent, "-", "_", 1), false},
		{"non-hex", "00-4bf92f3577b34da6a3ce929d0e0e47zz-00f067aa0ba902b7-01", false},
		{"empty", "", false},
	}
	for _, tc := range cases {
		parsed, err := observe.ParseTraceparent(tc.value)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: err=%v, want ok=%v", tc.name, err, tc.ok)
		}
		if tc.ok && tc.value[:2] == "00" && parsed.Traceparent() != tc.value {
			t.Fatalf("%s: round trip %q", tc.name, parsed.Traceparent())
		}
	}
	parsed, _ := observe.ParseTraceparent(validParent)
	if !parsed.Sampled() || parsed.TraceID.String() != "4bf92f3577b34da6a3ce929d0e0e4736" || parsed.SpanID.String() != "00f067aa0ba902b7" {
		t.Fatalf("parsed %+v", parsed)
	}
}

func TestTracestateIsBoundedPrintableASCII(t *testing.T) {
	for value, ok := range map[string]bool{
		"":                       true,
		"vendor=opaque,other=1":  true,
		strings.Repeat("v", 256): true,
		strings.Repeat("v", 257): false,
		"k=v\n":                  false,
		"k=é":                    false,
	} {
		if observe.ValidTracestate(value) != ok {
			t.Fatalf("%q: want %v", value, ok)
		}
		parsed, _ := observe.ParseTraceparent(validParent)
		parsed.State = value
		if parsed.Valid() != ok {
			t.Fatalf("context with tracestate %q valid=%v", value, !ok)
		}
	}
}

func TestTracePolicyIsExplicitAndDeterministic(t *testing.T) {
	for _, ratio := range []float64{-0.1, 1.1, math.NaN(), math.Inf(1)} {
		if err := (observe.TracePolicy{Ratio: ratio}).Validate(); err == nil {
			t.Fatalf("ratio %v accepted", ratio)
		}
		if (observe.TracePolicy{Ratio: ratio}).Enabled() {
			t.Fatalf("invalid ratio %v enabled", ratio)
		}
	}
	if (observe.TracePolicy{}).Enabled() || (observe.TracePolicy{}).Root(observe.TraceContext{}).SpanID.IsValid() {
		t.Fatal("zero policy must disable tracing and allocate no span")
	}
	full, quarter := observe.TracePolicy{Ratio: 1}, observe.TracePolicy{Ratio: 0.25}
	sampled := 0
	const n = 20000
	for i := 0; i < n; i++ {
		id := observe.NewTraceID()
		if !full.Samples(id) {
			t.Fatal("ratio 1 must sample every trace")
		}
		if quarter.Samples(id) != quarter.Samples(id) {
			t.Fatal("sampling is not deterministic for one trace id")
		}
		if quarter.Samples(id) {
			sampled++
			if !(observe.TracePolicy{Ratio: 0.5}).Samples(id) {
				t.Fatal("a trace sampled at 0.25 must be sampled at 0.5")
			}
		}
	}
	if got := float64(sampled) / n; got < 0.23 || got > 0.27 {
		t.Fatalf("ratio 0.25 sampled %.4f of %d traces", got, n)
	}
}

func TestRootJoinsParentAndChildKeepsTheDecision(t *testing.T) {
	parent, _ := observe.ParseTraceparent(validParent)
	parent.State = "vendor=opaque"
	// A run with a parent keeps the parent's decision even when its own
	// ratio would not sample it: one trace has one decision.
	never := observe.TracePolicy{Ratio: math.SmallestNonzeroFloat64}
	run := never.Root(parent)
	if run.TraceID != parent.TraceID || run.Parent != parent.SpanID || !run.Sampled() || run.State != "vendor=opaque" || run.SpanID == parent.SpanID || !run.SpanID.IsValid() {
		t.Fatalf("joined run %+v", run)
	}
	unsampled := parent
	unsampled.Flags = 0
	if never.Root(unsampled).Sampled() {
		t.Fatal("an unsampled parent was sampled")
	}
	step := run.Child()
	if step.TraceID != run.TraceID || step.Parent != run.SpanID || step.SpanID == run.SpanID || step.Flags != run.Flags || step.State != run.State {
		t.Fatalf("child %+v of %+v", step, run)
	}
	root := (observe.TracePolicy{Ratio: 1}).Root(observe.TraceContext{})
	if !root.TraceID.IsValid() || root.Parent.IsValid() || !root.Sampled() {
		t.Fatalf("root %+v", root)
	}
}

func TestContextCarriesOnlyValidTraceContexts(t *testing.T) {
	parent, _ := observe.ParseTraceparent(validParent)
	if _, ok := observe.TraceFrom(context.Background()); ok {
		t.Fatal("background context has a trace")
	}
	ctx := observe.WithTrace(context.Background(), parent)
	if got, ok := observe.TraceFrom(ctx); !ok || got != parent {
		t.Fatalf("carried %+v %v", got, ok)
	}
	invalid := parent
	invalid.State = strings.Repeat("x", observe.MaxTracestateBytes+1)
	if _, ok := observe.TraceFrom(observe.WithTrace(context.Background(), invalid)); ok {
		t.Fatal("an invalid context was stored")
	}
	if _, ok := observe.TraceFrom(observe.WithTrace(ctx, observe.TraceContext{})); !ok {
		t.Fatal("an invalid context replaced a valid parent")
	}
}

func TestLabelsAreBoundedShapes(t *testing.T) {
	for value, ok := range map[string]bool{
		"shop/quote":             true,
		"calculate-quote_1.v2:x": true,
		strings.Repeat("a", 64):  true,
		strings.Repeat("a", 65):  false,
		"":                       false,
		"user 42":                false,
		"user@example.test":      false,
		`{"sku":"coffee"}`:       false,
		"café":                   false,
	} {
		if observe.ValidLabel(value) != ok {
			t.Fatalf("%q: want %v", value, ok)
		}
	}
}

func TestRedactLogMessageMatchesCredentialShapes(t *testing.T) {
	for _, message := range []string{"password=synthetic", "Authorization: Bearer abc.def", "token: synthetic-value", "AKIAABCDEFGHIJKLMNOP"} {
		if observe.RedactLogMessage(message) == message {
			t.Fatalf("%q not redacted", message)
		}
	}
	if got := observe.RedactLogMessage("quote calculated"); got != "quote calculated" {
		t.Fatalf("ordinary message changed: %q", got)
	}
}

// TestPortStaysTransportFree pins ADR 0020's dependency rule: the port the
// engine imports reaches only the standard library, never a network,
// process or exporter package, and the engine reaches it (so the check is
// not vacuous) but no observation adapter.
func TestPortStaysTransportFree(t *testing.T) {
	graph, err := graphcheck.Analyze(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	module := graph.Module
	port := graph.Packages[module+"/contract/observe"]
	if !port.Source {
		t.Fatal("graph does not contain contract/observe; the check would be vacuous")
	}
	for _, imported := range port.Imports {
		first, _, _ := strings.Cut(imported, "/")
		if strings.Contains(first, ".") || strings.HasPrefix(imported, module) || strings.HasPrefix(imported, "net") || strings.HasPrefix(imported, "os") || strings.HasPrefix(imported, "crypto/tls") {
			t.Errorf("contract/observe imports %s", imported)
		}
	}
	seen := map[string]bool{}
	var walk func(string)
	walk = func(path string) {
		if seen[path] {
			return
		}
		seen[path] = true
		for _, imported := range graph.Packages[path].Imports {
			walk(imported)
		}
	}
	walk(module + "/internal/engine")
	if !seen[module+"/contract/observe"] {
		t.Fatal("the engine does not reach contract/observe; the check is not seeing the port")
	}
	for path := range seen {
		if strings.HasPrefix(path, module+"/observe/") || strings.HasPrefix(path, "go.opentelemetry.io/") {
			t.Errorf("the engine reaches %s", path)
		}
	}
}
