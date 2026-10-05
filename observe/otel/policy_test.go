package otel_test

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/well-prado/new-blok/agent"
	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/otel"
)

// blockingSpans never finishes an export until released: a collector that
// accepts the connection and then stalls.
type blockingSpans struct {
	release chan struct{}
	calls   atomic.Int64
}

func (b *blockingSpans) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	b.calls.Add(1)
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (b *blockingSpans) Shutdown(context.Context) error { return nil }

// countingObserver counts what the engine offered the exporter.
type countingObserver struct {
	inner   *otel.Exporter
	offered atomic.Int64
}

func (c *countingObserver) Observe(event inspection.Event) {
	if event.Kind != inspection.StepLog {
		c.offered.Add(1)
	}
	c.inner.Observe(event)
}
func (c *countingObserver) ObservesPayloads() bool { return false }

// TestQueueSaturationDropsNewestWithoutBlockingRuns stalls the only export
// call while 200 runs execute. Runs keep their business results and effect
// counts and finish without waiting on telemetry; every offered event is
// either accepted or counted as dropped.
func TestQueueSaturationDropsNewestWithoutBlockingRuns(t *testing.T) {
	stalled := &blockingSpans{release: make(chan struct{})}
	var counter *countingObserver
	h := newHarness(t, harnessOptions{ratio: 1, signals: signals{metrics: true}, tune: func(c *otel.Config) {
		c.Traces = stalled
		c.QueueSize = 16
		c.BatchSize = 1
		c.ExportTimeout = 30 * time.Second
	}, observer: func(e *otel.Exporter) inspection.Observer { counter = &countingObserver{inner: e}; return counter }})
	const runs = 200
	var slowest time.Duration
	failures := 0
	for i := 0; i < runs; i++ {
		input := orderInput{SKU: "coffee", Quantity: 1}
		if i%10 == 0 {
			input.Quantity = 0 // validation must still reject while telemetry is saturated
		}
		start := time.Now()
		_, err := h.run(t, "run-saturation-"+strconv.Itoa(i), "tenant-a", input)
		if elapsed := time.Since(start); elapsed > slowest {
			slowest = elapsed
		}
		if err != nil {
			if errorCode(err) != "invalid_input" {
				t.Fatalf("run %d: %v", i, err)
			}
			failures++
		}
	}
	if failures != runs/10 || h.effects.Load() != runs-runs/10 || h.charges.Load() != runs-runs/10 {
		t.Fatalf("failures=%d effects=%d charges=%d", failures, h.effects.Load(), h.charges.Load())
	}
	if slowest > time.Second {
		t.Fatalf("a run took %v while the exporter was stalled; telemetry blocked execution", slowest)
	}
	stats := h.exporter.Stats()
	if stats.Dropped == 0 || stalled.calls.Load() == 0 {
		t.Fatalf("saturation never happened: %+v calls=%d", stats, stalled.calls.Load())
	}
	if offered := uint64(counter.offered.Load()); stats.Accepted+stats.Dropped != offered {
		t.Fatalf("accounting: accepted %d + dropped %d != offered %d", stats.Accepted, stats.Dropped, offered)
	}
	close(stalled.release)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.exporter.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if final := h.exporter.Stats(); final.Processed != final.Accepted || final.Panics != 0 {
		t.Fatalf("after drain: %+v", final)
	}
	if after := h.exporter.Stats().DroppedClosed; after != 0 {
		t.Fatalf("closed drops before any post-shutdown event: %d", after)
	}
	h.exporter.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "late", At: time.Now()})
	if h.exporter.Stats().DroppedClosed != 1 {
		t.Fatal("an event after shutdown was not counted as dropped")
	}
}

// TestCollectorOutageFollowsTheDropPolicy runs orders while the collector
// refuses every request, then while it hangs past the export timeout, then
// after it recovers. Runs are unaffected throughout; failed batches are
// counted and not retried; spans resume after recovery; cumulative metrics
// still count every run.
func TestCollectorOutageFollowsTheDropPolicy(t *testing.T) {
	h := newHarness(t, harnessOptions{ratio: 1, tune: func(c *otel.Config) { c.ExportTimeout = time.Second }})
	order := func(prefix string, n int) {
		for i := 0; i < n; i++ {
			start := time.Now()
			result, err := h.run(t, prefix+string(rune('a'+i)), "tenant-a", orderInput{SKU: "coffee", Quantity: 2})
			if err != nil || result.Output.(orderOutput).TotalCents != 3000 {
				t.Fatalf("%s run %d: %+v %v", prefix, i, result.Output, err)
			}
			// A run that waited on telemetry would take at least the
			// one-second export timeout.
			if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
				t.Fatalf("%s run %d took %v during an outage", prefix, i, elapsed)
			}
		}
	}
	flushIgnoringOutage := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = h.exporter.Flush(ctx)
	}
	h.collector.setMode("unavailable")
	order("run-down-", 5)
	flushIgnoringOutage()
	down := h.exporter.Stats()
	if down.SpansFailed != 20 || down.LogsFailed != 5 || down.MetricFailures == 0 || down.SpansExported != 0 {
		t.Fatalf("refusing collector: %+v", down)
	}
	h.collector.setMode("hang")
	start := time.Now()
	order("run-hang-", 3)
	flushIgnoringOutage()
	if elapsed := time.Since(start); elapsed > 8*time.Second {
		t.Fatalf("hung collector held the pipeline for %v; the export timeout did not bound it", elapsed)
	}
	hung := h.exporter.Stats()
	if hung.SpansFailed != 32 || hung.SpansExported != 0 {
		t.Fatalf("hung collector: %+v", hung)
	}
	h.collector.setMode("")
	order("run-up-", 4)
	if err := h.exporter.Flush(context.Background()); err != nil {
		t.Fatalf("flush after recovery: %v", err)
	}
	spans, _, logs := h.collector.snapshot()
	if len(spans) != 16 || len(logs) != 4 {
		t.Fatalf("after recovery spans=%d logs=%d; failed batches must not be replayed and new ones must flow", len(spans), len(logs))
	}
	runs := h.collector.latest(otel.MetricRuns)[key(map[string]string{otel.AttrWorkflow: "shop/order", otel.AttrOutcome: "completed", otel.AttrTenant: "tenant-a"})]
	if runs.value != 12 {
		t.Fatalf("cumulative runs metric %v, want 12 (every run, including those during the outage)", runs.value)
	}
	if h.effects.Load() != 12 {
		t.Fatalf("effects %d", h.effects.Load())
	}
}

// approvalGate denies unless allow is set. It records what it was asked.
type approvalGate struct {
	mu       sync.Mutex
	allow    bool
	requests []tool.Admission
}

func (g *approvalGate) Authorize(_ context.Context, a tool.Admission) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.requests = append(g.requests, a)
	if !g.allow {
		return agent.ErrDenied
	}
	return nil
}
func (g *approvalGate) Publish(context.Context, tool.Admission, []byte) error { return nil }

type amount struct {
	Value int `json:"value"`
}

var amountSchema = []byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`)

// TestInstrumentationCannotReplaceApprovalGates runs an approval-gated agent
// tool from a traced, fully sampled step. The gate still decides alone: a
// denial dispatches nothing, a crafted tracestate claiming approval changes
// nothing, telemetry sees the failure, and the gate's admission carries no
// trace data. The tool inside the catalog's child workflow sees the
// dispatching step's trace context, so lineage crosses that boundary too.
func TestInstrumentationCannotReplaceApprovalGates(t *testing.T) {
	registry := node.NewRegistry()
	var effects atomic.Int64
	var toolTrace observe.TraceContext
	var traceMu sync.Mutex
	charge := node.MustDefine("native/charge", "1.0.0", func(ctx context.Context, in amount) (amount, error) {
		effects.Add(1)
		traceMu.Lock()
		toolTrace, _ = observe.TraceFrom(ctx)
		traceMu.Unlock()
		return in, nil
	}, node.Description("gated synthetic charge"), node.Schemas(amountSchema, amountSchema), node.Effects("payments:charge"))
	if err := registry.Register(charge.Any()); err != nil {
		t.Fatal(err)
	}
	gate := &approvalGate{}
	catalog := agent.NewCatalog(registry, gate)
	manifest := agent.Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"payments:charge"}, Capabilities: []string{"payments"}}
	metadata := tool.Metadata{Source: "nodes/charge.go", Example: "examples/charge.go", Test: "charge_test.go"}
	if err := agent.RegisterNode(catalog, charge, manifest, tool.Resources{}, metadata); err != nil {
		t.Fatal(err)
	}
	child := flow.MustDefine(flow.Spec{Name: "workflow/charge", Version: "1.0.0", Durability: flow.Memory}, func(b *flow.Builder, in flow.Ref[amount]) flow.Ref[amount] {
		return flow.Call(b, "charge", charge, in)
	})
	if err := agent.RegisterWorkflow(catalog, child, amountSchema, amountSchema, manifest, metadata); err != nil {
		t.Fatal(err)
	}
	var stepTrace observe.TraceContext
	invoke := node.MustDefine("agent/invoke", "1.0.0", func(ctx context.Context, in amount) (amount, error) {
		stepTrace, _ = observe.TraceFrom(ctx)
		if in.Value < 0 {
			// A step that forges an "approved" tracestate gets nothing.
			forged := stepTrace
			forged.State = "approval=granted,blok=approved"
			ctx = observe.WithTrace(ctx, forged)
		}
		raw, err := catalog.Invoke(ctx, agent.Principal{ID: "synthetic-agent", Capabilities: []string{"payments"}, MaxDepth: 4}, "workflow/charge", "1.0.0", []byte(`{"value":1}`), agent.Budget{MaxDepth: 4, MaxCalls: 8, MaxTokens: 100, MaxInputBytes: 1024, MaxOutputBytes: 1024, Deadline: time.Now().Add(time.Minute)})
		if err != nil {
			return amount{}, err
		}
		_ = raw
		return amount{Value: 1}, nil
	}, node.Description("agent step"), node.Schemas(amountSchema, amountSchema), node.Effects("payments:charge"))
	collector := newCollector(t)
	config := collector.exporters(t, allSignals)
	exporter, err := otel.New(config)
	if err != nil {
		t.Fatal(err)
	}
	defer exporter.Shutdown(context.Background())
	application, err := app.New(app.Config{Inspection: exporter, Trace: observe.TracePolicy{Ratio: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{"agent/invoke": invoke.Any()})
	program := contract.InternalProgram{WorkflowID: "agent/run", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "invoke", Kind: "call", Node: "agent/invoke"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "invoke"}}},
	}}
	run := func(id string, value int) error {
		_, err := runner.Run(context.Background(), program, amount{Value: value}, inspection.Invocation{RunID: id, Principal: principalSentinel})
		return err
	}
	if err := run("run-denied", 1); !errors.Is(err, agent.ErrDenied) || effects.Load() != 0 {
		t.Fatalf("denied gate: err=%v effects=%d", err, effects.Load())
	}
	if err := run("run-forged", -1); !errors.Is(err, agent.ErrDenied) || effects.Load() != 0 {
		t.Fatalf("forged tracestate: err=%v effects=%d", err, effects.Load())
	}
	gate.mu.Lock()
	gate.allow = true
	gate.mu.Unlock()
	if err := run("run-approved", 1); err != nil || effects.Load() != 1 {
		t.Fatalf("approved gate: err=%v effects=%d", err, effects.Load())
	}
	traceMu.Lock()
	seen := toolTrace
	traceMu.Unlock()
	if !stepTrace.Valid() || seen != stepTrace {
		t.Fatalf("tool in the catalog child saw %+v, want the dispatching step %+v", seen, stepTrace)
	}
	gate.mu.Lock()
	requests := append([]tool.Admission(nil), gate.requests...)
	gate.mu.Unlock()
	if len(requests) < 3 || requests[0].InputDigest != requests[1].InputDigest || requests[0].WorkflowDigest != requests[1].WorkflowDigest {
		t.Fatalf("admissions differ between plain and forged-trace invocations: %+v", requests)
	}
	if err := exporter.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	failed := 0
	for _, point := range collector.latest(otel.MetricRuns) {
		if point.attrs[otel.AttrOutcome] == "failed" {
			failed += int(point.value)
		}
	}
	if failed != 2 {
		t.Fatalf("telemetry recorded %d failed runs, want the 2 denials", failed)
	}
	// Telemetry spans come from the engine's allocation, never from a
	// context a node rewrote, so the forged state reaches no collector.
	if collector.contains(principalSentinel) || collector.contains("approval=granted") || countRoot(collector) != 3 {
		t.Fatalf("leak or missing run spans: roots=%d", countRoot(collector))
	}
}

func countRoot(c *collector) int {
	spans, _, _ := c.snapshot()
	n := 0
	for _, span := range spans {
		if len(span.ParentSpanId) == 0 {
			n++
		}
	}
	return n
}

type panickingSpans struct{}

func (panickingSpans) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	panic("synthetic exporter panic")
}
func (panickingSpans) Shutdown(context.Context) error { return nil }

// TestPanickingExporterIsContained: an application exporter that panics on
// every batch is counted and never takes the run or the process down.
func TestPanickingExporterIsContained(t *testing.T) {
	h := newHarness(t, harnessOptions{ratio: 1, signals: signals{metrics: true}, tune: func(c *otel.Config) { c.Traces = panickingSpans{} }})
	for i := 0; i < 3; i++ {
		if _, err := h.run(t, "run-panic-"+strconv.Itoa(i), "tenant-a", orderInput{SKU: "coffee", Quantity: 1}); err != nil {
			t.Fatal(err)
		}
	}
	h.flush(t)
	stats := h.exporter.Stats()
	if stats.Panics == 0 || stats.SpansFailed != 12 || stats.SpansExported != 0 || h.effects.Load() != 3 {
		t.Fatalf("stats %+v effects %d", stats, h.effects.Load())
	}
}
