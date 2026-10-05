package otel_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/otel"
	"github.com/well-prado/new-blok/runtime/worker"
)

type labelInput struct {
	Label string `json:"label"`
}

// traceSeen is what the callee observed: the trace context it received.
type traceSeen struct {
	Traceparent string `json:"traceparent"`
	Tracestate  string `json:"tracestate"`
}

var (
	labelSchema = []byte(`{"type":"object","properties":{"label":{"type":"string"}},"required":["label"]}`)
	seenSchema  = []byte(`{"type":"object","properties":{"traceparent":{"type":"string"},"tracestate":{"type":"string"}},"required":["traceparent","tracestate"]}`)
)

// goTraceNode is the native equivalent of the Node fixture/trace node.
func goTraceNode() node.Definition[labelInput, traceSeen] {
	return node.MustDefine("fixture/trace", "1.0.0", func(ctx context.Context, in labelInput) (traceSeen, error) {
		node.Logger(ctx).Info("trace observed", "label", in.Label)
		trace, ok := observe.TraceFrom(ctx)
		if !ok {
			return traceSeen{}, nil
		}
		return traceSeen{Traceparent: trace.Traceparent(), Tracestate: trace.State}, nil
	}, node.Description("Native trace context echo"), node.Schemas(labelSchema, seenSchema), node.Pure())
}

// lineageResult is what the parent workflow returns: what each callee saw.
type lineageResult struct {
	Parent traceSeen `json:"parent"`
	Child  traceSeen `json:"child"`
}

// runLineage composes parent(price → spawn → output) where spawn starts
// child(inner → output) through the same execution.Runner, as an
// application-composed child run with ParentRun/ParentStep. price and inner
// are the given trace node (native or the actual Node worker).
func runLineage(t *testing.T, traceNode node.Any, ratio float64) (*collector, *otel.Exporter, traceSeen, traceSeen) {
	t.Helper()
	collector := newCollector(t)
	exporter, err := otel.New(collector.exporters(t, allSignals))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exporter.Shutdown(context.Background()) })
	application, err := app.New(app.Config{Inspection: exporter, Trace: observe.TracePolicy{Ratio: ratio}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	child := contract.InternalProgram{WorkflowID: "lineage/child", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "inner", Kind: "call", Node: "fixture/trace"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "inner"}}},
	}}
	var runner *execution.Runner
	var mu sync.Mutex
	var childSeen traceSeen
	spawn := node.MustDefine("lineage/spawn", "1.0.0", func(ctx context.Context, in labelInput) (traceSeen, error) {
		result, err := runner.Run(ctx, child, in, inspection.Invocation{RunID: "run-lineage-child", Principal: principalSentinel, ParentRun: "run-lineage", ParentStep: "spawn"})
		if err != nil {
			return traceSeen{}, err
		}
		raw, err := json.Marshal(result.Output)
		if err != nil {
			return traceSeen{}, err
		}
		var seen traceSeen
		if err := json.Unmarshal(raw, &seen); err != nil {
			return traceSeen{}, err
		}
		mu.Lock()
		childSeen = seen
		mu.Unlock()
		return seen, nil
	}, node.Description("Starts the child workflow"), node.Schemas(labelSchema, seenSchema), node.Pure())
	runner = execution.NewRunner(application, map[string]node.Any{"fixture/trace": traceNode, "lineage/spawn": spawn.Any()})
	parent := contract.InternalProgram{WorkflowID: "lineage/parent", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "price", Kind: "call", Node: "fixture/trace"},
		{Index: 1, ID: "spawn", Kind: "call", Node: "lineage/spawn"},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "price"}}},
	}}
	result, err := runner.Run(context.Background(), parent, labelInput{Label: "synthetic"}, inspection.Invocation{RunID: "run-lineage", Principal: principalSentinel})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(result.Output)
	var parentSeen traceSeen
	if err := json.Unmarshal(raw, &parentSeen); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	return collector, exporter, parentSeen, childSeen
}

func spanBy(t *testing.T, spans []*tracepb.Span, name, runID string) *tracepb.Span {
	t.Helper()
	for _, span := range spans {
		if span.Name == name && attrs(span.Attributes)[otel.AttrRunID] == runID {
			return span
		}
	}
	t.Fatalf("no span %q for %s", name, runID)
	return nil
}

func id(b []byte) string { return hex.EncodeToString(b) }

// assertLineage checks the exported tree and what each callee saw:
// parent run → price, spawn → child run → inner, all in one trace, with each
// callee's traceparent naming its own step span as the parent.
func assertLineage(t *testing.T, collector *collector, exporter *otel.Exporter, parentSeen, childSeen traceSeen, wantLogs int) {
	t.Helper()
	var spans []*tracepb.Span
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if err := exporter.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
		var logs int
		spans, _, _ = collector.snapshot()
		_, _, records := collector.snapshot()
		logs = len(records)
		if len(spans) == 7 && logs >= wantLogs || time.Now().After(deadline) {
			break
		}
	}
	if len(spans) != 7 {
		t.Fatalf("spans=%d, want parent run+3 steps and child run+2 steps", len(spans))
	}
	parentRun := spanBy(t, spans, "run lineage/parent", "run-lineage")
	price := spanBy(t, spans, "step price", "run-lineage")
	spawn := spanBy(t, spans, "step spawn", "run-lineage")
	childRun := spanBy(t, spans, "run lineage/child", "run-lineage-child")
	inner := spanBy(t, spans, "step inner", "run-lineage-child")
	trace := id(parentRun.TraceId)
	for _, span := range spans {
		if id(span.TraceId) != trace {
			t.Fatalf("%s left the trace", span.Name)
		}
	}
	if len(parentRun.ParentSpanId) != 0 || id(price.ParentSpanId) != id(parentRun.SpanId) || id(spawn.ParentSpanId) != id(parentRun.SpanId) {
		t.Fatal("parent steps are not children of the parent run")
	}
	if id(childRun.ParentSpanId) != id(spawn.SpanId) {
		t.Fatalf("child run's parent %s, want the spawn step %s", id(childRun.ParentSpanId), id(spawn.SpanId))
	}
	if a := attrs(childRun.Attributes); a[otel.AttrParentRun] != "run-lineage" || a[otel.AttrParentStep] != "spawn" {
		t.Fatalf("child run lineage attributes %+v", a)
	}
	if id(inner.ParentSpanId) != id(childRun.SpanId) {
		t.Fatal("inner step is not a child of the child run")
	}
	if childRun.StartTimeUnixNano < spawn.StartTimeUnixNano || childRun.EndTimeUnixNano > spawn.EndTimeUnixNano {
		t.Fatal("child run is not inside the step that started it")
	}
	if parentSeen.Traceparent != "00-"+trace+"-"+id(price.SpanId)+"-01" {
		t.Fatalf("price callee saw %q, want its own step span in trace %s", parentSeen.Traceparent, trace)
	}
	if childSeen.Traceparent != "00-"+trace+"-"+id(inner.SpanId)+"-01" {
		t.Fatalf("child callee saw %q, want the inner step span", childSeen.Traceparent)
	}
	_, _, records := collector.snapshot()
	bySpan := map[string]bool{}
	for _, record := range records {
		if id(record.TraceId) != trace || record.Body.GetStringValue() != "trace observed" {
			t.Fatalf("log record outside the trace: %+v", record)
		}
		bySpan[id(record.SpanId)] = true
	}
	if len(records) != wantLogs || wantLogs > 0 && (!bySpan[id(price.SpanId)] || !bySpan[id(inner.SpanId)]) {
		t.Fatalf("logs=%d by span %v, want one per callee step", len(records), bySpan)
	}
	if collector.contains(principalSentinel) {
		t.Fatal("principal reached the collector")
	}
}

func TestTraceLineageAcrossGoStepsAndAChildWorkflow(t *testing.T) {
	collector, exporter, parentSeen, childSeen := runLineage(t, goTraceNode().Any(), 1)
	assertLineage(t, collector, exporter, parentSeen, childSeen, 2)
}

// TestUnsampledLineageStillPropagates: an unsampled trace still reaches the
// callee and the child run with one trace id and flag 00, so they agree not
// to record it; nothing is exported as a span, metrics still count both runs.
func TestUnsampledLineageStillPropagates(t *testing.T) {
	collector, exporter, parentSeen, childSeen := runLineage(t, goTraceNode().Any(), 1e-12)
	if err := exporter.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	parent, err := observe.ParseTraceparent(parentSeen.Traceparent)
	if err != nil {
		t.Fatalf("parent callee saw %q: %v", parentSeen.Traceparent, err)
	}
	child, err := observe.ParseTraceparent(childSeen.Traceparent)
	if err != nil || child.TraceID != parent.TraceID || child.Sampled() || parent.Sampled() {
		t.Fatalf("child %q parent %q", childSeen.Traceparent, parentSeen.Traceparent)
	}
	spans, _, logs := collector.snapshot()
	if len(spans) != 0 || len(logs) != 0 {
		t.Fatalf("unsampled trace exported spans=%d logs=%d", len(spans), len(logs))
	}
	completed := 0.0
	for _, point := range collector.latest(otel.MetricRuns) {
		completed += point.value
	}
	if completed != 2 {
		t.Fatalf("runs metric %v, want both runs", completed)
	}
}

// TestTraceLineageAcrossTheNodeWorkerAndAChildWorkflow runs the same
// composition with price and inner served by the actual Node worker over the
// persistent gRPC protocol: the Node SDK's ctx.trace must name each step span.
func TestTraceLineageAcrossTheNodeWorkerAndAChildWorkflow(t *testing.T) {
	supervisor, descriptors := startNodeWorker(t)
	descriptor, ok := descriptors["fixture/trace"]
	if !ok {
		t.Fatal("Node worker did not discover fixture/trace")
	}
	definition, err := worker.Define[labelInput, traceSeen](supervisor, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	collector, exporter, parentSeen, childSeen := runLineage(t, definition.Any(), 1)
	assertLineage(t, collector, exporter, parentSeen, childSeen, 2)
}

// startNodeWorker starts the actual built Node worker as a supervised
// process. It skips unless BLOK_NODE_INTEGRATION_ROOT names a checkout with
// the worker built (cd runtime/nodejs && npm ci && npm run build).
func startNodeWorker(t *testing.T) (*worker.Supervisor, map[string]node.Descriptor) {
	t.Helper()
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("set BLOK_NODE_INTEGRATION_ROOT to run the actual Node worker lineage scenario")
	}
	nodePath, err := exec.LookPath("node")
	if err != nil {
		t.Fatal(err)
	}
	main := filepath.Join(root, "runtime/nodejs/dist/runtime/nodejs/main.js")
	module := filepath.Join(root, "runtime/nodejs/dist/testdata/worker/nodejs/nodes.js")
	raw, err := exec.Command(nodePath, main, module, "--discover").Output()
	if err != nil {
		t.Fatal(err)
	}
	var discovery struct {
		Nodes         []node.Descriptor `json:"nodes"`
		CatalogDigest string            `json:"catalogDigest"`
	}
	if err := json.Unmarshal(raw, &discovery); err != nil {
		t.Fatal(err)
	}
	digest, err := runtimecontract.CatalogDigest(discovery.Nodes)
	if err != nil || digest != discovery.CatalogDigest {
		t.Fatalf("catalog digest=%q discovered=%q err=%v", digest, discovery.CatalogDigest, err)
	}
	descriptors := map[string]node.Descriptor{}
	for _, candidate := range discovery.Nodes {
		descriptors[candidate.Name] = candidate
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	artifact := runtimecontract.CanonicalDigest([]byte("synthetic-otel-lineage-artifact"))
	token := "synthetic-otel-lineage-token-000000001"
	caps := []runtimecontract.Capability{"http:synthetic"}
	hello := runtimecontract.Hello{Protocol: runtimecontract.ProtocolName, Major: 1, Minor: runtimecontract.ProtocolMinor, ArtifactDigest: artifact, CatalogDigest: digest, Generation: 1, Capabilities: caps, Limits: runtimecontract.DefaultLimits()}
	factory := worker.ProcessFactory{
		Command: nodePath, Args: []string{main, module}, Address: address, Token: token, Principal: "app-1", Capabilities: caps, StartupTimeout: 5 * time.Second,
		Env: []string{"BLOK_WORKER_ADDRESS=" + address, "BLOK_WORKER_TOKEN=" + token, "BLOK_WORKER_PRINCIPAL=app-1", "BLOK_WORKER_ARTIFACT=" + artifact, "BLOK_WORKER_GENERATION=1", `BLOK_WORKER_CAPABILITIES=["http:synthetic"]`},
	}
	supervisor, err := worker.New(worker.Config{Hello: hello, Factory: factory, Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := supervisor.Shutdown(ctx); err != nil {
			t.Errorf("Node worker shutdown: %v", err)
		}
	})
	return supervisor, descriptors
}
