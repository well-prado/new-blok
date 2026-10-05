package trigger_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
	queue "github.com/well-prado/new-blok/trigger/worker"

	internalruntime "github.com/well-prado/new-blok/internal/runtime"
	"github.com/well-prado/new-blok/runtime/worker"
)

// The caller's tracestate is forwarded: to every worker call
// (Call.tracestate, the Node SDK's ctx.trace.tracestate) and to the
// exporter. A credential a caller put in it must never get that far (#285
// review F1). The exporter side is checked in observe/otel.

// sensitiveTracestate is the review's reproduction plus an encoded form only
// the decoding redaction layer (observe/redact) finds.
var sensitiveTracestate = "token=SYNTHETIC-ts-0001,pw=password:hunter2,vendor=ok,enc=" + strings.TrimRight(base64.StdEncoding.EncodeToString([]byte("password=SYNTHETIC-b64-0003")), "=")

func requireClean(t *testing.T, where, state string) {
	t.Helper()
	for _, secret := range []string{"SYNTHETIC", "hunter2", "enc="} {
		if strings.Contains(state, secret) {
			t.Fatalf("%s carries %q: %q", where, secret, state)
		}
	}
	if state != "vendor=ok" {
		t.Fatalf("%s state %q, want only the clean member vendor=ok", where, state)
	}
}

// recordingConnection is a worker connection that records each call's trace
// context: exactly what a Node worker receives as ctx.trace.
type recordingConnection struct {
	mu    sync.Mutex
	calls []runtimecontract.Call
}

func (c *recordingConnection) Call(_ context.Context, call runtimecontract.Call) (runtimecontract.Result, error) {
	c.mu.Lock()
	c.calls = append(c.calls, call)
	c.mu.Unlock()
	return runtimecontract.Result{CallID: call.CallID, AttemptID: call.AttemptID, Generation: call.Generation, Output: []byte(`{"totalCents":3000}`)}, nil
}

func (*recordingConnection) Close(context.Context) error { return nil }

type recordingFactory struct{ conn *recordingConnection }

func (f recordingFactory) Connect(_ context.Context, h runtimecontract.Hello) (internalruntime.Connection, runtimecontract.Ready, error) {
	return f.conn, runtimecontract.Ready{Protocol: h.Protocol, Major: h.Major, Minor: h.Minor, ArtifactDigest: h.ArtifactDigest, CatalogDigest: h.CatalogDigest, Generation: h.Generation, Limits: h.Limits}, nil
}

func TestTraceIngressSensitiveTracestateNeverReachesAWorkerCall(t *testing.T) {
	conn := &recordingConnection{}
	hello := runtimecontract.Hello{Protocol: runtimecontract.ProtocolName, Major: runtimecontract.ProtocolMajor, Minor: runtimecontract.ProtocolMinor,
		ArtifactDigest: "sha256:" + strings.Repeat("a", 64), CatalogDigest: "sha256:" + strings.Repeat("b", 64), Generation: 1, Limits: runtimecontract.DefaultLimits()}
	supervisor, err := worker.New(worker.Config{Hello: hello, Factory: recordingFactory{conn: conn}})
	if err != nil {
		t.Fatal(err)
	}
	remote, err := worker.Define[quote.Input, quote.Output](supervisor, node.Descriptor{Name: "remote/quote", Version: "1.0.0", Description: "Prices a quote in a worker", InputSchema: quote.InputSchema, OutputSchema: quoteOutput, Deterministic: true})
	if err != nil {
		t.Fatal(err)
	}
	defined, err := flow.Define(flow.Spec{Name: "remote/quote", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[quote.Input]) flow.Ref[quote.Output] {
		return flow.Call(b, "price", remote, in)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := defined.Lower()
	if err != nil {
		t.Fatal(err)
	}
	recorder := &runRecorder{roots: map[string]observe.Span{}}
	application, err := app.New(app.Config{Inspection: recorder, Trace: observe.TracePolicy{Ratio: 1}, Dependencies: []app.Dependency{{Name: "worker", Start: supervisor.Start, Close: supervisor.Shutdown}}})
	if err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{"remote/quote": remote.Any()})
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	jobs, err := queue.New(context.Background(), database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	run := func(ctx context.Context, id string, body []byte) (any, error) {
		var in quote.Input
		if err := json.Unmarshal(body, &in); err != nil {
			return nil, err
		}
		result, err := runner.Run(ctx, program, in, inspection.Invocation{RunID: id, Principal: "alice"})
		return result.Output, err
	}
	server, err := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/quotes", InputSchema: quote.InputSchema, Authenticate: bearer, Trace: trigger.TraceIngress{Extract: true, Sampling: observe.HonorInboundSampling}, Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
		return run(ctx, "run-http", in.Body)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = application.Shutdown(ctx)
	}()
	listener := httptest.NewServer(server)
	defer listener.Close()

	// An inbound request.
	request, err := http.NewRequest(http.MethodPost, listener.URL+"/quotes", strings.NewReader(order))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token("alice"))
	request.Header.Set(trigger.TraceparentField, traceparentOf(inboundTraceID, "01"))
	request.Header.Set(trigger.TracestateField, sensitiveTracestate)
	response, err := listener.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("%d %s", response.StatusCode, body)
	}

	// A trusted producer's job, whose trace came from somewhere else.
	parent, err := observe.ParseTraceparent(traceparentOf(inboundTraceID, "01"))
	if err != nil {
		t.Fatal(err)
	}
	parent.State = sensitiveTracestate
	enqueued, err := jobs.Enqueue(context.Background(), queue.EnqueueRequest{RequestKey: "job-1", Kind: "quote", Payload: []byte(order), Trace: parent})
	if err != nil {
		t.Fatal(err)
	}
	var claimed queue.Job
	if _, err := jobs.ProcessOnce(context.Background(), func(ctx context.Context, _ queue.Tx, job queue.Job) error {
		claimed = job
		_, err := run(ctx, "run-job", job.Payload)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	conn.mu.Lock()
	calls := append([]runtimecontract.Call(nil), conn.calls...)
	conn.mu.Unlock()
	if len(calls) == 0 {
		t.Fatal("no worker call")
	}
	requireClean(t, "the worker call of the HTTP run", calls[0].Tracestate)
	for _, call := range calls {
		if !strings.Contains(call.Traceparent, inboundTraceID) {
			t.Fatalf("the worker call left the inbound trace: %q", call.Traceparent)
		}
		requireClean(t, "the worker call", call.Tracestate)
	}
	if len(calls) != 2 {
		t.Fatalf("%d worker calls, want 2", len(calls))
	}
	requireClean(t, "the enqueued job", enqueued.Job.Trace.State)
	requireClean(t, "the claimed job", claimed.Trace.State)
	requireJoined(t, "http", recorder.root(t, "run-http"), inboundTraceID, "vendor=ok")
}
