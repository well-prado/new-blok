package trigger_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	bgrpc "github.com/well-prado/new-blok/trigger/grpc"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
	"github.com/well-prado/new-blok/trigger/pubsub"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
	"github.com/well-prado/new-blok/trigger/webhook"
	bws "github.com/well-prado/new-blok/trigger/websocket"
	"github.com/well-prado/new-blok/trigger/worker"
)

// These tests drive every trigger that can carry an inbound W3C trace
// context (#276) end to end: a real request through the adapter, durable
// work through the real worker.Queue, and the run through execution.Runner
// with an inspection observer and a TracePolicy, so the root span checked is
// the one the engine allocated (ADR 0020).

const (
	inboundTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	inboundSpanID  = "00f067aa0ba902b7"
	inboundState   = "vendor=opaque"
	// craftedTraceID ends in eight zero bytes: OpenTelemetry's ratio
	// arithmetic, applied to it, samples it under any ratio above zero.
	craftedTraceID = "4bf92f3577b34da60000000000000000"
)

func traceparentOf(traceID, flags string) string {
	return "00-" + traceID + "-" + inboundSpanID + "-" + flags
}

// carrier is the trace context one request carries: every traceparent and
// tracestate value as sent, so a duplicated header is two entries.
type carrier struct {
	traceparent, tracestate []string
}

func validCarrier() carrier {
	return carrier{traceparent: []string{traceparentOf(inboundTraceID, "01")}, tracestate: []string{inboundState}}
}

// ingressTriggers carry a trace context in their protocol. Cron has no
// upstream and the worker's producers pass a TraceContext, not a carrier;
// both are covered separately.
var ingressTriggers = []string{"http", "grpc", "websocket", "mcp", "webhook", "sse", "pubsub"}

// runRecorder is the inspection observer: it keeps every run's root span.
type runRecorder struct {
	mu    sync.Mutex
	roots map[string]observe.Span
}

func (r *runRecorder) Observe(event inspection.Event) {
	if event.Kind != inspection.RunStarted {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.roots[event.RunID] = event.Trace
}

func (r *runRecorder) root(t *testing.T, runID string) observe.Span {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	span, ok := r.roots[runID]
	if !ok {
		t.Fatalf("run %s was never observed", runID)
	}
	return span
}

type tracedRun struct {
	id, via, principal string
	input              quote.Input
	output             json.RawMessage
}

// tracedFlow runs the quote workflow through execution.Runner, so every run
// is observed and traced under the application's policy.
type tracedFlow struct {
	runner  *execution.Runner
	program contract.InternalProgram
	mu      sync.Mutex
	runs    []tracedRun
}

func newTracedFlow(t *testing.T, application *app.Application) *tracedFlow {
	t.Helper()
	calculate, err := node.Define("trace/calculate-quote", "1.0.0", func(_ context.Context, in quote.Input) (quote.Output, error) {
		return quote.Output{TotalCents: int64(in.Quantity) * 1500}, nil
	}, node.Description("Prices a synthetic quote"), node.Schemas(quote.InputSchema, quoteOutput), node.Pure())
	if err != nil {
		t.Fatal(err)
	}
	defined, err := flow.Define(flow.Spec{Name: "trace/quote", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[quote.Input]) flow.Ref[quote.Output] {
		return flow.Call(b, "calculate", calculate, in)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := defined.Lower()
	if err != nil {
		t.Fatal(err)
	}
	return &tracedFlow{runner: execution.NewRunner(application, map[string]node.Any{"trace/calculate-quote": calculate.Any()}), program: program}
}

// run starts one run. Its invocation carries no Trace: the parent can only
// come from the context the adapter handed over.
func (f *tracedFlow) run(ctx context.Context, via, principal string, input []byte) (json.RawMessage, error) {
	var in quote.Input
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	f.mu.Lock()
	id := fmt.Sprintf("run-%s-%d", via, len(f.runs))
	f.mu.Unlock()
	result, err := f.runner.Run(ctx, f.program, in, inspection.Invocation{RunID: id, Principal: principal})
	if err != nil {
		return nil, err
	}
	output, err := json.Marshal(result.Output)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	f.runs = append(f.runs, tracedRun{id: id, via: via, principal: principal, input: in, output: output})
	f.mu.Unlock()
	return output, nil
}

func (f *tracedFlow) count() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.runs) }

func (f *tracedFlow) since(n int) []tracedRun {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]tracedRun(nil), f.runs[n:]...)
}

// recordingSubmitter records every durable submission and its result.
type recordingSubmitter struct {
	queue *worker.Queue
	mu    sync.Mutex
	seen  []submitted
}

type submitted struct {
	submission trigger.Submission
	accepted   bool
	err        error
}

func (r *recordingSubmitter) Submit(ctx context.Context, submission trigger.Submission) (bool, error) {
	accepted, err := r.queue.Submit(ctx, submission)
	r.mu.Lock()
	r.seen = append(r.seen, submitted{submission: submission, accepted: accepted, err: err})
	r.mu.Unlock()
	return accepted, err
}

func (r *recordingSubmitter) last(t *testing.T) submitted {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.seen) == 0 {
		t.Fatal("nothing was submitted")
	}
	return r.seen[len(r.seen)-1]
}

// traceMessage is a broker message with headers (pubsub.TraceCarrier).
type traceMessage struct {
	memMessage
	carrier carrier
}

func (m *traceMessage) TraceHeaders() ([]string, []string) {
	return m.carrier.traceparent, m.carrier.tracestate
}

type traceBroker struct {
	mu      sync.Mutex
	pending []pubsub.Message
}

func (b *traceBroker) Fetch(_ context.Context, max int, _ time.Duration) ([]pubsub.Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	batch := b.pending[:min(max, len(b.pending))]
	b.pending = b.pending[len(batch):]
	return batch, nil
}

func (*traceBroker) DeadLetter(context.Context, pubsub.Message, string) error {
	return errors.New("unexpected dead letter")
}

func (b *traceBroker) publish(id string, c carrier) *traceMessage {
	m := &traceMessage{memMessage: memMessage{id: id, data: []byte(order), acked: make(chan struct{})}, carrier: c}
	b.mu.Lock()
	b.pending = append(b.pending, m)
	b.mu.Unlock()
	return m
}

// headerTransport adds a request's trace headers to every MCP HTTP request.
type headerTransport struct {
	base   http.RoundTripper
	header http.Header
}

func (h headerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	for name, values := range h.header {
		for _, value := range values {
			request.Header.Add(name, value)
		}
	}
	return h.base.RoundTrip(request)
}

type tracedHost struct {
	t          *testing.T
	recorder   *runRecorder
	app        *app.Application
	flow       *tracedFlow
	database   store.Database
	queue      *worker.Queue
	submit     *recordingSubmitter
	broker     *traceBroker
	consumer   *pubsub.Consumer
	secret     webhook.Secret
	server     *httptest.Server
	method     protoreflect.MethodDescriptor
	grpcServer *grpc.Server
	conn       *grpc.ClientConn
	streams    *sse.Server
	sockets    *bws.Server
	tools      *tmcp.Server
}

// newTracedHost is one application with every carrier-bearing trigger, each
// configured with ingress, and a TracePolicy of ratio.
func newTracedHost(t *testing.T, ingress trigger.TraceIngress, ratio float64) *tracedHost {
	t.Helper()
	ctx := context.Background()
	h := &tracedHost{t: t, recorder: &runRecorder{roots: map[string]observe.Span{}}, broker: &traceBroker{}}
	var err error
	if h.app, err = app.New(app.Config{DrainTimeout: 10 * time.Second, Inspection: h.recorder, Trace: observe.TracePolicy{Ratio: ratio}}); err != nil {
		t.Fatal(err)
	}
	h.flow = newTracedFlow(t, h.app)
	if h.database, err = (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "trace.db")); err != nil {
		t.Fatal(err)
	}
	if h.queue, err = worker.New(ctx, h.database, time.Now); err != nil {
		t.Fatal(err)
	}
	for _, via := range []string{"webhook", "sse", "pubsub", "worker", "cron"} {
		if err := h.queue.RegisterKind("quote."+via, quote.InputSchema); err != nil {
			t.Fatal(err)
		}
	}
	h.submit = &recordingSubmitter{queue: h.queue}
	if h.consumer, err = pubsub.New(h.broker, pubsub.Subscription{Name: "orders", Principal: trigger.Principal{ID: "publisher"}, Kind: "quote.pubsub", Submit: h.submit, InputSchema: quote.InputSchema, FetchWait: time.Millisecond, Trace: ingress, TracePolicy: h.app.TracePolicy()}); err != nil {
		t.Fatal(err)
	}
	httpServer, err := blokhttp.New(h.app, []blokhttp.Endpoint{{Method: "POST", Path: "/quotes", InputSchema: quote.InputSchema, Authenticate: bearer, Trace: ingress, Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
		return h.flow.run(ctx, "http", in.Principal.ID, in.Body)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if h.secret, err = webhook.NewSecret([]byte("synthetic-webhook-secret-0123456789")); err != nil {
		t.Fatal(err)
	}
	webhooks, err := webhook.New(h.app, time.Now, []webhook.Endpoint{{Path: "/webhooks/shop", Provider: "shop", Principal: trigger.Principal{ID: "shop"}, Kind: "quote.webhook", Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "k1", Secret: h.secret}}}, Submit: h.submit, InputSchema: quote.InputSchema, Trace: ingress}})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := sse.NewHub(sse.HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if h.streams, err = sse.New(h.app, hub, []sse.Endpoint{{Name: "orders", Path: "/orders", Kind: "quote.sse", Submit: h.submit, Tracker: h.queue, Authenticate: bearer, InputSchema: quote.InputSchema, Trace: ingress}}); err != nil {
		t.Fatal(err)
	}
	if h.sockets, err = bws.New(h.app, bws.Endpoint{Path: "/ws", Authenticate: bearer, InputSchema: quote.InputSchema, Trace: ingress, OnMessage: func(ctx context.Context, m bws.Message) (json.RawMessage, error) {
		return h.flow.run(ctx, "websocket", m.Connection.Principal.ID, m.Input)
	}}); err != nil {
		t.Fatal(err)
	}
	budget := tool.Budget{MaxDepth: 4, MaxInputBytes: 1 << 10, MaxOutputBytes: 1 << 10, MaxTokens: 1, MaxCalls: 1, Deadline: time.Now().Add(time.Hour)}
	if h.tools, err = tmcp.New(h.app, tmcp.Config{Name: "trace", Version: "1.0.0", Catalog: tracedCatalog{flow: h.flow}, Authenticate: func(_ context.Context, credential string, _ *http.Request) (tool.Principal, error) {
		principal, err := principalOf("Bearer " + credential)
		return tool.Principal{ID: principal.ID, MaxDepth: 4}, err
	}, Expose: []string{"selection/quote@1.0.0"}, Budget: budget, Trace: ingress}); err != nil {
		t.Fatal(err)
	}
	h.method = quoteMethod(t)
	methods, err := bgrpc.New(h.app, func(ctx context.Context) (trigger.Principal, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("authorization"); len(values) == 1 {
			return principalOf(values[0])
		}
		return trigger.Principal{}, errors.New("unauthenticated")
	}, []bgrpc.Binding{{Method: h.method, Workflow: "selection/quote", WorkflowInput: quote.InputSchema, InputSchema: quote.InputSchema, OutputSchema: quoteOutput, Authorize: bgrpc.AllowAuthenticated, Trace: ingress, Handle: func(ctx context.Context, call bgrpc.Call) (json.RawMessage, error) {
		return h.flow.run(ctx, "grpc", call.Principal.ID, call.Input)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	h.grpcServer = grpc.NewServer(methods.ServerOptions()...)
	if err := methods.Register(h.grpcServer); err != nil {
		t.Fatal(err)
	}
	if err := h.app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/quotes", httpServer)
	mux.Handle("/webhooks/", webhooks)
	mux.Handle("/orders", h.streams)
	mux.Handle("/orders/", h.streams)
	mux.Handle("/ws", h.sockets)
	mux.Handle("/mcp", h.tools)
	h.server = httptest.NewServer(mux)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- h.grpcServer.Serve(listener) }()
	if h.conn, err = grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := errors.Join(h.streams.Shutdown(ctx), h.sockets.Shutdown(ctx), h.tools.Shutdown(ctx)); err != nil {
			t.Errorf("adapter shutdown: %v", err)
		}
		h.grpcServer.GracefulStop()
		_ = h.conn.Close()
		<-served
		h.server.Close()
		if err := h.app.Shutdown(ctx); err != nil {
			t.Errorf("application shutdown: %v", err)
		}
		if err := h.database.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})
	return h
}

// tracedCatalog exposes the traced workflow as one MCP tool.
type tracedCatalog struct{ flow *tracedFlow }

func (tracedCatalog) List(context.Context, tool.Principal) ([]tmcp.Tool, error) {
	return []tmcp.Tool{{Name: "selection/quote", Version: "1.0.0", Description: "Prices a synthetic quote", InputSchema: quote.InputSchema, OutputSchema: quoteOutput}}, nil
}

func (c tracedCatalog) Invoke(ctx context.Context, principal tool.Principal, call tmcp.Call) (json.RawMessage, error) {
	return c.flow.run(ctx, "mcp", principal.ID, call.Input)
}

func (c carrier) header() http.Header {
	header := http.Header{}
	for _, value := range c.traceparent {
		header.Add(trigger.TraceparentField, value)
	}
	for _, value := range c.tracestate {
		header.Add(trigger.TracestateField, value)
	}
	return header
}

// representable reports whether a transport can carry the carrier at all:
// gRPC clients refuse metadata values outside printable ASCII.
func (c carrier) representable(via string) bool {
	if via != "grpc" {
		return true
	}
	for _, value := range append(append([]string(nil), c.traceparent...), c.tracestate...) {
		for i := 0; i < len(value); i++ {
			if value[i] < 0x20 || value[i] > 0x7e {
				return false
			}
		}
	}
	return true
}

// request is one request through a trigger: who sends it, its identity for
// keyed triggers, and its trace carrier.
type request struct {
	via       string
	principal string
	id        string
	carrier   carrier
	// forge breaks the request's authentication (a wrong token or
	// signature), to check that a trace context cannot change a refusal.
	forge bool
}

// result is what a request produced: what its source saw, the durable
// submission it made, and the runs it started.
type result struct {
	outcome    string
	submission *submitted
	runs       []tracedRun
}

// send makes one request, processes every job it queued and returns what it
// produced.
func (h *tracedHost) send(r request) result {
	h.t.Helper()
	if r.principal == "" {
		r.principal = "alice"
	}
	before, submissions := h.flow.count(), h.submissions()
	outcome := h.deliver(r)
	h.drain()
	out := result{outcome: outcome, runs: h.flow.since(before)}
	if after := h.submissions(); after > submissions {
		last := h.submit.last(h.t)
		out.submission = &last
	}
	return out
}

func (h *tracedHost) submissions() int {
	h.submit.mu.Lock()
	defer h.submit.mu.Unlock()
	return len(h.submit.seen)
}

func (h *tracedHost) token(r request) string {
	if r.forge {
		return "Bearer forged"
	}
	return "Bearer " + token(r.principal)
}

func (h *tracedHost) deliver(r request) string {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch r.via {
	case "http", "sse", "webhook":
		path, header := "/quotes", r.carrier.header()
		header.Set("Content-Type", "application/json")
		switch r.via {
		case "http":
			header.Set("Authorization", h.token(r))
		case "sse":
			path = "/orders"
			header.Set("Authorization", h.token(r))
			header.Set("Idempotency-Key", r.id)
		case "webhook":
			path = "/webhooks/shop"
			now := time.Now()
			secret := h.secret
			if r.forge {
				secret, _ = webhook.NewSecret([]byte("another-synthetic-secret-0123456789"))
			}
			header.Set("webhook-id", r.id)
			header.Set("webhook-timestamp", fmt.Sprint(now.Unix()))
			header.Set("webhook-signature", webhook.SignStandard(secret, r.id, now, []byte(order)))
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.server.URL+path, strings.NewReader(order))
		if err != nil {
			h.t.Fatal(err)
		}
		request.Header = header
		response, err := h.server.Client().Do(request)
		if err != nil {
			h.t.Fatal(err)
		}
		defer response.Body.Close()
		body, _ := io.ReadAll(response.Body)
		return fmt.Sprintf("%d %s", response.StatusCode, scrubRequestID(body))
	case "grpc":
		md := metadata.MD{"authorization": {h.token(r)}}
		if len(r.carrier.traceparent) > 0 {
			md[trigger.TraceparentField] = r.carrier.traceparent
		}
		if len(r.carrier.tracestate) > 0 {
			md[trigger.TracestateField] = r.carrier.tracestate
		}
		in := dynamicpb.NewMessage(h.method.Input())
		in.Set(h.method.Input().Fields().ByName("sku"), protoreflect.ValueOfString("coffee"))
		in.Set(h.method.Input().Fields().ByName("quantity"), protoreflect.ValueOfInt32(2))
		reply := dynamicpb.NewMessage(h.method.Output())
		if err := h.conn.Invoke(metadata.NewOutgoingContext(ctx, md), "/nine.Quotes/Quote", in, reply); err != nil {
			return "error " + err.Error()
		}
		return fmt.Sprintf("ok %d", reply.Get(h.method.Output().Fields().ByName("totalCents")).Int())
	case "websocket":
		header := r.carrier.header()
		header.Set("Authorization", h.token(r))
		socket, response, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(h.server.URL, "http")+"/ws", &websocket.DialOptions{HTTPClient: h.server.Client(), HTTPHeader: header})
		if err != nil {
			status := 0
			if response != nil {
				status = response.StatusCode
			}
			return fmt.Sprintf("refused %d", status)
		}
		defer socket.CloseNow()
		output, err := wsQuote(ctx, socket, "m1")
		if err != nil {
			return "error " + err.Error()
		}
		return "ok " + string(output)
	case "mcp":
		client := sdk.NewClient(&sdk.Implementation{Name: "trace", Version: "1.0.0"}, nil)
		transport := headerTransport{base: bearerTransport{base: h.server.Client().Transport, token: strings.TrimPrefix(h.token(r), "Bearer ")}, header: r.carrier.header()}
		session, err := client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: h.server.URL + "/mcp", HTTPClient: &http.Client{Transport: transport}, MaxRetries: -1}, nil)
		if err != nil {
			return "refused"
		}
		defer session.Close()
		output, err := mcpQuote(ctx, session)
		if err != nil {
			return "error " + err.Error()
		}
		return "ok " + string(output)
	case "pubsub":
		message := h.broker.publish(r.id, r.carrier)
		var outcome pubsub.Outcome
		h.consumer.Observe(func(o pubsub.Outcome) { outcome = o })
		if _, err := h.consumer.ProcessBatch(ctx); err != nil {
			h.t.Fatal(err)
		}
		select {
		case <-message.acked:
		default:
			return "unacknowledged " + outcome.Result
		}
		return outcome.Result + " " + outcome.Reason
	}
	h.t.Fatalf("no trigger %s", r.via)
	return ""
}

// scrubRequestID removes the per-response request id from an error body,
// the one field two otherwise identical answers differ in.
func scrubRequestID(body []byte) string {
	var fields map[string]any
	if json.Unmarshal(body, &fields) != nil {
		return strings.TrimSpace(string(body))
	}
	delete(fields, "requestId")
	encoded, _ := json.Marshal(fields)
	return string(encoded)
}

// drain processes every queued job: a durable trigger's run happens here.
func (h *tracedHost) drain() {
	h.t.Helper()
	for {
		processed, err := h.queue.ProcessOnce(context.Background(), func(ctx context.Context, _ worker.Tx, job worker.Job) error {
			_, err := h.flow.run(ctx, strings.TrimPrefix(job.Kind, "quote."), job.Principal.ID, job.Payload)
			return err
		})
		if err != nil {
			h.t.Fatal(err)
		}
		if !processed {
			return
		}
	}
}

// only returns the one run a request started.
func (h *tracedHost) only(r request, res result) tracedRun {
	h.t.Helper()
	if len(res.runs) != 1 {
		h.t.Fatalf("%s: %d runs, want 1 (outcome %q)", r.via, len(res.runs), res.outcome)
	}
	return res.runs[0]
}

func hexTrace(t *testing.T, value string) observe.TraceID {
	t.Helper()
	parsed, err := observe.ParseTraceparent("00-" + value + "-" + inboundSpanID + "-01")
	if err != nil {
		t.Fatal(err)
	}
	return parsed.TraceID
}

func inboundSpan(t *testing.T) observe.SpanID {
	t.Helper()
	parsed, err := observe.ParseTraceparent(traceparentOf(inboundTraceID, "01"))
	if err != nil {
		t.Fatal(err)
	}
	return parsed.SpanID
}

// requireJoined fails unless root is a child of the inbound span, in the
// inbound trace.
func requireJoined(t *testing.T, via string, root observe.Span, traceID string, state string) {
	t.Helper()
	if root.TraceID != hexTrace(t, traceID) || root.Parent != inboundSpan(t) || root.SpanID == inboundSpan(t) || root.State != state {
		t.Fatalf("%s: root span %+v did not join inbound %s/%s with state %q", via, root, traceID, inboundSpanID, state)
	}
}

// requireFresh fails unless root started a trace of its own.
func requireFresh(t *testing.T, via string, root observe.Span) {
	t.Helper()
	if !root.TraceID.IsValid() || root.Parent.IsValid() || root.TraceID == hexTrace(t, inboundTraceID) || root.State != "" {
		t.Fatalf("%s: root span %+v is not a fresh trace", via, root)
	}
}

func TestTraceIngressRootSpanJoinsInboundParentOnEveryTrigger(t *testing.T) {
	h := newTracedHost(t, trigger.TraceIngress{Extract: true}, 1)
	for i, via := range ingressTriggers {
		r := request{via: via, id: fmt.Sprintf("joined-%d", i), carrier: validCarrier()}
		res := h.send(r)
		run := h.only(r, res)
		if run.via != via || string(run.output) != `{"totalCents":3000}` {
			t.Fatalf("%s: run %+v", via, run)
		}
		requireJoined(t, via, h.recorder.root(t, run.id), inboundTraceID, inboundState)
	}
}

func TestTraceIngressIsOffByDefault(t *testing.T) {
	h := newTracedHost(t, trigger.TraceIngress{}, 1)
	for i, via := range ingressTriggers {
		r := request{via: via, id: fmt.Sprintf("off-%d", i), carrier: validCarrier()}
		res := h.send(r)
		requireFresh(t, via, h.recorder.root(t, h.only(r, res).id))
		if res.submission != nil && res.submission.submission.Trace != (observe.TraceContext{}) {
			t.Fatalf("%s submitted a trace with extraction off: %+v", via, res.submission.submission.Trace)
		}
	}
}

// TestTraceIngressExtractsNothingWhenTheApplicationDoesNotTrace: with no
// TracePolicy nothing inbound is propagated, even with Extract set.
func TestTraceIngressExtractsNothingWhenTheApplicationDoesNotTrace(t *testing.T) {
	h := newTracedHost(t, trigger.TraceIngress{Extract: true, Sampling: observe.HonorInboundSampling}, 0)
	for i, via := range []string{"webhook", "sse", "pubsub"} {
		r := request{via: via, id: fmt.Sprintf("untraced-%d", i), carrier: validCarrier()}
		res := h.send(r)
		h.only(r, res)
		if res.submission == nil || res.submission.submission.Trace != (observe.TraceContext{}) {
			t.Fatalf("%s submitted %+v without a trace policy", via, res.submission)
		}
	}
}

func TestTraceIngressIgnoredSamplingCannotBeForcedByTheCaller(t *testing.T) {
	// A ratio far below one in a billion samples none of these runs unless
	// the caller decides. The crafted trace id is one OpenTelemetry's ratio
	// arithmetic would sample: the local decision must not be derived from
	// it.
	forced := carrier{traceparent: []string{traceparentOf(craftedTraceID, "01")}}
	for _, policy := range []struct {
		name     string
		sampling observe.InboundSampling
		want     bool
	}{{"ignore", observe.IgnoreInboundSampling, false}, {"honor", observe.HonorInboundSampling, true}} {
		t.Run(policy.name, func(t *testing.T) {
			h := newTracedHost(t, trigger.TraceIngress{Extract: true, Sampling: policy.sampling}, 1e-12)
			for i, via := range ingressTriggers {
				r := request{via: via, id: fmt.Sprintf("forced-%d", i), carrier: forced}
				root := h.recorder.root(t, h.only(r, h.send(r)).id)
				requireJoined(t, via, root, craftedTraceID, "")
				if root.Sampled() != policy.want {
					t.Fatalf("%s under %s: sampled=%v, want %v", via, policy.name, root.Sampled(), policy.want)
				}
			}
		})
	}
}

// TestTraceIngressForcedSamplingDoesNotRaiseTheSampleRate measures the rate:
// every request asks to be sampled, half with a crafted trace id.
func TestTraceIngressForcedSamplingDoesNotRaiseTheSampleRate(t *testing.T) {
	const requests, ratio = 400, 0.25
	for _, policy := range []struct {
		name     string
		sampling observe.InboundSampling
		min, max int
	}{{"ignore", observe.IgnoreInboundSampling, 50, 150}, {"honor", observe.HonorInboundSampling, requests, requests}} {
		t.Run(policy.name, func(t *testing.T) {
			h := newTracedHost(t, trigger.TraceIngress{Extract: true, Sampling: policy.sampling}, ratio)
			sampled := 0
			for i := range requests {
				traceID := observe.NewTraceID().String()
				if i%2 == 0 {
					traceID = traceID[:16] + strings.Repeat("0", 16)
				}
				r := request{via: "http", carrier: carrier{traceparent: []string{traceparentOf(traceID, "01")}}}
				root := h.recorder.root(t, h.only(r, h.send(r)).id)
				requireJoined(t, "http", root, traceID, "")
				if root.Sampled() {
					sampled++
				}
			}
			// Binomial(400, 0.25): mean 100, standard deviation 8.7.
			if sampled < policy.min || sampled > policy.max {
				t.Fatalf("%s: %d of %d forced-sampled requests sampled, want %d..%d", policy.name, sampled, requests, policy.min, policy.max)
			}
			t.Logf("%s: %d of %d forced-sampled requests sampled at ratio %v", policy.name, sampled, requests, ratio)
		})
	}
}

// malformedTraceparents are carriers no run may join.
func malformedTraceparents() map[string]carrier {
	valid := traceparentOf(inboundTraceID, "01")
	other := "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	return map[string]carrier{
		"duplicate traceparent":           {traceparent: []string{valid, other}},
		"duplicate identical traceparent": {traceparent: []string{valid, valid}},
		"comma-joined traceparents":       {traceparent: []string{valid + "," + other}},
		"version ff":                      {traceparent: []string{"ff" + valid[2:]}},
		"upper-case hex":                  {traceparent: []string{strings.ToUpper(valid)}},
		"zero trace id":                   {traceparent: []string{traceparentOf(strings.Repeat("0", 32), "01")}},
		"zero span id":                    {traceparent: []string{"00-" + inboundTraceID + "-0000000000000000-01"}},
		"non-ASCII":                       {traceparent: []string{"00-" + inboundTraceID[:31] + "é-" + inboundSpanID + "-01"}},
		"oversized future version":        {traceparent: []string{"01" + valid[2:] + "-" + strings.Repeat("a", observe.MaxTraceparentBytes)}},
		"truncated":                       {traceparent: []string{valid[:54]}},
		"empty":                           {traceparent: []string{""}},
		"tracestate without traceparent":  {tracestate: []string{inboundState}},
	}
}

// malformedTracestates keep a valid traceparent; only the tracestate is
// dropped.
func malformedTracestates() map[string]carrier {
	valid := []string{traceparentOf(inboundTraceID, "01")}
	return map[string]carrier{
		"tracestate over the limit": {traceparent: valid, tracestate: []string{"vendor=" + strings.Repeat("x", observe.MaxTracestateBytes)}},
		"duplicate tracestate":      {traceparent: valid, tracestate: []string{"a=1", "b=2"}},
		"non-ASCII tracestate":      {traceparent: valid, tracestate: []string{"vendor=é"}},
	}
}

func TestTraceIngressIgnoresMalformedCarriersWithoutError(t *testing.T) {
	h := newTracedHost(t, trigger.TraceIngress{Extract: true, Sampling: observe.HonorInboundSampling}, 1)
	id := 0
	check := func(name string, c carrier, joined bool) {
		for _, via := range ingressTriggers {
			if !c.representable(via) {
				t.Logf("%s: %s cannot be sent over %s; covered by the other transports", name, strings.Join(c.traceparent, "|"), via)
				continue
			}
			id++
			r := request{via: via, id: fmt.Sprintf("malformed-%d", id), carrier: c}
			res := h.send(r)
			run := h.only(r, res)
			if string(run.output) != `{"totalCents":3000}` {
				t.Fatalf("%s via %s: output %s", name, via, run.output)
			}
			root := h.recorder.root(t, run.id)
			if joined {
				requireJoined(t, via+" "+name, root, inboundTraceID, "")
			} else {
				requireFresh(t, via+" "+name, root)
			}
		}
	}
	for name, c := range malformedTraceparents() {
		check(name, c, false)
	}
	for name, c := range malformedTracestates() {
		check(name, c, true)
	}
}

// TestTraceIngressDoesNotChangeOutcomeEffectsOrIdempotency is the
// differential check: the same request with and without a forged trace
// context is answered the same, runs the same work once, and is the same
// durable submission.
func TestTraceIngressDoesNotChangeOutcomeEffectsOrIdempotency(t *testing.T) {
	forged := []carrier{
		{traceparent: []string{traceparentOf(craftedTraceID, "01")}, tracestate: []string{"admin=true,role=root"}},
		{traceparent: []string{traceparentOf(inboundTraceID, "00")}},
		malformedTraceparents()["duplicate traceparent"],
		malformedTracestates()["tracestate over the limit"],
	}
	for _, sampling := range []observe.InboundSampling{observe.IgnoreInboundSampling, observe.HonorInboundSampling} {
		h := newTracedHost(t, trigger.TraceIngress{Extract: true, Sampling: sampling}, 1)
		id := 0
		next := func() string { id++; return fmt.Sprintf("diff-%d-%d", sampling, id) }
		for _, via := range ingressTriggers {
			plain := request{via: via, id: next()}
			baseline := h.send(plain)
			want := h.only(plain, baseline)
			// Pubsub has no caller to refuse: its publishers are trusted
			// through the broker (ADR 0005).
			refuses := via != "pubsub"
			var refusal result
			if refuses {
				refusal = h.send(request{via: via, id: next(), forge: true})
			}
			for index, c := range forged {
				if !c.representable(via) {
					continue
				}
				traced := request{via: via, id: next(), carrier: c}
				got := h.send(traced)
				run := h.only(traced, got)
				if normalize(via, got.outcome) != normalize(via, baseline.outcome) || run.input != want.input || string(run.output) != string(want.output) || run.principal != want.principal {
					t.Fatalf("%s forged[%d]: outcome %q run %+v, want %q run %+v", via, index, got.outcome, run, baseline.outcome, want)
				}
				// A forged context cannot turn a refusal into work.
				if refuses {
					refused := h.send(request{via: via, id: next(), carrier: c, forge: true})
					if refused.outcome != refusal.outcome || len(refused.runs) != 0 || len(refusal.runs) != 0 || refused.submission != nil {
						t.Fatalf("%s forged[%d]: refusal %q (%d runs), want %q", via, index, refused.outcome, len(refused.runs), refusal.outcome)
					}
				}
				if got.submission == nil {
					continue
				}
				// Same durable identity, and a repeat that differs only in its
				// trace context is a duplicate, never a conflict or a second run.
				if got.submission.submission.Kind != baseline.submission.submission.Kind || string(got.submission.submission.Payload) != string(baseline.submission.submission.Payload) || got.submission.submission.Principal.ID != baseline.submission.submission.Principal.ID ||
					baseline.submission.submission.Key != keyOf(via, plain) || got.submission.submission.Key != keyOf(via, traced) {
					t.Fatalf("%s forged[%d]: submission %+v, baseline %+v", via, index, got.submission.submission, baseline.submission.submission)
				}
				first, err := h.queue.Get(context.Background(), got.submission.submission.Key)
				if err != nil {
					t.Fatal(err)
				}
				for _, repeat := range []carrier{{}, validCarrier(), c} {
					again := h.send(request{via: via, id: traced.id, carrier: repeat})
					if again.submission == nil || again.submission.err != nil || again.submission.accepted || len(again.runs) != 0 {
						t.Fatalf("%s forged[%d]: repeat with another trace was %+v (%d runs), want a duplicate", via, index, again.submission, len(again.runs))
					}
					stored, err := h.queue.Get(context.Background(), got.submission.submission.Key)
					if err != nil || stored.Trace != first.Trace || stored.Attempt != first.Attempt {
						t.Fatalf("%s forged[%d]: a duplicate changed the job: %+v -> %+v (%v)", via, index, first, stored, err)
					}
				}
				// The baseline's own repeat, now carrying a trace, is a duplicate too.
				again := h.send(request{via: via, id: plain.id, carrier: c})
				if again.submission == nil || again.submission.err != nil || again.submission.accepted || len(again.runs) != 0 {
					t.Fatalf("%s forged[%d]: traced repeat of an untraced event was %+v, want a duplicate", via, index, again.submission)
				}
			}
		}
	}
}

// normalize removes what legitimately differs between two requests that
// differ only in their identity: the SSE stream id derives from the key.
func normalize(via, outcome string) string {
	if via != "sse" {
		return outcome
	}
	status, body, _ := strings.Cut(outcome, " ")
	var fields map[string]any
	if json.Unmarshal([]byte(body), &fields) == nil {
		if _, ok := fields["stream"]; ok {
			fields["stream"] = "<stream>"
		}
		encoded, _ := json.Marshal(fields)
		body = string(encoded)
	}
	return status + " " + body
}

// keyOf is a request's durable identity computed from its identity alone,
// with each adapter's own key function: the trace context contributes
// nothing to it.
func keyOf(via string, r request) string {
	switch via {
	case "webhook":
		return webhook.SubmissionKey("shop", r.id)
	case "sse":
		return sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, r.id)
	case "pubsub":
		return pubsub.SubmissionKey("orders", r.id)
	}
	return ""
}

// TestTraceIngressWorkerJobCarriesItsProducersTrace: a trusted producer
// passes a TraceContext with the job; the run the job starts joins it.
func TestTraceIngressWorkerJobCarriesItsProducersTrace(t *testing.T) {
	h := newTracedHost(t, trigger.TraceIngress{}, 1)
	parent, err := observe.ParseTraceparent(traceparentOf(inboundTraceID, "01"))
	if err != nil {
		t.Fatal(err)
	}
	parent.State = inboundState
	before := h.flow.count()
	if _, err := h.queue.Enqueue(context.Background(), worker.EnqueueRequest{RequestKey: "worker-traced", Kind: "quote.worker", Payload: []byte(order), Principal: trigger.Principal{ID: "producer"}, Trace: parent}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.queue.Enqueue(context.Background(), worker.EnqueueRequest{RequestKey: "worker-plain", Kind: "quote.worker", Payload: []byte(order), Principal: trigger.Principal{ID: "producer"}}); err != nil {
		t.Fatal(err)
	}
	h.drain()
	runs := h.flow.since(before)
	if len(runs) != 2 {
		t.Fatalf("%d runs", len(runs))
	}
	requireJoined(t, "worker", h.recorder.root(t, runs[0].id), inboundTraceID, inboundState)
	requireFresh(t, "worker", h.recorder.root(t, runs[1].id))
}

// TestTraceIngressCronHasNoUpstreamTrace: a cron occurrence has no caller,
// so its submission carries no trace even when the scheduler's own context
// is traced, and its run starts a trace of its own.
func TestTraceIngressCronHasNoUpstreamTrace(t *testing.T) {
	h := newTracedHost(t, trigger.TraceIngress{Extract: true, Sampling: observe.HonorInboundSampling}, 1)
	clock := &cronClock{now: occurrence.Add(-time.Minute)}
	scheduler, err := cron.New(context.Background(), h.database, h.submit, h.queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Add(context.Background(), cron.Schedule{Name: "traced-quote", Spec: "0 0 1 1 *", TimeZone: "UTC", Kind: "quote.cron", Payload: json.RawMessage(order), InputSchema: quote.InputSchema, Principal: trigger.Principal{ID: "scheduler"}, MaxCatchUp: 1}); err != nil {
		t.Fatal(err)
	}
	clock.set(occurrence)
	parent, err := observe.ParseTraceparent(traceparentOf(inboundTraceID, "01"))
	if err != nil {
		t.Fatal(err)
	}
	before := h.flow.count()
	if _, err := scheduler.Tick(observe.WithTrace(context.Background(), parent)); err != nil {
		t.Fatal(err)
	}
	last := h.submit.last(t)
	if last.err != nil || !last.accepted || last.submission.Kind != "quote.cron" || last.submission.Trace != (observe.TraceContext{}) {
		t.Fatalf("cron submission %+v", last)
	}
	h.drain()
	runs := h.flow.since(before)
	if len(runs) != 1 {
		t.Fatalf("%d cron runs", len(runs))
	}
	requireFresh(t, "cron", h.recorder.root(t, runs[0].id))
}

// TestTraceIngressUndefinedSamplingIsRefusedAtStartup: every adapter
// validates its ingress policy before it serves anything.
func TestTraceIngressUndefinedSamplingIsRefusedAtStartup(t *testing.T) {
	bad := trigger.TraceIngress{Extract: true, Sampling: 9}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	handle := func(context.Context, blokhttp.Input) (any, error) { return nil, nil }
	secret, _ := webhook.NewSecret([]byte("synthetic-webhook-secret-0123456789"))
	hub, _ := sse.NewHub(sse.HubConfig{})
	queue := &recordingSubmitter{}
	_, httpErr := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/q", Handle: handle, Trace: bad}})
	_, webhookErr := webhook.New(application, nil, []webhook.Endpoint{{Path: "/w", Provider: "shop", Principal: trigger.Principal{ID: "shop"}, Kind: "k", Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "k1", Secret: secret}}}, Submit: queue, InputSchema: quote.InputSchema, Trace: bad}})
	_, sseErr := sse.New(application, hub, []sse.Endpoint{{Name: "orders", Path: "/orders", Kind: "k", Submit: queue, Tracker: alwaysSettled{}, Authenticate: bearer, InputSchema: quote.InputSchema, Trace: bad}})
	_, wsErr := bws.New(application, bws.Endpoint{Path: "/ws", Authenticate: bearer, InputSchema: quote.InputSchema, OnMessage: func(context.Context, bws.Message) (json.RawMessage, error) { return nil, nil }, Trace: bad})
	_, mcpErr := tmcp.New(application, tmcp.Config{Name: "n", Version: "1", Catalog: tracedCatalog{}, Authenticate: func(context.Context, string, *http.Request) (tool.Principal, error) { return tool.Principal{}, nil }, Expose: []string{"selection/quote@1.0.0"}, Trace: bad})
	_, grpcErr := bgrpc.New(application, func(context.Context) (trigger.Principal, error) { return trigger.Principal{}, nil }, []bgrpc.Binding{{Method: quoteMethod(t), Workflow: "selection/quote", WorkflowInput: quote.InputSchema, InputSchema: quote.InputSchema, OutputSchema: quoteOutput, Authorize: bgrpc.AllowAuthenticated, Handle: func(context.Context, bgrpc.Call) (json.RawMessage, error) { return nil, nil }, Trace: bad}})
	_, pubsubErr := pubsub.New(&traceBroker{}, pubsub.Subscription{Name: "orders", Principal: trigger.Principal{ID: "p"}, Kind: "k", Submit: queue, InputSchema: quote.InputSchema, Trace: bad})
	_, pubsubPolicyErr := pubsub.New(&traceBroker{}, pubsub.Subscription{Name: "orders", Principal: trigger.Principal{ID: "p"}, Kind: "k", Submit: queue, InputSchema: quote.InputSchema, TracePolicy: observe.TracePolicy{Ratio: 2}})
	for name, err := range map[string]error{"http": httpErr, "webhook": webhookErr, "sse": sseErr, "websocket": wsErr, "mcp": mcpErr, "grpc": grpcErr, "pubsub": pubsubErr} {
		if err == nil || !strings.Contains(err.Error(), "invalid_trace_ingress") {
			t.Errorf("%s accepted an undefined sampling policy: %v", name, err)
		}
	}
	if !errors.Is(pubsubPolicyErr, observe.ErrInvalidTracePolicy) {
		t.Errorf("pubsub accepted an invalid trace policy: %v", pubsubPolicyErr)
	}
}

type alwaysSettled struct{}

func (alwaysSettled) Settled(context.Context, string) (bool, error) { return true, nil }
