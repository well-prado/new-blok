package trigger_test

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	bgrpc "github.com/well-prado/new-blok/trigger/grpc"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
	"github.com/well-prado/new-blok/trigger/pubsub"
	"github.com/well-prado/new-blok/trigger/pubsub/natsjs"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
	"github.com/well-prado/new-blok/trigger/webhook"
	bws "github.com/well-prado/new-blok/trigger/websocket"
	"github.com/well-prado/new-blok/trigger/worker"
)

// The one request every trigger carries, and the output the quote workflow
// computes for it.
const (
	order     = `{"sku":"coffee","quantity":2}`
	wantCents = 3000
)

// inBand and durable name the nine triggers by how they complete.
var (
	inBand  = []string{"http", "grpc", "websocket", "mcp"}
	durable = []string{"webhook", "sse", "cron", "pubsub", "worker"}
)

// quoteOutput is the workflow's output schema, bounded so the gRPC binding
// can prove it fits its 32-bit response field.
var quoteOutput = []byte(`{"type":"object","properties":{"totalCents":{"type":"integer","minimum":0,"maximum":2147483647}},"required":["totalCents"]}`)

// token returns the bearer credential of a principal; bearer authenticates
// it back. Distinct principals keep per-principal quotas out of load tests.
func token(id string) string { return "token-" + id }

func principalOf(header string) (trigger.Principal, error) {
	id, ok := strings.CutPrefix(header, "Bearer token-")
	if !ok || id == "" {
		return trigger.Principal{}, errors.New("unauthenticated")
	}
	return trigger.Principal{ID: id}, nil
}

func bearer(request *http.Request) (trigger.Principal, error) {
	return principalOf(request.Header.Get("Authorization"))
}

// gate holds work at a point until released, and reports what reached it.
// A point held until canceled ignores the release: its work only stops when
// its context ends.
type gate struct {
	mu       sync.Mutex
	hold     map[string]bool
	canceled map[string]bool
	entered  chan string
	release  chan struct{}
}

func newGate() *gate {
	return &gate{hold: map[string]bool{}, canceled: map[string]bool{}, entered: make(chan string, 64), release: make(chan struct{})}
}

func (g *gate) holdingUntilCanceled(names ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, name := range names {
		g.hold[name] = true
		g.canceled[name] = true
	}
}

func (g *gate) holding(names ...string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, name := range names {
		g.hold[name] = true
	}
}

func (g *gate) pass(ctx context.Context, name string) error {
	g.mu.Lock()
	held, untilCanceled := g.hold[name], g.canceled[name]
	g.mu.Unlock()
	if !held {
		return nil
	}
	g.entered <- name
	if untilCanceled {
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// workflow runs the one shared quote workflow and counts its runs by the
// trigger that caused them.
type workflow struct {
	runner *quote.Runner
	gate   *gate
	mu     sync.Mutex
	runs   map[string]int
}

func (w *workflow) run(ctx context.Context, via string, input []byte) (json.RawMessage, error) {
	if err := w.gate.pass(ctx, via); err != nil {
		return nil, err
	}
	var in quote.Input
	if err := json.Unmarshal(input, &in); err != nil {
		return nil, err
	}
	out, err := w.runner.Run(ctx, in)
	if err != nil {
		return nil, err
	}
	w.mu.Lock()
	w.runs[via]++
	w.mu.Unlock()
	return json.Marshal(out)
}

func (w *workflow) counts() map[string]int {
	w.mu.Lock()
	defer w.mu.Unlock()
	counts := map[string]int{}
	for via, n := range w.runs {
		counts[via] = n
	}
	return counts
}

func cents(t *testing.T, via string, output []byte) {
	t.Helper()
	var result struct {
		TotalCents int64 `json:"totalCents"`
	}
	if err := json.Unmarshal(output, &result); err != nil || result.TotalCents != wantCents {
		t.Fatalf("%s returned %s (err %v), want totalCents %d", via, output, err, wantCents)
	}
}

// heldSubmitter is the shared queue, behind a gate a test can close on the
// durable admissions of webhook and SSE.
type heldSubmitter struct {
	queue *worker.Queue
	gate  *gate
}

func (h heldSubmitter) Submit(ctx context.Context, submission trigger.Submission) (bool, error) {
	if err := h.gate.pass(ctx, "submit:"+strings.TrimPrefix(submission.Kind, "quote.")); err != nil {
		return false, err
	}
	return h.queue.Submit(ctx, submission)
}

// broker is the pubsub source: a real NATS JetStream broker when
// NEWBLOK_NATS_URL is set, otherwise a minimal in-memory one.
type broker interface {
	consumer(t *testing.T, sub pubsub.Subscription) *pubsub.Consumer
	publish(t *testing.T, id string, data []byte)
	// settled waits until every published message is acknowledged.
	settled(t *testing.T)
	// fetches counts the fetches made, or -1 when the broker cannot tell.
	fetches() int64
	name() string
	close()
	// messageID is the identity the driver gives a message published with
	// id; it is part of the message's submission key.
	messageID(id string) string
}

type memMessage struct {
	id    string
	data  []byte
	acked chan struct{}
	once  sync.Once
}

func (m *memMessage) ID() string                               { return m.id }
func (m *memMessage) Data() []byte                             { return m.data }
func (m *memMessage) Attempt() int                             { return 1 }
func (m *memMessage) Cursor() string                           { return m.id }
func (m *memMessage) Ack(context.Context) error                { m.once.Do(func() { close(m.acked) }); return nil }
func (m *memMessage) Nak(context.Context, time.Duration) error { return errors.New("unexpected nak") }
func (m *memMessage) Term(context.Context, string) error       { return errors.New("unexpected term") }

type memBroker struct {
	mu        sync.Mutex
	pending   []pubsub.Message
	published []*memMessage
	fetched   atomic.Int64
}

func (b *memBroker) Fetch(ctx context.Context, max int, wait time.Duration) ([]pubsub.Message, error) {
	b.fetched.Add(1)
	b.mu.Lock()
	if len(b.pending) > 0 {
		batch := b.pending[:min(max, len(b.pending))]
		b.pending = b.pending[len(batch):]
		b.mu.Unlock()
		return batch, nil
	}
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(wait):
		return nil, nil
	}
}

func (*memBroker) DeadLetter(context.Context, pubsub.Message, string) error {
	return errors.New("unexpected dead letter")
}

func (b *memBroker) consumer(t *testing.T, sub pubsub.Subscription) *pubsub.Consumer {
	t.Helper()
	consumer, err := pubsub.New(b, sub)
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func (b *memBroker) publish(_ *testing.T, id string, data []byte) {
	m := &memMessage{id: id, data: data, acked: make(chan struct{})}
	b.mu.Lock()
	b.pending = append(b.pending, m)
	b.published = append(b.published, m)
	b.mu.Unlock()
}

func (b *memBroker) settled(t *testing.T) {
	t.Helper()
	b.mu.Lock()
	published := append([]*memMessage(nil), b.published...)
	b.mu.Unlock()
	for _, m := range published {
		select {
		case <-m.acked:
		case <-time.After(30 * time.Second):
			t.Fatalf("pubsub message %s was never acknowledged", m.id)
		}
	}
}

func (b *memBroker) fetches() int64           { return b.fetched.Load() }
func (*memBroker) name() string               { return "in-memory" }
func (*memBroker) close()                     {}
func (*memBroker) messageID(id string) string { return id }

type natsBroker struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	prefix string
	once   sync.Once
}

func newNATSBroker(t *testing.T, url string) *natsBroker {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		t.Fatal(err)
	}
	var random [4]byte
	_, _ = rand.Read(random[:])
	b := &natsBroker{nc: nc, js: js, prefix: "nine" + hex.EncodeToString(random[:])}
	t.Cleanup(b.close)
	for name, subject := range map[string]string{b.prefix + "_orders": b.prefix + ".orders", b.prefix + "_dlq": b.prefix + ".dlq"} {
		if _, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: []string{subject}, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}
	}
	return b
}

// close deletes the test's streams and closes the connection, so its
// goroutines end before the leak check.
func (b *natsBroker) close() {
	b.once.Do(func() {
		for _, stream := range []string{b.prefix + "_orders", b.prefix + "_dlq"} {
			_ = b.js.DeleteStream(context.Background(), stream)
		}
		b.nc.Close()
	})
}

func (b *natsBroker) consumer(t *testing.T, sub pubsub.Subscription) *pubsub.Consumer {
	t.Helper()
	consumer, _, err := natsjs.NewConsumer(context.Background(), b.js, natsjs.Config{Stream: b.prefix + "_orders", Consumer: b.prefix + "_c", Subject: b.prefix + ".orders", DeadLetterSubject: b.prefix + ".dlq", AckWait: 5 * time.Second, IDHeader: "Blok-Message-Id"}, sub)
	if err != nil {
		t.Fatal(err)
	}
	return consumer
}

func (b *natsBroker) publish(t *testing.T, id string, data []byte) {
	t.Helper()
	msg := nats.NewMsg(b.prefix + ".orders")
	msg.Data = data
	msg.Header.Set("Blok-Message-Id", id)
	if _, err := b.js.PublishMsg(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
}

func (b *natsBroker) settled(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		consumer, err := b.js.Consumer(context.Background(), b.prefix+"_orders", b.prefix+"_c")
		if err == nil {
			if info, err := consumer.Info(context.Background()); err == nil && info.NumPending == 0 && info.NumAckPending == 0 {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("NATS messages were never all acknowledged")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (*natsBroker) fetches() int64 { return -1 }

// messageID: the NATS driver namespaces a publisher-set id ("h:") apart
// from a broker sequence ("s:").
func (*natsBroker) messageID(id string) string { return "h:" + id }
func (*natsBroker) name() string               { return "NATS JetStream" }

// cronClock is the scheduler's clock: the test moves it, and an After
// fires once the clock reaches it.
type cronClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []cronWaiter
}

type cronWaiter struct {
	at time.Time
	ch chan time.Time
}

func (c *cronClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *cronClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	at := c.now.Add(d)
	if !at.After(c.now) {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, cronWaiter{at: at, ch: ch})
	return ch
}

func (c *cronClock) set(now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = now
	kept := c.waiters[:0]
	for _, w := range c.waiters {
		if w.at.After(now) {
			kept = append(kept, w)
			continue
		}
		w.ch <- now
	}
	c.waiters = kept
}

// quoteMethod is a unary gRPC method built at run time: no generated code.
// The adapter maps fields by their proto names (ADR 0013), so the reply
// field carries the workflow output's property name.
func quoteMethod(t *testing.T) protoreflect.MethodDescriptor {
	t.Helper()
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum()
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name:    proto.String("nine/quotes.proto"),
		Package: proto.String("nine"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: proto.String("QuoteRequest"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("sku"), JsonName: proto.String("sku"), Number: proto.Int32(1), Label: optional, Type: descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()},
				{Name: proto.String("quantity"), JsonName: proto.String("quantity"), Number: proto.Int32(2), Label: optional, Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()},
			}},
			{Name: proto.String("QuoteReply"), Field: []*descriptorpb.FieldDescriptorProto{
				{Name: proto.String("totalCents"), JsonName: proto.String("totalCents"), Number: proto.Int32(1), Label: optional, Type: descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()},
			}},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{{Name: proto.String("Quotes"), Method: []*descriptorpb.MethodDescriptorProto{
			{Name: proto.String("Quote"), InputType: proto.String(".nine.QuoteRequest"), OutputType: proto.String(".nine.QuoteReply")},
		}}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return file.Services().ByName("Quotes").Methods().ByName("Quote")
}

// mcpCatalog exposes the quote workflow as one MCP tool.
type mcpCatalog struct{ workflow *workflow }

func (mcpCatalog) List(context.Context, tool.Principal) ([]tmcp.Tool, error) {
	return []tmcp.Tool{{Name: "selection/quote", Version: "1.0.0", Description: "Prices a synthetic quote", InputSchema: quote.InputSchema, OutputSchema: quoteOutput}}, nil
}

func (c mcpCatalog) Invoke(ctx context.Context, _ tool.Principal, call tmcp.Call) (json.RawMessage, error) {
	return c.workflow.run(ctx, "mcp", call.Input)
}

// loop runs fn on its own goroutine as an application dependency: the
// application starts it and, when it stops, cancels it and waits for it.
func loop(name string, fn func(context.Context)) app.Dependency {
	var cancel context.CancelFunc
	done := make(chan struct{})
	return app.Dependency{
		Name: name,
		Start: func(context.Context) error {
			var ctx context.Context
			ctx, cancel = context.WithCancel(context.Background())
			go func() { defer close(done); fn(ctx) }()
			return nil
		},
		Close: func(ctx context.Context) error {
			cancel()
			select {
			case <-done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	}
}

// occurrence is the cron instant the harness schedules; the schedule is
// added a minute before it.
var occurrence = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)

// nine is one application with all nine triggers over one SQLite store and
// one worker.Queue, the shared durable admission port. Cron, pubsub and the
// worker pool run as application dependencies, so they stop with it.
type nine struct {
	t        *testing.T
	path     string
	database store.Database
	queue    *worker.Queue
	gate     *gate
	flow     *workflow
	app      *app.Application
	hub      *sse.Hub
	secret   webhook.Secret
	streams  *sse.Server
	sockets  *bws.Server
	tools    *tmcp.Server
	method   protoreflect.MethodDescriptor
	grpc     *grpc.Server
	clock    *cronClock
	broker   broker
	cronRuns chan []cron.Result

	// The worker pool waits for begin before it processes anything.
	begin      chan struct{}
	outputs    sync.Map
	workerErrs atomic.Int64
	firstErr   atomic.Value

	server    *http.Server
	base      string
	wsURL     string
	transport *http.Transport
	client    *http.Client
	conn      *grpc.ClientConn
	served    chan error
	grpcDone  chan error
	stopped   bool
}

func newNine(t *testing.T, workers int) *nine {
	t.Helper()
	return newNineAt(t, workers, filepath.Join(t.TempDir(), "nine.db"))
}

// newNineAt is newNine over the store at path: a second one over the same
// path is the same application restarted.
func newNineAt(t *testing.T, workers int, path string) *nine {
	t.Helper()
	runner, err := quote.New()
	if err != nil {
		t.Fatal(err)
	}
	n := &nine{t: t, path: path, gate: newGate(), clock: &cronClock{now: occurrence.Add(-time.Minute)}, cronRuns: make(chan []cron.Result, 16), begin: make(chan struct{})}
	n.flow = &workflow{runner: runner, gate: n.gate, runs: map[string]int{}}
	ctx := context.Background()
	if n.database, err = (sqlite.Backend{}).Open(ctx, n.path); err != nil {
		t.Fatal(err)
	}
	if n.queue, err = worker.New(ctx, n.database, time.Now); err != nil {
		t.Fatal(err)
	}
	for _, via := range durable {
		if err := n.queue.RegisterKind("quote."+via, quote.InputSchema); err != nil {
			t.Fatal(err)
		}
	}
	submit := heldSubmitter{queue: n.queue, gate: n.gate}
	if url := os.Getenv("NEWBLOK_NATS_URL"); url != "" {
		n.broker = newNATSBroker(t, url)
	} else {
		n.broker = &memBroker{}
	}
	consumer := n.broker.consumer(t, pubsub.Subscription{Name: "orders", Principal: trigger.Principal{ID: "publisher"}, Kind: "quote.pubsub", Submit: n.queue, InputSchema: quote.InputSchema, FetchWait: 20 * time.Millisecond})
	scheduler, err := cron.New(ctx, n.database, n.queue, n.queue, n.clock)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Add(ctx, cron.Schedule{Name: "nightly-quote", Spec: "0 0 1 1 *", TimeZone: "UTC", Kind: "quote.cron", Payload: json.RawMessage(order), InputSchema: quote.InputSchema, Principal: trigger.Principal{ID: "scheduler"}, MaxCatchUp: 1}); err != nil {
		t.Fatal(err)
	}
	if n.hub, err = sse.NewHub(sse.HubConfig{}); err != nil {
		t.Fatal(err)
	}
	// A loop that ends for any reason but its own cancellation is a
	// failure the test must see, not a silently stopped trigger.
	failed := func(what string, err error) {
		n.workerErrs.Add(1)
		n.firstErr.CompareAndSwap(nil, what+": "+err.Error())
	}
	dependencies := []app.Dependency{
		loop("cron", func(ctx context.Context) {
			err := scheduler.Run(ctx, func(results []cron.Result, err error) {
				if err != nil {
					failed("cron tick", err)
				}
				n.cronRuns <- results
			})
			if ctx.Err() == nil {
				failed("cron run", err)
			}
		}),
		loop("pubsub", func(ctx context.Context) {
			if err := consumer.Run(ctx); ctx.Err() == nil {
				failed("pubsub run", err)
			}
		}),
	}
	for i := range workers {
		dependencies = append(dependencies, loop(fmt.Sprintf("worker-%d", i), n.work))
	}
	if n.app, err = app.New(app.Config{DrainTimeout: 10 * time.Second, Dependencies: dependencies}); err != nil {
		t.Fatal(err)
	}

	httpServer, err := blokhttp.New(n.app, []blokhttp.Endpoint{{Method: "POST", Path: "/quotes", InputSchema: quote.InputSchema, Authenticate: bearer, Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
		return n.flow.run(ctx, "http", in.Body)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if n.secret, err = webhook.NewSecret([]byte("synthetic-webhook-secret-0123456789")); err != nil {
		t.Fatal(err)
	}
	webhooks, err := webhook.New(n.app, time.Now, []webhook.Endpoint{{Path: "/webhooks/shop", Provider: "shop", Principal: trigger.Principal{ID: "shop"}, Kind: "quote.webhook", Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "k1", Secret: n.secret}}}, Submit: submit, InputSchema: quote.InputSchema}})
	if err != nil {
		t.Fatal(err)
	}
	if n.streams, err = sse.New(n.app, n.hub, []sse.Endpoint{{Name: "orders", Path: "/orders", Kind: "quote.sse", Submit: submit, Tracker: n.queue, Authenticate: bearer, InputSchema: quote.InputSchema}}); err != nil {
		t.Fatal(err)
	}
	if n.sockets, err = bws.New(n.app, bws.Endpoint{Path: "/ws", Authenticate: bearer, InputSchema: quote.InputSchema, OnMessage: func(ctx context.Context, m bws.Message) (json.RawMessage, error) {
		return n.flow.run(ctx, "websocket", m.Input)
	}}); err != nil {
		t.Fatal(err)
	}
	budget := tool.Budget{MaxDepth: 4, MaxInputBytes: 1 << 10, MaxOutputBytes: 1 << 10, MaxTokens: 1, MaxCalls: 1, Deadline: time.Now().Add(time.Hour)}
	if n.tools, err = tmcp.New(n.app, tmcp.Config{Name: "nine", Version: "1.0.0", Catalog: mcpCatalog{workflow: n.flow}, Authenticate: func(_ context.Context, credential string, _ *http.Request) (tool.Principal, error) {
		principal, err := principalOf("Bearer " + credential)
		return tool.Principal{ID: principal.ID, MaxDepth: 4}, err
	}, Expose: []string{"selection/quote@1.0.0"}, Budget: budget}); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/quotes", httpServer)
	mux.Handle("/webhooks/", webhooks)
	mux.Handle("/orders", n.streams)
	mux.Handle("/orders/", n.streams)
	mux.Handle("/ws", n.sockets)
	mux.Handle("/mcp", n.tools)

	n.method = quoteMethod(t)
	methods, err := bgrpc.New(n.app, func(ctx context.Context) (trigger.Principal, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("authorization"); len(values) == 1 {
			return principalOf(values[0])
		}
		return trigger.Principal{}, errors.New("unauthenticated")
	}, []bgrpc.Binding{{Method: n.method, Workflow: "selection/quote", WorkflowInput: quote.InputSchema, InputSchema: quote.InputSchema, OutputSchema: quoteOutput, Authorize: bgrpc.AllowAuthenticated, Handle: func(ctx context.Context, call bgrpc.Call) (json.RawMessage, error) {
		return n.flow.run(ctx, "grpc", call.Input)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	n.grpc = grpc.NewServer(methods.ServerOptions()...)
	if err := methods.Register(n.grpc); err != nil {
		t.Fatal(err)
	}

	if err := n.app.Start(ctx); err != nil {
		t.Fatal(err)
	}
	httpListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	grpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	n.server = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	n.served, n.grpcDone = make(chan error, 1), make(chan error, 1)
	go func() { n.served <- n.server.Serve(httpListener) }()
	go func() { n.grpcDone <- n.grpc.Serve(grpcListener) }()
	n.base = "http://" + httpListener.Addr().String()
	n.wsURL = "ws://" + httpListener.Addr().String() + "/ws"
	n.transport = &http.Transport{MaxIdleConnsPerHost: 64}
	n.client = &http.Client{Transport: n.transport, Timeout: 30 * time.Second}
	if n.conn, err = grpc.NewClient(grpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials())); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(n.stop)
	return n
}

// work is one worker of the pool: it waits for begin, then processes jobs
// until the application stops it. An SSE job's result is published to its
// stream after the job commits.
func (n *nine) work(ctx context.Context) {
	select {
	case <-n.begin:
	case <-ctx.Done():
		return
	}
	for ctx.Err() == nil {
		var key, via string
		var output json.RawMessage
		processed, err := n.queue.ProcessOnce(ctx, func(ctx context.Context, _ worker.Tx, job worker.Job) error {
			via = strings.TrimPrefix(job.Kind, "quote.")
			out, err := n.flow.run(ctx, via, job.Payload)
			key, output = job.RequestKey, out
			return err
		})
		if err != nil {
			if ctx.Err() == nil {
				n.workerErrs.Add(1)
				n.firstErr.CompareAndSwap(nil, err.Error())
			}
			continue
		}
		if !processed {
			select {
			case <-ctx.Done():
			case <-time.After(2 * time.Millisecond):
			}
			continue
		}
		n.outputs.Store(key, output)
		if via == "sse" {
			_, _ = n.hub.Finish(sse.StreamID(key), sse.Result(output))
		}
	}
}

// post sends one POST and returns its status, its Retry-After and its body.
func (n *nine) post(path string, headers map[string]string, body string) (int, string, []byte) {
	n.t.Helper()
	request, err := http.NewRequest(http.MethodPost, n.base+path, strings.NewReader(body))
	if err != nil {
		n.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response, err := n.client.Do(request)
	if err != nil {
		n.t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, response.Header.Get("Retry-After"), data
}

func auth(id string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + token(id)}
}

func (n *nine) signed(id string) map[string]string {
	now := time.Now()
	return map[string]string{"webhook-id": id, "webhook-timestamp": fmt.Sprint(now.Unix()), "webhook-signature": webhook.SignStandard(n.secret, id, now, []byte(order))}
}

func (n *nine) sseStart(principal, key string) (int, string, []byte) {
	n.t.Helper()
	headers := auth(principal)
	headers["Idempotency-Key"] = key
	return n.post("/orders", headers, order)
}

func (n *nine) callGRPC(ctx context.Context, principal string) (*dynamicpb.Message, error) {
	request := dynamicpb.NewMessage(n.method.Input())
	request.Set(n.method.Input().Fields().ByName("sku"), protoreflect.ValueOfString("coffee"))
	request.Set(n.method.Input().Fields().ByName("quantity"), protoreflect.ValueOfInt32(2))
	reply := dynamicpb.NewMessage(n.method.Output())
	callCtx, done := context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token(principal)), 30*time.Second)
	defer done()
	return reply, n.conn.Invoke(callCtx, "/nine.Quotes/Quote", request, reply)
}

func (n *nine) grpcCents(reply *dynamicpb.Message) int64 {
	return reply.Get(n.method.Output().Fields().ByName("totalCents")).Int()
}

func (n *nine) dialWS(ctx context.Context, principal string) (*websocket.Conn, *http.Response, error) {
	return websocket.Dial(ctx, n.wsURL, &websocket.DialOptions{HTTPClient: n.client, HTTPHeader: http.Header{"Authorization": {"Bearer " + token(principal)}}})
}

func wsQuote(ctx context.Context, socket *websocket.Conn, id string) (json.RawMessage, error) {
	if err := socket.Write(ctx, websocket.MessageText, []byte(`{"id":"`+id+`","input":`+order+`}`)); err != nil {
		return nil, err
	}
	_, frame, err := socket.Read(ctx)
	if err != nil {
		return nil, err
	}
	var reply struct {
		Output json.RawMessage `json:"output"`
		Error  string          `json:"error"`
	}
	if err := json.Unmarshal(frame, &reply); err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return nil, errors.New(reply.Error)
	}
	return reply.Output, nil
}

func (n *nine) mcpSession(ctx context.Context, principal string) (*sdk.ClientSession, error) {
	client := sdk.NewClient(&sdk.Implementation{Name: "nine", Version: "1.0.0"}, nil)
	// No retries: a dropped connection fails the call at once. The server
	// offers no standalone stream, so its shutdown leaves the client no
	// stream end to mistake for a failed connection (ADR 0014, #197).
	return client.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: n.base + "/mcp", HTTPClient: &http.Client{Transport: bearerTransport{base: n.transport, token: token(principal)}}, MaxRetries: -1}, nil)
}

func mcpQuote(ctx context.Context, session *sdk.ClientSession) (json.RawMessage, error) {
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: tmcp.ToolName("selection/quote", "1.0.0"), Arguments: json.RawMessage(order)})
	if err != nil {
		return nil, err
	}
	if result.IsError {
		return nil, fmt.Errorf("tool error %+v", result.Content)
	}
	return json.Marshal(result.StructuredContent)
}

// bearerTransport adds a bearer token to MCP requests.
type bearerTransport struct {
	base  http.RoundTripper
	token string
}

func (b bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(request)
}

// subscribe follows an SSE stream; the result event's data arrives on the
// returned channel.
func (n *nine) subscribe(ctx context.Context, principal, stream string) <-chan []byte {
	n.t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, n.base+"/orders/"+stream, nil)
	if err != nil {
		n.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token(principal))
	response, err := n.client.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		n.t.Fatalf("subscribe: %v %v", response, err)
	}
	result := make(chan []byte, 1)
	go func() {
		defer response.Body.Close()
		lines := bufio.NewScanner(response.Body)
		event := ""
		for lines.Scan() {
			line := lines.Text()
			if name, ok := strings.CutPrefix(line, "event: "); ok {
				event = name
			}
			if data, ok := strings.CutPrefix(line, "data: "); ok && event == "result" {
				result <- []byte(data)
				return
			}
		}
		close(result)
	}()
	return result
}

// waitSettled waits until every key's job has completed.
func (n *nine) waitSettled(keys ...string) {
	n.t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for _, key := range keys {
		for {
			job, err := n.queue.Get(context.Background(), key)
			if err == nil && job.State == worker.StateCompleted {
				break
			}
			if time.Now().After(deadline) {
				n.t.Fatalf("job %s never completed: %+v %v (worker errors %d, first %v)", key, job, err, n.workerErrs.Load(), n.firstErr.Load())
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
}

func (n *nine) output(key string) []byte {
	value, ok := n.outputs.Load(key)
	if !ok {
		n.t.Fatalf("no output recorded for %s", key)
	}
	return value.(json.RawMessage)
}

// stop shuts the host down in the order ADR 0005 documents:
//  1. the long-lived adapters stop admitting and end their streams and
//     sessions (SSE, WebSocket, MCP), the gRPC server finishes the calls
//     in flight, and the HTTP server finishes its requests;
//  2. the application drains what is left, then stops the queued sources
//     it owns as dependencies (cron, pubsub, the worker pool);
//  3. the broker connection and the store close last.
//
// The test's own idle HTTP connections go first, so none holds the
// server's shutdown open; its gRPC client goes once the server has
// finished the calls in flight.
func (n *nine) stop() {
	if n.stopped {
		return
	}
	n.stopped = true
	n.stopAdapters()
	n.stopApplication()
}

// stopAdapters is step 1 of stop.
func (n *nine) stopAdapters() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	n.transport.CloseIdleConnections()
	if err := errors.Join(n.streams.Shutdown(ctx), n.sockets.Shutdown(ctx), n.tools.Shutdown(ctx)); err != nil {
		n.t.Errorf("adapter shutdown: %v", err)
	}
	n.grpc.GracefulStop()
	_ = n.conn.Close()
	if err := n.server.Shutdown(ctx); err != nil {
		n.t.Errorf("http server shutdown: %v", err)
	}
	if err := <-n.served; !errors.Is(err, http.ErrServerClosed) {
		n.t.Errorf("http serve: %v", err)
	}
	if err := <-n.grpcDone; err != nil {
		n.t.Errorf("grpc serve: %v", err)
	}
}

// stopApplication is steps 2 and 3 of stop.
func (n *nine) stopApplication() {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if n.app.State() == app.ReadyState {
		if err := n.app.Shutdown(ctx); err != nil {
			n.t.Errorf("application shutdown: %v", err)
		}
	}
	n.transport.CloseIdleConnections()
	n.broker.close()
	if err := n.database.Close(); err != nil {
		n.t.Errorf("store close: %v", err)
	}
}

// gated names the six triggers whose work holds the application open, and
// the gate point each one's held work reaches.
var gated = map[string]string{"http": "http", "grpc": "grpc", "websocket": "websocket", "mcp": "mcp", "webhook": "submit:webhook", "sse": "submit:sse"}

// clients are the long-lived client connections a held request needs.
type clients struct {
	session *sdk.ClientSession
	socket  *websocket.Conn
}

func (n *nine) clients(ctx context.Context) clients {
	n.t.Helper()
	session, err := n.mcpSession(ctx, "alice")
	if err != nil {
		n.t.Fatal(err)
	}
	socket, _, err := n.dialWS(ctx, "alice")
	if err != nil {
		n.t.Fatal(err)
	}
	return clients{session: session, socket: socket}
}

func (c clients) close() {
	_ = c.session.Close()
	c.socket.CloseNow()
}

// request sends one request through a gated trigger in the background; id
// names its WebSocket message, webhook event or SSE key.
func (n *nine) request(ctx context.Context, c clients, via, id string) <-chan error {
	done := make(chan error, 1)
	go func() { done <- n.requestNow(ctx, c, via, id) }()
	return done
}

func (n *nine) requestNow(ctx context.Context, c clients, via, id string) error {
	switch via {
	case "http":
		if status, _, body := n.post("/quotes", auth("alice"), order); status != http.StatusOK {
			return fmt.Errorf("%d %s", status, body)
		}
	case "grpc":
		reply, err := n.callGRPC(ctx, "alice")
		if err == nil && n.grpcCents(reply) != wantCents {
			err = fmt.Errorf("totalCents %d", n.grpcCents(reply))
		}
		return err
	case "websocket":
		_, err := wsQuote(ctx, c.socket, id)
		return err
	case "mcp":
		_, err := mcpQuote(ctx, c.session)
		return err
	case "webhook":
		if status, _, body := n.post("/webhooks/shop", n.signed(id), order); status != http.StatusAccepted {
			return fmt.Errorf("%d %s", status, body)
		}
	case "sse":
		if status, _, body := n.sseStart("alice", id); status != http.StatusAccepted {
			return fmt.Errorf("%d %s", status, body)
		}
	default:
		return fmt.Errorf("no trigger %s", via)
	}
	return nil
}

// waitEntered waits until each named gate point holds work.
func (n *nine) waitEntered(points ...string) {
	n.t.Helper()
	held := map[string]bool{}
	for len(held) < len(points) {
		select {
		case point := <-n.gate.entered:
			held[point] = true
		case <-time.After(10 * time.Second):
			n.t.Fatalf("only %v of %v reached their hold", held, points)
		}
	}
}

// waitDraining waits until the application has begun to drain.
func (n *nine) waitDraining() {
	n.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for n.app.State() == app.ReadyState {
		if time.Now().After(deadline) {
			n.t.Fatal("the application never began draining")
		}
		time.Sleep(time.Millisecond)
	}
}

// finalEvent follows an SSE stream to its first result, expired or error
// event and returns its type.
func (n *nine) finalEvent(ctx context.Context, principal, stream string) string {
	n.t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, n.base+"/orders/"+stream, nil)
	if err != nil {
		n.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+token(principal))
	response, err := n.client.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		n.t.Fatalf("subscribe: %v %v", response, err)
	}
	defer response.Body.Close()
	lines := bufio.NewScanner(response.Body)
	for lines.Scan() {
		if event, ok := strings.CutPrefix(lines.Text(), "event: "); ok && event != "progress" {
			return event
		}
	}
	return ""
}

// goroutineSet returns the live goroutines by id, each with its stack.
func goroutineSet() map[string]string {
	buffer := make([]byte, 1<<16)
	for {
		size := runtime.Stack(buffer, true)
		if size < len(buffer) {
			buffer = buffer[:size]
			break
		}
		buffer = make([]byte, 2*len(buffer))
	}
	live := map[string]string{}
	for _, block := range strings.Split(string(buffer), "\n\n") {
		header, _, _ := strings.Cut(block, "\n")
		if fields := strings.Fields(header); len(fields) >= 2 && fields[0] == "goroutine" {
			live[fields[1]] = block
		}
	}
	return live
}

// noLeaks fails if a goroutine started after baseline is still alive once
// things settle: identity, not a count, so an unrelated exit hides nothing.
func noLeaks(t *testing.T, baseline map[string]string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var extra []string
		for id, stack := range goroutineSet() {
			if _, ok := baseline[id]; !ok && !strings.Contains(stack, "testing.tRunner") {
				extra = append(extra, stack)
			}
		}
		if len(extra) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d goroutines outlived the application; first:\n%s", len(extra), extra[0])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
