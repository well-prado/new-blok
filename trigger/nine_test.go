package trigger_test

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/tool"
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

// The one request every trigger carries, and the output the quote workflow
// computes for it.
const (
	order       = `{"sku":"coffee","quantity":2}`
	wantCents   = 3000
	bearerToken = "alice-token"
)

var principal = trigger.Principal{ID: "alice"}

// quoteOutput is the workflow's output schema, bounded so the gRPC binding
// can prove it fits its 32-bit response field.
var quoteOutput = []byte(`{"type":"object","properties":{"totalCents":{"type":"integer","minimum":0,"maximum":2147483647}},"required":["totalCents"]}`)

// workflow runs the one shared quote workflow and counts its runs by the
// trigger that caused them.
type workflow struct {
	runner *quote.Runner
	mu     sync.Mutex
	runs   map[string]int
}

func (w *workflow) run(ctx context.Context, via string, input []byte) (json.RawMessage, error) {
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

func bearer(request *http.Request) (trigger.Principal, error) {
	if request.Header.Get("Authorization") != "Bearer "+bearerToken {
		return trigger.Principal{}, errors.New("unauthenticated")
	}
	return principal, nil
}

// memMessage and memSource are a minimal broker for the pubsub adapter: the
// NATS JetStream driver is proven against a real broker in its own suite.
type memMessage struct {
	data  []byte
	acked chan struct{}
	once  sync.Once
}

func (m *memMessage) ID() string                               { return "m-1" }
func (m *memMessage) Data() []byte                             { return m.data }
func (m *memMessage) Attempt() int                             { return 1 }
func (m *memMessage) Cursor() string                           { return "1" }
func (m *memMessage) Ack(context.Context) error                { m.once.Do(func() { close(m.acked) }); return nil }
func (m *memMessage) Nak(context.Context, time.Duration) error { return errors.New("unexpected nak") }
func (m *memMessage) Term(context.Context, string) error       { return errors.New("unexpected term") }

type memSource struct {
	mu      sync.Mutex
	pending []pubsub.Message
}

func (s *memSource) Fetch(ctx context.Context, max int, wait time.Duration) ([]pubsub.Message, error) {
	s.mu.Lock()
	if len(s.pending) > 0 {
		batch := s.pending[:min(max, len(s.pending))]
		s.pending = s.pending[len(batch):]
		s.mu.Unlock()
		return batch, nil
	}
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(wait):
		return nil, nil
	}
}

func (s *memSource) DeadLetter(context.Context, pubsub.Message, string) error {
	return errors.New("unexpected dead letter")
}

// cronClock is the scheduler's clock, moved by the test.
type cronClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *cronClock) Now() time.Time                         { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *cronClock) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (c *cronClock) set(now time.Time)                      { c.mu.Lock(); c.now = now; c.mu.Unlock() }

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

// TestNineTriggersShareOneApplication wires all nine triggers into one
// application over one durable store and one workflow, drives one real
// request through each protocol, then stops the application and requires
// every application-gated trigger to refuse, every adapter to shut down and
// the goroutines to return to their baseline.
func TestNineTriggersShareOneApplication(t *testing.T) {
	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner, err := quote.New()
	if err != nil {
		t.Fatal(err)
	}
	flow := &workflow{runner: runner, runs: map[string]int{}}

	// One application, one store, one durable admission port.
	application, err := app.New(app.Config{DrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "nine.db"))
	if err != nil {
		t.Fatal(err)
	}
	queue, err := worker.New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	durable := []string{"webhook", "cron", "pubsub", "sse", "worker"}
	for _, via := range durable {
		if err := queue.RegisterKind("quote."+via, quote.InputSchema); err != nil {
			t.Fatal(err)
		}
	}

	// HTTP, webhook, SSE, WebSocket and MCP share one listener.
	httpServer, err := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/quotes", InputSchema: quote.InputSchema, Authenticate: bearer, Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
		return flow.run(ctx, "http", in.Body)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	secret, err := webhook.NewSecret([]byte("synthetic-webhook-secret-0123456789"))
	if err != nil {
		t.Fatal(err)
	}
	webhooks, err := webhook.New(application, time.Now, []webhook.Endpoint{{Path: "/webhooks/shop", Provider: "shop", Principal: principal, Kind: "quote.webhook", Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "k1", Secret: secret}}}, Submit: queue, InputSchema: quote.InputSchema}})
	if err != nil {
		t.Fatal(err)
	}
	hub, err := sse.NewHub(sse.HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	streams, err := sse.New(application, hub, []sse.Endpoint{{Name: "orders", Path: "/orders", Kind: "quote.sse", Submit: queue, Tracker: queue, Authenticate: bearer, InputSchema: quote.InputSchema}})
	if err != nil {
		t.Fatal(err)
	}
	sockets, err := bws.New(application, bws.Endpoint{Path: "/ws", Authenticate: bearer, InputSchema: quote.InputSchema, OnMessage: func(ctx context.Context, m bws.Message) (json.RawMessage, error) {
		return flow.run(ctx, "websocket", m.Input)
	}})
	if err != nil {
		t.Fatal(err)
	}
	budget := tool.Budget{MaxDepth: 4, MaxInputBytes: 1 << 10, MaxOutputBytes: 1 << 10, MaxTokens: 1, MaxCalls: 1, Deadline: time.Now().Add(time.Hour)}
	tools, err := tmcp.New(application, tmcp.Config{Name: "nine", Version: "1.0.0", Catalog: mcpCatalog{workflow: flow}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		if token != bearerToken {
			return tool.Principal{}, errors.New("unauthenticated")
		}
		return tool.Principal{ID: principal.ID, MaxDepth: 4}, nil
	}, Expose: []string{"selection/quote@1.0.0"}, Budget: budget})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("/quotes", httpServer)
	mux.Handle("/webhooks/", webhooks)
	mux.Handle("/orders", streams)
	mux.Handle("/orders/", streams)
	mux.Handle("/ws", sockets)
	mux.Handle("/mcp", tools)

	// gRPC on its own listener.
	method := quoteMethod(t)
	methods, err := bgrpc.New(application, func(ctx context.Context) (trigger.Principal, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		if values := md.Get("authorization"); len(values) != 1 || values[0] != "Bearer "+bearerToken {
			return trigger.Principal{}, errors.New("unauthenticated")
		}
		return principal, nil
	}, []bgrpc.Binding{{Method: method, Workflow: "selection/quote", WorkflowInput: quote.InputSchema, InputSchema: quote.InputSchema, OutputSchema: quoteOutput, Authorize: bgrpc.AllowAuthenticated, Handle: func(ctx context.Context, call bgrpc.Call) (json.RawMessage, error) {
		return flow.run(ctx, "grpc", call.Input)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	grpcServer := grpc.NewServer(methods.ServerOptions()...)
	if err := methods.Register(grpcServer); err != nil {
		t.Fatal(err)
	}

	// Cron and pubsub are driven by their own loops.
	clock := &cronClock{now: time.Date(2026, 12, 31, 23, 59, 0, 0, time.UTC)}
	scheduler, err := cron.New(ctx, database, queue, queue, clock)
	if err != nil {
		t.Fatal(err)
	}
	message := &memMessage{data: []byte(order), acked: make(chan struct{})}
	consumer, err := pubsub.New(&memSource{pending: []pubsub.Message{message}}, pubsub.Subscription{Name: "orders", Principal: principal, Kind: "quote.pubsub", Submit: queue, InputSchema: quote.InputSchema, FetchWait: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	if err := application.Start(ctx); err != nil {
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
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- server.Serve(httpListener) }()
	grpcServed := make(chan error, 1)
	go func() { grpcServed <- grpcServer.Serve(grpcListener) }()
	consumerCtx, stopConsumer := context.WithCancel(ctx)
	consumed := make(chan error, 1)
	go func() { consumed <- consumer.Run(consumerCtx) }()
	base := "http://" + httpListener.Addr().String()
	transport := &http.Transport{}
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	post := func(path string, headers map[string]string, body string) (int, []byte) {
		t.Helper()
		request, err := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		for name, value := range headers {
			request.Header.Set(name, value)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		data, _ := io.ReadAll(response.Body)
		return response.StatusCode, data
	}
	signed := func(id string) map[string]string {
		now := time.Now()
		return map[string]string{"webhook-id": id, "webhook-timestamp": fmt.Sprint(now.Unix()), "webhook-signature": webhook.SignStandard(secret, id, now, []byte(order))}
	}

	// 1. HTTP: in band.
	statusCode, body := post("/quotes", map[string]string{"Authorization": "Bearer " + bearerToken}, order)
	if statusCode != http.StatusOK {
		t.Fatalf("http: status=%d body=%s", statusCode, body)
	}
	cents(t, "http", body)

	// 2. gRPC: in band, with a dynamic message.
	conn, err := grpc.NewClient(grpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	callGRPC := func() (*dynamicpb.Message, error) {
		request := dynamicpb.NewMessage(method.Input())
		request.Set(method.Input().Fields().ByName("sku"), protoreflect.ValueOfString("coffee"))
		request.Set(method.Input().Fields().ByName("quantity"), protoreflect.ValueOfInt32(2))
		reply := dynamicpb.NewMessage(method.Output())
		callCtx, done := context.WithTimeout(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+bearerToken), 10*time.Second)
		defer done()
		return reply, conn.Invoke(callCtx, "/nine.Quotes/Quote", request, reply)
	}
	reply, err := callGRPC()
	if err != nil {
		t.Fatalf("grpc: %v", err)
	}
	if got := reply.Get(method.Output().Fields().ByName("totalCents")).Int(); got != wantCents {
		t.Fatalf("grpc returned totalCents %d", got)
	}

	// 3. WebSocket: in band, one message on one connection.
	wsURL := "ws://" + httpListener.Addr().String() + "/ws"
	socket, _, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPClient: client, HTTPHeader: http.Header{"Authorization": {"Bearer " + bearerToken}}})
	if err != nil {
		t.Fatalf("websocket: %v", err)
	}
	if err := socket.Write(ctx, websocket.MessageText, []byte(`{"id":"q-1","input":`+order+`}`)); err != nil {
		t.Fatal(err)
	}
	_, frame, err := socket.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var wsReply struct {
		Output json.RawMessage `json:"output"`
		Error  string          `json:"error"`
	}
	if err := json.Unmarshal(frame, &wsReply); err != nil || wsReply.Error != "" {
		t.Fatalf("websocket reply %s: %v", frame, err)
	}
	cents(t, "websocket", wsReply.Output)
	_ = socket.Close(websocket.StatusNormalClosure, "")

	// 4. MCP: in band, through the official client.
	mcpClient := sdk.NewClient(&sdk.Implementation{Name: "nine", Version: "1.0.0"}, nil)
	session, err := mcpClient.Connect(ctx, &sdk.StreamableClientTransport{Endpoint: base + "/mcp", HTTPClient: &http.Client{Transport: bearerTransport{base: transport}}, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatalf("mcp: %v", err)
	}
	result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: tmcp.ToolName("selection/quote", "1.0.0"), Arguments: json.RawMessage(order)})
	if err != nil || result.IsError {
		t.Fatalf("mcp: result=%+v err=%v", result, err)
	}
	structured, _ := json.Marshal(result.StructuredContent)
	cents(t, "mcp", structured)
	_ = session.Close()

	// 5. Webhook: durable, signed.
	if statusCode, body := post("/webhooks/shop", signed("evt-1"), order); statusCode != http.StatusAccepted {
		t.Fatalf("webhook: status=%d body=%s", statusCode, body)
	}

	// 6. SSE: durable start; the stream is read after the work runs.
	statusCode, body = post("/orders", map[string]string{"Authorization": "Bearer " + bearerToken, "Idempotency-Key": "order-1"}, order)
	var started struct {
		Stream string `json:"stream"`
	}
	if err := json.Unmarshal(body, &started); err != nil || statusCode != http.StatusAccepted || started.Stream == "" {
		t.Fatalf("sse start: status=%d body=%s", statusCode, body)
	}

	// 7. Cron: a durable occurrence.
	occurrence := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := scheduler.Add(ctx, cron.Schedule{Name: "nightly-quote", Spec: "0 0 1 1 *", TimeZone: "UTC", Kind: "quote.cron", Payload: json.RawMessage(order), InputSchema: quote.InputSchema, Principal: principal, MaxCatchUp: 1}); err != nil {
		t.Fatal(err)
	}
	clock.set(occurrence)
	ticked, err := scheduler.Tick(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(ticked) != 1 || len(ticked[0].Submitted) != 1 {
		t.Fatalf("cron submitted %+v", ticked)
	}

	// 8. Pubsub: durable transfer; the broker is acknowledged after commit.
	select {
	case <-message.acked:
	case <-time.After(5 * time.Second):
		t.Fatal("pubsub never acknowledged its message")
	}

	// 9. Worker: a job enqueued directly.
	if enqueued, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "worker:quote-1", Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || !enqueued.Accepted {
		t.Fatalf("worker enqueue: %+v %v", enqueued, err)
	}

	// One worker runs every durable submission, exactly once each.
	outputs := map[string]json.RawMessage{}
	var sseKey string
	for {
		processed, err := queue.ProcessOnce(ctx, func(ctx context.Context, _ *sql.Tx, job worker.Job) error {
			via := strings.TrimPrefix(job.Kind, "quote.")
			output, err := flow.run(ctx, via, job.Payload)
			if err != nil {
				return err
			}
			outputs[via] = output
			if via == "sse" {
				sseKey = job.RequestKey
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	for _, via := range durable {
		cents(t, via, outputs[via])
	}
	if sse.StreamID(sseKey) != started.Stream {
		t.Fatalf("the SSE job %q does not belong to stream %q", sseKey, started.Stream)
	}
	if _, err := hub.Finish(started.Stream, sse.Result(outputs["sse"])); err != nil {
		t.Fatal(err)
	}
	subscription, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/orders/"+started.Stream, nil)
	if err != nil {
		t.Fatal(err)
	}
	subscription.Header.Set("Authorization", "Bearer "+bearerToken)
	streamed, err := client.Do(subscription)
	if err != nil {
		t.Fatal(err)
	}
	cents(t, "sse stream", resultEvent(t, streamed.Body))
	streamed.Body.Close()

	want := map[string]int{"http": 1, "grpc": 1, "websocket": 1, "mcp": 1, "webhook": 1, "sse": 1, "cron": 1, "pubsub": 1, "worker": 1}
	if got := flow.counts(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("workflow runs by trigger %v, want %v", got, want)
	}

	// Stop the application: every application-gated trigger refuses.
	if err := application.Shutdown(ctx); err != nil {
		t.Fatalf("application shutdown: %v", err)
	}
	refused := map[string]int{}
	refused["http"], _ = post("/quotes", map[string]string{"Authorization": "Bearer " + bearerToken}, order)
	refused["webhook"], _ = post("/webhooks/shop", signed("evt-2"), order)
	refused["sse"], _ = post("/orders", map[string]string{"Authorization": "Bearer " + bearerToken, "Idempotency-Key": "order-2"}, order)
	refused["mcp"], _ = post("/mcp", map[string]string{"Authorization": "Bearer " + bearerToken, "Accept": "application/json, text/event-stream"}, `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"late","version":"1"}}}`)
	if _, response, err := websocket.Dial(ctx, wsURL, &websocket.DialOptions{HTTPClient: client, HTTPHeader: http.Header{"Authorization": {"Bearer " + bearerToken}}}); err == nil {
		t.Fatal("websocket: a stopped application accepted a connection")
	} else if response != nil {
		refused["websocket"] = response.StatusCode
	}
	if len(refused) != 5 {
		t.Errorf("refusals measured for %v; want http, webhook, sse, mcp and websocket", refused)
	}
	for via, code := range refused {
		if code != http.StatusServiceUnavailable {
			t.Errorf("%s: a stopped application answered %d, want 503", via, code)
		}
	}
	if _, err := callGRPC(); status.Code(err) != codes.Unavailable {
		t.Errorf("grpc: a stopped application answered %v, want Unavailable", err)
	}
	if processed, err := queue.ProcessOnce(ctx, func(context.Context, *sql.Tx, worker.Job) error { return errors.New("nothing should run") }); err != nil || processed {
		t.Errorf("a refused trigger submitted work: processed=%v err=%v", processed, err)
	}
	if got := flow.counts(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("the workflow ran after the stop: %v", got)
	}

	// Shut every adapter down; goroutines return to their baseline.
	stop, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	stopConsumer()
	if err := <-consumed; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("pubsub consumer: %v", err)
	}
	if err := errors.Join(tools.Shutdown(stop), sockets.Shutdown(stop), streams.Shutdown(stop), server.Shutdown(stop)); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	grpcServer.GracefulStop()
	_ = conn.Close()
	if err := <-served; !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("http serve: %v", err)
	}
	if err := <-grpcServed; err != nil {
		t.Errorf("grpc serve: %v", err)
	}
	cancel()
	if err := database.Close(); err != nil {
		t.Errorf("store close: %v", err)
	}
	transport.CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline+2 {
		if time.Now().After(deadline) {
			stack := make([]byte, 1<<16)
			t.Fatalf("goroutines %d, baseline %d\n%s", runtime.NumGoroutine(), baseline, stack[:runtime.Stack(stack, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// bearerTransport adds the bearer token to MCP requests.
type bearerTransport struct{ base http.RoundTripper }

func (b bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("Authorization", "Bearer "+bearerToken)
	return b.base.RoundTrip(request)
}

// resultEvent reads an SSE stream up to its result event and returns its
// data.
func resultEvent(t *testing.T, stream io.Reader) []byte {
	t.Helper()
	lines := bufio.NewScanner(stream)
	event := ""
	for lines.Scan() {
		line := lines.Text()
		if name, ok := strings.CutPrefix(line, "event: "); ok {
			event = name
		}
		if data, ok := strings.CutPrefix(line, "data: "); ok && event == "result" {
			return []byte(data)
		}
	}
	t.Fatalf("the stream ended without a result: %v", lines.Err())
	return nil
}

// adapters are the nine trigger adapters and the NATS driver.
var adapters = []string{"http", "webhook", "worker", "cron", "pubsub", "pubsub/natsjs", "grpc", "sse", "websocket", "mcp"}

// TestAdaptersAreIndependentlyRemovable: no adapter links another adapter
// except as declared, the store is linked only by the adapters that own
// durable state, and each protocol library only by the adapter that speaks
// it. So any adapter can be left out of a binary without dragging another
// in, and a binary pays only for the adapters it selects.
func TestAdaptersAreIndependentlyRemovable(t *testing.T) {
	declared := map[string][]string{"pubsub/natsjs": {"pubsub"}}
	owners := map[string][]string{
		module + "/store":                 {"worker", "cron"},
		"database/sql":                    {"worker", "cron"},
		"google.golang.org/grpc":          {"grpc"},
		"github.com/coder/websocket":      {"websocket"},
		"github.com/nats-io":              {"pubsub/natsjs"},
		"github.com/modelcontextprotocol": {"mcp"},
		"github.com/google/jsonschema-go": {"mcp"},
		module + "/internal/engine":       {},
		module + "/internal/journal":      {},
		module + "/contract/conformance":  {},
	}
	footprint := map[string]int{}
	for _, adapter := range adapters {
		output, err := exec.Command(goTool(t), "list", "-deps", module+"/trigger/"+adapter).CombinedOutput()
		if err != nil {
			t.Fatalf("go list %s: %v\n%s", adapter, err, output)
		}
		deps := strings.Fields(string(output))
		footprint[adapter] = len(deps)
		for _, dep := range deps {
			rel, ok := strings.CutPrefix(dep, module+"/trigger/")
			if ok && rel != adapter && !strings.Contains(rel, "internal") {
				allowed := false
				for _, other := range declared[adapter] {
					allowed = allowed || rel == other
				}
				if !allowed {
					t.Errorf("trigger/%s links trigger/%s", adapter, rel)
				}
			}
			for prefix, owned := range owners {
				if dep != prefix && !strings.HasPrefix(dep, prefix+"/") {
					continue
				}
				permitted := false
				for _, owner := range owned {
					permitted = permitted || owner == adapter
				}
				if !permitted {
					t.Errorf("trigger/%s links %s, which only %v may", adapter, dep, owned)
				}
			}
		}
	}
	names := append([]string(nil), adapters...)
	sort.Slice(names, func(i, j int) bool { return footprint[names[i]] < footprint[names[j]] })
	report := make([]string, 0, len(names))
	for _, name := range names {
		report = append(report, fmt.Sprintf("%s=%d", name, footprint[name]))
	}
	t.Logf("packages linked per adapter: %s", strings.Join(report, " "))
}
