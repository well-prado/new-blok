package inspect_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/node"
	triggerhttp "github.com/well-prado/new-blok/trigger/http"
)

// sseFrame is one parsed Server-Sent Event.
type sseFrame struct {
	ID, Event, Data string
	At              time.Time
}

func (f sseFrame) decode(t *testing.T) inspection.Event {
	t.Helper()
	var value inspection.Event
	if err := json.Unmarshal([]byte(f.Data), &value); err != nil {
		t.Fatalf("frame %s data %q: %v", f.Event, f.Data, err)
	}
	return value
}

// sseStream reads a live response in the background, like EventSource.
type sseStream struct {
	response *http.Response
	frames   chan sseFrame
	done     chan struct{}
	mu       sync.Mutex
	comments []string
	retry    string
}

func (s *sseStream) Comments() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.comments...)
}

func (s *sseStream) read() {
	defer close(s.done)
	defer close(s.frames)
	scanner := bufio.NewScanner(s.response.Body)
	scanner.Buffer(make([]byte, 64<<10), 4<<20)
	var current sseFrame
	seen := false
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case line == "":
			if seen && current.Event != "" {
				current.At = time.Now()
				s.frames <- current
			}
			current, seen = sseFrame{}, false
		case strings.HasPrefix(line, ":"):
			s.mu.Lock()
			s.comments = append(s.comments, strings.TrimSpace(strings.TrimPrefix(line, ":")))
			s.mu.Unlock()
		case strings.HasPrefix(line, "id: "):
			current.ID, seen = strings.TrimPrefix(line, "id: "), true
		case strings.HasPrefix(line, "event: "):
			current.Event, seen = strings.TrimPrefix(line, "event: "), true
		case strings.HasPrefix(line, "data: "):
			current.Data, seen = strings.TrimPrefix(line, "data: "), true
		case strings.HasPrefix(line, "retry: "):
			s.mu.Lock()
			s.retry = strings.TrimPrefix(line, "retry: ")
			s.mu.Unlock()
		}
	}
}

// next returns the next frame, or false when the stream ended.
func (s *sseStream) next(t *testing.T, timeout time.Duration) (sseFrame, bool) {
	t.Helper()
	select {
	case frame, ok := <-s.frames:
		return frame, ok
	case <-time.After(timeout):
		t.Fatalf("no frame within %s", timeout)
		return sseFrame{}, false
	}
}

// until reads frames up to and including the first named event.
func (s *sseStream) until(t *testing.T, name string, timeout time.Duration) []sseFrame {
	t.Helper()
	var out []sseFrame
	deadline := time.Now().Add(timeout)
	for {
		frame, ok := s.next(t, time.Until(deadline))
		if !ok {
			t.Fatalf("stream ended before %q; frames=%v", name, frameNames(out))
		}
		out = append(out, frame)
		if frame.Event == name {
			return out
		}
	}
}

// rest reads every frame until the server ends the stream.
func (s *sseStream) rest(t *testing.T, timeout time.Duration) []sseFrame {
	t.Helper()
	var out []sseFrame
	deadline := time.Now().Add(timeout)
	for {
		frame, ok := s.next(t, time.Until(deadline))
		if !ok {
			return out
		}
		out = append(out, frame)
	}
}

func (s *sseStream) close() { _ = s.response.Body.Close(); <-s.done }

func frameNames(frames []sseFrame) []string {
	out := make([]string, len(frames))
	for i, frame := range frames {
		out[i] = frame.Event
	}
	return out
}

func stepNames(t *testing.T, frames []sseFrame) []string {
	out := make([]string, 0, len(frames))
	for _, frame := range frames {
		if strings.HasPrefix(frame.Event, "step.") {
			out = append(out, frame.Event+":"+frame.decode(t).StepID)
		} else {
			out = append(out, frame.Event)
		}
	}
	return out
}

// openStream issues GET with an optional Last-Event-ID. A non-200 response
// is returned with its body read and no stream.
func openStream(t *testing.T, ctx context.Context, url, principal, cursor string) (*http.Response, string, *sseStream) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if principal != "" {
		request.Header.Set("X-Principal", principal)
	}
	if cursor != "" {
		request.Header.Set("Last-Event-ID", cursor)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		return response, string(body), nil
	}
	stream := &sseStream{response: response, frames: make(chan sseFrame, 1<<14), done: make(chan struct{})}
	go stream.read()
	return response, "", stream
}

// awaitStream retries until the run is known to the stream (its first
// observation published), then returns the open stream.
func awaitStream(t *testing.T, ctx context.Context, url, principal string) *sseStream {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		response, body, stream := openStream(t, ctx, url, principal, "")
		if stream != nil {
			return stream
		}
		if response.StatusCode != http.StatusNotFound || time.Now().After(deadline) {
			t.Fatalf("stream status=%d body=%s", response.StatusCode, body)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func principalFromHeader(request *http.Request) (string, error) {
	principal := request.Header.Get("X-Principal")
	if principal == "" {
		return "", errors.New("unauthenticated")
	}
	return principal, nil
}

func allFields() inspection.Policy {
	return inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true, inspection.FieldOutput: true, inspection.FieldError: true, inspection.FieldLogs: true}}
}

// gateNode is a synthetic native node that logs a credential-shaped attribute
// and waits until released, so a subscriber can watch a run in flight.
type gateNode struct {
	// hold, when set, makes only runs for that SKU wait for release.
	hold    string
	release chan struct{}
	entered chan struct{}
	logs    int
	attr    string
}

func (g *gateNode) node(t *testing.T) node.Any {
	t.Helper()
	definition, err := node.Define("test/gate", "1.0.0", func(ctx context.Context, input quote.Input) (quote.Input, error) {
		logger := node.Logger(ctx)
		for i := 0; i < max(g.logs, 1); i++ {
			logger.Info("gate opened", "sku", input.SKU, "api_token", "synthetic-token-value", "n", i, "pad", g.attr)
		}
		if g.entered != nil && (g.hold == "" || g.hold == input.SKU) {
			select {
			case g.entered <- struct{}{}:
			default:
			}
		}
		if g.release != nil && (g.hold == "" || g.hold == input.SKU) {
			select {
			case <-g.release:
			case <-ctx.Done():
				return quote.Input{}, ctx.Err()
			}
		}
		return input, nil
	}, node.Description("Synthetic stream test node"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)))
	if err != nil {
		t.Fatal(err)
	}
	return definition.Any()
}

func gatedQuoteProgram() contract.InternalProgram {
	return contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "gate", Kind: "call", Node: "test/gate"},
		{Index: 1, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"},
		{Index: 2, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
}

// skuCatalog is the synthetic catalog dependency: it counts calls (the
// workflow's effect) and fails for the "failure" and "uncertain" SKUs.
type skuCatalog struct{ calls atomic.Int64 }

func (c *skuCatalog) PriceCents(_ context.Context, sku string) (int64, error) {
	c.calls.Add(1)
	switch sku {
	case "failure":
		return 0, errors.New("synthetic provider failure")
	case "uncertain":
		return 0, &node.DomainError{Code: "provider_outcome_unknown", Class: "transport", Uncertain: true}
	}
	return 1500, nil
}

// liveApp is an application composed the way a user composes one: the real
// trigger/http adapter admits a request, authenticates its principal and
// runs the workflow through execution.Runner, whose observer is the stream.
type liveApp struct {
	t        *testing.T
	server   *httptest.Server
	stream   *inspect.EventStream
	runner   *execution.Runner
	app      *app.Application
	catalog  *skuCatalog
	recorder *inspect.Recorder
	// active counts event handlers still running.
	active *atomic.Int64
}

type liveConfig struct {
	stream   inspect.EventStreamConfig
	handler  inspect.EventHandlerConfig
	outcomes app.RunOutcomePort
	nodes    map[string]node.Any
	program  contract.InternalProgram
	// identity maps a request to its trusted invocation; nil uses the path
	// run ID and the authenticated principal.
	identity func(triggerhttp.Input) inspection.Invocation
}

func newLiveApp(t *testing.T, config liveConfig) *liveApp {
	t.Helper()
	stream, err := inspect.NewEventStream(config.stream)
	if err != nil {
		t.Fatal(err)
	}
	recorder := inspect.NewRecorder()
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "quote"}}, Inspection: inspect.CombineObservers(stream, recorder), RunOutcomes: config.outcomes})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixtureCatalog := &skuCatalog{}
	quoteNode, err := quote.NewNode(fixtureCatalog)
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[string]node.Any{"shop/calculate-quote": quoteNode.Any()}
	for name, item := range config.nodes {
		nodes[name] = item
	}
	runner := execution.NewRunner(application, nodes)
	program := config.program
	if len(program.Instructions) == 0 {
		program = gatedQuoteProgram()
	}
	identity := config.identity
	if identity == nil {
		identity = func(input triggerhttp.Input) inspection.Invocation {
			return inspection.Invocation{RunID: input.Params["run"], Principal: input.Principal.ID}
		}
	}
	trigger, err := triggerhttp.New(application, []triggerhttp.Endpoint{{
		Method: http.MethodPost, Path: "/quotes/:run",
		InputSchema: []byte(`{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"}},"required":["sku","quantity"]}`),
		Authenticate: func(request *http.Request) (triggerhttp.Principal, error) {
			principal, err := principalFromHeader(request)
			return triggerhttp.Principal{ID: principal}, err
		},
		Handle: func(ctx context.Context, input triggerhttp.Input) (any, error) {
			var request quote.Input
			if err := json.Unmarshal(input.Body, &request); err != nil {
				return nil, err
			}
			result, err := runner.Run(ctx, program, request, identity(input))
			if err != nil {
				return nil, err
			}
			return map[string]any{"totalCents": result.Output}, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	handlerConfig := config.handler
	if handlerConfig.Authenticate == nil {
		handlerConfig.Authenticate = principalFromHeader
	}
	if handlerConfig.Policy == nil {
		handlerConfig.Policy = func(string) inspection.Policy { return allFields() }
	}
	events, err := inspect.NewEventHandler(stream, handlerConfig)
	if err != nil {
		t.Fatal(err)
	}
	active := &atomic.Int64{}
	mux := http.NewServeMux()
	mux.Handle("/quotes/", trigger)
	mux.Handle("/inspect/", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		active.Add(1)
		defer active.Add(-1)
		events.ServeHTTP(writer, request)
	}))
	server := httptest.NewServer(mux)
	live := &liveApp{t: t, server: server, stream: stream, runner: runner, app: application, catalog: fixtureCatalog, recorder: recorder, active: active}
	t.Cleanup(func() {
		stream.Hub().Close()
		server.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = application.Shutdown(ctx)
	})
	return live
}

func (l *liveApp) eventsURL(runID string) string {
	return l.server.URL + "/inspect/runs/" + runID + "/events"
}

// start posts a quote through the real HTTP trigger and returns a channel
// with its status and body.
func (l *liveApp) start(runID, principal string, input quote.Input) <-chan [2]string {
	result := make(chan [2]string, 1)
	go func() {
		body, _ := json.Marshal(input)
		request, _ := http.NewRequest(http.MethodPost, l.server.URL+"/quotes/"+runID, strings.NewReader(string(body)))
		request.Header.Set("X-Principal", principal)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			result <- [2]string{"error", err.Error()}
			return
		}
		data, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		result <- [2]string{response.Status, strings.TrimSpace(string(data))}
	}()
	return result
}

func awaitResult(t *testing.T, result <-chan [2]string) [2]string {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(20 * time.Second):
		t.Fatal("HTTP run did not finish")
		return [2]string{}
	}
}
