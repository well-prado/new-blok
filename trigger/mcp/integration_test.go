package mcp_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/tool"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
)

// secretMarker is synthetic: every error a tool returns carries it, and it
// must never reach the wire.
const secretMarker = "SYNTHETIC-SECRET-7f3a"

type classified struct{ code, class string }

func (c classified) Error() string      { return secretMarker + ": " + c.code }
func (c classified) ErrorCode() string  { return c.code }
func (c classified) ErrorClass() string { return c.class }

type fakeTool struct {
	tool       tmcp.Tool
	capability string
	run        func(context.Context, tmcp.Call) (json.RawMessage, error)
}

// fakeCatalog is the application's admission as the adapter sees it. It
// enforces capabilities on every call, as agent.Catalog does, and counts
// the calls that reach it and the effects its tools perform.
type fakeCatalog struct {
	tools       map[string]fakeTool
	invocations atomic.Int64
	effects     atomic.Int64
	mu          sync.Mutex
	calls       []tmcp.Call
	started     chan struct{}
	ended       chan error
	release     chan struct{}
}

func allows(principal tool.Principal, capability string) bool {
	return slices.Contains(principal.Capabilities, capability)
}

func (c *fakeCatalog) List(_ context.Context, principal tool.Principal) ([]tmcp.Tool, error) {
	var listed []tmcp.Tool
	for _, t := range c.tools {
		if allows(principal, t.capability) {
			listed = append(listed, t.tool)
		}
	}
	return listed, nil
}

func (c *fakeCatalog) Invoke(ctx context.Context, principal tool.Principal, call tmcp.Call) (json.RawMessage, error) {
	c.invocations.Add(1)
	c.mu.Lock()
	c.calls = append(c.calls, call)
	c.mu.Unlock()
	t, ok := c.tools[call.Name+"@"+call.Version]
	if !ok || !allows(principal, t.capability) {
		return nil, approval.ErrDenied
	}
	return t.run(ctx, call)
}

func (c *fakeCatalog) lastCall() tmcp.Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[len(c.calls)-1]
}

func objectSchema(properties string, required ...string) json.RawMessage {
	names, _ := json.Marshal(required)
	if len(required) == 0 {
		names = []byte("[]")
	}
	return json.RawMessage(`{"type":"object","properties":{` + properties + `},"required":` + string(names) + `,"additionalProperties":false}`)
}

func newCatalog() *fakeCatalog {
	c := &fakeCatalog{tools: map[string]fakeTool{}, started: make(chan struct{}, 64), ended: make(chan error, 64), release: make(chan struct{})}
	empty := objectSchema("")
	echoOut := objectSchema(`"echo":{"type":"string"}`, "echo")
	add := func(name, capability string, input, output json.RawMessage, effects []string, run func(context.Context, tmcp.Call) (json.RawMessage, error)) {
		c.tools[name+"@1.0.0"] = fakeTool{tool: tmcp.Tool{Name: name, Version: "1.0.0", Description: "Synthetic " + name, InputSchema: input, OutputSchema: output, Effects: effects}, capability: capability, run: run}
	}
	add("demo/echo", "echo", objectSchema(`"text":{"type":"string"}`, "text"), echoOut, nil, func(_ context.Context, call tmcp.Call) (json.RawMessage, error) {
		var in struct{ Text string }
		if err := json.Unmarshal(call.Input, &in); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]string{"echo": in.Text})
	})
	add("demo/write", "write", objectSchema(`"amount":{"type":"integer","minimum":1}`, "amount"), objectSchema(`"ok":{"type":"boolean"}`, "ok"), []string{"db:write"}, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		c.effects.Add(1)
		return json.RawMessage(`{"ok":true}`), nil
	})
	add("demo/hidden", "echo", empty, nil, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		return json.RawMessage(`{}`), nil
	})
	add("demo/list", "echo", json.RawMessage(`{"type":"array","items":{"type":"string"}}`), nil, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		return json.RawMessage(`[]`), nil
	})
	add("demo/panic", "echo", empty, nil, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		panic(secretMarker + ": panic")
	})
	add("demo/secret", "echo", empty, nil, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		return nil, errors.New(secretMarker + ": dsn=postgres://synthetic:password@db")
	})
	add("demo/badout", "echo", empty, echoOut, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		return json.RawMessage(`{"echo":5}`), nil
	})
	add("demo/classified", "echo", empty, nil, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		return nil, fmt.Errorf("wrapped: %w", classified{code: "out_of_stock", class: "domain"})
	})
	add("demo/config", "echo", empty, nil, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		return nil, classified{code: "bad_config", class: "configuration"}
	})
	add("demo/budget", "echo", empty, nil, nil, func(context.Context, tmcp.Call) (json.RawMessage, error) {
		return nil, fmt.Errorf("%s: %w", secretMarker, tool.ErrBudget)
	})
	add("demo/meta", "echo", empty, objectSchema(`"approval":{"type":"string"}`), nil, func(_ context.Context, call tmcp.Call) (json.RawMessage, error) {
		return json.Marshal(map[string]string{"approval": call.Approval})
	})
	// block runs until its context ends or the test releases it, and
	// reports how it ended.
	add("demo/block", "echo", empty, nil, nil, func(ctx context.Context, _ tmcp.Call) (json.RawMessage, error) {
		c.started <- struct{}{}
		select {
		case <-ctx.Done():
			c.ended <- ctx.Err()
			return nil, ctx.Err()
		case <-c.release:
			c.ended <- nil
			return json.RawMessage(`{}`), nil
		}
	})
	return c
}

// wireLog keeps every response byte a client received.
type wireLog struct {
	mu   sync.Mutex
	data bytes.Buffer
}

func (w *wireLog) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.data.Write(p)
}

func (w *wireLog) String() string { w.mu.Lock(); defer w.mu.Unlock(); return w.data.String() }

type teeBody struct {
	io.Reader
	io.Closer
}

// recordingClient adds a bearer token and copies every response body into
// the wire log.
type recordingClient struct {
	token string
	log   *wireLog
}

func (r recordingClient) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	if r.token != "" {
		request.Header.Set("Authorization", "Bearer "+r.token)
	}
	response, err := http.DefaultTransport.RoundTrip(request)
	if err != nil || r.log == nil {
		return response, err
	}
	response.Body = teeBody{Reader: io.TeeReader(response.Body, r.log), Closer: response.Body}
	return response, nil
}

type rig struct {
	t        *testing.T
	catalog  *fakeCatalog
	app      *app.Application
	adapter  *tmcp.Server
	server   *http.Server
	endpoint string
	wire     *wireLog
	mu       sync.Mutex
	tokens   map[string]tool.Principal
	stopped  bool
}

func newRig(t *testing.T, configure func(*tmcp.Config)) *rig {
	t.Helper()
	r := &rig{t: t, catalog: newCatalog(), wire: &wireLog{}, tokens: map[string]tool.Principal{
		"alice":  {ID: "alice", Capabilities: []string{"echo", "write"}, MaxDepth: 4},
		"bob":    {ID: "bob", Capabilities: []string{"echo"}, MaxDepth: 4},
		"nobody": {ID: "nobody", MaxDepth: 4},
	}}
	application, err := app.New(app.Config{DrainTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	config := tmcp.Config{Name: "integration", Version: "1.0.0", Catalog: r.catalog, Authenticate: r.authenticate, Expose: []string{"demo/echo@1.0.0", "demo/write@1.0.0", "demo/block@1.0.0"}, Budget: budget()}
	if configure != nil {
		configure(&config)
	}
	adapter, err := tmcp.New(application, config)
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r.app, r.adapter, r.endpoint = application, adapter, "http://"+listener.Addr().String()
	r.server = &http.Server{Handler: adapter, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan error, 1)
	go func() { served <- r.server.Serve(listener) }()
	t.Cleanup(func() {
		r.stop()
		if err := <-served; err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("serve: %v", err)
		}
	})
	return r
}

func (r *rig) authenticate(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	principal, ok := r.tokens[token]
	if !ok {
		return tool.Principal{}, errors.New(secretMarker + ": unknown token")
	}
	principal.Capabilities = slices.Clone(principal.Capabilities)
	return principal, nil
}

func (r *rig) grant(token string, principal tool.Principal) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tokens[token] = principal
}

func (r *rig) stop() {
	r.mu.Lock()
	stopped := r.stopped
	r.stopped = true
	r.mu.Unlock()
	if stopped {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := r.adapter.Shutdown(ctx); err != nil {
		r.t.Errorf("adapter shutdown: %v", err)
	}
	if err := r.app.Shutdown(ctx); err != nil {
		r.t.Errorf("app shutdown: %v", err)
	}
	_ = r.server.Close()
}

func (r *rig) connect(token string) *sdk.ClientSession {
	r.t.Helper()
	session, err := r.dial(token)
	if err != nil {
		r.t.Fatalf("connect as %q: %v", token, err)
	}
	r.t.Cleanup(func() { _ = session.Close() })
	return session
}

func (r *rig) dial(token string) (*sdk.ClientSession, error) {
	client := sdk.NewClient(&sdk.Implementation{Name: "integration-client", Version: "1.0.0"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: r.endpoint, HTTPClient: &http.Client{Transport: recordingClient{token: token, log: r.wire}}, MaxRetries: -1}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return client.Connect(ctx, transport, nil)
}

// raw sends one JSON-RPC message as token on session, outside the SDK
// client, and returns the status and body.
func (r *rig) raw(method, token, session, version string, message any) (int, string) {
	r.t.Helper()
	var body io.Reader
	if message != nil {
		data, err := json.Marshal(message)
		if err != nil {
			r.t.Fatal(err)
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, r.endpoint, body)
	if err != nil {
		r.t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json, text/event-stream")
	if session != "" {
		request.Header.Set("Mcp-Session-Id", session)
	}
	if version != "" {
		request.Header.Set("Mcp-Protocol-Version", version)
	}
	response, err := (&http.Client{Transport: recordingClient{token: token, log: r.wire}, Timeout: 10 * time.Second}).Do(request)
	if err != nil {
		r.t.Fatal(err)
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(data)
}

func call(session *sdk.ClientSession, name string, arguments any, meta sdk.Meta) (*sdk.CallToolResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return session.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: arguments, Meta: meta})
}

func names(t *testing.T, session *sdk.ClientSession) []string {
	t.Helper()
	listed, err := session.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	found := []string{}
	for _, t := range listed.Tools {
		found = append(found, t.Name)
	}
	sort.Strings(found)
	return found
}

func protocolError(err error) (*jsonrpc.Error, bool) {
	var wire *jsonrpc.Error
	return wire, errors.As(err, &wire)
}

func jsonEqual(a, b []byte) bool {
	var x, y any
	return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
}

type fixture struct {
	Version   int      `json:"version"`
	Roadmap   string   `json:"roadmap"`
	Purpose   string   `json:"purpose"`
	Expose    []string `json:"expose"`
	Discovery []struct {
		Token string   `json:"token"`
		Tools []string `json:"tools"`
	} `json:"discovery"`
	Calls []struct {
		Name      string          `json:"name"`
		Token     string          `json:"token"`
		Tool      string          `json:"tool"`
		Arguments json.RawMessage `json:"arguments"`
		Approval  any             `json:"approval"`
		Expect    struct {
			Kind        string          `json:"kind"`
			Output      json.RawMessage `json:"output"`
			Code        string          `json:"code"`
			RPCCode     int64           `json:"rpcCode"`
			Message     string          `json:"message"`
			Invocations int64           `json:"invocations"`
			Effects     int64           `json:"effects"`
		} `json:"expect"`
	} `json:"calls"`
}

func loadFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "mcp", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&f); err != nil {
		t.Fatal(err)
	}
	if f.Version != 1 || f.Roadmap != "E09-T08" || len(f.Calls) != 19 || len(f.Discovery) != 3 {
		t.Fatalf("fixture version=%d roadmap=%q calls=%d discovery=%d", f.Version, f.Roadmap, len(f.Calls), len(f.Discovery))
	}
	return f
}

// TestFixtureCasesOverRealMCP runs every predeclared case with the official
// client over HTTP and checks the outcome, the calls that reached the
// catalog and the effects performed.
func TestFixtureCasesOverRealMCP(t *testing.T) {
	f := loadFixture(t)
	r := newRig(t, func(c *tmcp.Config) { c.Expose = f.Expose })
	sessions := map[string]*sdk.ClientSession{}
	session := func(token string) *sdk.ClientSession {
		if sessions[token] == nil {
			sessions[token] = r.connect(token)
		}
		return sessions[token]
	}
	for _, d := range f.Discovery {
		s := session(d.Token)
		if got := names(t, s); !slices.Equal(got, d.Tools) {
			t.Fatalf("%s sees tools %v, want %v", d.Token, got, d.Tools)
		}
		resources, err := s.ListResources(context.Background(), nil)
		if err != nil {
			t.Fatal(err)
		}
		uris := []string{}
		for _, resource := range resources.Resources {
			uris = append(uris, resource.Name)
			if !strings.HasPrefix(resource.URI, "newblok://tools/demo/") || resource.MIMEType != "application/json" {
				t.Fatalf("resource %+v", resource)
			}
		}
		sort.Strings(uris)
		if !slices.Equal(uris, d.Tools) {
			t.Fatalf("%s sees resources %v, want one per tool %v", d.Token, uris, d.Tools)
		}
	}
	read, err := session("bob").ReadResource(context.Background(), &sdk.ReadResourceParams{URI: "newblok://tools/demo/echo/1.0.0"})
	if err != nil || len(read.Contents) != 1 {
		t.Fatalf("read=%+v err=%v", read, err)
	}
	var descriptor struct {
		Name, Version string
		InputSchema   json.RawMessage
		Effects       []string
	}
	if err := json.Unmarshal([]byte(read.Contents[0].Text), &descriptor); err != nil || descriptor.Name != "demo/echo" || descriptor.Version != "1.0.0" || !jsonEqual(descriptor.InputSchema, r.catalog.tools["demo/echo@1.0.0"].tool.InputSchema) {
		t.Fatalf("descriptor %s err=%v", read.Contents[0].Text, err)
	}
	if _, err := session("bob").ReadResource(context.Background(), &sdk.ReadResourceParams{URI: "newblok://tools/demo/write/1.0.0"}); err == nil {
		t.Fatal("bob read the descriptor of a tool beyond his capabilities")
	}
	for _, c := range f.Calls {
		beforeCalls, beforeEffects := r.catalog.invocations.Load(), r.catalog.effects.Load()
		var meta sdk.Meta
		if c.Approval != nil {
			meta = sdk.Meta{tmcp.ApprovalMeta: c.Approval}
		}
		result, err := call(session(c.Token), c.Tool, c.Arguments, meta)
		switch c.Expect.Kind {
		case "protocol":
			wire, ok := protocolError(err)
			if !ok || wire.Code != c.Expect.RPCCode || (c.Expect.Message != "" && wire.Message != c.Expect.Message) {
				t.Fatalf("%s: result=%+v err=%v; want protocol error %d %q", c.Name, result, err, c.Expect.RPCCode, c.Expect.Message)
			}
		case "toolError":
			if err != nil || !result.IsError || toolCode(result) != c.Expect.Code {
				t.Fatalf("%s: result=%+v err=%v; want tool error %q", c.Name, result, err, c.Expect.Code)
			}
		case "result":
			if err != nil || result.IsError {
				t.Fatalf("%s: result=%+v err=%v", c.Name, result, err)
			}
			output, _ := json.Marshal(result.StructuredContent)
			if !jsonEqual(output, c.Expect.Output) {
				t.Fatalf("%s: output %s, want %s", c.Name, output, c.Expect.Output)
			}
		default:
			t.Fatalf("%s: unknown kind %q", c.Name, c.Expect.Kind)
		}
		if calls, effects := r.catalog.invocations.Load()-beforeCalls, r.catalog.effects.Load()-beforeEffects; calls != c.Expect.Invocations || effects != c.Expect.Effects {
			t.Fatalf("%s: invocations=%d effects=%d, want %d and %d", c.Name, calls, effects, c.Expect.Invocations, c.Expect.Effects)
		}
	}
	if call := r.catalog.lastCall(); call.Name != "demo/meta" || call.Approval != "review-7" {
		t.Fatalf("approval reached the catalog as %+v", call)
	}
	for _, leaked := range []string{secretMarker, "postgres://", "password"} {
		if strings.Contains(r.wire.String(), leaked) {
			t.Fatalf("a response carried %q", leaked)
		}
	}
	if len(r.wire.String()) == 0 {
		t.Fatal("no response bytes were recorded")
	}
}

func TestUnauthenticatedCallersAreRefused(t *testing.T) {
	r := newRig(t, nil)
	for _, token := range []string{"", "forged"} {
		if session, err := r.dial(token); err == nil {
			session.Close()
			t.Fatalf("token %q opened a session", token)
		}
	}
	status, body := r.raw(http.MethodPost, "forged", "", "", map[string]any{"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
	if status != http.StatusUnauthorized || strings.Contains(body, secretMarker) {
		t.Fatalf("status=%d body=%q", status, body)
	}
	if n := r.catalog.invocations.Load(); n != 0 {
		t.Fatalf("an unauthenticated caller reached the catalog %d times", n)
	}
}

// TestCallsRunAsTheCurrentPrincipal revokes a capability while a session
// stays open: the next call runs with the revoked set and is denied.
func TestCallsRunAsTheCurrentPrincipal(t *testing.T) {
	r := newRig(t, nil)
	session := r.connect("bob")
	if result, err := call(session, "demo.echo_v1.0.0", map[string]any{"text": "a"}, nil); err != nil || result.IsError {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	r.grant("bob", tool.Principal{ID: "bob", MaxDepth: 4})
	result, err := call(session, "demo.echo_v1.0.0", map[string]any{"text": "a"}, nil)
	if err != nil || !result.IsError || toolCode(result) != "denied" {
		t.Fatalf("revoked call: result=%+v err=%v", result, err)
	}
}

func TestSessionsCannotBeHijacked(t *testing.T) {
	r := newRig(t, nil)
	alice := r.connect("alice")
	version := alice.InitializeResult().ProtocolVersion
	t.Logf("negotiated protocol %s, session %q", version, alice.ID())
	if alice.ID() == "" {
		t.Fatal("the transport opened no session")
	}
	list := map[string]any{"jsonrpc": "2.0", "id": 9, "method": "tools/list"}
	if status, _ := r.raw(http.MethodPost, "bob", alice.ID(), version, list); status != http.StatusForbidden {
		t.Fatalf("bob used alice's session: status=%d", status)
	}
	done := make(chan error, 1)
	go func() {
		result, err := call(alice, "demo.block_v1.0.0", map[string]any{}, nil)
		if err == nil && result.IsError {
			err = errors.New(toolCode(result))
		}
		done <- err
	}()
	<-r.catalog.started
	if status, _ := r.raw(http.MethodDelete, "bob", alice.ID(), version, nil); status != http.StatusForbidden {
		t.Fatalf("bob ended alice's session: status=%d", status)
	}
	close(r.catalog.release)
	if err := <-r.catalog.ended; err != nil {
		t.Fatalf("bob's DELETE canceled alice's call: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("alice's call: %v", err)
	}
}

func TestUnsupportedProtocolVersionIsRefused(t *testing.T) {
	r := newRig(t, nil)
	alice := r.connect("alice")
	list := map[string]any{"jsonrpc": "2.0", "id": 9, "method": "tools/list"}
	if status, _ := r.raw(http.MethodPost, "alice", alice.ID(), "1999-01-01", list); status != http.StatusBadRequest {
		t.Fatalf("an unsupported protocol version was served: status=%d", status)
	}
	// A session-less call under the draft protocol would escape the session
	// that binds a caller and ends its work; the server must refuse it.
	draft := map[string]any{"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": map[string]any{"name": "demo.echo_v1.0.0", "arguments": map[string]any{"text": "a"}, "_meta": map[string]any{"io.modelcontextprotocol/protocolVersion": "2026-07-28"}}}
	if status, body := r.raw(http.MethodPost, "alice", "", "2026-07-28", draft); status != http.StatusBadRequest || !strings.Contains(body, "-32022") {
		t.Fatalf("a session-less draft-protocol call: status=%d body=%q", status, body)
	}
	if n := r.catalog.invocations.Load(); n != 0 {
		t.Fatalf("a refused protocol reached the catalog %d times", n)
	}
	initialize := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": map[string]any{"protocolVersion": "1999-01-01", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "old", "version": "0"}}}
	status, body := r.raw(http.MethodPost, "alice", "", "", initialize)
	if status != http.StatusOK || strings.Contains(body, `"protocolVersion":"1999-01-01"`) || !strings.Contains(body, `"protocolVersion":"`+version(t, body)+`"`) {
		t.Fatalf("initialize with an unknown version: status=%d body=%q", status, body)
	}
}

// version extracts the negotiated version and requires it to be one the SDK
// supports.
func version(t *testing.T, body string) string {
	t.Helper()
	_, after, ok := strings.Cut(body, `"protocolVersion":"`)
	if !ok {
		t.Fatalf("no protocol version in %q", body)
	}
	negotiated, _, _ := strings.Cut(after, `"`)
	if !slices.Contains(sdk.SupportedProtocolVersions(), negotiated) {
		t.Fatalf("negotiated unsupported version %q", negotiated)
	}
	return negotiated
}

func TestDeadlineBoundsACall(t *testing.T) {
	r := newRig(t, func(c *tmcp.Config) { c.Timeout = 200 * time.Millisecond })
	session := r.connect("alice")
	begin := time.Now()
	result, err := call(session, "demo.block_v1.0.0", map[string]any{}, nil)
	if err != nil || !result.IsError || toolCode(result) != "deadline_exceeded" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if ended := <-r.catalog.ended; !errors.Is(ended, context.DeadlineExceeded) {
		t.Fatalf("the work ended with %v", ended)
	}
	if elapsed := time.Since(begin); elapsed > 5*time.Second {
		t.Fatalf("the call took %v", elapsed)
	}
	call := r.catalog.lastCall()
	if call.Budget.Deadline.IsZero() || call.Budget.Deadline.Sub(begin) > time.Second {
		t.Fatalf("the budget deadline %v does not follow the timeout", call.Budget.Deadline)
	}
}

func TestClientCancelCancelsTheWork(t *testing.T) {
	r := newRig(t, nil)
	session := r.connect("alice")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { <-r.catalog.started; cancel() }()
	if _, err := session.CallTool(ctx, &sdk.CallToolParams{Name: "demo.block_v1.0.0", Arguments: map[string]any{}}); !errors.Is(err, context.Canceled) {
		t.Fatalf("call err=%v", err)
	}
	select {
	case err := <-r.catalog.ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the work ended with %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceling the call did not cancel the work")
	}
}

// TestSessionEndCancelsTheWork ends a session with a bare DELETE: no
// cancellation notification is sent, so only the session end can cancel.
func TestSessionEndCancelsTheWork(t *testing.T) {
	r := newRig(t, nil)
	session := r.connect("alice")
	go func() { _, _ = call(session, "demo.block_v1.0.0", map[string]any{}, nil) }()
	<-r.catalog.started
	begin := time.Now()
	status, _ := r.raw(http.MethodDelete, "alice", session.ID(), session.InitializeResult().ProtocolVersion, nil)
	if status != http.StatusNoContent {
		t.Fatalf("DELETE status=%d", status)
	}
	select {
	case err := <-r.catalog.ended:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the work ended with %v", err)
		}
	default:
		t.Fatal("the DELETE was answered before the work was canceled")
	}
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("ending the session took %v", elapsed)
	}
}

func TestOverloadIsRefusedNotQueued(t *testing.T) {
	r := newRig(t, func(c *tmcp.Config) { c.MaxConcurrency = 2 })
	session := r.connect("alice")
	results := make(chan string, 2)
	for range 2 {
		go func() {
			result, err := call(session, "demo.block_v1.0.0", map[string]any{}, nil)
			if err != nil {
				results <- err.Error()
				return
			}
			results <- toolCode(result)
		}()
	}
	<-r.catalog.started
	<-r.catalog.started
	result, err := call(session, "demo.echo_v1.0.0", map[string]any{"text": "a"}, nil)
	if err != nil || !result.IsError || toolCode(result) != "saturated" {
		t.Fatalf("third call: result=%+v err=%v", result, err)
	}
	if n := r.catalog.invocations.Load(); n != 2 {
		t.Fatalf("the catalog saw %d calls, want the 2 admitted", n)
	}
	close(r.catalog.release)
	for range 2 {
		if code := <-results; code != "" {
			t.Fatalf("an admitted call ended with %q", code)
		}
	}
	if result, err := call(session, "demo.echo_v1.0.0", map[string]any{"text": "a"}, nil); err != nil || result.IsError {
		t.Fatalf("a call after the burst: result=%+v err=%v", result, err)
	}
}

func TestDrainingApplicationRefusesRequests(t *testing.T) {
	r := newRig(t, nil)
	session := r.connect("alice")
	version := session.InitializeResult().ProtocolVersion
	go func() { _, _ = call(session, "demo.block_v1.0.0", map[string]any{}, nil) }()
	<-r.catalog.started
	drained := make(chan error, 1)
	go func() { drained <- r.app.Shutdown(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for r.app.State() != app.DrainingState {
		if time.Now().After(deadline) {
			t.Fatal("the application never started draining")
		}
		time.Sleep(time.Millisecond)
	}
	list := map[string]any{"jsonrpc": "2.0", "id": 9, "method": "tools/list"}
	if status, _ := r.raw(http.MethodPost, "alice", session.ID(), version, list); status != http.StatusServiceUnavailable {
		t.Fatalf("a draining application served a request: status=%d", status)
	}
	select {
	case err := <-drained:
		t.Fatalf("the application stopped with a call in flight: %v", err)
	default:
	}
	close(r.catalog.release)
	if err := <-drained; err != nil {
		t.Fatalf("drain: %v", err)
	}
}

func TestShutdownWaitsForCallsThenClosesSessions(t *testing.T) {
	r := newRig(t, nil)
	session := r.connect("alice")
	version := session.InitializeResult().ProtocolVersion
	done := make(chan error, 1)
	go func() {
		result, err := call(session, "demo.block_v1.0.0", map[string]any{}, nil)
		if err == nil && result.IsError {
			err = errors.New(toolCode(result))
		}
		done <- err
	}()
	<-r.catalog.started
	short, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := r.adapter.Shutdown(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown returned %v with a call in flight", err)
	}
	list := map[string]any{"jsonrpc": "2.0", "id": 9, "method": "tools/list"}
	if status, _ := r.raw(http.MethodPost, "alice", session.ID(), version, list); status != http.StatusServiceUnavailable {
		t.Fatalf("a closing server served a request: status=%d", status)
	}
	close(r.catalog.release)
	if err := <-done; err != nil {
		t.Fatalf("the call in flight did not complete: %v", err)
	}
	if err := r.adapter.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed := make(chan struct{})
	go func() { _ = session.Wait(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Shutdown left the session open")
	}
}

// TestEvictedViewsSessionsCloseOnShutdown opens more principals than the
// view cache holds and requires Shutdown to close every session.
func TestEvictedViewsSessionsCloseOnShutdown(t *testing.T) {
	r := newRig(t, func(c *tmcp.Config) { c.MaxPrincipals = 1 })
	r.grant("carol", tool.Principal{ID: "carol", Capabilities: []string{"echo"}, MaxDepth: 4})
	first := r.connect("alice")
	second := r.connect("carol")
	for _, s := range []*sdk.ClientSession{first, second} {
		if result, err := call(s, "demo.echo_v1.0.0", map[string]any{"text": "a"}, nil); err != nil || result.IsError {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if err := r.adapter.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, s := range []*sdk.ClientSession{first, second} {
		closed := make(chan struct{})
		go func() { _ = s.Wait(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("Shutdown left a session open")
		}
	}
}

// TestNoGoroutinesOutliveTheServer runs sessions and calls, stops
// everything, and requires the goroutine count to return to its baseline.
func TestNoGoroutinesOutliveTheServer(t *testing.T) {
	baseline := runtime.NumGoroutine()
	func() {
		r := newRig(t, nil)
		for range 8 {
			session, err := r.dial("alice")
			if err != nil {
				t.Fatal(err)
			}
			if result, err := call(session, "demo.echo_v1.0.0", map[string]any{"text": "a"}, nil); err != nil || result.IsError {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			_ = session.Close()
		}
		r.stop()
	}()
	http.DefaultTransport.(*http.Transport).CloseIdleConnections()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > baseline+2 {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			t.Fatalf("goroutines %d, baseline %d\n%s", runtime.NumGoroutine(), baseline, buf[:runtime.Stack(buf, true)])
		}
		time.Sleep(10 * time.Millisecond)
	}
}
