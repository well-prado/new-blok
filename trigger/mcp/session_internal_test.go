package mcp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/auth"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
)

type emptyCatalog struct{ invocations *atomic.Int64 }

func (emptyCatalog) List(context.Context, tool.Principal) ([]Tool, error) { return nil, nil }
func (c emptyCatalog) Invoke(context.Context, tool.Principal, Call) (json.RawMessage, error) {
	if c.invocations != nil {
		c.invocations.Add(1)
	}
	return json.RawMessage(`{}`), nil
}

// TestEndedSessionCancelsLateCalls: a call that registers after its owner
// ended the session (it was still queued) is canceled at once, and only the
// owner's DELETE ends a session.
func TestEndedSessionCancelsLateCalls(t *testing.T) {
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(application, Config{Name: "n", Version: "1", Catalog: emptyCatalog{}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		return tool.Principal{ID: token, MaxDepth: 4}, nil
	}, Expose: []string{"demo/echo@1"}, Budget: tool.Budget{MaxDepth: 4, MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxTokens: 1, MaxCalls: 1, Deadline: time.Now().Add(time.Hour)}})
	if err != nil {
		t.Fatal(err)
	}
	s.live["alice-session"] = liveSession{owner: "alice"}
	end := auth.RequireBearerToken(s.verify, nil)(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) { s.endSession(request) }))
	remove := func(token string) {
		request := httptest.NewRequest(http.MethodDelete, "/", nil)
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set(sessionHeader, "alice-session")
		end.ServeHTTP(httptest.NewRecorder(), request)
	}
	remove("bob")
	early, cancelEarly := context.WithCancel(context.Background())
	defer s.track("alice-session", cancelEarly)()
	if early.Err() != nil || s.ended["alice-session"] {
		t.Fatal("another principal's DELETE ended alice's session")
	}
	remove("alice")
	if early.Err() == nil {
		t.Fatal("the owner's DELETE did not cancel a call in flight")
	}
	late, cancelLate := context.WithCancel(context.Background())
	defer s.track("alice-session", cancelLate)()
	if late.Err() == nil {
		t.Fatal("a call that started after its session ended was not canceled")
	}
	s.sessions, s.opened["alice"] = 1, 1
	s.closed("alice", "alice-session")
	if s.ended["alice-session"] || s.live["alice-session"].owner != "" || s.sessions != 0 || len(s.opened) != 0 {
		t.Fatalf("closing the session left state behind: ended=%v live=%v sessions=%d opened=%v", s.ended, s.live, s.sessions, s.opened)
	}
}

// headerProbe records the open-session count at the moment the response
// header is written.
type headerProbe struct {
	*httptest.ResponseRecorder
	s        *Server
	sessions int
}

func (p *headerProbe) WriteHeader(status int) {
	p.s.mu.Lock()
	p.sessions = p.s.sessions
	p.s.mu.Unlock()
	p.ResponseRecorder.WriteHeader(status)
}

// TestSessionOpeningSettlesBeforeTheResponse: a request that opens no
// session frees its place before the client can see the response, so the
// client's next request is never refused for it.
func TestSessionOpeningSettlesBeforeTheResponse(t *testing.T) {
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	s, err := New(application, Config{Name: "n", Version: "1", Catalog: emptyCatalog{}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		return tool.Principal{ID: token, MaxDepth: 4}, nil
	}, Expose: []string{"demo/echo@1"}, Budget: tool.Budget{MaxDepth: 4, MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxTokens: 1, MaxCalls: 1, Deadline: time.Now().Add(time.Hour)}})
	if err != nil {
		t.Fatal(err)
	}
	refusing := http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		http.Error(writer, "unsupported", http.StatusBadRequest)
	})
	open := auth.RequireBearerToken(s.verify, nil)(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		s.open(writer, request, refusing)
	}))
	probe := &headerProbe{ResponseRecorder: httptest.NewRecorder(), s: s, sessions: -1}
	request := httptest.NewRequest(http.MethodPost, "/", nil)
	request.Header.Set("Authorization", "Bearer alice")
	open.ServeHTTP(probe, request)
	if probe.Code != http.StatusBadRequest || probe.sessions != 0 || s.sessions != 0 || len(s.opened) != 0 {
		t.Fatalf("status=%d sessions at header=%d after=%d opened=%v", probe.Code, probe.sessions, s.sessions, s.opened)
	}
}

// TestCanceledCallNeverReachesTheCatalog: a call whose request is already
// gone is refused before the catalog sees it.
func TestCanceledCallNeverReachesTheCatalog(t *testing.T) {
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	invocations := &atomic.Int64{}
	s, err := New(application, Config{Name: "n", Version: "1", Catalog: emptyCatalog{invocations: invocations}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		return tool.Principal{ID: token, MaxDepth: 4}, nil
	}, Expose: []string{"demo/echo@1"}, Budget: tool.Budget{MaxDepth: 4, MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxTokens: 1, MaxCalls: 1, Deadline: time.Now().Add(time.Hour)}})
	if err != nil {
		t.Fatal(err)
	}
	principal := tool.Principal{ID: "alice", MaxDepth: 4}
	handler := s.handler(principal, Tool{Name: "demo/echo", Version: "1"}, mustParse(t, `{"type":"object"}`), nil)
	request := func(carrier context.Context) *sdk.CallToolRequest {
		return &sdk.CallToolRequest{Params: &sdk.CallToolParamsRaw{Name: "demo.echo_v1", Arguments: json.RawMessage(`{}`)}, Extra: &sdk.RequestExtra{TokenInfo: &auth.TokenInfo{UserID: "alice", Extra: map[string]any{"principal": principal, "request": carrier}}}}
	}
	if result, err := handler(context.Background(), request(context.Background())); err != nil || result.IsError || invocations.Load() != 1 {
		t.Fatalf("a live call: result=%+v err=%v invocations=%d", result, err, invocations.Load())
	}
	gone, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := handler(context.Background(), request(gone))
	if err != nil || !result.IsError || invocations.Load() != 1 {
		t.Fatalf("a call whose request is gone: result=%+v err=%v invocations=%d", result, err, invocations.Load())
	}
}

func mustParse(t *testing.T, text string) schema.Schema {
	t.Helper()
	parsed, err := schema.Parse([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
