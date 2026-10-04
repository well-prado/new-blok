package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/internal/drainprobe"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
)

// heldCatalog exposes one tool whose call is held work. With listing set,
// listing the tools is held work instead.
type heldCatalog struct {
	work    *drainprobe.Held
	listing bool
}

var heldSchema = []byte(`{"type":"object"}`)

func (c heldCatalog) List(ctx context.Context, _ tool.Principal) ([]tmcp.Tool, error) {
	if c.listing {
		if err := c.work.Run(ctx); err != nil {
			return nil, err
		}
	}
	return []tmcp.Tool{{Name: "drain/hold", Version: "1.0.0", Description: "Holds", InputSchema: heldSchema, OutputSchema: heldSchema}}, nil
}

func (c heldCatalog) Invoke(ctx context.Context, _ tool.Principal, _ tmcp.Call) (json.RawMessage, error) {
	return nil, c.work.Run(ctx)
}

type bearer struct{ base http.RoundTripper }

func (b bearer) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("Authorization", "Bearer alice")
	return b.base.RoundTrip(request)
}

// TestDrainTimeoutCancelsAToolCall: when the drain times out under a tool
// call, the call is canceled and stops before the application closes its
// dependencies. It is answered canceled, not as a refusal to retry: it may
// have committed (#177).
func TestDrainTimeoutCancelsAToolCall(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	adapter, err := tmcp.New(application, tmcp.Config{Name: "drain", Version: "1.0.0", Catalog: heldCatalog{work: work}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		if token != "alice" {
			return tool.Principal{}, errors.New("unauthenticated")
		}
		return tool.Principal{ID: "alice", MaxDepth: 4}, nil
	}, Expose: []string{"drain/hold@1.0.0"}, Budget: budget(), Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(adapter)
	defer web.Close()
	ctx := context.Background()
	session, err := sdk.NewClient(&sdk.Implementation{Name: "drain", Version: "1"}, nil).Connect(ctx, &sdk.StreamableClientTransport{Endpoint: web.URL, HTTPClient: &http.Client{Transport: bearer{base: http.DefaultTransport}}, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	type answer struct {
		text string
		err  error
	}
	answered := make(chan answer, 1)
	go func() {
		result, err := session.CallTool(ctx, &sdk.CallToolParams{Name: tmcp.ToolName("drain/hold", "1.0.0"), Arguments: json.RawMessage(`{}`)})
		if err != nil {
			answered <- answer{err: err}
			return
		}
		text := ""
		if len(result.Content) > 0 {
			if content, ok := result.Content[0].(*sdk.TextContent); ok {
				text = content.Text
			}
		}
		answered <- answer{text: text}
	}()
	drainprobe.Abort(t, application, probe, work)
	if got := <-answered; got.err != nil || got.text != `{"code":"canceled"}` {
		t.Fatalf("tool call answered %+v; want canceled", got)
	}
	stop, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = adapter.Shutdown(stop)
}

// TestDrainTimeoutCancelsAListing: a request's own work, here listing the
// principal's tools to open its session, also stops when the drain times
// out, before the application closes its dependencies (#177).
func TestDrainTimeoutCancelsAListing(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	adapter, err := tmcp.New(application, tmcp.Config{Name: "drain", Version: "1.0.0", Catalog: heldCatalog{work: work, listing: true}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		if token != "alice" {
			return tool.Principal{}, errors.New("unauthenticated")
		}
		return tool.Principal{ID: "alice", MaxDepth: 4}, nil
	}, Expose: []string{"drain/hold@1.0.0"}, Budget: budget(), Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(adapter)
	defer web.Close()
	connected := make(chan error, 1)
	go func() {
		session, err := sdk.NewClient(&sdk.Implementation{Name: "drain", Version: "1"}, nil).Connect(context.Background(), &sdk.StreamableClientTransport{Endpoint: web.URL, HTTPClient: &http.Client{Transport: bearer{base: http.DefaultTransport}}, MaxRetries: -1}, nil)
		if err == nil {
			_ = session.Close()
		}
		connected <- err
	}()
	drainprobe.Abort(t, application, probe, work)
	if err := <-connected; err == nil {
		t.Fatal("a session opened although its listing was aborted")
	}
	stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = adapter.Shutdown(stop)
}

// TestDrainTimeoutCancelsAuthentication: a request's authentication runs
// under its lease, so a drain timeout cancels it too (#177).
func TestDrainTimeoutCancelsAuthentication(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	adapter, err := tmcp.New(application, tmcp.Config{Name: "drain", Version: "1.0.0", Catalog: heldCatalog{work: work}, Authenticate: func(ctx context.Context, _ string, _ *http.Request) (tool.Principal, error) {
		return tool.Principal{ID: "alice", MaxDepth: 4}, work.Run(ctx)
	}, Expose: []string{"drain/hold@1.0.0"}, Budget: budget(), Timeout: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(adapter)
	defer web.Close()
	defer work.Release() // before web.Close waits for the request
	connected := make(chan error, 1)
	go func() {
		session, err := sdk.NewClient(&sdk.Implementation{Name: "drain", Version: "1"}, nil).Connect(context.Background(), &sdk.StreamableClientTransport{Endpoint: web.URL, HTTPClient: &http.Client{Transport: bearer{base: http.DefaultTransport}}, MaxRetries: -1}, nil)
		if err == nil {
			_ = session.Close()
		}
		connected <- err
	}()
	drainprobe.Abort(t, application, probe, work)
	if err := <-connected; err == nil {
		t.Fatal("a session opened although its authentication was aborted")
	}
	stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = adapter.Shutdown(stop)
}
