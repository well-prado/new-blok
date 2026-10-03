package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/agent"
	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
)

type counter struct {
	Value int64 `json:"value"`
}

var counterSchema = []byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`)

// agentCatalog adapts the plain agent catalog (no review gate) to the MCP
// port, as an application exposing read-only or pre-authorized tools would.
type agentCatalog struct{ catalog *agent.Catalog }

func (c agentCatalog) List(_ context.Context, principal tool.Principal) ([]tmcp.Tool, error) {
	var tools []tmcp.Tool
	for _, l := range c.catalog.List(principal) {
		tools = append(tools, tmcp.Tool{Name: l.Name, Version: l.Version, Description: l.Description, InputSchema: l.InputSchema, OutputSchema: l.OutputSchema, Effects: l.Effects})
	}
	return tools, nil
}

func (c agentCatalog) Invoke(ctx context.Context, principal tool.Principal, call tmcp.Call) (json.RawMessage, error) {
	out, err := c.catalog.Invoke(ctx, principal, call.Name, call.Version, call.Input, call.Budget)
	if errors.Is(err, agent.ErrDenied) || errors.Is(err, agent.ErrNotAgentSafe) {
		return nil, approval.ErrDenied
	}
	return out, err
}

// TestWorkflowIsExposedAsAComposedTool registers two nodes and a workflow
// that composes them, exposes only the workflow, and calls it over MCP.
func TestWorkflowIsExposedAsAComposedTool(t *testing.T) {
	var runs atomic.Int64
	increment := node.MustDefine("native/increment", "1.0.0", func(_ context.Context, in counter) (counter, error) {
		runs.Add(1)
		return counter{Value: in.Value + 1}, nil
	}, node.Description("Adds one"), node.Schemas(counterSchema, counterSchema), node.Effects("db:read"))
	double := node.MustDefine("native/double", "1.0.0", func(_ context.Context, in counter) (counter, error) {
		runs.Add(1)
		return counter{Value: in.Value * 2}, nil
	}, node.Description("Doubles"), node.Schemas(counterSchema, counterSchema), node.Effects("db:read"))
	registry := node.NewRegistry()
	for _, n := range []node.Any{increment.Any(), double.Any()} {
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	catalog := agent.NewCatalog(registry, nil)
	manifest := tool.Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"db:read"}, Capabilities: []string{"read"}}
	metadata := tool.Metadata{Source: "trigger/mcp/workflow_test.go", Example: "trigger/mcp/workflow_test.go", Test: "trigger/mcp/workflow_test.go"}
	for _, n := range []node.Definition[counter, counter]{increment, double} {
		if err := agent.RegisterNode(catalog, n, manifest, tool.Resources{}, metadata); err != nil {
			t.Fatal(err)
		}
	}
	wf := flow.MustDefine(flow.Spec{Name: "workflow/increment-double", Version: "1.0.0", Durability: flow.Memory}, func(b *flow.Builder, in flow.Ref[counter]) flow.Ref[counter] {
		return flow.Call(b, "double", double, flow.Call(b, "increment", increment, in))
	})
	if err := agent.RegisterWorkflow(catalog, wf, counterSchema, counterSchema, manifest, metadata); err != nil {
		t.Fatal(err)
	}

	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	principals := map[string]tool.Principal{
		"reader":   {ID: "reader", Capabilities: []string{"read"}, MaxDepth: 4},
		"stranger": {ID: "stranger", Capabilities: []string{"other"}, MaxDepth: 4},
	}
	adapter, err := tmcp.New(application, tmcp.Config{Name: "workflows", Version: "1.0.0", Catalog: agentCatalog{catalog: catalog}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		principal, ok := principals[token]
		if !ok {
			return tool.Principal{}, errors.New("unknown token")
		}
		return principal, nil
	}, Expose: []string{"workflow/increment-double@1.0.0"}, Budget: tool.Budget{MaxDepth: 4, MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxTokens: 100, MaxCalls: 8}})
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
	server := &http.Server{Handler: adapter, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adapter.Shutdown(stop)
		_ = application.Shutdown(stop)
		_ = server.Close()
	})
	endpoint := "http://" + listener.Addr().String()

	reader, err := connect(context.Background(), endpoint, "reader")
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	// The nodes are in the catalog but not exposed: only the workflow shows.
	if got := names(t, reader); len(got) != 1 || got[0] != "workflow.increment-double_v1.0.0" {
		t.Fatalf("reader sees %v", got)
	}
	result, err := call(reader, "workflow.increment-double_v1.0.0", map[string]any{"value": 3}, nil)
	if err != nil || result.IsError {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if output, _ := json.Marshal(result.StructuredContent); string(output) != `{"value":8}` || runs.Load() != 2 {
		t.Fatalf("output=%s runs=%d; want (3+1)*2 from both nodes", output, runs.Load())
	}
	if _, err := call(reader, "native.increment_v1.0.0", map[string]any{"value": 3}, nil); err == nil {
		t.Fatal("an unexposed node was callable")
	}
	stranger, err := connect(context.Background(), endpoint, "stranger")
	if err != nil {
		t.Fatal(err)
	}
	defer stranger.Close()
	if got := names(t, stranger); len(got) != 0 {
		t.Fatalf("a principal without the capability sees %v", got)
	}
	if runs.Load() != 2 {
		t.Fatalf("runs=%d after refused calls", runs.Load())
	}
}
