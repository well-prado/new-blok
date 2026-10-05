package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/trigger"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
)

func budget() tool.Budget {
	return tool.Budget{MaxDepth: 4, MaxInputBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxTokens: 1000, MaxCalls: 100, Deadline: time.Now().Add(time.Hour)}
}

// harnessCatalog lists one tool and hands each call to the harness workflow.
type harnessCatalog struct {
	env conformance.TriggerEnv
}

func (c harnessCatalog) List(context.Context, tool.Principal) ([]tmcp.Tool, error) {
	output := json.RawMessage(`{"type":"object","properties":{"echo":` + string(c.env.InputSchema) + `}}`)
	return []tmcp.Tool{{Name: "conformance/order", Version: "1.0.0", Description: "Places an order", InputSchema: c.env.InputSchema, OutputSchema: output}}, nil
}

func (c harnessCatalog) Invoke(ctx context.Context, principal tool.Principal, call tmcp.Call) (json.RawMessage, error) {
	return c.env.Workflow(ctx, conformance.Call{Input: call.Input, Principal: trigger.Principal{ID: principal.ID}})
}

// bearerClient adds a bearer token to every request.
type bearerClient struct {
	token string
	base  http.RoundTripper
}

func (b bearerClient) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	if b.token != "" {
		request.Header.Set("Authorization", "Bearer "+b.token)
	}
	return b.base.RoundTrip(request)
}

// connect opens a real MCP client session as a bearer token.
func connect(ctx context.Context, endpoint, token string) (*sdk.ClientSession, error) {
	client := sdk.NewClient(&sdk.Implementation{Name: "conformance-client", Version: "1.0.0"}, nil)
	transport := &sdk.StreamableClientTransport{Endpoint: endpoint, HTTPClient: &http.Client{Transport: bearerClient{token: token, base: http.DefaultTransport}}, MaxRetries: -1}
	return client.Connect(ctx, transport, nil)
}

// toolCode reads the stable code of a tool error result.
func toolCode(result *sdk.CallToolResult) string {
	if len(result.Content) == 0 {
		return ""
	}
	text, ok := result.Content[0].(*sdk.TextContent)
	if !ok {
		return ""
	}
	var failure struct {
		Code string `json:"code"`
	}
	if json.Unmarshal([]byte(text.Text), &failure) != nil {
		return ""
	}
	return failure.Code
}

// mcpDriver serves the real adapter on a real listener and calls it with the
// official MCP client. The driver never calls the workflow itself.
type mcpDriver struct {
	env      conformance.TriggerEnv
	app      *app.Application
	adapter  *tmcp.Server
	server   *http.Server
	mu       sync.Mutex
	address  string
	serveErr chan error
}

func (*mcpDriver) Declaration() trigger.Declaration { return tmcp.Declaration }

func authenticator(authenticate func(string) (trigger.Principal, error)) tmcp.Authenticator {
	return func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		principal, err := authenticate(token)
		if err != nil {
			return tool.Principal{}, err
		}
		return tool.Principal{ID: principal.ID, MaxDepth: 4}, nil
	}
}

func (d *mcpDriver) Open(_ context.Context, env conformance.TriggerEnv) error {
	d.env = env
	application, err := app.New(app.Config{})
	if err != nil {
		return err
	}
	adapter, err := tmcp.New(application, tmcp.Config{Name: "conformance", Version: "1.0.0", Catalog: harnessCatalog{env: env}, Authenticate: authenticator(env.Authenticate), Expose: []string{"conformance/order@1.0.0"}, Budget: budget()})
	if err != nil {
		return err
	}
	d.app, d.adapter = application, adapter
	return nil
}

func (d *mcpDriver) Start(ctx context.Context) error {
	if err := d.app.Start(ctx); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.server = &http.Server{Handler: d.adapter, ReadHeaderTimeout: 5 * time.Second}
	d.address = listener.Addr().String()
	d.serveErr = make(chan error, 1)
	server := d.server
	d.mu.Unlock()
	go func() { d.serveErr <- server.Serve(listener) }()
	return nil
}

func (d *mcpDriver) Endpoint() string { d.mu.Lock(); defer d.mu.Unlock(); return d.address }

func (d *mcpDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	address := d.Endpoint()
	if address == "" {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	session, err := connect(ctx, "http://"+address, delivery.Credential)
	if err != nil {
		if strings.Contains(err.Error(), "Unauthorized") || strings.Contains(err.Error(), "401") {
			return conformance.Outcome{Kind: conformance.Rejected, Code: "unauthorized"}, nil
		}
		return conformance.Outcome{}, err
	}
	defer session.Close()
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if delivery.Disconnect != nil {
		go func() {
			select {
			case <-delivery.Disconnect:
				cancel()
			case <-callCtx.Done():
			}
		}()
	}
	arguments := json.RawMessage(delivery.Payload)
	if len(arguments) == 0 {
		arguments = nil
	}
	result, err := session.CallTool(callCtx, &sdk.CallToolParams{Name: tmcp.ToolName("conformance/order", "1.0.0"), Arguments: arguments})
	if err != nil {
		if delivery.Disconnect != nil && callCtx.Err() != nil {
			return conformance.Outcome{Kind: conformance.Disconnected}, nil
		}
		var wire *jsonrpc.Error
		if errors.As(err, &wire) && wire.Code == jsonrpc.CodeInvalidParams {
			return conformance.Outcome{Kind: conformance.Rejected, Code: wire.Message, Message: wire.Message}, nil
		}
		return conformance.Outcome{}, err
	}
	if result.IsError {
		return conformance.Outcome{Kind: conformance.Rejected, Code: toolCode(result)}, nil
	}
	output, err := json.Marshal(result.StructuredContent)
	if err != nil {
		return conformance.Outcome{}, err
	}
	return conformance.Outcome{Kind: conformance.Completed, Output: output}, nil
}

func (d *mcpDriver) Stop(ctx context.Context) error {
	d.mu.Lock()
	server := d.server
	d.address = ""
	d.mu.Unlock()
	shutdownErr := errors.Join(d.adapter.Shutdown(ctx), d.app.Shutdown(ctx))
	closeErr := server.Close()
	if err := <-d.serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errors.Join(shutdownErr, closeErr, err)
	}
	return errors.Join(shutdownErr, closeErr)
}

func TestMCPAdapterPassesTriggerConformance(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), &mcpDriver{}, corpus, conformance.TriggerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, result := range report.Results {
		if result.Applicable {
			ran++
		} else {
			t.Logf("not applicable: %s (%s)", result.CaseID, result.Reason)
		}
	}
	if ran != 12 {
		t.Fatalf("ran=%d report=%+v", ran, report)
	}
}
