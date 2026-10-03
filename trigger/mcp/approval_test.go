package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/well-prado/new-blok/agent"
	"github.com/well-prado/new-blok/agent/policy"
	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
)

type reviewer func(context.Context, approval.Proposal, []string) (string, error)

func (f reviewer) AuthorizeReview(ctx context.Context, p approval.Proposal, s []string) (string, error) {
	return f(ctx, p, s)
}

type executionScope []string

func (s executionScope) ExecutionScope(context.Context) ([]string, error) { return s, nil }

type verifier func(context.Context, approval.Proposal, []byte, []approval.Assertion) error

func (f verifier) Verify(ctx context.Context, p approval.Proposal, out []byte, claims []approval.Assertion) error {
	return f(ctx, p, out, claims)
}

type charge struct {
	Account     string `json:"account"`
	AmountCents int64  `json:"amountCents"`
}

type receipt struct {
	ID string `json:"id"`
}

// gateCatalog is how an application adapts the reviewed catalog (#144) to
// the MCP port: every call is one invocation of an admitted run, and the
// approval the caller names is only a name the gate looks up. The
// invocation identity a reviewer approves binds the calling principal, the
// tool and the exact input, so a decision cannot be used by another
// principal or for another input, and an approved call runs at most once.
type gateCatalog struct {
	gate  *policy.CatalogGate
	runID string
}

func invocation(runID string, principal tool.Principal, name, version string, input []byte, approvalID string) policy.Invocation {
	path := "mcp/" + principal.ID + "/" + name + "@" + version + "/" + approval.BytesDigest(input)
	return policy.Invocation{RunID: runID, InvocationPath: path, IterationPath: "root", ApprovalID: approvalID, Input: input}
}

func (c gateCatalog) List(_ context.Context, principal tool.Principal) ([]tmcp.Tool, error) {
	var tools []tmcp.Tool
	for _, l := range c.gate.Catalog().List(principal) {
		tools = append(tools, tmcp.Tool{Name: l.Name, Version: l.Version, Description: l.Description, InputSchema: l.InputSchema, OutputSchema: l.OutputSchema, Effects: l.Effects})
	}
	return tools, nil
}

func (c gateCatalog) Invoke(ctx context.Context, principal tool.Principal, call tmcp.Call) (json.RawMessage, error) {
	out, err := c.gate.Invoke(ctx, principal, call.Name, call.Version, invocation(c.runID, principal, call.Name, call.Version, call.Input, call.Approval), call.Budget)
	switch {
	case err == nil:
		return out, nil
	case errors.Is(err, approval.ErrStale):
		return nil, tmcp.ErrApprovalStale
	case errors.Is(err, approval.ErrEvidence):
		return nil, tmcp.ErrEvidenceRequired
	case errors.Is(err, approval.ErrConflict):
		return nil, tmcp.ErrConflict
	case errors.Is(err, approval.ErrDenied), errors.Is(err, agent.ErrDenied), errors.Is(err, agent.ErrNotAgentSafe):
		return nil, tmcp.ErrDenied
	case errors.Is(err, approval.ErrCapacity), errors.Is(err, agent.ErrCapacity):
		return nil, trigger.ErrSaturated
	}
	return nil, err
}

type approvalRig struct {
	store     *approval.JournalStore
	gate      *policy.CatalogGate
	runID     string
	principal tool.Principal
	clock     time.Time
	effects   atomic.Int64
	session   *sdk.ClientSession
	// other holds the same capabilities as principal.
	other        tool.Principal
	otherSession *sdk.ClientSession
}

// newApprovalRig builds the real #144 stack — sqlite, the journal, the
// journal-backed decision store and the policy — registers one effectful
// native node, binds it to a verifier, admits a run and serves the gate
// over MCP.
func newApprovalRig(t *testing.T) *approvalRig {
	t.Helper()
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	r := &approvalRig{principal: tool.Principal{ID: "agent-1", Capabilities: []string{"payment:write"}, MaxDepth: 4}, other: tool.Principal{ID: "agent-2", Capabilities: []string{"payment:write"}, MaxDepth: 4}, clock: time.Now()}
	j, err := journal.New(ctx, db, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	r.store, err = approval.NewJournalStore(ctx, db, approval.Config{Authorizer: reviewer(func(context.Context, approval.Proposal, []string) (string, error) {
		return "reviewer:synthetic", nil
	}), Clock: func() time.Time { return r.clock }, MaxDecisions: 16})
	if err != nil {
		t.Fatal(err)
	}
	p, err := policy.New(policy.Config{Journal: j, Approvals: r.store, Authorizer: executionScope{"payment:write"}, Clock: func() time.Time { return r.clock }, MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxAssertions: 4})
	if err != nil {
		t.Fatal(err)
	}
	input := []byte(`{"type":"object","properties":{"account":{"type":"string"},"amountCents":{"type":"integer","minimum":1}},"required":["account","amountCents"],"additionalProperties":false}`)
	output := []byte(`{"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}`)
	pay := node.MustDefine("native/charge", "1.0.0", func(context.Context, charge) (receipt, error) {
		r.effects.Add(1)
		return receipt{ID: "synthetic-receipt"}, nil
	}, node.Description("Synthetic charge"), node.Schemas(input, output), node.Effects("payment:charge"))
	registry := node.NewRegistry()
	if err := registry.Register(pay.Any()); err != nil {
		t.Fatal(err)
	}
	if r.gate, err = policy.BindCatalog(p, registry); err != nil {
		t.Fatal(err)
	}
	manifest := tool.Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"payment:charge"}, Capabilities: []string{"payment:write"}}
	if err := agent.RegisterNode(r.gate.Catalog(), pay, manifest, tool.Resources{}, tool.Metadata{Source: "trigger/mcp/approval_test.go", Example: "trigger/mcp/approval_test.go", Test: "trigger/mcp/approval_test.go"}); err != nil {
		t.Fatal(err)
	}
	if err := r.gate.Bind("native/charge", "1.0.0", []string{"payment:write"}, verifier(func(_ context.Context, _ approval.Proposal, out []byte, claims []approval.Assertion) error {
		if string(out) != `{"id":"synthetic-receipt"}` || r.effects.Load() == 0 || len(claims) != 0 {
			return approval.ErrEvidence
		}
		return nil
	})); err != nil {
		t.Fatal(err)
	}
	admission, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "mcp-request-1", Workflow: "orders@1", ArtifactDigest: approval.BytesDigest([]byte("synthetic-artifact")), Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	r.runID = admission.RunID

	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := tmcp.New(application, tmcp.Config{Name: "approval", Version: "1.0.0", Catalog: gateCatalog{gate: r.gate, runID: r.runID}, Authenticate: func(_ context.Context, token string, _ *http.Request) (tool.Principal, error) {
		switch token {
		case "agent-token":
			return r.principal, nil
		case "other-token":
			return r.other, nil
		}
		return tool.Principal{}, errors.New("unknown token")
	}, Expose: []string{"native/charge@1.0.0"}, Budget: tool.Budget{MaxDepth: 4, MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxTokens: 100, MaxCalls: 8}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: adapter, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = server.Serve(listener) }()
	r.session, err = connect(ctx, "http://"+listener.Addr().String(), "agent-token")
	if err != nil {
		t.Fatal(err)
	}
	if r.otherSession, err = connect(ctx, "http://"+listener.Addr().String(), "other-token"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = r.session.Close()
		_ = r.otherSession.Close()
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = adapter.Shutdown(stop)
		_ = application.Shutdown(stop)
		_ = server.Close()
	})
	return r
}

// decide records a reviewer's decision for exactly this call's proposal.
func (r *approvalRig) decide(t *testing.T, id string, input []byte, approved bool) {
	t.Helper()
	proposal, err := r.gate.Prepare(context.Background(), r.principal, "native/charge", "1.0.0", invocation(r.runID, r.principal, "native/charge", "1.0.0", input, id))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.store.Record(context.Background(), id, proposal, proposal.Scope, r.clock.Add(time.Hour), approved); err != nil {
		t.Fatal(err)
	}
}

func (r *approvalRig) charge(t *testing.T, session *sdk.ClientSession, arguments map[string]any, approvalID string) (*sdk.CallToolResult, error) {
	t.Helper()
	var meta sdk.Meta
	if approvalID != "" {
		meta = sdk.Meta{tmcp.ApprovalMeta: approvalID}
	}
	return call(session, tmcp.ToolName("native/charge", "1.0.0"), arguments, meta)
}

// TestEffectStaysBlockedUntilAScopedDecision drives the reviewed catalog
// over MCP: an effect runs only after a decision recorded for this exact
// call, and exactly once.
func TestEffectStaysBlockedUntilAScopedDecision(t *testing.T) {
	r := newApprovalRig(t)
	if got := names(t, r.session); len(got) != 1 || got[0] != "native.charge_v1.0.0" {
		t.Fatalf("tools %v", got)
	}
	one := map[string]any{"account": "synthetic-account", "amountCents": 1}
	two := map[string]any{"account": "synthetic-account", "amountCents": 2}
	oneBytes := []byte(`{"account":"synthetic-account","amountCents":1}`)
	twoBytes := []byte(`{"account":"synthetic-account","amountCents":2}`)
	steps := []struct {
		name      string
		before    func()
		session   *sdk.ClientSession
		arguments map[string]any
		approval  string
		code      string
		effects   int64
	}{
		{name: "no decision named", code: "denied"},
		{name: "a decision that does not exist", approval: "review-missing", code: "approval_stale"},
		{name: "a decision for a different input", before: func() { r.decide(t, "review-other", twoBytes, true) }, approval: "review-other", code: "approval_stale"},
		{name: "a rejected decision", before: func() { r.decide(t, "review-rejected", oneBytes, false) }, approval: "review-rejected", code: "approval_stale"},
		{name: "another principal naming this call's decision", before: func() { r.decide(t, "review-1", oneBytes, true) }, session: r.otherSession, approval: "review-1", code: "approval_stale"},
		{name: "the decision for this call", approval: "review-1", effects: 1},
		{name: "the same decision again", approval: "review-1", code: "internal", effects: 1},
		{name: "a decision for a second input runs its own effect", arguments: two, approval: "review-other", effects: 2},
	}
	for _, step := range steps {
		if step.before != nil {
			step.before()
		}
		session, arguments := r.session, one
		if step.session != nil {
			session = step.session
		}
		if step.arguments != nil {
			arguments = step.arguments
		}
		result, err := r.charge(t, session, arguments, step.approval)
		if err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		if step.code == "" {
			output, _ := json.Marshal(result.StructuredContent)
			if result.IsError || string(output) != `{"id":"synthetic-receipt"}` {
				t.Fatalf("%s: result=%+v output=%s", step.name, result, output)
			}
		} else if !result.IsError || toolCode(result) != step.code {
			t.Fatalf("%s: result=%+v code=%q; want %q", step.name, result, toolCode(result), step.code)
		}
		if n := r.effects.Load(); n != step.effects {
			t.Fatalf("%s: effects=%d, want %d", step.name, n, step.effects)
		}
	}
}
