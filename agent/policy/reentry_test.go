package policy

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/agent"
	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

// A separate valid write review cannot widen an actual catalog reader's scope.
func TestActualCatalogChildCannotReenterWithRootScope(t *testing.T) {
	r := setup(t, filepath.Join(t.TempDir(), "journal.db"))
	child := r.call
	child.InvocationPath, child.ApprovalID = "root/read/child", "child-review"
	proposal, err := r.p.Prepare(context.Background(), r.target, child)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.a.Record(context.Background(), child.ApprovalID, proposal, proposal.Scope, r.clock.Add(time.Hour), true); err != nil {
		t.Fatal(err)
	}
	schema := []byte(`{"type":"object","properties":{"amount":{"type":"integer"}},"required":["amount"]}`)
	reader := node.MustDefine("review/read", "1.0.0", func(ctx context.Context, in map[string]int) (map[string]int, error) {
		p, ok := tool.Scope(ctx)
		if !ok || !sameSet(p.Capabilities, []string{"payment:read"}) {
			t.Fatalf("not a narrowed catalog child: %+v", p)
		}
		if _, err := r.p.Invoke(ctx, r.target, child); err != nil {
			return nil, err
		}
		return in, nil
	}, node.Description("Synthetic reentry reader"), node.Schemas(schema, schema), node.Effects("database:read"))
	writer := node.MustDefine("review/write", "1.0.0", func(_ context.Context, in map[string]int) (map[string]int, error) {
		return in, nil
	}, node.Description("Synthetic downstream writer"), node.Schemas(schema, schema), node.Pure())
	registry := node.NewRegistry()
	for _, n := range []node.Any{reader.Any(), writer.Any()} {
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	gate, err := BindCatalog(r.p, registry)
	if err != nil {
		t.Fatal(err)
	}
	metadata := tool.Metadata{Source: "agent/policy/reentry_test.go", Example: "agent/policy/reentry_test.go", Test: "agent/policy/reentry_test.go"}
	if err := agent.RegisterNode(gate.Catalog(), reader, tool.Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"database:read"}, Capabilities: []string{"payment:read"}}, tool.Resources{}, metadata); err != nil {
		t.Fatal(err)
	}
	if err := agent.RegisterNode(gate.Catalog(), writer, tool.Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true, Capabilities: []string{"payment:write"}}, tool.Resources{}, metadata); err != nil {
		t.Fatal(err)
	}
	wf := flow.MustDefine(flow.Spec{Name: "review/root", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[map[string]int]) flow.Ref[map[string]int] {
		loaded := flow.Call(b, "read", reader, in)
		return flow.Call(b, "write", writer, loaded)
	})
	if err := agent.RegisterWorkflow(gate.Catalog(), wf, schema, schema, tool.Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}, metadata); err != nil {
		t.Fatal(err)
	}
	verifier := verifyFunc(func(_ context.Context, _ approval.Proposal, output []byte, claims []approval.Assertion) error {
		if string(output) != `{"amount":1}` || len(claims) != 0 {
			return approval.ErrEvidence
		}
		return nil
	})
	for _, binding := range []struct {
		name  string
		scope []string
	}{
		{"review/read", []string{"payment:read"}},
		{"review/write", []string{"payment:write"}},
		{"review/root", []string{"payment:read", "payment:write"}},
	} {
		if err := gate.Bind(binding.name, "1.0.0", binding.scope, verifier); err != nil {
			t.Fatal(err)
		}
	}
	principal := tool.Principal{ID: "review-actor", Capabilities: []string{"payment:read", "payment:write"}, MaxDepth: 4}
	proposal, err = gate.Prepare(context.Background(), principal, "review/root", "1.0.0", r.call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.a.Record(context.Background(), r.call.ApprovalID, proposal, proposal.Scope, r.clock.Add(time.Hour), true); err != nil {
		t.Fatal(err)
	}
	out, err := gate.Invoke(context.Background(), principal, "review/root", "1.0.0", r.call, catalogBudget())
	attempts, committed := r.trace(t)
	if out != nil || !errors.Is(err, approval.ErrDenied) || r.effects.Load() != 0 || attempts != 1 || committed != 0 {
		t.Fatalf("actual child widened scope: output=%s err=%v effects=%d attempts=%d commits=%d", out, err, r.effects.Load(), attempts, committed)
	}
}
