package policy

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/agent"
	"github.com/well-prado/new-blok/contract/approval"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/runtime/worker"
)

func TestActualNativeNodeCatalogDurablePolicy(t *testing.T) {
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("actual Node policy gate requires built SDK and Node 22/24")
	}
	main := filepath.Join(root, "runtime/nodejs/dist/runtime/nodejs/main.js")
	module := filepath.Join(root, "runtime/nodejs/dist/testdata/worker/nodejs/nodes.js")
	raw, err := exec.Command("node", main, module, "--discover").Output()
	if err != nil {
		t.Fatal(err)
	}
	var discovered struct {
		Nodes  []node.Descriptor `json:"nodes"`
		Digest string            `json:"catalogDigest"`
	}
	if err := json.Unmarshal(raw, &discovered); err != nil {
		t.Fatal(err)
	}
	var descriptor node.Descriptor
	for _, d := range discovered.Nodes {
		if d.Name == "fixture/quote" {
			descriptor = d
		}
	}
	if descriptor.Name == "" {
		t.Fatal("missing real Node descriptor")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	artifact := runtimecontract.CanonicalDigest([]byte("synthetic-policy-worker"))
	token := "synthetic-policy-token-000000000001"
	hello := runtimecontract.Hello{Protocol: runtimecontract.ProtocolName, Major: 1, ArtifactDigest: artifact, CatalogDigest: discovered.Digest, Generation: 1, Limits: runtimecontract.DefaultLimits()}
	supervisor, err := worker.New(worker.Config{Hello: hello, Factory: worker.ProcessFactory{Command: "node", Args: []string{main, module}, Address: address, Token: token, Principal: "policy-app", StartupTimeout: 3 * time.Second, Env: []string{"BLOK_WORKER_ADDRESS=" + address, "BLOK_WORKER_TOKEN=" + token, "BLOK_WORKER_PRINCIPAL=policy-app", "BLOK_WORKER_ARTIFACT=" + artifact, "BLOK_WORKER_GENERATION=1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := supervisor.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	type input struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	}
	type output struct {
		Total int64 `json:"totalCents"`
	}
	remote, err := worker.Define[input, output](supervisor, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	r := setup(t, filepath.Join(t.TempDir(), "policy.db"))
	native := node.MustDefine("native/policy-load-order", "1.0.0", func(ctx context.Context, in input) (input, error) {
		scope, ok := tool.Scope(ctx)
		if !ok || scope.ID != "policy-actor" || !sameSet(scope.Capabilities, []string{"payment:read"}) {
			return input{}, approval.ErrDenied
		}
		r.effects.Add(1)
		return in, nil
	}, node.Description("Synthetic authorized order read"), node.Schemas(descriptor.InputSchema, descriptor.InputSchema), node.Effects("database:read"))
	registry := node.NewRegistry()
	for _, n := range []node.Any{native.Any(), remote.Any()} {
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	gate, err := BindCatalog(r.p, registry)
	if err != nil {
		t.Fatal(err)
	}
	metadata := tool.Metadata{Source: "testdata/worker/nodejs/nodes.ts", Example: "examples/order/order.go", Test: "agent/policy/nodejs_integration_test.go"}
	if err := agent.RegisterNode(gate.Catalog(), native, tool.Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"database:read"}, Capabilities: []string{"payment:read"}}, tool.Resources{}, metadata); err != nil {
		t.Fatal(err)
	}
	if err := agent.RegisterNode(gate.Catalog(), remote, tool.Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}, tool.Resources{}, metadata); err != nil {
		t.Fatal(err)
	}
	wf := flow.MustDefine(flow.Spec{Name: "workflow/policy-quote", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[input]) flow.Ref[output] {
		loaded := flow.Call(b, "load", native, in)
		return flow.Call(b, "quote", remote, loaded)
	})
	if err := agent.RegisterWorkflow(gate.Catalog(), wf, descriptor.InputSchema, descriptor.OutputSchema, tool.Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}, metadata); err != nil {
		t.Fatal(err)
	}
	verified := 0
	readVerifier := verifyFunc(func(_ context.Context, p approval.Proposal, b []byte, claims []approval.Assertion) error {
		if p.ToolDigest == "" || approval.BytesDigest(b) != p.InputDigest || len(claims) != 0 {
			return approval.ErrEvidence
		}
		verified++
		return nil
	})
	quoteVerifier := verifyFunc(func(_ context.Context, p approval.Proposal, b []byte, claims []approval.Assertion) error {
		if p.ToolDigest == "" || string(b) != `{"totalCents":"3000"}` || len(claims) != 0 {
			return approval.ErrEvidence
		}
		verified++
		return nil
	})
	for _, binding := range []struct {
		name     string
		scope    []string
		verifier Verifier
	}{{"native/policy-load-order", []string{"payment:read"}, readVerifier}, {"fixture/quote", nil, quoteVerifier}, {"workflow/policy-quote", []string{"payment:read"}, quoteVerifier}} {
		if err := gate.Bind(binding.name, "1.0.0", binding.scope, binding.verifier); err != nil {
			t.Fatal(err)
		}
	}
	principal := tool.Principal{ID: "policy-actor", Capabilities: []string{"payment:read", "payment:write"}, MaxDepth: 4}
	r.call.Input = []byte(`{"sku":"coffee","quantity":2}`)
	if out, err := gate.Invoke(context.Background(), principal, "workflow/policy-quote", "1.0.0", r.call, catalogBudget()); out != nil || !errors.Is(err, approval.ErrStale) || r.effects.Load() != 0 {
		t.Fatalf("missing approval dispatched native/Node: %s %v", out, err)
	}
	p, err := gate.Prepare(context.Background(), principal, "workflow/policy-quote", "1.0.0", r.call)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.a.Record(context.Background(), r.call.ApprovalID, p, p.Scope, r.clock.Add(time.Hour), true); err != nil {
		t.Fatal(err)
	}
	out, err := gate.Invoke(context.Background(), principal, "workflow/policy-quote", "1.0.0", r.call, catalogBudget())
	if err != nil || string(out) != `{"totalCents":"3000"}` || r.effects.Load() != 1 || verified != 3 {
		t.Fatalf("native/Node/catalog/policy output=%s effects=%d verifiers=%d err=%v", out, r.effects.Load(), verified, err)
	}
	if attempts, committed := r.trace(t); attempts != 1 || committed != 1 {
		t.Fatalf("missing durable gate attempt=%d committed=%d", attempts, committed)
	}
	r.call.Input = []byte(`{"sku":"coffee","quantity":3}`)
	if out, err := gate.Invoke(context.Background(), principal, "workflow/policy-quote", "1.0.0", r.call, catalogBudget()); out != nil || !errors.Is(err, approval.ErrStale) || r.effects.Load() != 1 {
		t.Fatalf("changed proposal reused review: %s %v", out, err)
	}
	t.Log("real native → persistent Node gRPC → deterministic child/root evidence → SQLite trusted commit; missing/changed review denied without effects")
}
