package agent

import (
	"context"
	"encoding/json"
	"errors"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/runtime/worker"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestActualNativeNodeToolWorkflowAdmission(t *testing.T) {
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("actual Node agent gate requires built SDK")
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
		t.Fatal("missing descriptor")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	artifact := contract.CanonicalDigest([]byte("synthetic-agent-worker"))
	token := "synthetic-agent-token-0000000000001"
	hello := contract.Hello{Protocol: contract.ProtocolName, Major: 1, ArtifactDigest: artifact, CatalogDigest: discovered.Digest, Generation: 1, Limits: contract.DefaultLimits()}
	supervisor, err := worker.New(worker.Config{Hello: hello, Factory: worker.ProcessFactory{Command: "node", Args: []string{main, module}, Address: address, Token: token, Principal: "agent-app", StartupTimeout: 3 * time.Second, Env: []string{"BLOK_WORKER_ADDRESS=" + address, "BLOK_WORKER_TOKEN=" + token, "BLOK_WORKER_PRINCIPAL=agent-app", "BLOK_WORKER_ARTIFACT=" + artifact, "BLOK_WORKER_GENERATION=1"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := supervisor.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := supervisor.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
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
	effects := 0
	native := node.MustDefine("native/load-order", "1.0.0", func(ctx context.Context, in input) (input, error) {
		scope, ok := tool.Scope(ctx)
		if !ok || scope.ID != "reviewed-actor" || !equalSet(scope.Capabilities, []string{"read"}) {
			return input{}, ErrDenied
		}
		effects++
		return in, nil
	}, node.Description("Synthetic authorized order read"), node.Schemas(descriptor.InputSchema, descriptor.InputSchema), node.Effects("db:read"))
	registry := node.NewRegistry()
	for _, n := range []node.Any{native.Any(), remote.Any()} {
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	gate := &testGate{}
	catalog := NewCatalog(registry, gate)
	if err := RegisterNode(catalog, native, manifest("read"), tool.Resources{}, metadata()); err != nil {
		t.Fatal(err)
	}
	if err := RegisterNode(catalog, remote, Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}, tool.Resources{}, metadata()); err != nil {
		t.Fatal(err)
	}
	workflow := flow.MustDefine(flow.Spec{Name: "agent/order-quote", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[input]) flow.Ref[output] {
		order := flow.Call(b, "load", native, in)
		return flow.Call(b, "quote", remote, order)
	})
	if err := RegisterWorkflow(catalog, workflow, descriptor.InputSchema, descriptor.OutputSchema, manifest("read"), metadata()); err != nil {
		t.Fatal(err)
	}
	actor := Principal{ID: "reviewed-actor", Capabilities: []string{"read"}, MaxDepth: 4}
	out, err := catalog.Invoke(context.Background(), actor, "agent/order-quote", "1.0.0", []byte(`{"sku":"coffee","quantity":2}`), budget())
	if err != nil || string(out) != `{"totalCents":"3000"}` || effects != 1 {
		t.Fatalf("actual tool chain output %s effects %d error %v", out, effects, err)
	}
	if len(gate.requests) != 3 {
		t.Fatalf("native/worker bypassed admission: %d", len(gate.requests))
	}
	for _, admission := range gate.requests {
		if admission.Principal != actor.ID || admission.Workflow != "agent/order-quote@1.0.0" || admission.InputDigest != hash(admission.Input) {
			t.Fatalf("unbound admission %+v", admission)
		}
	}
	actor.Capabilities = nil
	if out, err := catalog.Invoke(context.Background(), actor, "agent/order-quote", "1.0.0", []byte(`{"sku":"coffee","quantity":2}`), budget()); !errors.Is(err, ErrDenied) || out != nil || effects != 1 {
		t.Fatalf("missing scope dispatched: %s %v %d", out, err, effects)
	}
}
