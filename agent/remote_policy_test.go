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
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestActualNodeEffectScopeCannotWidenOrBypassTokenBudget(t *testing.T) {
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("actual Node policy gate requires built SDK")
	}
	var effects atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		effects.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"done":true}`))
	}))
	defer provider.Close()
	main := filepath.Join(root, "runtime/nodejs/dist/runtime/nodejs/main.js")
	module := filepath.Join(root, "agent/testdata/effect-worker.mjs")
	command := exec.Command("node", main, module, "--discover")
	command.Env = append(os.Environ(), "BLOK_SYNTHETIC_EFFECT_URL="+provider.URL)
	raw, err := command.Output()
	if err != nil {
		t.Fatal(err)
	}
	var discovered struct {
		Nodes  []node.Descriptor `json:"nodes"`
		Digest string            `json:"catalogDigest"`
	}
	if err = json.Unmarshal(raw, &discovered); err != nil {
		t.Fatal(err)
	}
	if len(discovered.Nodes) != 1 || !equalSet(discovered.Nodes[0].RequiredCapabilities, []string{"http:charges"}) {
		t.Fatalf("unbound requirements: %+v", discovered.Nodes)
	}
	digest, err := contract.CatalogDigest(discovered.Nodes)
	if err != nil || digest != discovered.Digest {
		t.Fatalf("discovery mismatch: %v", err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	artifact := contract.CanonicalDigest([]byte("synthetic-effect-agent"))
	token := "synthetic-agent-effect-token-000001"
	caps := []contract.Capability{"http:charges"}
	h := contract.Hello{Protocol: contract.ProtocolName, Major: 1, ArtifactDigest: artifact, CatalogDigest: digest, Generation: 1, Limits: contract.DefaultLimits(), Capabilities: caps}
	s, err := worker.New(worker.Config{Hello: h, Factory: worker.ProcessFactory{Command: "node", Args: []string{main, module}, Address: address, Token: token, Principal: "agent-app", Capabilities: caps, StartupTimeout: 3 * time.Second, Env: []string{"BLOK_WORKER_ADDRESS=" + address, "BLOK_WORKER_TOKEN=" + token, "BLOK_WORKER_PRINCIPAL=agent-app", "BLOK_WORKER_ARTIFACT=" + artifact, "BLOK_WORKER_GENERATION=1", `BLOK_WORKER_CAPABILITIES=["http:charges"]`, "BLOK_SYNTHETIC_EFFECT_URL=" + provider.URL}}})
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	}()
	type done struct {
		Done bool `json:"done"`
	}
	remote, err := worker.Define[struct{}, done](s, discovered.Nodes[0])
	if err != nil {
		t.Fatal(err)
	}
	r := node.NewRegistry()
	if err = r.Register(remote.Any()); err != nil {
		t.Fatal(err)
	}
	m := Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"http:charges"}, Capabilities: []string{"http:charges"}}
	weak := m
	weak.Capabilities = []string{"read"}
	c := NewCatalog(r, nil)
	if err = RegisterNode(c, remote, weak, tool.Resources{}, metadata()); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("weak manifest accepted: %v", err)
	}
	if err = RegisterNode(c, remote, m, tool.Resources{TokenLimit: 1}, metadata()); !errors.Is(err, ErrNotAgentSafe) {
		t.Fatalf("unenforceable remote token limit accepted: %v", err)
	}
	if err = RegisterNode(c, remote, m, tool.Resources{}, metadata()); err != nil {
		t.Fatal(err)
	}
	actor := Principal{ID: "verified-actor", Capabilities: []string{"read"}, MaxDepth: 4}
	if out, err := c.Invoke(context.Background(), actor, remote.Descriptor().Name, "1.0.0", []byte(`{}`), budget()); !errors.Is(err, ErrDenied) || out != nil {
		t.Fatalf("weak actor dispatched: %s %v", out, err)
	}
	if _, err := remote.Invoke(tool.WithScope(context.Background(), actor), struct{}{}); !errors.Is(err, contract.ErrCapabilityDenied) {
		t.Fatalf("direct adapter widened scope: %v", err)
	}
	actor.Capabilities = []string{"http:charges"}
	if _, err := remote.Invoke(tool.WithTokenLimit(tool.WithScope(context.Background(), actor), 1), struct{}{}); !errors.Is(err, tool.ErrBudget) {
		t.Fatalf("direct adapter lost reservation: %v", err)
	}
	if effects.Load() != 0 {
		t.Fatalf("denied paths dispatched %d effects", effects.Load())
	}
	out, err := c.Invoke(context.Background(), actor, remote.Descriptor().Name, "1.0.0", []byte(`{}`), budget())
	if err != nil || string(out) != `{"done":true}` || effects.Load() != 1 {
		t.Fatalf("authorized Node output %s effects %d error %v", out, effects.Load(), err)
	}
}

func TestResourceOnlyChangeRebindsNodeAndWorkflowApprovalIdentity(t *testing.T) {
	var digests []string
	var workflowDigests []string
	for _, limit := range []int{1, 2} {
		r := node.NewRegistry()
		n := node.MustDefine("native/tokens", "1.0.0", func(ctx context.Context, in value) (value, error) { return in, nil }, node.Description("Trusted bounded adapter fixture"), node.Schemas(valueSchema, valueSchema), node.Pure())
		if err := r.Register(n.Any()); err != nil {
			t.Fatal(err)
		}
		c := NewCatalog(r, nil)
		m := Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}
		if err := RegisterNode(c, n, m, tool.Resources{TokenLimit: limit}, metadata()); err != nil {
			t.Fatal(err)
		}
		list := c.List(principal())
		if len(list) != 1 || list[0].Resources.TokenLimit != limit {
			t.Fatal("resource policy omitted")
		}
		digests = append(digests, list[0].ArtifactDigest)
		wf := flow.MustDefine(flow.Spec{Name: "workflow/resources", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[value]) flow.Ref[value] { return flow.Call(b, "tokens", n, in) })
		if err := RegisterWorkflow(c, wf, valueSchema, valueSchema, m, metadata()); err != nil {
			t.Fatal(err)
		}
		for _, item := range c.List(principal()) {
			if item.Name == "workflow/resources" {
				if item.Resources.TokenLimit != limit {
					t.Fatal("transitive resource policy omitted")
				}
				workflowDigests = append(workflowDigests, item.ArtifactDigest)
			}
		}
	}
	if digests[0] == digests[1] {
		t.Fatal("resource-only change reused approval identity")
	}
	if len(workflowDigests) != 2 || workflowDigests[0] == workflowDigests[1] {
		t.Fatal("transitive resource-only change reused approval identity")
	}
}
