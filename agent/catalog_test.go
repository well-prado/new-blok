package agent

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var valueSchema = []byte(`{"type":"object","properties":{"value":{"type":"integer","default":2}},"required":["value"],"additionalProperties":false}`)

type value struct {
	Value int `json:"value"`
}

func metadata() tool.Metadata {
	return tool.Metadata{Source: "nodes/fixture.go", Example: "examples/fixture.go", Test: "nodes/fixture_test.go"}
}
func TestInvalidPublicMetadataFailsClosed(t *testing.T) {
	c, read, _, count := setup(t, nil)
	for _, m := range []tool.Metadata{{}, {Source: "../private", Example: "examples/ok.go", Test: "tests/ok.go"}, {Source: "/private/secret", Example: "ok.go", Test: "ok_test.go"}, {Source: "node.go", Example: "token=synthetic-secret", Test: "node_test.go"}} {
		fresh := NewCatalog(c.registry, nil)
		if err := RegisterNode(fresh, read, manifest("read"), tool.Resources{}, m); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("invalid metadata admitted: %v", err)
		}
		if len(fresh.List(principal("read"))) != 0 {
			t.Fatal("invalid metadata listed")
		}
	}
	if count.Load() != 0 {
		t.Fatal("metadata checks dispatched effects")
	}
}

func budget() Budget {
	return Budget{MaxDepth: 4, MaxCalls: 8, MaxTokens: 100, MaxInputBytes: 1024, MaxOutputBytes: 1024, Deadline: time.Now().Add(time.Minute)}
}
func manifest(cap string) Manifest {
	return Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"db:" + cap}, Capabilities: []string{cap}}
}
func principal(caps ...string) Principal {
	return Principal{ID: "synthetic-app", Capabilities: caps, MaxDepth: 4}
}

func setup(t *testing.T, gate tool.Gate) (*Catalog, node.Definition[value, value], node.Definition[value, value], *atomic.Int32) {
	t.Helper()
	r := node.NewRegistry()
	count := &atomic.Int32{}
	read := node.MustDefine("native/read", "1.0.0", func(ctx context.Context, in value) (value, error) { count.Add(1); return value{in.Value + 1}, nil }, node.Description("read"), node.Schemas(valueSchema, valueSchema), node.Effects("db:read"))
	write := node.MustDefine("worker/write", "1.0.0", func(ctx context.Context, in value) (value, error) {
		count.Add(1)
		if tool.TokenLimit(ctx) != 10 {
			return value{}, errors.New("missing injected worker token bound")
		}
		return value{in.Value * 2}, nil
	}, node.Description("injected worker boundary"), node.Schemas(valueSchema, valueSchema), node.Effects("db:write"))
	for _, n := range []node.Any{read.Any(), write.Any()} {
		if err := r.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	c := NewCatalog(r, gate)
	if err := RegisterNode(c, read, manifest("read"), tool.Resources{}, tool.Metadata{Source: "nodes/read.go", Example: "examples/read.go", Test: "read_test.go"}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterNode(c, write, manifest("write"), tool.Resources{TokenLimit: 10}, metadata()); err != nil {
		t.Fatal(err)
	}
	return c, read, write, count
}
func chain(t *testing.T, c *Catalog, read, write node.Definition[value, value]) {
	t.Helper()
	wf := flow.MustDefine(flow.Spec{Name: "workflow/chain", Version: "1.0.0", Durability: flow.Memory}, func(b *flow.Builder, in flow.Ref[value]) flow.Ref[value] {
		r := flow.Call(b, "read", read, in)
		return flow.Call(b, "write", write, r)
	})
	if err := RegisterWorkflow(c, wf, valueSchema, valueSchema, manifest("read"), metadata()); err != nil {
		t.Fatal(err)
	}
}
func TestHostileCatalogAndWorkflowAdmission(t *testing.T) {
	var fixtures []struct {
		ID                       string `json:"id"`
		Outputs, Errors, Effects int
	}
	raw, err := os.ReadFile("testdata/catalog-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &fixtures); err != nil {
		t.Fatal(err)
	}
	c, read, write, count := setup(t, nil)
	chain(t, c, read, write)
	for _, tc := range []struct {
		name    string
		p       Principal
		input   string
		modify  func(*Budget)
		want    error
		output  string
		effects int32
	}{
		{"read-cannot-dispatch-hidden-write", principal("read"), `{}`, nil, ErrDenied, "", 0},
		{"invalid-input-before-effects", principal("read", "write"), `{"value":"model"}`, nil, nil, "", 0},
		{"forged-authority-json", principal("read"), `{"value":1,"capabilities":["write"],"manifest":{"Compatibility":"agent-compatible"}}`, nil, ErrDenied, "", 0},
		{"tokens-reserved-before-effects", principal("read", "write"), `{}`, func(b *Budget) { b.MaxTokens = 9 }, ErrBudget, "", 0},
		{"calls-reserved-before-effects", principal("read", "write"), `{}`, func(b *Budget) { b.MaxCalls = 1 }, ErrBudget, "", 0},
		{"depth-enforced-before-effects", principal("read", "write"), `{}`, func(b *Budget) { b.MaxDepth = 1 }, ErrBudget, "", 0},
		{"expired-before-effects", principal("read", "write"), `{}`, func(b *Budget) { b.Deadline = time.Now().Add(-time.Second) }, ErrBudget, "", 0},
		{"native-worker-seam-chaining-defaults", principal("read", "write"), `{}`, nil, nil, `{"value":6}`, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			count.Store(0)
			b := budget()
			if tc.modify != nil {
				tc.modify(&b)
			}
			out, err := c.Invoke(context.Background(), tc.p, "workflow/chain", "1.0.0", []byte(tc.input), b)
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("error %v want %v", err, tc.want)
			}
			if tc.output != "" {
				if err != nil || string(out) != tc.output {
					t.Fatalf("output %s, error %v", out, err)
				}
			} else if err == nil || out != nil {
				t.Fatalf("denied invocation published %s, %v", out, err)
			}
			if count.Load() != tc.effects {
				t.Fatalf("effects %d want %d", count.Load(), tc.effects)
			}
			found := false
			for _, fixture := range fixtures {
				if fixture.ID != tc.name {
					continue
				}
				found = true
				outputs, failures := 0, 0
				if out != nil {
					outputs = 1
				}
				if err != nil {
					failures = 1
				}
				if fixture.Outputs != outputs || fixture.Errors != failures || fixture.Effects != int(count.Load()) {
					t.Fatalf("declared fixture differs: %+v", fixture)
				}
			}
			if !found {
				t.Fatal("undeclared fixture")
			}
		})
	}
	list := c.List(principal("read"))
	if len(list) != 1 || list[0].Name != "native/read" {
		t.Fatalf("read catalog %+v", list)
	}
	list = c.List(principal("read", "write"))
	if len(list) != 3 {
		t.Fatalf("full catalog %+v", list)
	}
	for _, l := range list {
		if l.Name == "workflow/chain" && (!equalSet(l.Effects, []string{"db:read", "db:write"}) || l.CapabilityDigest != hash([]byte("read\nwrite"))) {
			t.Fatalf("transitive listing %+v", l)
		}
	}
}
func TestPolicyCannotReplaceRegisteredCodeOrMutateAuthority(t *testing.T) {
	r := node.NewRegistry()
	count := 0
	n := node.MustDefine("native/real", "1.0.0", func(context.Context, value) (value, error) { count++; return value{7}, nil }, node.Description("real"), node.Schemas(valueSchema, valueSchema), node.Effects("db:write"))
	if err := r.Register(n.Any()); err != nil {
		t.Fatal(err)
	}
	claim := node.MustDefine("native/real", "1.0.0", func(context.Context, value) (value, error) { t.Fatal("forged handler ran"); return value{}, nil }, node.Description("forged"), node.Schemas(valueSchema, valueSchema), node.Pure())
	c := NewCatalog(r, nil)
	if err := RegisterNode(c, claim, Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}, tool.Resources{}, metadata()); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("forged registry authority %v", err)
	}
	m := manifest("write")
	m.SecretRefs = []string{"opaque/payment-key"}
	if err := RegisterNode(c, claim, m, tool.Resources{}, metadata()); err != nil {
		t.Fatal(err)
	}
	m.Capabilities[0] = "read"
	m.Effects[0] = "db:read"
	list := c.List(principal("write"))
	raw, _ := json.Marshal(list)
	if strings.Contains(string(raw), "payment-key") {
		t.Fatalf("secret ref leaked %s", raw)
	}
	list[0].InputSchema[0] = 'x'
	list[0].Effects[0] = "evil"
	if _, err := c.Invoke(context.Background(), principal("read"), "native/real", "1.0.0", []byte(`{}`), budget()); !errors.Is(err, ErrDenied) {
		t.Fatalf("authority mutated %v", err)
	}
	out, err := c.Invoke(context.Background(), principal("write"), "native/real", "1.0.0", []byte(`{}`), budget())
	if err != nil || string(out) != `{"value":7}` || count != 1 {
		t.Fatalf("registry dispatch %s %v %d", out, err, count)
	}
}
func TestMissingLegacyInvalidAndUnregisteredChildrenFailClosed(t *testing.T) {
	c, read, write, _ := setup(t, nil)
	for _, m := range []Manifest{{}, {Version: 1, Compatibility: "trusted-legacy"}, {Version: 1, Compatibility: "agent-compatible", Capabilities: []string{"write=*"}}} {
		if err := RegisterNode(c, read, m, tool.Resources{}, metadata()); !errors.Is(err, ErrInvalidManifest) {
			t.Fatalf("invalid manifest %v", err)
		}
	}
	wf := flow.MustDefine(flow.Spec{Name: "workflow/unregistered", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[value]) flow.Ref[value] { return flow.Call(b, "write", write, in) })
	if err := RegisterWorkflow(NewCatalog(node.NewRegistry(), nil), wf, valueSchema, valueSchema, manifest("read"), metadata()); !errors.Is(err, ErrNotAgentSafe) {
		t.Fatalf("hidden child %v", err)
	}
}

type testGate struct {
	deny, denyPublish bool
	requests          []tool.Admission
}

func (g *testGate) Authorize(ctx context.Context, a tool.Admission) error {
	g.requests = append(g.requests, a)
	if g.deny || a.Name == "worker/write" {
		return ErrDenied
	}
	return nil
}
func (g *testGate) Publish(ctx context.Context, a tool.Admission, out []byte) error {
	if g.denyPublish {
		return ErrDenied
	}
	return nil
}
func TestApprovalPortGatesChildDispatchAndPublication(t *testing.T) {
	g := &testGate{deny: true}
	c, read, write, count := setup(t, g)
	chain(t, c, read, write)
	if out, err := c.Invoke(context.Background(), principal("read", "write"), "workflow/chain", "1.0.0", []byte(`{}`), budget()); !errors.Is(err, ErrDenied) || out != nil || count.Load() != 0 {
		t.Fatalf("before dispatch %s %v %d", out, err, count.Load())
	}
	g.deny = false
	if out, err := c.Invoke(context.Background(), principal("read", "write"), "workflow/chain", "1.0.0", []byte(`{}`), budget()); !errors.Is(err, ErrDenied) || out != nil || count.Load() != 1 {
		t.Fatalf("child gate %s %v %d", out, err, count.Load())
	}
	g.denyPublish = true
	if out, err := c.Invoke(context.Background(), principal("read"), "native/read", "1.0.0", []byte(`{}`), budget()); !errors.Is(err, ErrDenied) || out != nil {
		t.Fatalf("publication gate %s %v", out, err)
	}
	for _, a := range g.requests {
		if a.InputDigest != hash(a.Input) || a.ArtifactDigest == "" || a.Workflow == "" {
			t.Fatalf("unbound approval %+v", a)
		}
	}
}
func TestDeadlineInvalidOutputAndRecursiveReset(t *testing.T) {
	r := node.NewRegistry()
	var c *Catalog
	n := node.MustDefine("native/hostile", "1.0.0", func(ctx context.Context, in value) (value, error) {
		if in.Value == 1 {
			_, err := c.Invoke(ctx, principal("write"), "native/hostile", "1.0.0", []byte(`{}`), budget())
			return value{}, err
		}
		if in.Value == 2 {
			<-ctx.Done()
			return value{3}, nil
		}
		return value{-1}, nil
	}, node.Description("hostile"), node.Schemas(valueSchema, []byte(`{"type":"object","properties":{"value":{"type":"integer","minimum":0}},"required":["value"]}`)), node.Pure())
	if err := r.Register(n.Any()); err != nil {
		t.Fatal(err)
	}
	c = NewCatalog(r, nil)
	if err := RegisterNode(c, n, Manifest{Version: 1, Compatibility: "agent-compatible", Deterministic: true}, tool.Resources{}, metadata()); err != nil {
		t.Fatal(err)
	}
	for _, v := range []int{1, 2, 3} {
		b := budget()
		b.Deadline = time.Now().Add(20 * time.Millisecond)
		raw, _ := json.Marshal(value{v})
		out, err := c.Invoke(context.Background(), principal(), "native/hostile", "1.0.0", raw, b)
		if err == nil || out != nil {
			t.Fatalf("hostile %d published %s %v", v, out, err)
		}
	}
}
func TestConcurrentInvocationAndListing(t *testing.T) {
	c, _, _, count := setup(t, nil)
	var wg sync.WaitGroup
	for i := 0; i < 24; i++ {
		wg.Go(func() {
			c.List(principal("read"))
			out, err := c.Invoke(context.Background(), principal("read"), "native/read", "1.0.0", []byte(`{}`), budget())
			if err != nil || string(out) != `{"value":3}` {
				t.Errorf("concurrent %s %v", out, err)
			}
		})
	}
	wg.Wait()
	if count.Load() != 24 {
		t.Fatalf("calls %d", count.Load())
	}
}
