package agent

import (
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

// #260 guard: moving the stored program behind catalogprogram must not
// change what catalog dispatch does. For a corpus of literal and non-literal
// workflows the golden file records, per invocation, every gate admission
// (identity, digests, effects, capabilities, budget, the exact input bytes
// the tool receives), every published output, every node input, the result
// and the error, plus every listing digest. It was generated on origin/main
// before the change (`go test ./agent -run TestDispatchCorpus -update-dispatch-corpus`).

var updateDispatchCorpus = flag.Bool("update-dispatch-corpus", false, "rewrite testdata/dispatch_corpus.golden.json")

type corpusAdmission struct {
	Name, Version, Workflow, WorkflowDigest, ArtifactDigest string
	Principal, InputDigest, Input                           string
	Effects, Capabilities                                   []string
	MaxDepth, MaxCalls, MaxTokens                           int
	MaxInputBytes, MaxOutputBytes                           int
	Resources                                               tool.Resources
}

type corpusPublish struct{ Name, Output string }

type corpusGate struct {
	mu        sync.Mutex
	admitted  []corpusAdmission
	published []corpusPublish
}

func (g *corpusGate) Authorize(_ context.Context, a tool.Admission) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.admitted = append(g.admitted, corpusAdmission{
		Name: a.Name, Version: a.Version, Workflow: a.Workflow, WorkflowDigest: a.WorkflowDigest, ArtifactDigest: a.ArtifactDigest,
		Principal: a.Principal, InputDigest: a.InputDigest, Input: string(a.Input),
		Effects: a.Effects, Capabilities: a.Capabilities,
		MaxDepth: a.Budget.MaxDepth, MaxCalls: a.Budget.MaxCalls, MaxTokens: a.Budget.MaxTokens,
		MaxInputBytes: a.Budget.MaxInputBytes, MaxOutputBytes: a.Budget.MaxOutputBytes, Resources: a.Resources,
	})
	return nil
}

func (g *corpusGate) Publish(_ context.Context, a tool.Admission, output []byte) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.published = append(g.published, corpusPublish{Name: a.Name, Output: string(output)})
	return nil
}

type corpusInvocation struct {
	Input, Output, Error string
	Admitted             []corpusAdmission
	Published            []corpusPublish
	NodeInputs           map[string][]string
}

type corpusCase struct {
	RegisterError string
	Listings      map[string][2]string
	Invocations   []corpusInvocation
}

func TestDispatchCorpus(t *testing.T) {
	normalizing := func(r *recorder) node.Definition[object, object] {
		return node.MustDefine("conf/normalize", "1.0.0", func(_ context.Context, in object) (object, error) {
			r.record("conf/normalize", in)
			return object{"committed": in["sku"], "quantity": in["quantity"]}, nil
		}, node.Description("normalizes its input"), node.Schemas([]byte(normalizingSchema), []byte(confCommitSchema)), node.Effects("db:conf"))
	}
	childSpec := flow.Spec{Name: "conf/child", Version: "1.0.0", Durability: flow.Memory}
	type define func(c *Catalog, n confNodes, normalize node.Definition[object, object]) error
	register := func(c *Catalog, wf flow.Definition[object, object], output string) error {
		return RegisterWorkflow(c, wf, []byte(confOrderSchema), []byte(output), confManifest(), metadata())
	}
	cases := []struct {
		name   string
		budget func(Budget) Budget
		define define
	}{
		{"input-chain", nil, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			return register(c, flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				reserved := flow.Call(b, "reserve", n.reserve, in)
				return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body"))
			}), confCommitSchema)
		}},
		{"field-select-string", nil, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			return register(c, flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				reserved := flow.Call(b, "reserve", n.reserve, in)
				return flow.Call(b, "label", n.label, flow.Select[object, string](reserved, "id"))
			}), confLabelSchema)
		}},
		{"literal-only", nil, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			return register(c, flow.MustDefine(confSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, "commit", n.commit, flow.Lit(object{"sku": "tea", "quantity": 1}))
			}), confCommitSchema)
		}},
		{"mixed-literal", nil, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			return register(c, flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				reserved := flow.Call(b, "reserve", n.reserve, in)
				committed := flow.Call(b, "commit", n.commit, flow.Lit(object{"sku": "tea", "quantity": 1}))
				flow.Call(b, "audit", n.audit, reserved)
				return committed
			}), confCommitSchema)
		}},
		{"literal-string", nil, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			return register(c, flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				flow.Call(b, "reserve", n.reserve, in)
				return flow.Call(b, "label", n.label, flow.Lit("fixed-sku"))
			}), confLabelSchema)
		}},
		{"normalizing-literal", nil, func(c *Catalog, n confNodes, normalize node.Definition[object, object]) error {
			return register(c, flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				reserved := flow.Call(b, "reserve", n.reserve, in)
				committed := flow.Call(b, "commit", normalize, flow.Lit(object{"sku": "tea", "big": int64(9007199254740993)}))
				flow.Call(b, "audit", n.audit, reserved)
				return committed
			}), confCommitSchema)
		}},
		{"child-with-literal", nil, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			child := flow.MustDefine(childSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				flow.Call(b, "reserve", n.reserve, in)
				return flow.Call(b, "commit", n.commit, flow.Lit(object{"sku": "child-tea", "quantity": 5}))
			})
			if err := register(c, child, confCommitSchema); err != nil {
				return err
			}
			return register(c, flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				reserved := flow.Call(b, "reserve", n.reserve, in)
				return flow.Child[object, object](b, "nested", "conf/child@1.0.0", flow.Select[object, object](reserved, "body"))
			}), confCommitSchema)
		}},
		{"invalid-literal", nil, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			return register(c, literalWorkflow(n, object{"sku": "tea", "quantity": "one"}), confCommitSchema)
		}},
		{"literal-over-invocation-budget", func(b Budget) Budget { b.MaxInputBytes = 1024; return b }, func(c *Catalog, n confNodes, _ node.Definition[object, object]) error {
			return register(c, literalWorkflow(n, object{"sku": strings.Repeat("z", 2048), "quantity": 1}), confCommitSchema)
		}},
	}
	inputs := []string{`{"sku":"coffee","quantity":2}`, `{"quantity":7,"sku":"SECRET"}`}
	got := map[string]corpusCase{}
	for _, tc := range cases {
		r := &recorder{}
		n := newConfNodes(r)
		normalize := normalizing(r)
		registry := node.NewRegistry()
		for _, definition := range append(n.all(), normalize.Any()) {
			if err := registry.Register(definition); err != nil {
				t.Fatal(err)
			}
		}
		gate := &corpusGate{}
		c := NewCatalog(registry, gate)
		for _, definition := range []error{
			RegisterNode(c, n.reserve, confManifest(), tool.Resources{}, metadata()),
			RegisterNode(c, n.commit, confManifest(), tool.Resources{}, metadata()),
			RegisterNode(c, n.audit, confManifest(), tool.Resources{}, metadata()),
			RegisterNode(c, n.label, confManifest(), tool.Resources{}, metadata()),
			RegisterNode(c, normalize, confManifest(), tool.Resources{}, metadata()),
		} {
			if definition != nil {
				t.Fatal(definition)
			}
		}
		result := corpusCase{Listings: map[string][2]string{}}
		if err := tc.define(c, n, normalize); err != nil {
			result.RegisterError = err.Error()
		}
		for _, l := range c.List(confPrincipal()) {
			result.Listings[l.Name+"@"+l.Version] = [2]string{l.ArtifactDigest, l.CapabilityDigest}
		}
		if result.RegisterError == "" {
			for _, input := range inputs {
				gate.mu.Lock()
				gate.admitted, gate.published = nil, nil
				gate.mu.Unlock()
				r.mu.Lock()
				r.inputs = nil
				r.mu.Unlock()
				budget := confBudget()
				if tc.budget != nil {
					budget = tc.budget(budget)
				}
				output, err := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, []byte(input), budget)
				invocation := corpusInvocation{Input: input, Output: string(output), Admitted: gate.admitted, Published: gate.published, NodeInputs: r.inputs}
				if err != nil {
					invocation.Error = err.Error()
				}
				result.Invocations = append(result.Invocations, invocation)
			}
		}
		got[tc.name] = result
	}
	encoded, err := json.MarshalIndent(got, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	golden := filepath.Join("testdata", "dispatch_corpus.golden.json")
	if *updateDispatchCorpus {
		if err := os.WriteFile(golden, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != string(encoded) {
		actual := filepath.Join(t.TempDir(), "dispatch_corpus.actual.json")
		_ = os.WriteFile(actual, encoded, 0o644)
		t.Fatalf("catalog dispatch differs from the origin/main golden %s; actual written to %s", golden, actual)
	}
	// The corpus must exercise what it guards: literal calls dispatched with
	// the literal, and the workflow input reaching the non-literal calls.
	if !strings.Contains(string(encoded), `"Input": "{\"quantity\":1,\"sku\":\"tea\"}"`) || !strings.Contains(string(encoded), `SECRET`) {
		t.Fatal("corpus does not exercise literal and workflow-input dispatch")
	}
}
