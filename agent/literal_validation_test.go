package agent

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

// #261: RegisterWorkflow checks every literal call input against the input
// schema of the tool it is passed to, with the same Normalize dispatch runs,
// so a literal that can never be admitted fails at registration instead of
// at invocation after earlier steps have run their effects.

// literalWorkflow reserves the workflow input, then commits the literal, so
// on a catalog that defers the check the reserve effect runs before the
// literal is rejected.
func literalWorkflow(n confNodes, literal object) flow.Definition[object, object] {
	return flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		flow.Call(b, "reserve", n.reserve, in)
		return flow.Call(b, "commit", n.commit, flow.Lit(literal))
	})
}

func registered(c *Catalog, name, version string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.tools[name+"@"+version]
	return ok
}

func TestRegisterWorkflowRejectsLiteralViolatingToolSchema(t *testing.T) {
	for _, tc := range []struct {
		name    string
		literal object
		reason  string
	}{
		{"missing-required", object{"sku": "tea"}, "missing_required at $.quantity"},
		{"wrong-type", object{"sku": "tea", "quantity": "one"}, "type_mismatch at $.quantity"},
		{"unknown-field", object{"sku": "tea", "quantity": 1, "price": 3}, "unknown_field at $.price"},
		{"null-field", object{"sku": nil, "quantity": 1}, "null_not_allowed at $.sku"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recorder{}
			n := newConfNodes(r)
			c := confCatalog(t, n)
			err := RegisterWorkflow(c, literalWorkflow(n, tc.literal), []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata())
			if err == nil {
				// Show what the deferred check costs: reserve's effect runs,
				// then the literal fails.
				_, invokeErr := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, []byte(`{"sku":"coffee","quantity":2}`), confBudget())
				t.Fatalf("invalid literal registered; invocation then ran %v and failed with %v", r.inputs, invokeErr)
			}
			if !errors.Is(err, ErrNotAgentSafe) {
				t.Fatalf("error %v; want ErrNotAgentSafe", err)
			}
			for _, part := range []string{`call "commit"`, "conf/commit@1.0.0", tc.reason} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("error %q does not name %q", err, part)
				}
			}
			if registered(c, confSpec.Name, confSpec.Version) {
				t.Error("rejected workflow is registered")
			}
			if len(r.inputs) != 0 {
				t.Errorf("registration dispatched %v", r.inputs)
			}
		})
	}
}

// flow.Child records no literal, so a child call cannot carry one: the
// lowering rejects "$literal" without a recorded value. A child workflow's
// own literal is checked when the child registers, so a parent can never
// reach a child holding an invalid literal.
func TestNestedChildLiteralIsCheckedWhereTheChildRegisters(t *testing.T) {
	r := &recorder{}
	n := newConfNodes(r)
	c := confCatalog(t, n)
	childSpec := flow.Spec{Name: "conf/child", Version: "1.0.0", Durability: flow.Memory}
	child := flow.MustDefine(childSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		flow.Call(b, "reserve", n.reserve, in)
		return flow.Call(b, "commit", n.commit, flow.Lit(object{"sku": "tea", "quantity": "one"}))
	})
	err := RegisterWorkflow(c, child, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata())
	if !errors.Is(err, ErrNotAgentSafe) || !strings.Contains(err.Error(), `call "commit"`) {
		t.Fatalf("child with an invalid literal: %v", err)
	}
	parent := flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Child[object, object](b, "nested", "conf/child@1.0.0", flow.Select[object, object](reserved, "body"))
	})
	if err := RegisterWorkflow(c, parent, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); !errors.Is(err, ErrNotAgentSafe) {
		t.Fatalf("parent of a refused child: %v", err)
	}
	if registered(c, childSpec.Name, childSpec.Version) || registered(c, confSpec.Name, confSpec.Version) {
		t.Fatal("a workflow reaching the invalid literal is registered")
	}

	// A child call cannot take a literal at all.
	valid := flow.MustDefine(childSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		return flow.Call(b, "commit", n.commit, in)
	})
	if err := RegisterWorkflow(c, valid, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); err != nil {
		t.Fatalf("valid child: %v", err)
	}
	literalChild := flow.MustDefine(confSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
		return flow.Child[object, object](b, "nested", "conf/child@1.0.0", flow.Lit(object{"sku": "tea", "quantity": 1}))
	})
	err = RegisterWorkflow(c, literalChild, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata())
	if !errors.Is(err, ErrNotAgentSafe) || !strings.Contains(err.Error(), `child "nested": literal input has no recorded value`) {
		t.Fatalf("child call with a literal: %v", err)
	}
	if len(r.inputs) != 0 {
		t.Errorf("registration dispatched %v", r.inputs)
	}
}

// Registration validates a literal; it does not rewrite it. The catalog
// stores the literal exactly as flow recorded it, and dispatch normalizes it
// as it always has, so the node receives the bytes it received before #261:
// defaults applied and int64-string integers in wire form at admission.
const normalizingSchema = `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","default":3},"big":{"type":"integer","wire":"int64-string"}},"required":["sku","quantity"],"additionalProperties":false}`

type admissionRecorder struct {
	mu     sync.Mutex
	inputs map[string][]string
}

func (g *admissionRecorder) Authorize(_ context.Context, a tool.Admission) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inputs == nil {
		g.inputs = map[string][]string{}
	}
	g.inputs[a.Name] = append(g.inputs[a.Name], string(a.Input))
	return nil
}
func (g *admissionRecorder) Publish(context.Context, tool.Admission, []byte) error { return nil }

func TestValidLiteralRegistersAndDispatchesTheRecordedBytes(t *testing.T) {
	r := &recorder{}
	n := newConfNodes(r)
	normalize := node.MustDefine("conf/normalize", "1.0.0", func(_ context.Context, in object) (object, error) {
		r.record("conf/normalize", in)
		return object{"committed": in["sku"], "quantity": in["quantity"]}, nil
	}, node.Description("normalizes its input"), node.Schemas([]byte(normalizingSchema), []byte(confCommitSchema)), node.Effects("db:conf"))
	registry := node.NewRegistry()
	for _, definition := range []node.Any{n.reserve.Any(), n.audit.Any(), normalize.Any()} {
		if err := registry.Register(definition); err != nil {
			t.Fatal(err)
		}
	}
	gate := &admissionRecorder{}
	c := NewCatalog(registry, gate)
	for _, register := range []error{
		RegisterNode(c, n.reserve, confManifest(), tool.Resources{}, metadata()),
		RegisterNode(c, n.audit, confManifest(), tool.Resources{}, metadata()),
		RegisterNode(c, normalize, confManifest(), tool.Resources{}, metadata()),
	} {
		if register != nil {
			t.Fatal(register)
		}
	}
	literal := object{"sku": "tea", "big": int64(9007199254740993)}
	wf := flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		committed := flow.Call(b, "commit", normalize, flow.Lit(literal))
		// A step with no literal: its input exists only at invocation.
		flow.Call(b, "audit", n.audit, reserved)
		return committed
	})
	if err := RegisterWorkflow(c, wf, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); err != nil {
		t.Fatalf("valid literal: %v", err)
	}
	recorded := wf.Program().Instructions[1].Literal
	if string(recorded) != `{"big":9007199254740993,"sku":"tea"}` {
		t.Fatalf("flow recorded %s", recorded)
	}
	_, literals := catalogProgram(t, c, confSpec.Name, confSpec.Version)
	if len(literals) != 1 || string(literals["commit"]) != string(recorded) {
		t.Fatalf("stored literals %q; want commit=%s unchanged", literals, recorded)
	}
	output, err := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, []byte(`{"sku":"coffee","quantity":2}`), confBudget())
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if got := canonicalJSON(t, output); got != `{"committed":"tea","quantity":3}` {
		t.Errorf("output %s", got)
	}
	// The exact bytes dispatch admitted on origin/main for this literal.
	if got := gate.inputs["conf/normalize"]; !reflect.DeepEqual(got, []string{`{"big":"9007199254740993","quantity":3,"sku":"tea"}`}) {
		t.Errorf("admitted literal %q", got)
	}
	want := map[string][]string{
		"conf/reserve":   {`{"quantity":2,"sku":"coffee"}`},
		"conf/normalize": {`{"big":9007199254740993,"quantity":3,"sku":"tea"}`},
		"conf/audit":     {`{"body":{"quantity":2,"sku":"reserved-coffee"},"id":"r-coffee"}`},
	}
	if got := canonicalInputs(t, r.inputs); !reflect.DeepEqual(got, want) {
		t.Errorf("node inputs %v want %v", got, want)
	}
}

// A workflow without a literal registers and runs exactly as before: there
// is nothing for registration to check.
func TestStepWithoutLiteralIsUnaffected(t *testing.T) {
	r := &recorder{}
	n := newConfNodes(r)
	c := confCatalog(t, n)
	wf := flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body"))
	})
	if err := RegisterWorkflow(c, wf, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); err != nil {
		t.Fatalf("no-literal workflow: %v", err)
	}
	if _, literals := catalogProgram(t, c, confSpec.Name, confSpec.Version); literals != nil {
		t.Fatalf("literals %q", literals)
	}
	output, err := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, []byte(`{"sku":"coffee","quantity":2}`), confBudget())
	if err != nil || canonicalJSON(t, output) != `{"committed":"reserved-coffee","quantity":2}` {
		t.Fatalf("invoke %s %v", output, err)
	}
}

// An oversized literal keeps ErrBudget. A literal over the 1 MiB payload
// limit, before or after normalization, exceeds every budget
// (tool.Budget caps MaxInputBytes at 1 MiB), so registration refuses it
// with ErrBudget, not as a schema mismatch. A literal within the limit but
// over one invocation's MaxInputBytes registers and trips ErrBudget at that
// invocation, as before.
func TestOversizedLiteralTripsErrBudget(t *testing.T) {
	big := strings.Repeat("x", schema.MaxPayloadBytes)
	t.Run("over-the-payload-limit", func(t *testing.T) {
		n := newConfNodes(&recorder{})
		c := confCatalog(t, n)
		// Valid against the schema in every respect but size.
		err := RegisterWorkflow(c, literalWorkflow(n, object{"sku": big, "quantity": 1}), []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata())
		if !errors.Is(err, ErrBudget) || errors.Is(err, ErrNotAgentSafe) || !strings.Contains(err.Error(), `call "commit"`) {
			t.Fatalf("oversized literal: %v", err)
		}
		if registered(c, confSpec.Name, confSpec.Version) {
			t.Fatal("oversized literal registered")
		}
	})
	t.Run("over-the-limit-after-defaults", func(t *testing.T) {
		half := strings.Repeat("y", schema.MaxPayloadBytes/2+1)
		defaulted, _ := json.Marshal(half)
		inflating := `{"type":"object","properties":{"sku":{"type":"string"},"note":{"type":"string","default":` + string(defaulted) + `}},"required":["sku"],"additionalProperties":false}`
		inflate := node.MustDefine("conf/inflate", "1.0.0", func(_ context.Context, in object) (object, error) {
			return object{"committed": "x", "quantity": 1}, nil
		}, node.Description("inflates"), node.Schemas([]byte(inflating), []byte(confCommitSchema)), node.Effects("db:conf"))
		registry := node.NewRegistry()
		if err := registry.Register(inflate.Any()); err != nil {
			t.Fatal(err)
		}
		c := NewCatalog(registry, nil)
		if err := RegisterNode(c, inflate, confManifest(), tool.Resources{}, metadata()); err != nil {
			t.Fatal(err)
		}
		wf := flow.MustDefine(confSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
			return flow.Call(b, "commit", inflate, flow.Lit(object{"sku": half}))
		})
		err := RegisterWorkflow(c, wf, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata())
		if !errors.Is(err, ErrBudget) || !strings.Contains(err.Error(), `call "commit"`) {
			t.Fatalf("literal over the limit after defaults: %v", err)
		}
	})
	t.Run("over-the-invocation-budget", func(t *testing.T) {
		r := &recorder{}
		n := newConfNodes(r)
		c := confCatalog(t, n)
		if err := RegisterWorkflow(c, literalWorkflow(n, object{"sku": strings.Repeat("z", 2048), "quantity": 1}), []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); err != nil {
			t.Fatalf("literal within the payload limit: %v", err)
		}
		b := confBudget()
		b.MaxInputBytes = 1024
		if _, err := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, []byte(`{"sku":"coffee","quantity":2}`), b); !errors.Is(err, ErrBudget) {
			t.Fatalf("invocation budget: %v", err)
		}
		if got := r.inputs["conf/commit"]; len(got) != 0 {
			t.Fatalf("over-budget literal dispatched %d bytes", len(got[0]))
		}
	})
}
