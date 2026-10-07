package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/agent/internal/catalogprogram"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

// The conformance suite for #249: a workflow composed into the agent catalog
// lowers through the same rules as flow.Lower. For every workflow both paths
// accept, the catalog stores exactly the program flow.Lower produces and each
// node receives the same input; for every workflow flow.Lower rejects, the
// catalog rejects it for the same reason. The two documented catalog
// extensions — literal call inputs and child workflow calls — are checked
// against flow's program for the equivalent workflow.

type object = map[string]any

const (
	confOrderSchema       = `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"}},"required":["sku","quantity"]}`
	confReservationSchema = `{"type":"object","properties":{"id":{"type":"string"},"body":` + confOrderSchema + `},"required":["id","body"]}`
	confCommitSchema      = `{"type":"object","properties":{"committed":{"type":"string"},"quantity":{"type":"integer"}},"required":["committed","quantity"]}`
	confAuditSchema       = `{"type":"object","properties":{"audited":{"type":"string"}},"required":["audited"]}`
	confStringSchema      = `{"type":"string"}`
	confLabelSchema       = `{"type":"object","properties":{"label":{"type":"string"}},"required":["label"]}`
)

var confSpec = flow.Spec{Name: "conf/workflow", Version: "1.0.0", Durability: flow.Memory}

// recorder keeps every input each node receives, in order, as JSON.
type recorder struct {
	mu     sync.Mutex
	inputs map[string][]string
}

func (r *recorder) record(name string, value any) {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.inputs == nil {
		r.inputs = map[string][]string{}
	}
	r.inputs[name] = append(r.inputs[name], string(raw))
}

type confNodes struct {
	reserve, commit, audit node.Definition[object, object]
	label                  node.Definition[string, object]
}

func newConfNodes(r *recorder) confNodes {
	text := func(value any) string { s, _ := value.(string); return s }
	reserve := node.MustDefine("conf/reserve", "1.0.0", func(_ context.Context, in object) (object, error) {
		r.record("conf/reserve", in)
		return object{"id": "r-" + text(in["sku"]), "body": object{"sku": "reserved-" + text(in["sku"]), "quantity": in["quantity"]}}, nil
	}, node.Description("reserves an order"), node.Schemas([]byte(confOrderSchema), []byte(confReservationSchema)), node.Effects("db:conf"))
	commit := node.MustDefine("conf/commit", "1.0.0", func(_ context.Context, in object) (object, error) {
		r.record("conf/commit", in)
		return object{"committed": in["sku"], "quantity": in["quantity"]}, nil
	}, node.Description("commits a reserved order"), node.Schemas([]byte(confOrderSchema), []byte(confCommitSchema)), node.Effects("db:conf"))
	audit := node.MustDefine("conf/audit", "1.0.0", func(_ context.Context, in object) (object, error) {
		r.record("conf/audit", in)
		return object{"audited": in["id"]}, nil
	}, node.Description("audits a reservation"), node.Schemas([]byte(confReservationSchema), []byte(confAuditSchema)), node.Effects("db:conf"))
	label := node.MustDefine("conf/label", "1.0.0", func(_ context.Context, in string) (object, error) {
		r.record("conf/label", in)
		return object{"label": "label:" + in}, nil
	}, node.Description("labels a sku"), node.Schemas([]byte(confStringSchema), []byte(confLabelSchema)), node.Effects("db:conf"))
	return confNodes{reserve: reserve, commit: commit, audit: audit, label: label}
}

func (n confNodes) all() []node.Any {
	return []node.Any{n.reserve.Any(), n.commit.Any(), n.audit.Any(), n.label.Any()}
}

func confManifest() Manifest {
	return Manifest{Version: 1, Compatibility: "agent-compatible", Effects: []string{"db:conf"}, Capabilities: []string{"conf"}}
}

func confPrincipal() Principal {
	return Principal{ID: "synthetic-app", Capabilities: []string{"conf"}, MaxDepth: 4}
}

func confBudget() Budget {
	return Budget{MaxDepth: 4, MaxCalls: 16, MaxTokens: 100, MaxInputBytes: 4096, MaxOutputBytes: 4096, Deadline: time.Now().Add(time.Minute)}
}

// confCatalog registers every conformance node as a tool, plus any child
// workflows the case needs.
func confCatalog(t *testing.T, n confNodes) *Catalog {
	t.Helper()
	registry := node.NewRegistry()
	for _, definition := range n.all() {
		if err := registry.Register(definition); err != nil {
			t.Fatal(err)
		}
	}
	c := NewCatalog(registry, nil)
	register := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	register(RegisterNode(c, n.reserve, confManifest(), tool.Resources{}, metadata()))
	register(RegisterNode(c, n.commit, confManifest(), tool.Resources{}, metadata()))
	register(RegisterNode(c, n.audit, confManifest(), tool.Resources{}, metadata()))
	register(RegisterNode(c, n.label, confManifest(), tool.Resources{}, metadata()))
	return c
}

// catalogProgram returns the program and literals the catalog stored for a
// registered workflow tool. The program is opaque (#260): tests compare it
// with Equal and print it with %#v, and cannot run it.
func catalogProgram(t *testing.T, c *Catalog, name, version string) (*catalogprogram.Program, map[string][]byte) {
	t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()
	b, ok := c.tools[name+"@"+version]
	if !ok || b.program == nil {
		t.Fatalf("workflow tool %s@%s is not registered", name, version)
	}
	return b.program, b.program.Literals()
}

// canonicalJSON re-encodes a JSON document so two encodings of one value
// compare equal byte for byte.
func canonicalJSON(t *testing.T, raw []byte) string {
	t.Helper()
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
	out, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(out)
}

func canonicalInputs(t *testing.T, inputs map[string][]string) map[string][]string {
	t.Helper()
	out := make(map[string][]string, len(inputs))
	for name, values := range inputs {
		for _, value := range values {
			out[name] = append(out[name], canonicalJSON(t, []byte(value)))
		}
	}
	return out
}

func confCall(id, nodeName string, references ...contract.Reference) contract.InternalInstruction {
	return contract.InternalInstruction{ID: id, Kind: "call", Node: nodeName, References: references}
}

func confOutput(reference contract.Reference) contract.InternalInstruction {
	return contract.InternalInstruction{ID: flow.OutputID, Kind: "output", References: []contract.Reference{reference}}
}

// confRef is the reference shape flow.Lower produces: a nil path for a whole
// value, never an empty non-nil one.
func confRef(step string, path ...string) contract.Reference {
	if len(path) == 0 {
		return contract.Reference{Step: step}
	}
	return contract.Reference{Step: step, Path: path}
}

func confIndexed(instructions ...contract.InternalInstruction) []contract.InternalInstruction {
	for index := range instructions {
		instructions[index].Index = index
	}
	return instructions
}

// confWorkflow erases a definition's output type so cases with different
// outputs share one table. Both paths receive the same definition.
type confWorkflow struct {
	lower    func() (contract.InternalProgram, error)
	register func(c *Catalog, inputSchema, outputSchema []byte) error
}

func wrap[O any](definition flow.Definition[object, O], err error) (confWorkflow, error) {
	if err != nil {
		return confWorkflow{}, err
	}
	return confWorkflow{
		lower: definition.Lower,
		register: func(c *Catalog, inputSchema, outputSchema []byte) error {
			return RegisterWorkflow(c, definition, inputSchema, outputSchema, confManifest(), metadata())
		},
	}, nil
}

func TestCatalogLowersAcceptedWorkflowsExactlyLikeFlowLower(t *testing.T) {
	cases := []struct {
		name         string
		define       func(confNodes) (confWorkflow, error)
		outputSchema string
		want         []contract.InternalInstruction
	}{
		{
			name: "select field of earlier call",
			define: func(n confNodes) (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body"))
				}))
			},
			outputSchema: confCommitSchema,
			want:         confIndexed(confCall("reserve", "conf/reserve"), confCall("commit", "conf/commit", confRef("reserve", "body")), confOutput(confRef("commit"))),
		},
		{
			name: "whole value of earlier call has a nil path",
			define: func(n confNodes) (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					return flow.Call(b, "audit", n.audit, flow.Call(b, "reserve", n.reserve, in))
				}))
			},
			outputSchema: confAuditSchema,
			want:         confIndexed(confCall("reserve", "conf/reserve"), confCall("audit", "conf/audit", confRef("reserve")), confOutput(confRef("audit"))),
		},
		{
			name: "nested field path",
			define: func(n confNodes) (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					body := flow.Select[object, object](flow.Call(b, "reserve", n.reserve, in), "body")
					return flow.Call(b, "label", n.label, flow.Select[object, string](body, "sku"))
				}))
			},
			outputSchema: confLabelSchema,
			want:         confIndexed(confCall("reserve", "conf/reserve"), confCall("label", "conf/label", confRef("reserve", "body", "sku")), confOutput(confRef("label"))),
		},
		{
			name: "non-adjacent earlier call and workflow input after references",
			define: func(n confNodes) (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					first := flow.Call(b, "first", n.reserve, in)
					flow.Call(b, "again", n.reserve, flow.Select[object, object](first, "body"))
					committed := flow.Call(b, "commit", n.commit, flow.Select[object, object](first, "body"))
					flow.Call(b, "fresh", n.reserve, in)
					return committed
				}))
			},
			outputSchema: confCommitSchema,
			want: confIndexed(
				confCall("first", "conf/reserve"),
				confCall("again", "conf/reserve", confRef("first", "body")),
				confCall("commit", "conf/commit", confRef("first", "body")),
				confCall("fresh", "conf/reserve"),
				confOutput(confRef("commit")),
			),
		},
		{
			name: "output selects field of earlier call",
			define: func(n confNodes) (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[string] {
					return flow.Select[object, string](flow.Call(b, "reserve", n.reserve, in), "id")
				}))
			},
			outputSchema: confStringSchema,
			want:         confIndexed(confCall("reserve", "conf/reserve"), confOutput(confRef("reserve", "id"))),
		},
		{
			name: "output whole value has a nil path",
			define: func(n confNodes) (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					return flow.Call(b, "reserve", n.reserve, in)
				}))
			},
			outputSchema: confReservationSchema,
			want:         confIndexed(confCall("reserve", "conf/reserve"), confOutput(confRef("reserve"))),
		},
	}
	input := object{"sku": "coffee", "quantity": 2}
	rawInput, _ := json.Marshal(input)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Path 1: the flow builder → flow.Lower → the engine.
			flowRecorder := &recorder{}
			flowNodes := newConfNodes(flowRecorder)
			flowDefinition, err := tc.define(flowNodes)
			if err != nil {
				t.Fatalf("Define: %v", err)
			}
			lowered, err := flowDefinition.lower()
			if err != nil {
				t.Fatalf("flow.Lower: %v", err)
			}
			if !reflect.DeepEqual(lowered.Instructions, tc.want) {
				t.Fatalf("flow.Lower instructions:\n got=%#v\nwant=%#v", lowered.Instructions, tc.want)
			}
			registry := map[string]node.Any{}
			for _, definition := range flowNodes.all() {
				registry[definition.Descriptor().Name] = definition
			}
			flowResult, err := engine.New(registry).Run(context.Background(), lowered, input)
			if err != nil {
				t.Fatalf("flow program run: %v", err)
			}
			flowOutput, err := json.Marshal(flowResult.Output)
			if err != nil {
				t.Fatal(err)
			}

			// Path 2: the same workflow composed into the agent catalog.
			catalogRecorder := &recorder{}
			catalogNodes := newConfNodes(catalogRecorder)
			catalogDefinition, err := tc.define(catalogNodes)
			if err != nil {
				t.Fatalf("Define: %v", err)
			}
			c := confCatalog(t, catalogNodes)
			if err := catalogDefinition.register(c, []byte(confOrderSchema), []byte(tc.outputSchema)); err != nil {
				t.Fatalf("RegisterWorkflow: %v", err)
			}
			program, literals := catalogProgram(t, c, confSpec.Name, confSpec.Version)
			if !program.Equal(lowered) {
				t.Errorf("catalog program differs from flow.Lower:\n catalog=%#v\n    flow=%#v", program, lowered)
			}
			if len(literals) != 0 {
				t.Errorf("catalog recorded literals %v for a workflow without one", literals)
			}
			catalogOutput, err := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, rawInput, confBudget())
			if err != nil {
				t.Fatalf("catalog invoke: %v", err)
			}
			if got, want := canonicalInputs(t, catalogRecorder.inputs), canonicalInputs(t, flowRecorder.inputs); !reflect.DeepEqual(got, want) {
				t.Errorf("step inputs differ:\n catalog=%v\n    flow=%v", got, want)
			}
			if got, want := canonicalJSON(t, catalogOutput), canonicalJSON(t, flowOutput); got != want {
				t.Errorf("output: catalog=%s flow=%s", got, want)
			}
		})
	}
}

// Every workflow flow.Lower rejects, the catalog rejects too, as not
// agent-safe, and for the same reason: its error carries flow.Lower's.
// Control flow flow.Lower now lowers (#333, ADR 0031) stays not agent-safe
// in the catalog, which rejects its construct by kind.
func TestCatalogRejectsWhatFlowLowerRejectsForTheSameReason(t *testing.T) {
	n := newConfNodes(&recorder{})
	// A reference recorded by a different definition names a step this
	// program does not contain at that point. Only a leaked Ref can do that.
	var foreign flow.Ref[object]
	flow.MustDefine(flow.Spec{Name: "conf/other", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		foreign = flow.Call(b, "later", n.reserve, in)
		return foreign
	})
	var self flow.Ref[object]
	flow.MustDefine(flow.Spec{Name: "conf/other", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		self = flow.Call(b, "loop", n.reserve, in)
		return self
	})
	arm := func(id string, in flow.Ref[object]) func(*flow.ArmBuilder) flow.Ref[object] {
		return func(a *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(a, id, n.reserve, in) }
	}
	cases := []struct {
		name   string
		define func() (confWorkflow, error)
		want   string
		// flowLowers marks control flow flow.Lower accepts and the catalog
		// still rejects with want.
		flowLowers bool
	}{
		{
			name: "empty field segment inside a path",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body..sku"))
				}))
			},
			want: `flow: call "commit": input "$step.reserve.body..sku" has an empty field`,
		},
		{
			name: "trailing empty field segment",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body."))
				}))
			},
			want: `flow: call "commit": input "$step.reserve.body." has an empty field`,
		},
		{
			name: "empty field segment in the output",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					return flow.Select[object, object](flow.Call(b, "reserve", n.reserve, in), ".body")
				}))
			},
			want: `flow: output: "$step.reserve..body" has an empty field`,
		},
		{
			name: "field of the workflow input",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					return flow.Call(b, "label", n.label, flow.Select[object, string](in, "sku"))
				}))
			},
			want: `flow: call "label": input "$input.sku" cannot be lowered: it does not name a call result`,
		},
		{
			name: "zero reference",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
					return flow.Call(b, "commit", n.commit, flow.Ref[object]{})
				}))
			},
			want: `flow: call "commit": input "" cannot be lowered: it does not name a call result`,
		},
		{
			name: "forward reference",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					early := flow.Call(b, "early", n.audit, foreign)
					flow.Call(b, "later", n.reserve, in)
					return early
				}))
			},
			want: `flow: call "early": input "$step.later" does not reference an earlier call`,
		},
		{
			name: "self reference",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
					return flow.Call(b, "loop", n.reserve, self)
				}))
			},
			want: `flow: call "loop": input "$step.loop" does not reference an earlier call`,
		},
		{
			name: "output from another definition",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					flow.Call(b, "reserve", n.reserve, in)
					return foreign
				}))
			},
			want: `flow: output: "$step.later" does not reference an earlier call`,
		},
		{
			name: "output is the workflow input",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					flow.Call(b, "reserve", n.reserve, in)
					return in
				}))
			},
			want: `flow: output: "$input" cannot be lowered: it does not name a call result`,
		},
		{
			name: "if",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.If(b, "construct", flow.Select[object, bool](reserved, "ok"), arm("yes", in), arm("no", in))
				}))
			},
			want:       `flow: instruction "construct" of kind "if" cannot be lowered`,
			flowLowers: true,
		},
		{
			name: "choose",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Choose(b, "construct", flow.Select[object, string](reserved, "id"), map[string]func(*flow.ArmBuilder) flow.Ref[object]{"r-coffee": arm("coffee", in)}, arm("other", in))
				}))
			},
			want:       `flow: instruction "construct" of kind "choose" cannot be lowered`,
			flowLowers: true,
		},
		{
			name: "each",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[[]object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Each(b, "construct", flow.Select[object, []object](reserved, "lines"), 2, func(a *flow.ArmBuilder, item flow.Ref[object]) flow.Ref[object] {
						return flow.ArmCall(a, "line", n.commit, item)
					})
				}))
			},
			want: `flow: instruction "construct" of kind "each" cannot be lowered`,
		},
		{
			name: "parallel",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					flow.Parallel(b, "construct", func(a *flow.ArmBuilder) { flow.ArmCall(a, "left", n.audit, reserved) })
					return reserved
				}))
			},
			want: `flow: instruction "construct" of kind "parallel" cannot be lowered`,
		},
		{
			name: "try-finally",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					return flow.TryFinally(b, "construct", arm("attempt", in), func(a *flow.ArmBuilder) { flow.ArmCall(a, "cleanup", n.reserve, in) })
				}))
			},
			want:       `flow: instruction "construct" of kind "try-finally" cannot be lowered`,
			flowLowers: true,
		},
		{
			name: "compare",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[bool] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Compare(b, "construct", "eq", flow.Select[object, string](reserved, "id"), flow.Lit("r-coffee"))
				}))
			},
			want:       `flow: instruction "construct" of kind "compare" cannot be lowered`,
			flowLowers: true,
		},
		{
			name: "default",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Default(b, "construct", flow.Select[object, object](reserved, "body"), reserved)
				}))
			},
			want:       `flow: instruction "construct" of kind "default" cannot be lowered`,
			flowLowers: true,
		},
		{
			name: "template",
			define: func() (confWorkflow, error) {
				return wrap(flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[string] {
					reserved := flow.Call(b, "reserve", n.reserve, in)
					return flow.Template(b, "construct", "order {}", flow.Select[object, string](reserved, "id"))
				}))
			},
			want: `flow: instruction "construct" of kind "template" cannot be lowered`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			workflow, err := tc.define()
			if err != nil {
				t.Fatalf("Define: %v", err)
			}
			if _, err := workflow.lower(); tc.flowLowers && err != nil {
				t.Fatalf("flow.Lower err=%v; want the construct lowered", err)
			} else if !tc.flowLowers && (err == nil || err.Error() != tc.want) {
				t.Fatalf("flow.Lower err=%v; want %q", err, tc.want)
			}
			err = workflow.register(confCatalog(t, n), []byte(confOrderSchema), []byte(confCommitSchema))
			if !errors.Is(err, ErrNotAgentSafe) {
				t.Fatalf("catalog accepted or misclassified a workflow flow.Lower rejects: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("catalog rejected for a different reason: %q; want it to carry %q", err, tc.want)
			}
		})
	}
}

// Ids are checked once, by the builder, before either path sees the program:
// an id outside the grammar and the reserved output id are Define errors, so
// neither flow.Lower nor the catalog can be handed such a program.
func TestReservedAndOutOfGrammarIDsNeverReachEitherLowering(t *testing.T) {
	n := newConfNodes(&recorder{})
	for _, id := range []string{flow.OutputID, "a.b", "Reserve", "1st", "", "has space", strings.Repeat("a", 65)} {
		t.Run(fmt.Sprintf("%q", id), func(t *testing.T) {
			definition, err := flow.Define(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, id, n.reserve, in)
			})
			if err == nil {
				t.Fatalf("Define accepted id %q: %+v", id, definition.Program().Instructions)
			}
			if _, err := definition.Lower(); err == nil {
				t.Fatal("flow.Lower lowered the zero definition")
			}
			if err := RegisterWorkflow(confCatalog(t, n), definition, []byte(confOrderSchema), []byte(confReservationSchema), confManifest(), metadata()); err == nil {
				t.Fatal("catalog registered the zero definition")
			}
		})
	}
}

// A literal call input is the catalog's documented extension (#249, ADR
// 0001): flow.Lower keeps rejecting it, and the catalog lowers the call to
// exactly the program flow.Lower produces for the same workflow with the
// call taking the workflow input. The literal is the only difference, and
// it reaches the node.
func TestCatalogLiteralIsTheOnlyDifferenceFromFlowLower(t *testing.T) {
	literal := object{"sku": "tea", "quantity": 1}
	build := func(n confNodes, useLiteral bool) flow.Definition[object, object] {
		return flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			reserved := flow.Call(b, "reserve", n.reserve, in)
			commitInput := in
			if useLiteral {
				commitInput = flow.Lit(literal)
			}
			committed := flow.Call(b, "commit", n.commit, commitInput)
			flow.Call(b, "audit", n.audit, reserved)
			return committed
		})
	}
	n := newConfNodes(&recorder{})
	if _, err := build(n, true).Lower(); err == nil || err.Error() != `flow: call "commit": literal input cannot be lowered: the engine program has no literal form` {
		t.Fatalf("flow.Lower must keep rejecting literals: %v", err)
	}
	twin, err := build(n, false).Lower()
	if err != nil {
		t.Fatalf("flow.Lower of the workflow-input twin: %v", err)
	}

	r := &recorder{}
	catalogNodes := newConfNodes(r)
	c := confCatalog(t, catalogNodes)
	if err := RegisterWorkflow(c, build(catalogNodes, true), []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); err != nil {
		t.Fatalf("catalog literal: %v", err)
	}
	program, literals := catalogProgram(t, c, confSpec.Name, confSpec.Version)
	if !program.Equal(twin) {
		t.Errorf("catalog program differs from flow.Lower's twin:\n catalog=%#v\n    twin=%#v", program, twin)
	}
	encoded, _ := json.Marshal(literal)
	if len(literals) != 1 || string(literals["commit"]) != string(encoded) {
		t.Errorf("literals=%q; want only commit=%s", literals, encoded)
	}
	output, err := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, []byte(`{"sku":"coffee","quantity":2}`), confBudget())
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	want := map[string][]string{
		"conf/reserve": {`{"quantity":2,"sku":"coffee"}`},
		"conf/commit":  {`{"quantity":1,"sku":"tea"}`},
		"conf/audit":   {`{"body":{"quantity":2,"sku":"reserved-coffee"},"id":"r-coffee"}`},
	}
	if got := canonicalInputs(t, r.inputs); !reflect.DeepEqual(got, want) {
		t.Errorf("step inputs=%v want %v", got, want)
	}
	if got := canonicalJSON(t, output); got != `{"committed":"tea","quantity":1}` {
		t.Errorf("output=%s", got)
	}
}

// A child workflow call is the catalog's other extension: flow.Lower rejects
// Child, and the catalog lowers it as a call under the same reference rules,
// with a nil path for the child's whole result.
func TestCatalogChildWorkflowLowersUnderFlowReferenceRules(t *testing.T) {
	r := &recorder{}
	n := newConfNodes(r)
	c := confCatalog(t, n)
	childSpec := flow.Spec{Name: "conf/child", Version: "1.0.0", Durability: flow.Memory}
	child := flow.MustDefine(childSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		return flow.Call(b, "commit", n.commit, in)
	})
	if err := RegisterWorkflow(c, child, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); err != nil {
		t.Fatalf("child: %v", err)
	}
	parent := flow.MustDefine(confSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		committed := flow.Child[object, object](b, "nested", "conf/child@1.0.0", flow.Select[object, object](reserved, "body"))
		return committed
	})
	if _, err := parent.Lower(); err == nil || err.Error() != `flow: instruction "nested" of kind "child" cannot be lowered` {
		t.Fatalf("flow.Lower must keep rejecting Child: %v", err)
	}
	if err := RegisterWorkflow(c, parent, []byte(confOrderSchema), []byte(confCommitSchema), confManifest(), metadata()); err != nil {
		t.Fatalf("parent: %v", err)
	}
	program, _ := catalogProgram(t, c, confSpec.Name, confSpec.Version)
	want := confIndexed(confCall("reserve", "conf/reserve"), confCall("nested", "conf/child@1.0.0", confRef("reserve", "body")), confOutput(confRef("nested")))
	if !program.Equal(contract.InternalProgram{WorkflowID: confSpec.Name, Version: confSpec.Version, Instructions: want}) {
		t.Errorf("child program:\n got=%#v\nwant=%#v", program, want)
	}
	output, err := c.Invoke(context.Background(), confPrincipal(), confSpec.Name, confSpec.Version, []byte(`{"sku":"coffee","quantity":2}`), confBudget())
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if got := canonicalJSON(t, output); got != `{"committed":"reserved-coffee","quantity":2}` {
		t.Errorf("output=%s", got)
	}
	if got := canonicalInputs(t, r.inputs)["conf/commit"]; !reflect.DeepEqual(got, []string{`{"quantity":2,"sku":"reserved-coffee"}`}) {
		t.Errorf("child commit input=%v", got)
	}
}
