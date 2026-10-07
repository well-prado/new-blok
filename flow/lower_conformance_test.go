package flow_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/compile"
	"github.com/well-prado/new-blok/node"
)

// The conformance suite for #244: every flow construct either lowers to
// exactly the program the canonical document compiler produces for the same
// structure, and runs with each call receiving the value its input reference
// names, or Lower rejects it loudly. Lower used to drop every call input
// reference, so a later call silently received the workflow input.

type object = map[string]any

const (
	orderSchema       = `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"}},"required":["sku","quantity"]}`
	reservationSchema = `{"type":"object","properties":{"id":{"type":"string"},"body":` + orderSchema + `},"required":["id","body"]}`
	commitSchema      = `{"type":"object","properties":{"committed":{"type":"string"},"quantity":{"type":"integer"}},"required":["committed","quantity"]}`
	auditSchema       = `{"type":"object","properties":{"audited":{"type":"string"}},"required":["audited"]}`
	stringSchema      = `{"type":"string"}`
	labelSchema       = `{"type":"object","properties":{"label":{"type":"string"}},"required":["label"]}`
)

type conformanceNodes struct {
	reserve node.Definition[object, object]
	commit  node.Definition[object, object]
	audit   node.Definition[object, object]
	label   node.Definition[string, object]
}

func newConformanceNodes(t *testing.T) conformanceNodes {
	t.Helper()
	// reserve derives a body that differs from the workflow input, so a call
	// handed the workflow input instead of reserve's body is observable.
	reserve := node.MustDefine("reserve", "1.0.0", func(_ context.Context, in object) (object, error) {
		return object{"id": "r-" + in["sku"].(string), "body": object{"sku": "reserved-" + in["sku"].(string), "quantity": in["quantity"]}}, nil
	}, node.Description("reserves an order"), node.Schemas([]byte(orderSchema), []byte(reservationSchema)))
	commit := node.MustDefine("commit", "1.0.0", func(_ context.Context, in object) (object, error) {
		return object{"committed": in["sku"], "quantity": in["quantity"]}, nil
	}, node.Description("commits a reserved order"), node.Schemas([]byte(orderSchema), []byte(commitSchema)))
	audit := node.MustDefine("audit", "1.0.0", func(_ context.Context, in object) (object, error) {
		return object{"audited": in["id"]}, nil
	}, node.Description("audits a reservation"), node.Schemas([]byte(reservationSchema), []byte(auditSchema)))
	label := node.MustDefine("label", "1.0.0", func(_ context.Context, in string) (object, error) {
		return object{"label": "label:" + in}, nil
	}, node.Description("labels a sku"), node.Schemas([]byte(stringSchema), []byte(labelSchema)))
	return conformanceNodes{reserve: reserve, commit: commit, audit: audit, label: label}
}

func (n conformanceNodes) registry() map[string]node.Any {
	return map[string]node.Any{"reserve": n.reserve.Any(), "commit": n.commit.Any(), "audit": n.audit.Any(), "label": n.label.Any()}
}

func (n conformanceNodes) descriptors() []node.Descriptor {
	return []node.Descriptor{n.reserve.Descriptor(), n.commit.Descriptor(), n.audit.Descriptor(), n.label.Descriptor()}
}

var conformanceSpec = flow.Spec{Name: "conformance", Version: "1.0.0", Durability: flow.Memory}

func call(id, nodeName string, references ...contract.Reference) contract.InternalInstruction {
	return contract.InternalInstruction{ID: id, Kind: "call", Node: nodeName, References: references}
}

func output(reference contract.Reference) contract.InternalInstruction {
	return contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{reference}}
}

func ref(step string, path ...string) contract.Reference {
	if len(path) == 0 {
		return contract.Reference{Step: step}
	}
	return contract.Reference{Step: step, Path: path}
}

func indexed(instructions ...contract.InternalInstruction) []contract.InternalInstruction {
	for index := range instructions {
		instructions[index].Index = index
	}
	return instructions
}

// canonicalProgram compiles the same structure as a contract.Document through
// internal/compile, the path the #108 parity harness used instead of flow.
func canonicalProgram(t *testing.T, descriptors []node.Descriptor, instructions []contract.InternalInstruction) (contract.InternalProgram, error) {
	t.Helper()
	digest := func(data []byte) string {
		sum := sha256.Sum256(data)
		return "sha256:" + hex.EncodeToString(sum[:])
	}
	document := contract.Document{Version: contract.CurrentVersion, Workflow: contract.Workflow{
		ID: conformanceSpec.Name, Name: conformanceSpec.Name, Version: conformanceSpec.Version,
		Digest: digest([]byte("issue-244-conformance")), InputSchema: json.RawMessage(orderSchema), OutputSchema: json.RawMessage(`{"type":"object"}`),
	}}
	for _, instruction := range instructions {
		document.Workflow.Instructions = append(document.Workflow.Instructions, contract.Instruction{ID: instruction.ID, Kind: instruction.Kind, Node: instruction.Node, References: instruction.References})
	}
	for _, descriptor := range descriptors {
		document.Nodes = append(document.Nodes, contract.NodeDescriptor{ID: descriptor.Name, Version: descriptor.Version, Digest: digest(descriptor.InputSchema), InputSchema: descriptor.InputSchema, OutputSchema: descriptor.OutputSchema})
	}
	compiled, err := compile.Compile(document)
	return compiled.Program, err
}

func runProgram(t *testing.T, nodes map[string]node.Any, program contract.InternalProgram, input any) execution.Result {
	t.Helper()
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: conformanceSpec.Name}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := execution.NewRunner(application, nodes).Run(context.Background(), program, input, inspection.Invocation{})
	if err != nil {
		t.Fatalf("run: %v (steps=%+v)", err, result.Steps)
	}
	return result
}

func TestLowerCallInputsMatchCanonicalCompilerAndRun(t *testing.T) {
	n := newConformanceNodes(t)
	input := object{"sku": "coffee", "quantity": 2}
	reservation := object{"id": "r-coffee", "body": object{"sku": "reserved-coffee", "quantity": 2}}
	cases := []struct {
		name string
		// build authors the workflow through the public flow API.
		build func() (contract.InternalProgram, error)
		// want is the predeclared lowered program; the canonical compiler
		// must produce exactly the same instructions for it.
		want []contract.InternalInstruction
		// inputs is the value each call must receive, by step id.
		inputs map[string]any
		output any
	}{
		{
			name: "select field of earlier call (issue repro)",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				reserved := flow.Call(b, "reserve", n.reserve, in)
				return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body"))
			}).Lower,
			want:   indexed(call("reserve", "reserve"), call("commit", "commit", ref("reserve", "body")), output(ref("commit"))),
			inputs: map[string]any{"reserve": input, "commit": reservation["body"]},
			output: object{"committed": "reserved-coffee", "quantity": 2},
		},
		{
			name: "whole value of earlier call",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, "audit", n.audit, flow.Call(b, "reserve", n.reserve, in))
			}).Lower,
			want:   indexed(call("reserve", "reserve"), call("audit", "audit", ref("reserve")), output(ref("audit"))),
			inputs: map[string]any{"reserve": input, "audit": reservation},
			output: object{"audited": "r-coffee"},
		},
		{
			name: "nested field path",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				body := flow.Select[object, object](flow.Call(b, "reserve", n.reserve, in), "body")
				return flow.Call(b, "label", n.label, flow.Select[object, string](body, "sku"))
			}).Lower,
			want:   indexed(call("reserve", "reserve"), call("label", "label", ref("reserve", "body", "sku")), output(ref("label"))),
			inputs: map[string]any{"reserve": input, "label": "reserved-coffee"},
			output: object{"label": "label:reserved-coffee"},
		},
		{
			name: "non-adjacent earlier call and workflow input after references",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				first := flow.Call(b, "first", n.reserve, in)
				flow.Call(b, "again", n.reserve, flow.Select[object, object](first, "body"))
				committed := flow.Call(b, "commit", n.commit, flow.Select[object, object](first, "body"))
				flow.Call(b, "fresh", n.reserve, in)
				return committed
			}).Lower,
			want: indexed(
				call("first", "reserve"),
				call("again", "reserve", ref("first", "body")),
				call("commit", "commit", ref("first", "body")),
				call("fresh", "reserve"),
				output(ref("commit")),
			),
			inputs: map[string]any{
				"first":  input,
				"again":  reservation["body"],
				"commit": reservation["body"],
				"fresh":  input,
			},
			output: object{"committed": "reserved-coffee", "quantity": 2},
		},
		{
			name: "output selects field of earlier call",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[string] {
				return flow.Select[object, string](flow.Call(b, "reserve", n.reserve, in), "id")
			}).Lower,
			want:   indexed(call("reserve", "reserve"), output(ref("reserve", "id"))),
			inputs: map[string]any{"reserve": input},
			output: "r-coffee",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lowered, err := tc.build()
			if err != nil {
				t.Fatalf("Lower: %v", err)
			}
			if lowered.WorkflowID != conformanceSpec.Name || lowered.Version != conformanceSpec.Version {
				t.Fatalf("identity=%s@%s", lowered.WorkflowID, lowered.Version)
			}
			if !reflect.DeepEqual(lowered.Instructions, tc.want) {
				t.Errorf("lowered instructions:\n got=%+v\nwant=%+v", lowered.Instructions, tc.want)
			}
			canonical, err := canonicalProgram(t, n.descriptors(), tc.want)
			if err != nil {
				t.Fatalf("canonical compiler rejected the expected program: %v", err)
			}
			if !reflect.DeepEqual(canonical.Instructions, lowered.Instructions) {
				t.Errorf("flow and canonical compiler disagree:\n flow=%+v\n canonical=%+v", lowered.Instructions, canonical.Instructions)
			}
			result := runProgram(t, n.registry(), lowered, input)
			if len(result.Steps) != len(tc.want) {
				t.Fatalf("steps=%d want %d: %+v", len(result.Steps), len(tc.want), result.Steps)
			}
			for _, step := range result.Steps {
				if step.ID == "output" {
					continue
				}
				want, ok := tc.inputs[step.ID]
				if !ok {
					t.Fatalf("no predeclared input for step %q", step.ID)
				}
				if !reflect.DeepEqual(step.Input, want) {
					t.Errorf("step %q received %#v; want %#v", step.ID, step.Input, want)
				}
			}
			if !reflect.DeepEqual(result.Output, tc.output) {
				t.Errorf("output=%#v want %#v", result.Output, tc.output)
			}
		})
	}
}

func TestLowerRejectsCallInputsWithoutAProgramForm(t *testing.T) {
	n := newConformanceNodes(t)
	// A reference recorded by a different definition names a step this
	// program does not contain at that point. Only a leaked Ref can do that.
	var foreign flow.Ref[object]
	flow.MustDefine(flow.Spec{Name: "other", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		foreign = flow.Call(b, "later", n.reserve, in)
		return foreign
	})
	var self flow.Ref[object]
	flow.MustDefine(flow.Spec{Name: "other", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		self = flow.Call(b, "loop", n.reserve, in)
		return self
	})
	cases := []struct {
		name  string
		build func() (contract.InternalProgram, error)
		want  string
		// canonical is the same edge written as a document, when the
		// document form can express it; the canonical compiler must reject it
		// too.
		canonical []contract.InternalInstruction
	}{
		{
			name: "literal input",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, "commit", n.commit, flow.Lit(object{"sku": "tea", "quantity": 1}))
			}).Lower,
			want: `flow: call "commit": literal input cannot be lowered: the engine program has no literal form`,
		},
		{
			name: "field of the workflow input",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, "label", n.label, flow.Select[object, string](in, "sku"))
			}).Lower,
			want: `flow: call "label": input "$input.sku" cannot be lowered: it does not name a call result`,
		},
		{
			name: "zero reference",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, "commit", n.commit, flow.Ref[object]{})
			}).Lower,
			want: `flow: call "commit": input "" cannot be lowered: it does not name a call result`,
		},
		{
			name: "forward reference",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				early := flow.Call(b, "early", n.audit, foreign)
				flow.Call(b, "later", n.reserve, in)
				return early
			}).Lower,
			want:      `flow: call "early": input "$step.later" does not reference an earlier call`,
			canonical: indexed(call("early", "audit", ref("later")), call("later", "reserve"), output(ref("early"))),
		},
		{
			name: "self reference",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, "loop", n.reserve, self)
			}).Lower,
			want:      `flow: call "loop": input "$step.loop" does not reference an earlier call`,
			canonical: indexed(call("loop", "reserve", ref("loop")), output(ref("loop"))),
		},
		{
			name: "reference to a step from another definition",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, _ flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, "audit", n.audit, foreign)
			}).Lower,
			want:      `flow: call "audit": input "$step.later" does not reference an earlier call`,
			canonical: indexed(call("audit", "audit", ref("later")), output(ref("audit"))),
		},
		{
			name: "empty path segment",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				reserved := flow.Call(b, "reserve", n.reserve, in)
				return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body."))
			}).Lower,
			want: `flow: call "commit": input "$step.reserve.body." has an empty field`,
		},
		{
			name: "output from another definition",
			build: flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				flow.Call(b, "reserve", n.reserve, in)
				return foreign
			}).Lower,
			want:      `flow: output: "$step.later" does not reference an earlier call`,
			canonical: indexed(call("reserve", "reserve"), output(ref("later"))),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			program, err := tc.build()
			if err == nil {
				t.Fatalf("Lower accepted the program: %+v", program.Instructions)
			}
			if err.Error() != tc.want {
				t.Fatalf("error=%q want %q", err, tc.want)
			}
			if tc.canonical == nil {
				return
			}
			if _, err := canonicalProgram(t, n.descriptors(), tc.canonical); err == nil || !strings.Contains(err.Error(), "invalid_reference") {
				t.Fatalf("canonical compiler err=%v; want invalid_reference", err)
			}
		})
	}
}

type constructCase struct {
	kind    string
	program flow.Program
	lower   func() (contract.InternalProgram, error)
	// recorded checks the construct instruction's predeclared input sources.
	recorded func(flow.Instruction) bool
}

// Compare, Default, If, Choose, TryFinally, Each and Parallel lower to
// control instructions (#333, ADR 0028; control_run_test.go and
// each_parallel_test.go run them). Child and Template have no form in the
// engine program yet, so Lower must reject each one rather than lowering
// only its arm calls. Every construct's own input
// references are recorded structurally in Program.
func TestLowerRejectsEveryControlConstructWithoutAProgramForm(t *testing.T) {
	n := newConformanceNodes(t)
	arm := func(id string, in flow.Ref[object]) func(*flow.ArmBuilder) flow.Ref[object] {
		return func(a *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(a, id, n.reserve, in) }
	}
	var cases []constructCase
	add := func(kind string, program flow.Program, lower func() (contract.InternalProgram, error), recorded func(flow.Instruction) bool) {
		cases = append(cases, constructCase{kind: kind, program: program, lower: lower, recorded: recorded})
	}
	ifDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.If(b, "construct", flow.Select[object, bool](reserved, "ok"), arm("yes", in), arm("no", in))
	})
	add("if", ifDef.Program(), ifDef.Lower, func(i flow.Instruction) bool {
		return i.Input == "$step.reserve.ok" && i.Data["then"] == "$step.yes" && i.Data["else"] == "$step.no"
	})
	chooseDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Choose(b, "construct", flow.Select[object, string](reserved, "id"), map[string]func(*flow.ArmBuilder) flow.Ref[object]{"r-coffee": arm("coffee", in)}, arm("other", in))
	})
	add("choose", chooseDef.Program(), chooseDef.Lower, func(i flow.Instruction) bool {
		return i.Input == "$step.reserve.id" && i.Data["r-coffee"] == "$step.coffee" && i.Data["default"] == "$step.other"
	})
	eachDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[[]object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Each(b, "construct", flow.Select[object, []object](reserved, "lines"), 2, func(a *flow.ArmBuilder, item flow.Ref[object]) flow.Ref[object] {
			return flow.ArmCall(a, "line", n.commit, item)
		})
	})
	add("each", eachDef.Program(), eachDef.Lower, func(i flow.Instruction) bool {
		return i.Input == "$step.reserve.lines" && i.Data["body"] == "$step.line"
	})
	parallelDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		flow.Parallel(b, "construct", func(a *flow.ArmBuilder) { flow.ArmCall(a, "left", n.audit, reserved) })
		return reserved
	})
	add("parallel", parallelDef.Program(), parallelDef.Lower, func(i flow.Instruction) bool { return i.Data["policy"] == "fail-fast" })
	tryDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		return flow.TryFinally(b, "construct", arm("attempt", in), func(a *flow.ArmBuilder) { flow.ArmCall(a, "cleanup", n.reserve, in) })
	})
	add("try-finally", tryDef.Program(), tryDef.Lower, func(i flow.Instruction) bool { return i.Data["try"] == "$step.attempt" })
	childDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Child[object, object](b, "construct", "conformance/child", flow.Select[object, object](reserved, "body"))
	})
	add("child", childDef.Program(), childDef.Lower, func(i flow.Instruction) bool {
		return i.Input == "$step.reserve.body" && i.Data["workflow"] == "conformance/child"
	})
	compareDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[bool] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Compare(b, "construct", "eq", flow.Select[object, string](reserved, "id"), flow.Lit("r-coffee"))
	})
	add("compare", compareDef.Program(), compareDef.Lower, func(i flow.Instruction) bool {
		return i.Data["left"] == "$step.reserve.id" && i.Data["right"] == "$literal"
	})
	defaultDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Default(b, "construct", flow.Select[object, object](reserved, "body"), reserved)
	})
	add("default", defaultDef.Program(), defaultDef.Lower, func(i flow.Instruction) bool {
		return i.Data["value"] == "$step.reserve.body" && i.Data["fallback"] == "$step.reserve"
	})
	templateDef := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[string] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		return flow.Template(b, "construct", "order {}", flow.Select[object, string](reserved, "id"))
	})
	add("template", templateDef.Program(), templateDef.Lower, func(i flow.Instruction) bool {
		values, ok := i.Data["values"].([]string)
		return ok && reflect.DeepEqual(values, []string{"$step.reserve.id"})
	})
	if len(cases) != 9 {
		t.Fatalf("cases=%d; every flow construct must be covered", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.kind, func(t *testing.T) {
			var construct *flow.Instruction
			for index := range tc.program.Instructions {
				if tc.program.Instructions[index].ID == "construct" {
					construct = &tc.program.Instructions[index]
				}
			}
			if construct == nil || construct.Kind != tc.kind || !tc.recorded(*construct) {
				t.Fatalf("construct recorded as %+v", construct)
			}
			program, err := tc.lower()
			switch tc.kind {
			case "if", "choose", "try-finally", "compare", "default", "each", "parallel":
				if err != nil || program.Format != contract.ControlFormat {
					t.Fatalf("Lower err=%v format=%d; want the construct lowered", err, program.Format)
				}
				return
			}
			want := `flow: instruction "construct" of kind "` + tc.kind + `" cannot be lowered`
			if err == nil || err.Error() != want {
				t.Fatalf("Lower err=%v program=%+v; want %q", err, program.Instructions, want)
			}
		})
	}
}
