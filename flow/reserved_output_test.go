package flow_test

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/flow"
)

// Issue #247: Lower appends its own instruction under flow.OutputID, so an
// authored instruction with that id would give the lowered program two
// instructions sharing one id. The id is reserved at authoring time.

const reservedOutputPanic = `flow: instruction id "output" is reserved for the workflow output instruction Lower appends; rename the step`

// authoringPanic runs build and returns the recovered panic value, if any.
func authoringPanic(build func()) (recovered any) {
	defer func() { recovered = recover() }()
	build()
	return nil
}

func TestCallNamedOutputIsRejectedBeforeLowering(t *testing.T) {
	n := newConformanceNodes(t)
	var definition flow.Definition[object, object]
	recovered := authoringPanic(func() {
		definition = flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			reserved := flow.Call(b, "output", n.reserve, in)
			return flow.Call(b, "commit", n.commit, flow.Select[object, object](reserved, "body"))
		})
	})
	if recovered == nil {
		// Show the defect concretely: the lowered program, the step results
		// the engine reports for it, and the canonical document compiler's
		// verdict on the same structure.
		program, err := definition.Lower()
		ids := make([]string, 0, len(program.Instructions))
		for _, instruction := range program.Instructions {
			ids = append(ids, instruction.ID)
		}
		var steps []string
		if err == nil {
			for _, step := range runProgram(t, n.registry(), program, object{"sku": "coffee", "quantity": 2}).Steps {
				steps = append(steps, step.ID)
			}
		}
		_, canonicalErr := canonicalProgram(t, n.descriptors(), program.Instructions)
		t.Fatalf("flow.Call accepted the reserved id %q: Lower err=%v instruction ids=%v engine step ids=%v canonical compiler=%v", flow.OutputID, err, ids, steps, canonicalErr)
	}
	if fmt.Sprint(recovered) != reservedOutputPanic {
		t.Fatalf("panic=%v\nwant %s", recovered, reservedOutputPanic)
	}
}

// Every builder shares one id namespace, so every id-taking builder rejects
// the reserved id, including a call made inside a control construct's arm.
func TestEveryIDTakingBuilderRejectsTheReservedOutputID(t *testing.T) {
	n := newConformanceNodes(t)
	const id = flow.OutputID
	reserve := func(a *flow.ArmBuilder, in flow.Ref[object]) flow.Ref[object] {
		return flow.ArmCall(a, "arm", n.reserve, in)
	}
	define := func(build func(*flow.Builder, flow.Ref[object])) func() {
		return func() {
			flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				build(b, in)
				return in
			})
		}
	}
	cases := map[string]func(){
		"call": define(func(b *flow.Builder, in flow.Ref[object]) { flow.Call(b, id, n.reserve, in) }),
		"arm call": define(func(b *flow.Builder, in flow.Ref[object]) {
			flow.Parallel(b, "fan", func(a *flow.ArmBuilder) { flow.ArmCall(a, id, n.reserve, in) })
		}),
		"if": define(func(b *flow.Builder, in flow.Ref[object]) {
			flow.If(b, id, flow.Lit(true), func(a *flow.ArmBuilder) flow.Ref[object] { return reserve(a, in) }, func(*flow.ArmBuilder) flow.Ref[object] { return in })
		}),
		"choose": define(func(b *flow.Builder, in flow.Ref[object]) {
			flow.Choose(b, id, flow.Lit("a"), map[string]func(*flow.ArmBuilder) flow.Ref[object]{"a": func(a *flow.ArmBuilder) flow.Ref[object] { return reserve(a, in) }}, func(*flow.ArmBuilder) flow.Ref[object] { return in })
		}),
		"each": define(func(b *flow.Builder, in flow.Ref[object]) {
			flow.Each(b, id, flow.Lit([]object{}), 1, func(a *flow.ArmBuilder, item flow.Ref[object]) flow.Ref[object] { return reserve(a, item) })
		}),
		"parallel": define(func(b *flow.Builder, in flow.Ref[object]) {
			flow.Parallel(b, id, func(a *flow.ArmBuilder) { reserve(a, in) })
		}),
		"try-finally": define(func(b *flow.Builder, in flow.Ref[object]) {
			flow.TryFinally(b, id, func(a *flow.ArmBuilder) flow.Ref[object] { return reserve(a, in) }, func(*flow.ArmBuilder) {})
		}),
		"child":    define(func(b *flow.Builder, in flow.Ref[object]) { flow.Child[object, object](b, id, "child", in) }),
		"compare":  define(func(b *flow.Builder, _ flow.Ref[object]) { flow.Compare(b, id, "eq", flow.Lit(1), flow.Lit(1)) }),
		"default":  define(func(b *flow.Builder, in flow.Ref[object]) { flow.Default(b, id, in, in) }),
		"template": define(func(b *flow.Builder, _ flow.Ref[object]) { flow.Template(b, id, "x {}", flow.Lit("y")) }),
	}
	if len(cases) != 11 {
		t.Fatalf("cases=%d; every id-taking builder must be covered", len(cases))
	}
	for name, build := range cases {
		t.Run(name, func(t *testing.T) {
			if recovered := authoringPanic(build); fmt.Sprint(recovered) != reservedOutputPanic {
				t.Fatalf("panic=%v\nwant %s", recovered, reservedOutputPanic)
			}
		})
	}
}

// The reservation is exact: ids that merely resemble it still lower, and
// lower to the same program the canonical compiler produces.
func TestIDsResemblingTheReservedOutputIDStillLower(t *testing.T) {
	n := newConformanceNodes(t)
	for _, id := range []string{"outputs", "output-step", "result", "Output"} {
		t.Run(id, func(t *testing.T) {
			program, err := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				return flow.Call(b, id, n.reserve, in)
			}).Lower()
			if err != nil {
				t.Fatalf("Lower: %v", err)
			}
			want := indexed(call(id, "reserve"), output(ref(id)))
			if !reflect.DeepEqual(program.Instructions, want) {
				t.Fatalf("instructions=%+v want %+v", program.Instructions, want)
			}
			if !validDocumentID(id) {
				return
			}
			canonical, err := canonicalProgram(t, n.descriptors(), want)
			if err != nil || !reflect.DeepEqual(canonical.Instructions, program.Instructions) {
				t.Fatalf("canonical=%+v err=%v", canonical.Instructions, err)
			}
		})
	}
}

// validDocumentID reports whether id is expressible in a contract document,
// whose ids are lowercase; flow ids are not grammar-checked.
func validDocumentID(id string) bool {
	err := contract.Document{Version: contract.CurrentVersion, Workflow: contract.Workflow{ID: id}}.Validate()
	diagnostic, ok := err.(*contract.Error)
	return !ok || diagnostic.Path != "workflow.id"
}
