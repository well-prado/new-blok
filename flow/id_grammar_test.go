package flow_test

import (
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/flow"
)

// Issue #251: flow step ids follow the document id grammar
// (^[a-z][a-z0-9_-]{0,63}$). A step id outside it either has no document form
// or, worse, is misread: "$step.a.b" lowers as field b of step a.

func invalidIDPanic(id string) string {
	return fmt.Sprintf("flow: instruction id %q does not match the id grammar ^[a-z][a-z0-9_-]{0,63}$; rename the step", id)
}

// defineOutcome calls flow.Define and reports its error and any panic that
// escaped it.
func defineOutcome(build func(*flow.Builder, flow.Ref[object]) flow.Ref[object]) (definition flow.Definition[object, object], err error, recovered any) {
	defer func() { recovered = recover() }()
	definition, err = flow.Define(conformanceSpec, build)
	return definition, err, nil
}

func TestDottedStepIDIsRejectedInsteadOfMisreadAsAFieldPath(t *testing.T) {
	n := newConformanceNodes(t)
	definition, err, recovered := defineOutcome(func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "a", n.reserve, in)
		return flow.Call(b, "a.b", n.commit, flow.Select[object, object](reserved, "body"))
	})
	if err == nil && recovered == nil {
		program, lowerErr := definition.Lower()
		_, canonicalErr := canonicalProgram(t, n.descriptors(), program.Instructions)
		t.Fatalf("flow accepted step id %q: Lower err=%v instructions=%+v canonical compiler=%v", "a.b", lowerErr, program.Instructions, canonicalErr)
	}
	if recovered != nil {
		t.Fatalf("Define panicked instead of returning an error: %v", recovered)
	}
	if err.Error() != invalidIDPanic("a.b") {
		t.Fatalf("err=%v\nwant %s", err, invalidIDPanic("a.b"))
	}
}

// grammarInvalidIDs each break one clause of the grammar.
var grammarInvalidIDs = []string{
	"",                            // empty
	"a.b",                         // the reference field separator
	"Calculate",                   // uppercase
	"Output",                      // uppercase spelling of the reserved id
	"1st",                         // leading digit
	"-step",                       // leading hyphen
	"_step",                       // leading underscore
	"a b",                         // space
	"a/b",                         // slash
	"a$b",                         // dollar
	"a:b",                         // colon
	"caf\u00e9",                   // non-ASCII letter
	"$step",                       // reference syntax
	"a" + strings.Repeat("b", 64), // 65 characters
}

// idTakingBuilders returns one build per id-taking builder that gives that
// builder id; every other id is grammar-valid.
func idTakingBuilders(n conformanceNodes, id string) map[string]func(*flow.Builder, flow.Ref[object]) flow.Ref[object] {
	reserve := func(a *flow.ArmBuilder, in flow.Ref[object]) flow.Ref[object] {
		return flow.ArmCall(a, "arm", n.reserve, in)
	}
	step := func(build func(*flow.Builder, flow.Ref[object])) func(*flow.Builder, flow.Ref[object]) flow.Ref[object] {
		return func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			build(b, in)
			return in
		}
	}
	return map[string]func(*flow.Builder, flow.Ref[object]) flow.Ref[object]{
		"call": step(func(b *flow.Builder, in flow.Ref[object]) { flow.Call(b, id, n.reserve, in) }),
		"arm call": step(func(b *flow.Builder, in flow.Ref[object]) {
			flow.Parallel(b, "fan", func(a *flow.ArmBuilder) { flow.ArmCall(a, id, n.reserve, in) })
		}),
		"if": step(func(b *flow.Builder, in flow.Ref[object]) {
			flow.If(b, id, flow.Lit(true), func(a *flow.ArmBuilder) flow.Ref[object] { return reserve(a, in) }, func(*flow.ArmBuilder) flow.Ref[object] { return in })
		}),
		"choose": step(func(b *flow.Builder, in flow.Ref[object]) {
			flow.Choose(b, id, flow.Lit("a"), map[string]func(*flow.ArmBuilder) flow.Ref[object]{"a": func(a *flow.ArmBuilder) flow.Ref[object] { return reserve(a, in) }}, func(*flow.ArmBuilder) flow.Ref[object] { return in })
		}),
		"each": step(func(b *flow.Builder, _ flow.Ref[object]) {
			flow.Each(b, id, flow.Lit([]object{}), 1, func(a *flow.ArmBuilder, item flow.Ref[object]) flow.Ref[object] { return reserve(a, item) })
		}),
		"parallel": step(func(b *flow.Builder, in flow.Ref[object]) {
			flow.Parallel(b, id, func(a *flow.ArmBuilder) { reserve(a, in) })
		}),
		"try-finally": step(func(b *flow.Builder, in flow.Ref[object]) {
			flow.TryFinally(b, id, func(a *flow.ArmBuilder) flow.Ref[object] { return reserve(a, in) }, func(*flow.ArmBuilder) {})
		}),
		"child":    step(func(b *flow.Builder, in flow.Ref[object]) { flow.Child[object, object](b, id, "child", in) }),
		"compare":  step(func(b *flow.Builder, _ flow.Ref[object]) { flow.Compare(b, id, "eq", flow.Lit(1), flow.Lit(1)) }),
		"default":  step(func(b *flow.Builder, in flow.Ref[object]) { flow.Default(b, id, in, in) }),
		"template": step(func(b *flow.Builder, _ flow.Ref[object]) { flow.Template(b, id, "x {}", flow.Lit("y")) }),
	}
}

// Every builder shares one id namespace and one grammar, including a call
// made inside a control construct's arm. Define returns the violation as an
// error; MustDefine panics with the same message.
func TestEveryIDTakingBuilderRejectsGrammarInvalidIDs(t *testing.T) {
	n := newConformanceNodes(t)
	for _, id := range grammarInvalidIDs {
		builders := idTakingBuilders(n, id)
		if len(builders) != 11 {
			t.Fatalf("builders=%d; every id-taking builder must be covered", len(builders))
		}
		for name, build := range builders {
			t.Run(fmt.Sprintf("%s/%q", name, id), func(t *testing.T) {
				want := invalidIDPanic(id)
				definition, err, recovered := defineOutcome(build)
				if recovered != nil {
					t.Fatalf("Define panicked instead of returning an error: %v", recovered)
				}
				if err == nil {
					t.Fatalf("Define accepted step id %q: program=%+v", id, definition.Program().Instructions)
				}
				if err.Error() != want {
					t.Fatalf("err=%v\nwant %s", err, want)
				}
				if got := fmt.Sprint(authoringPanic(func() { flow.MustDefine(conformanceSpec, build) })); got != want {
					t.Fatalf("MustDefine panic=%v\nwant %s", got, want)
				}
			})
		}
	}
}

// Ids at the edges of the grammar still lower, to the program the canonical
// compiler produces for the same structure.
func TestGrammarBoundaryIDsLowerLikeTheCanonicalCompiler(t *testing.T) {
	n := newConformanceNodes(t)
	for _, id := range []string{"a", "z9", "line_items", "line-items", "a" + strings.Repeat("b", 63)} {
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
			canonical, err := canonicalProgram(t, n.descriptors(), want)
			if err != nil || !reflect.DeepEqual(canonical.Instructions, program.Instructions) {
				t.Fatalf("canonical=%+v err=%v", canonical.Instructions, err)
			}
		})
	}
}

// Define returns every authoring violation a builder detects, not only the
// id grammar: the reserved and duplicate ids and construct shape rules too.
func TestDefineReturnsBuilderViolationsAsErrors(t *testing.T) {
	n := newConformanceNodes(t)
	cases := map[string]struct {
		build func(*flow.Builder, flow.Ref[object]) flow.Ref[object]
		want  string
	}{
		"reserved id": {func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			return flow.Call(b, flow.OutputID, n.reserve, in)
		}, reservedOutputPanic},
		"duplicate id": {func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			flow.Call(b, "same", n.reserve, in)
			return flow.Call(b, "same", n.reserve, in)
		}, "flow: duplicate instruction id same"},
		"unbounded each": {func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			flow.Each(b, "loop", flow.Lit([]object{}), 0, func(a *flow.ArmBuilder, item flow.Ref[object]) flow.Ref[object] { return item })
			return in
		}, "flow: each concurrency must be between 1 and 1024"},
		"empty field path": {func(_ *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			return flow.Select[object, object](in, "")
		}, "flow: field path is required"},
	}
	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			_, err, recovered := defineOutcome(test.build)
			if recovered != nil {
				t.Fatalf("Define panicked instead of returning an error: %v", recovered)
			}
			if err == nil || err.Error() != test.want {
				t.Fatalf("err=%v\nwant %s", err, test.want)
			}
		})
	}
}

// Define converts only flow's own authoring violations. A panic raised by
// the application's build callback is a bug in that callback and propagates
// unchanged, so Define never reports it as a definition error.
func TestDefinePropagatesNonFlowPanics(t *testing.T) {
	sentinel := errors.New("application bug")
	_, err, recovered := defineOutcome(func(*flow.Builder, flow.Ref[object]) flow.Ref[object] {
		panic(sentinel)
	})
	if recovered != sentinel || err != nil {
		t.Fatalf("recovered=%v err=%v; want the callback's own panic value to propagate", recovered, err)
	}
}

// MustDefine does not route through Define's recovery: a broken rule panics
// at the builder call itself, so the crash stack names the offending line.
func TestMustDefinePanicsAtTheBuilderCall(t *testing.T) {
	n := newConformanceNodes(t)
	var stack string
	func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				stack = string(debug.Stack())
			}
		}()
		flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			return flow.Call(b, "a.b", n.reserve, in)
		})
	}()
	if !strings.Contains(stack, "flow.Call[") || !strings.Contains(stack, "TestMustDefinePanicsAtTheBuilderCall.func") {
		t.Fatalf("MustDefine panic stack does not reach the builder call:\n%s", stack)
	}
}
