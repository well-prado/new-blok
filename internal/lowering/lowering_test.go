package lowering

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

func call(id, input string) Instruction {
	return Instruction{Kind: "call", ID: id, Node: "node", Input: input}
}

// The builder rejects these ids before either caller can lower them; the
// lowering repeats the rules so a hand-assembled instruction list cannot
// produce a program the canonical compiler would reject.
func TestLowerRejectsIDsTheBuilderRejects(t *testing.T) {
	cases := map[string]struct {
		instructions []Instruction
		want         string
	}{
		"reserved output id": {[]Instruction{call(OutputID, "$input")}, `flow: instruction id "output" is reserved`},
		"dotted id":          {[]Instruction{call("a.b", "$input")}, `flow: instruction id "a.b" does not match the id grammar`},
		"uppercase id":       {[]Instruction{call("Reserve", "$input")}, `flow: instruction id "Reserve" does not match the id grammar`},
		"empty id":           {[]Instruction{call("", "$input")}, `flow: instruction id "" does not match the id grammar`},
		"too long id":        {[]Instruction{call(strings.Repeat("a", 65), "$input")}, `does not match the id grammar`},
		"duplicate id":       {[]Instruction{call("a", "$input"), call("a", "$input")}, `flow: duplicate instruction id a`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			for _, options := range []Options{{}, {Literals: true, Children: true}} {
				_, err := Lower("w", "1.0.0", tc.instructions, "$step.a", options)
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("options %+v: err=%v; want %q", options, err, tc.want)
				}
			}
		})
	}
}

// The zero Options are flow.Lower: literals and children stay rejected with
// the messages flow.Lower has always returned.
func TestZeroOptionsRejectBothCatalogExtensions(t *testing.T) {
	literal := Instruction{Kind: "call", ID: "a", Node: "node", Input: "$literal", Literal: []byte(`{"x":1}`)}
	if _, err := Lower("w", "1.0.0", []Instruction{literal}, "$step.a", Options{}); err == nil || err.Error() != `flow: call "a": literal input cannot be lowered: the engine program has no literal form` {
		t.Fatalf("literal err=%v", err)
	}
	child := Instruction{Kind: "child", ID: "a", Node: "child@1.0.0", Input: "$input"}
	if _, err := Lower("w", "1.0.0", []Instruction{child}, "$child.a", Options{}); err == nil || err.Error() != `flow: instruction "a" of kind "child" cannot be lowered` {
		t.Fatalf("child err=%v", err)
	}
	if _, err := Lower("w", "1.0.0", []Instruction{call("a", "$input")}, "$child.a", Options{}); err == nil || err.Error() != `flow: output: "$child.a" cannot be lowered: it does not name a call result` {
		t.Fatalf("child reference err=%v", err)
	}
}

func TestLiteralsLowerWithoutReferencesAndAreReturnedByCallID(t *testing.T) {
	encoded := []byte(`{"x":1}`)
	instructions := []Instruction{call("a", "$input"), {Kind: "call", ID: "b", Node: "node", Input: "$literal", Literal: encoded}}
	result, err := Lower("w", "1.0.0", instructions, "$step.b", Options{Literals: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []contract.InternalInstruction{
		{Index: 0, ID: "a", Kind: "call", Node: "node"},
		{Index: 1, ID: "b", Kind: "call", Node: "node"},
		{Index: 2, ID: OutputID, Kind: "output", References: []contract.Reference{{Step: "b"}}},
	}
	if !reflect.DeepEqual(result.Program.Instructions, want) {
		t.Fatalf("program=%#v", result.Program.Instructions)
	}
	if len(result.Literals) != 1 || string(result.Literals["b"]) != string(encoded) {
		t.Fatalf("literals=%q", result.Literals)
	}
	encoded[0] = 'x'
	if string(result.Literals["b"]) != `{"x":1}` {
		t.Fatal("literal aliases the recorded bytes")
	}
	missing := Instruction{Kind: "child", ID: "c", Node: "child@1.0.0", Input: "$literal"}
	if _, err := Lower("w", "1.0.0", []Instruction{missing}, "$child.c", Options{Literals: true, Children: true}); err == nil || err.Error() != `flow: child "c": literal input has no recorded value` {
		t.Fatalf("literal without a value err=%v", err)
	}
}

// A child result is referenced as "$child.<id>" and a call result as
// "$step.<id>"; a reference with the other kind's prefix names no earlier
// instruction of that kind.
func TestChildReferencesFollowTheCallRules(t *testing.T) {
	child := Instruction{Kind: "child", ID: "c", Node: "child@1.0.0", Input: "$step.a.body"}
	options := Options{Children: true}
	result, err := Lower("w", "1.0.0", []Instruction{call("a", "$input"), child}, "$child.c", options)
	if err != nil {
		t.Fatal(err)
	}
	want := []contract.InternalInstruction{
		{Index: 0, ID: "a", Kind: "call", Node: "node"},
		{Index: 1, ID: "c", Kind: "call", Node: "child@1.0.0", References: []contract.Reference{{Step: "a", Path: []string{"body"}}}},
		{Index: 2, ID: OutputID, Kind: "output", References: []contract.Reference{{Step: "c"}}},
	}
	if !reflect.DeepEqual(result.Program.Instructions, want) {
		t.Fatalf("program=%#v", result.Program.Instructions)
	}
	for _, tc := range []struct{ output, want string }{
		{"$step.c", `flow: output: "$step.c" does not reference an earlier call`},
		{"$child.a", `flow: output: "$child.a" does not reference an earlier call`},
		{"$child.c..x", `flow: output: "$child.c..x" has an empty field`},
	} {
		if _, err := Lower("w", "1.0.0", []Instruction{call("a", "$input"), child}, tc.output, options); err == nil || err.Error() != tc.want {
			t.Errorf("output %s: err=%v; want %q", tc.output, err, tc.want)
		}
	}
}

func ifOf(id string, then, otherwise []Instruction, thenOutput, elseOutput string) Instruction {
	return Instruction{Kind: "if", ID: id, Input: "$literal", Literal: []byte("true"), Arms: []Arm{
		{Name: "then", Instructions: then, Output: thenOutput}, {Name: "else", Instructions: otherwise, Output: elseOutput},
	}}
}

// Without Options.Control (the agent catalog) a control construct is
// rejected by kind wherever it is, as before #333.
func TestControlIsRejectedWithoutTheControlOption(t *testing.T) {
	nested := ifOf("outer", []Instruction{call("a", "$input"), ifOf("inner", nil, nil, "$step.a", "$step.a")}, nil, "$join.inner", "$literal")
	for _, options := range []Options{{}, {Literals: true, Children: true}} {
		if _, err := Lower("w", "1.0.0", []Instruction{nested}, "$join.outer", options); err == nil || err.Error() != `flow: instruction "outer" of kind "if" cannot be lowered` {
			t.Fatalf("options %+v: err=%v", options, err)
		}
	}
	// A construct nested in an arm is checked before anything else, even
	// with the option.
	template := Instruction{Kind: "template", ID: "label", Data: map[string]any{"template": "x"}}
	if _, err := Lower("w", "1.0.0", []Instruction{call("a", "$input"), ifOf("route", []Instruction{template}, nil, "$step.a", "$step.a")}, "$join.route", Options{Control: true}); err == nil || err.Error() != `flow: instruction "label" of kind "template" cannot be lowered` {
		t.Fatalf("nested template: err=%v", err)
	}
}

func TestControlNestingIsBounded(t *testing.T) {
	// build nests levels ifs, each in the then arm of the one before; every
	// arm reads "a", which the workflow computes first.
	build := func(levels int) ([]Instruction, string) {
		var block []Instruction
		output := "$step.a"
		for level := 0; level < levels; level++ {
			id := fmt.Sprintf("c%d", level)
			block = []Instruction{ifOf(id, block, nil, output, "$step.a")}
			output = "$join." + id
		}
		return append([]Instruction{call("a", "$input")}, block...), output
	}
	instructions, output := build(MaxNesting)
	if _, err := Lower("w", "1.0.0", instructions, output, Options{Control: true}); err != nil {
		t.Fatalf("at the bound: %v", err)
	}
	instructions, output = build(MaxNesting + 1)
	if _, err := Lower("w", "1.0.0", instructions, output, Options{Control: true}); err == nil || err.Error() != `flow: if "c0": control flow nests deeper than 64 levels` {
		t.Fatalf("past the bound: err=%v", err)
	}
}

func eachOf(id, items string, body []Instruction, output string) Instruction {
	return Instruction{Kind: "each", ID: id, Input: items, Data: map[string]any{"concurrency": 2}, Arms: []Arm{{Name: "body", Instructions: body, Output: output}}}
}

func parallelOf(id string, arms ...[]Instruction) Instruction {
	instruction := Instruction{Kind: "parallel", ID: id}
	for index, arm := range arms {
		instruction.Arms = append(instruction.Arms, Arm{Name: fmt.Sprint(index), Instructions: arm})
	}
	return instruction
}

// An each's item is readable only in its body, a body's steps nowhere
// outside it; a parallel's arm steps are readable after the parallel, never
// in a sibling arm. A control operand may read the workflow input.
func TestEachAndParallelScopes(t *testing.T) {
	control := Options{Control: true}
	lower := func(output string, instructions ...Instruction) error {
		_, err := Lower("w", "1.0.0", instructions, output, control)
		return err
	}
	cases := map[string]struct {
		err  error
		want string
	}{
		"item and input read in the body":    {lower("$join.loop", eachOf("loop", "$input.lines", []Instruction{call("line", "$item.loop.sku")}, "$step.line")), ""},
		"outer item read in a nested body":   {lower("$join.outer", eachOf("outer", "$input", []Instruction{eachOf("inner", "$item.outer.lines", []Instruction{call("line", "$item.outer")}, "$step.line")}, "$join.inner")), ""},
		"item read after its each":           {lower("$step.after", eachOf("loop", "$input", []Instruction{call("line", "$item.loop")}, "$step.line"), call("after", "$item.loop")), `flow: call "after": input "$item.loop" reads the item of each "loop", which is readable only inside that each's body`},
		"body step read after its each":      {lower("$step.after", eachOf("loop", "$input", []Instruction{call("line", "$item.loop")}, "$step.line"), call("after", "$step.line")), `flow: call "after": input "$step.line" names a step inside arm "body" of each "loop", which is not visible here: read the construct's result instead`},
		"each result read inside its body":   {lower("$join.loop", eachOf("loop", "$input", []Instruction{call("line", "$join.loop")}, "$step.line")), `flow: call "line": input "$join.loop" does not reference an earlier operation or construct`},
		"parallel arms read after it":        {lower("$step.after", call("a", "$input"), parallelOf("fan", []Instruction{call("left", "$step.a")}, []Instruction{call("right", "$step.a")}), call("after", "$step.right")), ""},
		"sibling arm read in a parallel":     {lower("$step.left", parallelOf("fan", []Instruction{call("left", "$input")}, []Instruction{call("right", "$step.left")})), `flow: call "right": input "$step.left" names a step inside arm "0" of parallel "fan", which is not visible here: read the construct's result instead`},
		"parallel has no result":             {lower("$join.fan", parallelOf("fan", []Instruction{call("left", "$input")})), `flow: output: "$join.fan" does not reference an earlier operation or construct`},
		"input field still not a call input": {lower("$step.a", call("a", "$input.sku")), `flow: call "a": input "$input.sku" cannot be lowered: it does not name a call result`},
		"empty input field in an operand":    {lower("$join.loop", eachOf("loop", "$input..lines", []Instruction{call("line", "$item.loop")}, "$step.line")), `flow: each "loop": items "$input..lines" has an empty field`},
	}
	for name, tc := range cases {
		if tc.want == "" && tc.err != nil || tc.want != "" && (tc.err == nil || tc.err.Error() != tc.want) {
			t.Errorf("%s: err=%v; want %q", name, tc.err, tc.want)
		}
	}
	bad := eachOf("loop", "$input", []Instruction{call("line", "$item.loop")}, "$step.line")
	bad.Data["concurrency"] = 0
	if err := lower("$join.loop", bad); err == nil || err.Error() != `flow: each "loop": concurrency 0 is not between 1 and 1024` {
		t.Errorf("unbounded each: err=%v", err)
	}
}

// A decoded recording holds counts as float64 or json.Number; a whole one
// lowers as flow's int does, a fractional one is refused.
func TestEachConcurrencyReadsDecodedNumbers(t *testing.T) {
	for _, concurrency := range []any{2, int64(2), float64(2), json.Number("2")} {
		each := eachOf("loop", "$input", []Instruction{call("line", "$item.loop")}, "$step.line")
		each.Data["concurrency"] = concurrency
		result, err := Lower("w", "1.0.0", []Instruction{each}, "$join.loop", Options{Control: true})
		if err != nil || result.Program.Instructions[0].Control.Concurrency != 2 {
			t.Errorf("%T: err=%v", concurrency, err)
		}
	}
	each := eachOf("loop", "$input", []Instruction{call("line", "$item.loop")}, "$step.line")
	each.Data["concurrency"] = 2.5
	if _, err := Lower("w", "1.0.0", []Instruction{each}, "$join.loop", Options{Control: true}); err == nil || !strings.Contains(err.Error(), "concurrency 2.5 is not between 1 and 1024") {
		t.Errorf("fractional: err=%v", err)
	}
}
