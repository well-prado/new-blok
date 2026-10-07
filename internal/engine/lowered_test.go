package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
)

func literal(value string) contract.Operand { return contract.Operand{Literal: json.RawMessage(value)} }

func ref(step string, path ...string) contract.Operand {
	return contract.Operand{Reference: &contract.Reference{Step: step, Path: path}}
}

func controlProgram(instructions ...contract.InternalInstruction) contract.InternalProgram {
	for index := range instructions {
		instructions[index].Index = index
	}
	return contract.InternalProgram{WorkflowID: "w", Version: "1.0.0", Format: contract.ControlFormat, Instructions: instructions}
}

func outputOf(step string) contract.InternalInstruction {
	return contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: step}}}
}

func compare(id, operator string, left, right contract.Operand) contract.InternalInstruction {
	return contract.InternalInstruction{ID: id, Kind: "compare", Control: &contract.Control{Operator: operator, Operands: []contract.Operand{left, right}}}
}

func TestCompareReadsValuesAsJSON(t *testing.T) {
	type flag bool
	cases := []struct {
		operator    string
		left, right any
		want        bool
	}{
		{"eq", 100, json.Number("100.0"), true},
		{"eq", int64(1) << 60, json.Number("1152921504606846977"), false},
		{"eq", map[string]any{"a": []any{1, "x"}}, map[string]any{"a": []any{json.Number("1"), "x"}}, true},
		{"eq", flag(true), true, true},
		{"ne", "a", "b", true},
		{"gt", 150, json.Number("100"), true},
		{"gte", 1.5, json.Number("1.5"), true},
		{"lt", "apple", "banana", true},
		{"lte", json.Number("2e3"), 2000, true},
		{"gt", nil, nil, false},
	}
	for _, tc := range cases {
		got, err := compareValues(tc.operator, tc.left, tc.right)
		if tc.left == nil {
			if err == nil || !strings.Contains(err.Error(), "gt compares two numbers or two strings, not null and null") {
				t.Errorf("%s nil: err=%v", tc.operator, err)
			}
			continue
		}
		if err != nil || got != tc.want {
			t.Errorf("%v %s %v = %v, %v; want %v", tc.left, tc.operator, tc.right, got, err, tc.want)
		}
	}
	if _, err := compareValues("lt", 1, "1"); err == nil || !strings.Contains(err.Error(), "not a number and a string") {
		t.Errorf("mixed ordering: err=%v", err)
	}
}

func TestIfConditionMustBeABoolean(t *testing.T) {
	program := controlProgram(
		compare("big", "eq", literal("1"), literal("1")),
		contract.InternalInstruction{ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{literal(`"yes"`)}, Arms: []contract.Arm{
			{Name: "then", Output: ptr(ref("big"))}, {Name: "else", Output: ptr(ref("big"))},
		}}},
		outputOf("route"),
	)
	_, err := New(nil).Run(context.Background(), program, nil)
	var classified *Error
	if !errors.As(err, &classified) || classified.Code != "invalid_condition" || classified.Step != "route" || !strings.Contains(err.Error(), "if condition is a string, not a boolean") {
		t.Fatalf("err=%v", err)
	}
}

func ptr(operand contract.Operand) *contract.Operand { return &operand }

// The interpreter checks a program's control instructions before running
// any of them, so a hand-assembled program fails closed with a diagnostic
// instead of panicking or running half.
func TestRunRejectsControlProgramsItCannotRun(t *testing.T) {
	arm := func(name string) contract.Arm { return contract.Arm{Name: name, Output: ptr(literal("1"))} }
	// nested holds controls control instructions, each in the try arm of
	// the one before.
	nested := func(controls int) contract.InternalProgram {
		inner := []contract.InternalInstruction{compare("leaf", "eq", literal("1"), literal("1"))}
		for level := 1; level < controls; level++ {
			inner = []contract.InternalInstruction{{ID: fmt.Sprintf("try-%d", level), Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{{Name: "try", Instructions: inner, Output: ptr(literal("1"))}, {Name: "finally"}}}}}
		}
		return controlProgram(append(inner, outputOf(inner[0].ID))...)
	}
	cases := map[string]struct {
		program contract.InternalProgram
		code    string
		text    string
	}{
		"unknown format":             {contract.InternalProgram{Format: 3, Instructions: []contract.InternalInstruction{outputOf("x")}}, "unsupported_program_format", "program format 3"},
		"control in the call format": {contract.InternalProgram{Instructions: []contract.InternalInstruction{compare("big", "eq", literal("1"), literal("1"))}}, "unsupported_instruction", "compare needs program format 2"},
		"missing operand":            {controlProgram(contract.InternalInstruction{ID: "big", Kind: "compare", Control: &contract.Control{Operator: "eq", Operands: []contract.Operand{literal("1")}}}), "invalid_control", "compare has 1 operands, want 2"},
		"operand with both forms":    {controlProgram(compare("big", "eq", contract.Operand{Reference: &contract.Reference{Step: "x"}, Literal: json.RawMessage("1")}, literal("1"))), "invalid_control", "exactly one of a reference and a literal"},
		"unknown operator":           {controlProgram(compare("big", "like", literal("1"), literal("1"))), "invalid_control", `compare operator "like"`},
		"if arms out of order":       {controlProgram(contract.InternalInstruction{ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{literal("true")}, Arms: []contract.Arm{arm("else"), arm("then")}}}), "invalid_control", `arm 0 is named "else", want "then"`},
		"finally with a result":      {controlProgram(contract.InternalInstruction{ID: "t", Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{arm("try"), arm("finally")}}}), "invalid_control", `only a finally arm returns no result`},
		"choose case without match":  {controlProgram(contract.InternalInstruction{ID: "c", Kind: "choose", Control: &contract.Control{Operands: []contract.Operand{literal(`"a"`)}, Arms: []contract.Arm{arm("case-0"), arm("default")}}}), "invalid_control", `invalid case value`},
		"output inside an arm":       {controlProgram(contract.InternalInstruction{ID: "t", Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{{Name: "try", Instructions: []contract.InternalInstruction{outputOf("x")}, Output: ptr(literal("1"))}, {Name: "finally"}}}}), "invalid_control", "an arm cannot hold a output instruction"},
		"body on a call":             {controlProgram(contract.InternalInstruction{ID: "c", Kind: "call", Node: "n", Control: &contract.Control{}}), "invalid_control", "a call instruction has no control body"},
		"too deep":                   {nested(maxNesting + 1), "invalid_control", "nests deeper than 64 levels"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			result, err := New(nil).Run(context.Background(), tc.program, nil)
			var classified *Error
			if !errors.As(err, &classified) || classified.Code != tc.code || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("err=%v; want %s containing %q", err, tc.code, tc.text)
			}
			if len(result.Steps) != 0 {
				t.Fatalf("ran %d steps before refusing", len(result.Steps))
			}
		})
	}
	if _, err := New(nil).Run(context.Background(), nested(maxNesting), nil); err != nil {
		t.Fatalf("nesting at the bound: %v", err)
	}
	// The step budget counts the instructions inside arms.
	wide := controlProgram(contract.InternalInstruction{ID: "t", Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{
		{Name: "try", Instructions: []contract.InternalInstruction{compare("a", "eq", literal("1"), literal("1")), compare("b", "eq", literal("1"), literal("1"))}, Output: ptr(ref("a"))}, {Name: "finally"},
	}}}, outputOf("t"))
	if _, err := New(nil).WithMaxSteps(3).Run(context.Background(), wide, nil); err == nil || !strings.Contains(err.Error(), "step_budget_exceeded") {
		t.Fatalf("budget: err=%v", err)
	}
}

type refusingJournal struct{ calls int }

func (j *refusingJournal) VerifyRun(context.Context, string, string, string) error {
	j.calls++
	return nil
}
func (j *refusingJournal) Load(context.Context, StepIdentity) (json.RawMessage, bool, error) {
	j.calls++
	return nil, false, nil
}
func (j *refusingJournal) Begin(context.Context, StepIdentity, json.RawMessage, []string) (StepAttempt, error) {
	j.calls++
	return StepAttempt{}, nil
}
func (j *refusingJournal) Complete(context.Context, StepAttempt, json.RawMessage) error {
	j.calls++
	return nil
}
func (j *refusingJournal) Fail(context.Context, StepAttempt, []string, error) error {
	j.calls++
	return nil
}

// Durable control flow is a later #333 slice: until scopes and joins are
// journaled, a durable runner refuses a control program before touching
// its journal, instead of replaying arm steps by id alone.
func TestDurableRunnerRefusesControlPrograms(t *testing.T) {
	program := controlProgram(compare("big", "eq", literal("1"), literal("1")), outputOf("big"))
	program.Digest = "sha256:" + strings.Repeat("0", 64)
	journal := &refusingJournal{}
	_, err := New(nil).RunJournaled(context.Background(), program, nil, "run-1", journal)
	var classified *Error
	if !errors.As(err, &classified) || classified.Code != "durable_control_unsupported" || journal.calls != 0 {
		t.Fatalf("err=%v journal calls=%d", err, journal.calls)
	}
	if result, err := New(nil).Run(context.Background(), program, nil); err != nil || result.Output != true {
		t.Fatalf("in memory: output=%v err=%v", result.Output, err)
	}
}

type eventLog struct{ events []inspection.Event }

func (l *eventLog) Observe(event inspection.Event) { l.events = append(l.events, event) }

// An observed run reports a construct as a step that starts before its
// arm's steps and completes after them, with its result.
func TestObservedRunReportsConstructsAroundTheirArms(t *testing.T) {
	program := controlProgram(
		compare("big", "gt", literal("150"), literal("100")),
		contract.InternalInstruction{ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{ref("big")}, Arms: []contract.Arm{
			{Name: "then", Instructions: []contract.InternalInstruction{{Index: 0, ID: "kept", Kind: "default", Control: &contract.Control{Operands: []contract.Operand{literal("null"), literal(`"vip"`)}}}}, Output: ptr(ref("kept"))},
			{Name: "else", Output: ptr(literal(`"standard"`))},
		}}},
		outputOf("route"),
	)
	log := &eventLog{}
	result, err := New(nil).WithObserver(log).RunObserved(context.Background(), program, nil, inspection.Invocation{RunID: "run-1", Principal: "p"})
	if err != nil || result.Output != "vip" {
		t.Fatalf("output=%v err=%v", result.Output, err)
	}
	var got []string
	for _, event := range log.events {
		got = append(got, string(event.Kind)+":"+event.StepID)
	}
	want := []string{"run.started:", "step.processing:big", "step.completed:big", "step.processing:route", "step.processing:kept", "step.completed:kept", "step.completed:route", "step.processing:output", "step.completed:output", "run.completed:"}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("events:\n got %v\nwant %v", got, want)
	}
	if paths := []string{result.Steps[1].InvocationPath, result.Steps[2].InvocationPath}; paths[0] != "route/then/kept" || paths[1] != "route" {
		t.Fatalf("paths=%v", paths)
	}
}
