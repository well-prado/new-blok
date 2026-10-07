package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
)

func each(id string, items contract.Operand, concurrency int, body ...contract.InternalInstruction) contract.InternalInstruction {
	for index := range body {
		body[index].Index = index
	}
	return contract.InternalInstruction{ID: id, Kind: "each", Control: &contract.Control{Operands: []contract.Operand{items}, Concurrency: concurrency, Arms: []contract.Arm{
		{Name: "body", Instructions: body, Output: ptr(ref(body[len(body)-1].ID))},
	}}}
}

// A step inside an each runs once per item: each run is its own inspection
// attempt, named by its iteration, so attempts never collide.
func TestEachIterationsAreSeparateInspectionAttempts(t *testing.T) {
	program := controlProgram(
		each("loop", literal("[1,2,3]"), 2, compare("big", "gt", ref("loop"), literal("1"))),
		outputOf("loop"),
	)
	log := &eventLog{}
	result, err := New(nil).WithObserver(log).RunObserved(context.Background(), program, nil, inspection.Invocation{RunID: "run-1", Principal: "p", AttemptID: "attempt:a"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := json.Marshal(result.Output); string(got) != "[false,true,true]" {
		t.Fatalf("output=%s", got)
	}
	attempts := map[string]int{}
	for _, event := range log.events {
		if event.StepID == "big" {
			attempts[event.AttemptID]++
		}
	}
	var ids []string
	for id, events := range attempts {
		if events != 2 {
			t.Errorf("attempt %s has %d events, want processing and completion", id, events)
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if want := "attempt:a/big@loop[0]/1 attempt:a/big@loop[1]/1 attempt:a/big@loop[2]/1"; strings.Join(ids, " ") != want {
		t.Fatalf("attempt ids=%v want %s", ids, want)
	}
	for _, event := range log.events {
		if event.StepID == "loop" && event.AttemptID != "attempt:a/loop/1" {
			t.Fatalf("the each itself is a root attempt: %s", event.AttemptID)
		}
	}
}

func TestEachItemsAreCheckedAndBounded(t *testing.T) {
	for name, tc := range map[string]struct {
		items contract.Operand
		code  string
		text  string
	}{
		"not an array": {literal(`{"a":1}`), "invalid_items", "each items are an object, not an array"},
		"null":         {literal(`null`), "invalid_items", "each items are null, not an array"},
		"too many":     {literal(`[1,2,3,4]`), "step_budget_exceeded", "each has 4 items; at most 3 run"},
	} {
		program := controlProgram(each("loop", tc.items, 1, compare("big", "gt", ref("loop"), literal("1"))), outputOf("loop"))
		_, err := New(nil).WithMaxSteps(3).Run(context.Background(), program, nil)
		var classified *Error
		if !errors.As(err, &classified) || classified.Code != tc.code || classified.Step != "loop" || !strings.Contains(err.Error(), tc.text) {
			t.Errorf("%s: err=%v; want %s containing %q", name, err, tc.code, tc.text)
		}
	}
	if result, err := New(nil).Run(context.Background(), controlProgram(each("loop", literal(`[]`), 1, compare("big", "gt", ref("loop"), literal("1"))), outputOf("loop")), nil); err != nil || len(result.Output.([]any)) != 0 {
		t.Fatalf("empty each: output=%v err=%v", result.Output, err)
	}
}

func TestEachAndParallelShapesAreChecked(t *testing.T) {
	leaf := compare("big", "eq", literal("1"), literal("1"))
	parallel := func(arms ...contract.Arm) contract.InternalInstruction {
		return contract.InternalInstruction{ID: "fan", Kind: "parallel", Control: &contract.Control{Arms: arms}}
	}
	cases := map[string]struct {
		instruction contract.InternalInstruction
		text        string
	}{
		"each without concurrency": {each("loop", literal("[1]"), 0, leaf), "each has concurrency 0; only an each has one, from 1 to 1024"},
		"each over the bound":      {each("loop", literal("[1]"), 1025, leaf), "each has concurrency 1025"},
		"concurrency elsewhere":    {contract.InternalInstruction{ID: "big", Kind: "compare", Control: &contract.Control{Operator: "eq", Operands: []contract.Operand{literal("1"), literal("1")}, Concurrency: 2}}, "compare has concurrency 2"},
		"parallel without arms":    {parallel(), "parallel needs at least one arm"},
		"parallel arm with output": {parallel(contract.Arm{Name: "0", Output: ptr(literal("1"))}), "only finally and parallel arms return no result"},
		"parallel arms misnamed":   {parallel(contract.Arm{Name: "1"}), `arm 0 is named "1", want "0"`},
	}
	for name, tc := range cases {
		_, err := New(nil).Run(context.Background(), controlProgram(tc.instruction, outputOf("x")), nil)
		var classified *Error
		if !errors.As(err, &classified) || classified.Code != "invalid_control" || !strings.Contains(err.Error(), tc.text) {
			t.Errorf("%s: err=%v; want invalid_control containing %q", name, err, tc.text)
		}
	}
}
