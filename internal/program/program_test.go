package program

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
)

func inputProgram() contract.InternalProgram {
	return contract.InternalProgram{
		WorkflowID: "quote",
		Version:    "1.0.0",
		Digest:     "sha256:" + strings.Repeat("0", 64),
		Instructions: []contract.InternalInstruction{
			{Index: 0, ID: "calculate", Kind: "call", Node: "quote-node"},
			{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
		},
	}
}

func TestProgramDigestAndDecodeAreDeterministic(t *testing.T) {
	first, err := Build(inputProgram(), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Build(inputProgram(), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	if first.Digest() != second.Digest() || first.Digest() == "" {
		t.Fatalf("digests differ: %s %s", first.Digest(), second.Digest())
	}
	data, err := first.JSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Digest() != first.Digest() || len(decoded.Instructions()) != 2 {
		t.Fatalf("decoded=%+v", decoded)
	}
}

func TestProgramRejectsUnsupportedVersionOpcodeAndCycles(t *testing.T) {
	program, err := Build(inputProgram(), Limits{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := program.JSON()
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	value["version"] = 99
	mutated, _ := json.Marshal(value)
	if _, err := Decode(mutated); err == nil || !strings.Contains(err.Error(), "unsupported_program_version") {
		t.Fatalf("got %v", err)
	}
	cycle := inputProgram()
	cycle.Instructions[0].Kind = "teleport"
	if _, err := Build(cycle, Limits{}); err == nil || !strings.Contains(err.Error(), "unknown_opcode") {
		t.Fatalf("got %v", err)
	}
	cycle = inputProgram()
	cycle.Instructions[0].References = []contract.Reference{{Step: "respond"}}
	if _, err := Build(cycle, Limits{}); err == nil || !strings.Contains(err.Error(), "cycle_or_future_reference") {
		t.Fatalf("got %v", err)
	}
}

func TestProgramEnforcesInstructionBudget(t *testing.T) {
	if _, err := Build(inputProgram(), Limits{MaxSteps: 1, MaxDepth: 1}); err == nil || !strings.Contains(err.Error(), "program_limits") {
		t.Fatalf("got %v", err)
	}
	if _, err := Build(inputProgram(), Limits{MaxSteps: 10, MaxDepth: 1}); err == nil || !strings.Contains(err.Error(), "dependency depth") {
		t.Fatalf("got %v", err)
	}
}
