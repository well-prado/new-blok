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

func TestProgramValidatesWaitMetadata(t *testing.T) {
	const maxTimeout = int64(365 * 24 * 60 * 60 * 1000)
	cases := []struct {
		name string
		kind string
		wait *contract.WaitInstruction
		code string
	}{
		{"missing metadata", "wait", nil, "invalid_wait"},
		{"blank name", "wait", &contract.WaitInstruction{}, "invalid_wait"},
		{"oversized name", "wait", &contract.WaitInstruction{Name: strings.Repeat("n", 181)}, "invalid_wait"},
		{"negative timeout", "wait", &contract.WaitInstruction{Name: "approval", TimeoutMillis: -1}, "invalid_wait"},
		{"timeout beyond bound", "wait", &contract.WaitInstruction{Name: "approval", TimeoutMillis: maxTimeout + 1}, "invalid_wait"},
		{"metadata on a call", "call", &contract.WaitInstruction{Name: "approval"}, "unexpected_wait"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input := inputProgram()
			input.Instructions[0].Kind = tc.kind
			input.Instructions[0].Wait = tc.wait
			if _, err := Build(input, Limits{}); err == nil || !strings.Contains(err.Error(), tc.code) {
				t.Fatalf("got %v, want %s", err, tc.code)
			}
		})
	}
	input := inputProgram()
	input.Instructions[0].Kind = "wait"
	input.Instructions[0].Node = ""
	input.Instructions[0].Wait = &contract.WaitInstruction{Name: strings.Repeat("n", 180), TimeoutMillis: maxTimeout}
	built, err := Build(input, Limits{})
	if err != nil {
		t.Fatal(err)
	}
	data, err := built.JSON()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	restored := decoded.Instructions()[0].Wait
	if restored == nil || restored.TimeoutMillis != maxTimeout {
		t.Fatalf("decoded wait=%+v", restored)
	}
	restored.Name = "changed"
	if decoded.Instructions()[0].Wait.Name == "changed" {
		t.Fatal("decoded program aliases its wait metadata")
	}
}
