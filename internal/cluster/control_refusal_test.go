package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
)

// TestClusterRefusesControlPrograms: the cluster's step journal journals
// no scopes (#396 comes first), so a control program run through it is
// refused before the journal is touched, even the if, choose and
// try-finally a single host now runs durably (#333 slice 2).
func TestClusterRefusesControlPrograms(t *testing.T) {
	if _, ok := any(&runStepJournal{}).(engine.ScopeJournal); ok {
		t.Fatal("the cluster step journal journals scopes; #333 must lift the cluster refusal deliberately")
	}
	if _, ok := any(&inspectionJournal{}).(engine.ScopeJournal); ok {
		t.Fatal("the cluster inspection journal journals scopes")
	}
	program := contract.InternalProgram{WorkflowID: "control", Digest: "sha256:0000000000000000000000000000000000000000000000000000000000000396", Format: contract.ControlFormat, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{{Literal: json.RawMessage(`true`)}}, Arms: []contract.Arm{
			{Name: "then", Output: &contract.Operand{Literal: json.RawMessage(`1`)}},
			{Name: "else", Output: &contract.Operand{Literal: json.RawMessage(`2`)}},
		}}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "route"}}},
	}}
	// A zero journal: the refusal must come before VerifyRun, which would
	// dereference its runtime.
	_, err := engine.New(nil).RunJournaled(context.Background(), program, nil, "run:396", &runStepJournal{})
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "durable_control_unsupported" {
		t.Fatalf("err=%v; want durable_control_unsupported", err)
	}
}
