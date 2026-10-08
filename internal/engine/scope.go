package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/well-prado/new-blok/contract"
)

// scopeDecision is the decision a scope records (ScopeJournal): the arm an
// if or a choose selected. A try-finally records none.
type scopeDecision struct {
	Arm string `json:"arm"`
}

// enterScope enters instruction's scope with decision (an arm name, empty
// for a try-finally) and returns the entry and the arm the run follows:
// the recorded one, which is decision the first time. A recorded decision
// that names no arm of instruction is journal_scope_conflict.
func enterScope(ctx context.Context, scopes ScopeJournal, identity ScopeIdentity, instruction contract.InternalInstruction, decision string) (ScopeEntry, string, error) {
	var encoded json.RawMessage
	if decision != "" {
		encoded, _ = json.Marshal(scopeDecision{Arm: decision})
	}
	entry, err := scopes.EnterScope(ctx, identity, encoded)
	if err != nil {
		return ScopeEntry{}, "", journalFailure("journal_scope_enter", instruction.ID, err, nil)
	}
	if decision == "" {
		if len(entry.Decision) != 0 {
			return ScopeEntry{}, "", scopeConflict(instruction.ID, identity.Path, entry.Decision)
		}
		return entry, "", nil
	}
	var recorded scopeDecision
	if err := json.Unmarshal(entry.Decision, &recorded); err != nil {
		return ScopeEntry{}, "", scopeConflict(instruction.ID, identity.Path, entry.Decision)
	}
	for _, arm := range instruction.Control.Arms {
		if arm.Name == recorded.Arm {
			return entry, recorded.Arm, nil
		}
	}
	return ScopeEntry{}, "", scopeConflict(instruction.ID, identity.Path, entry.Decision)
}

func scopeConflict(step, path string, recorded json.RawMessage) error {
	return &Error{Code: "journal_scope_conflict", Class: "conflict", Step: step, Err: fmt.Errorf("scope %s recorded the decision %q, which this construct cannot follow", path, recorded)}
}

// exitScope commits the result of the construct step to its scope.
func exitScope(ctx context.Context, scopes ScopeJournal, entry ScopeEntry, step string, output any) error {
	encoded, err := json.Marshal(output)
	if err != nil {
		return &Error{Code: "journal_scope_encode", Class: "persistence", Step: step, Err: err}
	}
	if err := scopes.ExitScope(ctx, entry, encoded); err != nil {
		return journalFailure("journal_scope_exit", step, err, nil)
	}
	return nil
}

// unjournaledControl is the kind of the first construct in instructions,
// at any depth, that a durable runner cannot journal yet (each and
// parallel need joins, #333 slice 3), or "".
func unjournaledControl(instructions []contract.InternalInstruction) string {
	for _, instruction := range instructions {
		if instruction.Kind == "each" || instruction.Kind == "parallel" {
			return instruction.Kind
		}
		if instruction.Control != nil {
			for _, arm := range instruction.Control.Arms {
				if kind := unjournaledControl(arm.Instructions); kind != "" {
					return kind
				}
			}
		}
	}
	return ""
}
