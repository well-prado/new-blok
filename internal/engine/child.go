package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/well-prado/new-blok/contract"
)

// ChildWaitPrefix begins the name of the wait a parent run suspends at
// while its child runs: ChildWaitPrefix + the child's run id. A journal
// refuses signals addressed to such a name: only the child's end fires it.
const ChildWaitPrefix = "blok.child:"

// ChildJournal starts a child run for a parent's child step (ADR 0028,
// #333 slice 4). StartChild, in one transaction under the parent's run
// lease: the first time, admits the child run (an id derived from the
// parent run and the step's scope path, the parent's principal, input as
// its admitted input), records it as the parent's child, enters the
// step's scope with the child's id as its decision, and schedules the
// parent's wait for it (Step identifies the wait, named
// ChildWaitPrefix+id); every later time (a replay), it returns the child
// recorded in the scope and admits nothing. The child's end fires that
// wait with the child's outcome (ChildOutcome). A refusal no retry can fix
// is a *ChildError.
type ChildJournal interface {
	ScopeJournal
	WaitJournal
	StartChild(context.Context, ChildRequest) (ChildStart, error)
}

// ChildRequest is a child step reaching StartChild.
type ChildRequest struct {
	Scope    ScopeIdentity
	Step     StepIdentity
	Workflow string
	Input    json.RawMessage
}

// ChildStart is the child a child step started (or started before).
type ChildStart struct {
	RunID string
	Entry ScopeEntry
}

// ChildOutcome is how a child run ended, the payload of its parent's wait:
// State is completed (with Output), failed or uncertain (with Code and
// Class), or canceled.
type ChildOutcome struct {
	State  string          `json:"state"`
	Output json.RawMessage `json:"output,omitempty"`
	Code   string          `json:"code,omitempty"`
	Class  string          `json:"class,omitempty"`
}

// ChildError is a child start no retry can fix: child_depth_exceeded,
// child_unavailable, child_workflow_unknown or child_input_invalid. The
// engine reports it under its own code and class.
type ChildError struct {
	Code, Class string
	Err         error
}

func (e *ChildError) Error() string { return e.Err.Error() }
func (e *ChildError) Unwrap() error { return e.Err }

// runChild runs a child step durably: it starts (or finds) the child, then
// waits at the child's wait; once the child has ended its outcome is the
// step's result or failure.
func runChild(ctx context.Context, children ChildJournal, runID, digest string, f *frame, instruction contract.InternalInstruction, input any) (any, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, &Error{Code: "journal_input_encode", Class: "persistence", Step: instruction.ID, Err: err}
	}
	plan, _ := json.Marshal(struct {
		Child string `json:"child"`
	}{instruction.Node})
	identity := f.stepIdentity(runID, digest, instruction.ID, plan)
	scope := ScopeIdentity{RunID: runID, ArtifactDigest: digest, Path: f.scopePath(instruction.ID), ParentPath: f.scope, Kind: "child"}
	started, err := children.StartChild(ctx, ChildRequest{Scope: scope, Step: identity, Workflow: instruction.Node, Input: encoded})
	if err != nil {
		var refused *ChildError
		if errors.As(err, &refused) {
			return nil, &Error{Code: refused.Code, Class: refused.Class, Step: instruction.ID, Err: err}
		}
		return nil, journalFailure("journal_child_start", instruction.ID, err, nil)
	}
	result, ready, err := children.Await(ctx, WaitIdentity{Step: identity, Name: ChildWaitPrefix + started.RunID})
	if err != nil {
		return nil, journalFailure("journal_wait", instruction.ID, err, nil)
	}
	if !ready {
		return nil, &Error{Code: "run_suspended", Class: "waiting", Step: instruction.ID, Suspended: true}
	}
	var outcome ChildOutcome
	if err := json.Unmarshal(result.Payload, &outcome); err != nil {
		return nil, &Error{Code: "journal_child_outcome", Class: "persistence", Step: instruction.ID, Err: err}
	}
	switch outcome.State {
	case "completed":
		output, err := decodeLiteral(outcome.Output)
		if err != nil {
			return nil, &Error{Code: "journal_child_outcome", Class: "persistence", Step: instruction.ID, Err: err}
		}
		if !started.Entry.Completed {
			summary, _ := json.Marshal(map[string]string{"child": started.RunID})
			if err := children.ExitScope(ctx, started.Entry, summary); err != nil {
				return nil, journalFailure("journal_scope_exit", instruction.ID, err, nil)
			}
		}
		return output, nil
	case "failed":
		return nil, &Error{Code: "child_failed", Class: "failure", Step: instruction.ID, Err: fmt.Errorf("child run %s failed: %s (%s)", started.RunID, outcome.Code, outcome.Class)}
	case "uncertain":
		return nil, &Error{Code: "child_uncertain", Class: "uncertain", Step: instruction.ID, Uncertain: true, Err: fmt.Errorf("child run %s ended uncertain: %s (%s)", started.RunID, outcome.Code, outcome.Class)}
	case "canceled":
		return nil, &Error{Code: "child_canceled", Class: "failure", Step: instruction.ID, Err: fmt.Errorf("child run %s was canceled", started.RunID)}
	}
	return nil, &Error{Code: "journal_child_outcome", Class: "persistence", Step: instruction.ID, Err: fmt.Errorf("child run %s ended %q", started.RunID, outcome.State)}
}

// hasChild reports a child instruction in instructions, at any depth.
func hasChild(instructions []contract.InternalInstruction) bool {
	for _, instruction := range instructions {
		if instruction.Kind == "child" {
			return true
		}
		if instruction.Control != nil {
			for _, arm := range instruction.Control.Arms {
				if hasChild(arm.Instructions) {
					return true
				}
			}
		}
	}
	return false
}
