package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/well-prado/new-blok/contract"
)

// ErrScopeConflict: a scope's recorded decision is one its construct
// cannot follow (it names no arm, or a try-finally recorded one). No retry
// can fix it; journal.Permanent reports it, so a runner settles the run
// as failed (journal_scope_conflict).
var ErrScopeConflict = errors.New("engine: a scope's recorded decision conflicts with its construct")

// scopeDecision is the decision a scope records (ScopeJournal): the arm an
// if or a choose selected; for an each or a parallel, its number of slots
// and, once it failed fast, that failure (LoopJournal.FailScope). A
// try-finally records none.
type scopeDecision struct {
	Arm    string           `json:"arm,omitempty"`
	Items  *int             `json:"items,omitempty"`
	Failed *recordedFailure `json:"failed,omitempty"`
}

// recordedFailure is the failure a loop recorded: what a replay reports.
type recordedFailure struct {
	Code      string `json:"code"`
	Class     string `json:"class"`
	Step      string `json:"step,omitempty"`
	Uncertain bool   `json:"uncertain,omitempty"`
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
	return &Error{Code: "journal_scope_conflict", Class: "conflict", Step: step, Err: fmt.Errorf("%w: scope %s recorded the decision %q, which this construct cannot follow", ErrScopeConflict, path, recorded)}
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

// loopScope journals one execution of an each or a parallel (ADR 0028,
// slice 3): its scope, whose decision is its number of slots, and one slot
// per item or arm. Items run concurrently, so slots is guarded.
type loopScope struct {
	journal  LoopJournal
	identity ScopeIdentity
	entry    ScopeEntry
	recorded map[string]json.RawMessage

	mu   sync.Mutex
	done []bool // which slots hold a result, by index
}

// begin enters the loop's scope for n slots and, when prefix is set (an
// each), reads the slots already recorded. A loop recorded for another
// number of slots is journal_scope_conflict; one whose failure was
// recorded fails again with it at once, starting no item.
func (l *loopScope) begin(ctx context.Context, step string, n int, prefix string) error {
	encoded, _ := json.Marshal(scopeDecision{Items: &n})
	entry, err := l.journal.EnterScope(ctx, l.identity, encoded)
	if err != nil {
		return journalFailure("journal_scope_enter", step, err, nil)
	}
	var recorded scopeDecision
	if json.Unmarshal(entry.Decision, &recorded) != nil || recorded.Items == nil || *recorded.Items != n || recorded.Arm != "" {
		return scopeConflict(step, l.identity.Path, entry.Decision)
	}
	if failed := recorded.Failed; failed != nil {
		return &Error{Code: failed.Code, Class: failed.Class, Step: failed.Step, Uncertain: failed.Uncertain, Err: fmt.Errorf("%s failed in an earlier execution of this run (recorded in scope %s)", step, l.identity.Path)}
	}
	l.entry, l.done = entry, make([]bool, n)
	if prefix != "" {
		if l.recorded, err = l.journal.Slots(ctx, entry, prefix); err != nil {
			return journalFailure("journal_scope_slots", step, err, nil)
		}
	}
	return nil
}

// filled returns the result recorded in the slot at path, as the JSON value
// a replay continues with, if the slot is recorded.
func (l *loopScope) filled(step string, index int, path string) (any, bool, error) {
	wrapped, ok := l.recorded[path]
	if !ok {
		return nil, false, nil
	}
	var slot map[string]json.RawMessage
	output, present := json.RawMessage(nil), false
	if json.Unmarshal(wrapped, &slot) == nil && len(slot) == 1 {
		output, present = slot["output"]
	}
	if !present {
		return nil, false, scopeConflict(step, path, wrapped)
	}
	value, err := decodeLiteral(output)
	if err != nil {
		return nil, false, scopeConflict(step, path, wrapped)
	}
	l.mu.Lock()
	l.done[index] = true
	l.mu.Unlock()
	return value, true, nil
}

// record writes the slot at path with output, the item's or arm's result,
// and returns the result as the JSON value it recorded, so a replay that
// reads the slot continues with the same value.
func (l *loopScope) record(ctx context.Context, step string, index int, path, kind string, output any) (any, error) {
	encoded, err := json.Marshal(output)
	if err != nil {
		return nil, &Error{Code: "journal_scope_encode", Class: "persistence", Step: step, Err: err}
	}
	wrapped := append(append([]byte(`{"output":`), encoded...), '}')
	slot := ScopeIdentity{RunID: l.identity.RunID, ArtifactDigest: l.identity.ArtifactDigest, Path: path, ParentPath: l.identity.Path, Kind: kind}
	if err := l.journal.RecordSlot(ctx, l.entry, slot, wrapped); err != nil {
		return nil, journalFailure("journal_scope_slot", step, err, nil)
	}
	l.mu.Lock()
	l.done[index] = true
	l.mu.Unlock()
	return decodeLiteral(encoded)
}

// exit completes the loop's scope once every slot is filled. Its output
// is a summary, {"items":N}: the loop's result lives in its slots, from
// which a run and inspection assemble it in index order; storing it again
// in the scope would bound a whole loop's results by one result's limit
// (#412 Review R round 1).
func (l *loopScope) exit(ctx context.Context, step string) error {
	if l.entry.Completed {
		return nil
	}
	for index, done := range l.done {
		if !done {
			return &Error{Code: "journal_scope_slots", Class: "persistence", Step: step, Err: fmt.Errorf("slot %d of %s has no result", index, l.identity.Path)}
		}
	}
	summary, _ := json.Marshal(scopeDecision{Items: ptrTo(len(l.done))})
	if err := l.journal.ExitScope(ctx, l.entry, summary); err != nil {
		return journalFailure("journal_scope_exit", step, err, nil)
	}
	return nil
}

func ptrTo(n int) *int { return &n }

// fail records the loop's failure as its decision when it is the run's
// own (fail-fast): not a suspension, the caller's cancellation or a
// journal fault, which a later execution may get past. If recording it
// fails, a replay runs the loop again.
func (l *loopScope) fail(ctx context.Context, failure error) {
	var classified *Error
	if !errors.As(failure, &classified) || classified.Suspended || classified.Class == "cancellation" || classified.Class == "persistence" ||
		errors.Is(failure, context.Canceled) || errors.Is(failure, context.DeadlineExceeded) || l.entry.AttemptID == "" {
		return
	}
	var recorded scopeDecision
	_ = json.Unmarshal(l.entry.Decision, &recorded)
	recorded.Failed = &recordedFailure{Code: classified.Code, Class: classified.Class, Step: classified.Step, Uncertain: classified.Uncertain}
	encoded, _ := json.Marshal(recorded)
	_ = l.journal.FailScope(ctx, l.entry, encoded)
}

// unjournaledControl is the kind of the first each or parallel in
// instructions, at any depth: a durable runner journals them only through
// a LoopJournal (#333 slice 3), or "".
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
