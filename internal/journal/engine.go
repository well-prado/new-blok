package journal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/internal/engine"
)

// rootIteration is the iteration path of a step outside every loop. A
// step's iteration path is the engine's (engine.StepIdentity.Iteration),
// and its invocation path too: its instruction ID at the top level, its
// path through the constructs' arms inside one (ADR 0028).
const rootIteration = engine.RootIteration

// neverDue is the due time of a wait without a timeout: no timer claim
// ever fires it; only a signal does.
var neverDue = time.Unix(0, math.MaxInt64).UTC()

// ErrWaitCanceled: the engine reached a wait that was canceled.
var ErrWaitCanceled = errors.New("journal: wait was canceled")

// RunJournal is the engine's durable journal (engine.StepJournal and
// engine.WaitJournal) for one run, executed under the run lease token
// names (ADR 0027). Every commit it makes checks that lease in its own
// transaction, so an execution whose lease was taken over cannot commit.
// A wait's outcome read by Await is acknowledged in the transaction of the
// next commit (a step's result, or the run's end through CompleteRun or
// FailRun), never before: a crash in between leaves the wait fired, and
// the run listed for resumption, which replays to the same outcome.
type RunJournal struct {
	journal *Journal
	runID   string
	token   int64

	mu       sync.Mutex
	artifact string
	pending  []string
}

// ForRun returns the engine journal for runID under lease token, from
// ClaimDueWaits, PendingResumptions or TakeRunLease.
func (j *Journal) ForRun(runID string, token int64) *RunJournal {
	return &RunJournal{journal: j, runID: runID, token: token}
}

var (
	_ engine.StepJournal  = (*RunJournal)(nil)
	_ engine.WaitJournal  = (*RunJournal)(nil)
	_ engine.ScopeJournal = (*RunJournal)(nil)
)

// Permanent reports an error no retry of the run can fix: a conflict with
// what the run already recorded (ErrRequestConflict: another engine input,
// step input or wait plan), a canceled wait the run reached
// (ErrWaitCanceled), a step result over the bound (ErrStepResultLimit), a
// child run of another principal or one that would close a cycle
// (ErrChildPrincipalMismatch, ErrChildCycle; #372), a final recovery
// record the run would change, such as a canceled scope it reached again
// (ErrRecordFinal). A runner settles such a run as failed. A lost lease
// (another holder runs it) and storage faults are not permanent.
func Permanent(err error) bool {
	return errors.Is(err, ErrRequestConflict) || errors.Is(err, ErrWaitCanceled) || errors.Is(err, ErrStepResultLimit) ||
		errors.Is(err, ErrChildPrincipalMismatch) || errors.Is(err, ErrChildCycle) || errors.Is(err, ErrRecordFinal)
}

// uncertainStep marks an effect whose outcome is unknown; the engine fails
// the run as uncertain instead of invoking the effect again.
type uncertainStep struct{ err error }

func (u uncertainStep) Error() string   { return u.err.Error() }
func (u uncertainStep) Unwrap() error   { return u.err }
func (uncertainStep) IsUncertain() bool { return true }

func (r *RunJournal) VerifyRun(ctx context.Context, runID, artifact, inputDigest string) error {
	if runID != r.runID {
		return ErrRequestConflict
	}
	run, err := r.journal.Run(ctx, runID)
	if err != nil {
		return err
	}
	if run.State != runAccepted {
		return ErrRunNotActive
	}
	if run.ArtifactDigest != artifact {
		return ErrRequestConflict
	}
	// The run's engine input was fixed at admission or by its first
	// execution: the runner decodes the admitted input the same way every
	// time, so a later execution hands the engine the same encoding, and a
	// different one is a different input (a changed decoder, a wrong run).
	// Read first: only the first execution of a run admitted without an
	// engine input writes.
	var bound sql.NullString
	if err := r.journal.withRead(ctx, func(tx *sql.Tx) error {
		if err := r.journal.checkRunLease(ctx, tx, runID, r.token, false); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT engine_input_digest FROM journal_runs WHERE run_id = ?`, runID).Scan(&bound)
	}); err != nil {
		return err
	}
	if bound.Valid && bound.String != inputDigest {
		return ErrRequestConflict
	}
	if !bound.Valid {
		if err := r.journal.withTx(ctx, "run-verify", func(tx *sql.Tx) error {
			if err := r.fence(ctx, tx, true); err != nil {
				return err
			}
			result, err := tx.ExecContext(ctx, `UPDATE journal_runs SET engine_input_digest = ? WHERE run_id = ? AND (engine_input_digest IS NULL OR engine_input_digest = ?)`, inputDigest, runID, inputDigest)
			if err != nil {
				return err
			}
			changed, err := result.RowsAffected()
			if err != nil {
				return err
			}
			if changed != 1 {
				return ErrRequestConflict
			}
			return nil
		}); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.artifact = artifact
	r.mu.Unlock()
	return nil
}

func (r *RunJournal) operation(identity engine.StepIdentity) (OperationIdentity, error) {
	r.mu.Lock()
	artifact := r.artifact
	r.mu.Unlock()
	if identity.RunID != r.runID || artifact == "" || identity.ArtifactDigest != artifact || identity.StepID == "" || identity.OperationKey == "" {
		return OperationIdentity{}, ErrRequestConflict
	}
	invocation := identity.Invocation()
	if invocation != identity.StepID && !strings.HasSuffix(invocation, "/"+identity.StepID) {
		return OperationIdentity{}, ErrRequestConflict
	}
	return OperationIdentity{RunID: r.runID, ArtifactDigest: artifact, InvocationPath: invocation, IterationPath: identity.Iteration()}, nil
}

// fence checks, inside a write transaction, that this execution still
// holds the run lease and, when live is set, that the run has not ended:
// every RunJournal write runs under it.
func (r *RunJournal) fence(ctx context.Context, tx *sql.Tx, live bool) error {
	if err := r.journal.checkRunLease(ctx, tx, r.runID, r.token, false); err != nil || !live {
		return err
	}
	var state string
	if err := tx.QueryRowContext(ctx, `SELECT state FROM journal_runs WHERE run_id = ?`, r.runID).Scan(&state); err != nil {
		return err
	}
	if state != runAccepted {
		return ErrRunNotActive
	}
	return nil
}

func (r *RunJournal) Load(ctx context.Context, identity engine.StepIdentity) (json.RawMessage, bool, error) {
	operation, err := r.operation(identity)
	if err != nil {
		return nil, false, err
	}
	var state, attemptID string
	var result []byte
	var digest sql.NullString
	err = r.journal.withRead(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT state, current_attempt_id, result_json, input_digest FROM journal_operations WHERE operation_key = ?`, operation.Key()).Scan(&state, &attemptID, &result, &digest)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	// A step whose input resolved differently than when it was recorded is
	// not the same step execution.
	if digest.Valid && digest.String != identity.InputDigest {
		return nil, false, fmt.Errorf("%w: step %s was recorded for input %s, not %s", ErrRequestConflict, operation.InvocationPath, digest.String, identity.InputDigest)
	}
	switch state {
	case operationCommitted:
		return append(json.RawMessage(nil), result...), true, nil
	case operationUncertain:
		return nil, false, uncertainStep{ErrUncertain}
	case operationDispatched:
		// Only an effect is dispatched durably: its outcome is unknown.
		if err := r.journal.withTx(ctx, "effect-uncertain", func(tx *sql.Tx) error {
			if err := r.fence(ctx, tx, false); err != nil {
				return err
			}
			return r.journal.markUncertain(ctx, tx, operation.Key(), attemptID, "dispatch interrupted before its result was committed")
		}); err != nil {
			return nil, false, err
		}
		return nil, false, uncertainStep{ErrUncertain}
	default:
		return nil, false, nil
	}
}

// pureAttempt marks a call with no declared effects: nothing is journaled
// before it runs, and it is safe to run again until its result commits.
const pureAttempt = "pure"

func (r *RunJournal) Begin(ctx context.Context, identity engine.StepIdentity, input json.RawMessage, effects []string) (engine.StepAttempt, error) {
	operation, err := r.operation(identity)
	if err != nil {
		return engine.StepAttempt{}, err
	}
	if len(effects) == 0 {
		return engine.StepAttempt{Identity: identity, AttemptID: pureAttempt}, nil
	}
	if len(input) > MaxInspectionInputBytes {
		return engine.StepAttempt{}, ErrObservationLimit
	}
	var attempt Attempt
	err = r.journal.withTx(ctx, "effect-dispatch", func(tx *sql.Tx) error {
		if err := r.fence(ctx, tx, true); err != nil {
			return err
		}
		key := operation.Key()
		if _, err := r.journal.beginEffect(ctx, tx, EffectIntent{Identity: operation, Input: input}, key); err != nil {
			return err
		}
		if err := r.journal.bindInput(ctx, tx, key, identity.InputDigest); err != nil {
			return err
		}
		attempt, err = r.journal.startAttempt(ctx, tx, key)
		return err
	})
	if err != nil {
		return engine.StepAttempt{}, err
	}
	return engine.StepAttempt{Identity: identity, AttemptID: attempt.ID}, nil
}

// bindInput records the engine's input digest on a step's operation, or
// refuses one recorded for another input.
func (j *Journal) bindInput(ctx context.Context, tx *sql.Tx, key, digest string) error {
	var stored sql.NullString
	if err := tx.QueryRowContext(ctx, `SELECT input_digest FROM journal_operations WHERE operation_key = ?`, key).Scan(&stored); err != nil {
		return err
	}
	if stored.Valid {
		if stored.String != digest {
			return ErrRequestConflict
		}
		return nil
	}
	_, err := tx.ExecContext(ctx, `UPDATE journal_operations SET input_digest = ? WHERE operation_key = ?`, digest, key)
	return err
}

// MaxStepResultBytes bounds a step result the engine journal commits, as
// a run's output is bounded.
const MaxStepResultBytes = MaxRunOutputBytes

// ErrStepResultLimit: a step result over MaxStepResultBytes.
var ErrStepResultLimit = errors.New("journal: step result exceeds the hard byte limit")

// Complete commits a step's result, and acknowledges the waits whose
// outcome led to it, in one transaction under the run lease.
func (r *RunJournal) Complete(ctx context.Context, attempt engine.StepAttempt, output json.RawMessage) error {
	operation, err := r.operation(attempt.Identity)
	if err != nil {
		return err
	}
	if len(output) > MaxStepResultBytes {
		return ErrStepResultLimit
	}
	return r.settle(ctx, "step-commit", true, func(tx *sql.Tx) error {
		if attempt.AttemptID == pureAttempt {
			return r.journal.commitPure(ctx, tx, operation, attempt.Identity.InputDigest, output)
		}
		if err := r.journal.bindInput(ctx, tx, operation.Key(), attempt.Identity.InputDigest); err != nil {
			return err
		}
		return r.journal.commitEffect(ctx, tx, EffectCommit{OperationKey: operation.Key(), AttemptID: attempt.AttemptID, Result: output})
	})
}

func (r *RunJournal) Fail(ctx context.Context, attempt engine.StepAttempt, effects []string, cause error) error {
	if attempt.AttemptID == pureAttempt {
		return nil
	}
	operation, err := r.operation(attempt.Identity)
	if err != nil {
		return err
	}
	return r.journal.withTx(ctx, "effect-uncertain", func(tx *sql.Tx) error {
		if err := r.fence(ctx, tx, false); err != nil {
			return err
		}
		return r.journal.markUncertain(ctx, tx, operation.Key(), attempt.AttemptID, "node failed after dispatch")
	})
}

// waitID is a wait's ID: a digest of the run, the artifact, the step, the
// engine's digest of the wait plan (its name and timeout) and the
// iteration, in an explicit encoding of the journal's own, so a changed
// plan at the same step and iteration is ErrRequestConflict and nothing
// but these values moves it.
func waitID(operation OperationIdentity, identity engine.StepIdentity) string {
	key := sha256.Sum256([]byte(strings.Join([]string{"wait/v1", operation.RunID, operation.ArtifactDigest, operation.InvocationPath, identity.InputDigest, operation.IterationPath}, "\x00")))
	return "wait:" + hex.EncodeToString(key[:])
}

// Await schedules the step's wait the first time the engine reaches it, and
// on every replay reads that same wait (see waitID). Scheduling a wait, or
// finding the run still waiting at it, is durable progress: the outcomes of
// the waits read since the last commit are acknowledged in that
// transaction, so a run that waits twice in a row is not woken again for
// the first wait while it waits at the second.
func (r *RunJournal) Await(ctx context.Context, identity engine.WaitIdentity) (engine.WaitResult, bool, error) {
	operation, err := r.operation(identity.Step)
	if err != nil || identity.Name == "" || identity.TimeoutMillis < 0 || identity.Step.InputDigest == "" {
		return engine.WaitResult{}, false, errors.Join(ErrRequestConflict, err)
	}
	id := waitID(operation, identity.Step)
	record, err := r.journal.WaitAt(ctx, r.runID, operation.InvocationPath, operation.IterationPath)
	if errors.Is(err, ErrNotFound) {
		due := neverDue
		if identity.TimeoutMillis > 0 {
			due = r.journal.clock().UTC().Add(time.Duration(identity.TimeoutMillis) * time.Millisecond)
		}
		request := WaitRequest{RunID: r.runID, WaitID: id, Name: identity.Name, InvocationPath: operation.InvocationPath, IterationPath: operation.IterationPath, DueAt: due}
		err = r.settle(ctx, "wait-schedule", true, func(tx *sql.Tx) error {
			var err error
			record, err = r.journal.scheduleWait(ctx, tx, request)
			return err
		})
		if errors.Is(err, ErrWaitExists) {
			record, err = r.journal.WaitAt(ctx, r.runID, operation.InvocationPath, operation.IterationPath)
		}
	}
	if err != nil {
		return engine.WaitResult{}, false, err
	}
	if record.WaitID != id || record.Name != identity.Name {
		return engine.WaitResult{}, false, fmt.Errorf("%w: step %s waits on %q as %s", ErrRequestConflict, operation.InvocationPath, record.Name, record.WaitID)
	}
	result := engine.WaitResult{SignalID: record.SignalID, TimedOut: record.SignalID == ""}
	if record.SignalID != "" {
		result.Payload = append(json.RawMessage(nil), record.Payload...)
	}
	switch record.State {
	case waitWaiting:
		// Still waiting (a replay after a crash, or a run that waits right
		// after another wait): consume what was read before suspending.
		r.mu.Lock()
		read := len(r.pending) > 0
		r.mu.Unlock()
		if read {
			if err := r.settle(ctx, "wait-acknowledge", true, func(*sql.Tx) error { return nil }); err != nil {
				return engine.WaitResult{}, false, err
			}
		}
		return engine.WaitResult{}, false, nil
	case waitFired:
		r.mu.Lock()
		r.pending = append(r.pending, record.WaitID)
		r.mu.Unlock()
		return result, true, nil
	case waitAcknowledged:
		return result, true, nil
	default:
		return engine.WaitResult{}, false, ErrWaitCanceled
	}
}

// MarkRunUncertain ends the run as uncertain, as CompleteRun completes it:
// under the run lease, acknowledging the waits read since the last commit.
func (r *RunJournal) MarkRunUncertain(ctx context.Context, errorCode, errorClass string) error {
	if !validDiagnosticLabel(errorCode) || !validDiagnosticLabel(errorClass) {
		return errors.New("journal: safe uncertainty code/class are required")
	}
	return r.settle(ctx, "run-uncertain", false, func(tx *sql.Tx) error {
		return r.journal.markRunUncertain(ctx, tx, r.runID, errorCode, errorClass)
	})
}

// CompleteRun completes the run and acknowledges the waits whose outcome
// the engine read since its last commit, in one transaction under the run
// lease.
func (r *RunJournal) CompleteRun(ctx context.Context, output json.RawMessage) error {
	if len(output) > MaxRunOutputBytes {
		return ErrRunOutputLimit
	}
	if !json.Valid(output) {
		return errors.New("journal: valid run output is required")
	}
	return r.settle(ctx, "run-complete", false, func(tx *sql.Tx) error {
		return r.journal.completeRun(ctx, tx, r.runID, output)
	})
}

// FailRun fails the run as CompleteRun completes it. The scopes the
// failure unwound through (the constructs it failed in, which never exit)
// end canceled, with errorCode as their error, in the same transaction.
func (r *RunJournal) FailRun(ctx context.Context, errorCode, errorClass string) error {
	if !validDiagnosticLabel(errorCode) || !validDiagnosticLabel(errorClass) {
		return errors.New("journal: safe failure code/class are required")
	}
	return r.settle(ctx, "run-fail", false, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE journal_scopes SET state = ?, error_text = ?, updated_at = ? WHERE run_id = ? AND state = ?`, checkpointCanceled, errorCode, r.journal.now(), r.runID, checkpointRunning); err != nil {
			return err
		}
		return r.journal.failRun(ctx, tx, r.runID, errorCode, errorClass)
	})
}

// EnterScope enters a construct's scope under the run lease (see
// engine.ScopeJournal): the first entry writes it, running, with decision
// as its input; a later one (a replay) starts a new attempt of a running
// scope and returns the decision recorded first, whatever decision it
// brings. A completed scope comes back Completed. A canceled scope is
// ErrRecordFinal, and a scope recorded with another kind or parent is
// ErrRequestConflict.
func (r *RunJournal) EnterScope(ctx context.Context, scope engine.ScopeIdentity, decision json.RawMessage) (engine.ScopeEntry, error) {
	if err := r.checkScope(scope); err != nil {
		return engine.ScopeEntry{}, err
	}
	if len(decision) > MaxInspectionInputBytes {
		return engine.ScopeEntry{}, ErrObservationLimit
	}
	if len(decision) > 0 && !json.Valid(decision) {
		return engine.ScopeEntry{}, errors.New("journal: a scope decision must be valid JSON")
	}
	attemptID, err := randomID("scope-attempt")
	if err != nil {
		return engine.ScopeEntry{}, err
	}
	entry := engine.ScopeEntry{Scope: scope}
	err = r.settle(ctx, "scope-enter", true, func(tx *sql.Tx) error {
		var kind, parent, state string
		var input []byte
		queryErr := tx.QueryRowContext(ctx, `SELECT kind, parent_path, state, input_json FROM journal_scopes WHERE run_id = ? AND path = ?`, r.runID, scope.Path).Scan(&kind, &parent, &state, &input)
		if errors.Is(queryErr, sql.ErrNoRows) {
			entry.AttemptID, entry.Decision = attemptID, append(json.RawMessage(nil), decision...)
			return r.journal.insertScope(ctx, tx, ScopeRecord{RunID: r.runID, Path: scope.Path, Kind: scope.Kind, ParentPath: scope.ParentPath, Input: decision}, attemptID)
		}
		if queryErr != nil {
			return queryErr
		}
		if kind != scope.Kind || parent != scope.ParentPath {
			return fmt.Errorf("%w: scope %s was recorded as a %s in %q, not a %s in %q", ErrRequestConflict, scope.Path, kind, parent, scope.Kind, scope.ParentPath)
		}
		entry.Decision = append(json.RawMessage(nil), input...)
		switch state {
		case checkpointCompleted:
			entry.Completed = true
			return nil
		case checkpointRunning:
			entry.AttemptID = attemptID
			_, err := tx.ExecContext(ctx, `UPDATE journal_scopes SET attempt_id = ?, updated_at = ? WHERE run_id = ? AND path = ?`, attemptID, r.journal.now(), r.runID, scope.Path)
			return err
		default:
			return fmt.Errorf("%w: scope %s is %s", ErrRecordFinal, scope.Path, state)
		}
	})
	if err != nil {
		return engine.ScopeEntry{}, err
	}
	return entry, nil
}

// ExitScope commits a construct's result to the scope entry entered,
// under the run lease; only the attempt that entered it last may (see
// CompleteScope). A result over MaxStepResultBytes is ErrStepResultLimit.
func (r *RunJournal) ExitScope(ctx context.Context, entry engine.ScopeEntry, output json.RawMessage) error {
	if err := r.checkScope(entry.Scope); err != nil {
		return err
	}
	if len(output) > MaxStepResultBytes {
		return ErrStepResultLimit
	}
	if !json.Valid(output) {
		return errors.New("journal: valid scope output is required")
	}
	if entry.AttemptID == "" {
		return ErrStaleAttempt
	}
	return r.settle(ctx, "scope-exit", true, func(tx *sql.Tx) error {
		return r.journal.completeScope(ctx, tx, r.runID, entry.Scope.Path, entry.AttemptID, output)
	})
}

// checkScope refuses a scope of another run or artifact than this
// execution verified, or without a path or kind.
func (r *RunJournal) checkScope(scope engine.ScopeIdentity) error {
	r.mu.Lock()
	artifact := r.artifact
	r.mu.Unlock()
	if scope.RunID != r.runID || artifact == "" || scope.ArtifactDigest != artifact || scope.Path == "" || scope.Kind == "" {
		return ErrRequestConflict
	}
	return nil
}

// settle runs commit in a transaction that first fences it (see fence)
// and acknowledges the pending waits; they stay pending if it fails. The
// run's end checks its state itself, so it is not fenced as live.
func (r *RunJournal) settle(ctx context.Context, name string, live bool, commit func(*sql.Tx) error) error {
	r.mu.Lock()
	pending := append([]string(nil), r.pending...)
	r.mu.Unlock()
	err := r.journal.withTx(ctx, name, func(tx *sql.Tx) error {
		if err := r.fence(ctx, tx, live); err != nil {
			return err
		}
		if err := r.journal.acknowledgeWaits(ctx, tx, r.runID, r.token, pending); err != nil {
			return err
		}
		return commit(tx)
	})
	if err == nil {
		r.mu.Lock()
		r.pending = r.pending[len(pending):]
		r.mu.Unlock()
	}
	return err
}

// commitPure records a call with no effects as committed in one step: it
// was never dispatched durably, because running it again is safe.
func (j *Journal) commitPure(ctx context.Context, tx *sql.Tx, identity OperationIdentity, inputDigest string, output json.RawMessage) error {
	if !json.Valid(output) {
		return errors.New("journal: valid step result is required")
	}
	key := identity.Key()
	result, err := tx.ExecContext(ctx, `INSERT INTO journal_operations (operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, result_json, input_digest, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(operation_key) DO NOTHING`,
		key, identity.RunID, identity.ArtifactDigest, identity.InvocationPath, identity.IterationPath, key, operationCommitted, []byte(output), inputDigest, j.now(), j.now())
	if err != nil {
		return err
	}
	if inserted, err := result.RowsAffected(); err != nil || inserted == 0 {
		if err == nil {
			err = ErrStaleAttempt
		}
		return err
	}
	attemptID, err := randomID("attempt")
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE journal_operations SET current_attempt_id = ? WHERE operation_key = ?`, attemptID, key); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO journal_attempts (attempt_id, operation_key, attempt_number, provider_operation_key, state, result_json, started_at, finished_at) VALUES (?, ?, 1, ?, ?, ?, ?, ?)`, attemptID, key, key, attemptCommitted, []byte(output), j.now(), j.now())
	return err
}
