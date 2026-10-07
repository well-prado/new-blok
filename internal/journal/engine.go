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
	"sync"
	"time"

	"github.com/well-prado/new-blok/internal/engine"
)

// rootIteration is the iteration path of a step outside every loop. Until
// #333 gives the engine iteration paths, every step of a run is at the
// root, and a step's invocation path is its instruction ID (ADR 0031).
const rootIteration = "root"

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
	_ engine.StepJournal = (*RunJournal)(nil)
	_ engine.WaitJournal = (*RunJournal)(nil)
)

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
	// The engine digests its input re-encoded; compare the same encoding.
	var input any
	if err := json.Unmarshal(run.Input, &input); err != nil {
		return err
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		return err
	}
	if run.ArtifactDigest != artifact || digestBytes(canonical) != inputDigest {
		return ErrRequestConflict
	}
	if err := r.journal.withRead(ctx, func(tx *sql.Tx) error {
		return r.journal.checkRunLease(ctx, tx, runID, r.token, false)
	}); err != nil {
		return err
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
	return OperationIdentity{RunID: r.runID, ArtifactDigest: artifact, InvocationPath: identity.StepID, IterationPath: rootIteration}, nil
}

func (r *RunJournal) Load(ctx context.Context, identity engine.StepIdentity) (json.RawMessage, bool, error) {
	operation, err := r.operation(identity)
	if err != nil {
		return nil, false, err
	}
	stored, err := r.journal.Operation(ctx, operation.Key())
	if errors.Is(err, ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	switch stored.State {
	case operationCommitted:
		return append(json.RawMessage(nil), stored.Result...), true, nil
	case operationUncertain:
		return nil, false, uncertainStep{ErrUncertain}
	case operationDispatched:
		// Only an effect is dispatched durably: its outcome is unknown.
		if err := r.journal.MarkUncertain(ctx, stored.Key, stored.CurrentAttemptID, "dispatch interrupted before its result was committed"); err != nil {
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
	if err := r.journal.withRead(ctx, func(tx *sql.Tx) error {
		return r.journal.checkRunLease(ctx, tx, r.runID, r.token, false)
	}); err != nil {
		return engine.StepAttempt{}, err
	}
	stored, err := r.journal.BeginEffect(ctx, EffectIntent{Identity: operation, Input: input})
	if err != nil {
		return engine.StepAttempt{}, err
	}
	attempt, err := r.journal.StartAttempt(ctx, stored.Key)
	if err != nil {
		return engine.StepAttempt{}, err
	}
	return engine.StepAttempt{Identity: identity, AttemptID: attempt.ID}, nil
}

// Complete commits a step's result, and acknowledges the waits whose
// outcome led to it, in one transaction under the run lease.
func (r *RunJournal) Complete(ctx context.Context, attempt engine.StepAttempt, output json.RawMessage) error {
	operation, err := r.operation(attempt.Identity)
	if err != nil {
		return err
	}
	return r.settle(ctx, "step-commit", func(tx *sql.Tx) error {
		if attempt.AttemptID == pureAttempt {
			return r.journal.commitPure(ctx, tx, operation, output)
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
	return r.journal.MarkUncertain(ctx, operation.Key(), attempt.AttemptID, "node failed after dispatch")
}

// Await schedules the step's wait the first time the engine reaches it, and
// on every replay reads that same wait. Its ID is derived from the engine's
// operation key, which covers the step and the wait's name and timeout, and
// from the iteration path, so a changed plan at the same step and
// iteration is ErrRequestConflict.
func (r *RunJournal) Await(ctx context.Context, identity engine.WaitIdentity) (engine.WaitResult, bool, error) {
	operation, err := r.operation(identity.Step)
	if err != nil || identity.Name == "" || identity.TimeoutMillis < 0 {
		return engine.WaitResult{}, false, errors.Join(ErrRequestConflict, err)
	}
	key := sha256.Sum256([]byte(identity.Step.OperationKey + "\x00" + operation.IterationPath))
	waitID := "wait:" + hex.EncodeToString(key[:])
	record, err := r.journal.WaitAt(ctx, r.runID, operation.InvocationPath, operation.IterationPath)
	if errors.Is(err, ErrNotFound) {
		due := neverDue
		if identity.TimeoutMillis > 0 {
			due = r.journal.clock().UTC().Add(time.Duration(identity.TimeoutMillis) * time.Millisecond)
		}
		record, err = r.journal.ScheduleWait(ctx, WaitRequest{RunID: r.runID, WaitID: waitID, Name: identity.Name, InvocationPath: operation.InvocationPath, IterationPath: operation.IterationPath, DueAt: due})
		if errors.Is(err, ErrWaitExists) {
			record, err = r.journal.WaitAt(ctx, r.runID, operation.InvocationPath, operation.IterationPath)
		}
	}
	if err != nil {
		return engine.WaitResult{}, false, err
	}
	if record.WaitID != waitID || record.Name != identity.Name {
		return engine.WaitResult{}, false, fmt.Errorf("%w: step %s waits on %q as %s", ErrRequestConflict, operation.InvocationPath, record.Name, record.WaitID)
	}
	result := engine.WaitResult{SignalID: record.SignalID, TimedOut: record.SignalID == ""}
	if record.SignalID != "" {
		result.Payload = append(json.RawMessage(nil), record.Payload...)
	}
	switch record.State {
	case waitWaiting:
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
	return r.settle(ctx, "run-complete", func(tx *sql.Tx) error {
		return r.journal.completeRun(ctx, tx, r.runID, output)
	})
}

// FailRun fails the run as CompleteRun completes it.
func (r *RunJournal) FailRun(ctx context.Context, errorCode, errorClass string) error {
	if !validDiagnosticLabel(errorCode) || !validDiagnosticLabel(errorClass) {
		return errors.New("journal: safe failure code/class are required")
	}
	return r.settle(ctx, "run-fail", func(tx *sql.Tx) error {
		return r.journal.failRun(ctx, tx, r.runID, errorCode, errorClass)
	})
}

// settle runs commit in a transaction that first checks the run lease and
// acknowledges the pending waits; they stay pending if it fails.
func (r *RunJournal) settle(ctx context.Context, name string, commit func(*sql.Tx) error) error {
	r.mu.Lock()
	pending := append([]string(nil), r.pending...)
	r.mu.Unlock()
	err := r.journal.withTx(ctx, name, func(tx *sql.Tx) error {
		if err := r.journal.checkRunLease(ctx, tx, r.runID, r.token, false); err != nil {
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
func (j *Journal) commitPure(ctx context.Context, tx *sql.Tx, identity OperationIdentity, output json.RawMessage) error {
	if !json.Valid(output) {
		return errors.New("journal: valid step result is required")
	}
	key := identity.Key()
	result, err := tx.ExecContext(ctx, `INSERT INTO journal_operations (operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, result_json, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(operation_key) DO NOTHING`,
		key, identity.RunID, identity.ArtifactDigest, identity.InvocationPath, identity.IterationPath, key, operationCommitted, []byte(output), j.now(), j.now())
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
