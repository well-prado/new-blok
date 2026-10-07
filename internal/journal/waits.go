package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
)

const (
	waitWaiting   = "waiting"
	waitResumed   = "resumed"
	waitCanceled  = "canceled"
	signalStored  = "stored"
	signalPending = "pending"
	signalLate    = "late"
)

var (
	ErrWaitExists   = errors.New("journal: wait already exists")
	ErrUnauthorized = errors.New("journal: signal is unauthorized")
	ErrLateSignal   = errors.New("journal: signal arrived after wait was closed")
)

// WaitRequest schedules one wait. InvocationPath and IterationPath identify
// the step that waits and its iteration, as they identify an effect
// (OperationIdentity): a run holds at most one wait per step and iteration,
// whatever its name, and may wait on the same name in successive iterations
// or steps (#332). Name is what a signal addresses.
type WaitRequest struct {
	RunID          string
	WaitID         string
	Name           string
	InvocationPath string
	IterationPath  string
	DueAt          time.Time
}

// WaitRecord is a stored wait. A wait written before #332 has empty paths.
type WaitRecord struct {
	WaitID         string
	RunID          string
	Name           string
	InvocationPath string
	IterationPath  string
	DueAt          time.Time
	State          string
	SignalID       string
	Payload        json.RawMessage
}

type SignalResult struct {
	Accepted  bool
	Duplicate bool
	Late      bool
	Resumed   bool
}

// ScheduleWait stores a waiting wait and, in the same transaction, hands it
// the pending signal of its name that arrived first. A wait whose ID, or whose step and
// iteration, the run already used returns ErrWaitExists and writes nothing.
func (j *Journal) ScheduleWait(ctx context.Context, request WaitRequest) (WaitRecord, error) {
	if request.RunID == "" || request.WaitID == "" || request.Name == "" || request.InvocationPath == "" || request.IterationPath == "" || request.DueAt.IsZero() {
		return WaitRecord{}, errors.New("journal: run, wait ID, name, invocation and iteration path, and due time are required")
	}
	var record WaitRecord
	err := j.withTx(ctx, "wait-schedule", func(tx *sql.Tx) error {
		// No conflict target: both the wait ID and the step identity are
		// uniqueness constraints, and either one is a duplicate.
		result, err := tx.ExecContext(ctx, `INSERT INTO journal_waits (wait_id, run_id, name, invocation_path, iteration_path, due_at, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`, request.WaitID, request.RunID, request.Name, request.InvocationPath, request.IterationPath, request.DueAt.UTC().UnixNano(), waitWaiting, j.now(), j.now())
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed == 0 {
			return ErrWaitExists
		}
		record = WaitRecord{WaitID: request.WaitID, RunID: request.RunID, Name: request.Name, InvocationPath: request.InvocationPath, IterationPath: request.IterationPath, DueAt: request.DueAt.UTC(), State: waitWaiting}
		var signalID string
		var payload []byte
		err = tx.QueryRowContext(ctx, `SELECT signal_id, payload_json FROM journal_signals WHERE run_id = ? AND name = ? AND state = ? ORDER BY rowid LIMIT 1`, request.RunID, request.Name, signalPending).Scan(&signalID, &payload)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_signals SET state = ? WHERE run_id = ? AND signal_id = ? AND state = ?`, signalStored, request.RunID, signalID, signalPending); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, signal_id = ?, payload_json = ?, updated_at = ? WHERE wait_id = ? AND state = ?`, waitResumed, signalID, payload, j.now(), request.WaitID, waitWaiting); err != nil {
			return err
		}
		record.State = waitResumed
		record.SignalID = signalID
		record.Payload = append([]byte(nil), payload...)
		return nil
	})
	return record, err
}

// WaitTarget addresses one wait of a run, by wait ID, by the step and
// iteration that wait at it, or by both (which must name the same wait).
// The zero target addresses the signal's name instead.
type WaitTarget struct {
	WaitID         string
	InvocationPath string
	IterationPath  string
}

// Signal delivers a signal addressed by name. It goes to the run's oldest
// open wait of its name; with none open it is pending, and the next waits
// of the name take pending signals in the order they arrived (#332). It is
// late only when the run has ended. A retry with the same signal ID is a
// duplicate. A run the journal does not hold is ErrNotFound.
func (j *Journal) Signal(ctx context.Context, envelope signal.Envelope, authorized bool) (SignalResult, error) {
	return j.SignalWait(ctx, envelope, WaitTarget{}, authorized)
}

// SignalWait delivers a signal to one wait only: the wait target names, of
// the signal's name. It is never handed to another wait. An open wait
// takes it; a closed one, or an ended run, makes it late. A wait the run
// does not have yet is refused with ErrNotFound and nothing is stored, as
// internal/cluster refuses a signal for an unknown wait: the sender
// retries once the wait exists. The zero target is Signal.
func (j *Journal) SignalWait(ctx context.Context, envelope signal.Envelope, target WaitTarget, authorized bool) (SignalResult, error) {
	if err := envelope.Validate(); err != nil {
		return SignalResult{}, err
	}
	if (target.InvocationPath == "") != (target.IterationPath == "") {
		return SignalResult{}, errors.New("journal: a wait target needs both its invocation and iteration path, or neither")
	}
	if !authorized {
		return SignalResult{}, ErrUnauthorized
	}
	targeted := target != WaitTarget{}
	var result SignalResult
	err := j.withTx(ctx, "signal", func(tx *sql.Tx) error {
		var existing string
		err := tx.QueryRowContext(ctx, `SELECT state FROM journal_signals WHERE run_id = ? AND signal_id = ?`, envelope.RunID, envelope.SignalID).Scan(&existing)
		if err == nil {
			result = SignalResult{Duplicate: true, Accepted: existing == signalStored || existing == signalPending, Resumed: existing == signalStored}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		store := func(state string) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO journal_signals (run_id, signal_id, name, principal, payload_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, envelope.RunID, envelope.SignalID, envelope.Name, envelope.Principal, []byte(envelope.Payload), state, j.now())
			return err
		}
		// Accepted is the journal's only live run state: completed, failed,
		// canceled and uncertain runs never return to it.
		var runState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM journal_runs WHERE run_id = ?`, envelope.RunID).Scan(&runState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if runState != runAccepted {
			result = SignalResult{Late: true}
			return store(signalLate)
		}
		var waitID, waitState string
		if targeted {
			err = tx.QueryRowContext(ctx, `SELECT wait_id, state FROM journal_waits WHERE run_id = ? AND name = ? AND (? = '' OR wait_id = ?) AND (? = '' OR (invocation_path = ? AND iteration_path = ?))`, envelope.RunID, envelope.Name, target.WaitID, target.WaitID, target.InvocationPath, target.InvocationPath, target.IterationPath).Scan(&waitID, &waitState)
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
		} else {
			// Insertion order, not created_at: the clock is injectable and
			// may tie or step back.
			waitState = waitWaiting
			err = tx.QueryRowContext(ctx, `SELECT wait_id FROM journal_waits WHERE run_id = ? AND name = ? AND state = ? ORDER BY rowid LIMIT 1`, envelope.RunID, envelope.Name, waitWaiting).Scan(&waitID)
			if errors.Is(err, sql.ErrNoRows) {
				result = SignalResult{Accepted: true}
				return store(signalPending)
			}
		}
		if err != nil {
			return err
		}
		if waitState != waitWaiting {
			result = SignalResult{Late: true}
			return store(signalLate)
		}
		if err := store(signalStored); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, signal_id = ?, payload_json = ?, updated_at = ? WHERE wait_id = ? AND state = ?`, waitResumed, envelope.SignalID, []byte(envelope.Payload), j.now(), waitID, waitWaiting)
		if err != nil {
			return err
		}
		result = SignalResult{Accepted: true, Resumed: true}
		return nil
	})
	return result, err
}

func (j *Journal) ClaimDueWaits(ctx context.Context, now time.Time, limit int) ([]WaitRecord, error) {
	if limit <= 0 {
		return nil, errors.New("journal: wait claim limit must be positive")
	}
	var records []WaitRecord
	err := j.withTx(ctx, "wait-claim", func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT wait_id, run_id, name, COALESCE(invocation_path, ''), COALESCE(iteration_path, ''), due_at, state, signal_id, payload_json FROM journal_waits WHERE state = ? AND due_at <= ? ORDER BY due_at, wait_id LIMIT ?`, waitWaiting, now.UTC().UnixNano(), limit)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			record, err := scanWait(rows)
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, updated_at = ? WHERE wait_id = ? AND state = ?`, waitResumed, j.now(), record.WaitID, waitWaiting); err != nil {
				return err
			}
			record.State = waitResumed
			records = append(records, record)
		}
		return rows.Err()
	})
	return records, err
}

func (j *Journal) CancelWait(ctx context.Context, waitID string) error {
	if waitID == "" {
		return errors.New("journal: wait ID is required")
	}
	return j.withTx(ctx, "wait-cancel", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, updated_at = ? WHERE wait_id = ? AND state = ?`, waitCanceled, j.now(), waitID, waitWaiting)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrLateSignal
		}
		return nil
	})
}

func (j *Journal) Wait(ctx context.Context, waitID string) (WaitRecord, error) {
	var record WaitRecord
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var err error
		record, err = scanWait(tx.QueryRowContext(ctx, `SELECT wait_id, run_id, name, COALESCE(invocation_path, ''), COALESCE(iteration_path, ''), due_at, state, signal_id, payload_json FROM journal_waits WHERE wait_id = ?`, waitID))
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return WaitRecord{}, ErrNotFound
	}
	return record, err
}

func scanWait(row interface{ Scan(...any) error }) (WaitRecord, error) {
	var record WaitRecord
	var dueAt int64
	var payload []byte
	if err := row.Scan(&record.WaitID, &record.RunID, &record.Name, &record.InvocationPath, &record.IterationPath, &dueAt, &record.State, &record.SignalID, &payload); err != nil {
		return WaitRecord{}, err
	}
	record.DueAt = time.Unix(0, dueAt).UTC()
	record.Payload = append([]byte(nil), payload...)
	return record, nil
}
