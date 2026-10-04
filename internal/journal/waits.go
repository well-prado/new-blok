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

type WaitRequest struct {
	RunID  string
	WaitID string
	Name   string
	DueAt  time.Time
}

type WaitRecord struct {
	WaitID   string
	RunID    string
	Name     string
	DueAt    time.Time
	State    string
	SignalID string
	Payload  json.RawMessage
}

type SignalResult struct {
	Accepted  bool
	Duplicate bool
	Late      bool
	Resumed   bool
}

func (j *Journal) ScheduleWait(ctx context.Context, request WaitRequest) (WaitRecord, error) {
	if request.RunID == "" || request.WaitID == "" || request.Name == "" || request.DueAt.IsZero() {
		return WaitRecord{}, errors.New("journal: run, wait ID, name and due time are required")
	}
	var record WaitRecord
	err := j.withTx(ctx, "wait-schedule", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `INSERT INTO journal_waits (wait_id, run_id, name, due_at, state, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(wait_id) DO NOTHING`, request.WaitID, request.RunID, request.Name, request.DueAt.UTC().UnixNano(), waitWaiting, j.now(), j.now())
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
		record = WaitRecord{WaitID: request.WaitID, RunID: request.RunID, Name: request.Name, DueAt: request.DueAt.UTC(), State: waitWaiting}
		var signalID string
		var payload []byte
		err = tx.QueryRowContext(ctx, `SELECT signal_id, payload_json FROM journal_signals WHERE run_id = ? AND name = ? AND state = ? ORDER BY created_at, signal_id LIMIT 1`, request.RunID, request.Name, signalPending).Scan(&signalID, &payload)
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

func (j *Journal) Signal(ctx context.Context, envelope signal.Envelope, authorized bool) (SignalResult, error) {
	if err := envelope.Validate(); err != nil {
		return SignalResult{}, err
	}
	if !authorized {
		return SignalResult{}, ErrUnauthorized
	}
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
		var waitID, state string
		err = tx.QueryRowContext(ctx, `SELECT wait_id, state FROM journal_waits WHERE run_id = ? AND name = ?`, envelope.RunID, envelope.Name).Scan(&waitID, &state)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, `INSERT INTO journal_signals (run_id, signal_id, name, principal, payload_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, envelope.RunID, envelope.SignalID, envelope.Name, envelope.Principal, []byte(envelope.Payload), signalPending, j.now())
			result = SignalResult{Accepted: true}
			return err
		}
		if err != nil {
			return err
		}
		if state != waitWaiting {
			_, err = tx.ExecContext(ctx, `INSERT INTO journal_signals (run_id, signal_id, name, principal, payload_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, envelope.RunID, envelope.SignalID, envelope.Name, envelope.Principal, []byte(envelope.Payload), signalLate, j.now())
			result = SignalResult{Late: true}
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_signals (run_id, signal_id, name, principal, payload_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, envelope.RunID, envelope.SignalID, envelope.Name, envelope.Principal, []byte(envelope.Payload), signalStored, j.now()); err != nil {
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
		rows, err := tx.QueryContext(ctx, `SELECT wait_id, run_id, name, due_at, state, signal_id, payload_json FROM journal_waits WHERE state = ? AND due_at <= ? ORDER BY due_at, wait_id LIMIT ?`, waitWaiting, now.UTC().UnixNano(), limit)
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
		record, err = scanWait(tx.QueryRowContext(ctx, `SELECT wait_id, run_id, name, due_at, state, signal_id, payload_json FROM journal_waits WHERE wait_id = ?`, waitID))
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
	if err := row.Scan(&record.WaitID, &record.RunID, &record.Name, &dueAt, &record.State, &record.SignalID, &payload); err != nil {
		return WaitRecord{}, err
	}
	record.DueAt = time.Unix(0, dueAt).UTC()
	record.Payload = append([]byte(nil), payload...)
	return record, nil
}
