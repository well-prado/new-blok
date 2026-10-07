package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
)

// A wait is waiting until a timer claim or a signal fires it. A fired wait
// is a wakeup the run has not yet consumed: it stays fired until
// AcknowledgeWait records that the engine committed the step the wait
// resumed, and PendingResumptions lists it whenever no holder has its run
// under a live run lease (#332, defect D1).
const (
	waitWaiting      = "waiting"
	waitFired        = "fired"
	waitAcknowledged = "acknowledged"
	waitCanceled     = "canceled"
	// waitLegacyResumed is the fired state before #332's slice B, mapped on
	// open to fired or acknowledged.
	waitLegacyResumed = "resumed"
	signalStored      = "stored"
	signalPending     = "pending"
	signalLate        = "late"
)

var (
	ErrWaitExists   = errors.New("journal: wait already exists")
	ErrUnauthorized = errors.New("journal: signal is unauthorized")
	ErrLateSignal   = errors.New("journal: signal arrived after wait was closed")
	ErrWaitNotFired = errors.New("journal: wait has not fired")
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
// LeaseOwner and LeaseUntil are the lease on the wait's run: the holder
// executing it and when that lapses; both are empty when none has it.
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
	LeaseOwner     string
	LeaseUntil     time.Time
}

// waitColumns is what scanWait reads, from journal_waits w joined to its
// run r (waitsWithRun).
const (
	waitColumns  = `w.wait_id, w.run_id, w.name, COALESCE(w.invocation_path, ''), COALESCE(w.iteration_path, ''), w.due_at, w.state, w.signal_id, w.payload_json, COALESCE(r.lease_owner, ''), COALESCE(r.lease_until, 0)`
	waitsWithRun = ` FROM journal_waits w JOIN journal_runs r ON r.run_id = w.run_id`
)

// ErrLeaseLost: another holder has the run, or this holder no longer does.
var ErrLeaseLost = errors.New("journal: the run lease is held by another holder")

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
		if _, err := tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, signal_id = ?, payload_json = ?, fired_at = ?, updated_at = ? WHERE wait_id = ? AND state = ?`, waitFired, signalID, payload, j.now(), j.now(), request.WaitID, waitWaiting); err != nil {
			return err
		}
		record.State = waitFired
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
		_, err = tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, signal_id = ?, payload_json = ?, fired_at = ?, updated_at = ? WHERE wait_id = ? AND state = ?`, waitFired, envelope.SignalID, []byte(envelope.Payload), j.now(), j.now(), waitID, waitWaiting)
		if err != nil {
			return err
		}
		result = SignalResult{Accepted: true, Resumed: true}
		return nil
	})
	return result, err
}

// ClaimDueWaits fires up to limit waits of live runs due at now, and takes
// the run lease (until now plus the wakeup lease) of each of their runs no
// holder has under a live lease, this one included. It returns the waits
// of the runs it leased: the caller executes those runs. A fired wait of a
// run already held stays fired for its holder, or for PendingResumptions
// once that lease lapses or is released.
func (j *Journal) ClaimDueWaits(ctx context.Context, now time.Time, limit int) ([]WaitRecord, error) {
	if limit <= 0 {
		return nil, errors.New("journal: wait claim limit must be positive")
	}
	var records []WaitRecord
	err := j.withTx(ctx, "wait-claim", func(tx *sql.Tx) error {
		due, err := queryStrings(ctx, tx, `SELECT w.wait_id`+waitsWithRun+` WHERE w.state = ? AND w.due_at <= ? AND r.state = ? ORDER BY w.due_at, w.wait_id LIMIT ?`, waitWaiting, now.UTC().UnixNano(), runAccepted, limit)
		if err != nil {
			return err
		}
		leased := map[string]bool{}
		for _, waitID := range due {
			var run string
			if err := tx.QueryRowContext(ctx, `UPDATE journal_waits SET state = ?, fired_at = ?, updated_at = ? WHERE wait_id = ? AND state = ? RETURNING run_id`, waitFired, j.now(), j.now(), waitID, waitWaiting).Scan(&run); err != nil {
				return err
			}
			if _, seen := leased[run]; !seen {
				if leased[run], err = j.takeRunLease(ctx, tx, run, now); err != nil {
					return err
				}
			}
			if leased[run] {
				record, err := scanWait(tx.QueryRowContext(ctx, `SELECT `+waitColumns+waitsWithRun+` WHERE w.wait_id = ?`, waitID))
				if err != nil {
					return err
				}
				records = append(records, record)
			}
		}
		return nil
	})
	return records, err
}

// PendingResumptions takes the run lease of up to limit live runs that have
// a fired wait and no live run lease, and returns those runs' fired waits:
// the caller executes them. Runs leased least recently come first, so a
// run whose resumption never finishes cannot starve the others. Run it on
// start, and periodically, to resume the runs whose wakeup a crash or a
// lapsed lease interrupted.
func (j *Journal) PendingResumptions(ctx context.Context, now time.Time, limit int) ([]WaitRecord, error) {
	if limit <= 0 {
		return nil, errors.New("journal: resumption limit must be positive")
	}
	var records []WaitRecord
	err := j.withTx(ctx, "wait-lease", func(tx *sql.Tx) error {
		runs, err := queryStrings(ctx, tx, `SELECT w.run_id`+waitsWithRun+` WHERE w.state = ? AND r.state = ? AND (r.lease_until IS NULL OR r.lease_until <= ?)
			GROUP BY w.run_id ORDER BY COALESCE(MAX(r.leased_at), 0), MIN(w.fired_at), w.run_id LIMIT ?`, waitFired, runAccepted, now.UTC().UnixNano(), limit)
		if err != nil {
			return err
		}
		for _, run := range runs {
			if _, err := j.takeRunLease(ctx, tx, run, now); err != nil {
				return err
			}
			rows, err := tx.QueryContext(ctx, `SELECT `+waitColumns+waitsWithRun+` WHERE w.run_id = ? AND w.state = ? ORDER BY w.fired_at, w.rowid`, run, waitFired)
			if err != nil {
				return err
			}
			for rows.Next() {
				record, err := scanWait(rows)
				if err != nil {
					rows.Close()
					return err
				}
				records = append(records, record)
			}
			if err := rows.Close(); err != nil {
				return err
			}
		}
		return nil
	})
	return records, err
}

// takeRunLease leases a live run to this holder until now plus the wakeup
// lease, when no holder (this one included) has it under a live lease. A
// holder that wants to keep a run it holds renews it instead.
func (j *Journal) takeRunLease(ctx context.Context, tx *sql.Tx, runID string, now time.Time) (bool, error) {
	result, err := tx.ExecContext(ctx, `UPDATE journal_runs SET lease_owner = ?, lease_until = ?, leased_at = ? WHERE run_id = ? AND state = ? AND (lease_until IS NULL OR lease_until <= ?)`, j.holder, now.Add(j.lease).UTC().UnixNano(), now.UTC().UnixNano(), runID, runAccepted, now.UTC().UnixNano())
	if err != nil {
		return false, err
	}
	changed, err := result.RowsAffected()
	return changed == 1, err
}

// RenewRunLease extends this holder's lease on a run it is executing to now
// plus the wakeup lease. A run another holder has taken over, or one with
// no lease, is ErrLeaseLost.
func (j *Journal) RenewRunLease(ctx context.Context, runID string, now time.Time) error {
	return j.changeRunLease(ctx, "run-lease-renew", runID, `lease_until = ?`, now.Add(j.lease).UTC().UnixNano())
}

// ReleaseRunLease gives up this holder's lease on a run, so the next holder
// may take it at once. A run with no lease is a no-op; one another holder
// has is ErrLeaseLost.
func (j *Journal) ReleaseRunLease(ctx context.Context, runID string) error {
	return j.changeRunLease(ctx, "run-lease-release", runID, `lease_owner = NULL, lease_until = NULL`)
}

func (j *Journal) changeRunLease(ctx context.Context, name, runID, set string, args ...any) error {
	return j.withTx(ctx, name, func(tx *sql.Tx) error {
		var owner sql.NullString
		if err := tx.QueryRowContext(ctx, `SELECT lease_owner FROM journal_runs WHERE run_id = ?`, runID).Scan(&owner); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if !owner.Valid && name == "run-lease-release" {
			return nil
		}
		if owner.String != j.holder {
			return ErrLeaseLost
		}
		_, err := tx.ExecContext(ctx, `UPDATE journal_runs SET `+set+` WHERE run_id = ? AND lease_owner = ?`, append(args, runID, j.holder)...)
		return err
	})
}

func queryStrings(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var values []string
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, rows.Err()
}

// AcknowledgeWait records that the engine committed the step a fired wait
// resumed: the wakeup is consumed and the wait leaves PendingResumptions.
// Acknowledging it again is a no-op; a wait that has not fired is
// ErrWaitNotFired. It does not touch the run lease: the holder is still
// executing the run, and renews or releases the lease itself. It does not
// check the lease holder either: a holder whose lease was taken over can
// still acknowledge (slice C acknowledges inside the resumed step's own
// transaction, checked against the run lease).
func (j *Journal) AcknowledgeWait(ctx context.Context, waitID string) error {
	if waitID == "" {
		return errors.New("journal: wait ID is required")
	}
	return j.withTx(ctx, "wait-acknowledge", func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM journal_waits WHERE wait_id = ?`, waitID).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		switch state {
		case waitAcknowledged:
			return nil
		case waitFired:
			_, err := tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, updated_at = ? WHERE wait_id = ? AND state = ?`, waitAcknowledged, j.now(), waitID, waitFired)
			return err
		default:
			return ErrWaitNotFired
		}
	})
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
		record, err = scanWait(tx.QueryRowContext(ctx, `SELECT `+waitColumns+waitsWithRun+` WHERE w.wait_id = ?`, waitID))
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return WaitRecord{}, ErrNotFound
	}
	return record, err
}

func scanWait(row interface{ Scan(...any) error }) (WaitRecord, error) {
	var record WaitRecord
	var dueAt, leaseUntil int64
	var payload []byte
	if err := row.Scan(&record.WaitID, &record.RunID, &record.Name, &record.InvocationPath, &record.IterationPath, &dueAt, &record.State, &record.SignalID, &payload, &record.LeaseOwner, &leaseUntil); err != nil {
		return WaitRecord{}, err
	}
	record.DueAt = time.Unix(0, dueAt).UTC()
	if leaseUntil != 0 {
		record.LeaseUntil = time.Unix(0, leaseUntil).UTC()
	}
	record.Payload = append([]byte(nil), payload...)
	return record, nil
}
