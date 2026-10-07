package journal

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// RunLease is a run leased to this journal's holder, and its lease token.
type RunLease struct {
	RunID string
	Token int64
}

// InterruptedRuns takes the run lease of up to limit live runs whose last
// execution stopped without suspending or ending: a holder leased them
// before (leased_at is set), no lease on them is live, and they have no
// open or fired wait (a run suspended at a wait has an open one; a woken
// run is PendingResumptions'). That is a run whose holder died, lost its
// lease, or was stopped mid-execution, including after it consumed a
// wakeup. A run admitted but never leased is not one: its admitter
// executes it. Runs leased least recently come first.
func (j *Journal) InterruptedRuns(ctx context.Context, now time.Time, limit int) ([]RunLease, error) {
	if limit <= 0 {
		return nil, errors.New("journal: interrupted run limit must be positive")
	}
	var leases []RunLease
	// Most live runs are suspended at a wait: find the candidates with a
	// read, so the scan never holds the writer, then take each in a write
	// transaction that checks it again.
	const interrupted = `state = ? AND leased_at IS NOT NULL AND (lease_until IS NULL OR lease_until <= ?)
			AND NOT EXISTS (SELECT 1 FROM journal_waits w WHERE w.run_id = r.run_id AND w.state IN (?, ?))`
	at := now.UTC().UnixNano()
	var runs []string
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var err error
		runs, err = queryStrings(ctx, tx, `SELECT run_id FROM journal_runs r WHERE `+interrupted+` ORDER BY leased_at, run_id LIMIT ?`, runAccepted, at, waitWaiting, waitFired, limit)
		return err
	})
	if err != nil || len(runs) == 0 {
		return nil, err
	}
	err = j.withTx(ctx, "run-interrupted", func(tx *sql.Tx) error {
		for _, run := range runs {
			var still bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM journal_runs r WHERE run_id = ? AND `+interrupted+`)`, run, runAccepted, at, waitWaiting, waitFired).Scan(&still); err != nil {
				return err
			}
			if !still {
				continue
			}
			token, err := j.takeRunLease(ctx, tx, run, now)
			if err != nil {
				return err
			}
			if token != 0 {
				leases = append(leases, RunLease{RunID: run, Token: token})
			}
		}
		return nil
	})
	return leases, err
}
