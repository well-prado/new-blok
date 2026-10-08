package journal

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// RunLease is a run leased to this journal's holder, and its lease token.
type RunLease struct {
	RunID string
	Token int64
}

// WakeupLease is how long a run stays leased to the holder executing it
// unless renewed (Config.WakeupLease).
func (j *Journal) WakeupLease() time.Duration { return j.lease }

// InterruptedRuns takes the run lease of up to limit live runs of the named
// workflows whose execution stopped without suspending or ending, or never
// started: no lease on them is live, they have no open or fired wait (a run
// suspended at a wait has an open one; a woken run is
// PendingResumptions'), and either a holder leased them before (it died,
// lost its lease, or was stopped mid-execution, including after it
// consumed a wakeup) or they were admitted at least one wakeup lease ago
// and never leased (their admitter stopped before starting them). Runs that
// have waited longest come first: the later of their last lease and their
// admission.
func (j *Journal) InterruptedRuns(ctx context.Context, now time.Time, limit int, workflows []string) ([]RunLease, error) {
	if limit <= 0 {
		return nil, errors.New("journal: interrupted run limit must be positive")
	}
	if len(workflows) == 0 {
		return nil, nil
	}
	at := now.UTC().UnixNano()
	// Most live runs are suspended at a wait: find the candidates with a
	// read, so the scan never holds the writer, then take each in a write
	// transaction that checks it again.
	interrupted := `state = ? AND (lease_until IS NULL OR lease_until <= ?)
			AND (leased_at IS NOT NULL OR created_at <= ?)
			AND workflow IN (?` + strings.Repeat(", ?", len(workflows)-1) + `)
			AND NOT EXISTS (SELECT 1 FROM journal_waits w WHERE w.run_id = r.run_id AND w.state IN (?, ?))`
	args := []any{runAccepted, at, at - j.lease.Nanoseconds()}
	for _, workflow := range workflows {
		args = append(args, workflow)
	}
	args = append(args, waitWaiting, waitFired)
	var runs []string
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var err error
		runs, err = queryStrings(ctx, tx, `SELECT run_id FROM journal_runs r WHERE `+interrupted+` ORDER BY COALESCE(leased_at, created_at), run_id LIMIT ?`, append(args, limit)...)
		return err
	})
	if err != nil || len(runs) == 0 {
		return nil, err
	}
	var leases []RunLease
	err = j.withTx(ctx, "run-interrupted", func(tx *sql.Tx) error {
		for _, run := range runs {
			var still bool
			if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM journal_runs r WHERE run_id = ? AND `+interrupted+`)`, append([]any{run}, args...)...).Scan(&still); err != nil {
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
