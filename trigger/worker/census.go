package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/well-prado/new-blok/observe/slo"
)

// censusQuery is the census aggregate. The literal state <> 'completed' is
// what lets SQLite use the worker_jobs_unfinished partial index, and the
// states are literals for the same reason. Its parameters are now, now, the
// stall cutoff twice, and now.
const censusQuery = `SELECT
		SUM(CASE WHEN state = 'pending' AND available_at <= ? THEN 1 ELSE 0 END),
		SUM(CASE WHEN state = 'pending' AND available_at > ? THEN 1 ELSE 0 END),
		SUM(CASE WHEN state = 'processing' AND lease_until > ? THEN 1 ELSE 0 END),
		SUM(CASE WHEN state = 'processing' AND (lease_until IS NULL OR lease_until <= ?) THEN 1 ELSE 0 END),
		SUM(CASE WHEN state = 'dead' THEN 1 ELSE 0 END),
		MIN(CASE WHEN state = 'pending' AND available_at <= ? THEN available_at END)
	FROM worker_jobs WHERE state <> 'completed'`

// liveHandler identifies a handler running in this process: the claim turn
// shared by every Queue on its write domain, and the job.
type liveHandler struct {
	turn chan struct{}
	job  string
}

// liveHandlers holds the handlers this process is running now. Only one runs
// per write domain at a time (it holds the claim turn).
var liveHandlers sync.Map

// Census reports the queue's unfinished jobs by liveness (ADR 0022) in one
// read-only transaction over the worker_jobs_unfinished partial index, so
// its cost follows the unfinished and dead jobs, not completed history:
//
//   - pending, available now: Pending; the oldest one's wait since it became
//     available is the backlog lag (OldestPending);
//   - pending, available later (a retry or deferral backoff): Waiting;
//   - processing under a lease that has not expired: Active;
//   - processing under a lease that expired less than handlerBudget ago, or
//     held by a handler this process is still running: Active. A worker
//     extends its lease only inside its uncommitted handler transaction
//     (#245), so from committed state a slow handler and a dead worker look
//     the same until the budget passes; handlerBudget is the application's
//     longest handler (zero means the lease);
//   - processing under a lease that expired longer than handlerBudget ago,
//     with no live handler here: Stalled, its worker died or lost its lease.
//     The next ProcessOnce reclaims it, so a stall that persists means no
//     worker is consuming;
//   - dead: DeadLetters.
//
// The census itself waits out the lease and the budget before it reports a
// job stalled, and a live worker reclaims an expired job at its next
// ProcessOnce, so the queue declares no Takeover for the alert to cover. The
// census writes nothing and takes no claim turn.
func (q *Queue) Census(ctx context.Context, source string, handlerBudget time.Duration) (slo.Work, error) {
	if handlerBudget < 0 {
		return slo.Work{}, errors.New("worker: census handler budget must not be negative")
	}
	if handlerBudget == 0 {
		handlerBudget = q.lease
	}
	work := slo.Work{Source: source}
	now := q.now()
	stallBefore := now - int64(handlerBudget)
	err := q.withTx(ctx, func(tx *sql.Tx) error {
		var pending, delayed, active, stalled, dead, oldest sql.NullInt64
		if err := tx.QueryRowContext(ctx, censusQuery,
			now, now, stallBefore, stallBefore, now,
		).Scan(&pending, &delayed, &active, &stalled, &dead, &oldest); err != nil {
			return err
		}
		// A handler this process runs is alive whatever its committed lease
		// says: move it from stalled to active.
		var rescued int64
		var lookupErr error
		liveHandlers.Range(func(key, _ any) bool {
			live := key.(liveHandler)
			if live.turn != q.claimTurn {
				return true
			}
			var lease sql.NullInt64
			err := tx.QueryRowContext(ctx, `SELECT lease_until FROM worker_jobs WHERE job_id = ? AND state = 'processing'`, live.job).Scan(&lease)
			if errors.Is(err, sql.ErrNoRows) {
				return true
			}
			if err != nil {
				lookupErr = err
				return false
			}
			if !lease.Valid || lease.Int64 <= stallBefore {
				rescued++
			}
			return true
		})
		if lookupErr != nil {
			return fmt.Errorf("worker: census live handler: %w", lookupErr)
		}
		// Liveness comes from the shared classification, not from the SQL,
		// so the queue and every other census agree on what pages.
		work.AddN(slo.Classify(slo.Observation{OwnerLive: true}), int(pending.Int64))
		work.AddN(slo.Classify(slo.Observation{Waiting: true}), int(delayed.Int64))
		work.AddN(slo.Classify(slo.Observation{Claimed: true, OwnerLive: true}), int(active.Int64+rescued))
		work.AddN(slo.Classify(slo.Observation{Claimed: true}), int(stalled.Int64-rescued))
		work.DeadLetters = int(dead.Int64)
		if oldest.Valid && oldest.Int64 < now {
			work.OldestPending = time.Duration(now - oldest.Int64)
		}
		return nil
	})
	return work, err
}

// CensusSource is Census as an operational source named source.
func (q *Queue) CensusSource(source string, handlerBudget time.Duration) slo.Source {
	return slo.Func(source, func(ctx context.Context) (slo.Snapshot, error) {
		work, err := q.Census(ctx, source, handlerBudget)
		if err != nil {
			return slo.Snapshot{}, err
		}
		return slo.Snapshot{Work: []slo.Work{work}}, nil
	})
}
