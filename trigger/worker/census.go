package worker

import (
	"context"
	"database/sql"
	"time"

	"github.com/well-prado/new-blok/observe/slo"
)

// Census reports the queue's unfinished jobs by liveness (ADR 0022) in one
// read-only transaction:
//
//   - pending, available now: Pending; the oldest one's wait since it became
//     available is the backlog lag (OldestPending);
//   - pending, available later (a retry or deferral backoff): Waiting;
//   - processing under a lease that has not expired: Active;
//   - processing under an expired lease: Stalled, because the worker that
//     claimed it died or lost its lease before acknowledging. The next
//     ProcessOnce reclaims it, so a stall that persists means no worker is
//     consuming;
//   - dead: DeadLetters.
//
// The read is a fixed aggregate over the queue table; it writes nothing and
// takes no claim turn, so sampling never delays a worker's claim.
func (q *Queue) Census(ctx context.Context, source string) (slo.Work, error) {
	work := slo.Work{Source: source}
	now := q.now()
	err := q.withTx(ctx, func(tx *sql.Tx) error {
		var pending, delayed, active, stalled, dead sql.NullInt64
		var oldest sql.NullInt64
		if err := tx.QueryRowContext(ctx, `SELECT
				SUM(CASE WHEN state = ? AND available_at <= ? THEN 1 ELSE 0 END),
				SUM(CASE WHEN state = ? AND available_at > ? THEN 1 ELSE 0 END),
				SUM(CASE WHEN state = ? AND lease_until > ? THEN 1 ELSE 0 END),
				SUM(CASE WHEN state = ? AND (lease_until IS NULL OR lease_until <= ?) THEN 1 ELSE 0 END),
				SUM(CASE WHEN state = ? THEN 1 ELSE 0 END),
				MIN(CASE WHEN state = ? AND available_at <= ? THEN available_at END)
			FROM worker_jobs WHERE state <> ?`,
			StatePending, now, StatePending, now, StateProcessing, now, StateProcessing, now, StateDead, StatePending, now, StateCompleted,
		).Scan(&pending, &delayed, &active, &stalled, &dead, &oldest); err != nil {
			return err
		}
		// Liveness comes from the shared classification, not from the SQL,
		// so the queue and every other census agree on what pages.
		work.AddN(slo.Classify(slo.Observation{OwnerLive: true}), int(pending.Int64))
		work.AddN(slo.Classify(slo.Observation{Waiting: true}), int(delayed.Int64))
		work.AddN(slo.Classify(slo.Observation{Claimed: true, OwnerLive: true}), int(active.Int64))
		work.AddN(slo.Classify(slo.Observation{Claimed: true}), int(stalled.Int64))
		work.DeadLetters = int(dead.Int64)
		if oldest.Valid && oldest.Int64 < now {
			work.OldestPending = time.Duration(now - oldest.Int64)
		}
		return nil
	})
	return work, err
}

// CensusSource is Census as an operational source named source.
func (q *Queue) CensusSource(source string) slo.Source {
	return func(ctx context.Context) (slo.Snapshot, error) {
		work, err := q.Census(ctx, source)
		if err != nil {
			return slo.Snapshot{}, err
		}
		return slo.Snapshot{Work: []slo.Work{work}}, nil
	}
}
