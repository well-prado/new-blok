package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/trigger"
)

// Retention says which finished jobs Compact erases (#290). A zero cutoff
// erases nothing of its kind, so the zero Retention erases nothing.
type Retention struct {
	// Completed erases completed jobs that finished strictly before it.
	Completed time.Time
	// Dead erases dead-lettered jobs that finished strictly before it. Dead
	// letters are retained for an operator (ADR 0022) until the application
	// says otherwise, so this cutoff is separate from Completed.
	Dead time.Time
	// Tombstones ends the dedupe window: the tombstones of compacted jobs
	// that finished strictly before it are deleted, and their request keys
	// may then be submitted again as new jobs. The zero time keeps every
	// tombstone, so a key once committed is never run twice (ADR 0006).
	Tombstones time.Time
	// Batch bounds the jobs, or tombstones, one write transaction erases:
	// DefaultCompactBatch when zero, at most MaxCompactBatch.
	Batch int
}

const (
	// DefaultCompactBatch is Retention.Batch when it is zero.
	DefaultCompactBatch = 256
	// MaxCompactBatch bounds Retention.Batch, so one compaction transaction
	// never holds the write lock for an unbounded number of rows.
	MaxCompactBatch = 4096
)

// RetainedJob identifies a finished job Compact is about to erase, for the
// queue's legal hold (WithRetentionHold).
type RetainedJob struct {
	ID, RequestKey, Kind, State string
	Principal                   trigger.Principal
	FinishedAt                  time.Time
}

// CompactionReport says what one Compact erased.
type CompactionReport struct {
	// Compacted counts jobs erased to digest-only tombstones.
	Compacted int
	// Held counts finished jobs past their cutoff that the legal hold kept.
	Held int
	// ExpiredTombstones counts tombstones deleted because their dedupe
	// window (Retention.Tombstones) ended.
	ExpiredTombstones int
	// Batches counts the write transactions this pass committed.
	Batches int
	// LogPurged reports that this pass truncated the store's write-ahead
	// log because an erasure, this pass's or an earlier one's, was waiting
	// for it, so no older copy of an erased page survives there.
	LogPurged bool
	// PurgePending reports that erased content may still be in the log: a
	// reader kept the log in use. The debt is persisted and every later
	// Compact retries it. Both flags stay false on a store that cannot purge
	// (store.PurgerOf).
	PurgePending bool
}

const (
	metaErasureGeneration = "erasure_generation"
	metaPurgedGeneration  = "purged_generation"
)

// compactionCandidates reads one batch of finished jobs of one state, in
// (updated_at, job_id) order after the cursor, through worker_jobs_finished.
// The literal state IN (...) is what lets SQLite use the partial index.
const compactionCandidates = `SELECT job_id, request_key, kind, payload_digest, principal_json, state, attempt, max_attempts, created_at, updated_at
	FROM worker_jobs
	WHERE state IN ('completed', 'dead') AND state = ? AND updated_at < ? AND (updated_at > ? OR (updated_at = ? AND job_id > ?))
	ORDER BY updated_at, job_id LIMIT ?`

// Compact erases finished jobs past their retention (#290). Each one is
// reduced to a digest-only tombstone in worker_compacted: the digest of its
// request key, its job id, kind, state, attempt counts and times, and one
// digest of its identity (kind, payload digest and principal). Its payload,
// principal, trace context, error text and the request key itself are
// deleted with its row. The tombstone keeps the dedupe contract of ADR 0006:
// a duplicate submission of a compacted job is answered as a duplicate
// (EnqueueResult.Accepted false, Job.Compacted true) and a submission that
// reuses its key with other content conflicts, until Retention.Tombstones
// ends its window.
//
// Pending and processing jobs are never touched. Neither is a job the
// queue's legal hold keeps (WithRetentionHold; a hold that panics keeps the
// job) or one that finished less than the queue's minimum retention ago
// (WithMinRetention; each cutoff is clamped to now minus it). The hold runs
// inside the write transaction: keep it fast and free of side effects.
//
// Work is done in write transactions of at most Retention.Batch rows each,
// marked store.Writer and writing first (#176, #214), so other writers get
// their turn between batches however large the backlog. Then, as journal
// compaction does (ADR 0021 §7), the store's log is purged whenever an
// erasure, this pass's or an earlier blocked one, still owes a purge. On an
// error the batches already committed stay committed and the report counts
// them.
func (q *Queue) Compact(ctx context.Context, retention Retention) (CompactionReport, error) {
	batch := retention.Batch
	switch {
	case batch == 0:
		batch = DefaultCompactBatch
	case batch < 0 || batch > MaxCompactBatch:
		return CompactionReport{}, fmt.Errorf("worker: compaction batch %d must be between 1 and %d", batch, MaxCompactBatch)
	}
	purger, purgeable := store.PurgerOf(q.database)
	var report CompactionReport
	for _, pass := range []struct {
		state  string
		cutoff time.Time
	}{{StateCompleted, retention.Completed}, {StateDead, retention.Dead}} {
		if pass.cutoff.IsZero() {
			continue
		}
		cutoff := q.retentionCutoff(pass.cutoff)
		lastAt, lastID := int64(math.MinInt64), ""
		for {
			if err := ctx.Err(); err != nil {
				return report, fmt.Errorf("worker: compact: %w", err)
			}
			var compacted, held, seen int
			err := q.withTx(store.Writer(ctx), func(tx *sql.Tx) error {
				var err error
				var at int64
				var id string
				at, id, compacted, held, seen, err = q.compactBatch(ctx, tx, pass.state, cutoff, lastAt, lastID, batch)
				if err != nil {
					return err
				}
				// The erasure and the record that its purge is owed commit
				// together, so a crash or a blocked purge never loses the debt.
				if purgeable && compacted > 0 {
					if _, err := tx.ExecContext(ctx, `UPDATE worker_meta SET value = value + 1 WHERE name = ?`, metaErasureGeneration); err != nil {
						return err
					}
				}
				lastAt, lastID = at, id
				return nil
			})
			if err != nil {
				return report, fmt.Errorf("worker: compact: %w", err)
			}
			report.Batches++
			report.Compacted += compacted
			report.Held += held
			if seen < batch {
				break
			}
		}
	}
	if !retention.Tombstones.IsZero() {
		cutoff := retention.Tombstones.UTC().UnixNano()
		for {
			if err := ctx.Err(); err != nil {
				return report, fmt.Errorf("worker: compact: %w", err)
			}
			var expired int64
			err := q.withTx(store.Writer(ctx), func(tx *sql.Tx) error {
				result, err := tx.ExecContext(ctx, `DELETE FROM worker_compacted WHERE request_digest IN (SELECT request_digest FROM worker_compacted WHERE finished_at < ? ORDER BY finished_at LIMIT ?)`, cutoff, batch)
				if err != nil {
					return err
				}
				expired, err = result.RowsAffected()
				return err
			})
			if err != nil {
				return report, fmt.Errorf("worker: compact: %w", err)
			}
			report.Batches++
			report.ExpiredTombstones += int(expired)
			if expired < int64(batch) {
				break
			}
		}
	}
	if !purgeable {
		return report, nil
	}
	// Every Compact, even one that erases nothing, retries a purge an
	// earlier erasure still owes.
	var pending int64
	if err := q.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		pending, err = pendingPurge(ctx, tx)
		return err
	}); err != nil {
		return report, fmt.Errorf("worker: compact: %w", err)
	}
	if pending > 0 {
		report.LogPurged = purger.PurgeLog(ctx) == nil
		report.PurgePending = !report.LogPurged
		if report.LogPurged {
			// Record what the purge covered: every erasure committed before
			// it, up to the generation read before it. A later
			// erasure keeps a higher generation and stays pending.
			if err := q.withTx(store.Writer(ctx), func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO worker_meta (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = max(value, excluded.value)`, metaPurgedGeneration, pending)
				return err
			}); err != nil {
				report.PurgePending = true
			}
		}
	}
	return report, nil
}

// retentionCutoff clamps a cutoff to the queue's legal minimum. The zero
// time is a cutoff before every job.
func (q *Queue) retentionCutoff(cutoff time.Time) int64 {
	if cutoff.IsZero() {
		return math.MinInt64
	}
	if legal := q.clock().Add(-q.minRetention); q.minRetention > 0 && legal.Before(cutoff) {
		cutoff = legal
	}
	return cutoff.UTC().UnixNano()
}

// compactBatch erases at most limit finished jobs of state after the cursor,
// and returns the cursor past the last one it read. Its first statement
// writes (#176): it makes sure the erasure counter exists, which takes the
// write lock before anything is read.
func (q *Queue) compactBatch(ctx context.Context, tx *sql.Tx, state string, cutoff, lastAt int64, lastID string, limit int) (int64, string, int, int, int, error) {
	if _, err := tx.ExecContext(ctx, `INSERT INTO worker_meta (name, value) VALUES (?, 0) ON CONFLICT(name) DO NOTHING`, metaErasureGeneration); err != nil {
		return lastAt, lastID, 0, 0, 0, err
	}
	rows, err := tx.QueryContext(ctx, compactionCandidates, state, cutoff, lastAt, lastAt, lastID, limit)
	if err != nil {
		return lastAt, lastID, 0, 0, 0, err
	}
	type candidate struct {
		id, key, kind, payloadDigest, principal, state string
		attempt, maxAttempts                           int
		createdAt, finishedAt                          int64
	}
	var batch []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.key, &c.kind, &c.payloadDigest, &c.principal, &c.state, &c.attempt, &c.maxAttempts, &c.createdAt, &c.finishedAt); err != nil {
			rows.Close()
			return lastAt, lastID, 0, 0, 0, err
		}
		batch = append(batch, c)
	}
	if err := rows.Close(); err != nil {
		return lastAt, lastID, 0, 0, 0, err
	}
	if err := rows.Err(); err != nil {
		return lastAt, lastID, 0, 0, 0, err
	}
	compacted, held := 0, 0
	now := q.now()
	for _, c := range batch {
		lastAt, lastID = c.finishedAt, c.id
		retained := RetainedJob{ID: c.id, RequestKey: c.key, Kind: c.kind, State: c.state, FinishedAt: time.Unix(0, c.finishedAt).UTC()}
		if c.principal != "" {
			// A principal that does not decode is still handed to the
			// hold without it; the hold sees the rest of the job.
			_ = json.Unmarshal([]byte(c.principal), &retained.Principal)
		}
		if q.held(retained) {
			held++
			continue
		}
		// A tombstone already there for the key can only be an older job's,
		// whose key a worker from before #290 accepted again: the job being
		// erased now is the key's latest identity.
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_compacted (request_digest, job_id, kind, identity_digest, state, attempt, max_attempts, created_at, finished_at, compacted_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_digest) DO UPDATE SET job_id = excluded.job_id, kind = excluded.kind, identity_digest = excluded.identity_digest,
				state = excluded.state, attempt = excluded.attempt, max_attempts = excluded.max_attempts, created_at = excluded.created_at, finished_at = excluded.finished_at, compacted_at = excluded.compacted_at`,
			digest([]byte(c.key)), c.id, c.kind, identityDigest(c.kind, c.payloadDigest, c.principal), c.state, c.attempt, c.maxAttempts, c.createdAt, c.finishedAt, now); err != nil {
			return lastAt, lastID, 0, 0, 0, err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM worker_jobs WHERE job_id = ? AND state = ?`, c.id, c.state); err != nil {
			return lastAt, lastID, 0, 0, 0, err
		}
		compacted++
	}
	return lastAt, lastID, compacted, held, len(batch), nil
}

// identityDigest is what a tombstone keeps of a job's request identity: its
// kind, payload digest and encoded principal, as Enqueue compares them. One
// digest over all three, so the principal is not recoverable from the
// tombstone without the exact payload.
func identityDigest(kind, payloadDigest, principal string) string {
	return digest([]byte(kind + "\x00" + payloadDigest + "\x00" + principal))
}

// pendingPurge returns the erasure generation still owed a log purge, or 0.
func pendingPurge(ctx context.Context, tx *sql.Tx) (int64, error) {
	var erased, purged int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM worker_meta WHERE name = ?), 0), COALESCE((SELECT value FROM worker_meta WHERE name = ?), 0)`, metaErasureGeneration, metaPurgedGeneration).Scan(&erased, &purged)
	if err != nil || erased <= purged {
		return 0, err
	}
	return erased, nil
}

// held fails closed: a hold that panics keeps the job.
func (q *Queue) held(job RetainedJob) (keep bool) {
	if q.hold == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			keep = true
		}
	}()
	return q.hold(job)
}

// compacted reads the tombstone of requestKey, if Compact erased its job.
func compacted(ctx context.Context, tx *sql.Tx, requestKey string) (Job, string, error) {
	job := Job{RequestKey: requestKey, Compacted: true}
	var identity string
	err := tx.QueryRowContext(ctx, `SELECT job_id, kind, identity_digest, state, attempt, max_attempts FROM worker_compacted WHERE request_digest = ?`, digest([]byte(requestKey))).
		Scan(&job.ID, &job.Kind, &identity, &job.State, &job.Attempt, &job.MaxAttempts)
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, "", ErrNotFound
	}
	if err != nil {
		return Job{}, "", err
	}
	return job, identity, nil
}

// migrateRetention installs what Compact needs, in place and idempotently,
// inside New's schema transaction.
func migrateRetention(ctx context.Context, tx *sql.Tx) error {
	for _, statement := range []string{
		// The tombstone of a compacted job holds digests, counts and times
		// only (#290).
		`CREATE TABLE IF NOT EXISTS worker_compacted (
			request_digest TEXT PRIMARY KEY,
			job_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			identity_digest TEXT NOT NULL,
			state TEXT NOT NULL,
			attempt INTEGER NOT NULL,
			max_attempts INTEGER NOT NULL,
			created_at INTEGER NOT NULL,
			finished_at INTEGER NOT NULL,
			compacted_at INTEGER NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS worker_compacted_finished ON worker_compacted (finished_at)`,
		// worker_meta holds the queue's erasure counters: erasure_generation
		// counts compactions that erased jobs, purged_generation the latest
		// one whose log purge succeeded.
		`CREATE TABLE IF NOT EXISTS worker_meta (name TEXT PRIMARY KEY, value INTEGER NOT NULL)`,
		// Compaction reads finished jobs in batches through this partial
		// index, so each batch costs its own rows, not the history before
		// it. Unfinished jobs are not in it, so claims do not maintain it.
		`CREATE INDEX IF NOT EXISTS worker_jobs_finished ON worker_jobs (state, updated_at, job_id) WHERE state IN ('completed', 'dead')`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}
