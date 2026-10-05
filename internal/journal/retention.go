package journal

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type CompactionReport struct {
	RemovedRuns   int
	RetainedAudit int
	// HeldRuns counts completed runs kept by the application's legal hold.
	HeldRuns int
}

// Compact deletes completed runs finished before the cutoff, leaving a
// compaction tombstone. It never deletes an active, uncertain, failed or
// canceled run, and never one the configured legal Hold keeps. The audit
// contract's records (contract/audit) are separate and are never deleted
// here; they have their own retention (ADR 0021).
func (j *Journal) Compact(ctx context.Context, before time.Time) (CompactionReport, error) {
	var report CompactionReport
	err := j.withTx(ctx, "compact", func(tx *sql.Tx) error {
		report = CompactionReport{}
		rows, err := tx.QueryContext(ctx, `SELECT run_id, request_key, artifact_digest, state, output_json, completed_at, workflow, principal FROM journal_runs WHERE state = ? AND completed_at IS NOT NULL AND completed_at < ? ORDER BY completed_at`, runCompleted, before.UTC().UnixNano())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var runID, requestKey, artifactDigest, state, workflow, principal string
			var output []byte
			var completedAt int64
			if err := rows.Scan(&runID, &requestKey, &artifactDigest, &state, &output, &completedAt, &workflow, &principal); err != nil {
				return err
			}
			if j.held(RetainedRun{RunID: runID, Workflow: workflow, Principal: principal}) {
				report.HeldRuns++
				continue
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO journal_audit (audit_id, run_id, request_key, artifact_digest, state, output_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(audit_id) DO NOTHING`, "audit:"+runID, runID, requestKey, artifactDigest, state, output, completedAt); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM journal_attempts WHERE operation_key IN (SELECT operation_key FROM journal_operations WHERE run_id = ?)`, runID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM journal_operations WHERE run_id = ?`, runID); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `DELETE FROM journal_runs WHERE run_id = ? AND state = ?`, runID, runCompleted); err != nil {
				return err
			}
			report.RemovedRuns++
		}
		if err := rows.Err(); err != nil {
			return err
		}
		err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_audit`).Scan(&report.RetainedAudit)
		return err
	})
	return report, err
}

// held fails closed: a Hold that panics keeps the run.
func (j *Journal) held(run RetainedRun) (keep bool) {
	if j.hold == nil {
		return false
	}
	defer func() {
		if recover() != nil {
			keep = true
		}
	}()
	return j.hold(run)
}

func (j *Journal) Backup(ctx context.Context, destination string) error {
	if destination == "" {
		return errors.New("journal: backup destination is required")
	}
	return j.database.Backup(ctx, destination)
}

func (j *Journal) AuditCount(ctx context.Context) (int, error) {
	var count int
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_audit`).Scan(&count)
	})
	return count, err
}
