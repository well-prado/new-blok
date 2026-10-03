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
}

func (j *Journal) Compact(ctx context.Context, before time.Time) (CompactionReport, error) {
	var report CompactionReport
	err := j.withTx(ctx, "compact", func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT run_id, request_key, artifact_digest, state, output_json, completed_at FROM journal_runs WHERE state = ? AND completed_at IS NOT NULL AND completed_at < ? ORDER BY completed_at`, runCompleted, before.UTC().UnixNano())
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var runID, requestKey, artifactDigest, state string
			var output []byte
			var completedAt int64
			if err := rows.Scan(&runID, &requestKey, &artifactDigest, &state, &output, &completedAt); err != nil {
				return err
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
