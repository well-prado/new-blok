package journal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/store"
)

type CompactionReport struct {
	RemovedRuns int
	// Tombstones counts compaction tombstones after this pass.
	Tombstones int
	// HeldRuns counts completed runs kept by the application's legal hold.
	HeldRuns int
	// ErasedReconciliations counts reconciliations whose evidence, provider
	// result and actor this pass erased, keeping their identity and digests.
	ErasedReconciliations int
	// LogPurged reports that this pass truncated the store's write-ahead
	// log because an erasure, this pass's or an earlier one's, was waiting
	// for it, so no older copy of an erased page survives there.
	LogPurged bool
	// PurgePending reports that erased content may still be in the log: a
	// reader kept the log in use. The pending purge is persisted and every
	// later Compact retries it until it succeeds. Both flags stay false on
	// a store that cannot purge (store.PurgerOf).
	PurgePending bool
}

const (
	metaErasureGeneration = "erasure_generation"
	metaPurgedGeneration  = "purged_generation"
)

// runOwnedTables hold rows that belong to one run and die with it, children
// before the run itself so no foreign key is ever left dangling (#281).
// journal_attempts and journal_operations come first, separately, because
// attempts are keyed by operation.
var runOwnedTables = []string{"journal_waits", "journal_signals", "journal_checkpoints", "journal_scopes", "journal_children", "journal_joins"}

// Compact erases completed runs finished before the cutoff (#49, #281). For
// each it deletes every row the run owns, erases its reconciliations'
// evidence, provider results and actors while keeping their identity and
// digests, so audit.Verify still knows each decision existed, and leaves a
// digest-only tombstone. It never touches an active, uncertain, failed or
// canceled run, one the configured legal Hold keeps, or one completed less
// than Config.MinRetention ago. The audit contract's records
// (contract/audit) are not deleted here; they have their own retention (ADR
// 0021). It then purges the store's log whenever an erasure, this pass's
// or an earlier blocked one, still owes a purge.
func (j *Journal) Compact(ctx context.Context, before time.Time) (CompactionReport, error) {
	cutoff := before
	if legal := j.clock().Add(-j.minimum); j.minimum > 0 && legal.Before(cutoff) {
		cutoff = legal
	}
	purger, purgeable := store.PurgerOf(j.database)
	var report CompactionReport
	var backfilled []audit.Record
	var pending int64
	err := j.withTx(ctx, "compact", func(tx *sql.Tx) error {
		report, backfilled, pending = CompactionReport{}, nil, 0
		runs, err := compactionCandidates(ctx, tx, cutoff)
		if err != nil {
			return err
		}
		now := j.now()
		for _, run := range runs {
			if j.held(RetainedRun{RunID: run.runID, Workflow: run.workflow, Principal: run.principal}) {
				report.HeldRuns++
				continue
			}
			erased, records, err := j.compactRun(ctx, tx, run, now)
			if err != nil {
				return err
			}
			report.RemovedRuns++
			report.ErasedReconciliations += erased
			backfilled = append(backfilled, records...)
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_compacted`).Scan(&report.Tombstones); err != nil {
			return err
		}
		if !purgeable {
			return nil
		}
		// The erasure and the record that its purge is owed commit together,
		// so a crash or a blocked purge can never lose the debt.
		if report.RemovedRuns > 0 {
			if _, err := tx.ExecContext(ctx, `INSERT INTO journal_meta (name, value) VALUES (?, 1) ON CONFLICT(name) DO UPDATE SET value = value + 1`, metaErasureGeneration); err != nil {
				return err
			}
		}
		pending, err = pendingPurge(ctx, tx)
		return err
	})
	if err != nil {
		return report, err
	}
	if j.audit != nil && len(backfilled) > 0 {
		j.audit.Notify(backfilled...)
	}
	if pending > 0 {
		report.LogPurged = purger.PurgeLog(ctx) == nil
		report.PurgePending = !report.LogPurged
		if report.LogPurged {
			// Record what the purge covered: every erasure committed before
			// it, up to the generation read in the compaction. A later
			// erasure keeps a higher generation and stays pending.
			if err := j.withTx(ctx, "purge-record", func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO journal_meta (name, value) VALUES (?, ?) ON CONFLICT(name) DO UPDATE SET value = max(value, excluded.value)`, metaPurgedGeneration, pending)
				return err
			}); err != nil {
				report.PurgePending = true
			}
		}
	}
	return report, nil
}

// pendingPurge returns the erasure generation still owed a log purge, or 0.
func pendingPurge(ctx context.Context, tx *sql.Tx) (int64, error) {
	var erased, purged int64
	err := tx.QueryRowContext(ctx, `SELECT COALESCE((SELECT value FROM journal_meta WHERE name = ?), 0), COALESCE((SELECT value FROM journal_meta WHERE name = ?), 0)`, metaErasureGeneration, metaPurgedGeneration).Scan(&erased, &purged)
	if err != nil || erased <= purged {
		return 0, err
	}
	return erased, nil
}

type compactionCandidate struct {
	runID, requestKey, artifactDigest, inputDigest, state, workflow, principal string
	output                                                                     []byte
	completedAt                                                                int64
}

func compactionCandidates(ctx context.Context, tx *sql.Tx, cutoff time.Time) ([]compactionCandidate, error) {
	rows, err := tx.QueryContext(ctx, `SELECT run_id, request_key, artifact_digest, input_digest, state, output_json, completed_at, workflow, principal FROM journal_runs WHERE state = ? AND completed_at IS NOT NULL AND completed_at < ? ORDER BY completed_at`, runCompleted, cutoff.UTC().UnixNano())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []compactionCandidate
	for rows.Next() {
		var run compactionCandidate
		if err := rows.Scan(&run.runID, &run.requestKey, &run.artifactDigest, &run.inputDigest, &run.state, &run.output, &run.completedAt, &run.workflow, &run.principal); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

// compactRun erases one run inside the compaction transaction. Order
// matters: reconciliations are erased in place, then attempts before their
// operations, then the run's other rows, and the run itself last.
func (j *Journal) compactRun(ctx context.Context, tx *sql.Tx, run compactionCandidate, now int64) (int, []audit.Record, error) {
	records, err := j.backfillReconciliationRecords(ctx, tx, run.runID)
	if err != nil {
		return 0, nil, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE journal_reconciliations SET actor = '', evidence = NULL, result_json = NULL, erased_at = ? WHERE run_id = ? AND erased_at IS NULL`, now, run.runID)
	if err != nil {
		return 0, nil, err
	}
	erased, err := result.RowsAffected()
	if err != nil {
		return 0, nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM journal_attempts WHERE operation_key IN (SELECT operation_key FROM journal_operations WHERE run_id = ?)`, run.runID); err != nil {
		return 0, nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM journal_operations WHERE run_id = ?`, run.runID); err != nil {
		return 0, nil, err
	}
	for _, table := range runOwnedTables {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table+` WHERE run_id = ?`, run.runID); err != nil {
			return 0, nil, err
		}
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO journal_compacted (run_id, request_digest, artifact_digest, input_digest, output_digest, state, completed_at, compacted_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(run_id) DO NOTHING`,
		run.runID, digestBytes([]byte(run.requestKey)), run.artifactDigest, run.inputDigest, optionalDigest(run.output), run.state, run.completedAt, now); err != nil {
		return 0, nil, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM journal_runs WHERE run_id = ? AND state = ?`, run.runID, runCompleted); err != nil {
		return 0, nil, err
	}
	return int(erased), records, nil
}

// backfillReconciliationRecords writes the audit record of a reconciliation
// that predates audit before its actor is erased, since afterwards it could
// not be reproduced. The record takes the decision's own tenant when the row
// stored one (#286), and the system tenant ("") otherwise, never the tenant
// of whoever runs the compaction. Without an audit journal nothing is
// written, and Verify reports such a decision as it reports any pre-audit
// decision (#284).
func (j *Journal) backfillReconciliationRecords(ctx context.Context, tx *sql.Tx, runID string) ([]audit.Record, error) {
	if j.audit == nil {
		return nil, nil
	}
	rows, err := tx.QueryContext(ctx, `SELECT operation_key, tenant, actor, evidence_digest, result_digest, created_at FROM journal_reconciliations WHERE run_id = ? AND erased_at IS NULL ORDER BY operation_key`, runID)
	if err != nil {
		return nil, err
	}
	type pending struct {
		key, actor, evidence, result string
		tenant                       sql.NullString
		at                           int64
	}
	var all []pending
	for rows.Next() {
		var p pending
		if err := rows.Scan(&p.key, &p.tenant, &p.actor, &p.evidence, &p.result, &p.at); err != nil {
			rows.Close()
			return nil, err
		}
		all = append(all, p)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	var records []audit.Record
	for _, p := range all {
		recorded, err := j.audit.Recorded(ctx, tx, "reconcile:"+p.key)
		if err != nil {
			return nil, err
		}
		if recorded {
			continue
		}
		// The record is missing, so the stored tenant (or "") is the only
		// account of who decided it.
		record, inserted, err := j.audit.Append(ctx, tx, reconciliationRecord(p.tenant.String, p.key, p.actor, p.evidence, p.result, runID, p.at))
		if err != nil {
			return nil, err
		}
		if inserted {
			records = append(records, record)
		}
	}
	return records, nil
}

func optionalDigest(data []byte) string {
	if len(data) == 0 {
		return ""
	}
	return digestBytes(data)
}

// migrateErasure upgrades a journal written before #281, inside the schema
// transaction, so a crash leaves the old journal intact and the next open
// retries. It is a no-op on a journal already migrated.
//
//   - Legacy tombstones (journal_audit) kept a compacted run's output and
//     request key; they are rewritten as digest-only tombstones and the
//     table is dropped.
//   - Legacy reconciliations kept evidence, result and actor under a
//     foreign key to their operation, which made their run impossible to
//     compact. The table is rebuilt without it, with digests and the run
//     identity added. Content stays only while the run does: a
//     reconciliation whose operation is gone is erased now.
func (j *Journal) migrateErasure(ctx context.Context, tx *sql.Tx) error {
	now := j.now()
	if exists, err := tableExists(ctx, tx, "journal_audit"); err != nil {
		return err
	} else if exists {
		if err := migrateLegacyTombstones(ctx, tx); err != nil {
			return fmt.Errorf("migrate tombstones: %w", err)
		}
	}
	current, err := hasColumn(ctx, tx, "journal_reconciliations", "erased_at")
	if err != nil {
		return err
	}
	if !current {
		if err := migrateLegacyReconciliations(ctx, tx, now); err != nil {
			return fmt.Errorf("migrate reconciliations: %w", err)
		}
	}
	_, err = tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS journal_reconciliations_run ON journal_reconciliations(run_id, erased_at)`)
	return err
}

// migrateLegacyTombstones rewrites each legacy tombstone with digests
// computed in Go, which SQLite cannot do, then drops the legacy table. A
// legacy tombstone recorded neither the input digest nor when it was
// compacted: input_digest stays empty and compacted_at stays 0.
func migrateLegacyTombstones(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT run_id, request_key, artifact_digest, state, output_json, created_at FROM journal_audit`)
	if err != nil {
		return err
	}
	type tombstone struct {
		runID, request, artifact, state string
		output                          []byte
		completedAt                     int64
	}
	var all []tombstone
	for rows.Next() {
		var t tombstone
		if err := rows.Scan(&t.runID, &t.request, &t.artifact, &t.state, &t.output, &t.completedAt); err != nil {
			rows.Close()
			return err
		}
		all = append(all, t)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, t := range all {
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_compacted (run_id, request_digest, artifact_digest, input_digest, output_digest, state, completed_at, compacted_at) VALUES (?, ?, ?, '', ?, ?, ?, 0) ON CONFLICT(run_id) DO NOTHING`,
			t.runID, digestBytes([]byte(t.request)), t.artifact, optionalDigest(t.output), t.state, t.completedAt); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `DROP TABLE journal_audit`)
	return err
}

func migrateLegacyReconciliations(ctx context.Context, tx *sql.Tx, now int64) error {
	if _, err := tx.ExecContext(ctx, reconciliationsTable("journal_reconciliations_281")); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT r.operation_key, COALESCE(o.run_id, ''), o.operation_key IS NOT NULL, r.actor, r.evidence, r.result_json, r.state, r.created_at
		FROM journal_reconciliations r LEFT JOIN journal_operations o ON o.operation_key = r.operation_key`)
	if err != nil {
		return err
	}
	type reconciliation struct {
		key, runID, actor, evidence, state string
		live                               bool
		result                             []byte
		createdAt                          int64
	}
	var all []reconciliation
	for rows.Next() {
		var r reconciliation
		if err := rows.Scan(&r.key, &r.runID, &r.live, &r.actor, &r.evidence, &r.result, &r.state, &r.createdAt); err != nil {
			rows.Close()
			return err
		}
		all = append(all, r)
	}
	if err := rows.Close(); err != nil {
		return err
	}
	for _, r := range all {
		evidenceDigest, resultDigest := audit.Digest([]byte(r.evidence)), audit.Digest(r.result)
		if r.live {
			_, err = tx.ExecContext(ctx, `INSERT INTO journal_reconciliations_281 (operation_key, run_id, actor, evidence, result_json, evidence_digest, result_digest, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				r.key, r.runID, r.actor, r.evidence, r.result, evidenceDigest, resultDigest, r.state, r.createdAt)
		} else {
			_, err = tx.ExecContext(ctx, `INSERT INTO journal_reconciliations_281 (operation_key, run_id, actor, evidence_digest, result_digest, state, created_at, erased_at) VALUES (?, ?, '', ?, ?, ?, ?, ?)`,
				r.key, r.runID, evidenceDigest, resultDigest, r.state, r.createdAt, now)
		}
		if err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `DROP TABLE journal_reconciliations`); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `ALTER TABLE journal_reconciliations_281 RENAME TO journal_reconciliations`)
	return err
}

func tableExists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	var found string
	err := tx.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

func hasColumn(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	found := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return false, err
		}
		found = found || name == column
	}
	return found, rows.Close()
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

// TombstoneCount counts compaction tombstones.
func (j *Journal) TombstoneCount(ctx context.Context) (int, error) {
	var count int
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_compacted`).Scan(&count)
	})
	return count, err
}
