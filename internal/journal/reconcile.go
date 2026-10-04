package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

var (
	ErrArtifactMissing      = errors.New("journal: required artifact is missing")
	ErrArtifactConflict     = errors.New("journal: artifact digest is already registered with different content")
	ErrReconciliationDenied = errors.New("journal: reconciliation is unauthorized")
	ErrNotReconciliable     = errors.New("journal: operation is not uncertain")
	ErrUpgradeWouldDiscard  = errors.New("journal: upgrade policy would discard accepted work")
)

const runCanceled = "canceled"

type ArtifactRecord struct {
	Digest       string
	Version      string
	ManifestJSON json.RawMessage
}

type Reconciliation struct {
	OperationKey string
	Actor        string
	Evidence     string
	Result       json.RawMessage
	State        string
	Duplicate    bool
}

type UpgradePlan struct {
	FromDigest   string
	ToDigest     string
	RetainRuns   bool
	AffectedRuns int
}

func (j *Journal) RegisterArtifact(ctx context.Context, artifact ArtifactRecord) error {
	if artifact.Digest == "" || artifact.Version == "" || !json.Valid(artifact.ManifestJSON) {
		return errors.New("journal: valid artifact digest, version and manifest are required")
	}
	return j.withTx(ctx, "artifact-register", func(tx *sql.Tx) error {
		var existing []byte
		err := tx.QueryRowContext(ctx, `SELECT manifest_json FROM journal_artifacts WHERE digest = ?`, artifact.Digest).Scan(&existing)
		if err == nil {
			if string(existing) != string(artifact.ManifestJSON) {
				return ErrArtifactConflict
			}
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO journal_artifacts (digest, version, manifest_json, created_at) VALUES (?, ?, ?, ?)`, artifact.Digest, artifact.Version, []byte(artifact.ManifestJSON), j.now())
		return err
	})
}

func (j *Journal) RequireArtifact(ctx context.Context, digest string) error {
	if digest == "" {
		return ErrArtifactMissing
	}
	return j.withRead(ctx, func(tx *sql.Tx) error {
		var found string
		if err := tx.QueryRowContext(ctx, `SELECT digest FROM journal_artifacts WHERE digest = ?`, digest).Scan(&found); errors.Is(err, sql.ErrNoRows) {
			return ErrArtifactMissing
		} else {
			return err
		}
	})
}

func (j *Journal) Reconcile(ctx context.Context, operationKey, actor, evidence string, result json.RawMessage, authorized bool) (Reconciliation, error) {
	if !authorized {
		return Reconciliation{}, ErrReconciliationDenied
	}
	if operationKey == "" || actor == "" || evidence == "" || !json.Valid(result) {
		return Reconciliation{}, errors.New("journal: operation, actor, evidence and valid result are required")
	}
	for attempt := 0; attempt < 5; attempt++ {
		reconciliation, err := j.reconcileOnce(ctx, operationKey, actor, evidence, result)
		if err == nil || !strings.Contains(err.Error(), "database is locked") {
			return reconciliation, err
		}
		select {
		case <-ctx.Done():
			return Reconciliation{}, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 10 * time.Millisecond):
		}
	}
	return Reconciliation{}, errors.New("journal: reconciliation remained locked after bounded retries")
}

func (j *Journal) reconcileOnce(ctx context.Context, operationKey, actor, evidence string, result json.RawMessage) (Reconciliation, error) {
	var reconciliation Reconciliation
	err := j.withTx(ctx, "reconcile", func(tx *sql.Tx) error {
		var existing Reconciliation
		var existingResult []byte
		err := tx.QueryRowContext(ctx, `SELECT operation_key, actor, evidence, result_json, state FROM journal_reconciliations WHERE operation_key = ?`, operationKey).Scan(&existing.OperationKey, &existing.Actor, &existing.Evidence, &existingResult, &existing.State)
		if err == nil {
			existing.Result = append([]byte(nil), existingResult...)
			existing.Duplicate = true
			reconciliation = existing
			return nil
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		var state, attemptID string
		if err := tx.QueryRowContext(ctx, `SELECT state, current_attempt_id FROM journal_operations WHERE operation_key = ?`, operationKey).Scan(&state, &attemptID); err != nil {
			return err
		}
		if state != operationUncertain {
			return ErrNotReconciliable
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_reconciliations (operation_key, actor, evidence, result_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?)`, operationKey, actor, evidence, []byte(result), operationCommitted, j.now()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_attempts SET state = ?, result_json = ?, finished_at = ? WHERE attempt_id = ? AND operation_key = ? AND state = ?`, attemptCommitted, []byte(result), j.now(), attemptID, operationKey, attemptUncertain); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_operations SET state = ?, result_json = ?, updated_at = ? WHERE operation_key = ? AND state = ?`, operationCommitted, []byte(result), j.now(), operationKey, operationUncertain); err != nil {
			return err
		}
		reconciliation = Reconciliation{OperationKey: operationKey, Actor: actor, Evidence: evidence, Result: append([]byte(nil), result...), State: operationCommitted}
		return nil
	})
	return reconciliation, err
}

func (j *Journal) PlanUpgrade(ctx context.Context, fromDigest, toDigest string, retainRuns bool) (UpgradePlan, error) {
	if err := j.RequireArtifact(ctx, fromDigest); err != nil {
		return UpgradePlan{}, err
	}
	if err := j.RequireArtifact(ctx, toDigest); err != nil {
		return UpgradePlan{}, err
	}
	var affected int
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_runs WHERE artifact_digest = ? AND state = ?`, fromDigest, runAccepted).Scan(&affected)
	})
	if err != nil {
		return UpgradePlan{}, err
	}
	if !retainRuns && affected > 0 {
		return UpgradePlan{}, ErrUpgradeWouldDiscard
	}
	return UpgradePlan{FromDigest: fromDigest, ToDigest: toDigest, RetainRuns: retainRuns, AffectedRuns: affected}, nil
}

func (j *Journal) CancelRun(ctx context.Context, runID, reason string) error {
	if runID == "" || reason == "" {
		return errors.New("journal: run and cancellation reason are required")
	}
	return j.withTx(ctx, "run-cancel", func(tx *sql.Tx) error {
		if err := requireQuiescentRun(ctx, tx, runID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE journal_runs SET state = ?, completed_at = ? WHERE run_id = ? AND state = ?`, runCanceled, j.now(), runID, runAccepted)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrStaleAttempt
		}
		return nil
	})
}
