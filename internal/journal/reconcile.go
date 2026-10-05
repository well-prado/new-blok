package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
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
	// Erased reports a duplicate whose run was compacted: its actor,
	// evidence and result were erased (#281) and are returned empty. Only
	// the operation key and state remain.
	Erased bool
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

// Reconcile decides the outcome of an uncertain effect, with its audit
// record in the same transaction (ADR 0021). The decision belongs to the
// tenant of ctx (audit.WithTenant), which is stored with it. A re-delivery
// by that tenant is a duplicate that returns the original actor, evidence
// and result (only the key and state once its run is compacted). A
// re-delivery under any other tenant is answered as for an operation that
// was never reconciled: ErrNotReconciliable while the operation is
// retained, not found once it is compacted, and nothing is written (#286).
func (j *Journal) Reconcile(ctx context.Context, operationKey, actor, evidence string, result json.RawMessage, authorized bool) (Reconciliation, error) {
	if !authorized {
		return Reconciliation{}, ErrReconciliationDenied
	}
	if operationKey == "" || actor == "" || evidence == "" || !json.Valid(result) {
		return Reconciliation{}, errors.New("journal: operation, actor, evidence and valid result are required")
	}
	// A reconciliation decides an effect's outcome; it requires durable
	// audit and is refused, with nothing applied, when the record cannot be
	// written (ADR 0021).
	if j.audit == nil {
		return Reconciliation{}, audit.ErrRequired
	}
	for attempt := 0; attempt < 5; attempt++ {
		reconciliation, record, err := j.reconcileOnce(ctx, operationKey, actor, evidence, result)
		if err == nil && record.ID != "" {
			j.audit.Notify(record)
		}
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

func (j *Journal) reconcileOnce(ctx context.Context, operationKey, actor, evidence string, result json.RawMessage) (Reconciliation, audit.Record, error) {
	var reconciliation Reconciliation
	var record audit.Record
	inserted := false
	err := j.withTx(ctx, "reconcile", func(tx *sql.Tx) error {
		caller := audit.TenantFrom(ctx)
		var existing Reconciliation
		var existingTenant, existingEvidence sql.NullString
		var existingResult []byte
		var evidenceDigest, resultDigest string
		var createdAt int64
		var erasedAt sql.NullInt64
		var runID string
		err := tx.QueryRowContext(ctx, `SELECT operation_key, run_id, tenant, actor, evidence, result_json, evidence_digest, result_digest, state, created_at, erased_at FROM journal_reconciliations WHERE operation_key = ?`, operationKey).Scan(&existing.OperationKey, &runID, &existingTenant, &existing.Actor, &existingEvidence, &existingResult, &evidenceDigest, &resultDigest, &existing.State, &createdAt, &erasedAt)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		foreign := false
		if err == nil {
			owner, owned := reconciliationTenant(existingTenant)
			// A reconciliation is answered only to the tenant that decided
			// it (#286). Any other caller's operation is looked up below as
			// if it had none, so it learns exactly what it would of an
			// operation that settled without one: not its evidence, not its
			// result, not even that it was reconciled.
			foreign = !owned || owner != caller
			if !foreign && erasedAt.Valid {
				// The run was compacted: the decision is still a duplicate,
				// but there is no content left to return, and an audit
				// record that is missing can no longer be reproduced
				// (compaction writes it first when audit is composed).
				reconciliation = Reconciliation{OperationKey: existing.OperationKey, State: existing.State, Duplicate: true, Erased: true}
				return nil
			}
			if !foreign {
				existing.Evidence = existingEvidence.String
				existing.Result = append([]byte(nil), existingResult...)
				existing.Duplicate = true
				reconciliation = existing
				// A re-delivered reconciliation succeeds as a duplicate.
				// When its record is missing (the original predates audit)
				// it writes it, under the decision's own tenant; an
				// existing record is never rewritten.
				recorded, err := j.audit.Recorded(ctx, tx, "reconcile:"+existing.OperationKey)
				if err != nil || recorded {
					return err
				}
				record, inserted, err = j.audit.Append(ctx, tx, reconciliationRecord(owner, existing.OperationKey, existing.Actor, evidenceDigest, resultDigest, runID, createdAt))
				return err
			}
		}
		var state, attemptID string
		if err := tx.QueryRowContext(ctx, `SELECT state, current_attempt_id, run_id FROM journal_operations WHERE operation_key = ?`, operationKey).Scan(&state, &attemptID, &runID); err != nil {
			return err
		}
		if state != operationUncertain || foreign {
			return ErrNotReconciliable
		}
		now := j.now()
		evidenceDigest, resultDigest = audit.Digest([]byte(evidence)), audit.Digest(result)
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_reconciliations (operation_key, run_id, tenant, actor, evidence, result_json, evidence_digest, result_digest, state, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, operationKey, runID, caller, actor, evidence, []byte(result), evidenceDigest, resultDigest, operationCommitted, now); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_attempts SET state = ?, result_json = ?, finished_at = ? WHERE attempt_id = ? AND operation_key = ? AND state = ?`, attemptCommitted, []byte(result), now, attemptID, operationKey, attemptUncertain); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_operations SET state = ?, result_json = ?, updated_at = ? WHERE operation_key = ? AND state = ?`, operationCommitted, []byte(result), now, operationKey, operationUncertain); err != nil {
			return err
		}
		reconciliation = Reconciliation{OperationKey: operationKey, Actor: actor, Evidence: evidence, Result: append([]byte(nil), result...), State: operationCommitted}
		record, inserted, err = j.audit.Append(ctx, tx, reconciliationRecord(caller, operationKey, actor, evidenceDigest, resultDigest, runID, now))
		return err
	})
	if err != nil {
		return Reconciliation{}, audit.Record{}, err
	}
	if !inserted {
		record = audit.Record{}
	}
	return reconciliation, record, nil
}

// reconciliationTenant is the tenant a reconciliation belongs to (#286):
// the one stored with it. Every row has one: every reconciliation since
// #286 stores it, and opening a journal fixes it, once, for a row from
// before (backfillReconciliationTenants), so ownership never depends on
// audit retention. A row still without one belongs to nobody: an older
// binary wrote it after the migration (the next open fixes it), or its
// audit record failed verification or had been pruned when the journal was
// opened. Every re-delivery of it is answered as for a never-reconciled
// operation.
func reconciliationTenant(stored sql.NullString) (tenant string, owned bool) {
	return stored.String, stored.Valid
}

// backfillReconciliationTenants runs in the schema transaction on every
// open. It gives each reconciliation without a tenant the tenant of its
// verified audit record, which the original decision wrote under its own
// tenant, or the system tenant "" when it has no record (it predates
// audit, or audit was never composed on this database). It is written
// once and never re-derived, so pruning the record later cannot change who
// owns the decision. Two rows stay without a tenant, owned by nobody: one
// whose record fails verification (it is not trusted, and Verify reports
// it, ErrCorrupt), and one whose record was pruned before this open (a
// prune tombstone proves it had a tenant, so it is not the system
// tenant's). Rows that already have a tenant are not touched, so reopening
// changes nothing.
//
// It reads audit's tables without a composed audit.Journal, so before it
// reads them it checks their stamp, as audit.NewJournal would (#321): an
// audit store stamped newer than this binary understands refuses the open
// with audit's *store.NewerSchemaError, and no row is given a tenant. The
// check precedes even asking whether audit_records_v1 exists, because a
// newer audit may keep its records elsewhere, and "no audit table" would
// then hand every row to the system tenant. A journal with no row without
// a tenant reads nothing of audit's and is not refused for it. A row the
// repair leaves unowned (its record pruned or unverifiable) is tried again
// on every open, so under a newer audit every open is refused while it
// exists (ADR 0003, #321).
func backfillReconciliationTenants(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT operation_key FROM journal_reconciliations WHERE tenant IS NULL ORDER BY operation_key`)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	if err := rows.Close(); err != nil || len(keys) == 0 {
		return err
	}
	if err := audit.CheckSchema(ctx, tx); err != nil {
		return err
	}
	audited, err := tableExists(ctx, tx, "audit_records_v1")
	if err != nil {
		return err
	}
	for _, key := range keys {
		tenant := ""
		if audited {
			stored, found, err := audit.StoredTenant(ctx, tx, "reconcile:"+key)
			if errors.Is(err, audit.ErrCorrupt) {
				continue
			}
			if err != nil {
				return err
			}
			if found {
				tenant = stored
			} else if pruned, err := audit.Pruned(ctx, tx, "reconcile:"+key, audit.KindReconciliation); err != nil {
				return err
			} else if pruned {
				// The record existed and was pruned before this open, so
				// the decision had a tenant that is now unknown. It is not
				// the system tenant's: the row stays unowned.
				continue
			}
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_reconciliations SET tenant = ? WHERE operation_key = ? AND tenant IS NULL`, tenant, key); err != nil {
			return err
		}
	}
	return nil
}

// reconciliationRecord binds evidence and result by digest only: the
// evidence text and the provider result never enter audit. The digests are
// the ones stored with the reconciliation, and its time is the
// reconciliation's own, so a re-delivery produces the identical record.
func reconciliationRecord(tenant, operationKey, actor, evidenceDigest, resultDigest, runID string, at int64) audit.Record {
	return audit.Record{
		ID: "reconcile:" + operationKey, Kind: audit.KindReconciliation, Tenant: tenant,
		Actor: actor, Subject: operationKey, RunID: runID, Action: "reconcile", Outcome: audit.OutcomeApplied,
		Digests: map[string]string{"evidence": evidenceDigest, "result": resultDigest},
		At:      time.Unix(0, at).UTC(),
	}
}

// AuditKind and AuditedIDs make the journal an audit.Owner: every
// reconciliation requires its record, which audit.Journal.Verify checks.
func (j *Journal) AuditKind() audit.Kind { return audit.KindReconciliation }

func (j *Journal) AuditedIDs(ctx context.Context, tx *sql.Tx, fn func(string) error) error {
	rows, err := tx.QueryContext(ctx, `SELECT operation_key FROM journal_reconciliations ORDER BY operation_key`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		if err := fn("reconcile:" + key); err != nil {
			return err
		}
	}
	return rows.Err()
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

// DecideUpgrade is the audited deployment decision: PlanUpgrade's checks
// plus a durable record of who decided, between which artifacts, under which
// retention choice, and whether it was accepted or refused, committed in one
// transaction. A refusal is recorded too and still returns
// ErrUpgradeWouldDiscard. Without audit, or when the record cannot be
// written, no decision is returned. PlanUpgrade stays an unaudited preview.
func (j *Journal) DecideUpgrade(ctx context.Context, actor, fromDigest, toDigest string, retainRuns bool) (UpgradePlan, error) {
	if j.audit == nil {
		return UpgradePlan{}, audit.ErrRequired
	}
	if actor == "" || fromDigest == "" || toDigest == "" {
		return UpgradePlan{}, errors.New("journal: actor and artifact digests are required")
	}
	id, err := randomID("deployment")
	if err != nil {
		return UpgradePlan{}, err
	}
	var plan UpgradePlan
	var record audit.Record
	refused := false
	err = j.withTx(ctx, "upgrade-decision", func(tx *sql.Tx) error {
		for _, digest := range []string{fromDigest, toDigest} {
			var found string
			if err := tx.QueryRowContext(ctx, `SELECT digest FROM journal_artifacts WHERE digest = ?`, digest).Scan(&found); errors.Is(err, sql.ErrNoRows) {
				return ErrArtifactMissing
			} else if err != nil {
				return err
			}
		}
		var affected int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_runs WHERE artifact_digest = ? AND state = ?`, fromDigest, runAccepted).Scan(&affected); err != nil {
			return err
		}
		outcome, reason, action := audit.OutcomeAccepted, "", "upgrade.discard-runs"
		if retainRuns {
			action = "upgrade.retain-runs"
		}
		if !retainRuns && affected > 0 {
			outcome, reason, refused = audit.OutcomeRefused, "would_discard_accepted_work", true
		}
		digests := map[string]string{}
		for name, digest := range map[string]string{"from": fromDigest, "to": toDigest} {
			if audit.ValidDigest(digest) {
				digests[name] = digest
			}
		}
		var err error
		record, _, err = j.audit.Append(ctx, tx, audit.Record{
			ID: id, Kind: audit.KindDeployment, Tenant: audit.TenantFrom(ctx), Actor: actor,
			Subject: fromDigest + " -> " + toDigest, Action: action, Outcome: outcome, Reason: reason, Digests: digests,
		})
		plan = UpgradePlan{FromDigest: fromDigest, ToDigest: toDigest, RetainRuns: retainRuns, AffectedRuns: affected}
		return err
	})
	if err != nil {
		return UpgradePlan{}, err
	}
	j.audit.Notify(record)
	if refused {
		return UpgradePlan{}, ErrUpgradeWouldDiscard
	}
	return plan, nil
}

// ActiveRuns implements audit.RunActivity and fails closed: a run is active
// while it is accepted or uncertain (awaiting reconciliation), and so is a
// run the journal does not know at all, unless a compaction tombstone proves
// it ended. An approval for a run held elsewhere (a cluster run, ADR 0019)
// is therefore never pruned on a guess. It reads in the audit's
// transaction, so it must share the journal's database.
func (j *Journal) ActiveRuns(ctx context.Context, tx *sql.Tx, runIDs []string) (map[string]bool, error) {
	active := map[string]bool{}
	for start := 0; start < len(runIDs); start += 200 {
		chunk := runIDs[start:min(start+200, len(runIDs))]
		placeholders := strings.TrimSuffix(strings.Repeat("?,", len(chunk)), ",")
		args := make([]any, 0, 2*len(chunk))
		for _, id := range chunk {
			args = append(args, id)
		}
		for _, id := range chunk {
			args = append(args, id)
		}
		known := map[string]bool{}
		rows, err := tx.QueryContext(ctx, `SELECT run_id, state FROM journal_runs WHERE run_id IN (`+placeholders+`)
			UNION ALL SELECT run_id, 'compacted' FROM journal_compacted WHERE run_id IN (`+placeholders+`)`, args...)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id, state string
			if err := rows.Scan(&id, &state); err != nil {
				rows.Close()
				return nil, err
			}
			known[id] = true
			if state == runAccepted || state == runUncertain {
				active[id] = true
			}
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		for _, id := range chunk {
			if !known[id] {
				active[id] = true
			}
		}
	}
	return active, nil
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
