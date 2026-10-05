package audit_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
)

// insertLegacyRow writes a record row exactly as an earlier release would
// have: correct digest, columns and counters, but content today's Validate
// refuses (a rule that was widened after the record was accepted).
func (r *rig) insertLegacyRow(record audit.Record) {
	r.t.Helper()
	encoded := `{"id":"` + record.ID + `","kind":"` + string(record.Kind) + `","actor":"` + record.Actor + `","subject":"` + record.Subject + `","outcome":"` + string(record.Outcome) + `","at":"` + record.At.UTC().Format(time.RFC3339Nano) + `"}`
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.ctx, `INSERT INTO audit_meta_v1 (name, value) VALUES ('tenant:', 1) ON CONFLICT(name) DO UPDATE SET value = value + 1`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(r.ctx, `INSERT INTO audit_records_v1 (id, kind, tenant, tenant_seq, run_id, recorded_at, record, digest) VALUES (?, ?, '', (SELECT value FROM audit_meta_v1 WHERE name = 'tenant:'), '', ?, ?, ?)`,
			record.ID, string(record.Kind), record.At.UnixNano(), []byte(encoded), audit.Digest([]byte(encoded))); err != nil {
			return err
		}
		_, err := tx.ExecContext(r.ctx, `UPDATE audit_meta_v1 SET value = value + 1 WHERE name = 'records'`)
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
}

// TestRecordsAcceptedUnderAnOlderRuleStayReadable: the sensitivity rule
// widened after a record was accepted ("ops pwd=abc" was not credential-
// shaped before #80's review added pwd). Reads verify integrity only, so the
// record stays verifiable, listable and prunable instead of making every
// Verify, List and Prune fail and the store fill up.
func TestRecordsAcceptedUnderAnOlderRuleStayReadable(t *testing.T) {
	r := newRig(t, rigOptions{})
	legacy := audit.Record{ID: "legacy-1", Kind: audit.KindDeployment, Actor: "ops pwd=abc", Subject: "legacy", Outcome: audit.OutcomeAccepted, At: r.clock.Add(-48 * time.Hour)}
	if err := legacy.Validate(); !errors.Is(err, audit.ErrSensitive) {
		t.Fatalf("fixture must be refused by today's rule, got %v", err)
	}
	r.insertLegacyRow(legacy)
	if n, err := r.audit.Verify(r.ctx); err != nil || n != 1 {
		t.Fatalf("verify=%d err=%v", n, err)
	}
	records, _, err := r.audit.List(asReader(r.ctx, "auditor:all"), "", "", 0)
	if err != nil || len(records) != 1 || records[0].ID != "legacy-1" {
		t.Fatalf("list=%+v err=%v", records, err)
	}
	report, err := r.audit.Prune(r.ctx, r.clock, r.journal)
	if err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
}

// TestVerifyCrossChecksAuditAgainstDurableState: integrity alone cannot
// tell an empty or thinned audit table from a correct one. With the
// owners composed, Verify refuses a store whose decisions lack their
// records (tables dropped and recreated, or one row deleted with its
// counter adjusted) and a record whose decision is gone, while a pruned
// record's tombstone is accepted.
func TestVerifyCrossChecksAuditAgainstDurableState(t *testing.T) {
	exec := func(r *rig, statements ...string) {
		t.Helper()
		if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
			for _, statement := range statements {
				if _, err := tx.ExecContext(r.ctx, statement); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	setup := func(t *testing.T) (*rig, string) {
		r := openRig(t, t.TempDir()+"/journal.db", rigOptions{})
		for _, id := range []string{"a", "b", "c"} {
			if _, err := r.approve("decision-"+id, "run-1", true); err != nil {
				t.Fatal(err)
			}
		}
		_, op := r.uncertainEffect("cross")
		if err := r.reconcile(op, "operator:bob"); err != nil {
			t.Fatal(err)
		}
		if n, err := r.audit.Verify(r.ctx, r.approval, r.journal); err != nil || n != 4 {
			t.Fatalf("intact store verify=%d err=%v", n, err)
		}
		return r, r.path
	}
	t.Run("tables dropped and recreated", func(t *testing.T) {
		r, path := setup(t)
		exec(r, `DROP TABLE audit_records_v1`, `DROP TABLE audit_meta_v1`, `DROP TABLE audit_pruned_v1`)
		reopened := openRig(t, path, rigOptions{})
		if n, err := reopened.audit.Verify(reopened.ctx); err != nil || n != 0 {
			t.Fatalf("integrity-only verify=%d err=%v (empty tables are internally consistent)", n, err)
		}
		if !reopened.authorizes("decision-a", "run-1") {
			t.Fatal("fixture: the decision must still authorize")
		}
		if _, err := reopened.audit.Verify(reopened.ctx, reopened.approval, reopened.journal); !errors.Is(err, audit.ErrMismatch) {
			t.Fatalf("cross-checked verify err=%v, want ErrMismatch", err)
		}
	})
	t.Run("row deleted with counter adjusted", func(t *testing.T) {
		r, _ := setup(t)
		exec(r, `DELETE FROM audit_records_v1 WHERE id = 'approval:decision-b'`, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
		if _, err := r.audit.Verify(r.ctx); err != nil {
			t.Fatalf("integrity-only verify err=%v", err)
		}
		if _, err := r.audit.Verify(r.ctx, r.approval); !errors.Is(err, audit.ErrMismatch) {
			t.Fatalf("approval cross-check err=%v, want ErrMismatch", err)
		}
	})
	t.Run("reconciliation record deleted", func(t *testing.T) {
		r, _ := setup(t)
		exec(r, `DELETE FROM audit_records_v1 WHERE kind = 'reconciliation.decision'`, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
		if _, err := r.audit.Verify(r.ctx, r.journal); !errors.Is(err, audit.ErrMismatch) {
			t.Fatalf("reconciliation cross-check err=%v, want ErrMismatch", err)
		}
	})
	t.Run("record without its decision", func(t *testing.T) {
		r, _ := setup(t)
		exec(r, `DELETE FROM approval_decisions_v1 WHERE id = 'decision-c'`)
		if _, err := r.audit.Verify(r.ctx, r.approval); !errors.Is(err, audit.ErrMismatch) {
			t.Fatalf("ghost record err=%v, want ErrMismatch", err)
		}
	})
	t.Run("pruned record tombstone accepted", func(t *testing.T) {
		r := newRig(t, rigOptions{})
		completed, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "done", Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.journal.CompleteRun(r.ctx, completed.RunID, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := r.approve("decision-old", completed.RunID, true); err != nil {
			t.Fatal(err)
		}
		r.clock = r.clock.Add(time.Hour)
		if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
			t.Fatalf("prune=%+v err=%v", report, err)
		}
		if n, err := r.audit.Verify(r.ctx, r.approval, r.journal); err != nil || n != 0 {
			t.Fatalf("verify after prune=%d err=%v", n, err)
		}
	})
}

// TestPruneRetentionBoundaries pins the prune rules the first mutation
// review found unguarded: the cutoff is strict, uncertain runs are active,
// a run the journal does not know is active unless a compaction tombstone
// proves it ended, and a hold that panics keeps the record.
func TestPruneRetentionBoundaries(t *testing.T) {
	panicking := func(audit.Record) bool { panic("synthetic hold failure") }
	r := newRig(t, rigOptions{hold: func(rec audit.Record) bool {
		if rec.Reason == "hold_panics" {
			return panicking(rec)
		}
		return false
	}})
	uncertain, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "uncertain", Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.journal.MarkRunUncertain(r.ctx, uncertain.RunID, "outcome_unknown", "uncertain"); err != nil {
		t.Fatal(err)
	}
	compacted, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "compacted", Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.journal.CompleteRun(r.ctx, compacted.RunID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if report, err := r.journal.Compact(r.ctx, r.clock.Add(time.Second)); err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	cutoff := r.clock.Add(-time.Hour)
	record := func(id, runID, reason string, at time.Time) audit.Record {
		return audit.Record{ID: id, Kind: audit.KindApproval, Actor: "reviewer:alice", Subject: id, RunID: runID, Outcome: audit.OutcomeApproved, Reason: reason, At: at}
	}
	r.seedAudit(record("at-cutoff", "", "", cutoff))
	r.seedAudit(record("before-cutoff", "", "", cutoff.Add(-time.Nanosecond)))
	r.seedAudit(record("uncertain-run", uncertain.RunID, "", cutoff.Add(-time.Hour)))
	r.seedAudit(record("cluster-run", "cluster-run-7", "", cutoff.Add(-time.Hour)))
	r.seedAudit(record("compacted-run", compacted.RunID, "", cutoff.Add(-time.Hour)))
	r.seedAudit(record("hold-panics", "", "hold_panics", cutoff.Add(-time.Hour)))
	report, err := r.audit.Prune(r.ctx, cutoff, r.journal)
	if err != nil {
		t.Fatal(err)
	}
	records, _, err := r.audit.List(asReader(r.ctx, "auditor:all"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	kept := []string{}
	for _, item := range records {
		kept = append(kept, item.ID)
	}
	if strings.Join(kept, ",") != "at-cutoff,uncertain-run,cluster-run,hold-panics" || report.Removed != 2 || report.KeptActive != 2 || report.KeptHeld != 1 {
		t.Fatalf("kept=%v report=%+v; want at-cutoff, uncertain-run, cluster-run, hold-panics kept and before-cutoff, compacted-run removed", kept, report)
	}
}

// TestRunCompactionHoldThatPanicsKeepsTheRun: the run-data legal hold fails
// closed too.
func TestRunCompactionHoldThatPanicsKeepsTheRun(t *testing.T) {
	r := newRig(t, rigOptions{runHold: func(journal.RetainedRun) bool { panic("synthetic hold failure") }})
	run, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "held", Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.journal.CompleteRun(r.ctx, run.RunID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	report, err := r.journal.Compact(r.ctx, r.clock.Add(time.Hour))
	if err != nil || report.RemovedRuns != 0 || report.HeldRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	if _, err := r.journal.Run(r.ctx, run.RunID); err != nil {
		t.Fatalf("run deleted under a panicking hold: %v", err)
	}
}

// TestRedeliveredReconciliationBackfillsItsRecord: a reconciliation whose
// record is missing (it predates audit) gets it on re-delivery, and the
// re-delivery of a recorded one is a no-op that is not mirrored twice.
func TestRedeliveredReconciliationBackfillsItsRecord(t *testing.T) {
	r := newRig(t, rigOptions{})
	_, op := r.uncertainEffect("backfill")
	if err := r.reconcile(op, "operator:bob"); err != nil {
		t.Fatal(err)
	}
	if err := r.reconcile(op, "operator:bob"); err != nil {
		t.Fatal(err)
	}
	if got := r.mirror.accepted.Load(); got != 1 {
		t.Fatalf("mirrored %d copies of one reconciliation", got)
	}
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.ctx, `DELETE FROM audit_records_v1 WHERE kind = 'reconciliation.decision'`); err != nil {
			return err
		}
		_, err := tx.ExecContext(r.ctx, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.audit.Verify(r.ctx, r.journal); !errors.Is(err, audit.ErrMismatch) {
		t.Fatalf("fixture: missing record must mismatch, got %v", err)
	}
	if err := r.reconcile(op, "operator:bob"); err != nil {
		t.Fatal(err)
	}
	if _, err := r.audit.Verify(r.ctx, r.journal); err != nil {
		t.Fatalf("re-delivery did not backfill: %v", err)
	}
	if got := r.mirror.accepted.Load(); got != 2 {
		t.Fatalf("backfilled record mirrored %d times in total, want 2", got-1)
	}
}

// TestApprovalRetryIsNotMirroredTwice: an idempotent approval retry writes
// nothing and offers nothing.
func TestApprovalRetryIsNotMirroredTwice(t *testing.T) {
	r := newRig(t, rigOptions{})
	for range 3 {
		if _, err := r.approve("decision-retry", "run-1", true); err != nil {
			t.Fatal(err)
		}
	}
	if got := r.mirror.accepted.Load(); got != 1 {
		t.Fatalf("mirrored %d copies of one decision", got)
	}
}

// TestFullStoreIsDistinguishableFromAnOutage: both refuse the decision and
// both are ErrUnavailable, but a full store says so and is counted apart.
func TestFullStoreIsDistinguishableFromAnOutage(t *testing.T) {
	full := newRig(t, rigOptions{maxRecords: 1})
	if _, err := full.approve("first", "run-1", true); err != nil {
		t.Fatal(err)
	}
	_, err := full.approve("second", "run-1", true)
	if !errors.Is(err, audit.ErrUnavailable) || !errors.Is(err, audit.ErrCapacity) || err.Error() != "audit: durable audit unavailable: capacity exhausted" {
		t.Fatalf("full store err=%v", err)
	}
	if stats := full.audit.Stats(); stats.Refused != 1 || stats.RefusedCapacity != 1 {
		t.Fatalf("full store stats=%+v", stats)
	}
	down := newRig(t, rigOptions{})
	down.breakAudit()
	_, err = down.approve("first", "run-1", true)
	if !errors.Is(err, audit.ErrUnavailable) || errors.Is(err, audit.ErrCapacity) || err.Error() != "audit: durable audit unavailable" || strings.Contains(err.Error(), "synthetic audit outage") {
		t.Fatalf("outage err=%v", err)
	}
	if stats := down.audit.Stats(); stats.Refused != 1 || stats.RefusedCapacity != 0 {
		t.Fatalf("outage stats=%+v", stats)
	}
}

// TestListCursorIsPerTenant: a tenant's cursor counts only its own records,
// so it reveals nothing about how much other tenants write.
func TestListCursorIsPerTenant(t *testing.T) {
	r := newRig(t, rigOptions{readers: tenantReaders{"auditor:a": {"tenant-a"}}})
	write := func(tenant, id string) {
		ctx := audit.WithTenant(r.ctx, tenant)
		if _, err := r.approval.Record(ctx, id, proposal("run-1"), []string{"payment:write"}, r.clock.Add(time.Hour), true); err != nil {
			t.Fatal(err)
		}
	}
	write("tenant-a", "a-1")
	for i := range 40 {
		write("tenant-b", "b-"+string(rune('a'+i%26))+string(rune('a'+i/26)))
	}
	write("tenant-a", "a-2")
	write("tenant-a", "a-3")
	ctx := context.WithValue(r.ctx, readerKey{}, "auditor:a")
	cursor := ""
	for i, want := range []struct{ subject, next string }{{"a-1", "1"}, {"a-2", "2"}, {"a-3", ""}} {
		page, next, err := r.audit.List(ctx, "tenant-a", cursor, 1)
		if err != nil || len(page) != 1 || page[0].Subject != want.subject || next != want.next {
			t.Fatalf("page %d=%+v next=%q err=%v; want %s next %q", i, page, next, err, want.subject, want.next)
		}
		cursor = next
	}
}

// TestPruneTombstonesHoldDigestsAndKinds: a tombstone stores the sha256 of
// the record id, never the id (approval ids are application-chosen and may
// carry personal data), and Verify accepts only a tombstone of the right
// digest and kind: a wrong-kind or raw-id tombstone does not stand in for a
// missing record.
func TestPruneTombstonesHoldDigestsAndKinds(t *testing.T) {
	r := newRig(t, rigOptions{})
	completed, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "done", Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.journal.CompleteRun(r.ctx, completed.RunID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	personal := "decision-for-jane.doe@example.test"
	if _, err := r.approve(personal, completed.RunID, true); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	var stored, kind string
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(r.ctx, `SELECT id_digest, kind FROM audit_pruned_v1`).Scan(&stored, &kind)
	}); err != nil {
		t.Fatal(err)
	}
	if stored != audit.Digest([]byte("approval:"+personal)) || kind != string(audit.KindApproval) || strings.Contains(stored, "jane") {
		t.Fatalf("tombstone=%q kind=%q", stored, kind)
	}
	if _, err := r.audit.Verify(r.ctx, r.approval); err != nil {
		t.Fatalf("verify with a correct tombstone: %v", err)
	}
	for name, statement := range map[string]string{
		"wrong kind": `UPDATE audit_pruned_v1 SET kind = 'deployment.decision'`,
		"raw id":     `UPDATE audit_pruned_v1 SET id_digest = 'approval:` + personal + `', kind = 'approval.decision'`,
	} {
		if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error { _, err := tx.ExecContext(r.ctx, statement); return err }); err != nil {
			t.Fatal(err)
		}
		if _, err := r.audit.Verify(r.ctx, r.approval); !errors.Is(err, audit.ErrMismatch) {
			t.Fatalf("%s tombstone satisfied Verify: %v", name, err)
		}
	}
}
