package audit_test

import (
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
)

// A record Prune deleted stays deleted (#294). Its tombstone says the
// record existed and that retention removed it, so nothing writes it again:
// not a re-delivered reconciliation, not a compaction backfill, not a
// retried approval. A store that holds a record next to its own tombstone
// was written by something that ignored retention, and Verify reports it.

// prunedReconciliation decides a reconciliation under tenant A, completes
// its run and prunes its record. It returns the operation key.
func prunedReconciliation(t *testing.T, r *rig) string {
	t.Helper()
	op, _ := r.decideUnder(tenantA(r.ctx), "PRUNED-294")
	if err := r.journal.CompleteRun(r.ctx, r.operationRun(op.Key), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	r.requireRetired(1)
	return op.Key
}

// requireRetired: no record exists, the record counter agrees, and the
// given number of prune tombstones stand.
func (r *rig) requireRetired(tombstones int) {
	r.t.Helper()
	records := r.count(`SELECT COUNT(*) FROM audit_records_v1`)
	counter := r.count(`SELECT value FROM audit_meta_v1 WHERE name = 'records'`)
	pruned := r.count(`SELECT COUNT(*) FROM audit_pruned_v1`)
	if records != 0 || counter != 0 || pruned != tombstones {
		r.t.Fatalf("records=%d counter=%d tombstones=%d, want 0, 0, %d", records, counter, pruned, tombstones)
	}
}

// TestRedeliveringAPrunedReconciliationDoesNotRecreateItsRecord: the
// deciding tenant re-delivers a reconciliation whose record was pruned. It
// is answered the same duplicate as before the prune, and the record is not
// written again, mirrored, or counted; Verify passes with the tombstone
// alone, also after a restart.
func TestRedeliveringAPrunedReconciliationDoesNotRecreateItsRecord(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, original := r.decideUnder(tenantA(r.ctx), "PRUNED-294")
	want := original
	want.Duplicate = true
	if err := r.journal.CompleteRun(r.ctx, r.operationRun(op.Key), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	mirrored := r.mirror.accepted.Load()
	for _, stage := range []*rig{r, r.reopen()} {
		for range 2 {
			again, err := stage.redeliverAs(tenantA(stage.ctx), op.Key)
			if err != nil || !reflect.DeepEqual(again, want) {
				t.Fatalf("re-delivery after the prune=%+v err=%v, want %+v", again, err, want)
			}
		}
		stage.requireRetired(1)
		stage.mustVerify(0)
	}
	if got := r.mirror.accepted.Load(); got != mirrored {
		t.Fatalf("a re-delivery mirrored a pruned record (%d -> %d)", mirrored, got)
	}
}

// TestCompactingAPrunedReconciliationDoesNotRecreateItsRecord: compaction
// writes a reconciliation's missing record before it erases the actor. A
// record that is missing because Prune removed it is not missing in that
// sense, and is not written again.
func TestCompactingAPrunedReconciliationDoesNotRecreateItsRecord(t *testing.T) {
	r := newRig(t, rigOptions{})
	key := prunedReconciliation(t, r)
	report, err := r.journal.Compact(r.ctx, r.clock)
	if err != nil || report.RemovedRuns != 1 || report.ErasedReconciliations != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	r.requireRetired(1)
	r.mustVerify(0)
	erased, err := r.redeliverAs(tenantA(r.ctx), key)
	if err != nil || !erased.Duplicate || !erased.Erased {
		t.Fatalf("re-delivery after compaction=%+v err=%v", erased, err)
	}
	r.requireRetired(1)
	r.mustVerify(0)
}

// TestRetryingAPrunedApprovalDoesNotRecreateItsRecord: an approval retried
// with the identical decision after its record was pruned returns the
// decision and writes nothing.
func TestRetryingAPrunedApprovalDoesNotRecreateItsRecord(t *testing.T) {
	r := newRig(t, rigOptions{})
	completed, _ := r.settledEffect("approved-run")
	if err := r.journal.CompleteRun(r.ctx, completed, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	expires := r.clock.Add(2 * time.Hour)
	decide := func() error {
		_, err := r.approval.Record(r.ctx, "decision-294", proposal(completed), []string{"payment:write"}, expires, true)
		return err
	}
	if err := decide(); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	mirrored := r.mirror.accepted.Load()
	if err := decide(); err != nil {
		t.Fatalf("retry after the prune: %v", err)
	}
	r.requireRetired(1)
	r.mustVerify(0)
	if got := r.mirror.accepted.Load(); got != mirrored {
		t.Fatalf("a retry mirrored a pruned record (%d -> %d)", mirrored, got)
	}
}

// TestPrunedIDIsNotReusedByAnotherKind: an id Prune retired cannot carry a
// record of another kind either; that would be a different record under the
// same id, ErrConflict, and nothing is written.
func TestPrunedIDIsNotReusedByAnotherKind(t *testing.T) {
	r := newRig(t, rigOptions{})
	r.seedAudit(audit.Record{ID: "retired-294", Kind: audit.KindApproval, Actor: "reviewer:alice", Subject: "retired-294", Outcome: audit.OutcomeApproved, At: r.clock.Add(-time.Hour)})
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		_, inserted, err := r.audit.Append(r.ctx, tx, audit.Record{ID: "retired-294", Kind: audit.KindDeployment, Actor: "operator:bob", Subject: "a -> b", Action: "upgrade.retain-runs", Outcome: audit.OutcomeAccepted})
		if inserted {
			t.Error("a record of another kind was written under a pruned id")
		}
		return err
	})
	if !errors.Is(err, audit.ErrConflict) {
		t.Fatalf("append under a pruned id of another kind: %v, want ErrConflict", err)
	}
	r.requireRetired(1)
	if _, err := r.audit.Verify(r.ctx); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

// TestVerifyReportsARecordNextToItsTombstone: a pruned record put back
// verbatim, counter adjusted, passes every integrity and agreement check
// but one: its own tombstone says it was pruned. Verify reports ErrCorrupt,
// with and without owners. Removing the tombstone instead (the control)
// leaves an ordinary record, and Verify passes.
func TestVerifyReportsARecordNextToItsTombstone(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, _ := r.decideUnder(tenantA(r.ctx), "RESURRECTED-294")
	r.exec(`CREATE TABLE saved_records AS SELECT * FROM audit_records_v1`)
	if err := r.journal.CompleteRun(r.ctx, r.operationRun(op.Key), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	r.mustVerify(0)
	r.exec(`INSERT INTO audit_records_v1 SELECT * FROM saved_records`,
		`UPDATE audit_meta_v1 SET value = value + 1 WHERE name = 'records'`)
	if _, err := r.audit.Verify(r.ctx, r.approval, r.journal); !errors.Is(err, audit.ErrCorrupt) {
		t.Fatalf("verify with owners=%v, want ErrCorrupt", err)
	}
	if _, err := r.audit.Verify(r.ctx); !errors.Is(err, audit.ErrCorrupt) {
		t.Fatalf("verify without owners=%v, want ErrCorrupt", err)
	}
	r.exec(`DELETE FROM audit_pruned_v1`)
	r.mustVerify(1)
}
