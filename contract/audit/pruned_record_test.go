package audit_test

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
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

// plantTombstone leaves a prune tombstone for id and kind that no Prune of
// this store wrote: a stale tombstone from a partial restore, or one put
// there by hand.
func (r *rig) plantTombstone(id string, kind audit.Kind) {
	r.t.Helper()
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.ctx, `INSERT INTO audit_pruned_v1 (id_digest, kind, pruned_at) VALUES (?, ?, ?)`, audit.Digest([]byte(id)), string(kind), r.clock.UnixNano())
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
}

// TestPrunedRowsUnderAnOlderRuleAreLeftAlone: a reconciliation decided
// under an older sensitivity rule ("ops pwd=abc" passed before #80's review
// added pwd) whose record was pruned. Its record cannot be written again
// under today's rule, and it is not meant to be: the tombstone answers
// first. Compacting it when it has no tenant (the row an upgrade leaves
// unowned), and re-delivering it as its owner, both succeed and write
// nothing, as compaction did for the unowned row before #294.
func TestPrunedRowsUnderAnOlderRuleAreLeftAlone(t *testing.T) {
	const legacyActor = "ops pwd=abc"
	if err := (audit.Record{ID: "x", Kind: audit.KindReconciliation, Actor: legacyActor, Subject: "x", Outcome: audit.OutcomeApplied, At: time.Now()}).Validate(); !errors.Is(err, audit.ErrSensitive) {
		t.Fatalf("fixture must be refused by today's rule, got %v", err)
	}
	t.Run("compacting the unowned row", func(t *testing.T) {
		r := newRig(t, rigOptions{})
		key := prunedReconciliation(t, r)
		r.exec(`UPDATE journal_reconciliations SET tenant = NULL, actor = '` + legacyActor + `'`)
		next := r.reopen()
		if n := next.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE tenant IS NULL`); n != 1 {
			t.Fatalf("unowned rows=%d, want 1", n)
		}
		report, err := next.journal.Compact(next.ctx, next.clock)
		if err != nil || report.RemovedRuns != 1 || report.ErasedReconciliations != 1 {
			t.Fatalf("compact=%+v err=%v", report, err)
		}
		next.requireRetired(1)
		next.mustVerify(0)
		if n := next.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE operation_key = ? AND erased_at IS NOT NULL`, key); n != 1 {
			t.Fatalf("erased reconciliations=%d, want 1", n)
		}
	})
	t.Run("re-delivering the owned row", func(t *testing.T) {
		r := newRig(t, rigOptions{})
		key := prunedReconciliation(t, r)
		r.exec(`UPDATE journal_reconciliations SET actor = '` + legacyActor + `'`)
		again, err := r.redeliverAs(tenantA(r.ctx), key)
		if err != nil || !again.Duplicate || again.Actor != legacyActor {
			t.Fatalf("re-delivery=%+v err=%v", again, err)
		}
		r.requireRetired(1)
		r.mustVerify(0)
	})
}

// TestATombstoneDoesNotSilenceAFirstDecision: a decision that has never
// been recorded meets a tombstone for its own record id, which no Prune of
// this store could have written (a stale tombstone from a partial restore,
// or one planted by hand). The decision is refused, ErrConflict, with
// nothing applied, written or mirrored: a decision without its record is
// exactly what audit exists to prevent. Removing the tombstone (the
// control) lets the same decision commit with its record.
func TestATombstoneDoesNotSilenceAFirstDecision(t *testing.T) {
	t.Run("reconciliation", func(t *testing.T) {
		r := newRig(t, rigOptions{})
		_, op := r.uncertainEffect("PLANTED-294")
		r.plantTombstone("reconcile:"+op.Key, audit.KindReconciliation)
		mirrored := r.mirror.accepted.Load()
		decide := func() (journal.Reconciliation, error) {
			return r.journal.Reconcile(tenantA(r.ctx), op.Key, "operator:planted", "provider lookup", []byte(`{"receipt":"r-1"}`), true)
		}
		if got, err := decide(); !errors.Is(err, audit.ErrConflict) {
			t.Fatalf("reconcile under a planted tombstone=%+v err=%v, want ErrConflict", got, err)
		}
		if n := r.count(`SELECT COUNT(*) FROM journal_reconciliations`); n != 0 {
			t.Fatalf("a refused reconciliation was applied (%d rows)", n)
		}
		r.requireRetired(1)
		if got := r.mirror.accepted.Load(); got != mirrored {
			t.Fatalf("a refused reconciliation was mirrored (%d -> %d)", mirrored, got)
		}
		r.exec(`DELETE FROM audit_pruned_v1`)
		if got, err := decide(); err != nil || got.Duplicate {
			t.Fatalf("reconcile after the tombstone is gone=%+v err=%v", got, err)
		}
		r.mustVerify(1)
	})
	t.Run("approval", func(t *testing.T) {
		r := newRig(t, rigOptions{})
		completed, _ := r.settledEffect("approved-run")
		r.plantTombstone("approval:decision-planted", audit.KindApproval)
		mirrored := r.mirror.accepted.Load()
		decide := func() error {
			_, err := r.approval.Record(r.ctx, "decision-planted", proposal(completed), []string{"payment:write"}, r.clock.Add(time.Hour), true)
			return err
		}
		if err := decide(); !errors.Is(err, audit.ErrConflict) {
			t.Fatalf("approval under a planted tombstone: %v, want ErrConflict", err)
		}
		if n := r.count(`SELECT COUNT(*) FROM approval_decisions_v1`); n != 0 {
			t.Fatalf("a refused approval was recorded (%d rows)", n)
		}
		r.requireRetired(1)
		if got := r.mirror.accepted.Load(); got != mirrored {
			t.Fatalf("a refused approval was mirrored (%d -> %d)", mirrored, got)
		}
		r.exec(`DELETE FROM audit_pruned_v1`)
		if err := decide(); err != nil {
			t.Fatalf("approval after the tombstone is gone: %v", err)
		}
		r.mustVerify(1)
	})
}

// TestVerifyReportsARecordNextToATombstoneOfAnotherKind: Append refuses a
// record of another kind under a pruned id (ErrConflict), so a record next
// to a tombstone of its id digest is corrupt whatever kind the tombstone
// names.
func TestVerifyReportsARecordNextToATombstoneOfAnotherKind(t *testing.T) {
	r := newRig(t, rigOptions{})
	r.seedAudit(audit.Record{ID: "mixed-294", Kind: audit.KindDeployment, Actor: "operator:bob", Subject: "a -> b", Action: "upgrade.retain-runs", Outcome: audit.OutcomeAccepted})
	r.plantTombstone("other-294", audit.KindDeployment)
	r.mustVerify(1)
	r.plantTombstone("mixed-294", audit.KindApproval)
	if _, err := r.audit.Verify(r.ctx); !errors.Is(err, audit.ErrCorrupt) {
		t.Fatalf("verify=%v, want ErrCorrupt", err)
	}
}

// TestPruneRepairsARecordRecreatedNextToItsTombstone: the repair for a store
// in which a binary before #294 re-created a pruned record. Such a record is
// written as a new row with the record's original time, so it is as old as
// it was when it was pruned, and the next Prune removes it again: Verify
// names it, one Prune removes it and refreshes its tombstone, and Verify
// passes with the tombstone alone.
func TestPruneRepairsARecordRecreatedNextToItsTombstone(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, _ := r.decideUnder(tenantA(r.ctx), "RECREATED-294")
	id := "reconcile:" + op.Key
	r.exec(`CREATE TABLE saved_records AS SELECT * FROM audit_records_v1`)
	if err := r.journal.CompleteRun(r.ctx, r.operationRun(op.Key), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	// What the older binary's Append did on re-delivery: the same encoded
	// record (original time) as a new row, the next tenant sequence, and
	// both counters bumped.
	r.exec(`UPDATE audit_meta_v1 SET value = value + 1 WHERE name IN ('records', 'tenant:tenant-a')`,
		`INSERT INTO audit_records_v1 (id, kind, tenant, tenant_seq, run_id, recorded_at, record, digest)
			SELECT id, kind, tenant, (SELECT value FROM audit_meta_v1 WHERE name = 'tenant:tenant-a'), run_id, recorded_at, record, digest FROM saved_records`)
	_, err := r.audit.Verify(r.ctx, r.approval, r.journal)
	if !errors.Is(err, audit.ErrCorrupt) || !strings.Contains(err.Error(), id) {
		t.Fatalf("verify=%v, want ErrCorrupt naming %q", err, id)
	}
	r.clock = r.clock.Add(time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("repairing prune=%+v err=%v", report, err)
	}
	r.requireRetired(1)
	r.mustVerify(0)
	if again, err := r.redeliverAs(tenantA(r.ctx), op.Key); err != nil || !again.Duplicate {
		t.Fatalf("re-delivery after the repair=%+v err=%v", again, err)
	}
	r.requireRetired(1)
	r.mustVerify(0)
}

// TestVerifyFindsATombstonedRecordInAnyBatch: Verify looks tombstones up in
// batches of 256 records. A record next to its tombstone is found and named
// wherever it falls: the first record, either side of a batch boundary, and
// the last, in a partial batch.
func TestVerifyFindsATombstonedRecordInAnyBatch(t *testing.T) {
	const records = 2*256 + 1
	r := newRig(t, rigOptions{})
	id := func(i int) string { return "batch-294-" + strconv.Itoa(i) }
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		for i := range records {
			if _, _, err := r.audit.Append(r.ctx, tx, audit.Record{ID: id(i), Kind: audit.KindDeployment, Actor: "operator:bob", Subject: "a -> b", Action: "upgrade.retain-runs", Outcome: audit.OutcomeAccepted}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	r.mustVerify(records)
	for _, i := range []int{0, 255, 256, 511, 512} {
		r.plantTombstone(id(i), audit.KindDeployment)
		_, err := r.audit.Verify(r.ctx)
		if !errors.Is(err, audit.ErrCorrupt) || !strings.Contains(err.Error(), strconv.Quote(id(i))) {
			t.Fatalf("record %d next to its tombstone: verify=%v, want ErrCorrupt naming %q", i, err, id(i))
		}
		r.exec(`DELETE FROM audit_pruned_v1`)
	}
	r.mustVerify(records)
}
