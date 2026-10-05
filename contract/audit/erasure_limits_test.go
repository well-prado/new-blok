package audit_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store"
)

// TestMinimumRetentionKeepsYoungRunContent: a cutoff later than the legal
// minimum allows is clamped, so a run younger than the minimum keeps all
// of its content, and it is erased once it is old enough.
func TestMinimumRetentionKeepsYoungRunContent(t *testing.T) {
	r := newRig(t, rigOptions{runMinRetention: 30 * 24 * time.Hour})
	young := r.contentRun("YOUNG", true)
	r.clock = r.clock.Add(10 * 24 * time.Hour)
	report, err := r.journal.Compact(r.ctx, r.clock.Add(time.Hour))
	if err != nil || report.RemovedRuns != 0 {
		t.Fatalf("compact under the minimum=%+v err=%v", report, err)
	}
	kept, err := r.journal.Run(r.ctx, young.RunID)
	if err != nil || !strings.Contains(string(kept.Output), marker("YOUNG", "OUTPUT")) {
		t.Fatalf("young run=%+v err=%v", kept, err)
	}
	if again, err := r.journal.Reconcile(r.ctx, young.Operation.Key, "operator:bob", "x", []byte(`{}`), true); err != nil || again.Erased || !strings.Contains(again.Evidence, marker("YOUNG", "EVIDENCE")) {
		t.Fatalf("young reconciliation=%+v err=%v", again, err)
	}
	r.clock = r.clock.Add(21 * 24 * time.Hour)
	report, err = r.journal.Compact(r.ctx, r.clock)
	if err != nil || report.RemovedRuns != 1 || report.ErasedReconciliations != 1 || !report.LogPurged {
		t.Fatalf("compact past the minimum=%+v err=%v", report, err)
	}
	if found := fileMarkers(t, r.path, young.Markers); len(found) != 0 {
		t.Fatalf("content after the minimum: %v", found)
	}
	again, err := r.journal.Reconcile(r.ctx, young.Operation.Key, "operator:bob", "x", []byte(`{}`), true)
	if err != nil || !again.Duplicate || !again.Erased || again.State == "" {
		t.Fatalf("erased re-delivery=%+v err=%v", again, err)
	}
	if _, err := journal.New(r.ctx, r.db, journal.Config{MinRetention: -time.Second}); err == nil {
		t.Fatal("a negative minimum retention was accepted")
	}
}

// TestMigratedDatabaseNeedsOnePurgeOfFreeSpace: origin/main deleted rows
// without secure deletion, so their bytes stay in the file's free space,
// beyond any row. The migration cannot reach them; one PurgeFree does.
func TestMigratedDatabaseNeedsOnePurgeOfFreeSpace(t *testing.T) {
	path := legacyDatabase(t)
	r := openRig(t, path, rigOptions{})
	r.clock = legacyClock.Add(48 * time.Hour)
	if report, err := r.journal.Compact(r.ctx, legacyClock.Add(24*time.Hour)); err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	all := append(append([]string{erasureMarker}, legacyCompacted...), legacyReconciled...)
	residue := fileMarkers(t, path, all)
	if len(residue) == 0 {
		t.Fatal("fixture: origin/main's free-space residue is expected before PurgeFree")
	}
	purger, ok := store.PurgerOf(r.db)
	if !ok {
		t.Fatal("the SQLite store cannot purge")
	}
	if err := purger.PurgeFree(r.ctx); err != nil {
		t.Fatal(err)
	}
	if found := fileMarkers(t, path, all); len(found) != 0 {
		t.Fatalf("content after PurgeFree: %v", found)
	}
	r.mustVerify(2)
}

// TestLogPurgeWaitsForReaders: a reader's open snapshot keeps the log in
// use, so the purge reports it did not happen instead of claiming erasure.
func TestLogPurgeWaitsForReaders(t *testing.T) {
	r := newRig(t, rigOptions{busyTimeout: 100 * time.Millisecond})
	run := r.effectRun("READER")
	r.clock = r.clock.Add(48 * time.Hour)
	held := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- r.db.WithTx(context.Background(), func(tx *sql.Tx) error {
			var n int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM journal_runs`).Scan(&n); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	report, err := r.journal.Compact(r.ctx, r.clock)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err != nil || report.RemovedRuns != 1 || report.LogPurged {
		t.Fatalf("compact beside a reader=%+v err=%v", report, err)
	}
	purger, _ := store.PurgerOf(r.db)
	if err := purger.PurgeLog(r.ctx); err != nil {
		t.Fatal(err)
	}
	if found := fileMarkers(t, r.path, run.Markers); len(found) != 0 {
		t.Fatalf("content after the later purge: %v", found)
	}
}

// TestCompactionBackfillsAPreAuditReconciliationRecord: a reconciliation
// with no audit record (it predates audit) gets its record, under the
// system tenant, before compaction erases the actor that record needs.
// Afterwards it could not be reproduced, and Verify would mismatch forever.
func TestCompactionBackfillsAPreAuditReconciliationRecord(t *testing.T) {
	r := newRig(t, rigOptions{})
	run := r.contentRun("PREAUDIT", true)
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(r.ctx, `DELETE FROM audit_records_v1 WHERE kind = 'reconciliation.decision'`); err != nil {
			return err
		}
		_, err := tx.ExecContext(r.ctx, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.audit.Verify(r.ctx, r.journal); err == nil {
		t.Fatal("fixture: a reconciliation without its record must mismatch")
	}
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.journal.Compact(r.ctx, r.clock); err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	r.mustVerify(1)
	if n := r.count(`SELECT COUNT(*) FROM audit_records_v1 WHERE id = ? AND tenant = '' AND run_id = ?`, "reconcile:"+run.Operation.Key, run.RunID); n != 1 {
		t.Fatalf("backfilled system-tenant records=%d, want 1", n)
	}
	if found := r.rowMarkers(); len(found) != 0 {
		t.Fatalf("content after compaction: %v", found)
	}
}
