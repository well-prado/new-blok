package audit_test

import (
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store/sqlite"
)

// A reconciliation belongs to the tenant that decided it (#286). Only that
// tenant may repeat it and read back its evidence and provider result; to
// any other tenant the operation looks exactly like one that settled
// without a reconciliation. A record a re-delivery or compaction backfills
// takes the decision's tenant, or the system tenant "" when it is unknown,
// never the tenant of whoever happens to re-deliver or compact.

const tenantMarker = "SYNTHETIC-286-"

var (
	tenantA = func(ctx context.Context) context.Context { return audit.WithTenant(ctx, "tenant-a") }
	tenantB = func(ctx context.Context) context.Context { return audit.WithTenant(ctx, "tenant-b") }
)

// decideUnder reconciles an uncertain effect of a fresh run under ctx, with
// evidence and result marked by label.
func (r *rig) decideUnder(ctx context.Context, label string) (journal.Operation, journal.Reconciliation) {
	r.t.Helper()
	_, op := r.uncertainEffect(label)
	decided, err := r.journal.Reconcile(ctx, op.Key, "operator:"+label, "provider lookup "+tenantMarker+label+"-EVIDENCE", []byte(`{"receipt":"`+tenantMarker+label+`-RESULT"}`), true)
	if err != nil || decided.Duplicate {
		r.t.Fatalf("reconcile %s=%+v err=%v", label, decided, err)
	}
	return op, decided
}

// settledEffect commits an effect of a fresh run normally: an operation that
// is no longer uncertain and was never reconciled.
func (r *rig) settledEffect(label string) (string, journal.Operation) {
	r.t.Helper()
	run, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: label, Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		r.t.Fatal(err)
	}
	op, err := r.journal.BeginEffect(r.ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: run.RunID, ArtifactDigest: digest("artifact"), InvocationPath: "charge-" + label, IterationPath: "root"}})
	if err != nil {
		r.t.Fatal(err)
	}
	attempt, err := r.journal.StartAttempt(r.ctx, op.Key)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.CommitEffect(r.ctx, journal.EffectCommit{OperationKey: op.Key, AttemptID: attempt.ID, Result: []byte(`{"charged":true}`)}); err != nil {
		r.t.Fatal(err)
	}
	return run.RunID, op
}

// dropReconciliationRecords makes every reconciliation pre-audit: its row
// stays, its record is gone, as in a journal written before audit existed.
func (r *rig) dropReconciliationRecords() {
	r.t.Helper()
	r.exec(`UPDATE audit_meta_v1 SET value = value - (SELECT COUNT(*) FROM audit_records_v1 WHERE kind = 'reconciliation.decision') WHERE name = 'records'`,
		`DELETE FROM audit_records_v1 WHERE kind = 'reconciliation.decision'`)
	if _, err := r.audit.Verify(r.ctx, r.journal); !errors.Is(err, audit.ErrMismatch) {
		r.t.Fatalf("fixture: a reconciliation without its record must mismatch, got %v", err)
	}
}

func (r *rig) exec(statements ...string) {
	r.t.Helper()
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		for _, statement := range statements {
			if _, err := tx.ExecContext(r.ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) records(tenant string) int {
	r.t.Helper()
	return r.count(`SELECT COUNT(*) FROM audit_records_v1 WHERE kind = 'reconciliation.decision' AND tenant = ?`, tenant)
}

// redeliverAs re-delivers op under ctx with evidence and a result of its
// own, which a duplicate must never adopt.
func (r *rig) redeliverAs(ctx context.Context, key string) (journal.Reconciliation, error) {
	return r.journal.Reconcile(ctx, key, "operator:redelivery", "redelivered evidence", []byte(`{"redelivered":true}`), true)
}

// requireIndistinguishable: the response to another tenant's reconciliation
// is the response to an operation that was never reconciled, field for
// field and error for error, so it carries no evidence or result and says
// nothing a settled operation would not.
func requireIndistinguishable(t *testing.T, got journal.Reconciliation, gotErr error, control journal.Reconciliation, controlErr error) {
	t.Helper()
	if gotErr == nil || controlErr == nil || gotErr.Error() != controlErr.Error() || !reflect.DeepEqual(got, control) {
		t.Fatalf("cross-tenant re-delivery=%+v err=%v; never-reconciled control=%+v err=%v", got, gotErr, control, controlErr)
	}
	if got.Actor != "" || got.Evidence != "" || len(got.Result) != 0 || got.Duplicate || strings.Contains(got.Evidence+string(got.Result), tenantMarker) {
		t.Fatalf("cross-tenant re-delivery carried content: %+v", got)
	}
}

// TestCrossTenantRedeliveryOfAPreAuditReconciliation: tenant A's
// reconciliation has no audit record (it predates audit). Tenant B
// re-delivers it: B gets what it would get for an operation that settled
// without one, and nothing is written, so no record lands under B. A's own
// re-delivery still returns the original and backfills the record under A.
func TestCrossTenantRedeliveryOfAPreAuditReconciliation(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, original := r.decideUnder(tenantA(r.ctx), "A")
	_, settled := r.settledEffect("settled")
	r.dropReconciliationRecords()
	mirrored := r.mirror.accepted.Load()

	got, err := r.redeliverAs(tenantB(r.ctx), op.Key)
	control, controlErr := r.redeliverAs(tenantB(r.ctx), settled.Key)
	requireIndistinguishable(t, got, err, control, controlErr)
	if !errors.Is(err, journal.ErrNotReconciliable) {
		t.Fatalf("cross-tenant err=%v, want ErrNotReconciliable", err)
	}
	if n := r.records("tenant-b"); n != 0 {
		t.Fatalf("tenant B re-delivery filed %d records under tenant B", n)
	}
	if n := r.count(`SELECT COUNT(*) FROM audit_records_v1`); n != 0 || r.mirror.accepted.Load() != mirrored {
		t.Fatalf("a refused re-delivery wrote %d records", n)
	}

	again, err := r.redeliverAs(tenantA(r.ctx), op.Key)
	want := original
	want.Duplicate = true
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("own-tenant re-delivery=%+v err=%v, want the original %+v", again, err, want)
	}
	if r.records("tenant-a") != 1 || r.records("tenant-b") != 0 || r.records("") != 0 {
		t.Fatalf("backfill records a=%d b=%d system=%d, want it under tenant A only", r.records("tenant-a"), r.records("tenant-b"), r.records(""))
	}
	r.mustVerify(1)
}

// TestOwnTenantRedeliveryReturnsTheOriginal: the duplicate contract is
// unchanged for the deciding tenant, the system tenant included: the
// original actor, evidence and result, whatever the re-delivery carries,
// and no second record.
func TestOwnTenantRedeliveryReturnsTheOriginal(t *testing.T) {
	for _, tenant := range []string{"", "tenant-a"} {
		t.Run("tenant="+tenant, func(t *testing.T) {
			r := newRig(t, rigOptions{})
			ctx := audit.WithTenant(r.ctx, tenant)
			op, original := r.decideUnder(ctx, "OWN")
			again, err := r.redeliverAs(ctx, op.Key)
			want := original
			want.Duplicate = true
			if err != nil || !reflect.DeepEqual(again, want) || !strings.Contains(again.Evidence, tenantMarker+"OWN-EVIDENCE") {
				t.Fatalf("own re-delivery=%+v err=%v, want %+v", again, err, want)
			}
			if r.records(tenant) != 1 || r.mirror.accepted.Load() != 1 {
				t.Fatalf("records=%d mirrored=%d, want 1 and 1", r.records(tenant), r.mirror.accepted.Load())
			}
			r.mustVerify(1)
		})
	}
}

// TestRedeliveredReconciliationUnderAnotherTenantIsRefused: with its record
// intact, another tenant's re-delivery, the system tenant's included, is
// refused like a re-delivery of a settled operation, and the original
// record is not rewritten.
func TestRedeliveredReconciliationUnderAnotherTenantIsRefused(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, _ := r.decideUnder(tenantA(r.ctx), "RECORDED")
	_, settled := r.settledEffect("settled")
	for _, ctx := range []context.Context{tenantB(r.ctx), r.ctx} {
		got, err := r.redeliverAs(ctx, op.Key)
		control, controlErr := r.redeliverAs(ctx, settled.Key)
		requireIndistinguishable(t, got, err, control, controlErr)
	}
	if r.records("tenant-a") != 1 || r.records("tenant-b") != 0 || r.records("") != 0 {
		t.Fatal("the original record was rewritten or another was added")
	}
	r.mustVerify(1)

	// Reconcile commits the operation with its reconciliation, so a
	// reconciled operation is never uncertain. Should one read uncertain
	// anyway, another tenant still cannot decide it a second time.
	r.exec(`UPDATE journal_operations SET state = 'uncertain' WHERE operation_key = '` + op.Key + `'`)
	before := r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE operation_key = ? AND actor = 'operator:RECORDED' AND tenant = 'tenant-a'`, op.Key)
	if got, err := r.redeliverAs(tenantB(r.ctx), op.Key); !errors.Is(err, journal.ErrNotReconciliable) || !reflect.DeepEqual(got, journal.Reconciliation{}) {
		t.Fatalf("re-delivery of an uncertain-reading reconciled operation=%+v err=%v", got, err)
	}
	if after := r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE operation_key = ? AND actor = 'operator:RECORDED' AND tenant = 'tenant-a'`, op.Key); before != 1 || after != 1 {
		t.Fatalf("original reconciliation changed: before=%d after=%d", before, after)
	}
}

// TestCompactionBackfillsUnderTheDecisionsTenant: compaction writes a
// missing record before it erases the actor (§7). It takes the decision's
// tenant, not the tenant of the operator running Compact. After erasure,
// another tenant's re-delivery still learns only what it would of a
// compacted operation that was never reconciled; the deciding tenant gets
// the erased duplicate.
func TestCompactionBackfillsUnderTheDecisionsTenant(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, _ := r.decideUnder(tenantA(r.ctx), "COMPACT")
	settledRun, settled := r.settledEffect("settled")
	runID := r.operationRun(op.Key)
	for _, id := range []string{runID, settledRun} {
		if err := r.journal.CompleteRun(r.ctx, id, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	r.dropReconciliationRecords()
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.journal.Compact(tenantB(r.ctx), r.clock); err != nil || report.RemovedRuns != 2 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	if r.records("tenant-a") != 1 || r.records("tenant-b") != 0 || r.records("") != 0 {
		t.Fatalf("compaction backfill records a=%d b=%d system=%d, want it under tenant A", r.records("tenant-a"), r.records("tenant-b"), r.records(""))
	}
	r.mustVerify(1)
	got, err := r.redeliverAs(tenantB(r.ctx), op.Key)
	control, controlErr := r.redeliverAs(tenantB(r.ctx), settled.Key)
	requireIndistinguishable(t, got, err, control, controlErr)
	erased, err := r.redeliverAs(tenantA(r.ctx), op.Key)
	if err != nil || !erased.Duplicate || !erased.Erased || erased.State == "" || erased.Actor != "" || erased.Evidence != "" || len(erased.Result) != 0 {
		t.Fatalf("own-tenant erased re-delivery=%+v err=%v", erased, err)
	}
}

func (r *rig) operationRun(key string) string {
	r.t.Helper()
	var runID string
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(r.ctx, `SELECT run_id FROM journal_operations WHERE operation_key = ?`, key).Scan(&runID)
	}); err != nil {
		r.t.Fatal(err)
	}
	return runID
}

// reopen opens the rig's database again, as the next process would: the
// schema transaction runs, and with it the tenant backfill.
func (r *rig) reopen() *rig {
	r.t.Helper()
	next := openRig(r.t, r.path, rigOptions{})
	next.clock = r.clock
	return next
}

// TestReconciliationWithNoKnownTenantBelongsToTheSystemTenant: a row from
// before #286 stored no tenant, and with no audit record either (it
// predates audit) nothing says which tenant decided it. Opening the journal
// gives it the system tenant "", the tenant compaction files its record
// under. From then on a tenant-scoped re-delivery, the deciding tenant's
// included, is refused, terminally, and writes nothing; a system re-delivery
// returns it and backfills its record under "".
func TestReconciliationWithNoKnownTenantBelongsToTheSystemTenant(t *testing.T) {
	first := newRig(t, rigOptions{})
	op, original := first.decideUnder(tenantA(first.ctx), "UNKNOWN")
	_, settled := first.settledEffect("settled")
	first.exec(`UPDATE journal_reconciliations SET tenant = NULL`)
	first.dropReconciliationRecords()
	r := first.reopen()
	if n := r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE tenant = ''`); n != 1 {
		t.Fatalf("rows given the system tenant on open=%d, want 1", n)
	}
	for range 2 {
		for _, ctx := range []context.Context{tenantA(r.ctx), tenantB(r.ctx)} {
			got, err := r.redeliverAs(ctx, op.Key)
			control, controlErr := r.redeliverAs(ctx, settled.Key)
			requireIndistinguishable(t, got, err, control, controlErr)
		}
	}
	if n := r.count(`SELECT COUNT(*) FROM audit_records_v1`); n != 0 {
		t.Fatalf("refused re-deliveries wrote %d records", n)
	}
	again, err := r.redeliverAs(r.ctx, op.Key)
	want := original
	want.Duplicate = true
	if err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("system re-delivery=%+v err=%v, want %+v", again, err, want)
	}
	if r.records("") != 1 || r.records("tenant-a") != 0 || r.records("tenant-b") != 0 {
		t.Fatalf("backfill records system=%d a=%d b=%d, want it under the system tenant", r.records(""), r.records("tenant-a"), r.records("tenant-b"))
	}
	r.mustVerify(1)
}

// TestPruningTheRecordDoesNotChangeWhoOwnsTheReconciliation: a row from
// before #286 takes its tenant from its audit record when the journal is
// opened, once. That record can be pruned while the run is kept; the
// deciding tenant must still own the decision afterwards, and the system
// tenant must not inherit it.
func TestPruningTheRecordDoesNotChangeWhoOwnsTheReconciliation(t *testing.T) {
	first := newRig(t, rigOptions{})
	op, original := first.decideUnder(tenantA(first.ctx), "PRUNED")
	_, settled := first.settledEffect("settled")
	first.exec(`UPDATE journal_reconciliations SET tenant = NULL`)
	r := first.reopen()
	want := original
	want.Duplicate = true
	if again, err := r.redeliverAs(tenantA(r.ctx), op.Key); err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("own re-delivery before prune=%+v err=%v", again, err)
	}
	if err := r.journal.CompleteRun(r.ctx, r.operationRun(op.Key), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	// A restart after the prune must not re-derive the owner either.
	r = r.reopen()
	if again, err := r.redeliverAs(tenantA(r.ctx), op.Key); err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("own re-delivery after its record was pruned=%+v err=%v, want the original", again, err)
	}
	for _, ctx := range []context.Context{r.ctx, tenantB(r.ctx)} {
		got, err := r.redeliverAs(ctx, op.Key)
		control, controlErr := r.redeliverAs(ctx, settled.Key)
		requireIndistinguishable(t, got, err, control, controlErr)
	}
	// The owner's re-delivery wrote the pruned record again (re-delivery
	// backfills any missing record, as before #286); it is under the
	// deciding tenant, never the system tenant's.
	if r.records("tenant-a") != 1 || r.records("") != 0 || r.records("tenant-b") != 0 {
		t.Fatalf("records after prune a=%d system=%d b=%d", r.records("tenant-a"), r.records(""), r.records("tenant-b"))
	}
	r.reopen().mustVerify(1)
}

// TestRowWithoutATenantIsOwnedByNobodyUntilTheNextOpen: an older binary
// (#291) can still open a migrated journal and insert a reconciliation
// without a tenant. Until the journal is opened again, nobody owns it: every
// re-delivery, the deciding and the system tenant's included, is answered
// as for a never-reconciled operation and writes nothing, so ownership is
// never derived from audit at re-delivery time. The next open fixes it from
// its record.
func TestRowWithoutATenantIsOwnedByNobodyUntilTheNextOpen(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, original := r.decideUnder(tenantA(r.ctx), "OLDER")
	_, settled := r.settledEffect("settled")
	r.exec(`UPDATE journal_reconciliations SET tenant = NULL`)
	for _, ctx := range []context.Context{tenantA(r.ctx), r.ctx, tenantB(r.ctx)} {
		got, err := r.redeliverAs(ctx, op.Key)
		control, controlErr := r.redeliverAs(ctx, settled.Key)
		requireIndistinguishable(t, got, err, control, controlErr)
	}
	if n := r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE tenant IS NULL`); n != 1 || r.records("tenant-a") != 1 {
		t.Fatalf("a refused re-delivery wrote: unowned rows=%d records=%d", n, r.records("tenant-a"))
	}
	next := r.reopen()
	want := original
	want.Duplicate = true
	if again, err := next.redeliverAs(tenantA(next.ctx), op.Key); err != nil || !reflect.DeepEqual(again, want) {
		t.Fatalf("own re-delivery after the next open=%+v err=%v", again, err)
	}
}

// tenantFixture is a journal written by origin/main at 99a9228, after #281
// and before #286 (testdata/restore/reconcile-tenant-286/generate.go): its
// reconciliations table has no tenant column. One reconciliation was
// decided under tenant-a, one under the system tenant; both have records.
const tenantFixture = "../../testdata/restore/reconcile-tenant-286/legacy-main-99a9228.db.gz"

func tenantFixtureDatabase(t *testing.T) string {
	t.Helper()
	compressed, err := os.Open(tenantFixture)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	reader, err := gzip.NewReader(compressed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy-286.db")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(file, reader); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func (r *rig) keyWithEvidence(label string) string {
	r.t.Helper()
	var key string
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(r.ctx, `SELECT operation_key FROM journal_reconciliations WHERE evidence LIKE ?`, "%"+tenantMarker+label+"-EVIDENCE%").Scan(&key)
	}); err != nil {
		r.t.Fatal(err)
	}
	return key
}

// TestPreColumnReconciliationsAreAnsweredOnlyToTheirRecordedTenant opens a
// journal origin/main wrote. Opening gives each reconciliation the tenant
// its verified audit record carries, the one that decided it, once; each is
// then answered only to that tenant, also after its record is pruned.
// Reopening changes nothing, and Verify passes.
func TestPreColumnReconciliationsAreAnsweredOnlyToTheirRecordedTenant(t *testing.T) {
	path := tenantFixtureDatabase(t)
	r := openRig(t, path, rigOptions{})
	keyA, keySystem := r.keyWithEvidence("A"), r.keyWithEvidence("SYSTEM")
	if r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE tenant IS NULL`) != 0 ||
		r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE operation_key = ? AND tenant = 'tenant-a'`, keyA) != 1 ||
		r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE operation_key = ? AND tenant = ''`, keySystem) != 1 {
		t.Fatal("opening did not give each pre-column reconciliation its record's tenant")
	}
	r.mustVerify(2)
	migrated := r.snapshot()
	if again := openRig(t, path, rigOptions{}).snapshot(); again != migrated {
		t.Fatal("reopening a migrated journal changed it")
	}
	_, settled := r.settledEffect("settled")
	answers := func(stage string) {
		t.Helper()
		for _, c := range []struct {
			key   string
			owner context.Context
			label string
			other []context.Context
		}{
			{keyA, tenantA(r.ctx), "A", []context.Context{tenantB(r.ctx), r.ctx}},
			{keySystem, r.ctx, "SYSTEM", []context.Context{tenantA(r.ctx), tenantB(r.ctx)}},
		} {
			for _, ctx := range c.other {
				got, err := r.redeliverAs(ctx, c.key)
				control, controlErr := r.redeliverAs(ctx, settled.Key)
				requireIndistinguishable(t, got, err, control, controlErr)
			}
			own, err := r.redeliverAs(c.owner, c.key)
			if err != nil || !own.Duplicate || own.Erased || own.Actor != "operator:bob" || !strings.Contains(own.Evidence, tenantMarker+c.label+"-EVIDENCE") || !strings.Contains(string(own.Result), tenantMarker+c.label+"-RESULT") {
				t.Fatalf("%s: %s own re-delivery=%+v err=%v", stage, c.label, own, err)
			}
		}
	}
	answers("migrated")
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 2 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	answers("records pruned")
	if r.records("tenant-a") != 1 || r.records("") != 1 || r.records("tenant-b") != 0 {
		t.Fatalf("records after prune a=%d system=%d b=%d", r.records("tenant-a"), r.records(""), r.records("tenant-b"))
	}
	r.mustVerify(2)
}

// TestTamperedRecordDoesNotLendItsTenant: the tenant is taken from the
// verified record, never from a column alone. A record whose tenant column
// was altered before the upgrade fails verification, so the row keeps no
// tenant and is owned by nobody: neither the altered tenant nor the deciding
// one is answered, and Verify reports the record.
func TestTamperedRecordDoesNotLendItsTenant(t *testing.T) {
	path := tenantFixtureDatabase(t)
	db, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE audit_records_v1 SET tenant = 'tenant-b' WHERE tenant = 'tenant-a'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	r := openRig(t, path, rigOptions{})
	keyA := r.keyWithEvidence("A")
	if n := r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE operation_key = ? AND tenant IS NULL`, keyA); n != 1 {
		t.Fatalf("a row took its tenant from a record that fails verification")
	}
	_, settled := r.settledEffect("settled")
	for _, ctx := range []context.Context{tenantB(r.ctx), tenantA(r.ctx), r.ctx} {
		got, err := r.redeliverAs(ctx, keyA)
		control, controlErr := r.redeliverAs(ctx, settled.Key)
		requireIndistinguishable(t, got, err, control, controlErr)
	}
	if _, err := r.audit.Verify(r.ctx, r.journal); !errors.Is(err, audit.ErrCorrupt) {
		t.Fatalf("verify=%v, want ErrCorrupt", err)
	}
}

// TestRecordPrunedBeforeTheUpgradeLeavesTheRowUnowned: a reconciliation
// from before #286 whose audit record was pruned before the journal was
// opened by a #286 binary (or by an older binary after it, #291) has no
// record to take its tenant from, but a prune tombstone proves it had one.
// It must not become the system tenant's: the row stays without a tenant,
// owned by nobody, so no re-delivery is answered and none writes anything,
// and compaction does not re-create its record under the system tenant.
func TestRecordPrunedBeforeTheUpgradeLeavesTheRowUnowned(t *testing.T) {
	r := newRig(t, rigOptions{})
	op, _ := r.decideUnder(tenantA(r.ctx), "PRUNED-EARLY")
	settledRun, settled := r.settledEffect("settled")
	for _, id := range []string{r.operationRun(op.Key), settledRun} {
		if err := r.journal.CompleteRun(r.ctx, id, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	// The state an upgrade finds: no tenant column value, record pruned.
	r.exec(`UPDATE journal_reconciliations SET tenant = NULL`)
	r.clock = r.clock.Add(48 * time.Hour)
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune=%+v err=%v", report, err)
	}
	next := r.reopen()
	if n := next.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE tenant IS NULL`); n != 1 {
		t.Fatalf("a row whose record was pruned was given a tenant (unowned rows=%d)", n)
	}
	answeredToNobody := func(stage string) {
		t.Helper()
		for _, ctx := range []context.Context{tenantA(next.ctx), next.ctx, tenantB(next.ctx)} {
			got, err := next.redeliverAs(ctx, op.Key)
			control, controlErr := next.redeliverAs(ctx, settled.Key)
			requireIndistinguishable(t, got, err, control, controlErr)
		}
		if n := next.count(`SELECT COUNT(*) FROM audit_records_v1`); n != 0 {
			t.Fatalf("%s: re-deliveries of an unowned row wrote %d records", stage, n)
		}
	}
	answeredToNobody("retained")
	next.mustVerify(0)
	if again := next.reopen(); again.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE tenant IS NULL`) != 1 {
		t.Fatal("reopening gave the unowned row a tenant")
	}
	if report, err := next.journal.Compact(next.ctx, next.clock); err != nil || report.RemovedRuns != 2 || report.ErasedReconciliations != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	answeredToNobody("compacted")
	next.mustVerify(0)
}
