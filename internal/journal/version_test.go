package journal

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	sqlitedriver "modernc.org/sqlite"
)

// asBinary stands in for a binary that supports journal schema version.
func asBinary(t *testing.T, version int) {
	t.Helper()
	previous := schemaVersion
	schemaVersion = version
	t.Cleanup(func() { schemaVersion = previous })
}

// TestOlderBinaryRefusesAMigratedJournal: a journal this binary migrated
// (schema 7) is refused by a binary that supports only schema 6 (before
// #332's step input digest), 5 (after #334, before #332's wakeup
// durability), 4 (after #332's wait identity,
// before #334), 3 (after #286, before #332), 2 (after #281, before #286)
// or 1 (before #281), naming the journal and both versions. A schema-6
// binary would serve a step's result to a different input, a schema-5
// binary would strand fired wakeups and ignore run leases, a pre-#334
// binary would ignore the scope attempt fence and overwrite a completed
// scope, a pre-#332 binary would read waits by name alone, a pre-#286 one would insert reconciliations without a
// tenant (#286's interleaving) and a pre-#281 one would recreate the
// legacy tombstone table; refused at open, none writes anything. The same
// and a newer binary open it.
func TestOlderBinaryRefusesAMigratedJournal(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	log := newTestAudit(t, database)
	j, err := New(ctx, database, Config{Audit: log})
	if err != nil {
		t.Fatal(err)
	}
	artifact := audit.Digest([]byte("artifact"))
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "reconciled", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	op, err := j.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: admitted.RunID, ArtifactDigest: artifact, InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := j.StartAttempt(ctx, op.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkUncertain(ctx, op.Key, attempt.ID, "synthetic timeout"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Reconcile(audit.WithTenant(ctx, "tenant-a"), op.Key, "operator:bob", "SYNTHETIC-291-EVIDENCE", []byte(`{"ok":true}`), true); err != nil {
		t.Fatal(err)
	}

	for _, older := range []int{6, 5, 4, 3, 2, 1} {
		func() {
			asBinary(t, older)
			refused, err := New(ctx, database, Config{Audit: log})
			var newer *store.NewerSchemaError
			if refused != nil || !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "journal", Version: 7, Supported: older}) {
				t.Fatalf("a journal-%d binary opened a journal-7 database: journal=%v err=%v", older, refused, err)
			}
		}()
	}
	var untenanted, legacy int
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT COUNT(*) FROM journal_reconciliations WHERE tenant IS NULL`).Scan(&untenanted); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = 'journal_audit'`).Scan(&legacy)
	}); err != nil || untenanted != 0 || legacy != 0 {
		t.Fatalf("untenanted reconciliations=%d legacy table=%d err=%v", untenanted, legacy, err)
	}

	for _, version := range []int{7, 8} {
		asBinary(t, version)
		reopened, err := New(ctx, database, Config{Audit: log})
		if err != nil {
			t.Fatalf("journal-%d binary: %v", version, err)
		}
		if again, err := reopened.Reconcile(audit.WithTenant(ctx, "tenant-a"), op.Key, "operator:bob", "SYNTHETIC-291-EVIDENCE", []byte(`{"ok":true}`), true); err != nil || !again.Duplicate {
			t.Fatalf("journal-%d binary: the deciding tenant's re-delivery=%+v err=%v", version, again, err)
		}
	}
}

// untenantedReconciliation returns a database holding one reconciliation
// decided under tenant-a whose row has lost its tenant, as a pre-#286
// binary leaves it, with its verified audit record in place: the next open
// repairs it from that record. Its journal and audit were opened by this
// binary, so both are stamped.
func untenantedReconciliation(t *testing.T) store.Database {
	t.Helper()
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	j, err := New(ctx, database, Config{Audit: newTestAudit(t, database)})
	if err != nil {
		t.Fatal(err)
	}
	artifact := audit.Digest([]byte("artifact"))
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "reconciled", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	op, err := j.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: admitted.RunID, ArtifactDigest: artifact, InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := j.StartAttempt(ctx, op.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkUncertain(ctx, op.Key, attempt.ID, "synthetic timeout"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Reconcile(audit.WithTenant(ctx, "tenant-a"), op.Key, "operator:bob", "SYNTHETIC-321-EVIDENCE", []byte(`{"ok":true}`), true); err != nil {
		t.Fatal(err)
	}
	execAll(t, database, `UPDATE journal_reconciliations SET tenant = NULL`)
	return database
}

func execAll(t *testing.T, database store.Database, statements ...string) {
	t.Helper()
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		for _, statement := range statements {
			if _, err := tx.Exec(statement); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// reconciliationTenants reports the reconciliations' tenants, "<null>" for
// a row without one.
func reconciliationTenants(t *testing.T, database store.Database) []string {
	t.Helper()
	var tenants []string
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT COALESCE(tenant, '<null>') FROM journal_reconciliations ORDER BY operation_key`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var tenant string
			if err := rows.Scan(&tenant); err != nil {
				return err
			}
			tenants = append(tenants, tenant)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return tenants
}

// TestNewerAuditSchemaRefusesTheTenantRepair (#321): the tenant repair
// reads audit's tables directly, with no composed audit.Journal to check
// audit's stamp. With audit stamped newer than this binary understands,
// journal.New must refuse, naming audit, rather than read them, and must
// give the unowned row no tenant. Two newer shapes: one that keeps the v1
// table (the repair would adopt the tenant it reads there), and one that
// moved its records out of it (the repair would conclude there is no audit
// and give the row to the system tenant). Once a binary that understands
// the stamp opens the store, the repair runs and finds tenant-a, so the
// refused opens really did skip a read that would have changed ownership.
func TestNewerAuditSchemaRefusesTheTenantRepair(t *testing.T) {
	ctx := context.Background()
	for _, shape := range []struct {
		name            string
		newer, restored []string
	}{
		{name: "records kept", newer: nil, restored: nil},
		{
			name:     "records moved",
			newer:    []string{`ALTER TABLE audit_records_v1 RENAME TO audit_records_v2`},
			restored: []string{`ALTER TABLE audit_records_v2 RENAME TO audit_records_v1`},
		},
	} {
		t.Run(shape.name, func(t *testing.T) {
			database := untenantedReconciliation(t)
			execAll(t, database, append(shape.newer, `UPDATE blok_schema_versions SET version = 3 WHERE component = 'audit'`)...)
			for range 2 {
				refused, err := New(ctx, database, Config{})
				var newer *store.NewerSchemaError
				tenants := reconciliationTenants(t, database)
				if refused != nil || !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "audit", Version: 3, Supported: 2}) {
					t.Fatalf("journal opened over an audit-3 store: journal=%v err=%v, reconciliation tenants now %q", refused != nil, err, tenants)
				}
				if len(tenants) != 1 || tenants[0] != "<null>" {
					t.Fatalf("a refused open changed who owns the reconciliation: tenants=%q", tenants)
				}
			}
			// What the refusal withheld: the binary that understands
			// the stamp repairs the row from its record.
			execAll(t, database, append(shape.restored, `UPDATE blok_schema_versions SET version = 2 WHERE component = 'audit'`)...)
			if _, err := New(ctx, database, Config{}); err != nil {
				t.Fatal(err)
			}
			if got := reconciliationTenants(t, database); len(got) != 1 || got[0] != "tenant-a" {
				t.Fatalf("repair under an understood stamp: tenants=%q, want [tenant-a]", got)
			}
		})
	}
}

// TestNewerAuditSchemaWithNothingToRepairStillOpens: a journal with no
// reconciliation to repair reads nothing of audit's, so a journal-only
// binary (no composed audit) still opens it, whatever audit's stamp says.
// A component is refused only for a schema it reads.
func TestNewerAuditSchemaWithNothingToRepairStillOpens(t *testing.T) {
	database := untenantedReconciliation(t)
	execAll(t, database, `UPDATE journal_reconciliations SET tenant = 'tenant-a'`, `UPDATE blok_schema_versions SET version = 3 WHERE component = 'audit'`)
	if _, err := New(context.Background(), database, Config{}); err != nil {
		t.Fatalf("a journal with nothing to repair was refused for audit's stamp: %v", err)
	}
	if got := reconciliationTenants(t, database); len(got) != 1 || got[0] != "tenant-a" {
		t.Fatalf("tenants=%v", got)
	}
}

// requireAuditRefusal opens a journal-only binary over database and wants
// audit's refusal for an audit-3 stamp, with the reconciliation's row left
// without a tenant.
func requireAuditRefusal(t *testing.T, database store.Database) {
	t.Helper()
	refused, err := New(context.Background(), database, Config{})
	var newer *store.NewerSchemaError
	tenants := reconciliationTenants(t, database)
	if refused != nil || !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "audit", Version: 3, Supported: 2}) {
		t.Fatalf("want NewerSchemaError{audit 3, supported 2}, got journal=%v err=%v (reconciliation tenants now %q)", refused != nil, err, tenants)
	}
	if len(tenants) != 1 || tenants[0] != "<null>" {
		t.Fatalf("a refused open changed who owns the reconciliation: tenants=%q", tenants)
	}
}

// auditReads counts, per tripwire tag, the audit values SQLite computed for
// a statement. A tripwire computes one column of an audit table through
// blok_test_audit_read(tag, value), which returns value unchanged and
// counts the call against tag. SQLite computes a virtual column only for a
// statement that uses it, once per row it reads, so the count is the
// number of times anything read that column, whatever it then did with the
// value or any error: a read whose result is discarded counts too.
var auditReads sync.Map // tag -> *atomic.Int64

func init() {
	sqlitedriver.MustRegisterDeterministicScalarFunction("blok_test_audit_read", 2, func(_ *sqlitedriver.FunctionContext, args []driver.Value) (driver.Value, error) {
		tag, ok := args[0].(string)
		if !ok {
			return nil, fmt.Errorf("blok_test_audit_read: tag is %T, want string", args[0])
		}
		counter, _ := auditReads.LoadOrStore(tag, new(atomic.Int64))
		counter.(*atomic.Int64).Add(1)
		return args[1], nil
	})
}

func auditReadCount(tag string) int64 {
	if counter, ok := auditReads.Load(tag); ok {
		return counter.(*atomic.Int64).Load()
	}
	return 0
}

// auditTripwires names the counters of one database's two tripwires.
type auditTripwires struct{ records, tombstones string }

func (w auditTripwires) reads() (records, tombstones int64) {
	return auditReadCount(w.records), auditReadCount(w.tombstones)
}

// tripwireAuditReads rebuilds the two audit tables the tenant repair reads
// so that every read is counted, and changes nothing a reader sees:
// audit_records_v1.record (what audit.StoredTenant verifies and reads the
// tenant from) and audit_pruned_v1.kind (what audit.Pruned matches a
// tombstone on) become virtual columns computed through
// blok_test_audit_read from the stored value. Rows, ids, digests and
// values stay as they were, so a record still verifies. The tombstone's
// kind is declared without a type: declared TEXT, it was also computed
// once per row by the PRAGMA integrity_check journal.New runs before the
// schema transaction, which is not a read by the repair.
func tripwireAuditReads(t *testing.T, database store.Database) auditTripwires {
	t.Helper()
	wires := auditTripwires{records: t.Name() + "#records", tombstones: t.Name() + "#tombstones"}
	literal := func(tag string) string { return "'" + strings.ReplaceAll(tag, "'", "''") + "'" }
	execAll(t, database,
		`ALTER TABLE audit_records_v1 RENAME TO audit_records_untripped`,
		`CREATE TABLE audit_records_v1 (
			seq INTEGER PRIMARY KEY AUTOINCREMENT,
			id TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL,
			tenant TEXT NOT NULL,
			tenant_seq INTEGER NOT NULL,
			run_id TEXT NOT NULL,
			recorded_at INTEGER NOT NULL,
			stored BLOB NOT NULL,
			record BLOB GENERATED ALWAYS AS (blok_test_audit_read(`+literal(wires.records)+`, stored)) VIRTUAL,
			digest TEXT NOT NULL)`,
		`INSERT INTO audit_records_v1 (seq, id, kind, tenant, tenant_seq, run_id, recorded_at, stored, digest)
			SELECT seq, id, kind, tenant, tenant_seq, run_id, recorded_at, record, digest FROM audit_records_untripped`,
		`DROP TABLE audit_records_untripped`,
		`ALTER TABLE audit_pruned_v1 RENAME TO audit_pruned_untripped`,
		`CREATE TABLE audit_pruned_v1 (
			id_digest TEXT PRIMARY KEY,
			stored_kind TEXT NOT NULL,
			kind GENERATED ALWAYS AS (blok_test_audit_read(`+literal(wires.tombstones)+`, stored_kind)) VIRTUAL,
			pruned_at INTEGER NOT NULL)`,
		`INSERT INTO audit_pruned_v1 (id_digest, stored_kind, pruned_at) SELECT id_digest, kind, pruned_at FROM audit_pruned_untripped`,
		`DROP TABLE audit_pruned_untripped`,
	)
	// The wires are armed: reading each column of each row counts once
	// and returns the stored value, and each record still matches its
	// digest.
	var records, tombstones int64
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		recordsBefore, tombstonesBefore := wires.reads()
		rows, err := tx.Query(`SELECT record, digest FROM audit_records_v1`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var record []byte
			var digest string
			if err := rows.Scan(&record, &digest); err != nil {
				rows.Close()
				return err
			}
			if audit.Digest(record) != digest {
				rows.Close()
				return fmt.Errorf("a tripwired record no longer matches its digest")
			}
			records++
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := tx.QueryRow(`SELECT COUNT(kind) FROM audit_pruned_v1`).Scan(&tombstones); err != nil {
			return err
		}
		recordsAfter, tombstonesAfter := wires.reads()
		if recordsAfter-recordsBefore != records || tombstonesAfter-tombstonesBefore != tombstones {
			return fmt.Errorf("reading %d records and %d tombstones counted %d and %d", records, tombstones, recordsAfter-recordsBefore, tombstonesAfter-tombstonesBefore)
		}
		return nil
	}); err != nil {
		t.Fatalf("tripwire not armed: %v", err)
	}
	return wires
}

// pruneReconciliationRecord leaves the database's one reconciliation as
// audit.Prune leaves a pruned decision: its record deleted and its
// tombstone written.
func pruneReconciliationRecord(t *testing.T, database store.Database) {
	t.Helper()
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		var key string
		if err := tx.QueryRow(`SELECT operation_key FROM journal_reconciliations`).Scan(&key); err != nil {
			return err
		}
		id := "reconcile:" + key
		if _, err := tx.Exec(`DELETE FROM audit_records_v1 WHERE id = ?`, id); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO audit_pruned_v1 (id_digest, kind, pruned_at) VALUES (?, ?, 1)`, audit.Digest([]byte(id)), string(audit.KindReconciliation))
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// TestNewerAuditSchemaIsCheckedBeforeAnyAuditRowIsRead (#321): "the repair
// reads no audit row under a stamp it does not understand", made
// observable. Every read of a record (audit.StoredTenant) or a tombstone
// (audit.Pruned) is counted, including one whose result or error is
// discarded. Under an audit-3 stamp each refused open counts zero reads of
// either. With stamp 2 restored, the same open reads the record and
// repairs the row to tenant-a, or, when the record was pruned, reads the
// tombstone and leaves the row unowned: each wire is live where the repair
// needs it.
func TestNewerAuditSchemaIsCheckedBeforeAnyAuditRowIsRead(t *testing.T) {
	for _, shape := range []struct {
		name   string
		pruned bool
		want   string
	}{
		{name: "record kept", want: "tenant-a"},
		{name: "record pruned", pruned: true, want: "<null>"},
	} {
		t.Run(shape.name, func(t *testing.T) {
			database := untenantedReconciliation(t)
			if shape.pruned {
				pruneReconciliationRecord(t, database)
			}
			wires := tripwireAuditReads(t, database)
			execAll(t, database, `UPDATE blok_schema_versions SET version = 3 WHERE component = 'audit'`)
			records, tombstones := wires.reads()
			for range 2 {
				requireAuditRefusal(t, database)
			}
			if r, p := wires.reads(); r != records || p != tombstones {
				t.Fatalf("refused opens under audit 3 read audit rows: %d record reads, %d tombstone reads, want 0 and 0", r-records, p-tombstones)
			}

			execAll(t, database, `UPDATE blok_schema_versions SET version = 2 WHERE component = 'audit'`)
			if _, err := New(context.Background(), database, Config{}); err != nil {
				t.Fatalf("audit-2 open over the tripwired tables: %v", err)
			}
			if got := reconciliationTenants(t, database); len(got) != 1 || got[0] != shape.want {
				t.Fatalf("repair under an understood stamp: tenants=%q, want [%s]", got, shape.want)
			}
			r, p := wires.reads()
			live := r - records
			if shape.pruned {
				live = p - tombstones
			}
			if live < 1 {
				t.Fatalf("the tripwire is not live: the audit-2 repair counted %d record reads and %d tombstone reads", r-records, p-tombstones)
			}
			t.Logf("audit 3: 0 reads; audit 2: %d record reads, %d tombstone reads", r-records, p-tombstones)
		})
	}
}

// TestUnreadableAuditStampRefusesTheTenantRepair (#321): an audit stamp
// the repair cannot read is not "no stamp". journal.New refuses with the
// read's error, naming audit's schema version, and gives the row no
// tenant; once the stamp reads again, the repair finds tenant-a, so the
// refusal withheld a repair.
func TestUnreadableAuditStampRefusesTheTenantRepair(t *testing.T) {
	database := untenantedReconciliation(t)
	execAll(t, database, `UPDATE blok_schema_versions SET version = 'unreadable' WHERE component = 'audit'`)
	for range 2 {
		refused, err := New(context.Background(), database, Config{})
		tenants := reconciliationTenants(t, database)
		var newer *store.NewerSchemaError
		if refused != nil || err == nil || errors.As(err, &newer) || !strings.Contains(err.Error(), "audit schema version") {
			t.Fatalf("journal opened over an unreadable audit stamp: journal=%v err=%v, reconciliation tenants now %q", refused != nil, err, tenants)
		}
		if len(tenants) != 1 || tenants[0] != "<null>" {
			t.Fatalf("a refused open changed who owns the reconciliation: tenants=%q", tenants)
		}
	}
	execAll(t, database, `UPDATE blok_schema_versions SET version = 2 WHERE component = 'audit'`)
	if _, err := New(context.Background(), database, Config{}); err != nil {
		t.Fatal(err)
	}
	if got := reconciliationTenants(t, database); len(got) != 1 || got[0] != "tenant-a" {
		t.Fatalf("repair under a readable stamp: tenants=%q, want [tenant-a]", got)
	}
}

// TestUnrepairableRowsKeepAJournalOnlyOpenRefusedUnderANewerAudit pins a
// cost of #321's refusal. A row from before #286 whose record was pruned
// before the upgrade, or whose record fails verification, is left without
// a tenant on purpose (#286) and stays that way: every open tries it again.
// Under an audit stamp newer than this binary supports, each of those opens
// would have to read audit to try, so every journal-only open is refused,
// for as long as the row exists, even though an understood stamp opens it
// fine and leaves the row unowned. Before #321 such opens succeeded.
func TestUnrepairableRowsKeepAJournalOnlyOpenRefusedUnderANewerAudit(t *testing.T) {
	ctx := context.Background()
	for _, shape := range []struct {
		name   string
		damage func(t *testing.T, database store.Database, key string)
	}{
		{name: "record pruned", damage: func(t *testing.T, database store.Database, _ string) {
			pruneReconciliationRecord(t, database)
		}},
		{name: "record fails verification", damage: func(t *testing.T, database store.Database, _ string) {
			execAll(t, database, `UPDATE audit_records_v1 SET tenant = 'tenant-b' WHERE tenant = 'tenant-a'`)
		}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			database := untenantedReconciliation(t)
			var key string
			if err := database.WithTx(ctx, func(tx *sql.Tx) error {
				return tx.QueryRow(`SELECT operation_key FROM journal_reconciliations`).Scan(&key)
			}); err != nil {
				t.Fatal(err)
			}
			shape.damage(t, database, key)
			// An understood stamp opens it, and the repair leaves the row
			// unowned, as #286 intends.
			if _, err := New(ctx, database, Config{}); err != nil {
				t.Fatalf("audit-2 open: %v", err)
			}
			if got := reconciliationTenants(t, database); len(got) != 1 || got[0] != "<null>" {
				t.Fatalf("an unrepairable row was given a tenant: %q", got)
			}
			execAll(t, database, `UPDATE blok_schema_versions SET version = 3 WHERE component = 'audit'`)
			for range 3 {
				requireAuditRefusal(t, database)
			}
		})
	}
}
