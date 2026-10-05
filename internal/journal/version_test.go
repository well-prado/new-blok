package journal

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// asBinary stands in for a binary that supports journal schema version.
func asBinary(t *testing.T, version int) {
	t.Helper()
	previous := schemaVersion
	schemaVersion = version
	t.Cleanup(func() { schemaVersion = previous })
}

// TestOlderBinaryRefusesAMigratedJournal: a journal this binary migrated
// (schema 3) is refused by a binary that supports only schema 2 (after
// #281, before #286) or 1 (before #281), naming the journal and both
// versions. A pre-#286 binary would insert reconciliations without a
// tenant (#286's interleaving) and a pre-#281 one would recreate the
// legacy tombstone table; refused at open, neither writes anything. The
// same and a newer binary open it.
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

	for _, older := range []int{2, 1} {
		func() {
			asBinary(t, older)
			refused, err := New(ctx, database, Config{Audit: log})
			var newer *store.NewerSchemaError
			if refused != nil || !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "journal", Version: 3, Supported: older}) {
				t.Fatalf("a journal-%d binary opened a journal-3 database: journal=%v err=%v", older, refused, err)
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

	for _, version := range []int{3, 4} {
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
			execAll(t, database, append(shape.newer, `UPDATE blok_schema_versions SET version = 2 WHERE component = 'audit'`)...)
			for range 2 {
				refused, err := New(ctx, database, Config{})
				var newer *store.NewerSchemaError
				tenants := reconciliationTenants(t, database)
				if refused != nil || !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "audit", Version: 2, Supported: 1}) {
					t.Fatalf("journal opened over an audit-2 store: journal=%v err=%v, reconciliation tenants now %q", refused != nil, err, tenants)
				}
				if len(tenants) != 1 || tenants[0] != "<null>" {
					t.Fatalf("a refused open changed who owns the reconciliation: tenants=%q", tenants)
				}
			}
			// What the refusal withheld: the binary that understands
			// the stamp repairs the row from its record.
			execAll(t, database, append(shape.restored, `UPDATE blok_schema_versions SET version = 1 WHERE component = 'audit'`)...)
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
	execAll(t, database, `UPDATE journal_reconciliations SET tenant = 'tenant-a'`, `UPDATE blok_schema_versions SET version = 2 WHERE component = 'audit'`)
	if _, err := New(context.Background(), database, Config{}); err != nil {
		t.Fatalf("a journal with nothing to repair was refused for audit's stamp: %v", err)
	}
	if got := reconciliationTenants(t, database); len(got) != 1 || got[0] != "tenant-a" {
		t.Fatalf("tenants=%v", got)
	}
}
