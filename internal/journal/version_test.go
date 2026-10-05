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
