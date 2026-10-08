package audit_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// checkSchema runs audit.CheckSchema in a transaction of database.
func checkSchema(t *testing.T, database store.Database) error {
	t.Helper()
	ctx := context.Background()
	return database.WithTx(ctx, func(tx *sql.Tx) error { return audit.CheckSchema(ctx, tx) })
}

// TestCheckSchema (#321): CheckSchema is the stamp check an owner without
// a composed audit.Journal runs before it reads audit's tables. It accepts
// the stamps this binary understands (none, version 1, or version 2 since
// #284), refuses a newer
// one exactly as NewJournal does, and refuses a stamp it cannot read
// rather than treating it as absent.
func TestCheckSchema(t *testing.T) {
	ctx := context.Background()
	if err := audit.CheckSchema(ctx, nil); !errors.Is(err, audit.ErrRequired) {
		t.Fatalf("nil tx: %v, want ErrRequired", err)
	}
	open := func(t *testing.T) store.Database {
		t.Helper()
		database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "audit.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { database.Close() })
		return database
	}
	exec := func(t *testing.T, database store.Database, statement string) {
		t.Helper()
		if err := database.WithTx(ctx, func(tx *sql.Tx) error { _, err := tx.Exec(statement); return err }); err != nil {
			t.Fatal(err)
		}
	}
	stampRows := func(t *testing.T, database store.Database) (tables, journalRows, auditRows int) {
		t.Helper()
		if err := database.WithTx(ctx, func(tx *sql.Tx) error {
			if err := tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'blok_schema_versions'`).Scan(&tables); err != nil || tables == 0 {
				return err
			}
			return tx.QueryRow(`SELECT COUNT(*) FILTER (WHERE component = 'journal'), COUNT(*) FILTER (WHERE component = 'audit') FROM blok_schema_versions`).Scan(&journalRows, &auditRows)
		}); err != nil {
			t.Fatal(err)
		}
		return tables, journalRows, auditRows
	}
	newAudit := func(database store.Database) error {
		_, err := audit.NewJournal(ctx, database, audit.Config{MaxRecords: 1000, Readers: tenantReaders{}})
		return err
	}

	t.Run("no version table", func(t *testing.T) {
		database := open(t)
		if tables, _, _ := stampRows(t, database); tables != 0 {
			t.Fatal("a freshly opened store already has a version table")
		}
		if err := checkSchema(t, database); err != nil {
			t.Fatalf("an empty store was refused: %v", err)
		}
	})
	t.Run("no audit stamp", func(t *testing.T) {
		database := open(t)
		// The journal stamps its own row only.
		if _, err := journal.New(ctx, database, journal.Config{}); err != nil {
			t.Fatal(err)
		}
		if tables, journalRows, auditRows := stampRows(t, database); tables != 1 || journalRows != 1 || auditRows != 0 {
			t.Fatalf("want a version table with the journal's stamp only, got tables=%d journal=%d audit=%d", tables, journalRows, auditRows)
		}
		if err := checkSchema(t, database); err != nil {
			t.Fatalf("a store without an audit stamp was refused: %v", err)
		}
	})
	for _, version := range []string{"1", "2"} {
		t.Run("stamped "+version, func(t *testing.T) {
			database := open(t)
			if err := newAudit(database); err != nil {
				t.Fatal(err)
			}
			exec(t, database, `UPDATE blok_schema_versions SET version = `+version+` WHERE component = 'audit'`)
			if err := checkSchema(t, database); err != nil {
				t.Fatalf("a store stamped with a supported audit version was refused: %v", err)
			}
		})
	}
	t.Run("stamped 3", func(t *testing.T) {
		database := open(t)
		if err := newAudit(database); err != nil {
			t.Fatal(err)
		}
		exec(t, database, `UPDATE blok_schema_versions SET version = 3 WHERE component = 'audit'`)
		err := checkSchema(t, database)
		var newer *store.NewerSchemaError
		if !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "audit", Version: 3, Supported: 2}) {
			t.Fatalf("audit stamped 3: %v, want NewerSchemaError{audit 3, supported 2}", err)
		}
		var fromConstructor *store.NewerSchemaError
		if err := newAudit(database); !errors.As(err, &fromConstructor) || *fromConstructor != *newer {
			t.Fatalf("NewJournal refused audit 3 with %v, CheckSchema with %v: want the same refusal", err, newer)
		}
	})
	t.Run("unreadable stamp", func(t *testing.T) {
		database := open(t)
		if err := newAudit(database); err != nil {
			t.Fatal(err)
		}
		exec(t, database, `UPDATE blok_schema_versions SET version = 'unreadable' WHERE component = 'audit'`)
		err := checkSchema(t, database)
		var newer *store.NewerSchemaError
		if err == nil || errors.As(err, &newer) || !strings.Contains(err.Error(), "audit schema version") {
			t.Fatalf("unreadable audit stamp: %v, want the stamp read's error naming audit's schema version", err)
		}
	})
}
