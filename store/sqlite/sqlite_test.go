package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/store"
)

type hiddenWriteDomain struct{ store.Database }

func TestBackendPersistsTransactionsAndCreatesVerifiedBackup(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "journal.db")
	database, err := (Backend{}).Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "CREATE TABLE events (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO events (id, value) VALUES (?, ?)", 1, "accepted")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Integrity(ctx); err != nil {
		t.Fatal(err)
	}
	backupPath := filepath.Join(t.TempDir(), "backup.db")
	if err := database.Backup(ctx, backupPath); err != nil {
		t.Fatal(err)
	}
	backup, err := (Backend{}).Open(ctx, backupPath)
	if err != nil {
		t.Fatal(err)
	}
	defer backup.Close()
	var value string
	if err := backup.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, "SELECT value FROM events WHERE id = 1").Scan(&value)
	}); err != nil {
		t.Fatal(err)
	}
	if value != "accepted" {
		t.Fatalf("backup value=%q", value)
	}
}

func TestWriteDomainTracksTheSQLiteFileAcrossHandles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "shared.db")
	first, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	firstDomain, ok := store.WriteDomainOf(first)
	if !ok {
		t.Fatal("SQLite database did not expose its write domain")
	}
	if _, ok := store.WriteDomainOf(hiddenWriteDomain{Database: first}); ok {
		t.Fatal("an opaque wrapper unexpectedly exposed its embedded database's write domain")
	}
	secondDomain, ok := store.WriteDomainOf(second)
	if !ok || !store.SameWriteDomain(firstDomain, secondDomain) {
		t.Fatal("handles to one SQLite file did not share a write domain")
	}
	other, err := (Backend{}).Open(ctx, filepath.Join(t.TempDir(), "other.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	otherDomain, ok := store.WriteDomainOf(other)
	if !ok || store.SameWriteDomain(firstDomain, otherDomain) {
		t.Fatal("distinct SQLite files shared a write domain")
	}
}

func TestSharedMemoryHandlesShareStateAndWriteDomain(t *testing.T) {
	ctx := context.Background()
	first, err := (Backend{}).Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := (Backend{}).Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if err := first.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE shared_memory_proof (value INTEGER)`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO shared_memory_proof VALUES (47)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var value int
	if err := second.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT value FROM shared_memory_proof`).Scan(&value)
	}); err != nil {
		t.Fatal(err)
	}
	if value != 47 {
		t.Fatalf("second memory handle read %d; want 47", value)
	}
	firstDomain, firstOK := store.WriteDomainOf(first)
	secondDomain, secondOK := store.WriteDomainOf(second)
	if !firstOK || !secondOK || !store.SameWriteDomain(firstDomain, secondDomain) {
		t.Fatal("handles to the shared in-memory URI did not share a write domain")
	}
}

// #156: a file path became the authority of the file: URI ("C:\\Users\\…" on
// Windows, a relative "app.db" anywhere), and SQLite refused to open it.
func TestOpenAcceptsRelativeAndUnusualPaths(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	for _, path := range []string{
		"relative.db",
		filepath.Join("nested dir", "relative.db"),
		filepath.Join(root, "Área de Trabalho Blök", "a b#c%d&e;f=g.db"),
	} {
		t.Run(path, func(t *testing.T) {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			write := func(db store.Database) error {
				return db.WithTx(context.Background(), func(tx *sql.Tx) error {
					if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS t (v TEXT)`); err != nil {
						return err
					}
					_, err := tx.Exec(`INSERT INTO t VALUES ('kept')`)
					return err
				})
			}
			first, err := (Backend{}).Open(context.Background(), path)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := write(first); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := first.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := os.Stat(path); err != nil {
				t.Fatalf("database file is not at the path given: %v", err)
			}
			second, err := (Backend{}).Open(context.Background(), path)
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer second.Close()
			var got string
			if err := second.WithTx(context.Background(), func(tx *sql.Tx) error {
				return tx.QueryRow(`SELECT v FROM t`).Scan(&got)
			}); err != nil || got != "kept" {
				t.Fatalf("reopened value=%q err=%v", got, err)
			}
		})
	}
}
