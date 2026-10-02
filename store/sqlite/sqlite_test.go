package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
)

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
