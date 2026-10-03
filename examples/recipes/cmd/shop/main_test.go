package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/store/sqlite"
)

func TestFreshMigrationReplayStatusAndTeardownCommands(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "new", "shop.db")
	t.Setenv("SHOP_DB_PATH", databasePath)
	for range 2 {
		if err := run([]string{"migrate-up"}); err != nil {
			t.Fatalf("migrate-up: %v", err)
		}
	}
	if err := run([]string{"migrate-status"}); err != nil {
		t.Fatalf("migrate-status: %v", err)
	}
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	var version, count int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT MAX(version), COUNT(*) FROM shop_schema_migrations`).Scan(&version, &count)
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if version != 2 || count != 2 {
		t.Fatalf("migration version=%d rows=%d", version, count)
	}
	if err := run([]string{"teardown"}); err != nil {
		t.Fatalf("teardown: %v", err)
	}
	if err := run([]string{"migrate-up"}); err != nil {
		t.Fatalf("fresh migration after teardown: %v", err)
	}
}
