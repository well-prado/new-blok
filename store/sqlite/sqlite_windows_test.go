//go:build windows

package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// An extended-length path (\\?\C:\...) must open the same file as its plain
// form instead of a literal "/?/" path segment.
func TestOpenAcceptsExtendedLengthPath(t *testing.T) {
	plain := filepath.Join(t.TempDir(), "extended.db")
	db, err := (Backend{}).Open(context.Background(), `\\?\`+plain)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec(`CREATE TABLE t (v TEXT)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(plain); err != nil {
		t.Fatalf("database is not at the plain path: %v", err)
	}
}
