package deploy

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/store/sqlite"
)

func TestDurableStartupRejectsIncompatibleVolume(t *testing.T) {
	t.Setenv("BLOK_DEPLOY_TOKEN", "synthetic-fixture-token")
	path := filepath.Join(t.TempDir(), "orders.db")
	db, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.Exec("CREATE TABLE deployment_format (id INTEGER PRIMARY KEY, format TEXT NOT NULL)"); err != nil {
			return err
		}
		_, err := tx.Exec("INSERT INTO deployment_format VALUES (1,'unsupported')")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()
	d, err := NewDurable(deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second}, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Run(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "incompatible store") {
		t.Fatalf("startup: %v", err)
	}
}

func TestDurableStartupRejectsMissingSecretAndCorruptStore(t *testing.T) {
	t.Setenv("BLOK_DEPLOY_TOKEN", "")
	c := deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second}
	path := filepath.Join(t.TempDir(), "orders.db")
	d, err := NewDurable(c, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Run(context.Background(), nil); !errors.Is(err, deployment.ErrNotReady) {
		t.Fatalf("missing secret: %v", err)
	}
	// Test only synthetic corruption; it must never bind an HTTP listener.
	if err := os.WriteFile(path, []byte("synthetic-corrupt-database"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("BLOK_DEPLOY_TOKEN", "synthetic-fixture-token")
	d, err = NewDurable(c, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Run(context.Background(), nil); err == nil || strings.Contains(err.Error(), path) {
		t.Fatalf("corrupt store: %v", err)
	}
}
