package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
)

// TestBusyWriteLockMatchesErrBusy: a write that cannot get the write lock
// within the busy timeout fails with an error matching store.ErrBusy; any
// other failure does not match it.
func TestBusyWriteLockMatchesErrBusy(t *testing.T) {
	ctx := context.Background()
	db, err := (Backend{}).Open(ctx, filepath.Join(t.TempDir(), "busy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE rows (value INTEGER)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		held <- db.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO rows VALUES (1)`); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	begin := time.Now()
	err = db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO rows VALUES (2)`)
		return err
	})
	elapsed := time.Since(begin)
	close(release)
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("a write blocked for %v returned %v; want store.ErrBusy", elapsed, err)
	}
	if elapsed < time.Duration(busyTimeout)*time.Millisecond {
		t.Fatalf("the write gave up after %v, before the busy timeout", elapsed)
	}
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	other := db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO missing VALUES (1)`)
		return err
	})
	if other == nil || errors.Is(other, store.ErrBusy) {
		t.Fatalf("a failure that is not busy matched store.ErrBusy: %v", other)
	}
}
