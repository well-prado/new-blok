package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
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
	// The busy error names the domain it waited on, so a caller can tell
	// its own held lock from another store's even through a wrapper that
	// hides WriteDomain (#207).
	if domain, named := store.ErrorWriteDomain(err); !named || !store.SameWriteDomain(domain, mustWriteDomain(t, db)) {
		t.Fatalf("the busy error %v named domain %v (named=%v); want the database's own", err, domain, named)
	}
	if elapsed < defaultBusyTimeout {
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
	if _, named := store.ErrorWriteDomain(other); named {
		t.Fatalf("a failure that is not busy named a write domain: %v", other)
	}
}

func mustWriteDomain(t *testing.T, database store.Database) *store.WriteDomain {
	t.Helper()
	domain, ok := store.WriteDomainOf(database)
	if !ok {
		t.Fatal("the database exposes no write domain")
	}
	return domain
}

// TestSharedMemoryWriterConflictIsBusyWithinTimeout: two ":memory:" handles
// share one database. A writer blocked by the other's open write transaction
// must come back store.ErrBusy within the busy timeout, naming the shared
// domain. In SQLite's shared-cache mode the conflict was SQLITE_LOCKED, which
// the driver waits out with sqlite3_unlock_notify and no timeout, so a
// transaction blocked by one that waits for it hung forever (#207).
func TestSharedMemoryWriterConflictIsBusyWithinTimeout(t *testing.T) {
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
		_, err := tx.ExecContext(ctx, `CREATE TABLE memory_rows (value INTEGER)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	// Each memdb connection caches the schema; load it before the conflict,
	// because a stale cache cannot be refreshed while the writer holds the
	// lock either.
	if err := second.WithTx(ctx, func(tx *sql.Tx) error {
		var rows int
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM memory_rows`).Scan(&rows)
	}); err != nil {
		t.Fatal(err)
	}
	const wait = 200 * time.Millisecond
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		held <- first.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO memory_rows VALUES (1)`); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	blocked := make(chan error, 1)
	begin := time.Now()
	go func() {
		blocked <- second.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `PRAGMA busy_timeout = `+strconv.FormatInt(wait.Milliseconds(), 10)); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO memory_rows VALUES (2)`)
			return err
		})
	}()
	select {
	case err = <-blocked:
	case <-time.After(10 * time.Second):
		// The blocked writer cannot be canceled, and closing the holder
		// would only release it; fail with the evidence instead of waiting
		// for the package timeout.
		stacks := make([]byte, 1<<20)
		panic(fmt.Sprintf("a shared-memory writer blocked by an open write transaction did not return within 10s (#207)\n%s", stacks[:runtime.Stack(stacks, true)]))
	}
	elapsed := time.Since(begin)
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("the blocked shared-memory writer returned %v after %v; want store.ErrBusy", err, elapsed)
	}
	if domain, named := store.ErrorWriteDomain(err); !named || !store.SameWriteDomain(domain, mustWriteDomain(t, first)) {
		t.Fatalf("the shared-memory busy error named domain %v (named=%v); want the shared memory domain", domain, named)
	}
}
