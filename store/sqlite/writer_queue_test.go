package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
)

// TestWritersTakeTheLockInArrivalOrder: writers that queue behind a held
// write lock commit in the order they arrived (#214). Left to SQLite's busy
// handler, a writer that has waited longest polls least often, so later
// writers overtake it and it can be starved out to the busy timeout.
func TestWritersTakeTheLockInArrivalOrder(t *testing.T) {
	ctx := context.Background()
	db, err := (Backend{}).Open(ctx, filepath.Join(t.TempDir(), "order.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE commits (seq INTEGER PRIMARY KEY AUTOINCREMENT, writer INTEGER NOT NULL)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		held <- db.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO commits (writer) VALUES (-1)`); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	// Each writer starts well after the previous one is waiting, so arrival
	// order is unambiguous. The lock is held long enough for the earliest
	// writers to back off to SQLite's longest polling sleeps.
	const writers = 12
	var group sync.WaitGroup
	errs := make(chan error, writers)
	for i := range writers {
		group.Add(1)
		go func() {
			defer group.Done()
			errs <- db.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO commits (writer) VALUES (?)`, i)
				return err
			})
		}()
		time.Sleep(40 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)
	close(release)
	group.Wait()
	close(errs)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	for err := range errs {
		if err != nil {
			t.Fatalf("queued writer: %v", err)
		}
	}
	var order []int
	if err := db.WithTx(store.ReadOnly(ctx), func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT writer FROM commits WHERE writer >= 0 ORDER BY seq`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var w int
			if err := rows.Scan(&w); err != nil {
				return err
			}
			order = append(order, w)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	for i, w := range order {
		if w != i {
			t.Fatalf("writers committed in order %v; want arrival order 0..%d", order, writers-1)
		}
	}
	if len(order) != writers {
		t.Fatalf("%d of %d writers committed", len(order), writers)
	}
}

// TestReadOnlyTransactionDoesNotQueueBehindAWriter: a marked read runs beside
// a writer that holds the lock, reading the last committed state, instead of
// waiting for the writer's turn to end (#214).
func TestReadOnlyTransactionDoesNotQueueBehindAWriter(t *testing.T) {
	ctx := context.Background()
	db, err := (Backend{}).Open(ctx, filepath.Join(t.TempDir(), "read.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE rows (id INTEGER PRIMARY KEY)`)
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
	defer func() {
		close(release)
		if err := <-held; err != nil {
			t.Fatal(err)
		}
	}()
	begin := time.Now()
	var count int
	err = db.WithTx(store.ReadOnly(ctx), func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM rows`).Scan(&count)
	})
	if err != nil || count != 0 {
		t.Fatalf("read beside the writer: count=%d err=%v", count, err)
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("the read waited %v behind the writer", elapsed)
	}
	// An unmarked transaction is a writer and waits its turn, bounded by the
	// busy timeout.
	short, err := (Backend{BusyTimeout: 200 * time.Millisecond}).Open(ctx, filepath.Join(t.TempDir(), "short.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer short.Close()
	hold := make(chan struct{})
	holdingShort := make(chan struct{})
	go func() {
		_ = short.WithTx(ctx, func(*sql.Tx) error { close(holdingShort); <-hold; return nil })
	}()
	<-holdingShort
	defer close(hold)
	begin = time.Now()
	err = short.WithTx(ctx, func(*sql.Tx) error { return nil })
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("an unmarked transaction behind a writer returned %v; want store.ErrBusy", err)
	}
	if domain, named := store.ErrorWriteDomain(err); !named || !store.SameWriteDomain(domain, mustWriteDomain(t, short)) {
		t.Fatalf("the queue timeout named %v (named=%v); want the handle's domain", domain, named)
	}
	if elapsed := time.Since(begin); elapsed < 200*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("the queued writer gave up after %v; want the 200ms busy timeout", elapsed)
	}
}
