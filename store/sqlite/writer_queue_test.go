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

// TestWritersTakeTheLockInArrivalOrder: marked writers that queue behind a
// held write lock commit in the order they arrived (#214). Left to SQLite's busy
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
		held <- db.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
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
			errs <- db.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
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
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
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

// TestUnmarkedTransactionsAreNotQueued: only marked writers take turns. A
// read runs beside a queued writer that holds the lock, including a read
// nested inside that writer's own callback on the same handle, which waited
// out the busy timeout when every transaction was queued (#214 review). A
// marked writer behind another fails after the busy timeout with ErrBusy
// naming the handle's write domain.
func TestUnmarkedTransactionsAreNotQueued(t *testing.T) {
	ctx := context.Background()
	db, err := (Backend{BusyTimeout: 200 * time.Millisecond}).Open(ctx, filepath.Join(t.TempDir(), "read.db"))
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
	read := func() (int, time.Duration, error) {
		begin := time.Now()
		var count int
		err := db.WithTx(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM rows`).Scan(&count)
		})
		return count, time.Since(begin), err
	}
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var nestedCount int
	var nestedTook time.Duration
	var nestedErr error
	go func() {
		held <- db.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO rows VALUES (1)`); err != nil {
				return err
			}
			nestedCount, nestedTook, nestedErr = read()
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	if nestedErr != nil || nestedCount != 0 || nestedTook > 100*time.Millisecond {
		t.Fatalf("a read nested in the queued writer: count=%d after %v, err=%v", nestedCount, nestedTook, nestedErr)
	}
	if count, took, err := read(); err != nil || count != 0 || took > 100*time.Millisecond {
		t.Fatalf("a read beside the queued writer: count=%d after %v, err=%v", count, took, err)
	}
	begin := time.Now()
	err = db.WithTx(store.Writer(ctx), func(*sql.Tx) error { return nil })
	elapsed := time.Since(begin)
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("a marked writer behind another returned %v; want store.ErrBusy", err)
	}
	if domain, named := store.ErrorWriteDomain(err); !named || !store.SameWriteDomain(domain, mustWriteDomain(t, db)) {
		t.Fatalf("the queue timeout named %v (named=%v); want the handle's domain", domain, named)
	}
	if elapsed < 200*time.Millisecond || elapsed > 2*time.Second {
		t.Fatalf("the queued writer gave up after %v; want the 200ms busy timeout", elapsed)
	}
}
