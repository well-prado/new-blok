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
	// Arrival is defined by the queue itself: the next writer starts only
	// once the previous one has joined the queue (afterEnqueue), not after a
	// guessed delay, so a slow goroutine start under load cannot reorder
	// arrivals (#408). The lock is held long enough afterwards for the
	// earliest writers to have waited well past SQLite's polling sleeps.
	enqueued := make(chan struct{}, 1)
	db.(*connection).afterEnqueue = func() { enqueued <- struct{}{} }
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
		select {
		case <-enqueued:
		case <-time.After(30 * time.Second):
			close(release)
			t.Fatalf("writer %d never joined the queue", i)
		}
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

// TestWriterQueueGrantsTurnsInEnqueueOrder pins the queue without a
// database: waiters are enqueued one by one (the hook marks each arrival),
// then the turn is passed along, and a waiter that gives up leaves the line
// without disturbing the others or leaking the turn (#408).
func TestWriterQueueGrantsTurnsInEnqueueOrder(t *testing.T) {
	var queue writerQueue
	holder, _, err := queue.acquire(context.Background(), time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	const waiters = 20
	enqueued := make(chan struct{}, 1)
	granted := make(chan int, waiters)
	cancelled := make(chan error, 1)
	cancelCtx, cancel := context.WithCancel(context.Background())
	start := func(id int, ctx context.Context) {
		go func() {
			release, _, err := queue.acquire(ctx, 30*time.Second, func() { enqueued <- struct{}{} })
			if err != nil {
				cancelled <- err
				return
			}
			granted <- id
			release()
		}()
		<-enqueued
	}
	for i := range waiters {
		if i == 5 {
			start(-1, cancelCtx) // arrives fifth and leaves before its turn
		}
		start(i, context.Background())
	}
	cancel()
	if err := <-cancelled; !errors.Is(err, context.Canceled) {
		t.Fatalf("the cancelled waiter returned %v; want context.Canceled", err)
	}
	holder()
	for want := range waiters {
		select {
		case got := <-granted:
			if got != want {
				t.Fatalf("turn %d went to waiter %d; want enqueue order", want, got)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("waiter %d never got its turn", want)
		}
	}
	// Every turn was released: the queue is free again.
	deadline := time.Now().Add(30 * time.Second)
	for queue.heldTurns() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the turn stayed held after every waiter released it")
		}
		time.Sleep(time.Millisecond)
	}
	if _, timedOut, err := queue.acquire(context.Background(), time.Millisecond, nil); err != nil || timedOut {
		t.Fatalf("a free queue made the next caller wait: timedOut=%v err=%v", timedOut, err)
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
	// A read that waited for a turn would wait the busy timeout (5 s here),
	// whether it then failed or ran: the 2 s read bound catches both and
	// still leaves a loaded host ample time for a read that did not wait.
	const readBound = 2 * time.Second
	db, err := (Backend{BusyTimeout: 5 * time.Second}).Open(ctx, filepath.Join(t.TempDir(), "read.db"))
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
	if nestedErr != nil || nestedCount != 0 || nestedTook > readBound {
		close(release)
		t.Fatalf("a read nested in the queued writer: count=%d after %v, err=%v", nestedCount, nestedTook, nestedErr)
	}
	count, took, err := read()
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if err != nil || count != 0 || took > readBound {
		t.Fatalf("a read beside the queued writer: count=%d after %v, err=%v", count, took, err)
	}

	short, err := (Backend{BusyTimeout: 200 * time.Millisecond}).Open(ctx, filepath.Join(t.TempDir(), "short.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer short.Close()
	hold, holdingShort := make(chan struct{}), make(chan struct{})
	go func() {
		_ = short.WithTx(store.Writer(ctx), func(*sql.Tx) error { close(holdingShort); <-hold; return nil })
	}()
	<-holdingShort
	defer close(hold)
	begin := time.Now()
	err = short.WithTx(store.Writer(ctx), func(*sql.Tx) error { return nil })
	elapsed := time.Since(begin)
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("a marked writer behind another returned %v; want store.ErrBusy", err)
	}
	if domain, named := store.ErrorWriteDomain(err); !named || !store.SameWriteDomain(domain, mustWriteDomain(t, short)) {
		t.Fatalf("the queue timeout named %v (named=%v); want the handle's domain", domain, named)
	}
	// At least the busy timeout; the upper bound only catches a hang, since a
	// loaded host may wake the waiter late.
	if elapsed < 200*time.Millisecond || elapsed > 10*time.Second {
		t.Fatalf("the queued writer gave up after %v; want the 200ms busy timeout", elapsed)
	}
}

// TestBusyTimeoutIsReported: a handle reports the busy timeout it waits for
// the write lock, its configured one or the five-second default, so a caller
// such as the worker queue can bound its own waits by it (#245).
func TestBusyTimeoutIsReported(t *testing.T) {
	ctx := context.Background()
	for _, test := range []struct {
		configured, want time.Duration
	}{{0, 5 * time.Second}, {250 * time.Millisecond, 250 * time.Millisecond}} {
		db, err := (Backend{BusyTimeout: test.configured}).Open(ctx, filepath.Join(t.TempDir(), "timeout.db"))
		if err != nil {
			t.Fatal(err)
		}
		got, ok := store.BusyTimeoutOf(db)
		_ = db.Close()
		if !ok || got != test.want {
			t.Fatalf("BusyTimeout configured %v reported %v (%v); want %v", test.configured, got, ok, test.want)
		}
	}
}
