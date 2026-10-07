package order

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/worker"
)

func openAt(t *testing.T, path string, clock func() time.Time, opts ...Option) (*Service, store.Database) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, clock, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return service, database
}

// holdWriteLock takes the write lock on a second handle to the file, as
// another process would, and keeps it for hold. The result reports the
// holder's commit.
func holdWriteLock(t *testing.T, path string, hold time.Duration) <-chan error {
	t.Helper()
	other, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = other.Close() })
	locked, done := make(chan struct{}), make(chan error, 1)
	go func() {
		done <- other.WithTx(context.Background(), func(tx *sql.Tx) error {
			if _, err := tx.Exec(fmt.Sprintf(`CREATE TABLE lock_holder_%d (n INTEGER)`, time.Now().UnixNano())); err != nil {
				close(locked)
				return err
			}
			close(locked)
			time.Sleep(hold)
			return nil
		})
	}()
	<-locked
	return done
}

func leaseUntil(t *testing.T, database store.Database, eventID string) int64 {
	t.Helper()
	var until int64
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT lease_until FROM order_outbox WHERE event_id = ?`, eventID).Scan(&until)
	}); err != nil {
		t.Fatal(err)
	}
	return until
}

// B1: the lease is measured from when the claim holds the write lock, and the
// publisher's deadline ends with the lease, so a dispatcher that waited for
// the lock cannot still be publishing when another may claim the event.
func TestOutboxPublishDeadlineEndsWithTheLease(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orders.db")
	service, database := openAt(t, path, nil, WithOutboxLease(time.Second))
	pendingEvent(t, service, "req-deadline")
	released := holdWriteLock(t, path, 300*time.Millisecond)
	var deadline time.Time
	var until int64
	processed, err := service.DispatchOne(ctx, func(publishCtx context.Context, event Event) error {
		deadline, _ = publishCtx.Deadline()
		until = leaseUntil(t, database, event.ID)
		return nil
	})
	if err := <-released; err != nil {
		t.Fatal(err)
	}
	if !processed || err != nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if over := time.Duration(deadline.UnixNano() - until); over > 5*time.Millisecond {
		t.Fatalf("publish deadline ends %v after the lease", over)
	}
}

// B1: another dispatcher's sweep leaves a claim whose lease is still running
// alone, even on the event's last attempt.
func TestOutboxSweepLeavesALiveClaimAlone(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	service, database := openAt(t, filepath.Join(t.TempDir(), "orders.db"), clock.Now, WithOutboxMaxAttempts(1))
	pendingEvent(t, service, "req-live")
	processed, err := service.DispatchOne(ctx, func(context.Context, Event) error {
		clock.Advance(DefaultOutboxLease - time.Second)
		if polled, err := service.DispatchOne(ctx, func(context.Context, Event) error { t.Error("live claim published again"); return nil }); polled || err != nil {
			t.Errorf("second dispatcher processed=%v err=%v", polled, err)
		}
		return nil
	})
	if !processed || err != nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if state, attempts, _ := outboxRow(t, database, "event:req-live"); state != EventSent || attempts != 1 {
		t.Fatalf("state=%s attempts=%d, want sent 1", state, attempts)
	}
}

// B2: an attempt that reached the publisher is spent even when the caller's
// own deadline ended it, so callers with short deadlines cannot retry an event
// forever.
func TestOutboxCallerDeadlinesStillSpendAttempts(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	service, database := openAt(t, filepath.Join(t.TempDir(), "orders.db"), clock.Now, WithOutboxMaxAttempts(2))
	pendingEvent(t, service, "req-caller")
	publishes := 0
	for call := 1; call <= 4; call++ {
		clock.Advance(time.Hour)
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
		_, _ = service.DispatchOne(callCtx, func(publishCtx context.Context, _ Event) error {
			publishes++
			<-publishCtx.Done()
			return publishCtx.Err()
		})
		cancel()
	}
	if state, attempts, _ := outboxRow(t, database, "event:req-caller"); state != EventDead || attempts != 2 || publishes != 2 {
		t.Fatalf("state=%s attempts=%d publishes=%d, want dead 2 2", state, attempts, publishes)
	}
}

// S3: a failed event waits out a backoff before its next attempt, and the
// events behind it are dispatched meanwhile.
func TestOutboxFailedEventBacksOffWhileOthersAreSent(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	service, database := openAt(t, filepath.Join(t.TempDir(), "orders.db"), clock.Now)
	pendingEvent(t, service, "req-a")
	pendingEvent(t, service, "req-b")
	var published []string
	publish := func(_ context.Context, event Event) error {
		published = append(published, event.ID)
		if event.ID == "event:req-a" {
			return errors.New("provider unavailable")
		}
		return nil
	}
	for step, want := range []bool{true, true, false} {
		if processed, _ := service.DispatchOne(ctx, publish); processed != want {
			t.Fatalf("dispatch %d processed=%v, want %v (published %v)", step+1, processed, want, published)
		}
	}
	clock.Advance(time.Second)
	if processed, _ := service.DispatchOne(ctx, publish); !processed {
		t.Fatalf("event not retried after its backoff (published %v)", published)
	}
	if processed, _ := service.DispatchOne(ctx, publish); processed {
		t.Fatalf("event retried inside its second backoff (published %v)", published)
	}
	if fmt.Sprint(published) != "[event:req-a event:req-b event:req-a]" {
		t.Fatalf("published %v", published)
	}
	if state, attempts, _ := outboxRow(t, database, "event:req-a"); state != EventPending || attempts != 2 {
		t.Fatalf("req-a state=%s attempts=%d, want pending 2", state, attempts)
	}
}

// A pending event already over a lowered attempt budget is dead-lettered,
// not published.
func TestOutboxPendingEventOverBudgetIsDeadLettered(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	service, database := openAt(t, filepath.Join(t.TempDir(), "orders.db"), clock.Now)
	pendingEvent(t, service, "req-over")
	if processed, err := service.DispatchOne(ctx, func(context.Context, Event) error { return errors.New("provider unavailable") }); !processed || err == nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	service.maxAttempts = 1
	clock.Advance(time.Hour)
	if processed, err := service.DispatchOne(ctx, func(context.Context, Event) error { t.Error("over-budget event published"); return nil }); processed || err != nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if state, attempts, code := outboxRow(t, database, "event:req-over"); state != EventDead || attempts != 1 || code != AttemptsExhausted {
		t.Fatalf("state=%s attempts=%d code=%s, want dead 1 %s", state, attempts, code, AttemptsExhausted)
	}
}

// S1: opening an outbox that needs migrating waits for another process's
// write instead of failing busy at once.
func TestOutboxMigrationWaitsForAnotherWriter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orders.db")
	database, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := worker.New(ctx, database, nil); err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		for _, statement := range []string{
			`CREATE TABLE orders (order_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE, sku TEXT NOT NULL, quantity INTEGER NOT NULL, total_cents INTEGER NOT NULL, created_at INTEGER NOT NULL)`,
			`CREATE TABLE order_outbox (event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, kind TEXT NOT NULL, payload_json BLOB NOT NULL, state TEXT NOT NULL, created_at INTEGER NOT NULL, UNIQUE(order_id, kind))`,
			`INSERT INTO order_outbox VALUES ('event:old', 'order:old', 'order.created', x'7b7d', 'pending', 0)`,
		} {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	released := holdWriteLock(t, path, 300*time.Millisecond)
	service, err := New(ctx, database, map[string]int64{"coffee": 1500}, nil)
	if holdErr := <-released; holdErr != nil {
		t.Fatal(holdErr)
	}
	if err != nil {
		t.Fatalf("open while another writer held the lock: %v", err)
	}
	if processed, err := service.DispatchOne(ctx, func(context.Context, Event) error { return nil }); !processed || err != nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
}

// S2: claim and settle write first, so writers committing on another handle,
// as another process does, never fail them with a stale snapshot.
func TestOutboxDispatchUnderLoadFromAnotherHandle(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "orders.db")
	service, database := openAt(t, path, nil)
	const events = 20
	for i := range events {
		pendingEvent(t, service, fmt.Sprintf("req-load-%02d", i))
	}
	other, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := other.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE load (n INTEGER)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var loadErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := other.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO load VALUES (?)`, n)
				return err
			}); err != nil {
				loadErr = err
				return
			}
		}
	}()
	published := map[string]int{}
	var dispatchErr error
	for {
		processed, err := service.DispatchOne(ctx, func(_ context.Context, event Event) error {
			published[event.ID]++
			return nil
		})
		if err != nil {
			dispatchErr = err
			break
		}
		if !processed {
			break
		}
	}
	close(stop)
	wg.Wait()
	if dispatchErr != nil || loadErr != nil {
		t.Fatalf("dispatch err=%v load err=%v", dispatchErr, loadErr)
	}
	if len(published) != events {
		t.Fatalf("published %d events, want %d", len(published), events)
	}
	for id, n := range published {
		if n != 1 {
			t.Fatalf("%s published %d times", id, n)
		}
	}
	if n := count(t, database, `SELECT COUNT(*) FROM order_outbox WHERE state = 'sent'`); n != events {
		t.Fatalf("sent=%d, want %d", n, events)
	}
}

func queryPlan(t *testing.T, database store.Database, query string, args ...any) []string {
	t.Helper()
	var plan []string
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(context.Background(), `EXPLAIN QUERY PLAN `+query, args...)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, parent, unused int
			var detail string
			if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
				return err
			}
			plan = append(plan, detail)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return plan
}

// S4: the claim and the sweep read the outbox through its partial index of
// events still to be sent, never by scanning every sent event.
func TestOutboxClaimAndSweepUseTheDueIndex(t *testing.T) {
	_, database := openAt(t, filepath.Join(t.TempDir(), "orders.db"), nil)
	for name, plan := range map[string][]string{
		"claim": queryPlan(t, database, claimSQL, int64(1), "token", int64(0)),
		"sweep": queryPlan(t, database, sweepSQL, ClaimAbandoned, AttemptsExhausted, 5, int64(0)),
	} {
		t.Logf("%s: %q", name, plan)
		usesIndex := false
		for _, step := range plan {
			if strings.Contains(step, "order_outbox_due") {
				usesIndex = true
			}
			if step == "SCAN order_outbox" {
				t.Errorf("%s scans the whole outbox: %q", name, plan)
			}
		}
		if !usesIndex {
			t.Errorf("%s does not use order_outbox_due: %q", name, plan)
		}
	}
}
