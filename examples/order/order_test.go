package order

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/worker"
)

func TestOrderCommitAndOutboxAreAtomicAndDuplicateDeliveryIsIdempotent(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(context.Background(), Request{RequestKey: "req-1", SKU: "coffee", Quantity: 2}); err != nil {
		t.Fatal(err)
	}
	processed, err := service.ProcessOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if _, err := service.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := service.Get(context.Background(), "req-1")
	if err != nil || result.TotalCents != 3000 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	sent := 0
	for {
		processed, err := service.DispatchOne(context.Background(), func(_ context.Context, event Event) error {
			sent++
			if event.ID != "event:req-1" {
				t.Fatalf("event=%+v", event)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	if sent != 1 {
		t.Fatalf("sent=%d, want one outbox event", sent)
	}
	processed, err = service.DispatchOne(context.Background(), func(context.Context, Event) error { sent++; return nil })
	if err != nil || processed {
		t.Fatalf("second dispatch processed=%v err=%v", processed, err)
	}
}

func TestInvalidOrderIsDeadLetteredWithoutBusinessRows(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(context.Background(), Request{RequestKey: "bad", SKU: "coffee", Quantity: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Get(context.Background(), "bad"); err == nil {
		t.Fatal("invalid order was stored")
	}
	// A validation failure is never retried: the job is dead at its first attempt.
	assertJob(t, service, "bad", "dead", 1)
	if n := count(t, database, `SELECT COUNT(*) FROM order_outbox`); n != 0 {
		t.Fatalf("outbox rows=%d, want 0", n)
	}
}

func TestOutboxProviderFailureKeepsPendingEventAndRetriesStableID(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := newTestClock()
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, clock.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(context.Background(), Request{RequestKey: "req-timeout", SKU: "coffee", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var eventIDs []string
	processed, err := service.DispatchOne(context.Background(), func(ctx context.Context, event Event) error {
		eventIDs = append(eventIDs, event.ID)
		// The provider times out: its call runs into a real context deadline.
		call, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		defer cancel()
		<-call.Done()
		return call.Err()
	})
	if !processed || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("failed dispatch processed=%v err=%v", processed, err)
	}
	if state := outboxState(t, database, "event:req-timeout"); state != EventPending {
		t.Fatalf("outbox state after the timeout=%s, want pending", state)
	}
	clock.Advance(time.Minute) // past the retry backoff
	processed, err = service.DispatchOne(context.Background(), func(_ context.Context, event Event) error {
		eventIDs = append(eventIDs, event.ID)
		return nil
	})
	if !processed || err != nil {
		t.Fatalf("retry dispatch processed=%v err=%v", processed, err)
	}
	if len(eventIDs) != 2 || eventIDs[0] != eventIDs[1] {
		t.Fatalf("event IDs=%v, want stable provider identity across retry", eventIDs)
	}
	processed, err = service.DispatchOne(context.Background(), func(context.Context, Event) error {
		t.Fatal("sent outbox event was dispatched again")
		return nil
	})
	if processed || err != nil {
		t.Fatalf("post-commit dispatch processed=%v err=%v", processed, err)
	}
}

// testClock is a clock tests move by hand, shared by the queue and the outbox.
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock() *testClock { return &testClock{now: time.Unix(1_700_000_000, 0).UTC()} }

func (c *testClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }

func (c *testClock) Advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }

func openService(t *testing.T, clock func() time.Time) (*Service, store.Database) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, clock)
	if err != nil {
		t.Fatal(err)
	}
	return service, database
}

func count(t *testing.T, database store.Database, query string, args ...any) int {
	t.Helper()
	var n int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), query, args...).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func outboxState(t *testing.T, database store.Database, eventID string) string {
	t.Helper()
	var state string
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT state FROM order_outbox WHERE event_id = ?`, eventID).Scan(&state)
	}); err != nil {
		t.Fatal(err)
	}
	return state
}

func assertJob(t *testing.T, service *Service, requestKey, state string, attempt int) {
	t.Helper()
	job, err := service.queue.Get(context.Background(), requestKey)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != state || job.Attempt != attempt {
		t.Fatalf("job %s state=%s attempt=%d error=%q, want state=%s attempt=%d", requestKey, job.State, job.Attempt, job.Error, state, attempt)
	}
}

// D3: a writer that commits while an event is being published must not make
// the dispatcher publish it again. Read-then-publish-then-write lost its mark
// to SQLITE_BUSY_SNAPSHOT (517) and published the event on every pass.
func TestDispatchDoesNotRepublishWhenAWriterCommitsDuringPublish(t *testing.T) {
	ctx := context.Background()
	service, database := openService(t, nil)
	if _, err := service.Enqueue(ctx, Request{RequestKey: "req-d3", SKU: "coffee", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	if processed, err := service.ProcessOnce(ctx); err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	publishes := 0
	publish := func(_ context.Context, event Event) error {
		publishes++
		// Another order arrives on another connection and commits while the
		// event is with the provider, as it does under steady order load.
		committed := make(chan error, 1)
		go func() {
			_, err := service.Enqueue(context.Background(), Request{RequestKey: fmt.Sprintf("load-%d", publishes), SKU: "coffee", Quantity: 1})
			committed <- err
		}()
		return <-committed
	}
	for pass := 1; pass <= 3; pass++ {
		processed, err := service.DispatchOne(ctx, publish)
		if err != nil {
			t.Errorf("pass %d: processed=%v err=%v", pass, processed, err)
		}
	}
	if publishes != 1 {
		t.Fatalf("event published %d times across 3 passes, want exactly 1", publishes)
	}
	if state := outboxState(t, database, "event:req-d3"); state != "sent" {
		t.Fatalf("outbox state=%s, want sent", state)
	}
}

// ME: an outbox insert that fails must take the order row down with it. A
// foreign event already holds the event ID, so the insert hits the primary key.
// MF: a key resubmitted after its dedupe window ended reaches the handler
// again and must not fail on, or duplicate, the order it already committed.
func TestOutboxConflictRollsBackTheOrderAndRetriesWithinTheBound(t *testing.T) {
	for _, heal := range []bool{true, false} {
		name := "conflict persists"
		if heal {
			name = "conflict clears"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			clock := newTestClock()
			service, database := openService(t, clock.Now)
			if err := database.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `INSERT INTO order_outbox (event_id, order_id, kind, payload_json, state, created_at) VALUES ('event:req-me', 'order:foreign', 'foreign', x'7b7d', 'sent', 0)`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := service.Enqueue(ctx, Request{RequestKey: "req-me", SKU: "coffee", Quantity: 1}); err != nil {
				t.Fatal(err)
			}
			if processed, err := service.ProcessOnce(ctx); err != nil || !processed {
				t.Fatalf("processed=%v err=%v", processed, err)
			}
			assertJob(t, service, "req-me", "pending", 1)
			if n := count(t, database, `SELECT COUNT(*) FROM orders`); n != 0 {
				t.Fatalf("orders=%d after the outbox insert failed, want 0", n)
			}
			if n := count(t, database, `SELECT COUNT(*) FROM order_outbox WHERE order_id = 'order:req-me'`); n != 0 {
				t.Fatalf("outbox rows for the order=%d, want 0", n)
			}
			if heal {
				if err := database.WithTx(ctx, func(tx *sql.Tx) error {
					_, err := tx.ExecContext(ctx, `DELETE FROM order_outbox WHERE event_id = 'event:req-me'`)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			for attempt := 2; attempt <= 3; attempt++ {
				clock.Advance(time.Minute)
				if processed, err := service.ProcessOnce(ctx); err != nil || !processed {
					t.Fatalf("attempt %d processed=%v err=%v", attempt, processed, err)
				}
				if heal {
					assertJob(t, service, "req-me", "completed", 2)
					if n := count(t, database, `SELECT COUNT(*) FROM orders WHERE request_key = 'req-me'`); n != 1 {
						t.Fatalf("orders=%d, want 1", n)
					}
					if n := count(t, database, `SELECT COUNT(*) FROM order_outbox WHERE order_id = 'order:req-me' AND state = 'pending'`); n != 1 {
						t.Fatalf("pending outbox rows=%d, want 1", n)
					}
					return
				}
				want := "pending"
				if attempt == 3 {
					want = "dead"
				}
				assertJob(t, service, "req-me", want, attempt)
				if n := count(t, database, `SELECT COUNT(*) FROM orders`); n != 0 {
					t.Fatalf("orders=%d at attempt %d, want 0", n, attempt)
				}
			}
			clock.Advance(time.Minute)
			if processed, err := service.ProcessOnce(ctx); err != nil || processed {
				t.Fatalf("dead job processed again: processed=%v err=%v", processed, err)
			}
		})
	}
}

func TestKeyResubmittedAfterItsDedupeWindowKeepsOneOrder(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	service, database := openService(t, clock.Now)
	request := Request{RequestKey: "req-mf", SKU: "coffee", Quantity: 2}
	for delivery, wantAccepted := range []bool{true, false} {
		result, err := service.Enqueue(ctx, request)
		if err != nil || result.Accepted != wantAccepted {
			t.Fatalf("delivery %d accepted=%v err=%v, want accepted=%v", delivery+1, result.Accepted, err, wantAccepted)
		}
	}
	for pass, want := range []bool{true, false} {
		if processed, err := service.ProcessOnce(ctx); err != nil || processed != want {
			t.Fatalf("pass %d processed=%v err=%v, want %v", pass+1, processed, err, want)
		}
	}
	assertJob(t, service, "req-mf", "completed", 1)
	clock.Advance(time.Hour)
	report, err := service.queue.Compact(ctx, worker.Retention{Completed: clock.Now(), Tombstones: clock.Now()})
	if err != nil || report.Compacted != 1 || report.ExpiredTombstones != 1 {
		t.Fatalf("compaction report=%+v err=%v", report, err)
	}
	if result, err := service.Enqueue(ctx, request); err != nil || !result.Accepted {
		t.Fatalf("resubmission accepted=%v err=%v, want a new job", result.Accepted, err)
	}
	if processed, err := service.ProcessOnce(ctx); err != nil || !processed {
		t.Fatalf("resubmission processed=%v err=%v", processed, err)
	}
	assertJob(t, service, "req-mf", "completed", 1)
	if n := count(t, database, `SELECT COUNT(*) FROM orders WHERE request_key = 'req-mf' AND total_cents = 3000`); n != 1 {
		t.Fatalf("orders=%d, want 1", n)
	}
	if n := count(t, database, `SELECT COUNT(*) FROM order_outbox`); n != 1 {
		t.Fatalf("outbox rows=%d, want 1", n)
	}
}

func outboxRow(t *testing.T, database store.Database, eventID string) (state string, attempts int, code string) {
	t.Helper()
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT state, attempts, error_text FROM order_outbox WHERE event_id = ?`, eventID).Scan(&state, &attempts, &code)
	}); err != nil {
		t.Fatal(err)
	}
	return state, attempts, code
}

func pendingEvent(t *testing.T, service *Service, requestKey string) {
	t.Helper()
	if _, err := service.Enqueue(context.Background(), Request{RequestKey: requestKey, SKU: "coffee", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	if processed, err := service.ProcessOnce(context.Background()); err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
}

// The publisher's deadline is the event's lease, and an event that keeps
// failing is dead after its last attempt instead of being retried forever.
func TestOutboxPublishDeadlineAndAttemptsAreBounded(t *testing.T) {
	ctx := context.Background()
	// A 10ms busy timeout leaves a 50ms lease's publisher 30ms.
	database, err := (sqlite.Backend{BusyTimeout: 10 * time.Millisecond}).Open(ctx, filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := newTestClock()
	service, err := New(ctx, database, map[string]int64{"coffee": 1500}, clock.Now, WithOutboxLease(50*time.Millisecond), WithOutboxMaxAttempts(2))
	if err != nil {
		t.Fatal(err)
	}
	pendingEvent(t, service, "req-bound")
	calls := 0
	hang := func(ctx context.Context, _ Event) error {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("publisher context has no deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	for attempt, want := range []string{EventPending, EventDead} {
		clock.Advance(time.Minute) // past the retry backoff
		processed, err := service.DispatchOne(ctx, hang)
		if !processed || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("attempt %d processed=%v err=%v", attempt+1, processed, err)
		}
		if state, attempts, code := outboxRow(t, database, "event:req-bound"); state != want || attempts != attempt+1 || code != PublishTimedOut {
			t.Fatalf("attempt %d state=%s attempts=%d code=%s, want %s %d %s", attempt+1, state, attempts, code, want, attempt+1, PublishTimedOut)
		}
	}
	if processed, err := service.DispatchOne(ctx, hang); processed || err != nil {
		t.Fatalf("dead event dispatched again: processed=%v err=%v", processed, err)
	}
	if calls != 2 {
		t.Fatalf("publisher calls=%d, want 2", calls)
	}
}

// A dispatcher whose lease ends before it marks the event sent loses the
// event to the dispatcher that takes it over, and its late mark changes
// nothing. A claim whose dispatcher died on the event's last attempt is
// dead-lettered, not published again.
func TestOutboxLeaseIsFencedAndAbandonedClaimsAreDeadLettered(t *testing.T) {
	ctx := context.Background()
	clock := newTestClock()
	service, database := openService(t, clock.Now)
	pendingEvent(t, service, "req-lease")
	var takeover string
	processed, err := service.DispatchOne(ctx, func(context.Context, Event) error {
		// The publish outlives the lease and another dispatcher claims the event.
		clock.Advance(DefaultOutboxLease)
		_, held, err := service.claimEvent(ctx)
		if err != nil {
			t.Errorf("takeover claim: %v", err)
		}
		takeover = held.token
		return nil
	})
	if !processed || !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("late mark processed=%v err=%v, want ErrLeaseLost", processed, err)
	}
	if state, attempts, _ := outboxRow(t, database, "event:req-lease"); state != EventDispatching || attempts != 2 {
		t.Fatalf("after the late mark state=%s attempts=%d, want dispatching 2", state, attempts)
	}
	if err := service.settle(ctx, "event:req-lease", takeover, `state = 'sent', lease_until = NULL, error_text = ''`); err != nil {
		t.Fatalf("takeover mark: %v", err)
	}
	if state := outboxState(t, database, "event:req-lease"); state != EventSent {
		t.Fatalf("state=%s, want sent", state)
	}

	pendingEvent(t, service, "req-abandoned")
	service.maxAttempts = 2
	for attempt := 1; attempt <= 2; attempt++ {
		// The dispatcher dies after its claim commits: nothing settles it.
		if _, _, err := service.claimEvent(ctx); err != nil {
			t.Fatalf("claim %d: %v", attempt, err)
		}
		if processed, err := service.DispatchOne(ctx, func(context.Context, Event) error { t.Error("leased event published"); return nil }); processed || err != nil {
			t.Fatalf("leased event claimed: processed=%v err=%v", processed, err)
		}
		clock.Advance(DefaultOutboxLease)
	}
	if processed, err := service.DispatchOne(ctx, func(context.Context, Event) error { t.Error("abandoned event published"); return nil }); processed || err != nil {
		t.Fatalf("abandoned event dispatched: processed=%v err=%v", processed, err)
	}
	if state, attempts, code := outboxRow(t, database, "event:req-abandoned"); state != EventDead || attempts != 2 || code != ClaimAbandoned {
		t.Fatalf("state=%s attempts=%d code=%s, want dead 2 %s", state, attempts, code, ClaimAbandoned)
	}
}

// An outbox created before the dispatch columns gains them and its pending
// events are dispatched.
func TestOutboxCreatedBeforeDispatchColumnsIsMigrated(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE order_outbox (event_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, kind TEXT NOT NULL, payload_json BLOB NOT NULL, state TEXT NOT NULL, created_at INTEGER NOT NULL, UNIQUE(order_id, kind))`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO order_outbox VALUES ('event:old', 'order:old', 'order.created', x'7b7d', 'pending', 0)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	service, err := New(ctx, database, map[string]int64{"coffee": 1500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if processed, err := service.DispatchOne(ctx, func(context.Context, Event) error { return nil }); !processed || err != nil {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if state, attempts, _ := outboxRow(t, database, "event:old"); state != EventSent || attempts != 1 {
		t.Fatalf("state=%s attempts=%d, want sent 1", state, attempts)
	}
}
