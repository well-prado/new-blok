package worker

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// panicBusyTimeout is the store's busy timeout in this file. A write that
// has to wait for a transaction a panicking handler left open waits it out
// and fails; one that does not finishes in well under it.
const panicBusyTimeout = 2 * time.Second

// panickedError is the error a panicked attempt records: HandlerPanicked,
// spelled out so the stored value is pinned.
const panickedError = "handler_panicked"

type handlerPanic struct{ name string }

// processLeaving runs ProcessOnce on its own goroutine, so a handler's
// runtime.Goexit ends only that goroutine, and reports what its caller
// recovered and whether ProcessOnce returned at all.
func processLeaving(ctx context.Context, queue *Queue, handler Handler) (recovered any, returned bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { recovered = recover() }()
		_, _ = queue.ProcessOnce(ctx, handler)
		returned = true
	}()
	<-done
	return recovered, returned
}

// TestPanickingHandlerRollsBackAndFailsItsAttempt: a handler writes through
// its Tx and then panics, or calls runtime.Goexit, and the caller of
// ProcessOnce recovers the panic, as a supervisor that keeps its worker alive
// does (#267). Then:
//
//   - the panic reaches the caller with its own value;
//   - another job, processed by a separate Queue on its own handle to the
//     same file, completes in well under the busy timeout: the handle
//     transaction, and with it the write lock, was rolled back, and the
//     claim turn and the store's writer turn were released;
//   - the handler's write is not committed;
//   - the attempt, counted when it started, failed: the job is pending
//     again after the usual backoff, with error HandlerPanicked and no
//     deferral charged, and dead once MaxAttempts handler runs have panicked;
//   - a handler context retained past the panic no longer claims the write
//     domain, so a submission through it is not refused as nested.
//
// Before #267 the handle transaction stayed open: the next ProcessOnce, and
// every Enqueue, on the file failed busy until the process exited.
func TestPanickingHandlerRollsBackAndFailsItsAttempt(t *testing.T) {
	value := &handlerPanic{name: "handler bug"}
	for _, exit := range []struct {
		name      string
		leave     func()
		recovered any
	}{
		{name: "panic", leave: func() { panic(value) }, recovered: value},
		{name: "runtime.Goexit", leave: runtime.Goexit},
	} {
		t.Run(exit.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "panic.db")
			backend := sqlite.Backend{BusyTimeout: panicBusyTimeout}
			database, err := backend.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			now := time.Unix(1_700_000_000, 0)
			clock := func() time.Time { return now }
			queue, err := New(ctx, database, clock)
			if err != nil {
				t.Fatal(err)
			}
			if err := database.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `CREATE TABLE effects (job TEXT NOT NULL)`)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			for _, key := range []string{"panics", "next"} {
				if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: key, Kind: "test", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
					t.Fatal(err)
				}
			}
			var retained context.Context
			leaving := func(handlerCtx context.Context, tx Tx, job Job) error {
				if job.RequestKey != "panics" {
					t.Errorf("the panicking handler got job %q; want %q", job.RequestKey, "panics")
					return nil
				}
				retained = handlerCtx
				if _, err := tx.ExecContext(handlerCtx, `INSERT INTO effects VALUES ('panics')`); err != nil {
					return err
				}
				exit.leave()
				return nil
			}
			recovered, returned := processLeaving(ctx, queue, leaving)
			if returned || recovered != exit.recovered {
				t.Fatalf("returned=%v recovered=%#v; want ProcessOnce to propagate the handler's exit with its original value %#v", returned, recovered, exit.recovered)
			}

			otherDatabase, err := backend.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer otherDatabase.Close()
			other, err := New(ctx, otherDatabase, clock)
			if err != nil {
				t.Fatal(err)
			}
			var handled string
			begin := time.Now()
			processed, err := other.ProcessOnce(ctx, func(handlerCtx context.Context, tx Tx, job Job) error {
				handled = job.RequestKey
				_, err := tx.ExecContext(handlerCtx, `INSERT INTO effects VALUES (?)`, job.RequestKey)
				return err
			})
			if elapsed := time.Since(begin); err != nil || !processed || elapsed > panicBusyTimeout/4 {
				t.Fatalf("the next ProcessOnce, on a separate Queue, took %v: processed=%v err=%v; want it to complete within %v (busy timeout %v)", elapsed, processed, err, panicBusyTimeout/4, panicBusyTimeout)
			}
			if handled != "next" {
				t.Fatalf("the next ProcessOnce handled %q; want %q, the panicked job waiting out its backoff", handled, "next")
			}

			assertPanicked(t, other, now, 1, StatePending)
			effects := countEffects(t, otherDatabase)
			if effects["panics"] != 0 || effects["next"] != 1 {
				t.Fatalf("committed effects %v; want none from the panicked handler and one from the next job", effects)
			}

			// The handler context outlived its claim; it must not still claim
			// the write domain (#188, #207).
			if _, err := queue.Enqueue(retained, EnqueueRequest{RequestKey: "after", Kind: "test", Payload: []byte(`{}`)}); err != nil {
				if errors.Is(err, ErrNestedSubmission) {
					t.Fatalf("a submission through the panicked handler's retained context was refused as nested: %v", err)
				}
				t.Fatal(err)
			}

			// The attempt budget: each panic fails one attempt, and the third
			// dead-letters the job.
			for attempt := 2; attempt <= 3; attempt++ {
				now = now.Add(time.Hour)
				recovered, returned := processLeaving(ctx, queue, leaving)
				if returned || recovered != exit.recovered {
					t.Fatalf("attempt %d: returned=%v recovered=%#v; want the handler's exit to propagate", attempt, returned, recovered)
				}
				want := StatePending
				if attempt == 3 {
					want = StateDead
				}
				assertPanicked(t, other, now, attempt, want)
			}
			if countEffects(t, otherDatabase)["panics"] != 0 {
				t.Fatal("a panicked handler's write was committed")
			}
			// The queue still works: the job submitted through the retained
			// context runs.
			if processed, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { return nil }); err != nil || !processed {
				t.Fatalf("after the job dead-lettered: processed=%v err=%v", processed, err)
			}
		})
	}
}

// assertPanicked checks the panicked job's accounting: its attempt is
// counted and failed with handler_panicked, no deferral is charged, its lease
// is cleared, and a pending job waits the backoff of its attempt.
func assertPanicked(t *testing.T, queue *Queue, now time.Time, attempt int, state string) {
	t.Helper()
	job, err := queue.Get(context.Background(), "panics")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != state || job.Attempt != attempt || job.Deferrals != 0 || job.Error != panickedError {
		t.Fatalf("after panic %d the job is state=%s attempt=%d deferrals=%d error=%q; want state=%s attempt=%d deferrals=0 error=%q", attempt, job.State, job.Attempt, job.Deferrals, job.Error, state, attempt, panickedError)
	}
	var available int64
	var lease sql.NullInt64
	if err := queue.database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT available_at, lease_until FROM worker_jobs WHERE request_key = 'panics'`).Scan(&available, &lease)
	}); err != nil {
		t.Fatal(err)
	}
	if lease.Valid {
		t.Fatalf("after panic %d the job still holds lease %d", attempt, lease.Int64)
	}
	if want := now.Add(time.Duration(attempt) * time.Second).UnixNano(); state == StatePending && available != want {
		t.Fatalf("after panic %d the job is available at %d; want now + %d s = %d", attempt, available, attempt, want)
	}
}

func countEffects(t *testing.T, database store.Database) map[string]int {
	t.Helper()
	effects := map[string]int{}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(context.Background(), `SELECT job FROM effects`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var job string
			if err := rows.Scan(&job); err != nil {
				return err
			}
			effects[job]++
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return effects
}
