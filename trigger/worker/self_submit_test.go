package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
)

// defaultBusyTimeout mirrors the SQLite backend's busy timeout. A nested
// submission the queue cannot diagnose up front waits this long once.
const defaultBusyTimeout = 5 * time.Second

// hangBound fails a nested submission that never returns (#207) instead of
// letting it run into the package's test timeout.
const hangBound = 30 * time.Second

// shortBusyDatabase lowers SQLite's busy timeout on the connection of every
// transaction it opens, through the ordinary transaction API, so a test can
// observe a busy wait without spending the backend's default five seconds.
// It hides the wrapped database's write domain unless forward is set.
type shortBusyDatabase struct {
	store.Database
	timeout time.Duration
}

func (d shortBusyDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return d.Database.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout = %d", d.timeout.Milliseconds())); err != nil {
			return err
		}
		return fn(tx)
	})
}

type forwardingShortBusyDatabase struct{ shortBusyDatabase }

func (d forwardingShortBusyDatabase) WriteDomain() *store.WriteDomain {
	domain, _ := store.WriteDomainOf(d.Database)
	return domain
}

// unannotatedBusyDatabase models a backend that exposes its write domain
// but returns busy errors that do not name it.
type unannotatedBusyDatabase struct{ forwardingShortBusyDatabase }

func (d unannotatedBusyDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	err := d.forwardingShortBusyDatabase.WithTx(ctx, fn)
	if errors.Is(err, store.ErrBusy) {
		return fmt.Errorf("%w: unannotated backend", store.ErrBusy)
	}
	return err
}

type selfSubmit struct {
	// outer holds the claim; nested is the database the handler submits
	// through. They name the same SQLite write domain.
	outer, nested store.Database
	// detach submits with context.Background() instead of the handler's.
	detach bool
	// wait is the busy timeout the nested submission may spend once.
	wait time.Duration
}

// runSelfSubmit processes one job whose handler submits to its own store in
// a way the context check cannot see. The job must fail after a single busy
// wait, be dead-lettered as a nested submission and leave nothing behind.
func runSelfSubmit(t *testing.T, scenario selfSubmit) {
	t.Helper()
	expected := nestedStoreExpected(t, "same-store")
	ctx := context.Background()
	outer, err := New(ctx, scenario.outer, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	nested, err := New(ctx, scenario.nested, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.RegisterKind("outer", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := scenario.outer.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS self_submit_effects (id TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	outerKey, innerKey := "outer-"+t.Name(), "inner-"+t.Name()
	// Three attempts: a nested submission is a programming error, so the
	// job must not be retried even though it could be.
	if _, err := outer.Enqueue(ctx, EnqueueRequest{RequestKey: outerKey, Kind: "outer", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	var nestedErr error
	started := time.Now()
	processed, err := processBounded(outer, func(handlerCtx context.Context, tx Tx, _ Job) error {
		if _, err := tx.ExecContext(handlerCtx, `INSERT INTO self_submit_effects VALUES (?)`, innerKey); err != nil {
			return err
		}
		submitCtx := handlerCtx
		if scenario.detach {
			submitCtx = context.Background()
		}
		_, nestedErr = nested.Submit(submitCtx, trigger.Submission{Key: innerKey, Kind: "inner", Payload: []byte(`{}`)})
		return nestedErr
	})
	elapsed := time.Since(started)
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if !errors.Is(nestedErr, trigger.ErrSaturated) {
		t.Fatalf("the undetected nested submission returned %v; want it to wait out the busy timeout", nestedErr)
	}
	// The saturation names the domain it waited on: the claim's own.
	claimed, _ := store.WriteDomainOf(scenario.outer)
	if domain, named := store.ErrorWriteDomain(nestedErr); !named || !store.SameWriteDomain(domain, claimed) {
		t.Fatalf("the nested saturation %v named domain %v (named=%v); want the claim's", nestedErr, domain, named)
	}
	// One busy wait plus slack for a loaded machine. The job state below
	// proves the job was not redelivered for another wait.
	if limit := scenario.wait + 2*time.Second; elapsed >= limit {
		t.Fatalf("the nested submission took %v; want at most one %v busy wait", elapsed, scenario.wait)
	}
	job, err := outer.Get(ctx, outerKey)
	if err != nil {
		t.Fatal(err)
	}
	if job.State != expected.State || job.Attempt != expected.Attempt || job.Deferrals != expected.Deferrals || job.Error != expected.JobError {
		t.Fatalf("the self-submitting job is %+v; want state=%s attempt=%d deferrals=%d error=%q", job, expected.State, expected.Attempt, expected.Deferrals, expected.JobError)
	}
	if _, err := nested.Get(ctx, innerKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the rejected nested submission was persisted: %v", err)
	}
	var effects int
	if err := scenario.outer.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM self_submit_effects WHERE id = ?`, innerKey).Scan(&effects)
	}); err != nil {
		t.Fatal(err)
	}
	if effects != expected.HandlerEffectRows {
		t.Fatalf("handler effects=%d; want %d", effects, expected.HandlerEffectRows)
	}
}

// processBounded runs one ProcessOnce whose handler submits to a store its
// claim may hold. A submission that never returns holds the claim and cannot
// be canceled, so no cleanup could run: it aborts the test binary with every
// goroutine's stack instead of hanging until the package timeout (#207).
func processBounded(queue *Queue, handler Handler) (bool, error) {
	type outcome struct {
		processed bool
		err       error
	}
	done := make(chan outcome, 1)
	go func() {
		processed, err := queue.ProcessOnce(context.Background(), handler)
		done <- outcome{processed: processed, err: err}
	}()
	select {
	case result := <-done:
		return result.processed, result.err
	case <-time.After(hangBound):
		stacks := make([]byte, 1<<20)
		panic(fmt.Sprintf("nested submission did not return within %v (#207)\n%s", hangBound, stacks[:runtime.Stack(stacks, true)]))
	}
}

func openSQLite(t *testing.T, path string) store.Database {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

// TestHandlerSelfSubmitWithDetachedContextFails: a handler that submits to
// its own store with context.Background() hides the claim from the context
// check. The busy store it then hits is its own claim, not backpressure, so
// the job fails once as a nested submission instead of being deferred for
// MaxDeferrals busy waits (#207). Uses the backend's real busy timeout.
func TestHandlerSelfSubmitWithDetachedContextFails(t *testing.T) {
	database := openSQLite(t, filepath.Join(t.TempDir(), "self.db"))
	runSelfSubmit(t, selfSubmit{outer: database, nested: database, detach: true, wait: defaultBusyTimeout})
}

// TestHandlerSelfSubmitToUnannotatedBackendFails: a backend that exposes its
// write domain but does not name it on its busy errors. The queue names its
// own domain on the saturation it returns, so the job fails the same way.
func TestHandlerSelfSubmitToUnannotatedBackendFails(t *testing.T) {
	database := openSQLite(t, filepath.Join(t.TempDir(), "self.db"))
	wait := 250 * time.Millisecond
	nested := unannotatedBusyDatabase{forwardingShortBusyDatabase{shortBusyDatabase{Database: database, timeout: wait}}}
	runSelfSubmit(t, selfSubmit{outer: database, nested: nested, detach: true, wait: wait})
}

// TestHandlerSelfSubmitThroughOpaqueWrapperFails: a submission through a
// wrapper that does not forward the write domain is invisible to the
// context check even with the handler's context. The store still names the
// domain it was busy on, so the job fails as a nested submission (#207).
func TestHandlerSelfSubmitThroughOpaqueWrapperFails(t *testing.T) {
	database := openSQLite(t, filepath.Join(t.TempDir(), "self.db"))
	for _, detach := range []bool{false, true} {
		t.Run(fmt.Sprintf("detach=%v", detach), func(t *testing.T) {
			wait := 250 * time.Millisecond
			runSelfSubmit(t, selfSubmit{outer: database, nested: shortBusyDatabase{Database: database, timeout: wait}, detach: detach, wait: wait})
		})
	}
}

// TestHandlerSelfSubmitAcrossTwoFileHandlesFails: the queue and the handler's
// submission open the same SQLite file through separate store handles. The
// handles share one write domain, so the submission is diagnosed at once with
// the handler's context, and after one busy wait without it.
func TestHandlerSelfSubmitAcrossTwoFileHandlesFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "shared.db")
	queueDB, handlerDB := openSQLite(t, path), openSQLite(t, path)
	t.Run("handler context", func(t *testing.T) {
		expected := nestedStoreExpected(t, "same-store")
		ctx := context.Background()
		outer, err := New(ctx, queueDB, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		nested, err := New(ctx, handlerDB, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := outer.Enqueue(ctx, EnqueueRequest{RequestKey: "outer-handles", Kind: "outer", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
			t.Fatal(err)
		}
		var nestedErr error
		started := time.Now()
		processed, err := outer.ProcessOnce(ctx, func(ctx context.Context, _ Tx, _ Job) error {
			_, nestedErr = nested.Submit(ctx, trigger.Submission{Key: "inner-handles", Kind: "inner", Payload: []byte(`{}`)})
			return nestedErr
		})
		if err != nil || !processed {
			t.Fatalf("processed=%v err=%v", processed, err)
		}
		if !errors.Is(nestedErr, ErrNestedSubmission) || time.Since(started) >= time.Second {
			t.Fatalf("a submission through a second handle to the claimed file returned %v after %v; want immediate worker.ErrNestedSubmission", nestedErr, time.Since(started))
		}
		job, err := outer.Get(ctx, "outer-handles")
		if err != nil {
			t.Fatal(err)
		}
		if job.State != expected.State || job.Attempt != expected.Attempt || job.Deferrals != expected.Deferrals || job.Error != expected.JobError {
			t.Fatalf("job=%+v; want state=%s attempt=%d deferrals=%d error=%q", job, expected.State, expected.Attempt, expected.Deferrals, expected.JobError)
		}
		if _, err := nested.Get(ctx, "inner-handles"); !errors.Is(err, ErrNotFound) {
			t.Fatalf("the rejected nested submission was persisted: %v", err)
		}
	})
	t.Run("detached context", func(t *testing.T) {
		wait := 250 * time.Millisecond
		runSelfSubmit(t, selfSubmit{outer: queueDB, nested: forwardingShortBusyDatabase{shortBusyDatabase{Database: handlerDB, timeout: wait}}, detach: true, wait: wait})
	})
}

// TestHandlerDetachedSubmitAcrossSharedMemoryHandlesFails: every ":memory:"
// open is one shared database. A detached self-submit through a second
// handle must come back busy within the busy timeout and fail the job; it
// must never block without bound (#207).
func TestHandlerDetachedSubmitAcrossSharedMemoryHandlesFails(t *testing.T) {
	first, second := openSQLite(t, ":memory:"), openSQLite(t, ":memory:")
	runSelfSubmit(t, selfSubmit{outer: first, nested: second, detach: true, wait: defaultBusyTimeout})
}

// TestHandlerDetachedSubmitToAnotherBusyStoreDefers: the domain carried by a
// saturation error must not turn genuine backpressure into a failure. A
// detached submission, or one through an opaque wrapper, to a different busy
// store still defers the job without spending an attempt (#184/#190).
func TestHandlerDetachedSubmitToAnotherBusyStoreDefers(t *testing.T) {
	expected := nestedStoreExpected(t, "other-store-busy")
	wait := 250 * time.Millisecond
	for _, opaque := range []bool{false, true} {
		t.Run(fmt.Sprintf("opaque=%v", opaque), func(t *testing.T) {
			ctx := context.Background()
			outerDB := openSQLite(t, filepath.Join(t.TempDir(), "outer.db"))
			otherDB := openSQLite(t, filepath.Join(t.TempDir(), "other.db"))
			var otherQueueDB store.Database = forwardingShortBusyDatabase{shortBusyDatabase{Database: otherDB, timeout: wait}}
			if opaque {
				otherQueueDB = shortBusyDatabase{Database: otherDB, timeout: wait}
			}
			outer, err := New(ctx, outerDB, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			other, err := New(ctx, otherQueueDB, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := outer.Enqueue(ctx, EnqueueRequest{RequestKey: "outer", Kind: "outer", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
				t.Fatal(err)
			}
			holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
			go func() {
				held <- otherDB.WithTx(ctx, func(tx *sql.Tx) error {
					if _, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET updated_at = updated_at`); err != nil {
						return err
					}
					close(holding)
					<-release
					return nil
				})
			}()
			<-holding
			var released sync.Once
			t.Cleanup(func() { released.Do(func() { close(release) }) })
			var nestedErr error
			processed, err := outer.ProcessOnce(ctx, func(context.Context, Tx, Job) error {
				_, nestedErr = other.Submit(context.Background(), trigger.Submission{Key: "inner", Kind: "nested", Payload: []byte(`{}`)})
				return nestedErr
			})
			released.Do(func() { close(release) })
			if err := <-held; err != nil {
				t.Fatal(err)
			}
			if err != nil || !processed {
				t.Fatalf("processed=%v err=%v", processed, err)
			}
			if !errors.Is(nestedErr, trigger.ErrSaturated) || !errors.Is(nestedErr, store.ErrBusy) || errors.Is(nestedErr, ErrNestedSubmission) {
				t.Fatalf("other-store submission returned %v; want busy saturation", nestedErr)
			}
			job, err := outer.Get(ctx, "outer")
			if err != nil {
				t.Fatal(err)
			}
			if job.State != expected.State || job.Attempt != expected.Attempt || job.Deferrals != expected.Deferrals || job.Error != expected.JobError {
				t.Fatalf("outer job=%+v; want state=%s attempt=%d deferrals=%d error=%q", job, expected.State, expected.Attempt, expected.Deferrals, expected.JobError)
			}
		})
	}
}
