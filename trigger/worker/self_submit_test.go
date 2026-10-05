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
	return openSQLiteWaiting(t, path, 0)
}

// openSQLiteWaiting opens path with a busy timeout of wait (zero for the
// backend's default). A writer queued behind another on the same handle
// waits this long before ErrBusy (#214).
func openSQLiteWaiting(t *testing.T, path string, wait time.Duration) store.Database {
	t.Helper()
	database, err := (sqlite.Backend{BusyTimeout: wait}).Open(context.Background(), path)
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
	wait := 250 * time.Millisecond
	database := openSQLiteWaiting(t, filepath.Join(t.TempDir(), "self.db"), wait)
	nested := unannotatedBusyDatabase{forwardingShortBusyDatabase{shortBusyDatabase{Database: database, timeout: wait}}}
	runSelfSubmit(t, selfSubmit{outer: database, nested: nested, detach: true, wait: wait})
}

// TestHandlerSelfSubmitThroughOpaqueWrapperFails: a submission through a
// wrapper that does not forward the write domain is invisible to the
// context check even with the handler's context. The store still names the
// domain it was busy on, so the job fails as a nested submission (#207).
func TestHandlerSelfSubmitThroughOpaqueWrapperFails(t *testing.T) {
	wait := 250 * time.Millisecond
	database := openSQLiteWaiting(t, filepath.Join(t.TempDir(), "self.db"), wait)
	for _, detach := range []bool{false, true} {
		t.Run(fmt.Sprintf("detach=%v", detach), func(t *testing.T) {
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
			outerDB := openSQLiteWaiting(t, filepath.Join(t.TempDir(), "outer.db"), wait)
			otherDB := openSQLiteWaiting(t, filepath.Join(t.TempDir(), "other.db"), wait)
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

// TestHandlerJoinedSaturationIsClassifiedByEveryDomain: a handler may join a
// self-submit's saturation with saturation from another, genuinely busy
// store. The busy wait on the claim's own domain makes it a nested
// submission whichever order the failures are joined in; joined failures
// that name only other domains remain backpressure (#207).
func TestHandlerJoinedSaturationIsClassifiedByEveryDomain(t *testing.T) {
	for _, scenario := range []struct {
		name, expected string
		join           func(self, other error) error
	}{
		{"other-then-self", "same-store", func(self, other error) error { return errors.Join(other, self) }},
		{"self-then-other", "same-store", func(self, other error) error { return errors.Join(self, other) }},
		{"other-only", "other-store-busy", func(_, other error) error {
			return fmt.Errorf("handler: %w", errors.Join(errors.New("audit write skipped"), other))
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			expected := nestedStoreExpected(t, scenario.expected)
			wait := 250 * time.Millisecond
			ctx := context.Background()
			outerDB := openSQLiteWaiting(t, filepath.Join(t.TempDir(), "outer.db"), wait)
			otherDB := openSQLiteWaiting(t, filepath.Join(t.TempDir(), "other.db"), wait)
			outer, err := New(ctx, outerDB, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			// Detached and behind a wrapper that hides the write domain, so
			// only the saturation's annotation can identify the claim.
			self, err := New(ctx, shortBusyDatabase{Database: outerDB, timeout: wait}, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			other, err := New(ctx, forwardingShortBusyDatabase{shortBusyDatabase{Database: otherDB, timeout: wait}}, time.Now)
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
			var selfErr, otherErr error
			processed, err := processBounded(outer, func(context.Context, Tx, Job) error {
				_, otherErr = other.Submit(context.Background(), trigger.Submission{Key: "other", Kind: "nested", Payload: []byte(`{}`)})
				if scenario.expected == "same-store" {
					_, selfErr = self.Submit(context.Background(), trigger.Submission{Key: "self", Kind: "nested", Payload: []byte(`{}`)})
				}
				return scenario.join(selfErr, otherErr)
			})
			released.Do(func() { close(release) })
			if err := <-held; err != nil {
				t.Fatal(err)
			}
			if err != nil || !processed {
				t.Fatalf("processed=%v err=%v", processed, err)
			}
			claimed, _ := store.WriteDomainOf(outerDB)
			if domain, named := store.ErrorWriteDomain(otherErr); !errors.Is(otherErr, trigger.ErrSaturated) || !named || store.SameWriteDomain(domain, claimed) {
				t.Fatalf("other-store submission returned %v naming %v; want saturation on another domain", otherErr, domain)
			}
			if scenario.expected == "same-store" {
				if domain, named := store.ErrorWriteDomain(selfErr); !errors.Is(selfErr, trigger.ErrSaturated) || !named || !store.SameWriteDomain(domain, claimed) {
					t.Fatalf("self-submission returned %v naming %v; want saturation on the claim's domain", selfErr, domain)
				}
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

// TestNestedSubmissionJoinedWithRetryableErrorIsNotRetried: a handler that
// joins its nested submission with a retryable HandlerError still fails the
// job once. Retrying would submit to its own claimed store on every attempt
// (#225). Both diagnoses are covered: up front from the handler's context,
// and after one busy wait through a wrapper that hides the write domain. A
// retryable HandlerError on its own is still retried.
func TestNestedSubmissionJoinedWithRetryableErrorIsNotRetried(t *testing.T) {
	wait := 250 * time.Millisecond
	for _, scenario := range []struct {
		name     string
		nested   bool
		detected func(store.Database) store.Database
		submit   func(handlerCtx context.Context) context.Context
	}{
		{"context-diagnosed", true, func(db store.Database) store.Database { return db }, func(ctx context.Context) context.Context { return ctx }},
		{"busy-wait-diagnosed", true, func(db store.Database) store.Database { return shortBusyDatabase{Database: db, timeout: wait} }, func(context.Context) context.Context { return context.Background() }},
		{"retryable-alone", false, nil, nil},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			database := openSQLiteWaiting(t, filepath.Join(t.TempDir(), "jobs.db"), wait)
			queue, err := New(ctx, database, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			var self *Queue
			if scenario.nested {
				if self, err = New(ctx, scenario.detected(database), time.Now); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "job", Kind: "job", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
				t.Fatal(err)
			}
			var nestedErr error
			processed, err := processBounded(queue, func(handlerCtx context.Context, _ Tx, _ Job) error {
				retry := &HandlerError{Retryable: true, Message: "provider timeout"}
				if !scenario.nested {
					return retry
				}
				_, nestedErr = self.Submit(scenario.submit(handlerCtx), trigger.Submission{Key: "inner", Kind: "inner", Payload: []byte(`{}`)})
				return errors.Join(retry, nestedErr)
			})
			if err != nil || !processed {
				t.Fatalf("processed=%v err=%v", processed, err)
			}
			switch scenario.name {
			case "context-diagnosed":
				if !errors.Is(nestedErr, ErrNestedSubmission) {
					t.Fatalf("submission=%v; want ErrNestedSubmission from the context check", nestedErr)
				}
			case "busy-wait-diagnosed":
				if !errors.Is(nestedErr, trigger.ErrSaturated) || errors.Is(nestedErr, ErrNestedSubmission) {
					t.Fatalf("submission=%v; want saturation after a busy wait, diagnosed only by ProcessOnce", nestedErr)
				}
			}
			job, err := queue.Get(ctx, "job")
			if err != nil {
				t.Fatal(err)
			}
			if !scenario.nested {
				if job.State != StatePending || job.Attempt != 1 || job.Error != "provider timeout" {
					t.Fatalf("retryable job=%+v; want pending for another attempt", job)
				}
				return
			}
			expected := nestedStoreExpected(t, "same-store")
			if job.State != expected.State || job.Attempt != expected.Attempt || job.Deferrals != expected.Deferrals || job.Error != expected.JobError {
				t.Fatalf("job=%+v; want state=%s attempt=%d deferrals=%d error=%q", job, expected.State, expected.Attempt, expected.Deferrals, expected.JobError)
			}
		})
	}
}

// TestHandlerReadingItsOwnStoreCompletes: a handler that reads its own store
// through an ordinary transaction, as examples/order's Service.Get does,
// runs beside its claim and completes. Only the store's marked writers are
// queued (#214); a read nested in a claim is not one of them.
func TestHandlerReadingItsOwnStoreCompletes(t *testing.T) {
	ctx := context.Background()
	database := openSQLite(t, filepath.Join(t.TempDir(), "read.db"))
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "job", Kind: "job", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	var readErr error
	began := time.Now()
	processed, err := processBounded(queue, func(handlerCtx context.Context, _ Tx, _ Job) error {
		var jobs int
		readErr = database.WithTx(handlerCtx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(handlerCtx, `SELECT COUNT(*) FROM worker_jobs`).Scan(&jobs)
		})
		if readErr == nil && jobs != 1 {
			readErr = fmt.Errorf("read %d jobs", jobs)
		}
		if _, err := queue.Get(handlerCtx, "job"); err != nil && readErr == nil {
			readErr = err
		}
		return readErr
	})
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if readErr != nil {
		t.Fatalf("the handler's read of its own store failed: %v", readErr)
	}
	if took := time.Since(began); took > time.Second {
		t.Fatalf("the handler's read waited %v", took)
	}
	if job, err := queue.Get(ctx, "job"); err != nil || job.State != StateCompleted {
		t.Fatalf("job=%+v err=%v; want completed", job, err)
	}
}

// writerMarks records whether each transaction reached the store marked as
// a writer.
type writerMarks struct {
	store.Database
	mu    sync.Mutex
	marks []bool
}

func (d *writerMarks) WriteDomain() *store.WriteDomain {
	w, _ := store.WriteDomainOf(d.Database)
	return w
}

func (d *writerMarks) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	d.mu.Lock()
	d.marks = append(d.marks, store.IsWriter(ctx))
	d.mu.Unlock()
	return d.Database.WithTx(ctx, fn)
}

func (d *writerMarks) take() []bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	marks := d.marks
	d.marks = nil
	return marks
}

// TestQueueWritesAreMarkedAndReadsAreNot: the queue's write-first
// transactions take turns in the store's writer queue and its reads do not
// (#214). Losing a mark would let a write contend unordered again; adding
// one to a read would queue it behind running handlers.
func TestQueueWritesAreMarkedAndReadsAreNot(t *testing.T) {
	ctx := context.Background()
	marks := &writerMarks{Database: openSQLite(t, filepath.Join(t.TempDir(), "marks.db"))}
	queue, err := New(ctx, marks, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	marks.take()
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "job", Kind: "job", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if got := marks.take(); len(got) != 1 || !got[0] {
		t.Fatalf("Enqueue marks=%v; want one marked writer", got)
	}
	if _, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if got := marks.take(); len(got) != 1 || !got[0] {
		t.Fatalf("ProcessOnce marks=%v; want one marked claim", got)
	}
	if _, err := queue.Get(ctx, "job"); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Settled(ctx, "job"); err != nil {
		t.Fatal(err)
	}
	if got := marks.take(); len(got) != 2 || got[0] || got[1] {
		t.Fatalf("Get/Settled marks=%v; want two unmarked reads", got)
	}
}

// TestClaimAccountingWritesAreMarked: charging a lost claim and deferring a
// lost consumer write first and take turns in the store's writer queue
// (#214), like the claim itself.
func TestClaimAccountingWritesAreMarked(t *testing.T) {
	ctx := context.Background()
	marks := &writerMarks{Database: openSQLite(t, filepath.Join(t.TempDir(), "accounting.db"))}
	queue, err := New(ctx, marks, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "job", Kind: "job", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	job, err := queue.Get(ctx, "job")
	if err != nil {
		t.Fatal(err)
	}
	marks.take()
	if err := queue.chargeLostClaim(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := queue.deferLost(ctx, job); err != nil {
		t.Fatal(err)
	}
	if got := marks.take(); len(got) != 2 || !got[0] || !got[1] {
		t.Fatalf("chargeLostClaim/deferLost marks=%v; want two marked writers", got)
	}
}
