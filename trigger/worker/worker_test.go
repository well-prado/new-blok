package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
)

type nestedStoreExpectation struct {
	Name                 string `json:"name"`
	NestedError          string `json:"nestedError"`
	ProcessError         string `json:"processError"`
	State                string `json:"state"`
	JobError             string `json:"jobError"`
	Attempt              int    `json:"attempt"`
	Deferrals            int    `json:"deferrals"`
	NestedSubmissionRows int    `json:"nestedSubmissionRows"`
	HandlerEffectRows    int    `json:"handlerEffectRows"`
}

func nestedStoreExpected(t *testing.T, name string) nestedStoreExpectation {
	t.Helper()
	data, err := os.ReadFile("testdata/nested-store.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SchemaVersion string                   `json:"schemaVersion"`
		Cases         []nestedStoreExpectation `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != "worker-nested-store/v1" {
		t.Fatalf("unexpected nested-store fixture schema %q", fixture.SchemaVersion)
	}
	for _, expected := range fixture.Cases {
		if expected.Name == name {
			return expected
		}
	}
	t.Fatalf("nested-store fixture %q is missing", name)
	return nestedStoreExpectation{}
}

func nestedSubmissionRows(t *testing.T, database store.Database, requestKey string) int {
	t.Helper()
	rows := 0
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs WHERE request_key = ?`, requestKey).Scan(&rows)
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func TestEnqueueDeduplicatesAndConflicts(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := time.Unix(100, 0)
	queue, err := New(context.Background(), database, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	request := EnqueueRequest{RequestKey: "order-1", Kind: "order.create", Payload: []byte(`{"sku":"coffee"}`)}
	first, err := queue.Enqueue(context.Background(), request)
	if err != nil || !first.Accepted {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := queue.Enqueue(context.Background(), request)
	if err != nil || second.Accepted || second.Job.ID != first.Job.ID {
		t.Fatalf("second=%+v err=%v", second, err)
	}
	request.Payload = []byte(`{"sku":"tea"}`)
	if _, err := queue.Enqueue(context.Background(), request); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict=%v", err)
	}
}

func TestProcessRetriesThenDeadLettersAndSuccessAcknowledges(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := time.Unix(100, 0)
	queue, err := New(context.Background(), database, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: "retry", Kind: "test", Payload: []byte(`{}`), MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	called := 0
	for i := 0; i < 2; i++ {
		processed, err := queue.ProcessOnce(context.Background(), func(_ context.Context, _ Tx, _ Job) error {
			called++
			return &HandlerError{Retryable: true, Message: "provider timeout"}
		})
		if err != nil || !processed {
			t.Fatalf("attempt %d processed=%v err=%v", i, processed, err)
		}
		clock = clock.Add(time.Second)
	}
	job, err := queue.Get(context.Background(), "retry")
	if err != nil || job.State != StateDead || called != 2 {
		t.Fatalf("job=%+v called=%d err=%v", job, called, err)
	}
}

func TestHandlerFailureRollsBackBusinessWritesBeforeRetryState(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := time.Unix(100, 0)
	queue, err := New(context.Background(), database, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE handler_writes (id INTEGER PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: "partial", Kind: "test", Payload: []byte(`{}`), MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, _ Job) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO handler_writes (id) VALUES (1)`); err != nil {
			return err
		}
		return &HandlerError{Retryable: true, Message: "temporary failure"}
	})
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	var count int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM handler_writes`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("handler writes=%d, want rollback on handler failure", count)
	}
	job, err := queue.Get(context.Background(), "partial")
	if err != nil || job.State != StatePending || job.Attempt != 1 {
		t.Fatalf("job=%+v err=%v", job, err)
	}
}

func TestProcessKillRollsBackBusinessWriteAndAcknowledgment(t *testing.T) {
	if os.Getenv("NEWBLOK_WORKER_CHILD") == "1" {
		runWorkerCrashChild()
		return
	}

	directory := t.TempDir()
	databasePath := filepath.Join(directory, "worker.db")
	markerPath := filepath.Join(directory, "marker")
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE worker_effects (id INTEGER PRIMARY KEY, request_key TEXT NOT NULL)`)
		return err
	}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: "crash-order", Kind: "order.create", Payload: []byte(`{}`)}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	command := exec.Command(os.Args[0], "-test.run=TestProcessKillRollsBackBusinessWriteAndAcknowledgment", "-test.v")
	command.Env = append(os.Environ(), "NEWBLOK_WORKER_CHILD=1", "NEWBLOK_WORKER_PATH="+databasePath, "NEWBLOK_WORKER_MARKER="+markerPath)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForWorkerMarker(t, markerPath)
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()

	database, err = (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	// The killed worker started an attempt and leased the job; a worker
	// restarted after that lease expires claims it again (#245).
	queue, err = New(context.Background(), database, func() time.Time { return time.Now().Add(DefaultLease + time.Second) })
	if err != nil {
		t.Fatal(err)
	}
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, job Job) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO worker_effects (id, request_key) VALUES (?, ?)`, 1, job.RequestKey)
		return err
	})
	if err != nil || !processed {
		t.Fatalf("recovery processed=%v err=%v", processed, err)
	}
	var count int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_effects`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("effect rows=%d, want one committed recovery write", count)
	}
	// The killed attempt counts (#245): the recovery is the second.
	job, err := queue.Get(context.Background(), "crash-order")
	if err != nil || job.State != StateCompleted || job.Attempt != 2 {
		t.Fatalf("recovered job=%+v err=%v; want completed at attempt 2", job, err)
	}
}

func runWorkerCrashChild() {
	database, err := (sqlite.Backend{}).Open(context.Background(), os.Getenv("NEWBLOK_WORKER_PATH"))
	if err != nil {
		panic(err)
	}
	defer database.Close()
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		panic(err)
	}
	if _, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, job Job) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_effects (id, request_key) VALUES (?, ?)`, 1, job.RequestKey); err != nil {
			return err
		}
		if err := os.WriteFile(os.Getenv("NEWBLOK_WORKER_MARKER"), []byte("ready"), 0o600); err != nil {
			return err
		}
		time.Sleep(10 * time.Second)
		return nil
	}); err != nil {
		panic(err)
	}
}

// TestCrashLoopingJobDeadLettersWithinMaxAttempts: a poison job whose handler
// kills its own worker process, every time (#245). The killed process never
// commits its claim transaction, so the attempt that claim counted would roll
// back with it. The parent restarts the worker more times than the job has
// attempts, each restart later than the last lease and backoff; the job must
// be dead-lettered after exactly MaxAttempts handler runs, with none of the
// handler's writes committed, instead of being redelivered forever. A worker
// restarted before the killed attempt's lease expires claims nothing.
func TestCrashLoopingJobDeadLettersWithinMaxAttempts(t *testing.T) {
	if os.Getenv("NEWBLOK_WORKER_CRASH_LOOP_CHILD") == "1" {
		runCrashLoopChild()
		return
	}
	const maxAttempts, restarts = 3, 10
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "worker.db")
	entriesPath := filepath.Join(directory, "handler-entries")
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE worker_effects (request_key TEXT NOT NULL)`)
		return err
	}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: "poison", Kind: "crash", Payload: []byte(`{}`), MaxAttempts: maxAttempts}); err != nil {
		database.Close()
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	inspect := func() (Job, int) {
		t.Helper()
		database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		queue, err := New(context.Background(), database, nil)
		if err != nil {
			t.Fatal(err)
		}
		job, err := queue.Get(context.Background(), "poison")
		if err != nil {
			t.Fatal(err)
		}
		effects := 0
		if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
			return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_effects`).Scan(&effects)
		}); err != nil {
			t.Fatal(err)
		}
		return job, effects
	}
	handlerRuns := func() int {
		data, err := os.ReadFile(entriesPath)
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		if err != nil {
			t.Fatal(err)
		}
		return len(data)
	}

	var evolution []string
	deadAfter := -1
	for restart := 1; restart <= restarts; restart++ {
		// Each restart is an hour later on the worker's clock than the last:
		// past any lease and retry backoff the previous run left behind.
		command := exec.Command(os.Args[0], "-test.run=^TestCrashLoopingJobDeadLettersWithinMaxAttempts$")
		command.Env = append(os.Environ(), "NEWBLOK_WORKER_CRASH_LOOP_CHILD=1", "NEWBLOK_WORKER_PATH="+databasePath, "NEWBLOK_WORKER_ENTRIES="+entriesPath, fmt.Sprintf("NEWBLOK_WORKER_CLOCK_OFFSET=%dh", restart))
		output, runErr := command.CombinedOutput()
		exit := "exit 0"
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exit = exitErr.ProcessState.String()
		} else if runErr != nil {
			t.Fatalf("restart %d: %v", restart, runErr)
		}
		job, effects := inspect()
		evolution = append(evolution, fmt.Sprintf("restart %d: %s, handler runs=%d, state=%s attempt=%d error=%q effects=%d", restart, exit, handlerRuns(), job.State, job.Attempt, job.Error, effects))
		if effects != 0 {
			t.Fatalf("a killed handler's write was committed:\n%s", strings.Join(evolution, "\n"))
		}
		if exit != "exit 0" && handlerRuns() != restart {
			t.Fatalf("restart %d ended (%s) without the handler killing it: %s\n%s", restart, exit, output, strings.Join(evolution, "\n"))
		}
		if job.State == StateDead {
			deadAfter = restart
			break
		}
		// A worker restarted at once, on the same clock, finds the killed
		// attempt's lease still held and claims nothing.
		again := exec.Command(os.Args[0], "-test.run=^TestCrashLoopingJobDeadLettersWithinMaxAttempts$")
		again.Env = append(os.Environ(), "NEWBLOK_WORKER_CRASH_LOOP_CHILD=1", "NEWBLOK_WORKER_PATH="+databasePath, "NEWBLOK_WORKER_ENTRIES="+entriesPath, fmt.Sprintf("NEWBLOK_WORKER_CLOCK_OFFSET=%dh", restart))
		if output, err := again.CombinedOutput(); !errors.As(err, &exitErr) || exitErr.ExitCode() != 3 || handlerRuns() != restart {
			t.Fatalf("restart %d: a worker restarted within the lease returned %v (%s); want exit 3 with no handler run", restart, err, output)
		}
	}
	t.Logf("attempt evolution:\n%s", strings.Join(evolution, "\n"))
	if deadAfter < 0 {
		t.Fatalf("the crash-looping job was never dead-lettered after %d restarts (MaxAttempts=%d):\n%s", restarts, maxAttempts, strings.Join(evolution, "\n"))
	}
	job, _ := inspect()
	if runs := handlerRuns(); runs != maxAttempts || job.Attempt != maxAttempts {
		t.Fatalf("the job dead-lettered after %d handler runs at attempt %d; want exactly MaxAttempts=%d:\n%s", runs, job.Attempt, maxAttempts, strings.Join(evolution, "\n"))
	}
}

// handlerTxDatabase runs a hook just ahead of one transaction: by default
// ProcessOnce's handler transaction, the second after arm (the first is the
// claim that starts the attempt). An error from the hook is returned in place
// of that transaction, which never begins. It forwards the write domain and
// busy timeout of the database it wraps.
type handlerTxDatabase struct {
	store.Database
	mu           sync.Mutex
	before       func(context.Context, store.Database) error
	at           int
	transactions int
}

// arm runs before ahead of the at-th transaction from now.
func (database *handlerTxDatabase) arm(at int, before func(context.Context, store.Database) error) {
	database.mu.Lock()
	defer database.mu.Unlock()
	database.before, database.at, database.transactions = before, at, 0
}

func (database *handlerTxDatabase) WriteDomain() *store.WriteDomain {
	domain, _ := store.WriteDomainOf(database.Database)
	return domain
}

func (database *handlerTxDatabase) BusyTimeout() time.Duration {
	if provider, ok := database.Database.(interface{ BusyTimeout() time.Duration }); ok {
		return provider.BusyTimeout()
	}
	return 0
}

func (database *handlerTxDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	database.mu.Lock()
	database.transactions++
	before := database.before
	if database.transactions != database.at {
		before = nil
	}
	database.mu.Unlock()
	if before != nil {
		if err := before(ctx, database.Database); err != nil {
			return err
		}
	}
	return database.Database.WithTx(ctx, fn)
}

// TestStartedAttemptIsGivenBackWhenItsHandlerNeverRuns: the claim that
// starts an attempt commits, then the handler's transaction fails before the
// handler runs (here, busy). The attempt is given back and the job released
// at once, as when the claim itself fails busy; it is not left leased (#245).
func TestStartedAttemptIsGivenBackWhenItsHandlerNeverRuns(t *testing.T) {
	ctx := context.Background()
	sqliteDB, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "given-back.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteDB.Close()
	database := &handlerTxDatabase{Database: sqliteDB}
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "given-back", Kind: "test", Payload: []byte(`{}`), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	database.arm(2, func(context.Context, store.Database) error { return store.ErrBusy })
	ran := 0
	processed, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { ran++; return nil })
	if processed || !errors.Is(err, store.ErrBusy) || ran != 0 {
		t.Fatalf("processed=%v err=%v ran=%d; want the busy handler transaction reported and no handler run", processed, err, ran)
	}
	if job, err := queue.Get(ctx, "given-back"); err != nil || job.State != StatePending || job.Attempt != 0 {
		t.Fatalf("job=%+v err=%v; want pending with its attempt given back", job, err)
	}
	database.arm(0, nil)
	if processed, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { ran++; return nil }); err != nil || !processed || ran != 1 {
		t.Fatalf("redelivery processed=%v err=%v ran=%d; want the job's one attempt to run at once", processed, err, ran)
	}
	if job, err := queue.Get(ctx, "given-back"); err != nil || job.State != StateCompleted || job.Attempt != 1 {
		t.Fatalf("job=%+v err=%v; want completed at attempt 1", job, err)
	}
}

// TestExpiredLeaseIsNotTakenOver: a worker's started attempt outlives its
// lease before its handler transaction gets the write lock, and another
// worker claims the job meanwhile. The first worker must not run its
// handler over the other's claim (#245).
func TestExpiredLeaseIsNotTakenOver(t *testing.T) {
	ctx := context.Background()
	sqliteDB, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "expired.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteDB.Close()
	database := &handlerTxDatabase{Database: sqliteDB}
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "expired", Kind: "test", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	// Another worker's claim, after the lease expired: a new lease and the
	// next attempt.
	database.arm(2, func(ctx context.Context, other store.Database) error {
		return other.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET attempt = attempt + 1, lease_until = lease_until + ? WHERE request_key = 'expired'`, int64(time.Hour))
			return err
		})
	})
	ran := 0
	processed, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { ran++; return nil })
	if processed || !errors.Is(err, ErrClaimLost) || ran != 0 {
		t.Fatalf("processed=%v err=%v ran=%d; want ErrClaimLost and no handler run", processed, err, ran)
	}
	if job, err := queue.Get(ctx, "expired"); err != nil || job.State != StateProcessing || job.Attempt != 2 {
		t.Fatalf("job=%+v err=%v; want it left to the other worker's claim", job, err)
	}
}

// rawJob reads a job's row straight from the database, outside any queue.
func rawJob(t *testing.T, database store.Database, requestKey string) (state string, attempt, deferrals int) {
	t.Helper()
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT state, attempt, deferrals FROM worker_jobs WHERE request_key = ?`, requestKey).Scan(&state, &attempt, &deferrals)
	}); err != nil {
		t.Fatal(err)
	}
	return state, attempt, deferrals
}

// waitProcessing polls until at least want jobs are processing, or gives up
// after limit. It reports whether they were.
func waitProcessing(database store.Database, want int, limit time.Duration) bool {
	deadline := time.Now().Add(limit)
	for time.Now().Before(deadline) {
		processing := 0
		if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
			return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs WHERE state = ?`, StateProcessing).Scan(&processing)
		}); err == nil && processing >= want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestSlowHandlerDoesNotStrandAJobStartedBesideIt: worker A's handler takes
// longer than the busy timeout, and worker B, on another Queue over the same
// store in this process, tries to start a job between A's start and A's
// handler. B must not start an attempt it cannot finish: an attempt started
// there waited out the busy timeout behind A's handler, failed to give the
// attempt back for the same reason, and left the job leased with an attempt
// charged (#245 review). B fails busy and its job is untouched, as when
// a claim waited behind a handler before #245.
func TestSlowHandlerDoesNotStrandAJobStartedBesideIt(t *testing.T) {
	ctx := context.Background()
	sqliteDB, err := (sqlite.Backend{BusyTimeout: 100 * time.Millisecond}).Open(ctx, filepath.Join(t.TempDir(), "beside.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteDB.Close()
	aDB, bDB := &handlerTxDatabase{Database: sqliteDB}, &handlerTxDatabase{Database: sqliteDB}
	a, err := New(ctx, aDB, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	b, err := New(ctx, bDB, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"slow", "beside"} {
		if _, err := a.Enqueue(ctx, EnqueueRequest{RequestKey: key, Kind: "test", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	entered := make(chan struct{})
	bDone := make(chan error, 1)
	// B's handler transaction, if B gets that far, waits until A's slow
	// handler holds the write lock.
	bDB.arm(2, func(context.Context, store.Database) error {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
		}
		return nil
	})
	// Between A's start and A's handler, B tries to start the other job.
	aDB.arm(2, func(context.Context, store.Database) error {
		go func() {
			_, err := b.ProcessOnce(ctx, func(context.Context, Tx, Job) error { return nil })
			bDone <- err
		}()
		waitProcessing(sqliteDB, 2, time.Second)
		return nil
	})
	processed, err := a.ProcessOnce(ctx, func(_ context.Context, _ Tx, job Job) error {
		close(entered)
		time.Sleep(350 * time.Millisecond)
		return nil
	})
	if err != nil || !processed {
		t.Fatalf("A processed=%v err=%v", processed, err)
	}
	bErr := <-bDone
	if !errors.Is(bErr, store.ErrBusy) {
		t.Fatalf("B returned %v; want store.ErrBusy", bErr)
	}
	if state, attempt, _ := rawJob(t, sqliteDB, "slow"); state != StateCompleted || attempt != 1 {
		t.Fatalf("A's job is %s at attempt %d; want completed at 1", state, attempt)
	}
	if state, attempt, deferrals := rawJob(t, sqliteDB, "beside"); state != StatePending || attempt != 0 || deferrals != 0 {
		t.Fatalf("B's job is %s at attempt %d with %d deferrals; want it untouched (pending, 0, 0)", state, attempt, deferrals)
	}
}

// TestConsumerLostBeforeTheHandlerStartsGivesTheAttemptBack: the consumer
// is canceled after the claim that starts the attempt and before the
// handler's transaction. The handler must not run on a context already
// canceled; the attempt is given back without charging a deferral, and the
// job is available again at once (#245 review).
func TestConsumerLostBeforeTheHandlerStartsGivesTheAttemptBack(t *testing.T) {
	ctx := context.Background()
	sqliteDB, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "canceled.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sqliteDB.Close()
	database := &handlerTxDatabase{Database: sqliteDB}
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "canceled", Kind: "test", Payload: []byte(`{}`), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	consumer, cancel := context.WithCancel(ctx)
	defer cancel()
	database.arm(2, func(context.Context, store.Database) error { cancel(); return nil })
	ran := 0
	processed, err := queue.ProcessOnce(consumer, func(context.Context, Tx, Job) error { ran++; return nil })
	if processed || !errors.Is(err, ErrConsumerLost) || ran != 0 {
		t.Fatalf("processed=%v err=%v ran=%d; want ErrConsumerLost and no handler run", processed, err, ran)
	}
	if state, attempt, deferrals := rawJob(t, sqliteDB, "canceled"); state != StatePending || attempt != 0 || deferrals != 0 {
		t.Fatalf("job is %s at attempt %d with %d deferrals; want pending, attempt given back, no deferral", state, attempt, deferrals)
	}
	database.arm(0, nil)
	if processed, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { ran++; return nil }); err != nil || !processed || ran != 1 {
		t.Fatalf("redelivery processed=%v err=%v ran=%d; want the job's one attempt to run at once", processed, err, ran)
	}
}

// TestCrashLoopingJobDoesNotChargeABystander: two workers in one process,
// as an application running several workers does. Worker A runs a poison job
// whose handler kills the process; worker B, on another Queue over the same
// store, tries to start an innocent job while A is between its start and its
// handler. The crash must not charge the innocent job: B must not have
// started an attempt that the poison job's crash then abandons. After the
// poison job dead-letters, the innocent job completes on its first attempt
// (#245 review).
func TestCrashLoopingJobDoesNotChargeABystander(t *testing.T) {
	if os.Getenv("NEWBLOK_WORKER_BYSTANDER_CHILD") == "1" {
		runBystanderChild()
		return
	}
	const maxAttempts, restarts = 3, 8
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "worker.db")
	entriesPath := filepath.Join(directory, "handler-entries")
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	for _, key := range []string{"poison", "innocent"} {
		if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: key, Kind: "crash", Payload: []byte(`{}`), MaxAttempts: maxAttempts}); err != nil {
			database.Close()
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	runs := func(key string) int {
		data, err := os.ReadFile(entriesPath)
		if errors.Is(err, os.ErrNotExist) {
			return 0
		}
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, line := range strings.Split(string(data), "\n") {
			if line == key {
				n++
			}
		}
		return n
	}
	inspect := func(key string) (string, int, string) {
		t.Helper()
		database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		queue, err := New(context.Background(), database, nil)
		if err != nil {
			t.Fatal(err)
		}
		job, err := queue.Get(context.Background(), key)
		if err != nil {
			t.Fatal(err)
		}
		return job.State, job.Attempt, job.Error
	}
	var evolution []string
	for restart := 1; restart <= restarts; restart++ {
		command := exec.Command(os.Args[0], "-test.run=^TestCrashLoopingJobDoesNotChargeABystander$")
		command.Env = append(os.Environ(), "NEWBLOK_WORKER_BYSTANDER_CHILD=1", "NEWBLOK_WORKER_PATH="+databasePath, "NEWBLOK_WORKER_ENTRIES="+entriesPath, fmt.Sprintf("NEWBLOK_WORKER_CLOCK_OFFSET=%dh", restart))
		output, runErr := command.CombinedOutput()
		exit := "exit 0"
		var exitErr *exec.ExitError
		if errors.As(runErr, &exitErr) {
			exit = exitErr.ProcessState.String()
		} else if runErr != nil {
			t.Fatalf("restart %d: %v", restart, runErr)
		}
		poisonState, poisonAttempt, poisonError := inspect("poison")
		innocentState, innocentAttempt, innocentError := inspect("innocent")
		evolution = append(evolution, fmt.Sprintf("restart %d: %s; poison %s/%d %q runs=%d; innocent %s/%d %q runs=%d", restart, exit, poisonState, poisonAttempt, poisonError, runs("poison"), innocentState, innocentAttempt, innocentError, runs("innocent")))
		if exit != "exit 0" && runs("poison") != restart {
			t.Fatalf("restart %d ended (%s) without the poison handler killing it: %s\n%s", restart, exit, output, strings.Join(evolution, "\n"))
		}
		if poisonState == StateDead && (innocentState == StateCompleted || innocentState == StateDead) {
			break
		}
	}
	t.Logf("evolution:\n%s", strings.Join(evolution, "\n"))
	poisonState, poisonAttempt, _ := inspect("poison")
	innocentState, innocentAttempt, _ := inspect("innocent")
	if poisonState != StateDead || poisonAttempt != maxAttempts || runs("poison") != maxAttempts {
		t.Fatalf("poison job %s at attempt %d after %d runs; want dead at %d after %d:\n%s", poisonState, poisonAttempt, runs("poison"), maxAttempts, maxAttempts, strings.Join(evolution, "\n"))
	}
	if innocentState != StateCompleted || innocentAttempt != 1 || runs("innocent") != 1 {
		t.Fatalf("innocent job %s at attempt %d after %d runs; want completed on its first attempt, never charged for the poison job's crashes:\n%s", innocentState, innocentAttempt, runs("innocent"), strings.Join(evolution, "\n"))
	}
}

// runBystanderChild runs worker A, which claims the oldest job, and, between
// A's start and A's handler, worker B on another Queue over the same store.
// The poison job's handler kills the process once B's handler transaction,
// if B started one, is queued behind it.
func runBystanderChild() {
	offset, err := time.ParseDuration(os.Getenv("NEWBLOK_WORKER_CLOCK_OFFSET"))
	if err != nil {
		panic(err)
	}
	database, err := (sqlite.Backend{}).Open(context.Background(), os.Getenv("NEWBLOK_WORKER_PATH"))
	if err != nil {
		panic(err)
	}
	clock := func() time.Time { return time.Now().Add(offset) }
	aDB, bDB := &handlerTxDatabase{Database: database}, &handlerTxDatabase{Database: database}
	a, err := New(context.Background(), aDB, clock)
	if err != nil {
		panic(err)
	}
	b, err := New(context.Background(), bDB, clock)
	if err != nil {
		panic(err)
	}
	entered := make(chan struct{})
	var enter sync.Once
	var entries sync.Mutex
	handler := func(ctx context.Context, _ Tx, job Job) error {
		entries.Lock()
		file, err := os.OpenFile(os.Getenv("NEWBLOK_WORKER_ENTRIES"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err == nil {
			_, err = file.WriteString(job.RequestKey + "\n")
			if err == nil {
				err = file.Sync()
			}
			_ = file.Close()
		}
		entries.Unlock()
		if err != nil {
			return err
		}
		if job.RequestKey != "poison" {
			return nil
		}
		enter.Do(func() { close(entered) })
		// Let a handler transaction B started queue behind this one.
		time.Sleep(50 * time.Millisecond)
		self, err := os.FindProcess(os.Getpid())
		if err != nil {
			return err
		}
		_ = self.Kill()
		select {}
	}
	bDB.arm(2, func(context.Context, store.Database) error {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
		}
		return nil
	})
	bDone := make(chan struct{})
	started := false
	aDB.arm(2, func(context.Context, store.Database) error {
		started = true
		go func() {
			defer close(bDone)
			_, _ = b.ProcessOnce(context.Background(), handler)
		}()
		waitProcessing(database, 2, time.Second)
		return nil
	})
	if _, err := a.ProcessOnce(context.Background(), handler); err != nil {
		panic(err)
	}
	if started {
		<-bDone
	}
	_ = database.Close()
	os.Exit(0)
}

// TestAttemptAccountingLeavesAnotherClaimAlone: the accounting that follows
// a rolled-back handler transaction (charging a lost claim, deferring a lost
// consumer, giving back an attempt whose handler never ran) is its own
// transaction, matched on the lease its attempt started with. If that lease
// expired and another worker has claimed the job since, the accounting must
// leave that worker's claim alone (#245 review).
func TestAttemptAccountingLeavesAnotherClaimAlone(t *testing.T) {
	for name, account := range map[string]func(*Queue, context.Context, Job, int64) error{
		"charge lost claim": (*Queue).chargeLostClaim,
		"defer lost":        (*Queue).deferLost,
		"release":           (*Queue).release,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "accounting.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			now := time.Unix(1_700_000_000, 0)
			queue, err := New(ctx, database, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "accounted", Kind: "test", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
				t.Fatal(err)
			}
			var job Job
			var lease int64
			if err := database.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
				var err error
				job, lease, err = queue.claim(ctx, tx)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			// The lease expired and another worker claimed the job: its own
			// lease and the next attempt.
			taken := lease + int64(time.Hour)
			if err := database.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET attempt = attempt + 1, lease_until = ? WHERE request_key = 'accounted'`, taken)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			read := func() (string, int, int, int64, string) {
				var state, message string
				var attempt, deferrals int
				var until sql.NullInt64
				if err := database.WithTx(ctx, func(tx *sql.Tx) error {
					return tx.QueryRowContext(ctx, `SELECT state, attempt, deferrals, lease_until, error_text FROM worker_jobs WHERE request_key = 'accounted'`).Scan(&state, &attempt, &deferrals, &until, &message)
				}); err != nil {
					t.Fatal(err)
				}
				return state, attempt, deferrals, until.Int64, message
			}
			if err := account(queue, ctx, job, lease); err != nil {
				t.Fatal(err)
			}
			if state, attempt, deferrals, until, message := read(); state != StateProcessing || attempt != 2 || deferrals != 0 || until != taken || message != "" {
				t.Fatalf("stale accounting changed the other worker's claim: %s attempt=%d deferrals=%d lease=%d error=%q", state, attempt, deferrals, until, message)
			}
			// The same accounting on the current lease does change the job, so
			// the check above is not vacuous.
			if err := database.WithTx(ctx, func(tx *sql.Tx) error {
				var err error
				job, err = scanJob(tx.QueryRowContext(ctx, `SELECT job_id, request_key, kind, payload_json, attempt, max_attempts, deferrals, principal_json, state, error_text FROM worker_jobs WHERE request_key = 'accounted'`))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := account(queue, ctx, job, taken); err != nil {
				t.Fatal(err)
			}
			if state, _, _, _, _ := read(); state == StateProcessing {
				t.Fatal("accounting on the current lease left the job processing")
			}
		})
	}
}

// TestAttemptTransactionsWriteFirst: another handle on the same file holds
// the write lock as the claim that starts an attempt begins, and again as
// the handler's transaction begins. Each must write first: a SQLite
// transaction that reads before its first write cannot wait for the lock
// and fails busy at once (#176, ADR 0003). Writing first, each waits the
// other writer out and the job completes (#245 review).
func TestAttemptTransactionsWriteFirst(t *testing.T) {
	for _, phase := range []struct {
		name string
		at   int
	}{{"start", 1}, {"handle", 2}} {
		t.Run(phase.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "write-first.db")
			sqliteDB, err := (sqlite.Backend{}).Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer sqliteDB.Close()
			other, err := (sqlite.Backend{}).Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			defer other.Close()
			database := &handlerTxDatabase{Database: sqliteDB}
			queue, err := New(ctx, database, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "write-first", Kind: "test", Payload: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			held := make(chan error, 1)
			database.arm(phase.at, func(context.Context, store.Database) error {
				holding := make(chan struct{})
				go func() {
					held <- other.WithTx(context.Background(), func(tx *sql.Tx) error {
						if _, err := tx.ExecContext(context.Background(), `UPDATE worker_jobs SET updated_at = updated_at`); err != nil {
							return err
						}
						close(holding)
						time.Sleep(200 * time.Millisecond)
						return nil
					})
				}()
				<-holding
				return nil
			})
			ran := 0
			processed, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { ran++; return nil })
			if err := <-held; err != nil {
				t.Fatal(err)
			}
			if err != nil || !processed || ran != 1 {
				t.Fatalf("processed=%v err=%v ran=%d; want the %s transaction to wait for the other writer and the job to complete", processed, err, ran, phase.name)
			}
			if state, attempt, _ := rawJob(t, sqliteDB, "write-first"); state != StateCompleted || attempt != 1 {
				t.Fatalf("job %s at attempt %d; want completed at 1", state, attempt)
			}
		})
	}
}

// TestLeaseIsConfigurableAndLongerThanTwoBusyTimeouts: WithLease sets the
// lease a started attempt takes, and New refuses a lease no longer than twice
// the store's busy timeout, including the default lease on a store whose busy
// timeout is long (#245 review).
func TestLeaseIsConfigurableAndLongerThanTwoBusyTimeouts(t *testing.T) {
	ctx := context.Background()
	open := func(busy time.Duration) store.Database {
		database, err := (sqlite.Backend{BusyTimeout: busy}).Open(ctx, filepath.Join(t.TempDir(), "lease.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = database.Close() })
		return database
	}
	if _, err := New(ctx, open(time.Second), nil, WithLease(2*time.Second)); err == nil {
		t.Fatal("New accepted a lease of exactly twice the busy timeout")
	}
	if _, err := New(ctx, open(20*time.Second), nil); err == nil {
		t.Fatal("New accepted the default 30 s lease on a store whose busy timeout is 20 s")
	}
	if _, err := New(ctx, open(5*time.Second), nil); err != nil {
		t.Fatalf("New refused the default lease on the default busy timeout: %v", err)
	}
	sqliteDB := open(time.Second)
	database := &handlerTxDatabase{Database: sqliteDB}
	now := time.Unix(1_700_000_000, 0)
	queue, err := New(ctx, database, func() time.Time { return now }, WithLease(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "leased", Kind: "test", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	var until int64
	database.arm(2, func(ctx context.Context, underlying store.Database) error {
		return underlying.WithTx(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT lease_until FROM worker_jobs WHERE request_key = 'leased'`).Scan(&until)
		})
	})
	if processed, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { return nil }); err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if want := now.Add(3 * time.Second).UnixNano(); until != want {
		t.Fatalf("the started attempt's lease ends at %d; want now + 3 s = %d", until, want)
	}
}

// TestPanickingHandlerReleasesTheClaimTurn: a handler panics, and the panic
// unwinds through ProcessOnce to a caller that recovers it, as a supervisor
// that keeps its worker alive does. The claim turn must be released on the
// way out, or every later ProcessOnce on the write domain in this process
// would wait out the busy timeout for a turn nobody holds (#245 review).
//
// The test checks the turn itself, apart from the store: before #267 the
// panic also left the handler's SQLite transaction open, so every later
// write to the store failed busy whatever the turn did. That the next job
// then runs, and how the panicked attempt is counted, is
// TestPanickingHandlerRollsBackAndFailsItsAttempt.
func TestPanickingHandlerReleasesTheClaimTurn(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{BusyTimeout: 300 * time.Millisecond}).Open(ctx, filepath.Join(t.TempDir(), "panic.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "panics", Kind: "test", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	ran := false
	recovered := func() (value any) {
		defer func() { value = recover() }()
		_, _ = queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { ran = true; panic("handler bug") })
		return nil
	}()
	if !ran || recovered != "handler bug" {
		t.Fatalf("ran=%v recovered=%v; want the handler to run and its panic to reach the caller", ran, recovered)
	}
	// Every Queue in the process on this write domain shares the turn; a
	// fresh one must get it at once.
	other, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	release, err := other.takeClaimTurn()
	if err != nil {
		t.Fatalf("after a handler panic the write domain's claim turn is still held: %v", err)
	}
	release()
}

// TestHandlerProcessingItsOwnQueueFailsAfterOneBusyWait: a handler calls
// ProcessOnce on the queue whose claim turn it holds. The inner call must
// fail with store.ErrBusy after the store's busy timeout instead of waiting
// for the turn forever, and the outer job, whose handler returns that
// saturation naming its own write domain, fails as a nested submission and
// is not retried (#207, #225, #245 review). A deadline keeps a regression
// from hanging the package.
func TestHandlerProcessingItsOwnQueueFailsAfterOneBusyWait(t *testing.T) {
	ctx := context.Background()
	const busy = 300 * time.Millisecond
	database, err := (sqlite.Backend{BusyTimeout: busy}).Open(ctx, filepath.Join(t.TempDir(), "own-queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"outer", "waiting"} {
		if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: key, Kind: "test", Payload: []byte(`{}`), MaxAttempts: 3}); err != nil {
			t.Fatal(err)
		}
	}
	var inner error
	var innerElapsed time.Duration
	innerProcessed := false
	done := make(chan error, 1)
	go func() {
		_, err := queue.ProcessOnce(ctx, func(ctx context.Context, _ Tx, _ Job) error {
			started := time.Now()
			innerProcessed, inner = queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error { return nil })
			innerElapsed = time.Since(started)
			return inner
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("outer ProcessOnce: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a handler that runs ProcessOnce on its own queue never returned; the claim-turn wait is not bounded by the busy timeout")
	}
	if innerProcessed || !errors.Is(inner, store.ErrBusy) {
		t.Fatalf("inner ProcessOnce processed=%v err=%v; want store.ErrBusy and nothing processed", innerProcessed, inner)
	}
	if innerElapsed < busy || innerElapsed > busy+2*time.Second {
		t.Fatalf("inner ProcessOnce failed after %v; want about the %v busy timeout", innerElapsed, busy)
	}
	job, err := queue.Get(ctx, "outer")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != StateDead || job.Attempt != 1 || job.Error != "nested submission to claimed store; use worker.Tx for atomic writes" {
		t.Fatalf("outer job=%+v; want dead at attempt 1 as a nested submission", job)
	}
	if state, attempt, deferrals := rawJob(t, database, "waiting"); state != StatePending || attempt != 0 || deferrals != 0 {
		t.Fatalf("the job the inner call would have claimed is %s at attempt %d with %d deferrals; want it untouched", state, attempt, deferrals)
	}
}

// runCrashLoopChild processes one job whose handler writes, records that it
// ran, and kills its own process before the claim can commit.
func runCrashLoopChild() {
	offset, err := time.ParseDuration(os.Getenv("NEWBLOK_WORKER_CLOCK_OFFSET"))
	if err != nil {
		panic(err)
	}
	database, err := (sqlite.Backend{}).Open(context.Background(), os.Getenv("NEWBLOK_WORKER_PATH"))
	if err != nil {
		panic(err)
	}
	queue, err := New(context.Background(), database, func() time.Time { return time.Now().Add(offset) })
	if err != nil {
		panic(err)
	}
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, job Job) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO worker_effects (request_key) VALUES (?)`, job.RequestKey); err != nil {
			return err
		}
		entries, err := os.OpenFile(os.Getenv("NEWBLOK_WORKER_ENTRIES"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		if _, err := entries.Write([]byte{'x'}); err != nil {
			return err
		}
		if err := entries.Sync(); err != nil {
			return err
		}
		_ = entries.Close()
		self, err := os.FindProcess(os.Getpid())
		if err != nil {
			return err
		}
		_ = self.Kill()
		select {}
	})
	if err != nil {
		panic(err)
	}
	_ = database.Close()
	if !processed {
		os.Exit(3)
	}
	os.Exit(0)
}

func waitForWorkerMarker(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker child did not reach handoff marker: %s", path)
}

// TestConsumerLossRedeliversWithoutConsumingAttempt cancels the consumer while
// the handler holds the claim. The loss is reported as ErrConsumerLost, the
// handler's write is discarded, no attempt is consumed, one deferral is
// charged with backoff, and the job completes on the next delivery with
// exactly one committed effect.
func TestConsumerLossRedeliversWithoutConsumingAttempt(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Unix(1_700_000_000, 0)
	queue, err := New(context.Background(), database, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE effects (request_key TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	insert := func(ctx context.Context, tx Tx, job Job) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO effects (request_key) VALUES (?)`, job.RequestKey)
		return err
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("lost-%d", i)
		if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: key, Kind: "test", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		consumer, cancel := context.WithCancel(context.Background())
		_, err := queue.ProcessOnce(consumer, func(ctx context.Context, tx Tx, job Job) error {
			if err := insert(ctx, tx, job); err != nil {
				return err
			}
			cancel()
			<-ctx.Done()
			return ctx.Err()
		})
		if !errors.Is(err, ErrConsumerLost) {
			t.Fatalf("iteration %d: err=%v, want ErrConsumerLost", i, err)
		}
		job, err := queue.Get(context.Background(), key)
		if err != nil || job.State != StatePending || job.Attempt != 0 || job.Deferrals != 1 {
			t.Fatalf("iteration %d: job=%+v err=%v, want pending, no attempt consumed, one deferral", i, job, err)
		}
		if processed, err := queue.ProcessOnce(context.Background(), insert); err != nil || processed {
			t.Fatalf("iteration %d: redelivered before its backoff: processed=%v err=%v", i, processed, err)
		}
		now = now.Add(deferralDelay(1))
		if processed, err := queue.ProcessOnce(context.Background(), insert); err != nil || !processed {
			t.Fatalf("iteration %d: redelivery processed=%v err=%v", i, processed, err)
		}
	}
	var count int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM effects`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != 20 {
		t.Fatalf("effects=%d, want exactly one committed effect per job", count)
	}
}

// TestDeferralsAreBounded drives a handler that is always saturated and a
// consumer that is always lost. Each job is redelivered with growing backoff
// and dead-lettered once its deferral budget is spent; neither path loops
// forever.
func TestDeferralsAreBounded(t *testing.T) {
	for _, tc := range []struct {
		name    string
		consume func(*Queue) error
	}{
		{"always saturated", func(q *Queue) error {
			_, err := q.ProcessOnce(context.Background(), func(context.Context, Tx, Job) error { return trigger.ErrSaturated })
			return err
		}},
		{"consumer always lost", func(q *Queue) error {
			consumer, cancel := context.WithCancel(context.Background())
			_, err := q.ProcessOnce(consumer, func(ctx context.Context, _ Tx, _ Job) error { cancel(); return ctx.Err() })
			if !errors.Is(err, ErrConsumerLost) {
				return fmt.Errorf("want ErrConsumerLost, got %v", err)
			}
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			now := time.Unix(1_700_000_000, 0)
			queue, err := New(context.Background(), database, func() time.Time { return now })
			if err != nil {
				t.Fatal(err)
			}
			if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: "bounded", Kind: "test", Payload: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			claims := 0
			for ; claims < 10*MaxDeferrals; claims++ {
				job, err := queue.Get(context.Background(), "bounded")
				if err != nil {
					t.Fatal(err)
				}
				if job.State == StateDead {
					break
				}
				if err := tc.consume(queue); err != nil {
					t.Fatal(err)
				}
				now = now.Add(maxDeferralDelay)
			}
			job, err := queue.Get(context.Background(), "bounded")
			if err != nil || job.State != StateDead || job.Error != DeferralExhausted || job.Attempt != 0 || claims != MaxDeferrals+1 {
				t.Fatalf("job=%+v claims=%d err=%v; want dead-lettered after %d claims", job, claims, err, MaxDeferrals+1)
			}
		})
	}
	if deferralDelay(1) != time.Second || deferralDelay(3) != 4*time.Second || deferralDelay(30) != maxDeferralDelay {
		t.Fatalf("backoff is not exponential and capped: %v %v %v", deferralDelay(1), deferralDelay(3), deferralDelay(30))
	}
}

// TestQueueMigratesPreDeferralSchema opens a queue created before the
// deferral counter existed; pending jobs keep their identity and still run.
func TestQueueMigratesPreDeferralSchema(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(context.Background(), `CREATE TABLE worker_jobs (
			job_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, payload_json BLOB NOT NULL,
			payload_digest TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL, state TEXT NOT NULL,
			available_at INTEGER NOT NULL, lease_until INTEGER, error_text TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`); err != nil {
			return err
		}
		_, err := tx.ExecContext(context.Background(), `INSERT INTO worker_jobs (job_id, request_key, kind, payload_json, payload_digest, max_attempts, state, available_at, created_at, updated_at)
			VALUES ('job:1:legacy', 'legacy', 'test', '{}', 'digest', 3, 'pending', 0, 0, 0)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	if _, err := New(context.Background(), database, nil); err != nil {
		t.Fatalf("second open after migration: %v", err)
	}
	processed, err := queue.ProcessOnce(context.Background(), func(context.Context, Tx, Job) error { return nil })
	if err != nil || !processed {
		t.Fatalf("legacy job processed=%v err=%v", processed, err)
	}
	job, err := queue.Get(context.Background(), "legacy")
	if err != nil || job.ID != "job:1:legacy" || job.State != StateCompleted || job.Deferrals != 0 {
		t.Fatalf("legacy job=%+v err=%v", job, err)
	}
}

// TestPrincipalIsPersistedAndPartOfRequestIdentity: the producer's principal
// reaches the handler after a restart of the queue, and reusing a request key
// under another principal conflicts instead of deduplicating.
func TestPrincipalIsPersistedAndPartOfRequestIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.db")
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	owner := trigger.Principal{ID: "provider:shop", Roles: []string{"orders"}}
	accepted, err := queue.Submit(context.Background(), trigger.Submission{Key: "evt-1", Kind: "test", Payload: []byte(`{}`), Principal: owner})
	if err != nil || !accepted {
		t.Fatalf("accepted=%v err=%v", accepted, err)
	}
	if accepted, err := queue.Submit(context.Background(), trigger.Submission{Key: "evt-1", Kind: "test", Payload: []byte(`{}`), Principal: owner}); err != nil || accepted {
		t.Fatalf("duplicate accepted=%v err=%v", accepted, err)
	}
	_, err = queue.Submit(context.Background(), trigger.Submission{Key: "evt-1", Kind: "test", Payload: []byte(`{}`), Principal: trigger.Principal{ID: "provider:other"}})
	if !errors.Is(err, trigger.ErrConflict) || !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("other principal err=%v, want a conflict", err)
	}
	reordered := trigger.Principal{ID: "multi", Roles: []string{"b", "a"}}
	if _, err := queue.Submit(context.Background(), trigger.Submission{Key: "evt-3", Kind: "test", Payload: []byte(`{}`), Principal: reordered}); err != nil {
		t.Fatal(err)
	}
	if accepted, err := queue.Submit(context.Background(), trigger.Submission{Key: "evt-3", Kind: "test", Payload: []byte(`{}`), Principal: trigger.Principal{ID: "multi", Roles: []string{"a", "b", "a"}}}); err != nil || accepted {
		t.Fatalf("same principal with reordered roles: accepted=%v err=%v, want a duplicate", accepted, err)
	}
	if _, err := queue.Submit(context.Background(), trigger.Submission{Key: "evt-2", Kind: "test", Payload: []byte(`{`)}); !errors.Is(err, trigger.ErrInvalidInput) || !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("invalid payload err=%v", err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err = New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Jobs tied on created_at are claimed in enqueue order (#217); this test
	// is about principals, so it reads them by key either way.
	seen := map[string]trigger.Principal{}
	for range 2 {
		if _, err := queue.ProcessOnce(context.Background(), func(_ context.Context, _ Tx, job Job) error { seen[job.RequestKey] = job.Principal; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if got := seen["evt-1"]; got.ID != owner.ID || len(got.Roles) != 1 || got.Roles[0] != "orders" {
		t.Fatalf("handler principal for evt-1=%+v, want %+v", got, owner)
	}
	if got := seen["evt-3"]; got.ID != "multi" || len(got.Roles) != 2 {
		t.Fatalf("handler principal for evt-3=%+v, want multi with two roles", got)
	}
}

// TestConcurrentWorkersNeverFailBusy: several workers and concurrent
// submitters on one store. Every job runs exactly once and no worker sees
// SQLITE_BUSY: a claim writes first, so a contending worker waits under the
// busy timeout instead of failing on a stale read (#176). The handlers are
// deliberately instant: handlers run one at a time under the write lock, so
// slow ones can still exhaust the busy timeout (ADR 0003).
func TestConcurrentWorkersNeverFailBusy(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "busy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("busy", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	// Submitters keep at most 16 submissions in flight, as real clients do:
	// an unbounded burst queues on the single writer past the busy timeout,
	// which is saturation (#184), not what this test is about.
	const jobs, workers, inFlight = 200, 4, 16
	var submitters sync.WaitGroup
	submitErrs := make(chan error, jobs)
	slots := make(chan struct{}, inFlight)
	for i := range jobs {
		submitters.Add(1)
		go func() {
			defer submitters.Done()
			slots <- struct{}{}
			defer func() { <-slots }()
			if _, err := queue.Submit(ctx, trigger.Submission{Key: fmt.Sprintf("busy-%d", i), Kind: "busy", Payload: []byte(`{}`)}); err != nil {
				submitErrs <- err
			}
		}()
	}
	var mu sync.Mutex
	runs := map[string]int{}
	var failures []error
	var group sync.WaitGroup
	deadline := time.Now().Add(30 * time.Second)
	for range workers {
		group.Add(1)
		go func() {
			defer group.Done()
			for time.Now().Before(deadline) {
				mu.Lock()
				done := len(runs) == jobs
				mu.Unlock()
				if done {
					return
				}
				if _, err := queue.ProcessOnce(ctx, func(_ context.Context, _ Tx, job Job) error {
					mu.Lock()
					runs[job.RequestKey]++
					mu.Unlock()
					return nil
				}); err != nil {
					mu.Lock()
					failures = append(failures, err)
					mu.Unlock()
				}
			}
		}()
	}
	submitters.Wait()
	close(submitErrs)
	group.Wait()
	for err := range submitErrs {
		t.Errorf("submit: %v", err)
	}
	if len(failures) != 0 {
		t.Fatalf("%d ProcessOnce calls failed; first: %v", len(failures), failures[0])
	}
	if len(runs) != jobs {
		t.Fatalf("%d of %d jobs ran", len(runs), jobs)
	}
	for key, n := range runs {
		if n != 1 {
			t.Fatalf("job %s ran %d times", key, n)
		}
	}
}

// TestClaimPredicateAndOrder seeds jobs in every claimable and unclaimable
// shape: the claim takes pending jobs that are due and processing jobs whose
// lease expired, oldest first with ties broken by job id, and nothing else.
func TestClaimPredicateAndOrder(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "predicate.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	now := time.Unix(1_000, 0)
	queue, err := New(ctx, database, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("shape", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	at := func(offset time.Duration) int64 { return now.Add(offset).UnixNano() }
	shapes := []struct {
		key, id, state string
		available      int64
		lease          any
		created        int64
	}{
		{"future", "job:f", StatePending, at(time.Hour), nil, 1},
		{"live-lease", "job:l", StateProcessing, at(-time.Minute), at(10 * time.Second), 2},
		{"expired-lease", "job:0", StateProcessing, at(-time.Minute), at(-time.Second), 10},
		{"lease-ends-now", "job:9", StateProcessing, at(-time.Minute), at(0), 11},
		// Tied on created_at: the one enqueued first is claimed first, even
		// though its job_id sorts after the other's (#217).
		{"tie-enqueued-first", "job:b", StatePending, at(0), nil, 5},
		{"tie-enqueued-second", "job:a", StatePending, at(-time.Second), nil, 5},
		{"completed", "job:c", StateCompleted, at(-time.Minute), nil, 0},
		{"dead", "job:d", StateDead, at(-time.Minute), nil, 0},
	}
	for _, s := range shapes {
		if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: s.key, Kind: "shape", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		if err := database.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET job_id = ?, state = ?, available_at = ?, lease_until = ?, created_at = ? WHERE request_key = ?`, s.id, s.state, s.available, s.lease, s.created, s.key)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	var claimed []string
	for {
		processed, err := queue.ProcessOnce(ctx, func(_ context.Context, _ Tx, job Job) error {
			claimed = append(claimed, job.RequestKey)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	if want := []string{"tie-enqueued-first", "tie-enqueued-second", "expired-lease", "lease-ends-now"}; fmt.Sprint(claimed) != fmt.Sprint(want) {
		t.Fatalf("claimed %v, want %v", claimed, want)
	}
}

// TestCanceledClaimWaitReportsConsumerLost: a worker waiting behind another
// worker's handler, whose consumer is canceled meanwhile, reports
// ErrConsumerLost (not SQLITE_BUSY) within the busy timeout, and leaves the
// job untouched. Since #245 it waits for the write domain's claim turn,
// which the other worker holds until its attempt's outcome commits; like
// the write turn before it, that wait is not canceled with the consumer, so
// it runs out the busy timeout before it can report.
func TestCanceledClaimWaitReportsConsumerLost(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "cancel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("slow", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"held", "waiting"} {
		if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: key, Kind: "slow", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	// The two jobs may tie on created_at (Windows' clock steps in
	// milliseconds), so whichever the first worker claims, the other is the
	// one the canceled worker must leave untouched.
	holding, release := make(chan struct{}), make(chan struct{})
	first := make(chan error, 1)
	var held string
	go func() {
		_, err := queue.ProcessOnce(ctx, func(_ context.Context, _ Tx, job Job) error {
			held = job.RequestKey
			close(holding)
			<-release
			return nil
		})
		first <- err
	}()
	<-holding
	consumer, cancel := context.WithCancel(ctx)
	time.AfterFunc(100*time.Millisecond, cancel)
	begin := time.Now()
	_, err = queue.ProcessOnce(consumer, func(context.Context, Tx, Job) error { return nil })
	elapsed := time.Since(begin)
	close(release)
	if !errors.Is(err, ErrConsumerLost) || elapsed > 7*time.Second {
		t.Fatalf("a canceled worker waited %v for the lock and returned %v", elapsed, err)
	}
	// The report keeps what the wait itself ended with.
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("the consumer-lost report %v dropped the busy wait it replaced", err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	untouched := "waiting"
	if held == "waiting" {
		untouched = "held"
	}
	if job, err := queue.Get(ctx, untouched); err != nil || job.State != StatePending || job.Attempt != 0 {
		t.Fatalf("the canceled claim left %+v %v", job, err)
	}
}

// TestMeasureClaimContention reproduces ADR 0003's numbers: 4 workers drain
// 200 instant jobs, 5 samples. It runs only when NEWBLOK_MEASURE_CLAIM=1:
//
//	NEWBLOK_MEASURE_CLAIM=1 go test -run TestMeasureClaimContention -v ./trigger/worker/
func TestMeasureClaimContention(t *testing.T) {
	if os.Getenv("NEWBLOK_MEASURE_CLAIM") != "1" {
		t.Skip("set NEWBLOK_MEASURE_CLAIM=1 to measure claim contention")
	}
	for sample := range 5 {
		ctx := context.Background()
		database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), fmt.Sprintf("measure-%d.db", sample)))
		if err != nil {
			t.Fatal(err)
		}
		queue, err := New(ctx, database, time.Now)
		if err != nil {
			t.Fatal(err)
		}
		if err := queue.RegisterKind("m", []byte(`{"type":"object"}`)); err != nil {
			t.Fatal(err)
		}
		for i := range 200 {
			if _, err := queue.Submit(ctx, trigger.Submission{Key: fmt.Sprintf("m-%d", i), Kind: "m", Payload: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
		}
		var mu sync.Mutex
		runs, busy := 0, 0
		begin := time.Now()
		var group sync.WaitGroup
		for range 4 {
			group.Add(1)
			go func() {
				defer group.Done()
				for {
					mu.Lock()
					done := runs >= 200 || time.Since(begin) > 30*time.Second
					mu.Unlock()
					if done {
						return
					}
					_, err := queue.ProcessOnce(ctx, func(context.Context, Tx, Job) error {
						mu.Lock()
						runs++
						mu.Unlock()
						return nil
					})
					if err != nil {
						mu.Lock()
						busy++
						mu.Unlock()
					}
				}
			}()
		}
		group.Wait()
		t.Logf("sample=%d jobs=%d busy_errors=%d elapsed=%s", sample, runs, busy, time.Since(begin).Round(time.Millisecond))
		_ = database.Close()
	}
}

// TestBusyStoreSubmissionIsSaturation: a submission that cannot get the
// store's write lock within the busy timeout is saturation, retryable, and
// commits nothing.
func TestBusyStoreSubmissionIsSaturation(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "saturated.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("busy", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		held <- database.WithTx(ctx, func(tx *sql.Tx) error {
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
	_, err = queue.Submit(ctx, trigger.Submission{Key: "busy-1", Kind: "busy", Payload: []byte(`{}`)})
	released.Do(func() { close(release) })
	if !errors.Is(err, trigger.ErrSaturated) || !errors.Is(err, store.ErrBusy) {
		t.Fatalf("a submission to a busy store returned %v; want saturation", err)
	}
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Get(ctx, "busy-1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a saturated submission was committed: %v", err)
	}
	if accepted, err := queue.Submit(ctx, trigger.Submission{Key: "busy-1", Kind: "busy", Payload: []byte(`{}`)}); err != nil || !accepted {
		t.Fatalf("the retry: accepted=%v err=%v", accepted, err)
	}
}

// TestHandlerSubmittingToItsOwnStoreFails: a handler that submits to another
// Queue sharing its store is diagnosed before it waits on the claim's write
// lock, then fails with the actionable worker diagnostic.
func TestHandlerSubmittingToItsOwnStoreFails(t *testing.T) {
	expected := nestedStoreExpected(t, "same-store")
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "self.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("self", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	// A second Queue over the same Database must share the claim's write
	// domain; queue identity alone is not enough to catch the deadlock.
	otherQueue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := otherQueue.RegisterKind("self", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE nested_effects (id TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "outer", Kind: "self", Payload: []byte(`{}`), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	var nested error
	started := time.Now()
	processed, err := queue.ProcessOnce(ctx, func(ctx context.Context, tx Tx, _ Job) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO nested_effects VALUES ('rolled-back')`); err != nil {
			return err
		}
		_, nested = otherQueue.Submit(ctx, trigger.Submission{Key: "inner", Kind: "self", Payload: []byte(`{}`)})
		return nested
	})
	elapsed := time.Since(started)
	if expected.ProcessError != "none" || err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if expected.NestedError != "worker.ErrNestedSubmission" || !errors.Is(nested, ErrNestedSubmission) {
		t.Fatalf("the nested submission returned %v; fixture wants %s", nested, expected.NestedError)
	}
	if elapsed >= time.Second {
		t.Fatalf("same-store submission took %v; want immediate diagnosis", elapsed)
	}
	job, err := queue.Get(ctx, "outer")
	if err != nil {
		t.Fatal(err)
	}
	if job.Deferrals != expected.Deferrals || job.Attempt != expected.Attempt || job.State != expected.State || job.Error != expected.JobError {
		t.Fatalf("the self-submitting job is %+v; fixture wants state=%s attempt=%d deferrals=%d error=%q", job, expected.State, expected.Attempt, expected.Deferrals, expected.JobError)
	}
	if _, err := otherQueue.Get(ctx, "inner"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the rejected nested submission was persisted: %v", err)
	}
	var effects int
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nested_effects`).Scan(&effects)
	}); err != nil {
		t.Fatal(err)
	}
	if submissions := nestedSubmissionRows(t, database, "inner"); submissions != expected.NestedSubmissionRows || effects != expected.HandlerEffectRows {
		t.Fatalf("nested submissions=%d handler effects=%d; fixture wants %d and %d", submissions, effects, expected.NestedSubmissionRows, expected.HandlerEffectRows)
	}
}

// TestHandlerSubmittingToAnotherBusyStoreDefers: an actual write lock on a
// distinct SQLite database is saturation. It must backpressure the outer job
// instead of charging an attempt, even though the handler already owns a
// different database's claim transaction.
func TestHandlerSubmittingToAnotherBusyStoreDefers(t *testing.T) {
	expected := nestedStoreExpected(t, "other-store-busy")
	ctx := context.Background()
	open := func(name string) store.Database {
		database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), name))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = database.Close() })
		return database
	}
	outerDB, otherDB := open("outer.db"), open("other.db")
	clock := time.Unix(100, 0)
	outer, err := New(ctx, outerDB, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(ctx, otherDB, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.RegisterKind("outer", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := other.RegisterKind("nested", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := outerDB.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE nested_effects (id TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
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
	var nested error
	processed, err := outer.ProcessOnce(ctx, func(ctx context.Context, tx Tx, _ Job) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO nested_effects VALUES ('must-roll-back')`); err != nil {
			return err
		}
		_, nested = other.Submit(ctx, trigger.Submission{Key: "inner", Kind: "nested", Payload: []byte(`{}`)})
		return nested
	})
	released.Do(func() { close(release) })
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if expected.ProcessError != "none" || err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if expected.NestedError != "store.ErrBusy" || !errors.Is(nested, trigger.ErrSaturated) || !errors.Is(nested, store.ErrBusy) {
		t.Fatalf("other-store submission returned %v; fixture wants %s and saturation", nested, expected.NestedError)
	}
	job, err := outer.Get(ctx, "outer")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != expected.State || job.Attempt != expected.Attempt || job.Deferrals != expected.Deferrals || job.Error != expected.JobError {
		t.Fatalf("outer job=%+v; fixture wants state=%s attempt=%d deferrals=%d error=%q", job, expected.State, expected.Attempt, expected.Deferrals, expected.JobError)
	}
	if _, err := other.Get(ctx, "inner"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the saturated nested submission was persisted: %v", err)
	}
	var effects int
	if err := outerDB.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nested_effects`).Scan(&effects)
	}); err != nil {
		t.Fatal(err)
	}
	if submissions := nestedSubmissionRows(t, otherDB, "inner"); submissions != expected.NestedSubmissionRows || effects != expected.HandlerEffectRows {
		t.Fatalf("nested submissions=%d handler effects=%d; fixture wants %d and %d", submissions, effects, expected.NestedSubmissionRows, expected.HandlerEffectRows)
	}
}

func TestHandlerSubmittingAcrossSharedMemoryHandlesFails(t *testing.T) {
	expected := nestedStoreExpected(t, "same-store")
	ctx := context.Background()
	first, err := (sqlite.Backend{}).Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := (sqlite.Backend{}).Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	outer, err := New(ctx, first, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	other, err := New(ctx, second, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := outer.RegisterKind("shared-memory", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := other.RegisterKind("shared-memory", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := first.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE nested_memory_effects (id TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := outer.Enqueue(ctx, EnqueueRequest{RequestKey: "outer-memory", Kind: "shared-memory", Payload: []byte(`{}`), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	var nested error
	started := time.Now()
	// Bounded: an undiagnosed shared-memory self-submit once blocked forever
	// (#207).
	processed, err := processBounded(outer, func(ctx context.Context, tx Tx, _ Job) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO nested_memory_effects VALUES ('rolled-back')`); err != nil {
			return err
		}
		_, nested = other.Submit(ctx, trigger.Submission{Key: "inner-memory", Kind: "shared-memory", Payload: []byte(`{}`)})
		return nested
	})
	if expected.ProcessError != "none" || err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if !errors.Is(nested, ErrNestedSubmission) || time.Since(started) >= time.Second {
		t.Fatalf("shared-memory nested submission returned %v after %v; want immediate worker.ErrNestedSubmission", nested, time.Since(started))
	}
	job, err := outer.Get(ctx, "outer-memory")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != expected.State || job.Attempt != expected.Attempt || job.Deferrals != expected.Deferrals || job.Error != expected.JobError {
		t.Fatalf("outer job=%+v; fixture wants state=%s attempt=%d deferrals=%d error=%q", job, expected.State, expected.Attempt, expected.Deferrals, expected.JobError)
	}
	if _, err := other.Get(ctx, "inner-memory"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the rejected nested submission was persisted: %v", err)
	}
	var effects int
	if err := second.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM nested_memory_effects`).Scan(&effects)
	}); err != nil {
		t.Fatal(err)
	}
	if effects != expected.HandlerEffectRows {
		t.Fatalf("shared-memory handler effects=%d; fixture wants %d", effects, expected.HandlerEffectRows)
	}
	if submissions := nestedSubmissionRows(t, second, "inner-memory"); submissions != expected.NestedSubmissionRows {
		t.Fatalf("shared-memory nested submissions=%d; fixture wants %d", submissions, expected.NestedSubmissionRows)
	}
}

// rollbackNextDatabase forces a rollback of the transaction after the next
// one: ProcessOnce's handler transaction, after the claim that starts the
// attempt has committed (#245).
type rollbackNextDatabase struct {
	store.Database
	rollbackNext bool
	skipped      bool
}

func (database *rollbackNextDatabase) WriteDomain() *store.WriteDomain {
	domain, _ := store.WriteDomainOf(database.Database)
	return domain
}

func (database *rollbackNextDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	rollback := database.rollbackNext && database.skipped
	if database.rollbackNext && !database.skipped {
		database.skipped = true
	} else {
		database.rollbackNext = false
	}
	return database.Database.WithTx(ctx, func(tx *sql.Tx) error {
		if err := fn(tx); err != nil {
			return err
		}
		if rollback {
			return errors.New("worker test: force claim rollback")
		}
		return nil
	})
}

func TestClaimDiagnosticExpiresAfterTransaction(t *testing.T) {
	for _, rollback := range []bool{false, true} {
		name := "commit"
		if rollback {
			name = "rollback"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "worker.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			var queueDB store.Database = database
			var rollbackDB *rollbackNextDatabase
			if rollback {
				rollbackDB = &rollbackNextDatabase{Database: database}
				queueDB = rollbackDB
			}
			queue, err := New(ctx, queueDB, time.Now)
			if err != nil {
				t.Fatal(err)
			}
			if err := queue.RegisterKind("lifecycle", []byte(`{"type":"object"}`)); err != nil {
				t.Fatal(err)
			}
			if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "claimed", Kind: "lifecycle", Payload: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			if rollback {
				rollbackDB.rollbackNext = true
			}
			var retained context.Context
			processed, processErr := queue.ProcessOnce(ctx, func(handlerCtx context.Context, _ Tx, _ Job) error {
				retained = handlerCtx
				return nil
			})
			if rollback {
				if processErr == nil || processed {
					t.Fatalf("ProcessOnce did not report the forced rollback: processed=%v err=%v", processed, processErr)
				}
			} else if processErr != nil || !processed {
				t.Fatalf("ProcessOnce failed: processed=%v err=%v", processed, processErr)
			}
			claimedJob, err := queue.Get(ctx, "claimed")
			if err != nil {
				t.Fatal(err)
			}
			// A handler that ran under a transaction that then rolled back
			// has spent the attempt it started; the job is retried (#245).
			wantState, wantAttempt, wantError := StateCompleted, 1, ""
			if rollback {
				wantState, wantError = StatePending, "claim transaction ended"
			}
			if claimedJob.State != wantState || claimedJob.Attempt != wantAttempt || claimedJob.Error != wantError {
				t.Fatalf("claim after %s: state=%s attempt=%d error=%q; want state=%s attempt=%d error=%q", name, claimedJob.State, claimedJob.Attempt, claimedJob.Error, wantState, wantAttempt, wantError)
			}
			result, err := queue.Enqueue(retained, EnqueueRequest{RequestKey: "after-claim", Kind: "lifecycle", Payload: []byte(`{}`)})
			if err != nil || !result.Accepted {
				t.Fatalf("same-store enqueue with retained post-transaction context: accepted=%v err=%v; want accepted after %s", result.Accepted, err, name)
			}
		})
	}
}

// TestHandlerWritesNeverEscapeTheClaim: a consumer is canceled while its
// handler's statement runs, and the handler, carelessly, ignores that
// statement's error and writes again. SQLite rolls the whole transaction
// back when a statement in it is interrupted, so if the cancellation
// reached the statement, the second write would commit on its own, outside
// the claim, and the redelivery would write it again (#180). Through Tx the
// statement is never interrupted: the lost claim rolls back whole, and each
// job's rows commit exactly once, on the attempt that completes.
func TestHandlerWritesNeverEscapeTheClaim(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "escape.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("escape", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE business (request_key TEXT NOT NULL)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	const jobs = 3
	for i := range jobs {
		if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: fmt.Sprintf("e-%d", i), Kind: "escape", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	// slowInsert counts a million rows before it inserts one, so a
	// cancellation 20 ms in lands while it runs.
	const slowInsert = `INSERT INTO business (request_key) SELECT ? WHERE (WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM c WHERE x < 1000000) SELECT count(*) FROM c) > 0`
	attempts := map[string]int{}
	lost := 0
	for round := 0; round < 4*jobs; round++ {
		consumer, cancel := context.WithCancel(ctx)
		processed, err := queue.ProcessOnce(consumer, func(consumer context.Context, tx Tx, job Job) error {
			attempts[job.RequestKey]++
			if attempts[job.RequestKey] == 1 {
				time.AfterFunc(20*time.Millisecond, cancel)
			}
			_, _ = tx.ExecContext(consumer, slowInsert, job.RequestKey)
			_, err := tx.ExecContext(context.Background(), `INSERT INTO business (request_key) VALUES (?)`, job.RequestKey+"#2")
			return err
		})
		cancel()
		if errors.Is(err, ErrConsumerLost) {
			lost++
			if err := database.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET available_at = 0 WHERE state = ?`, StatePending)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	if lost != jobs {
		t.Fatalf("%d first attempts were lost; want %d (one per job)", lost, jobs)
	}
	var rows, distinct, completed int
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*), COUNT(DISTINCT request_key) FROM business`).Scan(&rows, &distinct); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_jobs WHERE state = ?`, StateCompleted).Scan(&completed)
	}); err != nil {
		t.Fatal(err)
	}
	if completed != jobs || rows != 2*jobs || distinct != 2*jobs {
		t.Fatalf("completed=%d business rows=%d distinct=%d; want %d jobs and %d rows, each once", completed, rows, distinct, jobs, 2*jobs)
	}
}

// TestLostClaimRefusesWritesAndCountsTheAttempt: a handler statement fails
// because the store is full, and SQLite rolls the whole transaction back.
// The handler, carelessly, ignores that and writes again: the write is
// refused with ErrClaimLost, nothing is committed, and the lost claim is
// counted as a failed attempt instead of being redelivered at once, forever
// (#180).
func TestLostClaimRefusesWritesAndCountsTheAttempt(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "full.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("full", []byte(`{"type":"object"}`)); err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE business (request_key TEXT NOT NULL, data BLOB)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "full-1", Kind: "full", Payload: []byte(`{}`), MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	var full, refused error
	processed, err := queue.ProcessOnce(ctx, func(ctx context.Context, tx Tx, job Job) error {
		var pages int64
		if err := tx.QueryRowContext(ctx, `PRAGMA page_count`).Scan(&pages); err != nil {
			return err
		}
		// The store may not grow: the next large write fails as full.
		if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA max_page_count = %d`, pages)); err != nil {
			return err
		}
		_, full = tx.ExecContext(ctx, `INSERT INTO business VALUES (?, zeroblob(1048576))`, job.RequestKey)
		_, refused = tx.ExecContext(ctx, `INSERT INTO business VALUES (?, NULL)`, job.RequestKey+"#2")
		return nil
	})
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if full == nil {
		t.Fatal("the large write did not fail as full")
	}
	if !errors.Is(refused, ErrClaimLost) {
		t.Fatalf("a write after the claim's transaction ended returned %v; want ErrClaimLost", refused)
	}
	var rows int
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM business`).Scan(&rows)
	}); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Fatalf("%d business rows committed outside the lost claim", rows)
	}
	job, err := queue.Get(ctx, "full-1")
	if err != nil {
		t.Fatal(err)
	}
	if job.State != StateDead || job.Attempt != 1 {
		t.Fatalf("the lost claim left the job %s at attempt %d; want dead at attempt 1", job.State, job.Attempt)
	}
}

// claimQueue is a queue with one job and a business table, for the
// claim-transaction tests.
func claimQueue(t *testing.T, maxAttempts int) (*Queue, store.Database) {
	t.Helper()
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "claim.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	queue, err := New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE business (request_key TEXT NOT NULL UNIQUE ON CONFLICT ROLLBACK, data BLOB)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: "claim-1", Kind: "claim", Payload: []byte(`{}`), MaxAttempts: maxAttempts}); err != nil {
		t.Fatal(err)
	}
	return queue, database
}

func businessRows(t *testing.T, database store.Database) int {
	t.Helper()
	rows := 0
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM business`).Scan(&rows)
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

// TestConflictRollbackEndsTheClaim: a constraint declared ON CONFLICT
// ROLLBACK ends the whole transaction when it fires, whichever way the
// handler runs the statement. The handler's next write is refused, nothing
// commits, and the attempt is counted (#180).
func TestConflictRollbackEndsTheClaim(t *testing.T) {
	const duplicate = `INSERT INTO business (request_key) VALUES ('a') RETURNING request_key`
	for _, test := range []struct {
		name     string
		conflict func(context.Context, Tx) error
	}{
		{"exec", func(ctx context.Context, tx Tx) error {
			_, err := tx.ExecContext(ctx, duplicate)
			return err
		}},
		{"query row", func(ctx context.Context, tx Tx) error {
			var key string
			return tx.QueryRowContext(ctx, duplicate).Scan(&key)
		}},
		{"query row error", func(ctx context.Context, tx Tx) error {
			return tx.QueryRowContext(ctx, duplicate).Err()
		}},
		{"query", func(ctx context.Context, tx Tx) error {
			rows, err := tx.QueryContext(ctx, duplicate)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
			}
			return rows.Err()
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			queue, database := claimQueue(t, 1)
			var conflict, refused error
			processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, _ Job) error {
				if _, err := tx.ExecContext(ctx, `INSERT INTO business (request_key) VALUES ('a')`); err != nil {
					return err
				}
				conflict = test.conflict(ctx, tx)
				_, refused = tx.ExecContext(ctx, `INSERT INTO business (request_key) VALUES ('b')`)
				return nil
			})
			if err != nil || !processed || conflict == nil || !errors.Is(refused, ErrClaimLost) {
				t.Fatalf("processed=%v err=%v conflict=%v refused=%v; want the conflict to end the claim and the next write refused", processed, err, conflict, refused)
			}
			if rows := businessRows(t, database); rows != 0 {
				t.Fatalf("%d business rows committed outside the lost claim", rows)
			}
			if job, err := queue.Get(context.Background(), "claim-1"); err != nil || job.State != StateDead || job.Attempt != 1 {
				t.Fatalf("job %+v %v; want dead at attempt 1", job, err)
			}
		})
	}
}

// TestConcurrentHandlerWritesNeverEscapeTheClaim: a handler writes from two
// goroutines while one statement ends the claim's transaction. Every
// statement is checked before the next starts, so none runs on the ended
// transaction and nothing commits (#180).
func TestConcurrentHandlerWritesNeverEscapeTheClaim(t *testing.T) {
	for trial := range 30 {
		queue, database := claimQueue(t, 1)
		_, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, _ Job) error {
			if _, err := tx.ExecContext(ctx, `INSERT INTO business (request_key) VALUES ('seed')`); err != nil {
				return err
			}
			done := make(chan struct{})
			go func() {
				defer close(done)
				for i := 0; ; i++ {
					if _, err := tx.ExecContext(ctx, `INSERT INTO business (request_key) VALUES (?)`, fmt.Sprint("w-", i)); errors.Is(err, ErrClaimLost) {
						return
					}
				}
			}()
			time.Sleep(time.Millisecond)
			_, _ = tx.ExecContext(ctx, `INSERT INTO business (request_key) VALUES ('seed')`)
			<-done
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if rows := businessRows(t, database); rows != 0 {
			t.Fatalf("trial %d: %d business rows committed outside the lost claim", trial, rows)
		}
	}
}

// TestHandlerCannotControlTheClaimTransaction: the claim owns its
// transaction. A handler statement that would begin, end or nest it is
// refused before it runs, and the claim stays intact (#180).
func TestHandlerCannotControlTheClaimTransaction(t *testing.T) {
	queue, database := claimQueue(t, 1)
	statements := []string{"COMMIT", " rollback", "END TRANSACTION", "BEGIN", "SAVEPOINT s", "release s", "/* a */ COMMIT", "-- a\nROLLBACK", ";COMMIT", ";; END", "/**/;ROLLBACK", " ; -- a\n;release s"}
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, _ Job) error {
		for _, statement := range statements {
			if _, err := tx.ExecContext(ctx, statement); !errors.Is(err, ErrTransactionControl) {
				return fmt.Errorf("exec %q: %v, want ErrTransactionControl", statement, err)
			}
			if _, err := tx.QueryContext(ctx, statement); !errors.Is(err, ErrTransactionControl) {
				return fmt.Errorf("query %q: %v, want ErrTransactionControl", statement, err)
			}
			if err := tx.QueryRowContext(ctx, statement).Scan(); !errors.Is(err, ErrTransactionControl) {
				return fmt.Errorf("query row %q: %v, want ErrTransactionControl", statement, err)
			}
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO business (request_key) VALUES ('kept')`)
		return err
	})
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if job, err := queue.Get(context.Background(), "claim-1"); err != nil || job.State != StateCompleted {
		t.Fatalf("job %+v %v; want completed", job, err)
	}
	if rows := businessRows(t, database); rows != 1 {
		t.Fatalf("%d business rows; want the handler's one", rows)
	}
}

// TestJobsEnqueuedInOneClockStepAreClaimedInOrder: Windows' clock advances in
// steps of about 2 ms, so jobs enqueued back to back often share a
// created_at. A frozen clock makes every job tie; they must still be claimed
// in the order they were enqueued, not in job_id (hash) order (#217).
func TestJobsEnqueuedInOneClockStepAreClaimedInOrder(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "fifo.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tick := time.Unix(2_000, 0)
	queue, err := New(ctx, database, func() time.Time { return tick })
	if err != nil {
		t.Fatal(err)
	}
	const jobs = 50
	var want []string
	for i := range jobs {
		key := fmt.Sprintf("order-%02d", i)
		want = append(want, key)
		if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: key, Kind: "fifo", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	var claimed []string
	for {
		processed, err := queue.ProcessOnce(ctx, func(_ context.Context, _ Tx, job Job) error {
			claimed = append(claimed, job.RequestKey)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	if fmt.Sprint(claimed) != fmt.Sprint(want) {
		t.Fatalf("claimed %v; want enqueue order %v", claimed, want)
	}
}

// TestEnqueueOrderSurvivesBackup: a queue restored from a backup (VACUUM
// INTO) claims tied jobs in enqueue order (#217). The order is stored in
// enqueue_seq; this test does not distinguish that from rowid, whose
// renumbering by VACUUM is a documented SQLite caveat, not observed here.
func TestEnqueueOrderSurvivesBackup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(dir, "live.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	tick := time.Unix(3_000, 0)
	queue, err := New(ctx, database, func() time.Time { return tick })
	if err != nil {
		t.Fatal(err)
	}
	var want []string
	for i := range 20 {
		key := fmt.Sprintf("backup-%02d", i)
		want = append(want, key)
		if _, err := queue.Enqueue(ctx, EnqueueRequest{RequestKey: key, Kind: "fifo", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	backup := filepath.Join(dir, "backup.db")
	if err := database.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	restored, err := (sqlite.Backend{}).Open(ctx, backup)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	again, err := New(ctx, restored, func() time.Time { return tick })
	if err != nil {
		t.Fatal(err)
	}
	var claimed []string
	for {
		processed, err := again.ProcessOnce(ctx, func(_ context.Context, _ Tx, job Job) error {
			claimed = append(claimed, job.RequestKey)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	if fmt.Sprint(claimed) != fmt.Sprint(want) {
		t.Fatalf("restored queue claimed %v; want enqueue order %v", claimed, want)
	}
}

// TestConcurrentOpensMigrateWithoutFailing: several processes opening a queue
// that still needs a column added must all start. The migration reads the
// schema before it writes, so SQLite cannot make a second opener wait for the
// first; it fails busy at once (#233). Six handles on one file open together,
// round after round, each round on a queue missing every added column.
func TestConcurrentOpensMigrateWithoutFailing(t *testing.T) {
	ctx := context.Background()
	const handles, rounds = 6, 10
	failures := 0
	var first error
	for round := range rounds {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("migrate-%d.db", round))
		seed, err := (sqlite.Backend{}).Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if err := seed.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE worker_jobs (
				job_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, payload_json BLOB NOT NULL,
				payload_digest TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0, max_attempts INTEGER NOT NULL, state TEXT NOT NULL,
				available_at INTEGER NOT NULL, lease_until INTEGER, error_text TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL)`)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		_ = seed.Close()
		databases := make([]store.Database, handles)
		for i := range databases {
			if databases[i], err = (sqlite.Backend{}).Open(ctx, path); err != nil {
				t.Fatal(err)
			}
		}
		start := make(chan struct{})
		errs := make(chan error, handles)
		var group sync.WaitGroup
		for _, database := range databases {
			group.Add(1)
			go func() {
				defer group.Done()
				<-start
				_, err := New(ctx, database, nil)
				errs <- err
			}()
		}
		close(start)
		group.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				failures++
				if first == nil {
					first = err
				}
			}
		}
		for _, database := range databases {
			_ = database.Close()
		}
	}
	if failures != 0 {
		t.Fatalf("%d of %d concurrent opens failed; first: %v", failures, handles*rounds, first)
	}
}
