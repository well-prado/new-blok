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
	queue, err = New(context.Background(), database, nil)
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
	job, err := queue.Get(context.Background(), "crash-order")
	if err != nil || job.State != StateCompleted || job.Attempt != 1 {
		t.Fatalf("recovered job=%+v err=%v", job, err)
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
	// Jobs enqueued within one clock step tie on created_at and are claimed
	// in job_id order, not submission order; Windows' clock steps in
	// milliseconds, so the test must not assume which comes first.
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
		{"tie-second", "job:b", StatePending, at(0), nil, 5},
		{"tie-first", "job:a", StatePending, at(-time.Second), nil, 5},
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
	if want := []string{"tie-first", "tie-second", "expired-lease", "lease-ends-now"}; fmt.Sprint(claimed) != fmt.Sprint(want) {
		t.Fatalf("claimed %v, want %v", claimed, want)
	}
}

// TestCanceledClaimWaitReportsConsumerLost: a worker waiting for the write
// lock held by another worker's handler, whose consumer is canceled
// meanwhile, reports ErrConsumerLost (not SQLITE_BUSY) within the busy
// timeout, and leaves the job untouched. Its transaction is not canceled
// with the consumer, so the wait for its write turn runs out the busy
// timeout before it can report.
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

type rollbackNextDatabase struct {
	store.Database
	rollbackNext bool
}

func (database *rollbackNextDatabase) WriteDomain() *store.WriteDomain {
	domain, _ := store.WriteDomainOf(database.Database)
	return domain
}

func (database *rollbackNextDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	rollback := database.rollbackNext
	database.rollbackNext = false
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
			wantState, wantAttempt := StateCompleted, 1
			if rollback {
				wantState, wantAttempt = StatePending, 0
			}
			if claimedJob.State != wantState || claimedJob.Attempt != wantAttempt {
				t.Fatalf("claim after %s: state=%s attempt=%d; want state=%s attempt=%d", name, claimedJob.State, claimedJob.Attempt, wantState, wantAttempt)
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
