package worker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
)

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
		processed, err := queue.ProcessOnce(context.Background(), func(_ context.Context, _ *sql.Tx, _ Job) error {
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
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx *sql.Tx, _ Job) error {
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
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx *sql.Tx, job Job) error {
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
	if _, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx *sql.Tx, job Job) error {
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
	insert := func(ctx context.Context, tx *sql.Tx, job Job) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO effects (request_key) VALUES (?)`, job.RequestKey)
		return err
	}
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("lost-%d", i)
		if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: key, Kind: "test", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		consumer, cancel := context.WithCancel(context.Background())
		_, err := queue.ProcessOnce(consumer, func(ctx context.Context, tx *sql.Tx, job Job) error {
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
			_, err := q.ProcessOnce(context.Background(), func(context.Context, *sql.Tx, Job) error { return trigger.ErrSaturated })
			return err
		}},
		{"consumer always lost", func(q *Queue) error {
			consumer, cancel := context.WithCancel(context.Background())
			_, err := q.ProcessOnce(consumer, func(ctx context.Context, _ *sql.Tx, _ Job) error { cancel(); return ctx.Err() })
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
	processed, err := queue.ProcessOnce(context.Background(), func(context.Context, *sql.Tx, Job) error { return nil })
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
	var seen trigger.Principal
	if _, err := queue.ProcessOnce(context.Background(), func(_ context.Context, _ *sql.Tx, job Job) error { seen = job.Principal; return nil }); err != nil {
		t.Fatal(err)
	}
	if seen.ID != owner.ID || len(seen.Roles) != 1 || seen.Roles[0] != "orders" {
		t.Fatalf("handler principal=%+v, want %+v", seen, owner)
	}
}
