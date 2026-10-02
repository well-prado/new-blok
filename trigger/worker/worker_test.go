package worker

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
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
