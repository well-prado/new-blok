package worker

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/promrule"
	"github.com/well-prado/new-blok/observe/slo"
	"github.com/well-prado/new-blok/store/sqlite"
)

var monitoringFixtures = filepath.Join("..", "..", "examples", "monitoring", "testdata")

func censusText(t *testing.T, queue *Queue) (slo.Work, string) {
	t.Helper()
	work, err := queue.Census(context.Background(), "orders")
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := slo.WriteText(&b, slo.Snapshot{Work: []slo.Work{work}}); err != nil {
		t.Fatal(err)
	}
	return work, b.String()
}

func checkScenario(t *testing.T, id, before, after string) {
	t.Helper()
	scenarios, err := promrule.LoadScenarios(filepath.Join(monitoringFixtures, "scenarios.json"))
	if err != nil {
		t.Fatal(err)
	}
	scenario, ok := scenarios.Find(id)
	if !ok {
		t.Fatalf("scenario %s is not declared", id)
	}
	parse := func(text string) []promrule.ExpositionSample {
		samples, err := promrule.ParseText(strings.NewReader(text))
		if err != nil {
			t.Fatal(err)
		}
		return samples
	}
	if err := scenario.Check(parse(before), parse(after)); err != nil {
		t.Fatalf("predeclared signals not observed:\n%v\n%s", err, after)
	}
	if os.Getenv("BLOK_RECORD_FIXTURES") == "1" {
		path := filepath.Join(monitoringFixtures, "recorded", id+".prom")
		if err := promrule.WriteRecording(path, promrule.Recording{Scenario: id, Source: "trigger/worker Queue.Census over SQLite, slo.WriteText", Before: before, After: after}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestWorkerDeathScenarioSignals kills a real worker process while it holds a
// job (SIGKILL, so it never acknowledges or releases its lease), then takes a
// census after the lease has expired with no other worker running: the
// killed worker's job is stalled and the job behind it is pending with a
// growing backlog age. A second queue whose jobs wait out a retry backoff is
// the negative case: waiting, never stalled, however long no worker runs.
func TestWorkerDeathScenarioSignals(t *testing.T) {
	if os.Getenv("NEWBLOK_CENSUS_CHILD") == "1" {
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
	defer database.Close()
	var offset atomic.Int64
	clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	// The queue is opened before the worker process starts: the worker holds
	// the write lock while its handler runs, and the census only reads.
	queue, err := New(context.Background(), database, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE worker_effects (id INTEGER PRIMARY KEY, request_key TEXT NOT NULL)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"order-1", "order-2"} {
		if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: key, Kind: "order.create", Payload: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond) // distinct created_at: order-1 is claimed first
	}
	work, before := censusText(t, queue)
	if work.Pending != 2 || work.Active+work.Stalled+work.Waiting != 0 {
		t.Fatalf("before: %+v", work)
	}
	command := exec.Command(os.Args[0], "-test.run=^TestWorkerDeathScenarioSignals$")
	command.Env = append(os.Environ(), "NEWBLOK_CENSUS_CHILD=1", "NEWBLOK_WORKER_PATH="+databasePath, "NEWBLOK_WORKER_MARKER="+markerPath)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForWorkerMarker(t, markerPath)
	// While the worker lives and holds its lease the job is active.
	if live, _ := censusText(t, queue); live.Active != 1 || live.Pending != 1 || live.Stalled != 0 {
		t.Fatalf("live worker: %+v", live)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	// Seven minutes later nothing has consumed the queue: the dead worker's
	// lease (30s) has expired and the next job has waited all that time.
	offset.Store(int64(7 * time.Minute))
	work, after := censusText(t, queue)
	if work.Stalled != 1 || work.Pending != 1 || work.OldestPending < 7*time.Minute {
		t.Fatalf("after death: %+v", work)
	}
	checkScenario(t, "worker-death", before, after)

	// A live worker reclaims the stalled job: the stall is gone.
	if processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, job Job) error { return nil }); err != nil || !processed {
		t.Fatalf("recovery processed=%v err=%v", processed, err)
	}
	if recovered, _ := censusText(t, queue); recovered.Stalled != 0 || recovered.Pending != 1 {
		t.Fatalf("after recovery: %+v", recovered)
	}

	// Negative case: retry backoff is waiting, with no worker at all.
	waitingDB, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(directory, "waiting.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer waitingDB.Close()
	waiting, err := New(context.Background(), waitingDB, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"retry-1", "retry-2", "retry-3"} {
		if _, err := waiting.Enqueue(context.Background(), EnqueueRequest{RequestKey: key, Kind: "order.create", Payload: []byte(`{}`), MaxAttempts: 5}); err != nil {
			t.Fatal(err)
		}
	}
	_, before = censusText(t, waiting)
	for i := 0; i < 3; i++ {
		if processed, err := waiting.ProcessOnce(context.Background(), func(context.Context, Tx, Job) error {
			return &HandlerError{Retryable: true, Message: "provider busy"}
		}); err != nil || !processed {
			t.Fatalf("retryable attempt %d processed=%v err=%v", i, processed, err)
		}
	}
	work, after = censusText(t, waiting)
	if work.Waiting != 3 || work.Stalled != 0 || work.Pending != 0 {
		t.Fatalf("backoff: %+v", work)
	}
	checkScenario(t, "suspended-not-stalled", before, after)
}
