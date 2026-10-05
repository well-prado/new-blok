package worker

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/tooling/promrule"
	"github.com/well-prado/new-blok/observe/slo"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

var monitoringFixtures = filepath.Join("..", "..", "examples", "monitoring", "testdata")

func censusText(t *testing.T, queue *Queue) (slo.Work, string) {
	t.Helper()
	return censusTextWithin(t, queue, 0)
}

func censusTextWithin(t *testing.T, queue *Queue, budget time.Duration) (slo.Work, string) {
	t.Helper()
	work, err := queue.Census(context.Background(), "orders", budget)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := slo.WriteText(&b, slo.Snapshot{Work: []slo.Work{work}, Sources: []slo.SourceStatus{{Name: "orders", Up: true, Pages: true}}}); err != nil {
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
	// The same census through a Sampler, as /metrics or observe/otel run it,
	// with an informational storage source beside it. The census reads
	// through a queue the test can swap for one over a slow store.
	var current atomic.Pointer[Queue]
	current.Store(queue)
	census := slo.Func("orders", func(ctx context.Context) (slo.Snapshot, error) {
		work, err := current.Load().Census(ctx, "orders", 0)
		return slo.Snapshot{Work: []slo.Work{work}}, err
	})
	storage := slo.FileStorage("journal", 0, databasePath)
	sampler, err := slo.NewSamplerWithClock(clock, time.Second, census, storage)
	if err != nil {
		t.Fatal(err)
	}
	sampledBefore := sampledText(t, sampler)
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
	// Three minutes into a slow handler in another process: the committed
	// lease expired long ago (the worker extends it only inside its
	// uncommitted transaction, #245). Within a declared five-minute handler
	// budget it is active; with the default budget (the lease) it would be
	// reported stalled, which is why the budget must cover the longest
	// handler.
	offset.Store(int64(3 * time.Minute))
	if slow, _ := censusTextWithin(t, queue, 5*time.Minute); slow.Active != 1 || slow.Stalled != 0 {
		t.Fatalf("slow handler within its budget: %+v", slow)
	}
	if slow, _ := censusTextWithin(t, queue, 0); slow.Stalled != 1 {
		t.Fatalf("slow handler past the default budget: %+v", slow)
	}
	offset.Store(0)
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
	// The dead worker is stalled under a five-minute budget too: its lease
	// expired six and a half minutes ago.
	if dead, _ := censusTextWithin(t, queue, 5*time.Minute); dead.Stalled != 1 {
		t.Fatalf("dead worker with a five-minute budget: %+v", dead)
	}
	checkScenario(t, "worker-death", before, after)

	// The same stall while the store slows to 3s per transaction: the census
	// times out at its 1s budget, so the stall is not visible; the census is
	// reported down with its age growing since its last answer.
	slowQueue, err := New(context.Background(), slowDatabase{Database: database, delay: 3 * time.Second}, clock)
	if err != nil {
		t.Fatal(err)
	}
	current.Store(slowQueue)
	timedOut := sampledText(t, sampler)
	if strings.Contains(timedOut, `blok_liveness="stalled"`) || !strings.Contains(timedOut, `blok_source_up{blok_source="orders",blok_pages="true"} 0`) {
		t.Fatalf("a timed-out census must be reported down, not vanish or report zero:\n%s", timedOut)
	}
	checkScenario(t, "census-timeout-during-stall", sampledBefore, timedOut)
	// The process restarts without its census configured: its series vanish.
	restarted, err := slo.NewSamplerWithClock(clock, time.Second, storage)
	if err != nil {
		t.Fatal(err)
	}
	checkScenario(t, "census-vanished", sampledBefore, sampledText(t, restarted))
	current.Store(queue)

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

// TestLiveHandlerInThisProcessIsNeverStalled: a handler this process is
// running is active however far past its committed lease the census reads,
// even with the default budget; once it has finished, an expired lease left
// by it would be stalled again.
func TestLiveHandlerInThisProcessIsNeverStalled(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	var offset atomic.Int64
	queue, err := New(context.Background(), database, func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }, WithLease(11*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: "slow-1", Kind: "order.create", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx Tx, job Job) error {
			close(entered)
			<-release
			return nil
		})
		done <- err
	}()
	<-entered
	offset.Store(int64(3 * time.Minute))
	work, err := queue.Census(context.Background(), "orders", 0)
	if err != nil {
		t.Fatal(err)
	}
	if work.Active != 1 || work.Stalled != 0 {
		t.Fatalf("a live slow handler: %+v", work)
	}
	close(release)
	if err := <-done; err != nil {
		t.Logf("slow handler outcome after its lease: %v", err)
	}
	offset.Store(0)
}

// TestCensusReadsThroughThePartialIndex: SQLite's plan for the census uses
// the covering partial index, so completed history is never scanned.
func TestCensusReadsThroughThePartialIndex(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if _, err := New(context.Background(), database, nil); err != nil {
		t.Fatal(err)
	}
	var plan strings.Builder
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(context.Background(), `EXPLAIN QUERY PLAN `+censusQuery, 1, 1, 1, 1, 1)
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
			plan.WriteString(detail + "\n")
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "worker_jobs_unfinished") || strings.Contains(plan.String(), "SCAN worker_jobs\n") {
		t.Fatalf("census plan does not use the partial index:\n%s", plan.String())
	}
}

// TestCensusCostAtOneMillionRows (BLOK_QUEUE_CENSUS_SCALE=1) loads a million
// completed jobs plus a live queue, then measures the census and whether a
// log purge (#281) still succeeds while the census runs back to back. It
// reports the same with the index dropped, the cost before this change.
func TestCensusCostAtOneMillionRows(t *testing.T) {
	if os.Getenv("BLOK_QUEUE_CENSUS_SCALE") != "1" {
		t.Skip("set BLOK_QUEUE_CENSUS_SCALE=1 to measure the census over a million rows")
	}
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(store.Writer(context.Background()), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 1000000)
			INSERT INTO worker_jobs (job_id, request_key, kind, payload_json, payload_digest, max_attempts, state, available_at, created_at, updated_at, enqueue_seq)
			SELECT 'done-' || i, 'done-' || i, 'order.create', '{}', 'd', 3, CASE WHEN i % 1000 = 0 THEN 'pending' ELSE 'completed' END, 0, i, i, i FROM n`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	purger, ok := database.(store.Purger)
	if !ok {
		t.Fatal("the SQLite store does not purge its log")
	}
	measure := func(label string) {
		samples := make([]time.Duration, 20)
		for i := range samples {
			start := time.Now()
			work, err := queue.Census(context.Background(), "orders", 0)
			samples[i] = time.Since(start)
			if err != nil || work.Pending < 1000 {
				t.Fatalf("%s census %+v %v", label, work, err)
			}
		}
		slices.Sort(samples)
		stop := make(chan struct{})
		var running sync.WaitGroup
		running.Add(1)
		go func() {
			defer running.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = queue.Census(context.Background(), "orders", 0)
				}
			}
		}()
		busy := 0
		for i := 0; i < 20; i++ {
			// A write between purges leaves frames in the log that a census
			// snapshot taken before it still needs.
			if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: fmt.Sprintf("%s-%d", label, i), Kind: "order.create", Payload: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			if err := purger.PurgeLog(context.Background()); errors.Is(err, store.ErrBusy) {
				busy++
			} else if err != nil {
				t.Fatal(err)
			}
		}
		close(stop)
		running.Wait()
		t.Logf("%s: census p50=%v max=%v over 1,000,000 rows; PurgeLog busy %d of 20 while the census runs back to back", label, samples[10], samples[19], busy)
		if label == "with index" && (samples[10] > 20*time.Millisecond || busy > 2) {
			t.Fatalf("with the index the census must stay cheap and not starve PurgeLog: p50 %v, busy %d/20", samples[10], busy)
		}
	}
	measure("with index")
	if err := database.WithTx(store.Writer(context.Background()), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `DROP INDEX worker_jobs_unfinished`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	measure("without index")
}

// slowDatabase delays every transaction: the slow-store fault.
type slowDatabase struct {
	store.Database
	delay time.Duration
}

func (d slowDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	select {
	case <-time.After(d.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	return d.Database.WithTx(ctx, fn)
}

func sampledText(t *testing.T, sampler *slo.Sampler) string {
	t.Helper()
	var b bytes.Buffer
	if err := slo.WriteText(&b, sampler.Sample(context.Background())); err != nil {
		t.Fatal(err)
	}
	return b.String()
}
