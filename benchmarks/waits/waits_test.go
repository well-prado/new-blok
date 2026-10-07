// Package waitsbench measures single-host durable waits (#332, ADR 0027):
// how much a suspended run costs while it waits (goroutines and resident
// memory), and how a burst of wakeups drains through the resumer.
package waitsbench

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/internal/resumer"
	"github.com/well-prado/new-blok/store/sqlite"
)

const artifact = "sha256:2222222222222222222222222222222222222222222222222222222222222332"

// program waits for "approval" and outputs its outcome: no node runs, so
// the samples measure suspension and resumption, not node work.
var program = contract.InternalProgram{WorkflowID: "approval", Digest: artifact, Instructions: []contract.InternalInstruction{
	{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
	{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
}}

// Sample is one repetition: a fresh journal, runs admitted, started and
// suspended, then every run signalled at once and resumed to completion.
type Sample struct {
	StartedAtUTC string
	Runs         int
	// Suspension: admitting every run, then starting each until all are
	// suspended at their wait.
	AdmitNanoseconds   int64
	SuspendNanoseconds int64
	// Process footprint once the runs are admitted (before any executes),
	// and once all are suspended; after runtime.GC. RSS is `ps -o rss=`,
	// an instantaneous resident snapshot of the whole test process.
	GoroutinesBefore    int
	GoroutinesSuspended int
	RSSBeforeKiB        int64
	RSSSuspendedKiB     int64
	HeapInuseBeforeB    uint64
	HeapInuseSuspendedB uint64
	// Burst: every run signalled back to back while the resumer sweeps;
	// latency is from a run's signal commit to its completion settling.
	SignalNanoseconds                              int64
	BurstNanoseconds                               int64
	ThroughputRunsPerSec                           float64
	LatencyMicroseconds                            []int64
	LatencyP50, LatencyP90, LatencyP99, LatencyMax int64
	GoroutinesAfterBurst                           int
}

func measure(t *testing.T, dir string, runs, workers, batch int) Sample {
	t.Helper()
	ctx := context.Background()
	sample := Sample{StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), Runs: runs}
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(dir, "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	j, err := journal.New(ctx, database, journal.Config{Holder: "bench"})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	signalled := make(map[string]time.Time, runs)
	completed := make(map[string]time.Time, runs)
	suspended, done := make(chan string, runs), make(chan string, runs)
	r, err := resumer.New(resumer.Config{
		Journal: j,
		Engine:  engine.New(nil),
		Workflows: map[string]resumer.Workflow{"approval": {Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
			var input map[string]any
			err := json.Unmarshal(raw, &input)
			return input, err
		}}},
		Interval: 20 * time.Millisecond,
		Batch:    batch,
		Workers:  workers,
		Settled: func(runID string, outcome resumer.Outcome, err error) {
			switch outcome {
			case resumer.Suspended:
				suspended <- runID
			case resumer.Completed:
				mu.Lock()
				completed[runID] = time.Now()
				mu.Unlock()
				done <- runID
			default:
				t.Errorf("run %s: %s %v", runID, outcome, err)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, runs)
	begin := time.Now()
	for i := range ids {
		admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: fmt.Sprintf("run-%d", i), Workflow: "approval", ArtifactDigest: artifact, Input: []byte(fmt.Sprintf(`{"n":%d}`, i))})
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = admitted.RunID
	}
	sample.AdmitNanoseconds = time.Since(begin).Nanoseconds()
	sample.GoroutinesBefore, sample.RSSBeforeKiB, sample.HeapInuseBeforeB = footprint(t)

	begin = time.Now()
	for _, id := range ids {
		if err := r.Start(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	collect(t, suspended, runs, "suspended")
	sample.SuspendNanoseconds = time.Since(begin).Nanoseconds()
	sample.GoroutinesSuspended, sample.RSSSuspendedKiB, sample.HeapInuseSuspendedB = footprint(t)

	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	go r.Run(runCtx)
	begin = time.Now()
	for _, id := range ids {
		if _, err := j.Signal(ctx, signal.Envelope{RunID: id, SignalID: "s-" + id, Name: "approval", Principal: "bench", Payload: []byte(`{"approved":true}`)}, true); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		signalled[id] = time.Now()
		mu.Unlock()
		r.Wake()
	}
	sample.SignalNanoseconds = time.Since(begin).Nanoseconds()
	collect(t, done, runs, "completed")
	stop()
	if err := r.Close(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	last := begin
	for _, id := range ids {
		finished := completed[id]
		if finished.After(last) {
			last = finished
		}
		sample.LatencyMicroseconds = append(sample.LatencyMicroseconds, finished.Sub(signalled[id]).Microseconds())
	}
	mu.Unlock()
	sample.BurstNanoseconds = last.Sub(begin).Nanoseconds()
	sample.ThroughputRunsPerSec = float64(runs) / last.Sub(begin).Seconds()
	sorted := slices.Clone(sample.LatencyMicroseconds)
	slices.Sort(sorted)
	at := func(q float64) int64 { return sorted[min(len(sorted)-1, int(q*float64(len(sorted))))] }
	sample.LatencyP50, sample.LatencyP90, sample.LatencyP99, sample.LatencyMax = at(0.50), at(0.90), at(0.99), sorted[len(sorted)-1]
	time.Sleep(50 * time.Millisecond)
	sample.GoroutinesAfterBurst = runtime.NumGoroutine()
	var count int
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM journal_runs WHERE state = 'completed'`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	if count != runs {
		t.Fatalf("%d of %d runs completed", count, runs)
	}
	return sample
}

func collect(t *testing.T, settled chan string, want int, what string) {
	t.Helper()
	timeout := time.After(10 * time.Minute)
	for n := 0; n < want; n++ {
		select {
		case <-settled:
		case <-timeout:
			t.Fatalf("%d of %d runs %s", n, want, what)
		}
	}
}

// footprint is the goroutine count, resident KiB and heap in use, after a
// collection, once finished executions have had a moment to exit.
func footprint(t *testing.T) (int, int64, uint64) {
	t.Helper()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	out, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	rss, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return runtime.NumGoroutine(), rss, stats.HeapInuse
}

// TestSuspendedRunsCostStorageNotGoroutines is the always-run check: 1,000
// runs suspended through the resumer leave the goroutine count where it
// was, and a burst wakes and completes every one.
func TestSuspendedRunsCostStorageNotGoroutines(t *testing.T) {
	sample := measure(t, t.TempDir(), 1000, 8, 500)
	if growth := sample.GoroutinesSuspended - sample.GoroutinesBefore; growth > 10 {
		t.Fatalf("goroutines %d before, %d with %d runs suspended", sample.GoroutinesBefore, sample.GoroutinesSuspended, sample.Runs)
	}
	if growth := sample.GoroutinesAfterBurst - sample.GoroutinesBefore; growth > 10 {
		t.Fatalf("goroutines %d before, %d after the burst", sample.GoroutinesBefore, sample.GoroutinesAfterBurst)
	}
	t.Logf("1000 runs: goroutines %d→%d, RSS %d→%d KiB, burst p50 %dµs p99 %dµs, %.0f runs/s", sample.GoroutinesBefore, sample.GoroutinesSuspended, sample.RSSBeforeKiB, sample.RSSSuspendedKiB, sample.LatencyP50, sample.LatencyP99, sample.ThroughputRunsPerSec)
}

// Report is the committed evidence file.
type Report struct {
	SchemaVersion  int
	Workload       string
	Guarantees     string
	SourceRevision string
	Topology       string
	Go, OS, Arch   string
	Kernel         string
	CPUsVisible    int
	GoMaxProcs     int
	Workers, Batch int
	StartedAtUTC   string
	FinishedAtUTC  string
	Samples        []Sample
}

// TestWaitFootprintAndBurstSamples writes the measured evidence: by
// default three repetitions of 10,000 runs, each in a fresh journal.
func TestWaitFootprintAndBurstSamples(t *testing.T) {
	path := os.Getenv("BLOK_WAITS_REPORT")
	if path == "" {
		t.Skip("explicit measured evidence gate: BLOK_WAITS_REPORT")
	}
	runs, repetitions := envInt(t, "BLOK_WAITS_RUNS", 10000), envInt(t, "BLOK_WAITS_REPETITIONS", 3)
	workers, batch := envInt(t, "BLOK_WAITS_WORKERS", 8), envInt(t, "BLOK_WAITS_BATCH", 500)
	kernel, _ := exec.Command("uname", "-srv").Output()
	report := Report{
		SchemaVersion:  1,
		Workload:       "issue332-wait-v1: admit, start to suspension at one wait, signal all back to back, resume to completion; no node runs",
		Guarantees:     "real SQLite file journal (WAL, synchronous FULL), real engine and resumer, one process; footprint is of the whole test process; not a capacity, fleet or durability-under-load claim",
		SourceRevision: os.Getenv("BLOK_BENCH_SOURCE_REVISION"),
		Topology:       os.Getenv("BLOK_BENCH_TOPOLOGY"),
		Go:             runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH,
		Kernel:       strings.TrimSpace(string(kernel)),
		CPUsVisible:  runtime.NumCPU(),
		GoMaxProcs:   runtime.GOMAXPROCS(0),
		Workers:      workers,
		Batch:        batch,
		StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano),
	}
	for i := range repetitions {
		sample := measure(t, t.TempDir(), runs, workers, batch)
		t.Logf("repetition %d: goroutines %d→%d, RSS %d→%d KiB, suspend %v, burst %v (%.0f runs/s), p50 %dµs p90 %dµs p99 %dµs max %dµs", i+1, sample.GoroutinesBefore, sample.GoroutinesSuspended, sample.RSSBeforeKiB, sample.RSSSuspendedKiB, time.Duration(sample.SuspendNanoseconds), time.Duration(sample.BurstNanoseconds), sample.ThroughputRunsPerSec, sample.LatencyP50, sample.LatencyP90, sample.LatencyP99, sample.LatencyMax)
		report.Samples = append(report.Samples, sample)
	}
	report.FinishedAtUTC = time.Now().UTC().Format(time.RFC3339Nano)
	raw, err := json.MarshalIndent(report, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(raw, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q: want a positive integer", name, value)
	}
	return n
}
