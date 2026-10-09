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
	"regexp"
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

// Modes of a sample's wakeup phase.
const (
	// Burst: every run is signalled back to back while Run sweeps, with
	// Wake after each signal.
	Burst = "burst"
	// Backlog: every run is signalled with Run stopped and no Wake, as
	// signals from another process or timers due together arrive; then
	// Run starts and drains them.
	Backlog = "backlog"
)

// Sample is one repetition: a fresh journal, runs admitted, started and
// suspended, then every run signalled and resumed to completion.
type Sample struct {
	StartedAtUTC string
	Mode         string
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
	// The same, with all runs suspended while Run sweeps (several sweeps
	// in, before any signal): a resumer that kept anything per suspended
	// run while sweeping would show it here.
	GoroutinesSweeping int
	RSSSweepingKiB     int64
	HeapInuseSweepingB uint64
	// Wakeup: every run signalled (see Mode); latency is from a run's
	// signal commit to its completion settling. BurstNanoseconds runs
	// from the first signal (Burst) or from Run's start (Backlog) to the
	// last completion; ThroughputRunsPerSec is Runs over it.
	SignalNanoseconds                              int64
	BurstNanoseconds                               int64
	ThroughputRunsPerSec                           float64
	LatencyMicroseconds                            []int64
	LatencyP50, LatencyP90, LatencyP99, LatencyMax int64
	GoroutinesAfterBurst                           int
}

func measure(t *testing.T, dir, mode string, runs, workers, batch int) Sample {
	t.Helper()
	ctx := context.Background()
	sample := Sample{StartedAtUTC: time.Now().UTC().Format(time.RFC3339Nano), Mode: mode, Runs: runs}
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
	unexpected := make(chan error, 1)
	r, err := resumer.New(resumer.Config{
		Journal: j,
		Engine:  engine.New(nil),
		Workflows: map[string]resumer.Workflow{"approval": {Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
			var input map[string]any
			err := json.Unmarshal(raw, &input)
			return input, err
		}}},
		Interval: interval,
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
				select {
				case unexpected <- fmt.Errorf("run %s: %s %v", runID, outcome, err):
				default:
				}
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
	collect(t, suspended, unexpected, runs, "suspended")
	sample.SuspendNanoseconds = time.Since(begin).Nanoseconds()
	sample.GoroutinesSuspended, sample.RSSSuspendedKiB, sample.HeapInuseSuspendedB = footprint(t)

	run := func() (stop func()) {
		runCtx, cancel := context.WithCancel(ctx)
		returned := make(chan struct{})
		go func() { r.Run(runCtx); close(returned) }()
		return func() { cancel(); <-returned }
	}
	stop := run()
	defer func() { stop() }()
	time.Sleep(sweepsBeforeSample * interval) // Run sweeps at once, then every interval
	// A sweep in flight holds the database's transient goroutines (a
	// context watcher per statement); the fewest of a few samples is the
	// count that persists, and a goroutine per suspended run would be in
	// every one.
	sample.GoroutinesSweeping = -1
	for range 5 {
		goroutines, rss, heap := footprint(t)
		if sample.GoroutinesSweeping < 0 || goroutines < sample.GoroutinesSweeping {
			sample.GoroutinesSweeping, sample.RSSSweepingKiB, sample.HeapInuseSweepingB = goroutines, rss, heap
		}
	}
	if mode == Backlog {
		stop()
	}
	begin = time.Now()
	for _, id := range ids {
		if _, err := j.Signal(ctx, signal.Envelope{RunID: id, SignalID: "s-" + id, Name: "approval", Principal: "bench", Payload: []byte(`{"approved":true}`)}, true); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		signalled[id] = time.Now()
		mu.Unlock()
		if mode == Burst {
			r.Wake()
		}
	}
	sample.SignalNanoseconds = time.Since(begin).Nanoseconds()
	if mode == Backlog {
		begin = time.Now()
		stop = run()
	}
	collect(t, done, unexpected, runs, "completed")
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

// interval is the resumer's sweep interval in every sample; the
// sweeping footprint is taken sweepsBeforeSample intervals after Run
// starts.
const (
	interval           = 20 * time.Millisecond
	sweepsBeforeSample = 5
)

// collect waits for want runs to settle as what, and fails at once on an
// outcome the workload never expects.
func collect(t *testing.T, settled chan string, unexpected chan error, want int, what string) {
	t.Helper()
	timeout := time.After(10 * time.Minute)
	for n := 0; n < want; n++ {
		select {
		case <-settled:
		case err := <-unexpected:
			t.Fatalf("after %d of %d runs %s: %v", n, want, what, err)
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
// was, while they wait and while Run sweeps over them, and both wakeup
// modes (a burst with Wake, a backlog without) complete every one.
func TestSuspendedRunsCostStorageNotGoroutines(t *testing.T) {
	for _, mode := range []string{Burst, Backlog} {
		sample := measure(t, t.TempDir(), mode, 1000, 8, 500)
		if growth := sample.GoroutinesSuspended - sample.GoroutinesBefore; growth > 10 {
			t.Fatalf("%s: goroutines %d before, %d with %d runs suspended", mode, sample.GoroutinesBefore, sample.GoroutinesSuspended, sample.Runs)
		}
		if growth := sample.GoroutinesSweeping - sample.GoroutinesBefore; growth > 10 {
			t.Fatalf("%s: goroutines %d before, %d with %d runs suspended while Run sweeps", mode, sample.GoroutinesBefore, sample.GoroutinesSweeping, sample.Runs)
		}
		if growth := sample.GoroutinesAfterBurst - sample.GoroutinesBefore; growth > 10 {
			t.Fatalf("%s: goroutines %d before, %d after the wakeups", mode, sample.GoroutinesBefore, sample.GoroutinesAfterBurst)
		}
		t.Logf("%s, 1000 runs: goroutines %d→%d→%d (sweeping), RSS %d→%d KiB, p50 %dµs p99 %dµs, %.0f runs/s", mode, sample.GoroutinesBefore, sample.GoroutinesSuspended, sample.GoroutinesSweeping, sample.RSSBeforeKiB, sample.RSSSuspendedKiB, sample.LatencyP50, sample.LatencyP99, sample.ThroughputRunsPerSec)
	}
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
	revision := os.Getenv("BLOK_BENCH_SOURCE_REVISION")
	if !fullRevision.MatchString(revision) {
		t.Fatalf("BLOK_BENCH_SOURCE_REVISION=%q: want the full commit of the measured source", revision)
	}
	if os.Getenv("BLOK_BENCH_TOPOLOGY") == "" {
		t.Fatal("BLOK_BENCH_TOPOLOGY is required: the host, CPU, disk and load the samples were taken on")
	}
	modes := strings.Split(envString("BLOK_WAITS_MODES", Burst+","+Backlog), ",")
	for _, mode := range modes {
		if mode != Burst && mode != Backlog {
			t.Fatalf("BLOK_WAITS_MODES: unknown mode %q", mode)
		}
	}
	runs, repetitions := envInt(t, "BLOK_WAITS_RUNS", 10000), envInt(t, "BLOK_WAITS_REPETITIONS", 3)
	workers, batch := envInt(t, "BLOK_WAITS_WORKERS", 8), envInt(t, "BLOK_WAITS_BATCH", 500)
	kernel, _ := exec.Command("uname", "-srv").Output()
	report := Report{
		SchemaVersion:  2,
		Workload:       "issue332-wait-v2: admit, start to suspension at one wait, sample while Run sweeps, then signal all and resume to completion, as a burst (Run sweeping, Wake per signal) or a backlog (signals committed with Run stopped, then Run started); no node runs",
		Guarantees:     "real SQLite file journal (WAL, synchronous FULL), real engine and resumer, one process; footprint is of the whole test process; not a capacity, fleet or durability-under-load claim",
		SourceRevision: revision,
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
		for _, mode := range modes {
			sample := measure(t, t.TempDir(), mode, runs, workers, batch)
			t.Logf("repetition %d %s: goroutines %d→%d→%d (sweeping), RSS %d→%d KiB, suspend %v, signals %v, wakeup %v (%.0f runs/s), p50 %dµs p90 %dµs p99 %dµs max %dµs", i+1, mode, sample.GoroutinesBefore, sample.GoroutinesSuspended, sample.GoroutinesSweeping, sample.RSSBeforeKiB, sample.RSSSuspendedKiB, time.Duration(sample.SuspendNanoseconds), time.Duration(sample.SignalNanoseconds), time.Duration(sample.BurstNanoseconds), sample.ThroughputRunsPerSec, sample.LatencyP50, sample.LatencyP90, sample.LatencyP99, sample.LatencyMax)
			report.Samples = append(report.Samples, sample)
		}
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

var fullRevision = regexp.MustCompile(`^[0-9a-f]{40}$`)

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
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
