package resumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

const (
	artifact    = "sha256:1111111111111111111111111111111111111111111111111111111111111332"
	valueSchema = `{"type":"object","properties":{"value":{"type":"integer"},"kind":{"type":"string"}},"required":["value"]}`
)

var base = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

// value encodes its fields out of key order, as typed inputs do: the run
// is admitted as JSON and executed with this decoded value.
type value struct {
	Value int    `json:"value"`
	Kind  string `json:"kind"`
}

// clock is a fake clock the journal and the resumer share.
type clock struct{ now atomic.Int64 }

func newClock() *clock                   { c := &clock{}; c.now.Store(base.UnixNano()); return c }
func (c *clock) Now() time.Time          { return time.Unix(0, c.now.Load()).UTC() }
func (c *clock) Advance(d time.Duration) { c.now.Add(int64(d)) }

// rig is a journal on a real SQLite file, an engine with a pure "notify"
// node (it blocks while gate is held) and a failing "reject" node, and a
// resumer whose outcomes are recorded.
type rig struct {
	t        *testing.T
	ctx      context.Context
	database store.Database
	journal  *journal.Journal
	clock    *clock
	resumer  *Resumer
	gate     sync.RWMutex
	entered  atomic.Int32 // notify calls begun, before the gate
	notices  atomic.Int32

	mu       sync.Mutex
	outcomes map[string][]Outcome
	settled  chan string
	errs     []error
	hold     chan struct{} // test/hold blocks until it closes or its context ends
	holding  atomic.Int32
}

// rigOptions changes the rig's journal lease and resumer configuration.
type rigOptions struct {
	lease  time.Duration
	config func(*Config)
}

func program(timeoutMillis int64, call string) contract.InternalProgram {
	return contract.InternalProgram{WorkflowID: "approval", Digest: artifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval", TimeoutMillis: timeoutMillis}},
		{Index: 1, ID: "call", Kind: "call", Node: call},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}}
}

// straight is a program with no wait: its call runs as soon as the run
// starts, so a run executing it has no open or fired wait.
func straight(call string) contract.InternalProgram {
	return contract.InternalProgram{WorkflowID: "approval", Digest: artifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "call", Kind: "call", Node: call},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "call"}}},
	}}
}

func newRig(t *testing.T, p contract.InternalProgram, holder string, path string) *rig {
	t.Helper()
	return newRigWith(t, p, holder, path, rigOptions{})
}

func newRigWith(t *testing.T, p contract.InternalProgram, holder string, path string, options rigOptions) *rig {
	t.Helper()
	if options.lease == 0 {
		options.lease = 30 * time.Second
	}
	r := &rig{t: t, ctx: context.Background(), clock: newClock(), outcomes: map[string][]Outcome{}, settled: make(chan string, 100000), hold: make(chan struct{})}
	if path == "" {
		path = filepath.Join(t.TempDir(), "journal.db")
	}
	database, err := (sqlite.Backend{}).Open(r.ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	r.database = database
	if r.journal, err = journal.New(r.ctx, database, journal.Config{Holder: holder, Clock: r.clock.Now, WakeupLease: options.lease}); err != nil {
		t.Fatal(err)
	}
	notify := node.MustDefine("test/notify", "1.0.0", func(_ context.Context, in value) (value, error) {
		r.entered.Add(1)
		r.gate.RLock()
		defer r.gate.RUnlock()
		r.notices.Add(1)
		return in, nil
	}, node.Description("notify"), node.Schemas([]byte(valueSchema), []byte(valueSchema))).Any()
	reject := node.MustDefine("test/reject", "1.0.0", func(context.Context, value) (value, error) {
		return value{}, errors.New("rejected")
	}, node.Description("reject"), node.Schemas([]byte(valueSchema), []byte(valueSchema))).Any()
	hold := node.MustDefine("test/hold", "1.0.0", func(ctx context.Context, in value) (value, error) {
		r.holding.Add(1)
		select {
		case <-r.hold:
			return in, nil
		case <-ctx.Done():
			return value{}, ctx.Err()
		}
	}, node.Description("hold"), node.Schemas([]byte(valueSchema), []byte(valueSchema))).Any()
	config := Config{
		Journal: r.journal,
		Engine:  engine.New(map[string]node.Any{"test/notify": notify, "test/reject": reject, "test/hold": hold}),
		Workflows: map[string]Workflow{"approval": {Program: p, DecodeInput: func(raw json.RawMessage) (any, error) {
			var in value
			err := json.Unmarshal(raw, &in)
			if in.Kind == "panic" {
				panic("decoder bug")
			}
			if in.Kind == "transient" {
				// The engine cannot encode this input: a persistence
				// fault, not the workflow's, standing in for a transient
				// one.
				return map[string]any{"value": in.Value, "fn": func() {}}, err
			}
			return in, err
		}}},
		Interval: time.Hour, // tests sweep explicitly
		Workers:  8,
		Clock:    r.clock.Now,
		OnError: func(err error) {
			r.mu.Lock()
			r.errs = append(r.errs, err)
			r.mu.Unlock()
		},
		Settled: func(runID string, outcome Outcome, err error) {
			if err != nil {
				t.Logf("run %s %s: %v", runID, outcome, err)
			}
			r.mu.Lock()
			r.outcomes[runID] = append(r.outcomes[runID], outcome)
			r.mu.Unlock()
			r.settled <- runID
		},
	}
	if options.config != nil {
		options.config(&config)
	}
	if r.resumer, err = New(config); err != nil {
		t.Fatal(err)
	}
	return r
}

func (r *rig) admitInput(key, input string) string {
	r.t.Helper()
	admitted, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: key, Workflow: "approval", ArtifactDigest: artifact, Input: []byte(input)})
	if err != nil {
		r.t.Fatal(err)
	}
	return admitted.RunID
}

func (r *rig) admit(key string) string {
	r.t.Helper()
	admitted, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: key, Workflow: "approval", ArtifactDigest: artifact, Input: []byte(`{"value":4,"kind":"order"}`)})
	if err != nil {
		r.t.Fatal(err)
	}
	return admitted.RunID
}

// await waits until n more executions have settled.
func (r *rig) await(n int) {
	r.t.Helper()
	for range n {
		select {
		case <-r.settled:
		case <-time.After(30 * time.Second):
			r.t.Fatalf("an execution did not settle")
		}
	}
}

func (r *rig) outcomesOf(runID string) []Outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]Outcome(nil), r.outcomes[runID]...)
}

func (r *rig) row(query string, args ...any) string {
	r.t.Helper()
	var out string
	if err := r.database.WithTx(r.ctx, func(tx *sql.Tx) error { return tx.QueryRow(query, args...).Scan(&out) }); err != nil {
		r.t.Fatal(err)
	}
	return out
}

// runRow is the run's state, its lease owner ("-" when released) and its
// wait's state.
func (r *rig) runRow(runID string) string {
	return r.row(`SELECT r.state || '|' || COALESCE(r.lease_owner, '-') || '|' || COALESCE((SELECT GROUP_CONCAT(state) FROM journal_waits WHERE run_id = r.run_id), '') FROM journal_runs r WHERE r.run_id = ?`, runID)
}

// awaitRenewal waits until an execution has renewed runID's lease under
// the clock's current time: it then lasts until now plus lease.
func (r *rig) awaitRenewal(runID string, lease time.Duration) {
	r.t.Helper()
	until := r.clock.Now().Add(lease).UnixNano()
	waitFor(r.t, func() bool {
		return r.row(`SELECT COALESCE(lease_until, 0) >= ? FROM journal_runs WHERE run_id = ?`, until, runID) == "1"
	})
}

func (r *rig) signal(runID, id string) {
	r.t.Helper()
	if _, err := r.journal.Signal(r.ctx, signal.Envelope{RunID: runID, SignalID: id, Name: "approval", Principal: "operator", Payload: []byte(`{"approved":true}`)}, true); err != nil {
		r.t.Fatal(err)
	}
}

// TestResumerSuspendsAndResumesRuns starts admitted runs, which suspend at
// their wait and release their lease; signals wake them, and one sweep
// resumes and completes each, consuming its wakeup. A node failure fails
// its run instead.
func TestResumerSuspendsAndResumesRuns(t *testing.T) {
	r := newRig(t, program(0, "test/notify"), "a", "")
	runs := []string{r.admit("one"), r.admit("two")}
	for _, run := range runs {
		if err := r.resumer.Start(r.ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	r.await(len(runs))
	for _, run := range runs {
		if got := r.runRow(run); got != "accepted|-|waiting" || fmt.Sprint(r.outcomesOf(run)) != "[suspended]" {
			t.Fatalf("after start: %s %v", got, r.outcomesOf(run))
		}
		r.signal(run, "s-"+run)
	}
	r.resumer.Sweep(r.ctx)
	r.await(len(runs))
	for _, run := range runs {
		if got := r.runRow(run); got != "completed|-|acknowledged" || fmt.Sprint(r.outcomesOf(run)) != "[suspended completed]" {
			t.Fatalf("after the signal: %s %v", got, r.outcomesOf(run))
		}
	}
	if n := r.notices.Load(); n != 2 {
		t.Fatalf("notices=%d; want one per run", n)
	}

	failing := newRig(t, program(0, "test/reject"), "a", "")
	run := failing.admit("rejected")
	if err := failing.resumer.Start(failing.ctx, run); err != nil {
		t.Fatal(err)
	}
	failing.await(1)
	failing.signal(run, "s1")
	failing.resumer.Sweep(failing.ctx)
	failing.await(1)
	if got := failing.runRow(run); got != "failed|-|acknowledged" || fmt.Sprint(failing.outcomesOf(run)) != "[suspended failed]" {
		t.Fatalf("a failing node: %s %v", got, failing.outcomesOf(run))
	}
}

// TestSuspendedRunsHoldNoGoroutine: 500 runs suspended at a wait leave the
// process with no more goroutines than before (#46 AC5); their state is in
// the journal, not in memory.
func TestSuspendedRunsHoldNoGoroutine(t *testing.T) {
	const runs = 500
	r := newRig(t, program(0, "test/notify"), "a", "")
	ids := make([]string, runs)
	for i := range ids {
		ids[i] = r.admit(fmt.Sprintf("run-%d", i))
	}
	before := runtime.NumGoroutine()
	for _, id := range ids {
		if err := r.resumer.Start(r.ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	r.await(runs)
	// Finished executions' goroutines exit just after they settle: allow
	// them a moment, however loaded the machine.
	after := runtime.NumGoroutine()
	for deadline := time.Now().Add(10 * time.Second); after-before > 10 && time.Now().Before(deadline); after = runtime.NumGoroutine() {
		time.Sleep(5 * time.Millisecond)
	}
	if suspended := r.row(`SELECT COUNT(*) FROM journal_waits WHERE state = 'waiting'`); suspended != fmt.Sprint(runs) {
		t.Fatalf("suspended=%s; want %d", suspended, runs)
	}
	if after-before > 10 {
		t.Fatalf("goroutines %d before, %d with %d runs suspended", before, after, runs)
	}
	t.Logf("goroutines %d before, %d with %d runs suspended", before, after, runs)
}

// TestTimerWakesARun: a wait with a timeout wakes when the shared clock
// passes its due time; the sweep claims it and the run completes timed out.
func TestTimerWakesARun(t *testing.T) {
	r := newRig(t, program(60_000, "test/notify"), "a", "")
	run := r.admit("timer")
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	r.resumer.Sweep(r.ctx)
	if got := r.runRow(run); got != "accepted|-|waiting" {
		t.Fatalf("before due: %s", got)
	}
	r.clock.Advance(time.Minute)
	r.resumer.Sweep(r.ctx)
	r.await(1)
	if got := r.runRow(run); got != "completed|-|acknowledged" {
		t.Fatalf("after due: %s", got)
	}
	if output := r.row(`SELECT output_json FROM journal_runs WHERE run_id = ?`, run); output != `{"timedOut":true}` {
		t.Fatalf("output=%s", output)
	}
}

// TestInterruptedRunIsTakenAgain: a run whose holder died mid-execution
// (here: leased and never released) is taken again once the lease lapses,
// and completes; a run admitted and never leased is left to its admitter.
func TestInterruptedRunIsTakenAgain(t *testing.T) {
	r := newRig(t, program(0, "test/notify"), "a", "")
	run, untouched := r.admit("interrupted"), r.admit("never-started")
	foreign, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "foreign", Workflow: "other", ArtifactDigest: artifact, Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	r.signal(run, "s1")
	// A holder that died: it took the woken run and never released it.
	if _, err := r.journal.PendingResumptions(r.ctx, r.clock.Now(), 10); err != nil {
		t.Fatal(err)
	}
	if _, err := r.database.WithTx(r.ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE journal_waits SET state = 'acknowledged' WHERE run_id = ?`, run)
		return err
	}), error(nil); err != nil {
		t.Fatal(err)
	}
	r.resumer.Sweep(r.ctx)
	if got := r.runRow(run); got != "accepted|a|acknowledged" {
		t.Fatalf("while the dead holder's lease lasts: %s", got)
	}
	// Interrupted runs are looked for a third of a lease (10 s) apart, not
	// every sweep: a scan at +25 s (lease live), then at +31 s the lapsed
	// lease is not looked at yet, and at +36 s it is.
	r.clock.Advance(25 * time.Second)
	r.resumer.Sweep(r.ctx)
	token := r.row(`SELECT lease_token FROM journal_runs WHERE run_id = ?`, run)
	r.clock.Advance(6 * time.Second)
	r.resumer.Sweep(r.ctx)
	if got := r.row(`SELECT lease_token FROM journal_runs WHERE run_id = ?`, run); got != token {
		t.Fatalf("6 s after the last scan the run was taken (token %s, was %s); want not looked at yet", got, token)
	}
	// A run admitted and never started is its admitter's for one lease
	// after admission; then (its admitter crashed) it is taken too.
	if got := r.runRow(untouched); got != "accepted|-|" {
		t.Fatalf("a never-started run within a lease of admission: %s", got)
	}
	r.clock.Advance(5 * time.Second)
	r.resumer.Sweep(r.ctx)
	r.await(2)
	if got := r.runRow(run); got != "completed|-|acknowledged" || fmt.Sprint(r.outcomesOf(run)) != "[suspended completed]" {
		t.Fatalf("after the lease lapsed: %s %v", got, r.outcomesOf(run))
	}
	if got := r.runRow(untouched); got != "accepted|-|waiting" || fmt.Sprint(r.outcomesOf(untouched)) != "[suspended]" {
		t.Fatalf("a never-started run a lease after admission: %s %v", got, r.outcomesOf(untouched))
	}
	// A run of a workflow this resumer does not register is never taken.
	if got := r.runRow(foreign.RunID); got != "accepted|-|" {
		t.Fatalf("another workflow's run: %s", got)
	}
}

// TestCloseDrainsThenCancelsAndReleases: Close stops sweeping and waits for
// running executions up to its deadline, then cancels them; each releases
// its lease, so another resumer takes the run at once and completes it.
// Here the canceled execution had already committed the step after the
// wait (consuming the wakeup), so the run is an interrupted one, not a
// woken one: the next resumer takes it as such.
func TestCloseDrainsThenCancelsAndReleases(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	r := newRig(t, program(0, "test/notify"), "a", path)
	run := r.admit("draining")
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	r.signal(run, "s1")
	r.gate.Lock() // notify blocks
	r.resumer.Sweep(r.ctx)
	// The drain deadline starts once the execution is in notify, past the
	// wait: otherwise, on a loaded machine, it can cancel the execution
	// before it consumes the wakeup (1 in 20 -race runs at load 35).
	waitFor(t, func() bool { return r.entered.Load() == 1 })
	deadline, cancel := context.WithTimeout(r.ctx, 200*time.Millisecond)
	defer cancel()
	go func() { <-deadline.Done(); time.Sleep(50 * time.Millisecond); r.gate.Unlock() }()
	if err := r.resumer.Close(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close=%v; want the drain cut short", err)
	}
	r.await(1)
	if outcomes := r.outcomesOf(run); fmt.Sprint(outcomes) != "[suspended interrupted]" {
		t.Fatalf("outcomes=%v", outcomes)
	}
	if got := r.runRow(run); got != "accepted|-|acknowledged" {
		t.Fatalf("after close: %s; want released, interrupted after its wakeup was consumed", got)
	}
	r.resumer.Sweep(r.ctx) // a closed resumer starts nothing
	if got := r.runRow(run); got != "accepted|-|acknowledged" {
		t.Fatalf("a closed resumer took the run: %s", got)
	}
	next := newRig(t, program(0, "test/notify"), "b", path)
	next.resumer.Sweep(next.ctx)
	next.await(1)
	if got := next.runRow(run); got != "completed|-|acknowledged" {
		t.Fatalf("the next resumer: %s", got)
	}
	if err := next.resumer.Close(next.ctx); err != nil {
		t.Fatal(err)
	}
}

// TestPermanentConflictsAreSettled is Review R round 1's B1 on #384: a run
// that can never succeed is failed with a diagnostic, never retried. A
// suspended run whose wait is canceled fails (journal_wait); and inputs a
// typed decoder reshapes (an unknown field dropped, an optional one
// zero-filled) run to completion. On 1fd524a the first was interrupted on
// every scan and the others could never run.
func TestPermanentConflictsAreSettled(t *testing.T) {
	r := newRig(t, program(0, "test/notify"), "a", "")
	canceled := r.admit("canceled")
	reshaped := []string{r.admitInput("unknown", `{"value":1,"kind":"x","extra":true}`), r.admitInput("optional", `{"value":1}`)}
	for _, run := range append([]string{canceled}, reshaped...) {
		if err := r.resumer.Start(r.ctx, run); err != nil {
			t.Fatal(err)
		}
	}
	r.await(3)
	waitID := r.row(`SELECT wait_id FROM journal_waits WHERE run_id = ?`, canceled)
	if err := r.journal.CancelWait(r.ctx, waitID); err != nil {
		t.Fatal(err)
	}
	for _, run := range reshaped {
		r.signal(run, "s-"+run)
	}
	r.resumer.Sweep(r.ctx)
	r.await(3)
	if got := r.row(`SELECT state || '|' || error_code || '|' || error_class FROM journal_runs WHERE run_id = ?`, canceled); got != "failed|journal_wait|conflict" || fmt.Sprint(r.outcomesOf(canceled)) != "[suspended failed]" {
		t.Fatalf("a run whose wait was canceled: %s %v", got, r.outcomesOf(canceled))
	}
	for _, run := range reshaped {
		if got := r.runRow(run); got != "completed|-|acknowledged" {
			t.Fatalf("a reshaped input: %s %v", got, r.outcomesOf(run))
		}
	}
	r.clock.Advance(time.Hour)
	if err := r.resumer.Sweep(r.ctx); err != nil {
		t.Fatal(err)
	}
	if outcomes := r.outcomesOf(canceled); len(outcomes) != 2 {
		t.Fatalf("the failed run was executed again: %v", outcomes)
	}
}

// TestTransientFaultsAreRetriedThenFailed: a run whose execution hits a
// transient (persistence) fault is retried at the interrupted-run scan's
// pace, MaxRetries times in a row, then failed with a diagnostic.
func TestTransientFaultsAreRetriedThenFailed(t *testing.T) {
	r := newRigWith(t, program(0, "test/notify"), "a", "", rigOptions{config: func(c *Config) { c.MaxRetries = 2 }})
	run := r.admitInput("transient", `{"value":1,"kind":"transient"}`)
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	for range 2 {
		r.clock.Advance(31 * time.Second)
		r.resumer.Sweep(r.ctx)
		r.await(1)
	}
	if got, want := fmt.Sprint(r.outcomesOf(run)), "[interrupted interrupted failed]"; got != want {
		t.Fatalf("outcomes %s; want %s", got, want)
	}
	if got := r.row(`SELECT state || '|' || error_code FROM journal_runs WHERE run_id = ?`, run); got != "failed|retries_exhausted" {
		t.Fatalf("run %s", got)
	}
}

// TestBusyWorkersTakeNoMoreRuns is Review R round 1's B2 on #384: with one
// worker busy, a sweep leases nothing more, however long the run waits; so
// a run is never executed twice in one process, and the queued run is
// taken once the worker frees. On 1fd524a a lapsed queued lease was taken
// again: 4 goroutines for 2 runs.
func TestBusyWorkersTakeNoMoreRuns(t *testing.T) {
	r := newRigWith(t, program(0, "test/hold"), "a", "", rigOptions{config: func(c *Config) { c.Workers = 1; c.RenewEvery = 10 * time.Millisecond }})
	runs := []string{r.admit("first"), r.admit("second")}
	for _, run := range runs {
		r.signal(run, "early-"+run) // pending: each run completes once started
	}
	if err := r.resumer.Start(r.ctx, runs[0]); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.holding.Load() == 1 })
	before := runtime.NumGoroutine()
	for range 5 {
		r.clock.Advance(31 * time.Second)
		r.awaitRenewal(runs[0], 30*time.Second)
		r.resumer.Sweep(r.ctx)
	}
	if n := r.holding.Load(); n != 1 {
		t.Fatalf("%d executions started with one worker busy", n)
	}
	// A second execution adds an execution and a renewal goroutine per
	// sweep; allow for the database's own transient goroutines.
	if got := runtime.NumGoroutine() - before; got > 4 {
		t.Fatalf("goroutines grew by %d while the worker was busy", got)
	}
	if got := r.row(`SELECT COUNT(*) FROM journal_runs WHERE lease_owner IS NOT NULL`); got != "1" {
		t.Fatalf("%s runs leased with one worker", got)
	}
	close(r.hold)
	r.await(1)
	// Settled runs before its worker is freed: a sweep at once could find
	// none free (2 in 20 -race runs under load).
	waitFor(t, func() bool { return len(r.resumer.slots) == 0 })
	r.clock.Advance(31 * time.Second)
	r.resumer.Sweep(r.ctx)
	r.await(1)
	for _, run := range runs {
		if fmt.Sprint(r.outcomesOf(run)) != "[completed]" {
			t.Fatalf("run %s outcomes %v", run, r.outcomesOf(run))
		}
	}
}

// TestStartWaitsForAWorkerBeforeLeasing: Start with every worker busy
// waits for one without leasing the run, so the run's lease cannot lapse
// while it queues and be taken again; once the worker frees, the run is
// leased and executed.
func TestStartWaitsForAWorkerBeforeLeasing(t *testing.T) {
	r := newRigWith(t, program(0, "test/hold"), "a", "", rigOptions{config: func(c *Config) { c.Workers = 1; c.RenewEvery = 10 * time.Millisecond }})
	first, second := r.admit("first"), r.admit("second")
	for _, run := range []string{first, second} {
		r.signal(run, "early-"+run)
	}
	if err := r.resumer.Start(r.ctx, first); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.holding.Load() == 1 })
	started := make(chan error, 1)
	go func() { started <- r.resumer.Start(r.ctx, second) }()
	// Start is queued for the worker; give a lease taken first the time to
	// show (it is one write transaction).
	time.Sleep(200 * time.Millisecond)
	select {
	case err := <-started:
		t.Fatalf("Start returned with the only worker busy: %v", err)
	default:
	}
	if got := r.runRow(second); got != "accepted|-|" {
		t.Fatalf("the queued run: %s; want not leased while it waits for a worker", got)
	}
	close(r.hold)
	if err := <-started; err != nil {
		t.Fatal(err)
	}
	r.await(2)
	for _, run := range []string{first, second} {
		if got := fmt.Sprint(r.outcomesOf(run)); got != "[completed]" {
			t.Fatalf("run %s outcomes %s", run, got)
		}
	}
}

// TestCloseReturnsAtItsDeadline: an execution stuck in a node that ignores
// cancellation does not hold Close past its deadline; Start after Close is
// refused.
func TestCloseReturnsAtItsDeadline(t *testing.T) {
	r := newRig(t, program(0, "test/notify"), "a", "")
	run := r.admit("stuck")
	r.signal(run, "s1")
	r.gate.Lock() // notify blocks and ignores cancellation
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	deadline, cancel := context.WithTimeout(r.ctx, 200*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := r.resumer.Close(deadline); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("close=%v", err)
	}
	// The node is released only after Close returns, so Close returning at
	// all shows it did not wait for it; the bound allows for a loaded
	// machine.
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("Close took %v with a 200ms deadline", elapsed)
	}
	if err := r.resumer.Start(r.ctx, r.admit("late")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Start after Close: err=%v; want ErrClosed", err)
	}
	r.gate.Unlock()
	r.await(1)
}

// TestRenewalKeepsAndLosesTheLease: an execution renews its run lease while
// it runs, so the run is not taken while it outlives the lease; and when
// another holder has taken it anyway, the failed renewal cancels the
// execution, which settles nothing.
func TestRenewalKeepsAndLosesTheLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	r := newRigWith(t, program(0, "test/hold"), "a", path, rigOptions{config: func(c *Config) { c.RenewEvery = 10 * time.Millisecond }})
	run := r.admit("renewed")
	r.signal(run, "s1")
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.holding.Load() == 1 })
	other := newRig(t, program(0, "test/hold"), "b", path)
	for range 3 {
		r.clock.Advance(20 * time.Second)
		r.awaitRenewal(run, 30*time.Second)
		if _, err := other.journal.TakeRunLease(r.ctx, run, r.clock.Now()); !errors.Is(err, journal.ErrLeaseLost) {
			t.Fatalf("taken while renewed, 20 s on: err=%v", err)
		}
	}
	// Another holder takes it over once its lease has lapsed by that
	// holder's clock: 31 s on, past anything a renewal under r's clock can
	// extend it to (a renewal racing the take cannot keep it, as zeroing
	// lease_until in the database could: 3 in 20 -race runs under load).
	if _, err := other.journal.TakeRunLease(r.ctx, run, r.clock.Now().Add(31*time.Second)); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	if got := fmt.Sprint(r.outcomesOf(run)); got != "[interrupted]" {
		t.Fatalf("outcomes %s; want interrupted by the lost renewal", got)
	}
	if got := r.row(`SELECT state || '|' || lease_owner FROM journal_runs WHERE run_id = ?`, run); got != "accepted|b" {
		t.Fatalf("run %s; want left to its new holder", got)
	}
}

// TestLeaseComesFromTheJournal: the resumer renews on the journal's lease,
// so a healthy execution longer than a short lease is not taken over.
func TestLeaseComesFromTheJournal(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	r := newRigWith(t, program(0, "test/hold"), "a", path, rigOptions{lease: 300 * time.Millisecond, config: func(c *Config) { c.Clock = time.Now }})
	if _, err := New(Config{Journal: r.journal, Engine: engine.New(nil), Workflows: map[string]Workflow{"approval": {Program: program(0, "test/hold"), DecodeInput: func(json.RawMessage) (any, error) { return nil, nil }}}, RenewEvery: time.Second}); err == nil {
		t.Fatal("renewing every 1s accepted for a 300ms lease")
	}
	run := r.admit("short")
	r.signal(run, "s1")
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.holding.Load() == 1 })
	other := newRigWith(t, program(0, "test/hold"), "b", path, rigOptions{lease: 300 * time.Millisecond})
	time.Sleep(time.Second)
	if _, err := other.journal.TakeRunLease(r.ctx, run, time.Now()); !errors.Is(err, journal.ErrLeaseLost) {
		t.Fatalf("a healthy 1s execution under a 300ms lease was taken: err=%v", err)
	}
	close(r.hold)
	r.await(1)
}

// TestSweepSurfacesErrors: a sweep that cannot read the journal returns
// the error and hands it to OnError, instead of idling silently.
func TestSweepSurfacesErrors(t *testing.T) {
	r := newRig(t, program(0, "test/notify"), "a", "")
	if err := r.database.Close(); err != nil {
		t.Fatal(err)
	}
	if err := r.resumer.Sweep(r.ctx); err == nil {
		t.Fatal("a sweep over a closed database returned no error")
	}
	r.mu.Lock()
	reported := len(r.errs)
	r.mu.Unlock()
	if reported == 0 {
		t.Fatal("OnError received nothing")
	}
}

// TestPanicsAreContained: a decoder panic fails the run with a diagnostic;
// a Settled panic does not take the process down.
func TestPanicsAreContained(t *testing.T) {
	r := newRigWith(t, program(0, "test/notify"), "a", "", rigOptions{config: func(c *Config) {
		settled := c.Settled
		c.Settled = func(runID string, outcome Outcome, err error) {
			settled(runID, outcome, err)
			panic("observer bug")
		}
	}})
	run := r.admitInput("panic", `{"value":1,"kind":"panic"}`)
	if err := r.resumer.Start(r.ctx, run); err != nil {
		t.Fatal(err)
	}
	r.await(1)
	if got := r.row(`SELECT state || '|' || error_code FROM journal_runs WHERE run_id = ?`, run); got != "failed|input_decode" {
		t.Fatalf("run %s", got)
	}
}

// TestInterruptedRunsAreNotCrowdedOut: with room for one run a sweep, a
// long execution under a live lease, leased longest ago and with no open
// wait, does not crowd out an interrupted run: the scan's read skips runs
// under a live lease, so its one candidate is the interrupted run. Without
// that filter the read returns the long run, the write re-check drops it,
// and the sweep takes nothing.
func TestInterruptedRunsAreNotCrowdedOut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	r := newRigWith(t, straight("test/hold"), "a", path, rigOptions{config: func(c *Config) { c.RenewEvery = 10 * time.Millisecond }})
	long, interrupted := r.admit("long"), r.admit("interrupted")
	if err := r.resumer.Start(r.ctx, long); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return r.holding.Load() == 1 })
	if got := r.runRow(long); got != "accepted|a|" {
		t.Fatalf("the long run: %s; want leased with no wait", got)
	}
	// interrupted: leased by a holder that died, after long was.
	r.clock.Advance(time.Second)
	if _, err := r.journal.TakeRunLease(r.ctx, interrupted, r.clock.Now()); err != nil {
		t.Fatal(err)
	}
	b := newRigWith(t, straight("test/hold"), "b", path, rigOptions{config: func(c *Config) { c.Batch = 1 }})
	close(b.hold)
	r.clock.Advance(40 * time.Second)
	b.clock.now.Store(r.clock.now.Load())
	r.awaitRenewal(long, 30*time.Second)
	if err := b.resumer.Sweep(b.ctx); err != nil {
		t.Fatal(err)
	}
	if got := b.row(`SELECT COALESCE(lease_owner, '-') FROM journal_runs WHERE run_id = ?`, interrupted); got != "b" {
		t.Fatalf("the interrupted run is leased to %q after b's sweep; want b, despite the live long run", got)
	}
	b.await(1)
	if got := fmt.Sprint(b.outcomesOf(interrupted)); got != "[completed]" {
		t.Fatalf("the interrupted run: %s; want completed", got)
	}
	if got := b.runRow(long); got != "accepted|a|" {
		t.Fatalf("the long run: %s; want still a's", got)
	}
	close(r.hold)
	r.await(1)
}

func waitFor(t *testing.T, ready func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ready() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(5 * time.Millisecond)
	}
}
