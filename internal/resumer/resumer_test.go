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
	notices  atomic.Int32

	mu       sync.Mutex
	outcomes map[string][]Outcome
	settled  chan string
}

func program(timeoutMillis int64, call string) contract.InternalProgram {
	return contract.InternalProgram{WorkflowID: "approval", Digest: artifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval", TimeoutMillis: timeoutMillis}},
		{Index: 1, ID: "call", Kind: "call", Node: call},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}}
}

func newRig(t *testing.T, p contract.InternalProgram, holder string, path string) *rig {
	t.Helper()
	r := &rig{t: t, ctx: context.Background(), clock: newClock(), outcomes: map[string][]Outcome{}, settled: make(chan string, 100000)}
	if path == "" {
		path = filepath.Join(t.TempDir(), "journal.db")
	}
	database, err := (sqlite.Backend{}).Open(r.ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	r.database = database
	if r.journal, err = journal.New(r.ctx, database, journal.Config{Holder: holder, Clock: r.clock.Now, WakeupLease: 30 * time.Second}); err != nil {
		t.Fatal(err)
	}
	notify := node.MustDefine("test/notify", "1.0.0", func(_ context.Context, in value) (value, error) {
		r.gate.RLock()
		defer r.gate.RUnlock()
		r.notices.Add(1)
		return in, nil
	}, node.Description("notify"), node.Schemas([]byte(valueSchema), []byte(valueSchema))).Any()
	reject := node.MustDefine("test/reject", "1.0.0", func(context.Context, value) (value, error) {
		return value{}, errors.New("rejected")
	}, node.Description("reject"), node.Schemas([]byte(valueSchema), []byte(valueSchema))).Any()
	if r.resumer, err = New(Config{
		Journal: r.journal,
		Engine:  engine.New(map[string]node.Any{"test/notify": notify, "test/reject": reject}),
		Workflows: map[string]Workflow{"approval": {Program: p, DecodeInput: func(raw json.RawMessage) (any, error) {
			var in value
			err := json.Unmarshal(raw, &in)
			return in, err
		}}},
		Interval: time.Hour, // tests sweep explicitly
		Workers:  8,
		Lease:    30 * time.Second,
		Clock:    r.clock.Now,
		Settled: func(runID string, outcome Outcome, err error) {
			if err != nil {
				t.Logf("run %s %s: %v", runID, outcome, err)
			}
			r.mu.Lock()
			r.outcomes[runID] = append(r.outcomes[runID], outcome)
			r.mu.Unlock()
			r.settled <- runID
		},
	}); err != nil {
		t.Fatal(err)
	}
	return r
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
	time.Sleep(50 * time.Millisecond) // let finished goroutines exit
	after := runtime.NumGoroutine()
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
	r.clock.Advance(5 * time.Second)
	r.resumer.Sweep(r.ctx)
	r.await(1)
	if got := r.runRow(run); got != "completed|-|acknowledged" || fmt.Sprint(r.outcomesOf(run)) != "[suspended completed]" {
		t.Fatalf("after the lease lapsed: %s %v", got, r.outcomesOf(run))
	}
	if got := r.runRow(untouched); got != "accepted|-|" {
		t.Fatalf("a never-started run: %s", got)
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
