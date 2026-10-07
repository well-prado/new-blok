package journal

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// signalRig is one admitted run and helpers to wait and signal on it.
type signalRig struct {
	t        *testing.T
	ctx      context.Context
	database store.Database
	journal  *Journal
	run      string
}

func newSignalRig(t *testing.T, clock func() time.Time) *signalRig {
	t.Helper()
	database, j := newJournal(t, "signals.db", Config{Clock: clock})
	t.Cleanup(func() { database.Close() })
	admitted, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "loop", Principal: "alice", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	return &signalRig{t: t, ctx: context.Background(), database: database, journal: j, run: admitted.RunID}
}

func (r *signalRig) wait(waitID, step, iteration string) WaitRecord {
	r.t.Helper()
	record, err := r.journal.ScheduleWait(r.ctx, WaitRequest{RunID: r.run, WaitID: waitID, Name: "approval", InvocationPath: step, IterationPath: iteration, DueAt: fixtureBase.Add(time.Hour)})
	if err != nil {
		r.t.Fatalf("schedule %s: %v", waitID, err)
	}
	return record
}

func (r *signalRig) send(signalID string) SignalResult {
	r.t.Helper()
	result, err := r.journal.Signal(r.ctx, signal.Envelope{RunID: r.run, SignalID: signalID, Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true)
	if err != nil {
		r.t.Fatalf("signal %s: %v", signalID, err)
	}
	return result
}

// rows lists wait_id|state|signal_id for every wait, then signal_id|state
// for every signal, each in insertion order.
func (r *signalRig) rows() []string {
	r.t.Helper()
	rows := waitRows(r.t, r.database, `SELECT wait_id || '|' || state || '|' || signal_id FROM journal_waits ORDER BY rowid`)
	return append(rows, waitRows(r.t, r.database, `SELECT signal_id || '|' || state FROM journal_signals ORDER BY rowid`)...)
}

var (
	pending   = SignalResult{Accepted: true}
	delivered = SignalResult{Accepted: true, Resumed: true}
	late      = SignalResult{Late: true}
)

// TestSignalsByNameQueueFirstInFirstOut is Review R's probe on #332: two
// signals arrive before any wait, the first wait takes the first, a third
// arrives between waits. Signals addressed by name are a queue: each wait
// takes the signal that arrived first, so the second wait takes early-b
// and the third takes "between". On 8426370 "between" was late and lost.
func TestSignalsByNameQueueFirstInFirstOut(t *testing.T) {
	r := newSignalRig(t, ticking(fixtureBase))
	for _, id := range []string{"early-a", "early-b"} {
		if result := r.send(id); result != pending {
			t.Fatalf("%s before any wait=%+v; want pending", id, result)
		}
	}
	if w := r.wait("w0", "approve", "0"); w.SignalID != "early-a" {
		t.Fatalf("w0 took %q; want early-a", w.SignalID)
	}
	if result := r.send("between"); result != pending {
		t.Fatalf("between, sent after w0 closed=%+v; want pending", result)
	}
	if w := r.wait("w1", "approve", "1"); w.SignalID != "early-b" {
		t.Fatalf("w1 took %q; want early-b, which arrived before between", w.SignalID)
	}
	if w := r.wait("w2", "approve", "2"); w.SignalID != "between" {
		t.Fatalf("w2 took %q; want between", w.SignalID)
	}
	want := []string{"w0|resumed|early-a", "w1|resumed|early-b", "w2|resumed|between", "early-a|stored", "early-b|stored", "between|stored"}
	if got := r.rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}

// TestLoopOfThreeIterationsReceivesEverySignal: a run waits on "approval"
// in three loop iterations, with a signal sent before the first wait, one
// between the first and second, and one while the third is open. Each
// iteration receives exactly one, in order, and nothing is late.
func TestLoopOfThreeIterationsReceivesEverySignal(t *testing.T) {
	r := newSignalRig(t, ticking(fixtureBase))
	if result := r.send("before-0"); result != pending {
		t.Fatalf("before-0=%+v", result)
	}
	if w := r.wait("loop[0]", "approve", "0"); w.State != waitResumed || w.SignalID != "before-0" {
		t.Fatalf("loop[0]=%+v", w)
	}
	if result := r.send("between-1"); result != pending {
		t.Fatalf("between-1, sent between iterations=%+v; want pending", result)
	}
	if w := r.wait("loop[1]", "approve", "1"); w.State != waitResumed || w.SignalID != "between-1" {
		t.Fatalf("loop[1]=%+v", w)
	}
	if w := r.wait("loop[2]", "approve", "2"); w.State != waitWaiting {
		t.Fatalf("loop[2]=%+v", w)
	}
	if result := r.send("during-2"); result != delivered {
		t.Fatalf("during-2, sent while loop[2] is open=%+v", result)
	}
	want := []string{"loop[0]|resumed|before-0", "loop[1]|resumed|between-1", "loop[2]|resumed|during-2", "before-0|stored", "between-1|stored", "during-2|stored"}
	if got := r.rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}

// TestSignalAfterTheRunEndsIsLate: a signal to a completed, failed,
// canceled or uncertain run is late, whether or not the run ever waited on
// its name; a signal to a run the journal does not hold is ErrNotFound and
// stores nothing. On 8426370 each was pending.
func TestSignalAfterTheRunEndsIsLate(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "ended.db", Config{})
	defer database.Close()
	for _, end := range []struct {
		name string
		end  func(string) error
	}{
		{"completed", func(run string) error { return j.CompleteRun(ctx, run, []byte(`{}`)) }},
		{"failed", func(run string) error { return j.FailRun(ctx, run, "boom", "internal") }},
		{"canceled", func(run string) error { return j.CancelRun(ctx, run, "operator") }},
		{"uncertain", func(run string) error { return j.MarkRunUncertain(ctx, run, "timeout", "uncertain") }},
	} {
		admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: end.name, Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := end.end(admitted.RunID); err != nil {
			t.Fatalf("%s: %v", end.name, err)
		}
		result, err := j.Signal(ctx, signal.Envelope{RunID: admitted.RunID, SignalID: "after-" + end.name, Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true)
		if err != nil || result != late {
			t.Fatalf("signal to a %s run=%+v err=%v; want late", end.name, result, err)
		}
	}
	if result, err := j.Signal(ctx, signal.Envelope{RunID: "run:unknown", SignalID: "nowhere", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true); !errors.Is(err, ErrNotFound) {
		t.Fatalf("signal to an unknown run=%+v err=%v; want ErrNotFound", result, err)
	}
	want := []string{"after-completed|late", "after-failed|late", "after-canceled|late", "after-uncertain|late"}
	if got := waitRows(t, database, `SELECT signal_id || '|' || state FROM journal_signals ORDER BY rowid`); !reflect.DeepEqual(got, want) {
		t.Fatalf("signals:\n got %q\nwant %q", got, want)
	}
}

// TestConcurrentDuplicateSignalsDeliverOnce: 32 sends of one signal ID,
// from 8 goroutines on each of 4 database handles, give exactly one fresh
// delivery to the open wait and 31 duplicates; then 32 sends of another ID
// with no wait open give one fresh pending signal and 31 duplicates.
func TestConcurrentDuplicateSignalsDeliverOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "concurrent.db")
	database, j := newJournalAtPath(t, path, Config{})
	defer database.Close()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "concurrent", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.ScheduleWait(ctx, WaitRequest{RunID: admitted.RunID, WaitID: "open", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: fixtureBase}); err != nil {
		t.Fatal(err)
	}
	journals := []*Journal{j}
	for range 3 {
		handle, err := (sqlite.Backend{}).Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		defer handle.Close()
		other, err := New(ctx, handle, Config{})
		if err != nil {
			t.Fatal(err)
		}
		journals = append(journals, other)
	}
	for _, c := range []struct {
		signalID    string
		fresh, dupe SignalResult
	}{
		{"once", delivered, SignalResult{Accepted: true, Duplicate: true, Resumed: true}},
		{"queued", pending, SignalResult{Accepted: true, Duplicate: true}},
	} {
		results := make(chan SignalResult, 32)
		start := make(chan struct{})
		var group sync.WaitGroup
		for _, handle := range journals {
			for range 8 {
				group.Go(func() {
					<-start
					result, err := handle.Signal(ctx, signal.Envelope{RunID: admitted.RunID, SignalID: c.signalID, Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true)
					if err != nil {
						t.Errorf("%s: %v", c.signalID, err)
					}
					results <- result
				})
			}
		}
		close(start)
		group.Wait()
		close(results)
		counts := map[SignalResult]int{}
		for result := range results {
			counts[result]++
		}
		if want := map[SignalResult]int{c.fresh: 1, c.dupe: 31}; !reflect.DeepEqual(counts, want) {
			t.Fatalf("%s: results=%v; want %v", c.signalID, counts, want)
		}
	}
	want := []string{"open|resumed|once", "once|stored", "queued|pending"}
	got := append(waitRows(t, database, `SELECT wait_id || '|' || state || '|' || signal_id FROM journal_waits`), waitRows(t, database, `SELECT signal_id || '|' || state FROM journal_signals ORDER BY rowid`)...)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}

// TestSignalOrderIsInsertionOrderNotIDOrder: with a clock that does not
// move, the first of two open waits ("loop[9]", then "loop[10]") takes the
// first signal, and the first wait takes the pending signal that arrived
// first ("s-b", then "s-a"). On 8426370 ties broke on ID text, so
// "loop[10]" and "s-a" went first.
func TestSignalOrderIsInsertionOrderNotIDOrder(t *testing.T) {
	frozen := func() time.Time { return fixtureBase }
	r := newSignalRig(t, frozen)
	r.wait("loop[9]", "approve", "9")
	r.wait("loop[10]", "approve", "10")
	if result := r.send("first"); result != delivered {
		t.Fatalf("first=%+v", result)
	}
	if w, err := r.journal.Wait(r.ctx, "loop[9]"); err != nil || w.SignalID != "first" {
		t.Fatalf("loop[9]=%+v err=%v; want it to take the first signal", w, err)
	}
	if result := r.send("second"); result != delivered {
		t.Fatalf("second=%+v", result)
	}
	for _, id := range []string{"s-b", "s-a"} {
		if result := r.send(id); result != pending {
			t.Fatalf("%s=%+v", id, result)
		}
	}
	if w := r.wait("next", "approve", "11"); w.SignalID != "s-b" {
		t.Fatalf("next took %q; want s-b, which arrived first", w.SignalID)
	}
}

// TestInspectionShowsTheOpenWaitOfAName: two waits of one name are open
// and the newer one is claimed by its timer; the run is suspended on the
// older one, and its wait step shows suspended, not completed. On 8426370
// the step showed the newer, claimed wait.
func TestInspectionShowsTheOpenWaitOfAName(t *testing.T) {
	r := newSignalRig(t, ticking(fixtureBase))
	r.wait("older", "approve", "0")
	if _, err := r.journal.ScheduleWait(r.ctx, WaitRequest{RunID: r.run, WaitID: "newer", Name: "approval", InvocationPath: "approve", IterationPath: "1", DueAt: fixtureBase}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := r.journal.ClaimDueWaits(r.ctx, fixtureBase.Add(time.Minute), 10); err != nil || len(claimed) != 1 || claimed[0].WaitID != "newer" {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	run, steps, _, err := r.journal.ReadInspection(r.ctx, "alice", r.run, "", 0, 10, nil, inspection.MinPayloadBytes)
	if err != nil || run.Status != inspection.StatusSuspended || len(steps) != 1 || steps[0].Status != inspection.StatusSuspended {
		t.Fatalf("run=%s steps=%+v err=%v; want the suspended run's wait step suspended", run.Status, steps, err)
	}
}
