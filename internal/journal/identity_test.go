package journal

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
)

// TestOperationKeyFormatIsPinned: every operation's key is a digest of
// OperationIdentity's JSON form, stored in journal_operations. This is the
// key origin/main a3d90d2 derived; it must not move when a Go field is
// renamed (#382), which on a3d90d2 it did.
func TestOperationKeyFormatIsPinned(t *testing.T) {
	operation := OperationIdentity{RunID: "run:00000000000000000000000000000382", ArtifactDigest: "sha256:" + strings.Repeat("a", 64), InvocationPath: "approval", IterationPath: rootIteration}
	if got, want := operation.Key(), "op:c34d757e5dc0d8b4a9d7d5910298a83095240455fc0b6ad47b9734ec250c791e"; got != want {
		t.Fatalf("Key=%s; want %s", got, want)
	}
}

// TestLoopIterationsWaitIndependently: the engine's iteration path reaches
// the journal, so one wait step in two iterations of a loop is two waits,
// each with its own outcome; the first iteration's signal does not answer
// the second. Before #382 the adapter put every step at the root.
func TestLoopIterationsWaitIndependently(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "iterations.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	run := admitApproval(t, j, "iterations")
	plan := []byte(`{"name":"approval"}`)
	at := func(iteration string) engine.WaitIdentity {
		return engine.WaitIdentity{Step: engine.NewStepIdentity(run, engineArtifact, "approval", iteration, plan), Name: "approval"}
	}
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	rj := verified(t, j, run, token)
	if _, ready, err := rj.Await(ctx, at("loop[0]")); err != nil || ready {
		t.Fatalf("first iteration: ready=%v err=%v; want waiting", ready, err)
	}
	// suspend ends the execution, signals the iteration's wait and resumes
	// the run under a new lease.
	hour := time.Duration(0)
	suspend := func(iteration, signalID string) {
		t.Helper()
		if err := j.ReleaseRunLease(ctx, run, token); err != nil {
			t.Fatal(err)
		}
		if _, err := j.SignalWait(ctx, signal.Envelope{RunID: run, SignalID: signalID, Name: "approval", Principal: "operator", Payload: []byte(`"` + signalID + `"`)}, WaitTarget{InvocationPath: "approval", IterationPath: iteration}, true); err != nil {
			t.Fatal(err)
		}
		hour += time.Hour
		listed, err := j.PendingResumptions(ctx, fixtureBase.Add(hour), 10)
		if err != nil || len(listed) != 1 {
			t.Fatalf("resumptions=%v err=%v; want the run", waitIDs(listed), err)
		}
		token = listed[0].LeaseToken
		rj = verified(t, j, run, token)
	}
	outcome := func(iteration, want string) {
		t.Helper()
		result, ready, err := rj.Await(ctx, at(iteration))
		if err != nil || !ready || result.SignalID != want || string(result.Payload) != `"`+want+`"` {
			t.Fatalf("%s: result=%+v ready=%v err=%v; want signal %s", iteration, result, ready, err, want)
		}
	}
	suspend("loop[0]", "s0")
	outcome("loop[0]", "s0")
	if result, ready, err := rj.Await(ctx, at("loop[1]")); err != nil || ready {
		t.Fatalf("second iteration: result=%+v ready=%v err=%v; want a wait of its own", result, ready, err)
	}
	suspend("loop[1]", "s1")
	outcome("loop[0]", "s0")
	outcome("loop[1]", "s1")
	first, err := j.WaitAt(ctx, run, "approval", "loop[0]")
	if err != nil {
		t.Fatal(err)
	}
	second, err := j.WaitAt(ctx, run, "approval", "loop[1]")
	if err != nil || first.WaitID == second.WaitID {
		t.Fatalf("waits %s and %s err=%v; want two", first.WaitID, second.WaitID, err)
	}
}
