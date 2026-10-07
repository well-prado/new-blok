package journal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
)

// stepIdentity is the identity the engine gives a step (its own
// stepIdentity), for driving a RunJournal directly.
func stepIdentity(runID, step, input string) engine.StepIdentity {
	hash := sha256.Sum256([]byte(input))
	identity := engine.StepIdentity{RunID: runID, ArtifactDigest: engineArtifact, StepID: step, InputDigest: "sha256:" + hex.EncodeToString(hash[:])}
	encoded, _ := json.Marshal(identity)
	key := sha256.Sum256(encoded)
	identity.OperationKey = "op:" + hex.EncodeToString(key[:])
	return identity
}

// verified returns a RunJournal for run under token, past VerifyRun.
func verified(t *testing.T, j *Journal, runID string, token int64) *RunJournal {
	t.Helper()
	rj := j.ForRun(runID, token)
	if err := rj.VerifyRun(context.Background(), runID, engineArtifact, digestBytes([]byte(`{"value":4}`))); err != nil {
		t.Fatal(err)
	}
	return rj
}

// TestWaitAfterWaitIsNotWokenAgain is Review R round 1's blocker on #380: a
// run that waits on "a" and then at once on "b" reads a's outcome, then
// schedules b. That schedule consumes a's wakeup in the same transaction,
// so the run is not listed and re-executed for a while it waits on b; a
// run found waiting at b with a still fired (an older shape) consumes it
// too. On fd7b4e2 a stayed fired and the run was listed on every poll.
func TestWaitAfterWaitIsNotWokenAgain(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "wait-wait.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	runner := engine.New(nil)
	program := contract.InternalProgram{WorkflowID: "approval", Digest: engineArtifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "a", Kind: "wait", Wait: &contract.WaitInstruction{Name: "a"}},
		{Index: 1, ID: "b", Kind: "wait", Wait: &contract.WaitInstruction{Name: "b"}},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "b"}}},
	}}
	run := admitApproval(t, j, "wait-wait")
	execute := func(token int64) error {
		_, err := runner.RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
		return err
	}
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	if err := execute(token); !suspended(err) {
		t.Fatalf("err=%v; want suspended at a", err)
	}
	if err := j.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Signal(ctx, signal.Envelope{RunID: run, SignalID: "sa", Name: "a", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
		t.Fatal(err)
	}
	resume := func(at time.Duration) {
		t.Helper()
		listed, err := j.PendingResumptions(ctx, fixtureBase.Add(at), 10)
		if err != nil || len(listed) != 1 {
			t.Fatalf("listed=%v err=%v", waitIDs(listed), err)
		}
		if err := execute(listed[0].LeaseToken); !suspended(err) {
			t.Fatalf("err=%v; want suspended at b", err)
		}
		if err := j.ReleaseRunLease(ctx, run, listed[0].LeaseToken); err != nil {
			t.Fatal(err)
		}
	}
	resume(time.Second)
	want := []string{"a|acknowledged|sa", "b|waiting|", "run|accepted"}
	if got := engineRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows while waiting at b:\n got %q\nwant %q", got, want)
	}
	if listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Hour), 10); err != nil || len(listed) != 0 {
		t.Fatalf("listed while waiting at b: %v err=%v; want nothing", waitIDs(listed), err)
	}
	// An older shape: a still fired while b waits. The next resumption
	// consumes it at b.
	execAll(t, database, `UPDATE journal_waits SET state = 'fired' WHERE name = 'a'`)
	resume(2 * time.Hour)
	if got := engineRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after resuming at b:\n got %q\nwant %q", got, want)
	}
	if listed, err := j.PendingResumptions(ctx, fixtureBase.Add(3*time.Hour), 10); err != nil || len(listed) != 0 {
		t.Fatalf("listed again: %v err=%v", waitIDs(listed), err)
	}
}

// TestStaleExecutionWritesNothing: after another holder took the run over,
// every write of the stale execution's RunJournal is ErrLeaseLost and
// changes nothing: reading a step the new holder has dispatched (which
// would mark it uncertain), dispatching a step, failing one, and
// scheduling a wait. On fd7b4e2 the stale Load marked the live dispatch
// uncertain and the stale Await scheduled a wait.
func TestStaleExecutionWritesNothing(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "stale.db", Config{Holder: "a"})
	defer database.Close()
	run := admitApproval(t, j, "stale")
	staleToken, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	stale := verified(t, j, run, staleToken)
	currentToken, err := j.TakeRunLease(ctx, run, fixtureBase.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	current := verified(t, j, run, currentToken)
	charge := stepIdentity(run, "charge", `{"value":4}`)
	attempt, err := current.Begin(ctx, charge, json.RawMessage(`{"value":4}`), []string{"fixture:charge"})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := stale.Load(ctx, charge); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Load of the live dispatch: err=%v; want ErrLeaseLost", err)
	}
	if _, err := stale.Begin(ctx, stepIdentity(run, "refund", `{"value":4}`), json.RawMessage(`{"value":4}`), []string{"fixture:refund"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Begin: err=%v; want ErrLeaseLost", err)
	}
	if err := stale.Fail(ctx, attempt, []string{"fixture:charge"}, errors.New("timeout")); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Fail: err=%v; want ErrLeaseLost", err)
	}
	if _, _, err := stale.Await(ctx, engine.WaitIdentity{Step: stepIdentity(run, "late", `{"name":"late"}`), Name: "late"}); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Await: err=%v; want ErrLeaseLost", err)
	}
	if err := current.Complete(ctx, attempt, json.RawMessage(`{"value":5}`)); err != nil {
		t.Fatalf("the live dispatch commits: %v", err)
	}
	if got, want := engineRows(t, j, run), []string{"charge|committed", "run|accepted"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}

// TestStepResultIsBoundToItsInput: a step recorded for one input is not
// served, dispatched or committed for another (a loop's next iteration, or
// an upgrade that resolves inputs differently): ErrRequestConflict. On
// fd7b4e2 Load returned the first input's result.
func TestStepResultIsBoundToItsInput(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "digest.db", Config{Holder: "a"})
	defer database.Close()
	run := admitApproval(t, j, "digest")
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	rj := verified(t, j, run, token)
	first, second := stepIdentity(run, "notify", `{"value":4}`), stepIdentity(run, "notify", `{"value":5}`)
	if err := rj.Complete(ctx, engine.StepAttempt{Identity: first, AttemptID: pureAttempt}, json.RawMessage(`{"value":4}`)); err != nil {
		t.Fatal(err)
	}
	if result, ok, err := rj.Load(ctx, first); err != nil || !ok || string(result) != `{"value":4}` {
		t.Fatalf("the same input: result=%s ok=%v err=%v", result, ok, err)
	}
	if result, ok, err := rj.Load(ctx, second); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("another input: result=%s ok=%v err=%v; want ErrRequestConflict", result, ok, err)
	}
	charge, recharge := stepIdentity(run, "charge", `{"value":4}`), stepIdentity(run, "charge", `{"value":5}`)
	attempt, err := rj.Begin(ctx, charge, json.RawMessage(`{"value":4}`), []string{"fixture:charge"})
	if err != nil {
		t.Fatal(err)
	}
	if err := rj.Complete(ctx, engine.StepAttempt{Identity: recharge, AttemptID: attempt.AttemptID}, json.RawMessage(`{}`)); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("committing the dispatch for another input: err=%v; want ErrRequestConflict", err)
	}
	if _, _, err := rj.Load(ctx, recharge); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("reading the dispatch for another input: err=%v; want ErrRequestConflict", err)
	}
}

// TestRunJournalMarksARunUncertain: a run with an uncertain step ends as
// uncertain under its lease, consuming the wakeup it read; a stale lease
// cannot. On fd7b4e2 RunJournal had no way to end it.
func TestRunJournalMarksARunUncertain(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "uncertain.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	run := admitApproval(t, j, "uncertain")
	if _, err := j.ScheduleWait(ctx, WaitRequest{RunID: run, WaitID: "approval", Name: "approval", InvocationPath: "approval", IterationPath: rootIteration, DueAt: neverDue}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Signal(ctx, signal.Envelope{RunID: run, SignalID: "s1", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
		t.Fatal(err)
	}
	stale, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Minute), 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("listed=%v err=%v", waitIDs(listed), err)
	}
	if err := j.ForRun(run, stale).MarkRunUncertain(ctx, "timeout", "uncertain"); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("under the stale lease: err=%v; want ErrLeaseLost", err)
	}
	rj := j.ForRun(run, listed[0].LeaseToken)
	rj.pending = []string{"approval"} // the engine read the wakeup
	if err := rj.MarkRunUncertain(ctx, "timeout", "uncertain"); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(waitRows(t, database, `SELECT state FROM journal_waits UNION ALL SELECT state FROM journal_runs`), ","); got != "acknowledged,uncertain" {
		t.Fatalf("wait,run=%s; want acknowledged,uncertain", got)
	}
}

// TestStepResultIsBounded: a step result over MaxStepResultBytes is refused.
func TestStepResultIsBounded(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "bound.db", Config{Holder: "a"})
	defer database.Close()
	run := admitApproval(t, j, "bound")
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	huge := json.RawMessage(`"` + strings.Repeat("x", MaxStepResultBytes) + `"`)
	if err := verified(t, j, run, token).Complete(ctx, engine.StepAttempt{Identity: stepIdentity(run, "notify", `{"value":4}`), AttemptID: pureAttempt}, huge); !errors.Is(err, ErrStepResultLimit) {
		t.Fatalf("err=%v; want ErrStepResultLimit", err)
	}
}
