package journal

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

const (
	engineArtifact = "sha256:0000000000000000000000000000000000000000000000000000000000000332"
	valueSchema    = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`
)

type engineValue struct {
	Value int `json:"value"`
}

// approvalFlow is a real engine program: an effect (charge), a wait on
// "approval", a pure call (notify), and the wait's outcome as output. The
// counters count each node's invocations.
type approvalFlow struct {
	charges, notices atomic.Int32
	runner           *engine.Engine
}

func newApprovalFlow() *approvalFlow {
	f := &approvalFlow{}
	charge := node.MustDefine("test/charge", "1.0.0", func(_ context.Context, in engineValue) (engineValue, error) {
		f.charges.Add(1)
		return engineValue{Value: in.Value + 1}, nil
	}, node.Description("charge"), node.Schemas([]byte(valueSchema), []byte(valueSchema)), node.Effects("fixture:charge")).Any()
	notify := node.MustDefine("test/notify", "1.0.0", func(_ context.Context, in engineValue) (engineValue, error) {
		f.notices.Add(1)
		return in, nil
	}, node.Description("notify"), node.Schemas([]byte(valueSchema), []byte(valueSchema))).Any()
	f.runner = engine.New(map[string]node.Any{"test/charge": charge, "test/notify": notify})
	return f
}

func approvalProgram(name string, timeoutMillis int64) contract.InternalProgram {
	return contract.InternalProgram{WorkflowID: "approval", Digest: engineArtifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "charge", Kind: "call", Node: "test/charge"},
		{Index: 1, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: name, TimeoutMillis: timeoutMillis}},
		{Index: 2, ID: "notify", Kind: "call", Node: "test/notify"},
		{Index: 3, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}}
}

func (f *approvalFlow) run(j *Journal, program contract.InternalProgram, runID string, token int64) (engine.Result, *RunJournal, error) {
	rj := j.ForRun(runID, token)
	result, err := f.runner.RunJournaled(context.Background(), program, engineValue{Value: 4}, runID, rj)
	return result, rj, err
}

func admitApproval(t *testing.T, j *Journal, key string) string {
	t.Helper()
	admitted, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: key, Workflow: "approval", ArtifactDigest: engineArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		t.Fatal(err)
	}
	return admitted.RunID
}

func suspended(err error) bool {
	var s interface{ IsSuspended() bool }
	return errors.As(err, &s) && s.IsSuspended()
}

// engineRows lists the run's waits as name|state|signal_id and its steps'
// operations as invocation_path|state, each in order.
func engineRows(t *testing.T, j *Journal, runID string) []string {
	t.Helper()
	rows := waitRows(t, j.database, `SELECT name || '|' || state || '|' || signal_id FROM journal_waits WHERE run_id = '`+runID+`' ORDER BY rowid`)
	rows = append(rows, waitRows(t, j.database, `SELECT invocation_path || '|' || state FROM journal_operations WHERE run_id = '`+runID+`' ORDER BY rowid`)...)
	return append(rows, waitRows(t, j.database, `SELECT 'run|' || state FROM journal_runs WHERE run_id = '`+runID+`'`)...)
}

// TestEngineSuspendsAndResumesThroughTheJournal runs a real engine program
// through the SQLite journal: it suspends at the wait holding no goroutine,
// a signal fires the wait, the run is listed for resumption, and the
// resumed execution replays the committed effect without invoking it again,
// reads the signal, commits the next step (acknowledging the wakeup in the
// same transaction) and completes. An execution under the lease the
// resumption replaced is refused.
func TestEngineSuspendsAndResumesThroughTheJournal(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "engine.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	flow, program := newApprovalFlow(), approvalProgram("approval", 0)
	run := admitApproval(t, j, "engine")
	first, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := flow.run(j, program, run, first); !suspended(err) || flow.charges.Load() != 1 || flow.notices.Load() != 0 {
		t.Fatalf("first execution err=%v charges=%d notices=%d; want suspended after one charge", err, flow.charges.Load(), flow.notices.Load())
	}
	if err := j.ReleaseRunLease(ctx, run, first); err != nil {
		t.Fatal(err)
	}
	if result, err := j.Signal(ctx, signal.Envelope{RunID: run, SignalID: "s1", Name: "approval", Principal: "operator", Payload: []byte(`{"approved":true}`)}, true); err != nil || result != delivered {
		t.Fatalf("signal=%+v err=%v", result, err)
	}
	listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Second), 10)
	if err != nil || len(listed) != 1 || listed[0].RunID != run {
		t.Fatalf("listed=%+v err=%v", listed, err)
	}
	if _, _, err := flow.run(j, program, run, first); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("an execution under the replaced lease: err=%v; want ErrLeaseLost", err)
	}
	result, rj, err := flow.run(j, program, run, listed[0].LeaseToken)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	output, ok := result.Output.(engine.WaitResult)
	if !ok || output.SignalID != "s1" || string(output.Payload) != `{"approved":true}` || flow.charges.Load() != 1 || flow.notices.Load() != 1 {
		t.Fatalf("output=%#v charges=%d notices=%d; want the signal, one charge, one notice", result.Output, flow.charges.Load(), flow.notices.Load())
	}
	if err := rj.CompleteRun(ctx, []byte(`{"approved":true}`)); err != nil {
		t.Fatal(err)
	}
	want := []string{"approval|acknowledged|s1", "charge|committed", "notify|committed", "run|completed"}
	if got := engineRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}

// TestEngineWaitTimesOutThroughTheJournal: a wait with a timeout is due at
// the journal's clock plus the timeout; the timer claim fires it and leases
// the run, and the resumed execution reads a timeout. The same step with a
// different wait plan is refused.
func TestEngineWaitTimesOutThroughTheJournal(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "timeout.db", Config{Holder: "a", Clock: func() time.Time { return fixtureBase }})
	defer database.Close()
	flow, program := newApprovalFlow(), approvalProgram("approval", 1000)
	run := admitApproval(t, j, "timeout")
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := flow.run(j, program, run, token); !suspended(err) {
		t.Fatalf("first execution err=%v; want suspended", err)
	}
	for _, changed := range []contract.InternalProgram{approvalProgram("review", 1000), approvalProgram("approval", 2000)} {
		if _, _, err := flow.run(j, changed, run, token); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("a changed wait plan %+v at the same step: err=%v; want ErrRequestConflict", changed.Instructions[1].Wait, err)
		}
	}
	if err := j.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	if claimed, err := j.ClaimDueWaits(ctx, fixtureBase.Add(999*time.Millisecond), 10); err != nil || len(claimed) != 0 {
		t.Fatalf("claimed before due: %v err=%v", waitIDs(claimed), err)
	}
	claimed, err := j.ClaimDueWaits(ctx, fixtureBase.Add(time.Second), 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimed=%v err=%v", waitIDs(claimed), err)
	}
	result, rj, err := flow.run(j, program, run, claimed[0].LeaseToken)
	if output, ok := result.Output.(engine.WaitResult); err != nil || !ok || !output.TimedOut || output.SignalID != "" {
		t.Fatalf("output=%#v err=%v; want a timeout", result.Output, err)
	}
	if err := rj.CompleteRun(ctx, []byte(`{"timedOut":true}`)); err != nil {
		t.Fatal(err)
	}
	want := []string{"approval|acknowledged|", "charge|committed", "notify|committed", "run|completed"}
	if got := engineRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}

// TestCompleteRunRefusesAnUnconsumedWakeup: a run whose wait fired cannot
// complete until the wakeup is acknowledged; on ece7f16 it completed and
// the wakeup was dropped.
func TestCompleteRunRefusesAnUnconsumedWakeup(t *testing.T) {
	r := newSignalRig(t, ticking(fixtureBase))
	r.wait("approval", "approve", "root")
	r.send("s1")
	if err := r.journal.CompleteRun(r.ctx, r.run, []byte(`{}`)); !errors.Is(err, ErrRunActiveWork) {
		t.Fatalf("completed with an unconsumed wakeup: err=%v; want ErrRunActiveWork", err)
	}
	consumeWakeup(t, r.journal, r.run, "approval")
	if err := r.journal.CompleteRun(r.ctx, r.run, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
}

// TestEngineReplaysAfterACrashMidResumption kills a process (SIGKILL)
// executing a resumed run, parked just before the commit of the step after
// the wait. The wakeup was read but not consumed: the wait is still fired,
// so once the dead process's lease lapses the run is listed again, and the
// replay reads the same signal, does not repeat the committed charge, and
// completes.
func TestEngineReplaysAfterACrashMidResumption(t *testing.T) {
	if os.Getenv("NEWBLOK_332_ENGINE_CHILD") == "1" {
		runEngineChild()
		return
	}
	ctx := context.Background()
	path := killEngineChild(t, "resume")
	database, j := newJournalAtPath(t, path, Config{Holder: "parent", Clock: ticking(fixtureBase.Add(time.Hour))})
	defer database.Close()
	run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
	if got, want := engineRows(t, j, run), []string{"approval|fired|s1", "charge|committed", "run|accepted"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows the kill left:\n got %q\nwant %q", got, want)
	}
	if listed, err := j.PendingResumptions(ctx, fixtureBase.Add(2*time.Second), 10); err != nil || len(listed) != 0 {
		t.Fatalf("listed while the dead process's lease lasts: %v err=%v", waitIDs(listed), err)
	}
	listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Minute), 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("listed=%v err=%v", waitIDs(listed), err)
	}
	flow := newApprovalFlow()
	result, rj, err := flow.run(j, approvalProgram("approval", 0), run, listed[0].LeaseToken)
	if output, ok := result.Output.(engine.WaitResult); err != nil || !ok || output.SignalID != "s1" || flow.charges.Load() != 0 || flow.notices.Load() != 1 {
		t.Fatalf("replay output=%#v err=%v charges=%d notices=%d; want the signal, no charge, one notice", result.Output, err, flow.charges.Load(), flow.notices.Load())
	}
	if err := rj.CompleteRun(ctx, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if got, want := engineRows(t, j, run), []string{"approval|acknowledged|s1", "charge|committed", "notify|committed", "run|completed"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after recovery:\n got %q\nwant %q", got, want)
	}
}

// TestEngineEffectInterruptedByACrashIsUncertain kills a process parked
// just before the charge's result commits: the effect was dispatched and
// its outcome is unknown, so the next execution fails the run as uncertain
// instead of charging again.
func TestEngineEffectInterruptedByACrashIsUncertain(t *testing.T) {
	if os.Getenv("NEWBLOK_332_ENGINE_CHILD") == "1" {
		runEngineChild()
		return
	}
	path := killEngineChild(t, "effect")
	database, j := newJournalAtPath(t, path, Config{Holder: "parent", Clock: ticking(fixtureBase.Add(time.Hour))})
	defer database.Close()
	run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
	if got, want := engineRows(t, j, run), []string{"charge|dispatched", "run|accepted"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows the kill left:\n got %q\nwant %q", got, want)
	}
	token, err := j.TakeRunLease(context.Background(), run, fixtureBase.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	flow := newApprovalFlow()
	_, _, err = flow.run(j, approvalProgram("approval", 0), run, token)
	var uncertain interface{ IsUncertain() bool }
	if !errors.As(err, &uncertain) || !uncertain.IsUncertain() || flow.charges.Load() != 0 {
		t.Fatalf("err=%v charges=%d; want an uncertain outcome and no second charge", err, flow.charges.Load())
	}
	if got, want := engineRows(t, j, run), []string{"charge|uncertain", "run|accepted"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after the replay:\n got %q\nwant %q", got, want)
	}
}

// killEngineChild runs runEngineChild in a child process in mode, waits
// for it to park, SIGKILLs it, and returns its database.
func killEngineChild(t *testing.T, mode string) string {
	t.Helper()
	directory := t.TempDir()
	path, marker := filepath.Join(directory, "journal.db"), filepath.Join(directory, "marker")
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), "NEWBLOK_332_ENGINE_CHILD=1", "NEWBLOK_332_ENGINE_MODE="+mode, "NEWBLOK_JOURNAL_PATH="+path, "NEWBLOK_JOURNAL_MARKER="+marker)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForJournalMarker(t, marker)
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	return path
}

// runEngineChild suspends a run at its wait, signals it, resumes it, and
// parks before the commit of the step after the wait; in "effect" mode it
// parks before the commit of the first step, the charge, instead.
func runEngineChild() {
	ctx := context.Background()
	var resuming atomic.Bool
	effect := os.Getenv("NEWBLOK_332_ENGINE_MODE") == "effect"
	park := func(name string) {
		if name == "step-commit" && (effect || resuming.Load()) {
			journalMarkerAndWait()
		}
	}
	database, j := newJournalAtPathForChild(os.Getenv("NEWBLOK_JOURNAL_PATH"), Config{Holder: "child", Clock: ticking(fixtureBase), Hooks: Hooks{BeforeCommit: park}})
	defer database.Close()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "crash", Workflow: "approval", ArtifactDigest: engineArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		panic(err)
	}
	flow, program := newApprovalFlow(), approvalProgram("approval", 0)
	token, err := j.TakeRunLease(ctx, admitted.RunID, fixtureBase)
	if err != nil {
		panic(err)
	}
	if _, _, err := flow.run(j, program, admitted.RunID, token); !suspended(err) {
		panic(err)
	}
	if err := j.ReleaseRunLease(ctx, admitted.RunID, token); err != nil {
		panic(err)
	}
	if _, err := j.Signal(ctx, signal.Envelope{RunID: admitted.RunID, SignalID: "s1", Name: "approval", Principal: "operator", Payload: json.RawMessage(`{"approved":true}`)}, true); err != nil {
		panic(err)
	}
	listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Second), 10)
	if err != nil || len(listed) != 1 {
		panic(errors.Join(errors.New("no resumption listed"), err))
	}
	resuming.Store(true)
	_, _, err = flow.run(j, program, admitted.RunID, listed[0].LeaseToken)
	panic(errors.Join(errors.New("the resumed execution was not parked"), err))
}

// reversed is a typed input whose fields encode out of key order.
type reversed struct {
	Zeta  int `json:"zeta"`
	Alpha int `json:"alpha"`
}

// TestTypedInputIsVerifiedAsTheSameJSONValue: a runner hands the engine a
// typed input, which encodes its fields in declaration order; the run was
// admitted as JSON. VerifyRun accepts it through WithInput when it is the
// same JSON value, and refuses a different one. On fd7b4e2 VerifyRun
// compared the sorted-key encoding only, so this run could never execute.
func TestTypedInputIsVerifiedAsTheSameJSONValue(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "typed.db", Config{Holder: "a"})
	defer database.Close()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "typed", Workflow: "typed", ArtifactDigest: engineArtifact, Input: []byte(`{"zeta":1,"alpha":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	token, err := j.TakeRunLease(ctx, admitted.RunID, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	const schema = `{"type":"object","properties":{"zeta":{"type":"integer"},"alpha":{"type":"integer"}},"required":["zeta","alpha"]}`
	echo := node.MustDefine("test/echo", "1.0.0", func(_ context.Context, in reversed) (reversed, error) { return in, nil }, node.Description("echo"), node.Schemas([]byte(schema), []byte(schema))).Any()
	program := contract.InternalProgram{WorkflowID: "typed", Digest: engineArtifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "echo", Kind: "call", Node: "test/echo"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "echo"}}},
	}}
	runner := engine.New(map[string]node.Any{"test/echo": echo})
	encode := func(v any) json.RawMessage { data, _ := json.Marshal(v); return data }
	input := reversed{Zeta: 1, Alpha: 2}
	if _, err := runner.RunJournaled(ctx, program, input, admitted.RunID, j.ForRun(admitted.RunID, token).WithInput(encode(input))); err != nil {
		t.Fatalf("the same JSON value, typed: %v", err)
	}
	// Refused by VerifyRun itself (journal_run_mismatch), before any step.
	refused := func(err error) bool {
		return errors.Is(err, ErrRequestConflict) && strings.Contains(err.Error(), "journal_run_mismatch")
	}
	other := reversed{Zeta: 1, Alpha: 3}
	if _, err := runner.RunJournaled(ctx, program, other, admitted.RunID, j.ForRun(admitted.RunID, token).WithInput(encode(other))); !refused(err) {
		t.Fatalf("a different input: err=%v; want the run refused", err)
	}
	if _, err := runner.RunJournaled(ctx, program, other, admitted.RunID, j.ForRun(admitted.RunID, token).WithInput(encode(input))); !refused(err) {
		t.Fatalf("an engine input other than the one given: err=%v; want the run refused", err)
	}
}
