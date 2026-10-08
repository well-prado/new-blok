package resumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/sqlite"
)

// loopWorkflow is a durable each of five charges (an effect), each charge
// logged to the file at log so counts span processes.
func loopWorkflow(log string) (Workflow, *engine.Engine) {
	charge := node.MustDefine("test/charge", "1.0.0", func(_ context.Context, in value) (value, error) {
		file, err := os.OpenFile(log, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return value{}, err
		}
		defer file.Close()
		if _, err := fmt.Fprintf(file, "charge-%d\n", in.Value); err != nil {
			return value{}, err
		}
		return value{Value: in.Value * 10}, file.Sync()
	}, node.Description("charge"), node.Schemas([]byte(valueSchema), []byte(valueSchema)), node.Effects("fixture:charge")).Any()
	program := contract.InternalProgram{WorkflowID: "loop", Digest: artifact, Format: contract.ControlFormat, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "loop", Kind: "each", Control: &contract.Control{Concurrency: 1,
			Operands: []contract.Operand{{Literal: json.RawMessage(`[{"value":1},{"value":2},{"value":3},{"value":4},{"value":5}]`)}},
			Arms: []contract.Arm{{Name: "body", Instructions: []contract.InternalInstruction{{Index: 0, ID: "charge", Kind: "call", Node: "test/charge", References: []contract.Reference{{Step: "loop"}}}},
				Output: &contract.Operand{Reference: &contract.Reference{Step: "charge"}}}},
		}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "loop"}}},
	}}
	decode := func(raw json.RawMessage) (any, error) {
		var in value
		err := json.Unmarshal(raw, &in)
		return in, err
	}
	return Workflow{Program: program, DecodeInput: decode}, engine.New(map[string]node.Any{"test/charge": charge})
}

// TestResumerRecoversAnEachKilledMidLoop: a resumer in a child process
// executes a run of a durable each and is SIGKILLed right after the third
// item's slot commits. Once the dead execution's lease lapses, a resumer in
// this process finds the run interrupted, takes it, skips the three
// recorded items and completes it: every charge ran exactly once across
// the two processes.
func TestResumerRecoversAnEachKilledMidLoop(t *testing.T) {
	if os.Getenv("NEWBLOK_333_RESUMER_CHILD") == "1" {
		resumerLoopChild()
		return
	}
	directory := t.TempDir()
	path, marker, log := filepath.Join(directory, "journal.db"), filepath.Join(directory, "marker"), filepath.Join(directory, "charges.log")
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), "NEWBLOK_333_RESUMER_CHILD=1", "NEWBLOK_333_PATH="+path, "NEWBLOK_333_MARKER="+marker, "NEWBLOK_333_LOG="+log)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			t.Fatal("the child never reached the third item's slot")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()

	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	later := newClock()
	later.Advance(time.Hour)
	j, err := journal.New(ctx, database, journal.Config{Holder: "parent", Clock: later.Now})
	if err != nil {
		t.Fatal(err)
	}
	workflow, runner := loopWorkflow(log)
	settled := make(chan Outcome, 4)
	resumer, err := New(Config{Journal: j, Engine: runner, Workflows: map[string]Workflow{"loop": workflow}, Interval: time.Hour, Clock: later.Now,
		Settled: func(runID string, outcome Outcome, err error) {
			if err != nil {
				t.Logf("run %s %s: %v", runID, outcome, err)
			}
			settled <- outcome
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer resumer.Close(ctx)
	if err := resumer.Sweep(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case outcome := <-settled:
		if outcome != Completed {
			t.Fatalf("outcome %s; want completed", outcome)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("the interrupted run was not resumed")
	}
	var runID string
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT run_id FROM journal_runs WHERE request_key = 'crash'`).Scan(&runID)
	}); err != nil {
		t.Fatal(err)
	}
	run, err := j.Run(ctx, runID)
	if err != nil || run.State != "completed" || string(run.Output) != `[{"kind":"","value":10},{"kind":"","value":20},{"kind":"","value":30},{"kind":"","value":40},{"kind":"","value":50}]` {
		t.Fatalf("run %+v err=%v", run, err)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(string(data)); strings.Join(got, ",") != "charge-1,charge-2,charge-3,charge-4,charge-5" {
		t.Fatalf("charges across both processes %q; want each once", got)
	}
}

// resumerLoopChild admits a run of the loop workflow and starts it on a
// resumer, parking right after the third slot commits.
func resumerLoopChild() {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, os.Getenv("NEWBLOK_333_PATH"))
	if err != nil {
		panic(err)
	}
	var slots atomic.Int32
	park := func(name string) {
		if name == "scope-slot" && slots.Add(1) == 3 {
			if err := os.WriteFile(os.Getenv("NEWBLOK_333_MARKER"), []byte("ready"), 0o600); err != nil {
				panic(err)
			}
			time.Sleep(time.Minute)
		}
	}
	at := newClock()
	j, err := journal.New(ctx, database, journal.Config{Holder: "child", Clock: at.Now, Hooks: journal.Hooks{AfterCommit: park}})
	if err != nil {
		panic(err)
	}
	workflow, runner := loopWorkflow(os.Getenv("NEWBLOK_333_LOG"))
	resumer, err := New(Config{Journal: j, Engine: runner, Workflows: map[string]Workflow{"loop": workflow}, Interval: time.Hour, Clock: at.Now})
	if err != nil {
		panic(err)
	}
	admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "crash", Workflow: "loop", ArtifactDigest: artifact, Input: []byte(`{"value":0}`)})
	if err != nil {
		panic(err)
	}
	if err := resumer.Start(ctx, admitted.RunID); err != nil {
		panic(err)
	}
	time.Sleep(time.Minute)
	panic("the execution was not parked")
}

// uncertainLoopWorkflow: an each of two items at once; item 1's pure gate
// fails once item 2's effect (block) is in flight, so fail-fast cancels
// that effect and its outcome is unknown.
func uncertainLoopWorkflow() (Workflow, *engine.Engine) {
	started := make(chan struct{})
	var once sync.Once
	schemas := node.Schemas([]byte(valueSchema), []byte(valueSchema))
	gate := node.MustDefine("test/gate", "1.0.0", func(_ context.Context, in value) (value, error) {
		if in.Value != 1 {
			return in, nil
		}
		select {
		case <-started:
		case <-time.After(10 * time.Second):
		}
		return value{}, errors.New("gate")
	}, node.Description("gate"), schemas).Any()
	block := node.MustDefine("test/block", "1.0.0", func(ctx context.Context, _ value) (value, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return value{}, ctx.Err()
	}, node.Description("block"), schemas, node.Effects("fixture:block")).Any()
	program := contract.InternalProgram{WorkflowID: "loop", Digest: artifact, Format: contract.ControlFormat, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "loop", Kind: "each", Control: &contract.Control{Concurrency: 2,
			Operands: []contract.Operand{{Literal: json.RawMessage(`[{"value":1},{"value":2}]`)}},
			Arms: []contract.Arm{{Name: "body", Instructions: []contract.InternalInstruction{
				{Index: 0, ID: "gate", Kind: "call", Node: "test/gate", References: []contract.Reference{{Step: "loop"}}},
				{Index: 1, ID: "block", Kind: "call", Node: "test/block", References: []contract.Reference{{Step: "gate"}}},
			}, Output: &contract.Operand{Reference: &contract.Reference{Step: "block"}}}},
		}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "loop"}}},
	}}
	decode := func(raw json.RawMessage) (any, error) {
		var in value
		err := json.Unmarshal(raw, &in)
		return in, err
	}
	return Workflow{Program: program, DecodeInput: decode}, engine.New(map[string]node.Any{"test/gate": gate, "test/block": block})
}

// uncertainRig is a journal and a resumer for the uncertain loop.
func uncertainRig(t *testing.T) (context.Context, *journal.Journal, *Resumer, chan Outcome) {
	t.Helper()
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	now := newClock()
	j, err := journal.New(ctx, database, journal.Config{Holder: "a", Clock: now.Now})
	if err != nil {
		t.Fatal(err)
	}
	workflow, runner := uncertainLoopWorkflow()
	settled := make(chan Outcome, 8)
	resumer, err := New(Config{Journal: j, Engine: runner, Workflows: map[string]Workflow{"loop": workflow}, Interval: time.Hour, Clock: now.Now,
		Settled: func(runID string, outcome Outcome, err error) { settled <- outcome }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resumer.Close(ctx) })
	return ctx, j, resumer, settled
}

func awaitOutcome(t *testing.T, settled chan Outcome) Outcome {
	t.Helper()
	select {
	case outcome := <-settled:
		return outcome
	case <-time.After(20 * time.Second):
		t.Fatal("the run was not settled")
		return ""
	}
}

// TestFailFastOverAnUncertainEffectSettlesUncertain (#412 Review R round 1,
// BLOCKER 1): a loop fails fast on a pure step while a sibling's effect is
// in flight. The resumer settles the run uncertain on its first execution;
// it used to fail it, be refused (ErrUncertain), leave it interrupted and
// take it again forever.
func TestFailFastOverAnUncertainEffectSettlesUncertain(t *testing.T) {
	ctx, j, resumer, settled := uncertainRig(t)
	admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "uncertain", Workflow: "loop", ArtifactDigest: artifact, Input: []byte(`{"value":0}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := resumer.Start(ctx, admitted.RunID); err != nil {
		t.Fatal(err)
	}
	if outcome := awaitOutcome(t, settled); outcome != Uncertain {
		t.Fatalf("outcome %s; want uncertain on the first execution", outcome)
	}
	if run, err := j.Run(ctx, admitted.RunID); err != nil || run.State != "uncertain" {
		t.Fatalf("run %+v err=%v", run, err)
	}
}

// TestCertainFailureOverAnUncertainEffectSettlesUncertain (BLOCKER 1, the
// resumer's half): whatever certain failure an execution reports, a run
// holding an effect of unknown outcome cannot fail (FailRun refuses it
// with ErrUncertain), so the resumer ends it uncertain instead of leaving
// it to be taken again forever. Here the loop's failure was recorded
// certain (as before the engine's half of the fix) next to an uncertain
// operation.
func TestCertainFailureOverAnUncertainEffectSettlesUncertain(t *testing.T) {
	ctx, j, resumer, settled := uncertainRig(t)
	admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "legacy", Workflow: "loop", ArtifactDigest: artifact, Input: []byte(`{"value":0}`)})
	if err != nil {
		t.Fatal(err)
	}
	run := admitted.RunID
	operation, err := j.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: run, ArtifactDigest: artifact, InvocationPath: "loop/body/block", IterationPath: "loop[1]"}, Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := j.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkUncertain(ctx, operation.Key, attempt.ID, "canceled by fail-fast"); err != nil {
		t.Fatal(err)
	}
	token, err := j.TakeRunLease(ctx, run, base)
	if err != nil {
		t.Fatal(err)
	}
	rj := j.ForRun(run, token)
	digest, _ := engine.InputDigest(value{})
	if err := rj.VerifyRun(ctx, run, artifact, digest); err != nil {
		t.Fatal(err)
	}
	entry, err := rj.EnterScope(ctx, engine.ScopeIdentity{RunID: run, ArtifactDigest: artifact, Path: "loop@root", Kind: "each"}, json.RawMessage(`{"items":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := rj.FailScope(ctx, entry, json.RawMessage(`{"items":2,"failed":{"code":"node_error","class":"failure","step":"gate"}}`)); err != nil {
		t.Fatal(err)
	}
	if err := j.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	if err := resumer.Start(ctx, run); err != nil {
		t.Fatal(err)
	}
	if outcome := awaitOutcome(t, settled); outcome != Uncertain {
		t.Fatalf("outcome %s; want uncertain", outcome)
	}
	if got, err := j.Run(ctx, run); err != nil || got.State != "uncertain" || got.ErrorCode != "node_error" {
		t.Fatalf("run %+v err=%v", got, err)
	}
}
