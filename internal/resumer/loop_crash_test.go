package resumer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
