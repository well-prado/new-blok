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

const kidArtifact = "sha256:1111111111111111111111111111111111111111111111111111111111111334"

// childWorkflows: parent charges, then runs a child of kid on the charge;
// kid runs vip on its input. Every effect is logged to the file at log.
func childWorkflows(log string) (map[string]Workflow, *engine.Engine) {
	effect := func(name string, run func(value) value) node.Any {
		return node.MustDefine("test/"+name, "1.0.0", func(_ context.Context, in value) (value, error) {
			file, err := os.OpenFile(log, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
			if err != nil {
				return value{}, err
			}
			defer file.Close()
			if _, err := fmt.Fprintln(file, name); err != nil {
				return value{}, err
			}
			return run(in), file.Sync()
		}, node.Description(name), node.Schemas([]byte(valueSchema), []byte(valueSchema)), node.Effects("fixture:"+name)).Any()
	}
	decode := func(raw json.RawMessage) (any, error) {
		var in value
		err := json.Unmarshal(raw, &in)
		return in, err
	}
	parent := contract.InternalProgram{WorkflowID: "parent", Digest: artifact, Format: contract.ControlFormat, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "charge", Kind: "call", Node: "test/charge"},
		{Index: 1, ID: "kid", Kind: "child", Node: "kid", References: []contract.Reference{{Step: "charge"}}},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "kid"}}},
	}}
	kid := contract.InternalProgram{WorkflowID: "kid", Digest: kidArtifact, Format: contract.ControlFormat, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "vip", Kind: "call", Node: "test/vip"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "vip"}}},
	}}
	runner := engine.New(map[string]node.Any{
		"test/charge": effect("charge", func(in value) value { return value{Value: in.Value + 1} }),
		"test/vip":    effect("vip", func(in value) value { return value{Value: in.Value * 10} }),
	})
	return map[string]Workflow{"parent": {Program: parent, DecodeInput: decode}, "kid": {Program: kid, DecodeInput: decode}}, runner
}

// TestResumerRecoversAChildKilledWhileItRuns: a resumer in a child process
// starts a parent, which suspends at its child step; the resumer starts the
// child at once and is SIGKILLed right after the child's effect commits.
// A resumer in this process takes the interrupted child once its lease
// lapses, completes it, which wakes the parent, and completes the parent:
// one child, every effect once.
func TestResumerRecoversAChildKilledWhileItRuns(t *testing.T) {
	if os.Getenv("NEWBLOK_333_RESUMER_CHILD") == "1" {
		resumerChildRunChild()
		return
	}
	directory := t.TempDir()
	path, marker, log := filepath.Join(directory, "journal.db"), filepath.Join(directory, "marker"), filepath.Join(directory, "effects.log")
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), "NEWBLOK_333_RESUMER_CHILD=1", "NEWBLOK_333_PATH="+path, "NEWBLOK_333_MARKER="+marker, "NEWBLOK_333_LOG="+log)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = command.Process.Kill()
			t.Fatal("the child run's effect never committed")
		}
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
	workflows, runner := childWorkflows(log)
	settled := make(chan string, 8)
	resumer, err := New(Config{Journal: j, Engine: runner, Workflows: workflows, Interval: time.Hour, Clock: later.Now,
		Settled: func(runID string, outcome Outcome, err error) {
			if err != nil {
				t.Logf("run %s %s: %v", runID, outcome, err)
			}
			settled <- string(outcome)
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer resumer.Close(ctx)
	var outcomes []string
	for len(outcomes) < 2 {
		if err := resumer.Sweep(ctx); err != nil {
			t.Fatal(err)
		}
		select {
		case outcome := <-settled:
			outcomes = append(outcomes, outcome)
		case <-time.After(20 * time.Second):
			t.Fatalf("outcomes %v; want the child and the parent completed", outcomes)
		}
	}
	rows := func(query string) []string {
		var out []string
		if err := database.WithTx(ctx, func(tx *sql.Tx) error {
			result, err := tx.Query(query)
			if err != nil {
				return err
			}
			defer result.Close()
			for result.Next() {
				var row string
				if err := result.Scan(&row); err != nil {
					return err
				}
				out = append(out, row)
			}
			return result.Err()
		}); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if got := strings.Join(rows(`SELECT workflow || '|' || state || '|' || CAST(output_json AS TEXT) FROM journal_runs ORDER BY rowid`), ","); got != `parent|completed|{"kind":"","value":10},kid|completed|{"value":10,"kind":""}` {
		t.Fatalf("runs %s", got)
	}
	data, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(strings.Fields(string(data)), ","); got != "charge,vip" {
		t.Fatalf("effects across both processes %q; want charge,vip once each", got)
	}
}

// resumerChildRunChild starts a parent on a resumer and parks right after
// the second effect (the child's) commits.
func resumerChildRunChild() {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, os.Getenv("NEWBLOK_333_PATH"))
	if err != nil {
		panic(err)
	}
	var commits atomic.Int32
	park := func(name string) {
		if name == "step-commit" && commits.Add(1) == 2 {
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
	workflows, runner := childWorkflows(os.Getenv("NEWBLOK_333_LOG"))
	resumer, err := New(Config{Journal: j, Engine: runner, Workflows: workflows, Interval: time.Hour, Clock: at.Now})
	if err != nil {
		panic(err)
	}
	admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "crash", Workflow: "parent", ArtifactDigest: artifact, Input: []byte(`{"value":0}`)})
	if err != nil {
		panic(err)
	}
	if err := resumer.Start(ctx, admitted.RunID); err != nil {
		panic(err)
	}
	time.Sleep(time.Minute)
	panic("the execution was not parked")
}
