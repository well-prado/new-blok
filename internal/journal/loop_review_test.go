package journal

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

// Tests from Review R round 1 of #412 (#333 slice 3).

// uncertainLoop: two items at once; item 1's pure gate fails once item
// 2's effect (block) is in flight, so fail-fast cancels that effect and
// its outcome is unknown.
func uncertainLoop() contract.InternalProgram {
	loop := eachOf("loop", 2, "gate")
	loop.Control.Concurrency = 2
	loop.Control.Arms[0].Instructions = append(loop.Control.Arms[0].Instructions, call(1, "block", "block", "gate"))
	loop.Control.Arms[0].Output = ref("block")
	return controlProgramOf(loop, output("loop"))
}

// TestDurableFailFastKeepsASiblingsUncertainEffect (BLOCKER 1): the first
// failure is a pure step's, but a sibling's effect it canceled has an
// unknown outcome. The loop reports and records its failure as uncertain,
// a replay reports it uncertain, and the run ends uncertain; it used to
// record a certain failure that FailRun refuses (ErrUncertain) on every
// execution, forever.
func TestDurableFailFastKeepsASiblingsUncertainEffect(t *testing.T) {
	ctx, j, run, token := loopRig(t, "uncertain-loop")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	rj := j.ForRun(run, token)
	_, err := nodes.engine().RunJournaled(ctx, uncertainLoop(), engineValue{Value: 4}, run, rj)
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "node_error" || classified.Step != "gate" || !classified.Uncertain {
		t.Fatalf("err=%v (%+v); want gate's node_error, uncertain", err, classified)
	}
	const recorded = `{"items":2,"failed":{"code":"node_error","class":"failure","step":"gate","uncertain":true}}`
	if got := oneRow(t, j.database, `SELECT CAST(input_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop@root'`); got != recorded {
		t.Fatalf("loop decision %s; want %s", got, recorded)
	}
	_, err = nodes.engine().RunJournaled(ctx, uncertainLoop(), engineValue{Value: 4}, run, j.ForRun(run, token))
	if !errors.As(err, &classified) || !classified.Uncertain {
		t.Fatalf("replay err=%v; want the recorded failure, uncertain", err)
	}
	if err := rj.FailRun(ctx, classified.Code, classified.Class); !errors.Is(err, ErrUncertain) {
		t.Fatalf("FailRun err=%v; want ErrUncertain (an effect's outcome is unknown)", err)
	}
	if err := rj.MarkRunUncertain(ctx, classified.Code, classified.Class); err != nil {
		t.Fatal(err)
	}
	if got := oneRow(t, j.database, `SELECT state FROM journal_runs WHERE run_id = '`+run+`'`); got != "uncertain" {
		t.Fatalf("run %s; want uncertain", got)
	}
}

// TestDurableFinallyFailureKeepsAnUncertainTry (follow-up 7): finally's
// failure replaces try's, but try's effect has an unknown outcome, so the
// failure stays uncertain.
func TestDurableFinallyFailureKeepsAnUncertainTry(t *testing.T) {
	ctx, j, run, token := loopRig(t, "uncertain-try")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	program := controlProgramOf(contract.InternalInstruction{ID: "pay", Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{
		{Name: "try", Instructions: []contract.InternalInstruction{call(0, "unsure", "unsure")}, Output: ref("unsure")},
		{Name: "finally", Instructions: []contract.InternalInstruction{call(0, "boom", "boom")}},
	}}}, output("pay"))
	_, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Step != "boom" || !classified.Uncertain {
		t.Fatalf("err=%v (%+v); want finally's failure, uncertain", err, classified)
	}
}

// TestDurableEachWithLargeItemsCompletes (SHOULD-FIX 3): four items of
// 300 KiB each; the loop's scope exits with a summary, not its items again
// (which a 1 MiB result bound refused after every effect ran).
func TestDurableEachWithLargeItemsCompletes(t *testing.T) {
	ctx, j, run, token := loopRig(t, "large")
	loop := eachOf("loop", 4, "pad")
	_, err := padEngine(300<<10).RunJournaled(ctx, controlProgramOf(loop, contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: contract.InputStep}}}), engineValue{Value: 4}, run, j.ForRun(run, token))
	if err != nil {
		t.Fatalf("err=%v", err)
	}
	if got := oneRow(t, j.database, `SELECT state || '|' || CAST(output_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop@root'`); got != `completed|{"items":4}` {
		t.Fatalf("loop scope %s", got)
	}
	if got := oneRow(t, j.database, `SELECT COUNT(*) FROM journal_scopes WHERE run_id = '`+run+`' AND kind = 'item' AND length(output_json) > 300000`); got != "4" {
		t.Fatalf("%s large slots; want 4", got)
	}
}

// TestDurableItemOfAFullStepResultFitsItsSlot (NIT 6): an item whose
// result is exactly the largest a step may commit (MaxStepResultBytes)
// still fits its slot with the {"output": …} wrapping.
func TestDurableItemOfAFullStepResultFitsItsSlot(t *testing.T) {
	ctx, j, run, token := loopRig(t, "full")
	pad := MaxStepResultBytes - len(`{"value":1,"pad":""}`)
	program := controlProgramOf(eachOf("loop", 1, "pad"), contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: contract.InputStep}}})
	if _, err := padEngine(pad).RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token)); err != nil {
		t.Fatalf("err=%v", err)
	}
	if got := oneRow(t, j.database, `SELECT length(result_json) || '|' || (SELECT length(output_json) FROM journal_scopes WHERE run_id = '`+run+`' AND kind = 'item') FROM journal_operations WHERE run_id = '`+run+`'`); got != "1048576|1048587" {
		t.Fatalf("step result|slot bytes %s; want 1048576|1048587", got)
	}
}

type padded struct {
	Value int    `json:"value"`
	Pad   string `json:"pad"`
}

// padEngine has one pure node, pad, returning its input's value with a
// pad of n bytes.
func padEngine(n int) *engine.Engine {
	pad := node.MustDefine("test/pad", "1.0.0", func(_ context.Context, in engineValue) (padded, error) {
		return padded{Value: in.Value, Pad: strings.Repeat("x", n)}, nil
	}, node.Description("pad"), node.Schemas([]byte(valueSchema), []byte(`{"type":"object","properties":{"value":{"type":"integer"},"pad":{"type":"string"}},"required":["value"]}`))).Any()
	return engine.New(map[string]node.Any{"test/pad": pad})
}

// TestDurableEachJournalFaultIsNotRecorded (SHOULD-FIX 4, X1): a journal
// fault writing one slot is not the loop's failure: nothing is recorded,
// and the next execution completes the loop.
func TestDurableEachJournalFaultIsNotRecorded(t *testing.T) {
	ctx, j, run, token := loopRig(t, "fault")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	exec := func(query string) {
		t.Helper()
		if err := j.database.WithTx(ctx, func(tx *sql.Tx) error { _, err := tx.Exec(query); return err }); err != nil {
			t.Fatal(err)
		}
	}
	exec(`CREATE TRIGGER fail_slot BEFORE INSERT ON journal_scopes WHEN NEW.path = 'loop/body@loop[1]' BEGIN SELECT RAISE(ABORT, 'disk fault'); END`)
	program := controlProgramOf(eachOf("loop", 3, "notify"), output("loop"))
	_, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Class != "persistence" {
		t.Fatalf("err=%v; want a persistence fault", err)
	}
	if got := oneRow(t, j.database, `SELECT CAST(input_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop@root'`); got != `{"items":3}` {
		t.Fatalf("loop decision after a journal fault %s; want nothing recorded", got)
	}
	exec(`DROP TRIGGER fail_slot`)
	result, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
	if err != nil || encoded(t, result.Output) != `[{"value":1},{"value":2},{"value":3}]` {
		t.Fatalf("next execution output=%s err=%v", encoded(t, result.Output), err)
	}
}
