package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
)

// Durable each and parallel on a single host (#333 slice 3, ADR 0028):
// every item or arm is a slot, a completed scope holding its result
// wrapped as {"output": …}, written once its last step committed.

// replayedIterations lists the iterations whose steps an execution ran or
// loaded: an item read from its slot runs none.
func replayedIterations(result engine.Result) []string {
	var iterations []string
	for _, step := range result.Steps {
		if step.IterationPath != engine.RootIteration && !contains(iterations, step.IterationPath) {
			iterations = append(iterations, step.IterationPath)
		}
	}
	return iterations
}

func contains(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func encoded(t *testing.T, value any) string {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestDurableEachKilledMidLoop kills the process executing an each of
// five charges after the third item's slot was written, and after the
// third item's charge committed but before its slot was written. The
// resumed execution skips every item with a slot, so their bodies do not
// run again; an item without one replays, its committed charge loading
// rather than running; every charge runs exactly once across both
// processes, and the each's result is its slots in order.
func TestDurableEachKilledMidLoop(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	charges := []string{"loop/body/charge@loop[0]|committed", "loop/body/charge@loop[1]|committed", "loop/body/charge@loop[2]|committed"}
	slot := func(index int) string {
		return "loop/body@loop[" + string(rune('0'+index)) + "]|item|loop@root|completed|"
	}
	for _, c := range []struct {
		name     string
		barrier  string
		left     []string
		replayed []string
	}{
		{"after item 3's slot", "scope-slot", append([]string{`loop@root|each||running|{"items":5}`, slot(0), slot(1), slot(2)}, append(charges, "run|accepted")...), []string{"loop[3]", "loop[4]"}},
		{"after item 3's last step, before its slot", "step-commit", append([]string{`loop@root|each||running|{"items":5}`, slot(0), slot(1)}, append(charges, "run|accepted")...), []string{"loop[2]", "loop[3]", "loop[4]"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			path, log := killControlChildAt(t, "each", c.barrier, 3)
			database, j := newJournalAtPath(t, path, Config{Holder: "inspect"})
			run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
			if got := controlRows(t, j, run); !reflect.DeepEqual(got, c.left) {
				t.Fatalf("rows the kill left:\n got %q\nwant %q", got, c.left)
			}
			_ = database.Close()
			j, run, result, rj, err := resumeControl(t, path, log, "each", nil)
			const want = `[{"value":2},{"value":3},{"value":4},{"value":5},{"value":6}]`
			if err != nil || encoded(t, result.Output) != want {
				t.Fatalf("resumed output=%s err=%v; want %s", encoded(t, result.Output), err, want)
			}
			if got := replayedIterations(result); !reflect.DeepEqual(got, c.replayed) {
				t.Fatalf("iterations the resumed execution ran %q; want %q (an item with a slot is skipped)", got, c.replayed)
			}
			if got, want := invocations(t, log), map[string]int{"charge": 5}; !reflect.DeepEqual(got, want) {
				t.Fatalf("invocations across both processes %v; want %v", got, want)
			}
			if err := rj.CompleteRun(context.Background(), []byte(want)); err != nil {
				t.Fatal(err)
			}
			if got := oneRow(t, j.database, `SELECT state || '|' || CAST(output_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop@root'`); got != `completed|{"items":5}` {
				t.Fatalf("each scope %s", got)
			}
			slots := waitRows(t, j.database, `SELECT path || '=' || CAST(output_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND kind = 'item' ORDER BY path`)
			if len(slots) != 5 || slots[0] != `loop/body@loop[0]={"output":{"value":2}}` || slots[4] != `loop/body@loop[4]={"output":{"value":6}}` {
				t.Fatalf("slots %q", slots)
			}
		})
	}
}

// TestDurableParallelKilledMidArms kills the process executing a parallel
// once one arm's slot is written while the other arm is still running a
// pure call. The resumed execution replays the finished arm (its charge
// loads; a later step reads an arm's results, so arms are never skipped),
// runs the other, and completes; the parallel's scope holds one slot per
// arm.
func TestDurableParallelKilledMidArms(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	path, log := killControlChildAt(t, "parallel", "scope-slot", 1)
	database, j := newJournalAtPath(t, path, Config{Holder: "inspect"})
	run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
	left := []string{`fan@root|parallel||running|{"items":2}`, "fan/0@root|arm|fan@root|completed|", "fan/0/charge@root|committed", "run|accepted"}
	if got := controlRows(t, j, run); !reflect.DeepEqual(got, left) {
		t.Fatalf("rows the kill left:\n got %q\nwant %q", got, left)
	}
	_ = database.Close()
	j, run, result, rj, err := resumeControl(t, path, log, "parallel", nil)
	if err != nil || !reflect.DeepEqual(result.Output, engineValue{Value: 40}) {
		t.Fatalf("resumed output=%#v err=%v; want vip's 40", result.Output, err)
	}
	// slow is pure: the killed child had started it and committed nothing.
	if got, want := invocations(t, log), map[string]int{"charge": 1, "slow": 2, "vip": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations across both processes %v; want %v", got, want)
	}
	if err := rj.CompleteRun(context.Background(), []byte(`{"value":40}`)); err != nil {
		t.Fatal(err)
	}
	want := []string{`fan@root|parallel||completed|{"items":2}`, "fan/0@root|arm|fan@root|completed|", "fan/1@root|arm|fan@root|completed|", "fan/0/charge@root|committed", "fan/1/slow@root|committed", "fan/1/vip@root|committed", "run|completed"}
	if got := controlRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after recovery:\n got %q\nwant %q", got, want)
	}
	if got := oneRow(t, j.database, `SELECT CAST(output_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'fan@root'`); got != `{"items":2}` {
		t.Fatalf("parallel scope output %s", got)
	}
}

// TestDurableEachInTryKilledMidLoop: an each inside a try, killed after
// two of four items; on resume the charges run once each and finally once,
// and the scope tree is try → each → items.
func TestDurableEachInTryKilledMidLoop(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	path, log := killControlChildAt(t, "each-try", "scope-slot", 2)
	j, run, result, rj, err := resumeControl(t, path, log, "each-try", nil)
	const want = `[{"value":2},{"value":3},{"value":4},{"value":5}]`
	if err != nil || encoded(t, result.Output) != want {
		t.Fatalf("resumed output=%s err=%v", encoded(t, result.Output), err)
	}
	if got, want := invocations(t, log), map[string]int{"charge": 4, "release": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations across both processes %v; want %v", got, want)
	}
	if err := rj.CompleteRun(context.Background(), []byte(want)); err != nil {
		t.Fatal(err)
	}
	scopes := waitRows(t, j.database, `SELECT path || '|' || kind || '|' || parent_path || '|' || state FROM journal_scopes WHERE run_id = '`+run+`' ORDER BY rowid`)
	tree := []string{
		"pay@root|try-finally||completed",
		"pay/try/loop@root|each|pay@root|completed",
		"pay/try/loop/body@loop[0]|item|pay/try/loop@root|completed",
		"pay/try/loop/body@loop[1]|item|pay/try/loop@root|completed",
		"pay/try/loop/body@loop[2]|item|pay/try/loop@root|completed",
		"pay/try/loop/body@loop[3]|item|pay/try/loop@root|completed",
	}
	if !reflect.DeepEqual(scopes, tree) {
		t.Fatalf("scope tree:\n got %q\nwant %q", scopes, tree)
	}
}

// loopRig admits a run of program and holds its lease.
func loopRig(t *testing.T, name string) (context.Context, *Journal, string, int64) {
	t.Helper()
	ctx := context.Background()
	database, j := newJournal(t, name+".db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	t.Cleanup(func() { _ = database.Close() })
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: name, Principal: "alice", Workflow: "control", ArtifactDigest: controlArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		t.Fatal(err)
	}
	token, err := j.TakeRunLease(ctx, admitted.RunID, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	return ctx, j, admitted.RunID, token
}

// TestDurableEmptyEach: an each over no items completes its scope with []
// and records no slot.
func TestDurableEmptyEach(t *testing.T) {
	ctx, j, run, token := loopRig(t, "empty")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	result, err := nodes.engine().RunJournaled(ctx, controlProgramOf(eachOf("loop", 0, "charge"), output("loop")), engineValue{Value: 4}, run, j.ForRun(run, token))
	if err != nil || encoded(t, result.Output) != "[]" {
		t.Fatalf("output=%s err=%v", encoded(t, result.Output), err)
	}
	if got := waitRows(t, j.database, `SELECT path || '|' || state || '|' || CAST(input_json AS TEXT) || '|' || CAST(output_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`'`); !reflect.DeepEqual(got, []string{`loop@root|completed|{"items":0}|{"items":0}`}) {
		t.Fatalf("scopes %q", got)
	}
}

// TestDurableEachRecordsItsFailFast: the third of five items fails, so the
// each fails fast and records the failure in its scope. A later execution
// of the run (the item would now succeed) reports that failure at once:
// it neither runs the failed item again nor starts the ones after it.
func TestDurableEachRecordsItsFailFast(t *testing.T) {
	ctx, j, run, token := loopRig(t, "failfast")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	program := controlProgramOf(eachOf("loop", 5, "flaky"), output("loop"))
	rj := j.ForRun(run, token)
	_, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, rj)
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "node_error" || classified.Step != "flaky" {
		t.Fatalf("first execution err=%v; want flaky's node_error", err)
	}
	const recorded = `{"items":5,"failed":{"code":"node_error","class":"failure","step":"flaky"}}`
	if got := oneRow(t, j.database, `SELECT state || '|' || CAST(input_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop@root'`); got != "running|"+recorded {
		t.Fatalf("each scope %s; want running|%s", got, recorded)
	}
	nodes.flakyOK.Store(true)
	_, err = nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
	if !errors.As(err, &classified) || classified.Code != "node_error" || classified.Class != "failure" || classified.Step != "flaky" {
		t.Fatalf("replay err=%v; want the recorded failure", err)
	}
	if got, want := invocations(t, nodes.log), map[string]int{"flaky": 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations %v; want %v (no item after the recorded failure)", got, want)
	}
	if err := rj.FailRun(ctx, classified.Code, classified.Class); err != nil {
		t.Fatal(err)
	}
	if got := oneRow(t, j.database, `SELECT state FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop@root'`); got != "canceled" {
		t.Fatalf("each scope after the run failed: %s", got)
	}
}

// TestConcurrentItemsCommitOnceAfterAWait: a run resumed after a wait runs
// an each of sixteen concurrent pure items. The wait's wakeup is
// acknowledged once, by the each's scope entry, which commits before any
// item runs: the items' concurrent commits never share a pending
// acknowledgement (waits cannot appear inside an each or parallel).
func TestConcurrentItemsCommitOnceAfterAWait(t *testing.T) {
	ctx, j, run, token := loopRig(t, "concurrent")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	loop := eachOf("loop", 16, "notify")
	loop.Control.Concurrency = 16
	program := controlProgramOf(contract.InternalInstruction{ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}}, loop, output("loop"))
	if _, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token)); !suspended(err) {
		t.Fatalf("first execution err=%v; want suspended", err)
	}
	if err := j.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Signal(ctx, signal.Envelope{RunID: run, SignalID: "s1", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
		t.Fatal(err)
	}
	listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Second), 10)
	if err != nil || len(listed) != 1 {
		t.Fatalf("listed=%v err=%v", waitIDs(listed), err)
	}
	rj := j.ForRun(run, listed[0].LeaseToken)
	result, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, rj)
	if err != nil || len(result.Output.([]any)) != 16 {
		t.Fatalf("resumed err=%v output=%v", err, result.Output)
	}
	if err := rj.CompleteRun(ctx, []byte(`[]`)); err != nil {
		t.Fatal(err)
	}
	if got := oneRow(t, j.database, `SELECT state FROM journal_waits WHERE run_id = '`+run+`'`); got != "acknowledged" {
		t.Fatalf("wait %s", got)
	}
}

// TestThousandItemLoopInspectsAndCompacts: a durable each of 1,000 items
// leaves one scope per item; inspection pages over them and finds one by
// its path, and compaction of the completed run removes every one.
func TestThousandItemLoopInspectsAndCompacts(t *testing.T) {
	ctx, j, run, token := loopRig(t, "thousand")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	loop := eachOf("loop", 1000, "notify")
	loop.Control.Concurrency = 8
	rj := j.ForRun(run, token)
	result, err := nodes.engine().RunJournaled(ctx, controlProgramOf(loop, output("loop")), engineValue{Value: 4}, run, rj)
	if err != nil || len(result.Output.([]any)) != 1000 {
		t.Fatalf("err=%v", err)
	}
	if err := rj.CompleteRun(ctx, []byte(`[]`)); err != nil {
		t.Fatal(err)
	}
	if got := oneRow(t, j.database, `SELECT COUNT(*) FROM journal_scopes WHERE run_id = '`+run+`' AND kind = 'item' AND parent_path = 'loop@root' AND state = 'completed'`); got != "1000" {
		t.Fatalf("%s item scopes; want 1000", got)
	}
	_, steps, total, err := j.ReadInspection(ctx, "alice", run, "", 0, 50, nil, 4096)
	if err != nil || total != 2001 || len(steps) != 50 {
		t.Fatalf("inspection total=%d page=%d err=%v; want 2001 paths (1,001 scopes, 1,000 operations), a page of 50", total, len(steps), err)
	}
	if _, steps, total, err = j.ReadInspection(ctx, "alice", run, "loop/body@loop[999]", 0, 50, nil, 4096); err != nil || total != 1 || len(steps) != 1 || !strings.HasPrefix(steps[0].ID, "loop/body@loop[999]") {
		t.Fatalf("inspecting one item: total=%d steps=%+v err=%v", total, steps, err)
	}
	report, err := j.Compact(ctx, fixtureBase.Add(24*time.Hour))
	if err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compaction %+v err=%v", report, err)
	}
	for _, table := range []string{"journal_scopes", "journal_operations", "journal_runs"} {
		if got := oneRow(t, j.database, `SELECT COUNT(*) FROM `+table+` WHERE run_id = '`+run+`'`); got != "0" {
			t.Fatalf("%s rows of the compacted run: %s", table, got)
		}
	}
}

// TestDurableEachCanceledByTheCallerIsNotRecorded: the caller cancels an
// execution inside an each. That is not the loop's failure, so nothing is
// recorded and the next execution runs the rest of the loop.
func TestDurableEachCanceledByTheCallerIsNotRecorded(t *testing.T) {
	ctx, j, run, token := loopRig(t, "canceled")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	program := controlProgramOf(eachOf("loop", 3, "hold"), output("loop"))
	canceled, cancel := context.WithCancel(ctx)
	go func() { <-nodes.released; cancel() }()
	if _, err := nodes.engine().RunJournaled(canceled, program, engineValue{Value: 4}, run, j.ForRun(run, token)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution err=%v", err)
	}
	if got := oneRow(t, j.database, `SELECT CAST(input_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop@root'`); got != `{"items":3}` {
		t.Fatalf("each decision after the caller's cancellation: %s", got)
	}
	result, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
	if err != nil || encoded(t, result.Output) != `[{"value":1},{"value":2},{"value":3}]` {
		t.Fatalf("next execution output=%s err=%v", encoded(t, result.Output), err)
	}
}

// TestDurableEachNullResultsFillTheirSlots: an item whose result is JSON
// null fills its slot ({"output":null}), so a later execution skips it like
// any other.
func TestDurableEachNullResultsFillTheirSlots(t *testing.T) {
	ctx, j, run, token := loopRig(t, "nulls")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	loop := eachOf("loop", 3, "charge")
	loop.Control.Arms[0].Output = &contract.Operand{Literal: json.RawMessage(`null`)}
	program := controlProgramOf(loop, output("loop"))
	for execution := 1; execution <= 2; execution++ {
		result, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
		if err != nil || encoded(t, result.Output) != "[null,null,null]" {
			t.Fatalf("execution %d output=%s err=%v", execution, encoded(t, result.Output), err)
		}
		if replayed := replayedIterations(result); execution == 2 && len(replayed) != 0 {
			t.Fatalf("the second execution ran iterations %q; want every null slot skipped", replayed)
		}
	}
	if got := oneRow(t, j.database, `SELECT CAST(output_json AS TEXT) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'loop/body@loop[1]'`); got != `{"output":null}` {
		t.Fatalf("slot %s", got)
	}
}

// TestDurableEachRecordedForOtherItemsIsAConflict: a loop scope recorded
// for another number of items than its items now are is a permanent
// journal_scope_conflict: exactly N slots, never a slot left over or
// missing.
func TestDurableEachRecordedForOtherItemsIsAConflict(t *testing.T) {
	ctx, j, run, token := loopRig(t, "items")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	program := controlProgramOf(eachOf("loop", 5, "flaky"), output("loop"))
	if _, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token)); err == nil {
		t.Fatal("the first execution did not fail")
	}
	if err := j.database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE journal_scopes SET input_json = CAST('{"items":6}' AS BLOB) WHERE run_id = ? AND path = 'loop@root'`, run)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	_, err := nodes.engine().RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token))
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "journal_scope_conflict" || !Permanent(err) {
		t.Fatalf("err=%v; want a permanent journal_scope_conflict", err)
	}
}

// TestDurableConstructsInsideItemsNestUnderTheirSlot: a construct inside
// an each's body is a child of its item's slot, in that item's iteration,
// and its steps carry the item's iteration.
func TestDurableConstructsInsideItemsNestUnderTheirSlot(t *testing.T) {
	ctx, j, run, token := loopRig(t, "nested")
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	route := contract.InternalInstruction{ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{{Literal: json.RawMessage(`true`)}}, Arms: []contract.Arm{
		{Name: "then", Instructions: []contract.InternalInstruction{call(0, "notify", "notify", "loop")}, Output: ref("notify")},
		{Name: "else", Output: &contract.Operand{Literal: json.RawMessage(`null`)}},
	}}}
	loop := eachOf("loop", 2, "notify")
	loop.Control.Arms[0].Instructions, loop.Control.Arms[0].Output = []contract.InternalInstruction{route}, ref("route")
	rj := j.ForRun(run, token)
	if _, err := nodes.engine().RunJournaled(ctx, controlProgramOf(loop, output("loop")), engineValue{Value: 4}, run, rj); err != nil {
		t.Fatal(err)
	}
	if err := rj.CompleteRun(ctx, []byte(`[]`)); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`loop@root|each||completed|{"items":2}`,
		`loop/body/route@loop[0]|if|loop/body@loop[0]|completed|{"arm":"then"}`,
		"loop/body@loop[0]|item|loop@root|completed|",
		`loop/body/route@loop[1]|if|loop/body@loop[1]|completed|{"arm":"then"}`,
		"loop/body@loop[1]|item|loop@root|completed|",
		"loop/body/route/then/notify@loop[0]|committed",
		"loop/body/route/then/notify@loop[1]|committed",
		"run|completed",
	}
	if got := controlRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}
