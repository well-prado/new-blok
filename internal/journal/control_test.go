package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

// Durable if, choose and try-finally on a single host (#333 slice 2, ADR
// 0028): real engine programs through the SQLite journal, crashed by a
// real SIGKILL at journal commit barriers and resumed by another process.

const controlArtifact = "sha256:0000000000000000000000000000000000000000000000000000000000000333"

// controlNodes are the nodes the control programs call. Every invocation
// appends the node's name to the file at log, so the count survives the
// process that made it. hold blocks until its context ends the first time
// it runs in a process (released closes when it starts).
type controlNodes struct {
	log      string
	held     atomic.Bool
	released chan struct{}
}

func (c *controlNodes) record(name string) {
	file, err := os.OpenFile(c.log, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		panic(err)
	}
	if _, err := file.WriteString(name + "\n"); err != nil {
		panic(err)
	}
	if err := file.Sync(); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
}

// invocations counts the invocations of each node logged so far.
func invocations(t *testing.T, log string) map[string]int {
	t.Helper()
	data, err := os.ReadFile(log)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, name := range strings.Fields(string(data)) {
		counts[name]++
	}
	return counts
}

func (c *controlNodes) engine() *engine.Engine {
	define := func(name string, effects bool, run func(context.Context, engineValue) (engineValue, error)) node.Any {
		options := []node.Option{node.Description(name), node.Schemas([]byte(valueSchema), []byte(valueSchema))}
		if effects {
			options = append(options, node.Effects("fixture:"+name))
		}
		return node.MustDefine("test/"+name, "1.0.0", func(ctx context.Context, in engineValue) (engineValue, error) {
			c.record(name)
			return run(ctx, in)
		}, options...).Any()
	}
	same := func(_ context.Context, in engineValue) (engineValue, error) { return in, nil }
	nodes := map[string]node.Any{}
	for _, definition := range []node.Any{
		define("charge", true, func(_ context.Context, in engineValue) (engineValue, error) {
			return engineValue{Value: in.Value + 1}, nil
		}),
		define("vip", true, func(_ context.Context, in engineValue) (engineValue, error) {
			return engineValue{Value: in.Value * 10}, nil
		}),
		define("plain", true, same),
		define("second", true, same),
		define("release", true, same),
		define("audit", true, same),
		define("notify", false, same),
		define("boom", false, func(context.Context, engineValue) (engineValue, error) { return engineValue{}, errors.New("boom") }),
		define("hold", false, func(ctx context.Context, in engineValue) (engineValue, error) {
			if c.held.Swap(true) {
				return in, nil
			}
			close(c.released)
			<-ctx.Done()
			return engineValue{}, ctx.Err()
		}),
	} {
		nodes[definition.Descriptor().Name] = definition
	}
	return engine.New(nodes)
}

func call(index int, id, name string, from ...string) contract.InternalInstruction {
	instruction := contract.InternalInstruction{Index: index, ID: id, Kind: "call", Node: "test/" + name}
	if len(from) > 0 {
		instruction.References = []contract.Reference{{Step: from[0]}}
	}
	return instruction
}

func ref(step string, path ...string) *contract.Operand {
	return &contract.Operand{Reference: &contract.Reference{Step: step, Path: path}}
}

func controlProgramOf(instructions ...contract.InternalInstruction) contract.InternalProgram {
	for index := range instructions {
		instructions[index].Index = index
	}
	return contract.InternalProgram{WorkflowID: "control", Digest: controlArtifact, Format: contract.ControlFormat, Instructions: instructions}
}

func output(step string) contract.InternalInstruction {
	return contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: step}}}
}

// branchProgram charges, compares the charge with 3 and, when it is
// bigger, runs vip on it, else plain.
func branchProgram() contract.InternalProgram {
	return controlProgramOf(
		call(0, "charge", "charge"),
		contract.InternalInstruction{ID: "big", Kind: "compare", Control: &contract.Control{Operator: "gt", Operands: []contract.Operand{*ref("charge", "value"), {Literal: json.RawMessage(`3`)}}}},
		contract.InternalInstruction{ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{*ref("big")}, Arms: []contract.Arm{
			{Name: "then", Instructions: []contract.InternalInstruction{call(0, "vip", "vip", "charge")}, Output: ref("vip")},
			{Name: "else", Instructions: []contract.InternalInstruction{call(0, "plain", "plain", "charge")}, Output: ref("plain")},
		}}},
		output("route"),
	)
}

// tryProgram runs try [charge, <second>] finally [release, audit]; second
// is "second" (an effect) or "boom" (a pure call that always fails).
func tryProgram(second string) contract.InternalProgram {
	return controlProgramOf(
		contract.InternalInstruction{ID: "pay", Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{
			{Name: "try", Instructions: []contract.InternalInstruction{call(0, "charge", "charge"), call(1, second, second, "charge")}, Output: ref(second)},
			{Name: "finally", Instructions: []contract.InternalInstruction{call(0, "release", "release"), call(1, "audit", "audit", "release")}},
		}}},
		output("pay"),
	)
}

func controlPrograms(mode string) contract.InternalProgram {
	switch mode {
	case "branch":
		return branchProgram()
	case "try":
		return tryProgram("second")
	case "try-fail":
		return tryProgram("boom")
	}
	panic("unknown control program " + mode)
}

// controlRows lists the run's scopes (path|kind|parent|state|input, in the
// order the run entered them), its step operations
// (invocation@iteration|state, in order) and its state.
func controlRows(t *testing.T, j *Journal, runID string) []string {
	t.Helper()
	rows := waitRows(t, j.database, `SELECT path || '|' || kind || '|' || parent_path || '|' || state || '|' || COALESCE(CAST(input_json AS TEXT), '') FROM journal_scopes WHERE run_id = '`+runID+`' ORDER BY rowid`)
	rows = append(rows, waitRows(t, j.database, `SELECT invocation_path || '@' || iteration_path || '|' || state FROM journal_operations WHERE run_id = '`+runID+`' ORDER BY rowid`)...)
	return append(rows, waitRows(t, j.database, `SELECT 'run|' || state FROM journal_runs WHERE run_id = '`+runID+`'`)...)
}

// killControlChild runs runControlChild for program mode in a child
// process that parks after the given barrier (see runControlChild), waits
// for it, SIGKILLs it, and returns its database and node log.
func killControlChild(t *testing.T, mode string, commits int) (string, string) {
	t.Helper()
	directory := t.TempDir()
	path, marker, log := filepath.Join(directory, "journal.db"), filepath.Join(directory, "marker"), filepath.Join(directory, "nodes.log")
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
	command.Env = append(os.Environ(), "NEWBLOK_333_CONTROL_CHILD=1", "NEWBLOK_333_CONTROL_MODE="+mode, "NEWBLOK_333_CONTROL_COMMITS="+strconv.Itoa(commits),
		"NEWBLOK_333_CONTROL_LOG="+log, "NEWBLOK_JOURNAL_PATH="+path, "NEWBLOK_JOURNAL_MARKER="+marker)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	waitForJournalMarker(t, marker)
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	return path, log
}

// runControlChild admits a run of its program and executes it, parking
// (for the parent's SIGKILL) right after the commit that enters the first
// scope when NEWBLOK_333_CONTROL_COMMITS is 0, else right after that many
// step commits following it.
func runControlChild() {
	ctx := context.Background()
	commits, err := strconv.Atoi(os.Getenv("NEWBLOK_333_CONTROL_COMMITS"))
	if err != nil {
		panic(err)
	}
	var entered atomic.Bool
	var committed atomic.Int32
	park := func(name string) {
		switch {
		case name == "scope-enter" && !entered.Swap(true) && commits == 0:
			journalMarkerAndWait()
		case name == "step-commit" && entered.Load() && int(committed.Add(1)) == commits:
			journalMarkerAndWait()
		}
	}
	database, j := newJournalAtPathForChild(os.Getenv("NEWBLOK_JOURNAL_PATH"), Config{Holder: "child", Clock: ticking(fixtureBase), Hooks: Hooks{AfterCommit: park}})
	defer database.Close()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "crash", Workflow: "control", ArtifactDigest: controlArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		panic(err)
	}
	token, err := j.TakeRunLease(ctx, admitted.RunID, fixtureBase)
	if err != nil {
		panic(err)
	}
	nodes := &controlNodes{log: os.Getenv("NEWBLOK_333_CONTROL_LOG"), released: make(chan struct{})}
	_, err = nodes.engine().RunJournaled(ctx, controlPrograms(os.Getenv("NEWBLOK_333_CONTROL_MODE")), engineValue{Value: 4}, admitted.RunID, j.ForRun(admitted.RunID, token))
	panic(errors.Join(errors.New("the execution was not parked"), err))
}

// resumeControl opens the database a killed child left, takes the run's
// lease once the child's has lapsed, and executes the program again.
func resumeControl(t *testing.T, path, log, mode string, tamper func(*Journal, string)) (*Journal, string, engine.Result, *RunJournal, error) {
	t.Helper()
	database, j := newJournalAtPath(t, path, Config{Holder: "parent", Clock: ticking(fixtureBase.Add(time.Hour))})
	t.Cleanup(func() { _ = database.Close() })
	run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
	if tamper != nil {
		tamper(j, run)
	}
	token, err := j.TakeRunLease(context.Background(), run, fixtureBase.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	nodes := &controlNodes{log: log, released: make(chan struct{})}
	rj := j.ForRun(run, token)
	result, err := nodes.engine().RunJournaled(context.Background(), controlPrograms(mode), engineValue{Value: 4}, run, rj)
	return j, run, result, rj, err
}

// TestDurableBranchReplaysTheRecordedArm kills the process executing an
// if right after its decision committed, and again right after the arm's
// effect committed. The resumed execution follows the recorded arm, never
// repeats the committed effect, never runs the other arm, and completes.
func TestDurableBranchReplaysTheRecordedArm(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	decided := `route@root|if||running|{"arm":"then"}`
	for _, c := range []struct {
		name    string
		commits int
		left    []string
	}{
		{"after the decision", 0, []string{decided, "charge@root|committed", "run|accepted"}},
		{"after the arm's effect", 1, []string{decided, "charge@root|committed", "route/then/vip@root|committed", "run|accepted"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			path, log := killControlChild(t, "branch", c.commits)
			database, j := newJournalAtPath(t, path, Config{Holder: "inspect"})
			run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
			if got := controlRows(t, j, run); !reflect.DeepEqual(got, c.left) {
				t.Fatalf("rows the kill left:\n got %q\nwant %q", got, c.left)
			}
			_ = database.Close()
			j, run, result, rj, err := resumeControl(t, path, log, "branch", nil)
			if err != nil || !reflect.DeepEqual(result.Output, engineValue{Value: 50}) {
				t.Fatalf("resumed output=%#v err=%v; want vip's 50", result.Output, err)
			}
			if got, want := invocations(t, log), map[string]int{"charge": 1, "vip": 1}; !reflect.DeepEqual(got, want) {
				t.Fatalf("invocations across both processes %v; want %v", got, want)
			}
			if err := rj.CompleteRun(context.Background(), []byte(`{"value":50}`)); err != nil {
				t.Fatal(err)
			}
			want := []string{`route@root|if||completed|{"arm":"then"}`, "charge@root|committed", "route/then/vip@root|committed", "run|completed"}
			if got := controlRows(t, j, run); !reflect.DeepEqual(got, want) {
				t.Fatalf("rows after recovery:\n got %q\nwant %q", got, want)
			}
		})
	}
}

// TestDurableBranchNeverReevaluatesItsDecision: after the kill right
// after the decision, the charge the condition read is changed in the
// journal so that the condition now selects else. The resumed execution
// still runs then, the recorded arm: a recorded decision is never
// re-evaluated, as a committed step result is never recomputed.
func TestDurableBranchNeverReevaluatesItsDecision(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	path, log := killControlChild(t, "branch", 0)
	tamper := func(j *Journal, run string) {
		if err := j.database.WithTx(context.Background(), func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE journal_operations SET result_json = '{"value":1}' WHERE run_id = ? AND invocation_path = 'charge'`, run)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	_, _, result, _, err := resumeControl(t, path, log, "branch", tamper)
	if err != nil || !reflect.DeepEqual(result.Output, engineValue{Value: 10}) {
		t.Fatalf("resumed output=%#v err=%v; want vip on the changed charge (10)", result.Output, err)
	}
	if got, want := invocations(t, log), map[string]int{"charge": 1, "vip": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations %v; want %v (plain must never run)", got, want)
	}
}

// TestDurableTryFinallyRunsFinallyOnce kills the process executing a
// try-finally inside try (after the charge committed) and inside finally
// (after the release committed). Every effect runs exactly once across the
// two processes, finally included, and the run completes.
func TestDurableTryFinallyRunsFinallyOnce(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	for _, c := range []struct {
		name    string
		commits int
	}{{"inside try", 1}, {"inside finally", 3}} {
		t.Run(c.name, func(t *testing.T) {
			path, log := killControlChild(t, "try", c.commits)
			j, run, result, rj, err := resumeControl(t, path, log, "try", nil)
			if err != nil || !reflect.DeepEqual(result.Output, engineValue{Value: 5}) {
				t.Fatalf("resumed output=%#v err=%v; want the try's 5", result.Output, err)
			}
			if got, want := invocations(t, log), map[string]int{"charge": 1, "second": 1, "release": 1, "audit": 1}; !reflect.DeepEqual(got, want) {
				t.Fatalf("invocations across both processes %v; want %v", got, want)
			}
			if err := rj.CompleteRun(context.Background(), []byte(`{"value":5}`)); err != nil {
				t.Fatal(err)
			}
			want := []string{"pay@root|try-finally||completed|", "pay/try/charge@root|committed", "pay/try/second@root|committed", "pay/finally/release@root|committed", "pay/finally/audit@root|committed", "run|completed"}
			if got := controlRows(t, j, run); !reflect.DeepEqual(got, want) {
				t.Fatalf("rows after recovery:\n got %q\nwant %q", got, want)
			}
		})
	}
}

// TestDurableTryFailureRunsFinallyOnceAndFails: try fails (a pure call),
// and the process is killed inside finally after the release committed.
// The resumed execution re-runs the failed pure call (nothing of it was
// committed), does not repeat the release, runs audit once and fails with
// the try's failure; failing the run ends the try-finally's scope.
func TestDurableTryFailureRunsFinallyOnceAndFails(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	path, log := killControlChild(t, "try-fail", 2)
	j, run, _, rj, err := resumeControl(t, path, log, "try-fail", nil)
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "node_error" || classified.Step != "boom" {
		t.Fatalf("err=%v; want boom's node_error", err)
	}
	if got, want := invocations(t, log), map[string]int{"charge": 1, "boom": 2, "release": 1, "audit": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations across both processes %v; want %v", got, want)
	}
	if err := rj.FailRun(context.Background(), classified.Code, classified.Class); err != nil {
		t.Fatalf("failing the run: %v", err)
	}
	want := []string{"pay@root|try-finally||canceled|", "pay/try/charge@root|committed", "pay/finally/release@root|committed", "pay/finally/audit@root|committed", "run|failed"}
	if got := controlRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after the failure:\n got %q\nwant %q", got, want)
	}
	if got := oneRow(t, j.database, `SELECT error_text FROM journal_scopes WHERE run_id = '`+run+`'`); got != "node_error" {
		t.Fatalf("scope error %q; want node_error", got)
	}
}

// TestDurableTryCanceledByTheCallerRunsFinallyOnResume: the caller
// cancels the execution inside try. Finally does not run then (#383: only
// the caller's cancellation skips it); the next execution replays the
// committed charge and runs finally once.
func TestDurableTryCanceledByTheCallerRunsFinallyOnResume(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "cancel.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	runner := nodes.engine()
	program := tryProgram("hold")
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "cancel", Workflow: "control", ArtifactDigest: controlArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		t.Fatal(err)
	}
	token, err := j.TakeRunLease(ctx, admitted.RunID, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	go func() { <-nodes.released; cancel() }()
	if _, err := runner.RunJournaled(canceled, program, engineValue{Value: 4}, admitted.RunID, j.ForRun(admitted.RunID, token)); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled execution err=%v", err)
	}
	if got, want := invocations(t, nodes.log), map[string]int{"charge": 1, "hold": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations after the cancellation %v; want %v (no finally)", got, want)
	}
	result, err := runner.RunJournaled(ctx, program, engineValue{Value: 4}, admitted.RunID, j.ForRun(admitted.RunID, token))
	if err != nil || !reflect.DeepEqual(result.Output, engineValue{Value: 5}) {
		t.Fatalf("resumed output=%#v err=%v", result.Output, err)
	}
	if got, want := invocations(t, nodes.log), map[string]int{"charge": 1, "hold": 2, "release": 1, "audit": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations after resuming %v; want %v", got, want)
	}
}

// waitInArmProgram: if (true) then [try [approval wait] finally [release]]
// else [notify]. The wait suspends the run inside both constructs.
func waitInArmProgram() contract.InternalProgram {
	pay := contract.InternalInstruction{ID: "pay", Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{
		{Name: "try", Instructions: []contract.InternalInstruction{{ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}}}, Output: ref("approval")},
		{Name: "finally", Instructions: []contract.InternalInstruction{call(0, "release", "release")}},
	}}}
	return controlProgramOf(
		contract.InternalInstruction{ID: "route", Kind: "if", Control: &contract.Control{Operands: []contract.Operand{{Literal: json.RawMessage(`true`)}}, Arms: []contract.Arm{
			{Name: "then", Instructions: []contract.InternalInstruction{pay}, Output: ref("pay")},
			{Name: "else", Instructions: []contract.InternalInstruction{call(0, "notify", "notify")}, Output: ref("notify")},
		}}},
		output("route"),
	)
}

// TestDurableWaitInsideAnArmSuspendsAndResumes: a run suspends at a wait
// inside a try inside an if, holding nothing; finally does not run at the
// suspension. A signal fires the wait, the run is listed for resumption,
// and the resumed execution (under the listed lease, as the resumer runs
// it) reads the signal, runs finally once and completes. The wait, the
// scopes and the steps are keyed by their real paths, and the scope tree
// (parent, kind, order) is what the run entered (M47a).
func TestDurableWaitInsideAnArmSuspendsAndResumes(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "arm-wait.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	nodes := &controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}
	runner, program := nodes.engine(), waitInArmProgram()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "arm-wait", Workflow: "control", ArtifactDigest: controlArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		t.Fatal(err)
	}
	run := admitted.RunID
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunJournaled(ctx, program, engineValue{Value: 4}, run, j.ForRun(run, token)); !suspended(err) {
		t.Fatalf("first execution err=%v; want suspended", err)
	}
	if got := invocations(t, nodes.log); len(got) != 0 {
		t.Fatalf("invocations at the suspension %v; want none (finally waits for the try)", got)
	}
	suspendedRows := []string{`route@root|if||running|{"arm":"then"}`, "route/then/pay@root|try-finally|route@root|running|", "run|accepted"}
	if got := controlRows(t, j, run); !reflect.DeepEqual(got, suspendedRows) {
		t.Fatalf("rows at the suspension:\n got %q\nwant %q", got, suspendedRows)
	}
	if wait, err := j.WaitAt(ctx, run, "route/then/pay/try/approval", engine.RootIteration); err != nil || wait.State != "waiting" {
		t.Fatalf("wait at its path: %+v err=%v", wait, err)
	}
	if err := j.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	if result, err := j.Signal(ctx, signal.Envelope{RunID: run, SignalID: "s1", Name: "approval", Principal: "operator", Payload: []byte(`{"approved":true}`)}, true); err != nil || result != delivered {
		t.Fatalf("signal=%+v err=%v", result, err)
	}
	listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Second), 10)
	if err != nil || len(listed) != 1 || listed[0].RunID != run {
		t.Fatalf("listed=%v err=%v", waitIDs(listed), err)
	}
	rj := j.ForRun(run, listed[0].LeaseToken)
	result, err := runner.RunJournaled(ctx, program, engineValue{Value: 4}, run, rj)
	if output, ok := result.Output.(engine.WaitResult); err != nil || !ok || output.SignalID != "s1" {
		t.Fatalf("resumed output=%#v err=%v; want the signal", result.Output, err)
	}
	if got, want := invocations(t, nodes.log), map[string]int{"release": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations %v; want %v", got, want)
	}
	if err := rj.CompleteRun(ctx, []byte(`{"approved":true}`)); err != nil {
		t.Fatal(err)
	}
	want := []string{
		`route@root|if||completed|{"arm":"then"}`,
		"route/then/pay@root|try-finally|route@root|completed|",
		"route/then/pay/finally/release@root|committed",
		"run|completed",
	}
	if got := controlRows(t, j, run); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows after resuming:\n got %q\nwant %q", got, want)
	}
	if got := oneRow(t, database, `SELECT state || '|' || invocation_path FROM journal_waits WHERE run_id = '`+run+`'`); got != "acknowledged|route/then/pay/try/approval" {
		t.Fatalf("wait %s", got)
	}
}

// TestDurableRunnerStillRefusesEachAndParallel: their joins are a later
// slice, so a durable run refuses them, at any depth, before writing
// anything.
func TestDurableRunnerStillRefusesEachAndParallel(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "refuse.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	runner := (&controlNodes{log: filepath.Join(t.TempDir(), "nodes.log"), released: make(chan struct{})}).engine()
	parallel := contract.InternalInstruction{ID: "fan", Kind: "parallel", Control: &contract.Control{Arms: []contract.Arm{{Name: "0", Instructions: []contract.InternalInstruction{call(0, "notify", "notify")}}}}}
	nested := controlProgramOf(contract.InternalInstruction{ID: "pay", Kind: "try-finally", Control: &contract.Control{Arms: []contract.Arm{
		{Name: "try", Instructions: []contract.InternalInstruction{parallel}, Output: &contract.Operand{Literal: json.RawMessage(`1`)}},
		{Name: "finally"},
	}}}, output("pay"))
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "refuse", Workflow: "control", ArtifactDigest: controlArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		t.Fatal(err)
	}
	token, err := j.TakeRunLease(ctx, admitted.RunID, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	_, err = runner.RunJournaled(ctx, nested, engineValue{Value: 4}, admitted.RunID, j.ForRun(admitted.RunID, token))
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "durable_control_unsupported" {
		t.Fatalf("err=%v; want durable_control_unsupported", err)
	}
	if got := controlRows(t, j, admitted.RunID); !reflect.DeepEqual(got, []string{"run|accepted"}) {
		t.Fatalf("rows %q; want nothing written", got)
	}
}

// TestRunJournalScopes pins the engine's scope journal on SQLite: a
// re-entry returns the decision recorded first and fences the earlier
// attempt; a scope recorded under another kind or parent is a conflict;
// a completed scope comes back completed; a canceled one is final and
// permanent; and every write is under the run lease.
func TestRunJournalScopes(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "scopes.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "scopes", Workflow: "control", ArtifactDigest: controlArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		t.Fatal(err)
	}
	run := admitted.RunID
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	rj := j.ForRun(run, token)
	digest, _ := engine.InputDigest(engineValue{Value: 4})
	if err := rj.VerifyRun(ctx, run, controlArtifact, digest); err != nil {
		t.Fatal(err)
	}
	route := engine.ScopeIdentity{RunID: run, ArtifactDigest: controlArtifact, Path: "route@root", Kind: "if"}
	first, err := rj.EnterScope(ctx, route, json.RawMessage(`{"arm":"then"}`))
	if err != nil || first.Completed || first.AttemptID == "" || string(first.Decision) != `{"arm":"then"}` {
		t.Fatalf("first entry %+v err=%v", first, err)
	}
	again, err := rj.EnterScope(ctx, route, json.RawMessage(`{"arm":"else"}`))
	if err != nil || string(again.Decision) != `{"arm":"then"}` || again.AttemptID == first.AttemptID {
		t.Fatalf("re-entry %+v err=%v; want the recorded decision under a new attempt", again, err)
	}
	if err := rj.ExitScope(ctx, first, json.RawMessage(`1`)); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("exit by the superseded attempt: err=%v; want ErrStaleAttempt", err)
	}
	for _, other := range []engine.ScopeIdentity{
		{RunID: run, ArtifactDigest: controlArtifact, Path: "route@root", Kind: "choose"},
		{RunID: run, ArtifactDigest: controlArtifact, Path: "route@root", Kind: "if", ParentPath: "pay@root"},
	} {
		if _, err := rj.EnterScope(ctx, other, json.RawMessage(`{"arm":"then"}`)); !errors.Is(err, ErrRequestConflict) || !Permanent(err) {
			t.Fatalf("entry as %+v: err=%v; want a permanent ErrRequestConflict", other, err)
		}
	}
	if err := rj.ExitScope(ctx, again, make(json.RawMessage, MaxStepResultBytes+1)); !errors.Is(err, ErrStepResultLimit) {
		t.Fatalf("oversize result: err=%v", err)
	}
	if err := rj.ExitScope(ctx, again, json.RawMessage(`{"value":50}`)); err != nil {
		t.Fatal(err)
	}
	if done, err := rj.EnterScope(ctx, route, json.RawMessage(`{"arm":"then"}`)); err != nil || !done.Completed || done.AttemptID != "" {
		t.Fatalf("entry of a completed scope %+v err=%v", done, err)
	}
	pay := engine.ScopeIdentity{RunID: run, ArtifactDigest: controlArtifact, Path: "pay@root", Kind: "try-finally"}
	if _, err := rj.EnterScope(ctx, pay, nil); err != nil {
		t.Fatal(err)
	}
	if err := j.CancelScope(ctx, run, "pay@root", "operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := rj.EnterScope(ctx, pay, nil); !errors.Is(err, ErrRecordFinal) || !Permanent(err) {
		t.Fatalf("entry of a canceled scope: err=%v; want a permanent ErrRecordFinal", err)
	}
	// An operator ends the run (CancelRun takes no lease) while this
	// execution still holds its lease and is inside a construct: from then
	// on the execution can neither enter a construct nor exit the one it is
	// in (#404 Review R round 1b).
	open, err := rj.EnterScope(ctx, engine.ScopeIdentity{RunID: run, ArtifactDigest: controlArtifact, Path: "open@root", Kind: "if"}, json.RawMessage(`{"arm":"then"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := j.CancelRun(ctx, run, "operator"); err != nil {
		t.Fatal(err)
	}
	if _, err := rj.EnterScope(ctx, engine.ScopeIdentity{RunID: run, ArtifactDigest: controlArtifact, Path: "after@root", Kind: "if"}, json.RawMessage(`{"arm":"then"}`)); !errors.Is(err, ErrRunNotActive) {
		t.Fatalf("entry after the run was canceled: err=%v; want ErrRunNotActive", err)
	}
	if err := rj.ExitScope(ctx, open, json.RawMessage(`1`)); !errors.Is(err, ErrRunNotActive) {
		t.Fatalf("exit after the run was canceled: err=%v; want ErrRunNotActive", err)
	}
	if got := oneRow(t, database, `SELECT state || '|' || error_text FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'open@root'`); got != "canceled|operator" {
		t.Fatalf("the open scope is %s; want canceled|operator", got)
	}
	if n := oneRow(t, database, `SELECT COUNT(*) FROM journal_scopes WHERE run_id = '`+run+`' AND path = 'after@root'`); n != "0" {
		t.Fatalf("an entry after the run ended wrote a scope")
	}
	if err := j.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	if _, err := rj.EnterScope(ctx, engine.ScopeIdentity{RunID: run, ArtifactDigest: controlArtifact, Path: "late@root", Kind: "if"}, json.RawMessage(`{"arm":"then"}`)); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("entry after the lease was released: err=%v; want ErrLeaseLost", err)
	}
}

// TestDurableUnfollowableDecisionSettlesFailed (#404 Review R round 1): a
// recorded decision its construct cannot follow (planted here after the
// kill: an arm the if does not have) is permanent, so a runner settles
// the run failed with journal_scope_conflict instead of retrying it
// forever; no arm runs.
func TestDurableUnfollowableDecisionSettlesFailed(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	path, log := killControlChild(t, "branch", 0)
	plant := func(j *Journal, run string) {
		if err := j.database.WithTx(context.Background(), func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE journal_scopes SET input_json = CAST('{"arm":"nowhere"}' AS BLOB) WHERE run_id = ? AND path = 'route@root'`, run)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	j, run, _, rj, err := resumeControl(t, path, log, "branch", plant)
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "journal_scope_conflict" || !errors.Is(err, engine.ErrScopeConflict) || !Permanent(err) {
		t.Fatalf("err=%v permanent=%v; want a permanent journal_scope_conflict", err, Permanent(err))
	}
	// What a runner does with a permanent error (internal/resumer, #384).
	if err := rj.FailRun(context.Background(), classified.Code, "conflict"); err != nil {
		t.Fatal(err)
	}
	if got := oneRow(t, j.database, `SELECT state || '|' || error_code || '|' || error_class FROM journal_runs WHERE run_id = '`+run+`'`); got != "failed|journal_scope_conflict|conflict" {
		t.Fatalf("run %s; want failed|journal_scope_conflict|conflict", got)
	}
	if got := oneRow(t, j.database, `SELECT state || '|' || error_text FROM journal_scopes WHERE run_id = '`+run+`'`); got != "canceled|journal_scope_conflict" {
		t.Fatalf("scope %s", got)
	}
	if got, want := invocations(t, log), map[string]int{"charge": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations %v; want %v (no arm)", got, want)
	}
}

// TestOperatorCanEndARunKilledMidArm (#404 Review R round 1): a run
// killed inside an arm is left inside its construct's scope. An operator
// can still cancel it or fail it (Journal.CancelRun, Journal.FailRun, as
// before control flow was durable), and its scopes end canceled with it.
func TestOperatorCanEndARunKilledMidArm(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CONTROL_CHILD") == "1" {
		runControlChild()
		return
	}
	for _, c := range []struct {
		name string
		end  func(*Journal, string) error
		want []string
	}{
		{"cancel", func(j *Journal, run string) error { return j.CancelRun(context.Background(), run, "operator") },
			[]string{`route@root|if||canceled|{"arm":"then"}`, "charge@root|committed", "route/then/vip@root|committed", "run|canceled"}},
		{"fail", func(j *Journal, run string) error {
			return j.FailRun(context.Background(), run, "operator_failed", "operator")
		}, []string{`route@root|if||canceled|{"arm":"then"}`, "charge@root|committed", "route/then/vip@root|committed", "run|failed"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			path, _ := killControlChild(t, "branch", 1)
			database, j := newJournalAtPath(t, path, Config{Holder: "operator", Clock: ticking(fixtureBase.Add(time.Hour))})
			defer database.Close()
			run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`)
			if err := c.end(j, run); err != nil {
				t.Fatalf("ending a run killed mid-arm: %v", err)
			}
			if got := controlRows(t, j, run); !reflect.DeepEqual(got, c.want) {
				t.Fatalf("rows:\n got %q\nwant %q", got, c.want)
			}
		})
	}
}
