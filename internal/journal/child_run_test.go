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
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/engine"
)

// Durable child runs on a single host (#333 slice 4, ADR 0028): a parent's
// child step admits the child run once, suspends at the child's wait, and
// continues with the child's outcome once the child ends.

const kidArtifact = "sha256:0000000000000000000000000000000000000000000000000000000000000334"

// parentProgram charges, then runs a child of workflow kid on the charge;
// its output is the child's result.
func parentProgram() contract.InternalProgram {
	return controlProgramOf(call(0, "charge", "charge"), contract.InternalInstruction{ID: "kid", Kind: "child", Node: "kid", References: []contract.Reference{{Step: "charge"}}}, output("kid"))
}

// kidProgram runs body (vip, boom or unsure) on its input; with nested set
// it starts a child of its own first.
func kidProgram(body string, nested bool) contract.InternalProgram {
	instructions := []contract.InternalInstruction{call(0, body, body)}
	if nested {
		instructions = append([]contract.InternalInstruction{{ID: "grandkid", Kind: "child", Node: "kid"}}, instructions...)
	}
	program := controlProgramOf(append(instructions, output(body))...)
	program.WorkflowID, program.Digest = "kid", kidArtifact
	return program
}

// childWorld is a parent run, its journal and its nodes, driven as the
// resumer drives them: a run is executed under a lease, then settled.
type childWorld struct {
	t      *testing.T
	ctx    context.Context
	j      *Journal
	nodes  *controlNodes
	kid    contract.InternalProgram
	parent string
	now    time.Time
}

func (w *childWorld) resolve(name string) (ChildWorkflow, bool) {
	if name != "kid" {
		return ChildWorkflow{}, false
	}
	return ChildWorkflow{ArtifactDigest: kidArtifact, DecodeInput: func(raw json.RawMessage) (any, error) {
		var in engineValue
		err := json.Unmarshal(raw, &in)
		return in, err
	}}, true
}

// execute runs runID once under a fresh lease and settles it as the
// resumer would; it returns the execution's error.
func (w *childWorld) execute(runID string) error {
	w.t.Helper()
	w.now = w.now.Add(time.Hour)
	token, err := w.j.TakeRunLease(w.ctx, runID, w.now)
	if err != nil {
		w.t.Fatalf("lease %s: %v", runID, err)
	}
	defer func() { _ = w.j.ReleaseRunLease(w.ctx, runID, token) }()
	run, err := w.j.Run(w.ctx, runID)
	if err != nil {
		w.t.Fatal(err)
	}
	program, input := parentProgram(), any(engineValue{Value: 4})
	if runID != w.parent {
		var in engineValue
		if err := json.Unmarshal(run.Input, &in); err != nil {
			w.t.Fatal(err)
		}
		program, input = w.kid, in
	}
	rj := w.j.ForRun(runID, token).WithChildren(w.resolve)
	result, runErr := w.nodes.engine().RunJournaled(w.ctx, program, input, runID, rj)
	var classified *engine.Error
	switch {
	case runErr == nil:
		output, _ := json.Marshal(result.Output)
		if err := rj.CompleteRun(w.ctx, output); err != nil {
			w.t.Fatalf("complete %s: %v", runID, err)
		}
	case errors.As(runErr, &classified) && classified.Suspended:
	case errors.As(runErr, &classified) && classified.Uncertain:
		if err := rj.MarkRunUncertain(w.ctx, classified.Code, classified.Class); err != nil {
			w.t.Fatal(err)
		}
	case errors.As(runErr, &classified):
		if err := rj.FailRun(w.ctx, classified.Code, classified.Class); err != nil {
			w.t.Fatalf("fail %s: %v", runID, err)
		}
	}
	return runErr
}

// drive executes the parent and its child until the parent has ended.
func (w *childWorld) drive() {
	w.t.Helper()
	for step := 0; step < 8; step++ {
		parent, err := w.j.Run(w.ctx, w.parent)
		if err != nil {
			w.t.Fatal(err)
		}
		if parent.State != runAccepted {
			return
		}
		if kid := w.child(); kid != "" {
			if run, err := w.j.Run(w.ctx, kid); err == nil && run.State == runAccepted {
				_ = w.execute(kid)
				continue
			}
		}
		_ = w.execute(w.parent)
	}
	w.t.Fatal("the parent did not end")
}

// child is the parent's child run, if it started one.
func (w *childWorld) child() string {
	rows := waitRows(w.t, w.j.database, `SELECT child_run_id FROM journal_children WHERE run_id = '`+w.parent+`'`)
	if len(rows) == 0 {
		return ""
	}
	return rows[0]
}

func (w *childWorld) runRows() []string {
	return waitRows(w.t, w.j.database, `SELECT workflow || '|' || state || '|' || error_code || '|' || COALESCE(CAST(output_json AS TEXT), '') FROM journal_runs ORDER BY rowid`)
}

func newChildWorld(t *testing.T, j *Journal, kid contract.InternalProgram, log string) *childWorld {
	t.Helper()
	ctx := context.Background()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "crash", Principal: "alice", Workflow: "control", ArtifactDigest: controlArtifact, Input: []byte(`{"value":4}`)})
	if err != nil {
		t.Fatal(err)
	}
	return &childWorld{t: t, ctx: ctx, j: j, nodes: &controlNodes{log: log, released: make(chan struct{})}, kid: kid, parent: admitted.RunID, now: fixtureBase}
}

// TestChildRunCompletesThroughItsParent: the parent suspends at its child,
// the child runs under the parent's principal and completes, its end fires
// the parent's wait, and the parent continues with the child's result.
func TestChildRunCompletesThroughItsParent(t *testing.T) {
	database, j := newJournal(t, "child.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	w := newChildWorld(t, j, kidProgram("vip", false), filepath.Join(t.TempDir(), "nodes.log"))
	if err := w.execute(w.parent); !suspended(err) {
		t.Fatalf("parent err=%v; want suspended at its child", err)
	}
	kid := w.child()
	run, err := j.Run(w.ctx, kid)
	if err != nil || run.Principal != "alice" || run.Workflow != "kid" || string(run.Input) != `{"value":5}` || !IsChildRun(run) {
		t.Fatalf("child run %+v err=%v", run, err)
	}
	if listed, err := j.ChildrenToStart(w.ctx, w.parent); err != nil || !reflect.DeepEqual(listed, []string{kid}) {
		t.Fatalf("children to start %v err=%v", listed, err)
	}
	w.drive()
	if got, want := w.runRows(), []string{"control|completed||{\"value\":50}", "kid|completed||{\"value\":50}"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("runs %q; want %q", got, want)
	}
	if got := oneRow(t, database, `SELECT state || '|' || CAST(result_json AS TEXT) FROM journal_children WHERE run_id = '`+w.parent+`'`); got != `completed|{"state":"completed","output":{"value":50}}` {
		t.Fatalf("child record %s", got)
	}
	if got := oneRow(t, database, `SELECT path || '|' || kind || '|' || state || '|' || CAST(input_json AS TEXT) FROM journal_scopes WHERE run_id = '`+w.parent+`'`); got != `kid@root|child|completed|{"child":"`+kid+`"}` {
		t.Fatalf("child scope %s", got)
	}
	if got, want := invocations(t, w.nodes.log), map[string]int{"charge": 1, "vip": 1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("invocations %v; want %v", got, want)
	}
}

// TestChildFailureAndUncertaintyReachTheParent: a child that fails makes
// its parent's step fail (child_failed); a child that ends uncertain makes
// it uncertain (child_uncertain), and the parent ends uncertain.
func TestChildFailureAndUncertaintyReachTheParent(t *testing.T) {
	for _, c := range []struct {
		body, kidRow, parentRow string
	}{
		{"boom", "kid|failed|node_error|", "control|failed|child_failed|"},
		{"unsure", "kid|uncertain|effect_outcome_uncertain|", "control|uncertain|child_uncertain|"},
	} {
		t.Run(c.body, func(t *testing.T) {
			database, j := newJournal(t, "child.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
			defer database.Close()
			w := newChildWorld(t, j, kidProgram(c.body, false), filepath.Join(t.TempDir(), "nodes.log"))
			w.drive()
			if got, want := w.runRows(), []string{c.parentRow, c.kidRow}; !reflect.DeepEqual(got, want) {
				t.Fatalf("runs %q; want %q", got, want)
			}
		})
	}
}

// TestChildDepthIsBounded: with MaxChildDepth 1, a child that starts a
// child of its own is refused before anything is admitted
// (child_depth_exceeded, permanent), so it fails, and so does its parent.
func TestChildDepthIsBounded(t *testing.T) {
	database, j := newJournal(t, "depth.db", Config{Holder: "a", Clock: ticking(fixtureBase), MaxChildDepth: 1})
	defer database.Close()
	w := newChildWorld(t, j, kidProgram("vip", true), filepath.Join(t.TempDir(), "nodes.log"))
	if err := w.execute(w.parent); !suspended(err) {
		t.Fatalf("parent err=%v", err)
	}
	err := w.execute(w.child())
	var classified *engine.Error
	if !errors.As(err, &classified) || classified.Code != "child_depth_exceeded" || classified.Class != "admission" || !Permanent(err) {
		t.Fatalf("child err=%v; want a permanent child_depth_exceeded", err)
	}
	w.drive()
	if got, want := w.runRows(), []string{"control|failed|child_failed|", "kid|failed|child_depth_exceeded|"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("runs %q; want %q (no grandchild admitted)", got, want)
	}
}

// TestChildOccupiedIdIsUnavailableWhoeverHoldsIt (#397): a run already
// holding the child's derived id that the step never recorded is refused
// the same way, code and message, whether another principal admitted it or
// this one: the workflow learns nothing about other principals' runs.
func TestChildOccupiedIdIsUnavailableWhoeverHoldsIt(t *testing.T) {
	diagnostic := func(principal string) string {
		database, j := newJournal(t, "occupied.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
		defer database.Close()
		w := newChildWorld(t, j, kidProgram("vip", false), filepath.Join(t.TempDir(), "nodes.log"))
		runID, requestKey := childIdentity(w.parent, "kid@root", 1)
		if err := database.WithTx(w.ctx, func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO journal_runs (run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, created_at, principal) VALUES (?, ?, 'kid', ?, '{}', 'x', 'accepted', 0, ?)`, runID, requestKey, kidArtifact, principal)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		err := w.execute(w.parent)
		var classified *engine.Error
		if !errors.As(err, &classified) || !Permanent(err) {
			t.Fatalf("occupant of %s: err=%v; want a permanent refusal", principal, err)
		}
		return classified.Code + "|" + classified.Class + "|" + classified.Error()
	}
	foreign, own := diagnostic("mallory"), diagnostic("alice")
	if foreign != own || foreign != "child_unavailable|conflict|child_unavailable: the child run is not available" {
		t.Fatalf("diagnostics differ or changed:\n foreign %q\n     own %q", foreign, own)
	}
}

// TestChildWaitsRefuseSignals: only a child's end fires its parent's wait;
// a signal addressed to it is refused and fires nothing.
func TestChildWaitsRefuseSignals(t *testing.T) {
	database, j := newJournal(t, "signal.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	w := newChildWorld(t, j, kidProgram("vip", false), filepath.Join(t.TempDir(), "nodes.log"))
	if err := w.execute(w.parent); !suspended(err) {
		t.Fatalf("parent err=%v", err)
	}
	name := engine.ChildWaitPrefix + w.child()
	if _, err := j.Signal(w.ctx, signal.Envelope{RunID: w.parent, SignalID: "forged", Name: name, Principal: "alice", Payload: []byte(`{"state":"completed","output":{"value":1}}`)}, true); !errors.Is(err, ErrReservedSignal) {
		t.Fatalf("forged child outcome: err=%v; want ErrReservedSignal", err)
	}
	if got := oneRow(t, database, `SELECT state FROM journal_waits WHERE run_id = '`+w.parent+`'`); got != "waiting" {
		t.Fatalf("the parent's wait is %s; want waiting", got)
	}
}

// TestChildRunKilledAtEveryBoundary kills the process driving a parent and
// its child at each boundary: just before and just after the commit that
// starts the child (admission, record, scope and wait in one transaction),
// while the child runs (after its effect committed), and after the child
// completed but before the parent resumed. In each case driving on in
// another process admits exactly one child, repeats no committed effect
// and completes both runs.
func TestChildRunKilledAtEveryBoundary(t *testing.T) {
	if os.Getenv("NEWBLOK_333_CHILD_CHILD") == "1" {
		runChildRunChild(t)
		return
	}
	for _, c := range []struct {
		name, phase string
		hook        string // BeforeCommit or AfterCommit
		runs        int    // runs the kill left
	}{
		{"before the child starts", "child-start", "before", 1},
		{"after the child starts", "child-start", "after", 2},
		{"while the child runs", "kid-step", "after", 2},
		{"after the child completes", "run-complete", "after", 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			directory := t.TempDir()
			path, marker, log := filepath.Join(directory, "journal.db"), filepath.Join(directory, "marker"), filepath.Join(directory, "nodes.log")
			command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$")
			command.Env = append(os.Environ(), "NEWBLOK_333_CHILD_CHILD=1", "NEWBLOK_333_CHILD_PHASE="+c.phase, "NEWBLOK_333_CHILD_HOOK="+c.hook, "NEWBLOK_333_CONTROL_LOG="+log, "NEWBLOK_JOURNAL_PATH="+path, "NEWBLOK_JOURNAL_MARKER="+marker)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForJournalMarker(t, marker)
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()
			database, j := newJournalAtPath(t, path, Config{Holder: "parent", Clock: ticking(fixtureBase.Add(24 * time.Hour))})
			defer database.Close()
			if got := len(waitRows(t, database, `SELECT run_id FROM journal_runs`)); got != c.runs {
				t.Fatalf("%d runs the kill left; want %d", got, c.runs)
			}
			w := &childWorld{t: t, ctx: context.Background(), j: j, nodes: &controlNodes{log: log, released: make(chan struct{})}, kid: kidProgram("vip", false),
				parent: oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'crash'`), now: fixtureBase.Add(24 * time.Hour)}
			w.drive()
			if got, want := w.runRows(), []string{"control|completed||{\"value\":50}", "kid|completed||{\"value\":50}"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("runs %q; want %q (exactly one child)", got, want)
			}
			if got, want := invocations(t, log), map[string]int{"charge": 1, "vip": 1}; !reflect.DeepEqual(got, want) {
				t.Fatalf("invocations across both processes %v; want %v", got, want)
			}
		})
	}
}

// runChildRunChild drives a parent and its child, parking at the phase's
// commit for the parent's SIGKILL: the commit that starts the child, the
// child's step commit, or the child's completion.
func runChildRunChild(t *testing.T) {
	phase, hook := os.Getenv("NEWBLOK_333_CHILD_PHASE"), os.Getenv("NEWBLOK_333_CHILD_HOOK")
	var kidRunning atomic.Bool
	park := func(when string) func(string) {
		return func(name string) {
			if when != hook {
				return
			}
			switch {
			case phase == "child-start" && name == "child-start",
				phase == "kid-step" && name == "step-commit" && kidRunning.Load(),
				phase == "run-complete" && name == "run-complete" && kidRunning.Load():
				journalMarkerAndWait()
			}
		}
	}
	database, j := newJournalAtPathForChild(os.Getenv("NEWBLOK_JOURNAL_PATH"), Config{Holder: "child", Clock: ticking(fixtureBase), Hooks: Hooks{BeforeCommit: park("before"), AfterCommit: park("after")}})
	defer database.Close()
	w := newChildWorld(t, j, kidProgram("vip", false), os.Getenv("NEWBLOK_333_CONTROL_LOG"))
	if err := w.execute(w.parent); !suspended(err) {
		panic(errors.Join(errors.New("the parent did not suspend"), err))
	}
	kidRunning.Store(true)
	_ = w.execute(w.child())
	panic("the execution was not parked")
}
