package journal

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// childBindingJournal admits one run per key under the given principal,
// all under the artifact fencingRun registered, each with a checkpoint so
// its child records can be read back.
func childBindingJournal(t *testing.T, runs map[string]string) (*Journal, map[string]string) {
	t.Helper()
	ctx := context.Background()
	j, _ := fencingRun(t, "child-binding")
	ids := map[string]string{}
	for key, principal := range runs {
		run, err := j.Admit(ctx, AdmissionRequest{Principal: principal, RequestKey: key, Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
		ids[key] = run.RunID
	}
	return j, ids
}

func childRecords(t *testing.T, j *Journal, runID string) []ChildRecord {
	t.Helper()
	return recovered(t, j, runID).Children
}

// #372: a parent run can only record, as its child, a run admitted for its
// own principal. A Child construct reads the child's output by run id, so
// a cross-principal child record would hand one principal another's
// result.
func TestChildRecordIsBoundToTheParentsPrincipal(t *testing.T) {
	ctx := context.Background()
	j, run := childBindingJournal(t, map[string]string{"parent": "alice", "same": "alice", "other": "bob", "system": ""})
	for name, record := range map[string]ChildRecord{
		"another principal's running child":   {RunID: run["parent"], Path: "child/other", ChildRunID: run["other"], State: childRunning},
		"another principal's completed child": {RunID: run["parent"], Path: "child/other", ChildRunID: run["other"], State: childCompleted, Result: []byte(`{"stolen":true}`)},
		"a run with no principal as child":    {RunID: run["parent"], Path: "child/system", ChildRunID: run["system"], State: childRunning},
		"a principal's run as a system child": {RunID: run["system"], Path: "child/alice", ChildRunID: run["same"], State: childRunning},
	} {
		err := j.RecordChild(ctx, record)
		if !errors.Is(err, ErrChildPrincipalMismatch) {
			t.Errorf("%s: err=%v, want ErrChildPrincipalMismatch", name, err)
		}
		if !Permanent(err) {
			t.Errorf("%s: err=%v is not permanent", name, err)
		}
		if got := childRecords(t, j, record.RunID); len(got) != 0 {
			t.Errorf("%s: recorded %+v", name, got)
		}
	}
	// Control: the same principal's child is recorded, runs and completes.
	if err := j.RecordChild(ctx, ChildRecord{RunID: run["parent"], Path: "child/same", ChildRunID: run["same"], State: childRunning}); err != nil {
		t.Fatalf("same-principal running child: %v", err)
	}
	done := ChildRecord{RunID: run["parent"], Path: "child/same", ChildRunID: run["same"], State: childCompleted, Result: []byte(`{"ok":true}`)}
	if err := j.RecordChild(ctx, done); err != nil {
		t.Fatalf("same-principal child completed: %v", err)
	}
	if got := childRecords(t, j, run["parent"]); len(got) != 1 || got[0].ChildRunID != run["same"] || got[0].State != childCompleted || string(got[0].Result) != `{"ok":true}` {
		t.Fatalf("same-principal child=%+v", got)
	}
	// Two runs with no principal (the system principal) bind as well.
	other, err := j.Admit(ctx, AdmissionRequest{RequestKey: "system-child", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: run["system"], Path: "child/system", ChildRunID: other.RunID, State: childRunning}); err != nil {
		t.Fatalf("system-principal child: %v", err)
	}
}

// #372: a run is never its own ancestor. Recovery walks a run's children,
// and a Child construct waits for its child's result: a cycle would be a
// run waiting for itself.
func TestChildRecordRefusesCycles(t *testing.T) {
	ctx := context.Background()
	j, run := childBindingJournal(t, map[string]string{"a": "alice", "b": "alice", "c": "alice", "d": "alice"})
	if err := j.RecordChild(ctx, ChildRecord{RunID: run["a"], Path: "self", ChildRunID: run["a"], State: childRunning}); !errors.Is(err, ErrChildCycle) || !Permanent(err) {
		t.Errorf("a run as its own child: err=%v, want permanent ErrChildCycle", err)
	}
	if got := childRecords(t, j, run["a"]); len(got) != 0 {
		t.Errorf("self-reference recorded: %+v", got)
	}
	// a -> b -> c, and a -> d.
	for _, edge := range [][2]string{{"a", "b"}, {"b", "c"}, {"a", "d"}} {
		if err := j.RecordChild(ctx, ChildRecord{RunID: run[edge[0]], Path: "child/" + edge[1], ChildRunID: run[edge[1]], State: childRunning}); err != nil {
			t.Fatalf("%s -> %s: %v", edge[0], edge[1], err)
		}
	}
	for _, edge := range [][2]string{{"b", "a"}, {"c", "a"}, {"c", "b"}} {
		err := j.RecordChild(ctx, ChildRecord{RunID: run[edge[0]], Path: "child/" + edge[1], ChildRunID: run[edge[1]], State: childCompleted, Result: []byte(`{}`)})
		if !errors.Is(err, ErrChildCycle) || !Permanent(err) {
			t.Errorf("%s -> %s closes a cycle: err=%v, want permanent ErrChildCycle", edge[0], edge[1], err)
		}
		if got := childRecords(t, j, run[edge[0]]); len(got) != 0 && got[0].ChildRunID == run[edge[1]] {
			t.Errorf("%s -> %s recorded: %+v", edge[0], edge[1], got)
		}
	}
	// Sharing a descendant is not a cycle: d -> c makes c a child of two
	// runs (a diamond a -> b -> c, a -> d -> c), and c's own child is free.
	if err := j.RecordChild(ctx, ChildRecord{RunID: run["d"], Path: "child/c", ChildRunID: run["c"], State: childRunning}); err != nil {
		t.Fatalf("d -> c (diamond): %v", err)
	}
	e, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "e", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: run["c"], Path: "child/e", ChildRunID: e.RunID, State: childRunning}); err != nil {
		t.Fatalf("c -> e: %v", err)
	}
}

// #372: a journal written before the cycle check may already hold a cycle.
// The walk visits each run once, so it still ends: a parent outside the
// cycle records a run inside it as its child, and a record that would
// close another cycle through it is refused.
func TestChildCycleCheckEndsOnARecordedCycle(t *testing.T) {
	ctx := context.Background()
	j, run := childBindingJournal(t, map[string]string{"a": "alice", "b": "alice", "c": "alice", "d": "alice"})
	execAll(t, j.database,
		`INSERT INTO journal_children (run_id, path, child_run_id, state) VALUES ('`+run["a"]+`', 'child/b', '`+run["b"]+`', 'running')`,
		`INSERT INTO journal_children (run_id, path, child_run_id, state) VALUES ('`+run["b"]+`', 'child/a', '`+run["a"]+`', 'running')`)
	done := make(chan [3]error, 1)
	go func() {
		// d is not below a, so the walk down from a goes round a -> b -> a
		// and finds nothing.
		outside := j.RecordChild(ctx, ChildRecord{RunID: run["d"], Path: "child/a", ChildRunID: run["a"], State: childRunning})
		below := j.RecordChild(ctx, ChildRecord{RunID: run["a"], Path: "child/c", ChildRunID: run["c"], State: childRunning})
		closing := j.RecordChild(ctx, ChildRecord{RunID: run["c"], Path: "child/b", ChildRunID: run["b"], State: childRunning})
		done <- [3]error{outside, below, closing}
	}()
	select {
	case errs := <-done:
		if errs[0] != nil {
			t.Errorf("d -> a, a in a recorded cycle: %v", errs[0])
		}
		if errs[1] != nil {
			t.Errorf("a -> c: %v", errs[1])
		}
		if !errors.Is(errs[2], ErrChildCycle) {
			t.Errorf("c -> b, b an ancestor of c through the recorded cycle: err=%v, want ErrChildCycle", errs[2])
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the cycle check did not end on a recorded cycle")
	}
}

// #372: a completed child's result is JSON, as a scope's output is. It
// used to be stored as given.
func TestChildRecordResultMustBeJSON(t *testing.T) {
	ctx := context.Background()
	j, run := childBindingJournal(t, map[string]string{"parent": "alice", "child": "alice"})
	for name, result := range map[string][]byte{"invalid JSON": []byte(`{"ok":`), "no result": nil} {
		if err := j.RecordChild(ctx, ChildRecord{RunID: run["parent"], Path: "child/new/" + name, ChildRunID: run["child"], State: childCompleted, Result: result}); err == nil {
			t.Errorf("new child completed with %s: accepted", name)
		}
	}
	if got := childRecords(t, j, run["parent"]); len(got) != 0 {
		t.Errorf("invalid child result recorded: %+v", got)
	}
	for name, result := range map[string][]byte{"invalid JSON": []byte(`not json`), "no result": nil} {
		path := "child/running/" + name
		if err := j.RecordChild(ctx, ChildRecord{RunID: run["parent"], Path: path, ChildRunID: run["child"], State: childRunning}); err != nil {
			t.Fatal(err)
		}
		if err := j.RecordChild(ctx, ChildRecord{RunID: run["parent"], Path: path, ChildRunID: run["child"], State: childCompleted, Result: result}); err == nil {
			t.Errorf("running child completed with %s: accepted", name)
		}
	}
	for _, got := range childRecords(t, j, run["parent"]) {
		if strings.HasPrefix(got.Path, "child/running/") && (got.State != childRunning || len(got.Result) != 0) {
			t.Errorf("running child changed by an invalid result: %+v", got)
		}
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: run["parent"], Path: "child/0", ChildRunID: run["child"], State: childRunning}); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: run["parent"], Path: "child/0", ChildRunID: run["child"], State: childCompleted, Result: []byte(`null`)}); err != nil {
		t.Fatalf("child completed with JSON null: %v", err)
	}
}
