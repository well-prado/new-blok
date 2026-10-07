package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/store/sqlite"
)

func TestRecoveryReusesCompletedNestedPathsAndRejectsChangedArtifact(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "recovery.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	j, err := New(context.Background(), database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.RegisterArtifact(context.Background(), ArtifactRecord{Digest: "sha256:artifact", Version: "1.0.0", ManifestJSON: []byte(`{"name":"nested","version":"1.0.0"}`)}); err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "recovery", Workflow: "nested", ArtifactDigest: "sha256:artifact", Input: []byte(`{"items":[1,2]}`)})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:artifact", CheckpointDigest: "sha256:checkpoint", State: []byte(`{"pc":"parallel.join","completed":["each/0"]}`)}
	if err := j.SaveCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatal(err)
	}
	started, err := j.StartScope(context.Background(), ScopeRecord{RunID: run.RunID, Path: "parallel/0/each/0", Kind: "each", ParentPath: "parallel/0"})
	if err != nil || started.AlreadyCompleted || started.AttemptID == "" {
		t.Fatalf("start=%+v err=%v", started, err)
	}
	if err := j.CompleteScope(context.Background(), run.RunID, "parallel/0/each/0", started.AttemptID, []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	child, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "recovery-child", Workflow: "nested", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.RecordChild(context.Background(), ChildRecord{RunID: run.RunID, Path: "child/0", ChildRunID: child.RunID, State: childCompleted, Result: []byte(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordJoin(context.Background(), JoinRecord{RunID: run.RunID, Path: "parallel/0", Expected: 2, Completed: 2, Results: []json.RawMessage{[]byte(`1`), []byte(`2`)}}); err != nil {
		t.Fatal(err)
	}
	recovered, err := j.Recover(context.Background(), run.RunID, checkpoint.ArtifactDigest, checkpoint.CheckpointDigest)
	if err != nil || len(recovered.Scopes) != 1 || recovered.Scopes[0].State != checkpointCompleted || len(recovered.Children) != 1 || len(recovered.Joins) != 1 || recovered.Joins[0].State != joinCompleted {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	again, err := j.StartScope(context.Background(), ScopeRecord{RunID: run.RunID, Path: "parallel/0/each/0", Kind: "each"})
	if err != nil || !again.AlreadyCompleted || again.AttemptID != "" {
		t.Fatalf("completed scope redispatched: start=%+v err=%v", again, err)
	}
	if _, err := j.Recover(context.Background(), run.RunID, "sha256:changed", checkpoint.CheckpointDigest); !errors.Is(err, ErrArtifactMismatch) {
		t.Fatalf("artifact mismatch=%v", err)
	}
}

func TestCancellationNeverClaimsEffectWasUndone(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "cancel.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	j, err := New(context.Background(), database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.RegisterArtifact(context.Background(), ArtifactRecord{Digest: "sha256:artifact", Version: "1.0.0", ManifestJSON: []byte(`{"name":"effect","version":"1.0.0"}`)}); err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "cancel", Workflow: "effect", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	started, err := j.StartScope(context.Background(), ScopeRecord{RunID: run.RunID, Path: "charge", Kind: "call"})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.SaveCheckpoint(context.Background(), Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:artifact", CheckpointDigest: "sha256:checkpoint", State: []byte(`{"pc":"charge"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := j.CancelScope(context.Background(), run.RunID, "charge", "caller canceled"); err != nil {
		t.Fatal(err)
	}
	recovered, err := j.Recover(context.Background(), run.RunID, "sha256:artifact", "sha256:checkpoint")
	if err != nil {
		t.Fatal(err)
	}
	_ = recovered
	if err := j.CompleteScope(context.Background(), run.RunID, "charge", started.AttemptID, []byte(`{"charged":false}`)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled effect was completed: %v", err)
	}
}

func TestRecoveryBlocksMissingArtifact(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "missing-artifact.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	j, err := New(context.Background(), database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "missing-artifact", Workflow: "orders", ArtifactDigest: "sha256:missing", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.SaveCheckpoint(context.Background(), Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:missing", CheckpointDigest: "sha256:checkpoint", State: []byte(`{"pc":"start"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Recover(context.Background(), run.RunID, "sha256:missing", "sha256:checkpoint"); !errors.Is(err, ErrArtifactMissing) {
		t.Fatalf("missing artifact recovery error=%v", err)
	}
}

// fencingRun admits a run under sha256:admitted, with sha256:other also
// registered, and saves its checkpoint so Recover can read its records.
func fencingRun(t *testing.T, name string) (*Journal, string) {
	t.Helper()
	ctx := context.Background()
	database, j := newJournal(t, name+".db", Config{})
	t.Cleanup(func() { _ = database.Close() })
	for _, digest := range []string{"sha256:admitted", "sha256:other"} {
		if err := j.RegisterArtifact(ctx, ArtifactRecord{Digest: digest, Version: "1.0.0", ManifestJSON: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: name, Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	return j, run.RunID
}

func recovered(t *testing.T, j *Journal, runID string) Recovery {
	t.Helper()
	recovery, err := j.Recover(context.Background(), runID, "sha256:admitted", "sha256:codec")
	if err != nil {
		t.Fatal(err)
	}
	return recovery
}

// slots builds a join's results, one per branch; "" is an empty slot.
func slots(values ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(values))
	for i, value := range values {
		if value != "" {
			out[i] = json.RawMessage(value)
		}
	}
	return out
}

// storedSlots reads a join's stored slots back through Recover, "" for an
// empty one.
func storedSlots(t *testing.T, j *Journal, runID, path string) (JoinRecord, []string) {
	t.Helper()
	for _, join := range recovered(t, j, runID).Joins {
		if join.Path != path {
			continue
		}
		values := make([]string, len(join.Results))
		for i, slot := range join.Results {
			if string(slot) != "null" {
				values[i] = string(slot)
			}
		}
		return join, values
	}
	t.Fatalf("join %s not stored", path)
	return JoinRecord{}, nil
}

// P2 (#334, D6): a completed join is final. Re-recording a completed 2-of-2
// join as 1 of 3 used to store expected=2 completed=1 state=running.
func TestCompletedJoinCannotRegress(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "join-final")
	done := JoinRecord{RunID: runID, Path: "parallel/0", Expected: 2, Completed: 2, Results: slots(`1`, `2`)}
	if err := j.RecordJoin(ctx, done); err != nil {
		t.Fatal(err)
	}
	for name, write := range map[string]JoinRecord{
		"1 of 3":       {RunID: runID, Path: "parallel/0", Expected: 3, Completed: 1, Results: slots(`9`, ``, ``)},
		"other result": {RunID: runID, Path: "parallel/0", Expected: 2, Completed: 2, Results: slots(`1`, `9`)},
	} {
		if err := j.RecordJoin(ctx, write); !errors.Is(err, ErrRecordFinal) {
			t.Errorf("%s over a completed join: err=%v, want ErrRecordFinal", name, err)
		}
	}
	unchanged := func(when string) {
		t.Helper()
		join, values := storedSlots(t, j, runID, "parallel/0")
		if join.Expected != 2 || join.Completed != 2 || join.State != joinCompleted || !slices.Equal(values, []string{`1`, `2`}) {
			t.Fatalf("completed join changed %s: %+v", when, join)
		}
	}
	unchanged("by the refused writes")
	if err := j.RecordJoin(ctx, done); err != nil {
		t.Fatalf("identical retry of the completing join: %v", err)
	}
	unchanged("by the identical retry")
}

// A join's results are positional (#334 review): one slot per expected
// branch, null while the branch has not come back, and Completed counts the
// filled slots. A filled slot never changes; Expected is fixed at creation.
func TestJoinSlotsAreFilledOnce(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "join-slots")
	two := JoinRecord{RunID: runID, Path: "parallel/0", Expected: 3, Completed: 2, Results: slots(`1`, `2`, ``)}
	if err := j.RecordJoin(ctx, two); err != nil {
		t.Fatal(err)
	}
	for name, write := range map[string]JoinRecord{
		// The #370 review's probe: completing it with other values used to
		// store [7,8,9].
		"filled slots changed":      {RunID: runID, Path: "parallel/0", Expected: 3, Completed: 3, Results: slots(`7`, `8`, `9`)},
		"one filled slot changed":   {RunID: runID, Path: "parallel/0", Expected: 3, Completed: 2, Results: slots(`1`, `8`, ``)},
		"expected changed":          {RunID: runID, Path: "parallel/0", Expected: 4, Completed: 3, Results: slots(`1`, `2`, `3`, ``)},
		"same count, other results": {RunID: runID, Path: "parallel/0", Expected: 3, Completed: 2, Results: slots(`7`, `8`, ``)},
	} {
		if err := j.RecordJoin(ctx, write); !errors.Is(err, ErrRecordConflict) {
			t.Errorf("%s: err=%v, want ErrRecordConflict", name, err)
		}
	}
	if join, values := storedSlots(t, j, runID, "parallel/0"); join.Completed != 2 || join.State != joinRunning || !slices.Equal(values, []string{`1`, `2`, ``}) {
		t.Fatalf("running join changed: %+v", join)
	}
	// Writes that bring nothing new change nothing: an identical retry, and
	// a writer that knows fewer branches than are stored.
	for name, write := range map[string]JoinRecord{
		"identical retry": two,
		"fewer branches":  {RunID: runID, Path: "parallel/0", Expected: 3, Completed: 1, Results: slots(`1`, ``, ``)},
	} {
		if err := j.RecordJoin(ctx, write); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if join, values := storedSlots(t, j, runID, "parallel/0"); join.Completed != 2 || !slices.Equal(values, []string{`1`, `2`, ``}) {
		t.Fatalf("join changed by a write with nothing new: %+v", join)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: "parallel/0", Expected: 3, Completed: 3, Results: slots(`1`, `2`, `3`)}); err != nil {
		t.Fatalf("moving forward: %v", err)
	}
	if join, values := storedSlots(t, j, runID, "parallel/0"); join.Completed != 3 || join.State != joinCompleted || !slices.Equal(values, []string{`1`, `2`, `3`}) {
		t.Fatalf("join did not complete: %+v", join)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: "run:missing", Path: "parallel/0", Expected: 1}); !errors.Is(err, ErrNotFound) {
		t.Errorf("join of an unknown run: err=%v, want ErrNotFound", err)
	}
}

// A join record that does not describe its own slots is refused and
// nothing is stored: results of another length than Expected, a Completed
// that is not the number of filled slots, or a State the counts contradict.
// No results at all is Expected empty slots.
func TestJoinRecordMustDescribeItsSlots(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "join-shape")
	for name, write := range map[string]JoinRecord{
		"fewer slots than expected": {RunID: runID, Path: "parallel/bad", Expected: 3, Completed: 2, Results: slots(`1`, `2`)},
		"more slots than expected":  {RunID: runID, Path: "parallel/bad", Expected: 1, Completed: 1, Results: slots(`1`, `2`)},
		"completed above filled":    {RunID: runID, Path: "parallel/bad", Expected: 3, Completed: 3, Results: slots(`1`, ``, ``)},
		"completed below filled":    {RunID: runID, Path: "parallel/bad", Expected: 3, Completed: 1, Results: slots(`1`, `2`, ``)},
		"completed without results": {RunID: runID, Path: "parallel/bad", Expected: 2, Completed: 1},
		"state contradicts counts":  {RunID: runID, Path: "parallel/bad", Expected: 2, Completed: 1, Results: slots(`1`, ``), State: joinCompleted},
	} {
		if err := j.RecordJoin(ctx, write); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if joins := recovered(t, j, runID).Joins; len(joins) != 0 {
		t.Fatalf("a malformed join was stored: %+v", joins)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: "parallel/0", Expected: 2}); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: "parallel/0", Expected: 2, Results: []json.RawMessage{}}); err != nil {
		t.Errorf("an empty retry of an empty join: %v", err)
	}
	if join, values := storedSlots(t, j, runID, "parallel/0"); join.Completed != 0 || !slices.Equal(values, []string{``, ``}) {
		t.Fatalf("empty join stored as %+v", join)
	}
}

// Branches come back in any order, and each fills only its own slot
// (#334 review): slot 2, then slot 0, then the rest.
func TestJoinFillsSlotsOutOfOrder(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "join-order")
	for _, write := range [][]string{{``, ``, `"c"`}, {`"a"`, ``, ``}, {``, `"b"`, ``}} {
		filled := 0
		for _, value := range write {
			if value != "" {
				filled++
			}
		}
		if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: "parallel/0", Expected: 3, Completed: filled, Results: slots(write...)}); err != nil {
			t.Fatalf("filling %q: %v", write, err)
		}
	}
	if join, values := storedSlots(t, j, runID, "parallel/0"); join.Completed != 3 || join.State != joinCompleted || !slices.Equal(values, []string{`"a"`, `"b"`, `"c"`}) {
		t.Fatalf("join = %+v %q", join, values)
	}
}

// Two journal handles on one database fill a join concurrently (#334
// review): branches filling different slots both succeed, and of two
// filling the same slot with different values one wins and the other is
// ErrRecordConflict.
func TestConcurrentJoinFillsFromTwoHandles(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "join-handles.db")
	first, j := newJournalAtPath(t, path, Config{})
	defer first.Close()
	second, k := newJournalAtPath(t, path, Config{})
	defer second.Close()
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "join-handles", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	race := func(path string, a, b []string) (error, error) {
		t.Helper()
		if err := j.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: path, Expected: 2}); err != nil {
			t.Fatal(err)
		}
		errs := make([]error, 2)
		var group sync.WaitGroup
		for i, write := range []struct {
			journal *Journal
			values  []string
		}{{j, a}, {k, b}} {
			group.Add(1)
			go func() {
				defer group.Done()
				errs[i] = write.journal.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: path, Expected: 2, Completed: 1, Results: slots(write.values...)})
			}()
		}
		group.Wait()
		return errs[0], errs[1]
	}
	stored := func(path string) []string {
		var values []string
		if err := first.WithTx(ctx, func(tx *sql.Tx) error {
			var encoded []byte
			if err := tx.QueryRowContext(ctx, `SELECT results_json FROM journal_joins WHERE run_id = ? AND path = ?`, run.RunID, path).Scan(&encoded); err != nil {
				return err
			}
			var raw []json.RawMessage
			if err := json.Unmarshal(encoded, &raw); err != nil {
				return err
			}
			for _, slot := range raw {
				values = append(values, string(slot))
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		return values
	}
	for round := range 10 {
		path := fmt.Sprintf("different/%d", round)
		if a, b := race(path, []string{`"A"`, ``}, []string{``, `"B"`}); a != nil || b != nil {
			t.Fatalf("branches filling different slots: %v, %v", a, b)
		}
		if got := stored(path); !slices.Equal(got, []string{`"A"`, `"B"`}) {
			t.Fatalf("different slots stored %q", got)
		}
		path = fmt.Sprintf("same/%d", round)
		a, b := race(path, []string{`"A"`, ``}, []string{`"Z"`, ``})
		if (a == nil) == (b == nil) || !errors.Is(errors.Join(a, b), ErrRecordConflict) {
			t.Fatalf("branches filling the same slot: %v, %v; want one winner and one ErrRecordConflict", a, b)
		}
		if got := stored(path); !slices.Equal(got, []string{`"A"`, `null`}) && !slices.Equal(got, []string{`"Z"`, `null`}) {
			t.Fatalf("same slot stored %q", got)
		}
	}
}

// A slot is compared in its compact JSON form, the form it is stored in
// (#370 review): repeating a slot written with spaces, or with characters
// json.Marshal escapes (<, >, &), is the same slot, so a writer that resends
// the slots it already knows while filling a new one is not refused.
func TestJoinSlotsCompareInCompactForm(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "join-compact")
	for _, first := range []string{`{"a": 1}`, `"<b>"`, `"x&y"`, " [1, 2] "} {
		path := "parallel/" + first
		if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: path, Expected: 2, Completed: 1, Results: slots(first, ``)}); err != nil {
			t.Fatalf("seeding %s: %v", first, err)
		}
		if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: path, Expected: 2, Completed: 2, Results: slots(first, `2`)}); err != nil {
			t.Errorf("resending %s while filling the next slot: %v", first, err)
		}
		if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: path, Expected: 2, Completed: 2, Results: slots(first, `2`)}); err != nil {
			t.Errorf("retrying the completing write with %s: %v", first, err)
		}
		if join, _ := storedSlots(t, j, runID, path); join.Completed != 2 || join.State != joinCompleted {
			t.Errorf("join with %s = %+v", first, join)
		}
	}
	// The compact form still tells different values apart: key order and
	// number spelling are not normalised.
	if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: "parallel/order", Expected: 2, Completed: 1, Results: slots(`{"a":1,"b":2}`, ``)}); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: "parallel/order", Expected: 2, Completed: 2, Results: slots(`{"b":2,"a":1}`, `2`)}); !errors.Is(err, ErrRecordConflict) {
		t.Errorf("reordered keys: err=%v, want ErrRecordConflict", err)
	}
}

// Rows the pre-#334 upsert wrote can hold what the writes no longer
// produce: a join or child in a state other than running or completed, or
// a join whose results are not one slot per branch. They are refused, not
// overwritten, and an identical write of such a join still succeeds.
func TestLegacyJoinAndChildRowsAreNotOverwritten(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "legacy-records.db", Config{})
	defer database.Close()
	if err := j.RegisterArtifact(ctx, ArtifactRecord{Digest: "sha256:admitted", Version: "1.0.0", ManifestJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "legacy", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	child, err := j.Admit(ctx, AdmissionRequest{RequestKey: "legacy-child", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	execAll(t, database,
		`INSERT INTO journal_joins (run_id, path, expected, completed, results_json, state) VALUES ('`+run.RunID+`', 'failed', 2, 1, '["1",null]', 'failed')`,
		`INSERT INTO journal_joins (run_id, path, expected, completed, results_json, state) VALUES ('`+run.RunID+`', 'packed', 3, 1, '[1]', 'running')`,
		`INSERT INTO journal_joins (run_id, path, expected, completed, results_json, state) VALUES ('`+run.RunID+`', 'miscounted', 2, 2, '[1,null]', 'running')`,
		`INSERT INTO journal_joins (run_id, path, expected, completed, results_json, state) VALUES ('`+run.RunID+`', 'nil-results', 2, 0, 'null', 'running')`,
		`INSERT INTO journal_children (run_id, path, child_run_id, state, result_json) VALUES ('`+run.RunID+`', 'child', '`+child.RunID+`', 'failed', NULL)`)
	if err := j.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: "failed", Expected: 2, Completed: 2, Results: slots(`"1"`, `"2"`)}); !errors.Is(err, ErrRecordConflict) {
		t.Errorf("completing a legacy failed join: err=%v, want ErrRecordConflict", err)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: "packed", Expected: 3, Completed: 2, Results: slots(`1`, `2`, ``)}); !errors.Is(err, ErrRecordConflict) {
		t.Errorf("extending a legacy packed join: err=%v, want ErrRecordConflict", err)
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: run.RunID, Path: "child", ChildRunID: child.RunID, State: childCompleted, Result: []byte(`{"ok":true}`)}); !errors.Is(err, ErrRecordConflict) {
		t.Errorf("completing a legacy failed child: err=%v, want ErrRecordConflict", err)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: "miscounted", Expected: 2, Completed: 2, Results: slots(`1`, `2`)}); !errors.Is(err, ErrRecordConflict) {
		t.Errorf("filling a legacy join whose count disagrees with its slots: err=%v, want ErrRecordConflict", err)
	}
	// A write that brings nothing new changes nothing on a row that reads
	// as slots, whatever its state; a row that does not read as slots
	// refuses it.
	if err := j.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: "failed", Expected: 2}); err != nil {
		t.Errorf("an empty write to a legacy failed join: %v", err)
	}
	for _, path := range []string{"packed", "miscounted"} {
		if err := j.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: path, Expected: map[string]int{"packed": 3, "miscounted": 2}[path]}); !errors.Is(err, ErrRecordConflict) {
			t.Errorf("an empty write to the legacy %s join: err=%v, want ErrRecordConflict", path, err)
		}
	}
	// A join the pre-#334 upsert stored with nil results (results_json
	// 'null') is read as its empty slots, so it can still be filled.
	if err := j.RecordJoin(ctx, JoinRecord{RunID: run.RunID, Path: "nil-results", Expected: 2, Completed: 1, Results: slots(``, `"b"`)}); err != nil {
		t.Errorf("filling a legacy join stored with nil results: %v", err)
	}
	got := recovered(t, j, run.RunID)
	states := map[string]string{}
	for _, join := range got.Joins {
		states[join.Path] = fmt.Sprintf("%s %d/%d %s", join.State, join.Completed, join.Expected, join.Results)
	}
	if states["failed"] != `failed 1/2 ["1" null]` || states["packed"] != `running 1/3 [1]` || states["miscounted"] != `running 2/2 [1 null]` || states["nil-results"] != `running 1/2 [null "b"]` || len(got.Children) != 1 || got.Children[0].State != "failed" || len(got.Children[0].Result) != 0 {
		t.Fatalf("legacy rows changed: joins=%v children=%+v", states, got.Children)
	}
}

// P3 and P3b (#334, D6): a child record keeps the child run it was created
// with, never leaves completed, and must name a run that exists.
func TestChildRecordIsBoundToItsChildRun(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "child-bound")
	var children []string
	for _, key := range []string{"child-a", "child-b"} {
		child, err := j.Admit(ctx, AdmissionRequest{RequestKey: key, Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		children = append(children, child.RunID)
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: runID, Path: "child/missing", ChildRunID: "run:does-not-exist"}); !errors.Is(err, ErrChildRunNotFound) {
		t.Errorf("child pointing at a missing run: err=%v, want ErrChildRunNotFound", err)
	}
	if got := recovered(t, j, runID).Children; len(got) != 0 {
		t.Errorf("missing child was recorded: %+v", got)
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: runID, Path: "child/0", ChildRunID: children[1], State: childRunning}); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: runID, Path: "child/0", ChildRunID: children[0], State: childRunning}); !errors.Is(err, ErrRecordConflict) {
		t.Errorf("running child re-pointed: err=%v, want ErrRecordConflict", err)
	}
	// A running child has no result: written as running with one, it is
	// refused whether it would create the record or update it, and neither
	// completes it nor changes it (#334 review).
	if err := j.RecordChild(ctx, ChildRecord{RunID: runID, Path: "child/0", ChildRunID: children[1], State: childRunning, Result: []byte(`{"early":true}`)}); err == nil {
		t.Errorf("running child given a result: accepted")
	}
	if err := j.RecordChild(ctx, ChildRecord{RunID: runID, Path: "child/early", ChildRunID: children[1], State: childRunning, Result: []byte(`{"early":true}`)}); err == nil {
		t.Errorf("running child created with a result: accepted")
	}
	if got := recovered(t, j, runID).Children; len(got) != 1 || got[0].State != childRunning || len(got[0].Result) != 0 {
		t.Errorf("running child changed: %+v", got)
	}
	done := ChildRecord{RunID: runID, Path: "child/0", ChildRunID: children[1], State: childCompleted, Result: []byte(`{"ok":true}`)}
	if err := j.RecordChild(ctx, done); err != nil {
		t.Fatal(err)
	}
	for name, stale := range map[string]struct {
		record ChildRecord
		want   error
	}{
		"other child":     {ChildRecord{RunID: runID, Path: "child/0", ChildRunID: children[0], State: childCompleted, Result: []byte(`{"ok":true}`)}, ErrRecordConflict},
		"back to running": {ChildRecord{RunID: runID, Path: "child/0", ChildRunID: children[1], State: childRunning}, ErrRecordFinal},
		"other result":    {ChildRecord{RunID: runID, Path: "child/0", ChildRunID: children[1], State: childCompleted, Result: []byte(`{"ok":false}`)}, ErrRecordFinal},
	} {
		if err := j.RecordChild(ctx, stale.record); !errors.Is(err, stale.want) {
			t.Errorf("%s: err=%v, want %v", name, err, stale.want)
		}
	}
	unchanged := func(when string) {
		t.Helper()
		got := recovered(t, j, runID).Children
		if len(got) != 1 || got[0].Path != "child/0" || got[0].ChildRunID != children[1] || got[0].State != childCompleted || string(got[0].Result) != `{"ok":true}` {
			t.Fatalf("completed child changed %s: %+v", when, got)
		}
	}
	unchanged("by the refused writes")
	if err := j.RecordChild(ctx, done); err != nil {
		t.Fatalf("identical retry: %v", err)
	}
	unchanged("by the identical retry")
	if err := j.RecordChild(ctx, ChildRecord{RunID: "run:missing", Path: "child/0", ChildRunID: children[0]}); !errors.Is(err, ErrNotFound) {
		t.Errorf("child of an unknown parent: err=%v, want ErrNotFound", err)
	}
}

// P4 (#334, D7): a checkpoint names the artifact its run was admitted
// under. Saving one for another artifact used to succeed, and so did
// Recover(run, other).
func TestCheckpointIsBoundToTheAdmittedArtifact(t *testing.T) {
	ctx := context.Background()
	j, _ := fencingRun(t, "checkpoint-artifact")
	// A run's first checkpoint: nothing stored yet to compare it with.
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "first-checkpoint", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	other := Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:other", CheckpointDigest: "sha256:codec", State: []byte(`{"pc":"other"}`)}
	if err := j.SaveCheckpoint(ctx, other); !errors.Is(err, ErrArtifactMismatch) {
		t.Errorf("checkpoint for another artifact: err=%v, want ErrArtifactMismatch", err)
	}
	if _, err := j.Recover(ctx, run.RunID, "sha256:other", "sha256:codec"); err == nil {
		t.Errorf("recover under another artifact succeeded")
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); err != nil {
		t.Fatalf("checkpoint for the admitted artifact: %v", err)
	}
	if got := recovered(t, j, run.RunID).Checkpoint; got.ArtifactDigest != "sha256:admitted" || string(got.State) != `{}` {
		t.Fatalf("checkpoint = %+v", got)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: "run:missing", ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); !errors.Is(err, ErrNotFound) {
		t.Errorf("checkpoint of an unknown run: err=%v, want ErrNotFound", err)
	}
}

// A checkpoint row written before #334 under another artifact is not its
// run's to resume: Recover checks the run's admitted artifact too.
func TestRecoverRefusesALegacyCheckpointForAnotherArtifact(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "legacy-checkpoint.db", Config{})
	defer database.Close()
	for _, digest := range []string{"sha256:admitted", "sha256:other"} {
		if err := j.RegisterArtifact(ctx, ArtifactRecord{Digest: digest, Version: "1.0.0", ManifestJSON: []byte(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "legacy", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO journal_checkpoints (run_id, artifact_digest, checkpoint_digest, status, state_json, updated_at) VALUES (?, 'sha256:other', 'sha256:codec', 'running', '{}', 0)`, run.RunID)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Recover(ctx, run.RunID, "sha256:other", "sha256:codec"); !errors.Is(err, ErrArtifactMismatch) {
		t.Errorf("legacy checkpoint for another artifact recovered: err=%v", err)
	}
	// It fails closed: not recovered under the admitted artifact either,
	// and not replaceable by a checkpoint for it.
	if _, err := j.Recover(ctx, run.RunID, "sha256:admitted", "sha256:codec"); !errors.Is(err, ErrArtifactMismatch) {
		t.Errorf("legacy checkpoint recovered under the admitted artifact: err=%v", err)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); !errors.Is(err, ErrArtifactMismatch) {
		t.Errorf("legacy checkpoint replaced: err=%v", err)
	}
}

// P5 (#334, D7): a run that is no longer accepted (canceled, completed,
// failed or uncertain) takes no new scope or checkpoint.
func TestInactiveRunTakesNoScopeOrCheckpoint(t *testing.T) {
	ctx := context.Background()
	for name, end := range map[string]func(*Journal, string) error{
		"canceled":  func(j *Journal, runID string) error { return j.CancelRun(ctx, runID, "caller canceled") },
		"completed": func(j *Journal, runID string) error { return j.CompleteRun(ctx, runID, []byte(`{}`)) },
		"failed":    func(j *Journal, runID string) error { return j.FailRun(ctx, runID, "boom", "permanent") },
		"uncertain": func(j *Journal, runID string) error { return j.MarkRunUncertain(ctx, runID, "timeout", "transient") },
	} {
		t.Run(name, func(t *testing.T) {
			j, runID := fencingRun(t, "inactive-"+name)
			if err := end(j, runID); err != nil {
				t.Fatal(err)
			}
			if _, err := j.StartScope(ctx, ScopeRecord{RunID: runID, Path: "late", Kind: "call"}); !errors.Is(err, ErrRunNotActive) {
				t.Errorf("scope after the run ended %s: err=%v, want ErrRunNotActive", name, err)
			}
			if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: runID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{"pc":"late"}`)}); !errors.Is(err, ErrRunNotActive) {
				t.Errorf("checkpoint after the run ended %s: err=%v, want ErrRunNotActive", name, err)
			}
			got := recovered(t, j, runID)
			if len(got.Scopes) != 0 || string(got.Checkpoint.State) != `{}` {
				t.Fatalf("%s run changed: %+v", name, got)
			}
		})
	}
	j, _ := fencingRun(t, "inactive-unknown")
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: "run:missing", Path: "late", Kind: "call"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("scope of an unknown run: err=%v, want ErrNotFound", err)
	}
}

// A canceled scope is final (#334 review): StartScope used to restart it
// with a fresh attempt, which could then complete it, so Recover showed
// the canceled effect as completed with an output.
func TestCanceledScopeStaysCanceled(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "scope-canceled")
	started, err := j.StartScope(ctx, ScopeRecord{RunID: runID, Path: "charge", Kind: "call"})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.CancelScope(ctx, runID, "charge", "caller canceled"); err != nil {
		t.Fatal(err)
	}
	restarted, err := j.StartScope(ctx, ScopeRecord{RunID: runID, Path: "charge", Kind: "call"})
	if !errors.Is(err, ErrRecordFinal) {
		t.Errorf("restarting a canceled scope: start=%+v err=%v, want ErrRecordFinal", restarted, err)
	}
	for _, attempt := range []string{started.AttemptID, restarted.AttemptID} {
		if attempt == "" {
			continue
		}
		if err := j.CompleteScope(ctx, runID, "charge", attempt, []byte(`{"charged":true}`)); !errors.Is(err, ErrNotFound) {
			t.Errorf("completing a canceled scope: err=%v, want ErrNotFound", err)
		}
	}
	scopes := recovered(t, j, runID).Scopes
	if len(scopes) != 1 || scopes[0].State != checkpointCanceled || len(scopes[0].Output) != 0 || scopes[0].Error != "caller canceled" {
		t.Fatalf("canceled scope changed: %+v", scopes)
	}
}

// An empty attempt id can never be the current attempt: it is refused
// with ErrStaleAttempt, not an untyped argument error (#334 review).
func TestCompleteScopeWithoutAttemptIsStale(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "scope-no-attempt")
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: runID, Path: "each/0", Kind: "each"}); err != nil {
		t.Fatal(err)
	}
	if err := j.CompleteScope(ctx, runID, "each/0", "", []byte(`{}`)); !errors.Is(err, ErrStaleAttempt) {
		t.Errorf("completing without an attempt: err=%v, want ErrStaleAttempt", err)
	}
	if scopes := recovered(t, j, runID).Scopes; len(scopes) != 1 || scopes[0].State != checkpointRunning {
		t.Fatalf("scope changed: %+v", scopes)
	}
}

// P1 (#334, D5): a committed scope output is final. A second CompleteScope
// used to overwrite {"v":"first"} with {"v":"stale-second"} and return nil.
func TestCompletedScopeOutputIsFinal(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "scope-final")
	started, err := j.StartScope(ctx, ScopeRecord{RunID: runID, Path: "each/0", Kind: "each"})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.CompleteScope(ctx, runID, "each/0", started.AttemptID, []byte(`{"v":"first"}`)); err != nil {
		t.Fatal(err)
	}
	if err := j.CompleteScope(ctx, runID, "each/0", started.AttemptID, []byte(`{"v":"stale-second"}`)); !errors.Is(err, ErrRecordFinal) {
		t.Errorf("overwriting a completed scope: err=%v, want ErrRecordFinal", err)
	}
	if err := j.CompleteScope(ctx, runID, "each/0", started.AttemptID, []byte(`{"v":"first"}`)); err != nil {
		t.Fatalf("identical retry of the completing attempt: %v", err)
	}
	if scopes := recovered(t, j, runID).Scopes; len(scopes) != 1 || scopes[0].State != checkpointCompleted || string(scopes[0].Output) != `{"v":"first"}` {
		t.Fatalf("completed scope changed: %+v", scopes)
	}
}

// An attempt superseded by a later StartScope cannot complete the scope,
// even across a restart (the journal reopened on the same file), and cannot
// replace the output the current attempt committed, even with the same
// bytes.
func TestSupersededScopeAttemptIsFenced(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "scope-fenced.db")
	database, j := newJournalAtPath(t, path, Config{})
	if err := j.RegisterArtifact(ctx, ArtifactRecord{Digest: "sha256:admitted", Version: "1.0.0", ManifestJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "fenced", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	first, err := j.StartScope(ctx, ScopeRecord{RunID: run.RunID, Path: "each/0", Kind: "each"})
	if err != nil {
		t.Fatal(err)
	}
	_ = database.Close()
	database, j = newJournalAtPath(t, path, Config{})
	defer database.Close()
	second, err := j.StartScope(ctx, ScopeRecord{RunID: run.RunID, Path: "each/0", Kind: "each"})
	if err != nil || second.AlreadyCompleted || second.AttemptID == "" || second.AttemptID == first.AttemptID {
		t.Fatalf("recovery attempt: first=%+v second=%+v err=%v", first, second, err)
	}
	if err := j.CompleteScope(ctx, run.RunID, "each/0", first.AttemptID, []byte(`{"v":"stale"}`)); !errors.Is(err, ErrStaleAttempt) {
		t.Errorf("superseded attempt completed a running scope: err=%v, want ErrStaleAttempt", err)
	}
	if err := j.CompleteScope(ctx, run.RunID, "each/0", second.AttemptID, []byte(`{"v":"current"}`)); err != nil {
		t.Fatal(err)
	}
	if err := j.CompleteScope(ctx, run.RunID, "each/0", first.AttemptID, []byte(`{"v":"current"}`)); !errors.Is(err, ErrRecordFinal) {
		t.Errorf("superseded attempt re-completed a completed scope: err=%v, want ErrRecordFinal", err)
	}
	if err := j.CompleteScope(ctx, run.RunID, "missing", second.AttemptID, []byte(`{}`)); !errors.Is(err, ErrNotFound) {
		t.Errorf("unknown scope: err=%v, want ErrNotFound", err)
	}
	recovery, err := j.Recover(ctx, run.RunID, "sha256:admitted", "sha256:codec")
	if err != nil || len(recovery.Scopes) != 1 || string(recovery.Scopes[0].Output) != `{"v":"current"}` {
		t.Fatalf("recovered=%+v err=%v", recovery, err)
	}
}

// A journal a version-4 binary (after #332, before #334) wrote has no scope
// attempts. Opening it raises it to version 5 and adds the column, and a
// scope that binary left running has the empty attempt, which no caller
// holds: completing it without an attempt is ErrStaleAttempt, and only a
// fresh StartScope can complete it.
func TestVersion4JournalGainsScopeAttempts(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal-4.db")
	database, j := newJournalAtPath(t, path, Config{})
	if err := j.RegisterArtifact(ctx, ArtifactRecord{Digest: "sha256:admitted", Version: "1.0.0", ManifestJSON: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "journal-4", Workflow: "nested", ArtifactDigest: "sha256:admitted", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	// Make it the database a version-4 binary leaves: no attempt column, a
	// running scope, stamp 4.
	execAll(t, database,
		`ALTER TABLE journal_scopes DROP COLUMN attempt_id`,
		`INSERT INTO journal_scopes (run_id, path, kind, parent_path, state, updated_at) VALUES ('`+run.RunID+`', 'each/0', 'each', '', 'running', 0)`,
		`UPDATE blok_schema_versions SET version = 4, upgraded_from = 3 WHERE component = 'journal'`)
	database.Close()

	database, j = newJournalAtPath(t, path, Config{})
	defer database.Close()
	if got := oneRow(t, database, `SELECT version || '|' || upgraded_from FROM blok_schema_versions WHERE component = 'journal'`); got != "7|4" {
		t.Fatalf("journal stamp=%s; want 7 (#332's 6 and 7 after #334's 5) upgraded from 4", got)
	}
	if err := j.CompleteScope(ctx, run.RunID, "each/0", "", []byte(`{"v":"guessed"}`)); !errors.Is(err, ErrStaleAttempt) {
		t.Errorf("completing a pre-#334 scope without an attempt: err=%v, want ErrStaleAttempt", err)
	}
	if scopes := recovered(t, j, run.RunID).Scopes; len(scopes) != 1 || scopes[0].State != checkpointRunning || len(scopes[0].Output) != 0 {
		t.Fatalf("pre-#334 scope changed: %+v", scopes)
	}
	started, err := j.StartScope(ctx, ScopeRecord{RunID: run.RunID, Path: "each/0", Kind: "each"})
	if err != nil || started.AttemptID == "" {
		t.Fatalf("start=%+v err=%v", started, err)
	}
	if err := j.CompleteScope(ctx, run.RunID, "each/0", started.AttemptID, []byte(`{"v":"current"}`)); err != nil {
		t.Fatal(err)
	}
}
