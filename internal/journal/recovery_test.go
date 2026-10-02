package journal

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
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
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "recovery", Workflow: "nested", ArtifactDigest: "sha256:artifact", Input: []byte(`{"items":[1,2]}`)})
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := Checkpoint{RunID: run.RunID, ArtifactDigest: "sha256:artifact", CheckpointDigest: "sha256:checkpoint", State: []byte(`{"pc":"parallel.join","completed":["each/0"]}`)}
	if err := j.SaveCheckpoint(context.Background(), checkpoint); err != nil {
		t.Fatal(err)
	}
	if done, err := j.StartScope(context.Background(), ScopeRecord{RunID: run.RunID, Path: "parallel/0/each/0", Kind: "each", ParentPath: "parallel/0"}); err != nil || done {
		t.Fatalf("start=%v err=%v", done, err)
	}
	if err := j.CompleteScope(context.Background(), run.RunID, "parallel/0/each/0", []byte(`{"value":1}`)); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordChild(context.Background(), ChildRecord{RunID: run.RunID, Path: "child/0", ChildRunID: "child-run", State: childCompleted, Result: []byte(`{"ok":true}`)}); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordJoin(context.Background(), JoinRecord{RunID: run.RunID, Path: "parallel/0", Expected: 2, Completed: 2, Results: []json.RawMessage{[]byte(`1`), []byte(`2`)}}); err != nil {
		t.Fatal(err)
	}
	recovered, err := j.Recover(context.Background(), run.RunID, checkpoint.ArtifactDigest, checkpoint.CheckpointDigest)
	if err != nil || len(recovered.Scopes) != 1 || recovered.Scopes[0].State != checkpointCompleted || len(recovered.Children) != 1 || len(recovered.Joins) != 1 || recovered.Joins[0].State != joinCompleted {
		t.Fatalf("recovered=%+v err=%v", recovered, err)
	}
	done, err := j.StartScope(context.Background(), ScopeRecord{RunID: run.RunID, Path: "parallel/0/each/0", Kind: "each"})
	if err != nil || !done {
		t.Fatalf("completed scope redispatched: done=%v err=%v", done, err)
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
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "cancel", Workflow: "effect", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StartScope(context.Background(), ScopeRecord{RunID: run.RunID, Path: "charge", Kind: "call"}); err != nil {
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
	if err := j.CompleteScope(context.Background(), run.RunID, "charge", []byte(`{"charged":false}`)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("canceled effect was completed: %v", err)
	}
}
