package journal

import (
	"context"
	"database/sql"
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
}

// P5 (#334, D7): a canceled run takes no new scope or checkpoint.
func TestCanceledRunTakesNoScopeOrCheckpoint(t *testing.T) {
	ctx := context.Background()
	j, runID := fencingRun(t, "canceled")
	if err := j.CancelRun(ctx, runID, "caller canceled"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: runID, Path: "late", Kind: "call"}); !errors.Is(err, ErrRunNotActive) {
		t.Errorf("scope after cancel: err=%v, want ErrRunNotActive", err)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: runID, ArtifactDigest: "sha256:admitted", CheckpointDigest: "sha256:codec", State: []byte(`{"pc":"late"}`)}); !errors.Is(err, ErrRunNotActive) {
		t.Errorf("checkpoint after cancel: err=%v, want ErrRunNotActive", err)
	}
	got := recovered(t, j, runID)
	if len(got.Scopes) != 0 || string(got.Checkpoint.State) != `{}` {
		t.Fatalf("canceled run changed: %+v", got)
	}
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: "run:missing", Path: "late", Kind: "call"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("scope of an unknown run: err=%v, want ErrNotFound", err)
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
