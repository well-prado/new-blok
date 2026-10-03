package journal

import (
	"context"
	"errors"
	"testing"
)

func TestRetainedInventoryIncludesAdmissionsAndTerminalCheckpoints(t *testing.T) {
	db, j := newJournal(t, "inventory.db", Config{})
	defer db.Close()
	ctx := context.Background()
	if err := j.RegisterArtifact(ctx, ArtifactRecord{Digest: "artifact-a", Version: "1.0.0", ManifestJSON: []byte(`{"fixture":true}`)}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"active", "completed", "missing"} {
		identity := "artifact-a"
		if key == "missing" {
			identity = "unavailable"
		}
		run, err := j.Admit(ctx, AdmissionRequest{RequestKey: key, Workflow: "orders", ArtifactDigest: identity, Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if key == "completed" {
			if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: identity, CheckpointDigest: "codec-v1", State: []byte(`{}`)}); err != nil {
				t.Fatal(err)
			}
			if err := j.CompleteRun(ctx, run.RunID, []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
		}
	}
	count, checkpoints, missing := 0, 0, 0
	if err := j.RetainedArtifacts(ctx, func(item RetainedArtifact) error {
		count++
		if item.CheckpointPresent {
			checkpoints++
		}
		if len(item.ManifestJSON) == 0 {
			missing++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 3 || checkpoints != 1 || missing != 1 {
		t.Fatalf("inventory: runs=%d checkpoints=%d missing=%d", count, checkpoints, missing)
	}
	denied := errors.New("visitor rejected inventory")
	if err := j.RetainedArtifacts(ctx, func(RetainedArtifact) error { return denied }); !errors.Is(err, denied) {
		t.Fatalf("visitor error: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := j.RetainedArtifacts(canceled, func(RetainedArtifact) error { t.Fatal("visited canceled inventory"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled: %v", err)
	}
	if err := j.RetainedArtifacts(ctx, nil); err == nil {
		t.Fatal("nil visitor accepted")
	}
}
