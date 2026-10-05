package journal

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/store/sqlite"
)

func TestReconciliationIsAuthorizedIdempotentAndRepairsUncertainEffect(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "reconcile.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	j, err := New(context.Background(), database, Config{Audit: newTestAudit(t, database)})
	if err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "reconcile", Workflow: "payments", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	op, err := j.BeginEffect(context.Background(), EffectIntent{Identity: OperationIdentity{RunID: run.RunID, ArtifactDigest: "sha256:artifact", InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := j.StartAttempt(context.Background(), op.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkUncertain(context.Background(), op.Key, attempt.ID, "provider timeout after success"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Reconcile(context.Background(), op.Key, "operator", "provider lookup receipt-1", []byte(`{"charged":true}`), false); !errors.Is(err, ErrReconciliationDenied) {
		t.Fatalf("unauthorized=%v", err)
	}
	var group sync.WaitGroup
	results := make(chan Reconciliation, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			result, err := j.Reconcile(context.Background(), op.Key, "operator", "provider lookup receipt-1", []byte(`{"charged":true}`), true)
			results <- result
			errs <- err
		}()
	}
	group.Wait()
	close(results)
	close(errs)
	var accepted int
	for result := range results {
		if !result.Duplicate {
			accepted++
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if accepted != 1 {
		t.Fatalf("accepted reconciliations=%d, want one", accepted)
	}
	restored, err := j.Operation(context.Background(), op.Key)
	if err != nil || restored.State != operationCommitted {
		t.Fatalf("operation=%+v err=%v", restored, err)
	}
}

func TestArtifactAvailabilityAndUpgradePoliciesProtectAcceptedRuns(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "upgrade.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	j, err := New(context.Background(), database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	manifest := []byte(`{"name":"orders","version":"1.0.0"}`)
	if err := j.RegisterArtifact(context.Background(), ArtifactRecord{Digest: "sha256:old", Version: "1.0.0", ManifestJSON: manifest}); err != nil {
		t.Fatal(err)
	}
	if err := j.RegisterArtifact(context.Background(), ArtifactRecord{Digest: "sha256:new", Version: "2.0.0", ManifestJSON: []byte(`{"name":"orders","version":"2.0.0"}`)}); err != nil {
		t.Fatal(err)
	}
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: "upgrade", Workflow: "orders", ArtifactDigest: "sha256:old", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.PlanUpgrade(context.Background(), "sha256:old", "sha256:new", false); !errors.Is(err, ErrUpgradeWouldDiscard) {
		t.Fatalf("discarding upgrade=%v", err)
	}
	plan, err := j.PlanUpgrade(context.Background(), "sha256:old", "sha256:new", true)
	if err != nil || plan.AffectedRuns != 1 || !plan.RetainRuns {
		t.Fatalf("plan=%+v err=%v", plan, err)
	}
	if err := j.CancelRun(context.Background(), run.RunID, "operator requested cancellation"); err != nil {
		t.Fatal(err)
	}
	if err := j.RequireArtifact(context.Background(), "sha256:missing"); !errors.Is(err, ErrArtifactMissing) {
		t.Fatalf("missing=%v", err)
	}
}
