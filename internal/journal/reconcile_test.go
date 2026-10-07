package journal

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

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
	// The cancellation is read back: the run is canceled, and a canceled run
	// no longer holds its artifact (#340, mutation M48g).
	canceled, err := j.Run(context.Background(), run.RunID)
	if err != nil || canceled.State != runCanceled {
		t.Fatalf("after CancelRun run=%+v err=%v, want state %q", canceled, err, runCanceled)
	}
	if plan, err := j.PlanUpgrade(context.Background(), "sha256:old", "sha256:new", false); err != nil || plan.AffectedRuns != 0 {
		t.Fatalf("upgrade after cancel plan=%+v err=%v", plan, err)
	}
	if err := j.RequireArtifact(context.Background(), "sha256:missing"); !errors.Is(err, ErrArtifactMissing) {
		t.Fatalf("missing=%v", err)
	}
}

// newUpgradeJournal opens a journal with audit composed, so DecideUpgrade
// can record its decisions, and registers the old and new artifacts.
func newUpgradeJournal(t *testing.T) *Journal {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "upgrade.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	j, err := New(context.Background(), database, Config{Audit: newTestAudit(t, database)})
	if err != nil {
		t.Fatal(err)
	}
	for _, artifact := range []ArtifactRecord{
		{Digest: "sha256:old", Version: "1.0.0", ManifestJSON: []byte(`{"name":"orders","version":"1.0.0"}`)},
		{Digest: "sha256:new", Version: "2.0.0", ManifestJSON: []byte(`{"name":"orders","version":"2.0.0"}`)},
	} {
		if err := j.RegisterArtifact(context.Background(), artifact); err != nil {
			t.Fatal(err)
		}
	}
	return j
}

func admitUpgradeRun(t *testing.T, j *Journal, key, digest string) string {
	t.Helper()
	run, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: key, Workflow: "orders", ArtifactDigest: digest, Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	return run.RunID
}

// markRunUncertain leaves the run the way an unknown provider outcome does:
// its effect uncertain and the run itself projected as uncertain.
func markRunUncertain(t *testing.T, j *Journal, runID string) {
	t.Helper()
	op, err := j.BeginEffect(context.Background(), EffectIntent{Identity: OperationIdentity{RunID: runID, ArtifactDigest: "sha256:old", InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := j.StartAttempt(context.Background(), op.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkUncertain(context.Background(), op.Key, attempt.ID, "provider timeout after dispatch"); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkRunUncertain(context.Background(), runID, "provider_timeout", "uncertain"); err != nil {
		t.Fatal(err)
	}
}

// scheduleUpgradeWait is this file's only ScheduleWait call, so a change to
// the wait request's shape is a one-line edit here.
func scheduleUpgradeWait(t *testing.T, j *Journal, runID, waitID string) {
	t.Helper()
	if _, err := j.ScheduleWait(context.Background(), WaitRequest{RunID: runID, WaitID: waitID, Name: "approval", DueAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
}

func runState(t *testing.T, j *Journal, runID string) string {
	t.Helper()
	run, err := j.Run(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	return run.State
}

// requireUpgradeHeld checks both upgrade paths while affected runs on the
// old artifact are unfinished: discarding them is refused (the audited
// decision too), and retaining them reports exactly that many.
func requireUpgradeHeld(t *testing.T, j *Journal, affected int) {
	t.Helper()
	ctx := context.Background()
	if plan, err := j.PlanUpgrade(ctx, "sha256:old", "sha256:new", false); !errors.Is(err, ErrUpgradeWouldDiscard) {
		t.Fatalf("PlanUpgrade(discard) plan=%+v err=%v, want ErrUpgradeWouldDiscard", plan, err)
	}
	if plan, err := j.PlanUpgrade(ctx, "sha256:old", "sha256:new", true); err != nil || plan.AffectedRuns != affected || !plan.RetainRuns {
		t.Fatalf("PlanUpgrade(retain) plan=%+v err=%v, want AffectedRuns %d", plan, err, affected)
	}
	if plan, err := j.DecideUpgrade(ctx, "operator", "sha256:old", "sha256:new", false); !errors.Is(err, ErrUpgradeWouldDiscard) {
		t.Fatalf("DecideUpgrade(discard) plan=%+v err=%v, want ErrUpgradeWouldDiscard", plan, err)
	}
	if plan, err := j.DecideUpgrade(ctx, "operator", "sha256:old", "sha256:new", true); err != nil || plan.AffectedRuns != affected || !plan.RetainRuns {
		t.Fatalf("DecideUpgrade(retain) plan=%+v err=%v, want AffectedRuns %d", plan, err, affected)
	}
}

// Probe P6 (#340): an uncertain run still needs its artifact to be
// reconciled and recovered, so it is counted, and canceling it to make room
// is refused while its effect is uncertain.
func TestUpgradeCountsUncertainRunOnOldArtifact(t *testing.T) {
	j := newUpgradeJournal(t)
	runID := admitUpgradeRun(t, j, "uncertain", "sha256:old")
	markRunUncertain(t, j, runID)
	if state := runState(t, j, runID); state != runUncertain {
		t.Fatalf("run state=%q, want %q", state, runUncertain)
	}
	requireUpgradeHeld(t, j, 1)
	if err := j.CancelRun(context.Background(), runID, "make room for the upgrade"); !errors.Is(err, ErrUncertain) {
		t.Fatalf("CancelRun on an uncertain run err=%v, want ErrUncertain", err)
	}
	if state := runState(t, j, runID); state != runUncertain {
		t.Fatalf("run state after refused cancel=%q, want %q", state, runUncertain)
	}
	requireUpgradeHeld(t, j, 1)
}

// #48 V4 (#340): a run suspended on a wait is an accepted run with a waiting
// wait. It holds its artifact, it cannot be canceled while it waits, and
// once its wait is canceled CancelRun really cancels it, which releases the
// artifact.
func TestUpgradeCountsWaitingRunUntilItIsCanceled(t *testing.T) {
	j := newUpgradeJournal(t)
	ctx := context.Background()
	runID := admitUpgradeRun(t, j, "waiting", "sha256:old")
	scheduleUpgradeWait(t, j, runID, "wait-approval")
	if wait, err := j.Wait(ctx, "wait-approval"); err != nil || wait.RunID != runID || wait.State != waitWaiting {
		t.Fatalf("wait=%+v err=%v, want %q on run %s", wait, err, waitWaiting, runID)
	}
	requireUpgradeHeld(t, j, 1)
	if err := j.CancelRun(ctx, runID, "make room for the upgrade"); !errors.Is(err, ErrRunActiveWork) {
		t.Fatalf("CancelRun on a waiting run err=%v, want ErrRunActiveWork", err)
	}
	if state := runState(t, j, runID); state != runAccepted {
		t.Fatalf("run state after refused cancel=%q, want %q", state, runAccepted)
	}
	if err := j.CancelWait(ctx, "wait-approval"); err != nil {
		t.Fatal(err)
	}
	if err := j.CancelRun(ctx, runID, "make room for the upgrade"); err != nil {
		t.Fatal(err)
	}
	if state := runState(t, j, runID); state != runCanceled {
		t.Fatalf("run state after CancelRun=%q, want %q", state, runCanceled)
	}
	if plan, err := j.PlanUpgrade(ctx, "sha256:old", "sha256:new", false); err != nil || plan.AffectedRuns != 0 {
		t.Fatalf("PlanUpgrade(discard) after cancel plan=%+v err=%v", plan, err)
	}
	if plan, err := j.DecideUpgrade(ctx, "operator", "sha256:old", "sha256:new", false); err != nil || plan.AffectedRuns != 0 {
		t.Fatalf("DecideUpgrade(discard) after cancel plan=%+v err=%v", plan, err)
	}
}

// The count is every run on the old artifact that is not completed, failed
// or canceled, including a state this binary does not know (fail closed),
// and never a run on another artifact.
func TestUpgradeCountsOnlyUnfinishedRunsOnOldArtifact(t *testing.T) {
	j := newUpgradeJournal(t)
	ctx := context.Background()
	completed := admitUpgradeRun(t, j, "completed", "sha256:old")
	if err := j.CompleteRun(ctx, completed, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	failed := admitUpgradeRun(t, j, "failed", "sha256:old")
	if err := j.FailRun(ctx, failed, "step_failed", "permanent"); err != nil {
		t.Fatal(err)
	}
	canceled := admitUpgradeRun(t, j, "canceled", "sha256:old")
	if err := j.CancelRun(ctx, canceled, "operator requested cancellation"); err != nil {
		t.Fatal(err)
	}
	admitUpgradeRun(t, j, "on-new", "sha256:new")
	if plan, err := j.DecideUpgrade(ctx, "operator", "sha256:old", "sha256:new", false); err != nil || plan.AffectedRuns != 0 {
		t.Fatalf("terminal runs only: plan=%+v err=%v", plan, err)
	}

	admitUpgradeRun(t, j, "accepted", "sha256:old")
	waiting := admitUpgradeRun(t, j, "waiting", "sha256:old")
	scheduleUpgradeWait(t, j, waiting, "wait-mixed")
	markRunUncertain(t, j, admitUpgradeRun(t, j, "uncertain", "sha256:old"))
	unknown := admitUpgradeRun(t, j, "unknown", "sha256:old")
	if err := j.withTx(ctx, "test-unknown-state", func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE journal_runs SET state = 'paused-by-a-newer-binary' WHERE run_id = ?`, unknown)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	requireUpgradeHeld(t, j, 4)
}

// #48 V4 (#340): a running run, with an attempt dispatched and in flight,
// holds its artifact and cannot be canceled to make room.
func TestUpgradeCountsRunningRunWithAttemptInFlight(t *testing.T) {
	j := newUpgradeJournal(t)
	runID := admitUpgradeRun(t, j, "running", "sha256:old")
	op, err := j.BeginEffect(context.Background(), EffectIntent{Identity: OperationIdentity{RunID: runID, ArtifactDigest: "sha256:old", InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StartAttempt(context.Background(), op.Key); err != nil {
		t.Fatal(err)
	}
	requireUpgradeHeld(t, j, 1)
	if err := j.CancelRun(context.Background(), runID, "make room for the upgrade"); !errors.Is(err, ErrRunActiveWork) {
		t.Fatalf("CancelRun on a running run err=%v, want ErrRunActiveWork", err)
	}
	if state := runState(t, j, runID); state != runAccepted {
		t.Fatalf("run state after refused cancel=%q, want %q", state, runAccepted)
	}
	requireUpgradeHeld(t, j, 1)
}
