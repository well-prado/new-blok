package journal

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/inspect"
)

func TestJournalInspectionReadsActualWaitUncertainAndChildLineage(t *testing.T) {
	db, j := newJournal(t, "inspect.db", Config{})
	defer db.Close()
	ctx := context.Background()
	waiting, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-wait", Workflow: "approval", ArtifactDigest: "sha256:one", Input: []byte(`{"token":"must-not-leak"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = j.ScheduleWait(ctx, WaitRequest{RunID: waiting.RunID, WaitID: "wait-real", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	page, err := inspect.InspectSource(ctx, j, "alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true, inspection.FieldOutput: true}, MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: waiting.RunID})
	if err != nil || page.Run.Status != inspection.StatusSuspended || len(page.Steps) != 1 || page.Steps[0].Status != inspection.StatusSuspended {
		t.Fatalf("waiting projection=%+v err=%v", page, err)
	}
	if string(page.Run.Input) == "" || strings.Contains(string(page.Run.Input), "must-not-leak") {
		t.Fatalf("journal payload redaction failed: %s", page.Run.Input)
	}
	tooSmall := inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true}, MaxPayloadBytes: 40}
	if got, err := inspect.InspectSource(ctx, j, "alice", tooSmall, inspection.Query{Version: inspection.Version, RunID: waiting.RunID}); !errors.Is(err, inspection.ErrPayloadLimit) || len(got.Run.Input) != 0 {
		t.Fatalf("small explicit durable-source limit silently changed: page=%+v err=%v", got, err)
	}
	if _, err := inspect.InspectSource(ctx, j, "mallory", inspection.Policy{}, inspection.Query{Version: inspection.Version, RunID: waiting.RunID}); !errors.Is(err, inspect.ErrNotFound) {
		t.Fatalf("cross-principal read=%v", err)
	}
	if _, err := j.Signal(ctx, signal.Envelope{RunID: waiting.RunID, SignalID: "approval-1", Name: "approval", Principal: "alice", Payload: []byte(`{"approved":true}`)}, true); err != nil {
		t.Fatal(err)
	}
	if err := j.CompleteRun(ctx, waiting.RunID, []byte(`{"approved":true}`)); err != nil {
		t.Fatal(err)
	}
	resumed, err := inspect.InspectSource(ctx, j, "alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldOutput: true}}, inspection.Query{Version: inspection.Version, RunID: waiting.RunID})
	if err != nil || resumed.Run.Status != inspection.StatusCompleted || resumed.Steps[0].Status != inspection.StatusCompleted {
		t.Fatalf("resumed journal inspection=%+v err=%v", resumed, err)
	}
	uncertain, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-uncertain", Workflow: "payment", ArtifactDigest: "sha256:two", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	op, err := j.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: uncertain.RunID, ArtifactDigest: "sha256:two", InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := j.StartAttempt(ctx, op.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err = j.MarkUncertain(ctx, op.Key, attempt.ID, "provider response omitted"); err != nil {
		t.Fatal(err)
	}
	if err = j.RecordChild(ctx, ChildRecord{RunID: waiting.RunID, Path: "child-step", ChildRunID: uncertain.RunID, State: childRunning}); err != nil {
		t.Fatal(err)
	}
	page, err = inspect.InspectSource(ctx, j, "alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldOutput: true}, MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: uncertain.RunID})
	if err != nil || page.Run.Status != inspection.StatusUncertain || page.Run.ParentRun != waiting.RunID || page.Run.ParentStep != "child-step" || len(page.Steps) != 1 || page.Steps[0].Attempts[0].ID != attempt.ID || page.Steps[0].Status != inspection.StatusUncertain {
		t.Fatalf("uncertain/lineage projection=%+v err=%v", page, err)
	}
	pagedRun, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-pages", Workflow: "many-steps", ArtifactDigest: "sha256:three", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"step-a", "step-b", "step-c"} {
		if _, err := j.StartScope(ctx, ScopeRecord{RunID: pagedRun.RunID, Path: path, Kind: "task"}); err != nil {
			t.Fatal(err)
		}
	}
	page, err = inspect.InspectSource(ctx, j, "alice", inspection.Policy{MaxPageSize: 1}, inspection.Query{Version: inspection.Version, RunID: pagedRun.RunID, Limit: 1})
	if err != nil || len(page.Steps) != 1 || page.Next == "" {
		t.Fatalf("journal first page=%+v err=%v", page, err)
	}
	page, err = inspect.InspectSource(ctx, j, "alice", inspection.Policy{MaxPageSize: 1}, inspection.Query{Version: inspection.Version, RunID: pagedRun.RunID, Limit: 1, Cursor: page.Next})
	if err != nil || len(page.Steps) != 1 || page.Steps[0].ID != "step-b" {
		t.Fatalf("journal second page=%+v err=%v", page, err)
	}
	metadataRun, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-metadata", Workflow: "metadata", ArtifactDigest: "sha256:four", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := j.StartScope(ctx, ScopeRecord{RunID: metadataRun.RunID, Path: fmt.Sprintf("step-%02d", i), Kind: "task"}); err != nil {
			t.Fatal(err)
		}
	}
	metadata, err := inspect.InspectSource(ctx, j, "alice", inspection.Policy{MaxPageSize: 50}, inspection.Query{Version: inspection.Version, RunID: metadataRun.RunID})
	if err != nil || len(metadata.Steps) != 20 {
		t.Fatalf("default durable metadata inspection=%+v err=%v", metadata, err)
	}
}

func TestJournalInspectionProjectsDurableStepAndAttemptInputs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inspect-inputs.db")
	db, j := newJournalAtPath(t, path, Config{})
	defer db.Close()
	ctx := context.Background()
	admission, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-inputs", Workflow: "payment", ArtifactDigest: "sha256:inputs", Input: []byte(`{"order":"o-17"}`)})
	if err != nil {
		t.Fatal(err)
	}
	stepInput := []byte(`{"amountCents":4200,"currency":"USD"}`)
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: admission.RunID, Path: "charge", Kind: "node", Input: stepInput}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: admission.RunID, Path: "charge", Kind: "node", Input: []byte(`{"amountCents":4300}`)}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed durable step input accepted: %v", err)
	}
	operation, err := j.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: admission.RunID, ArtifactDigest: "sha256:inputs", InvocationPath: "charge", IterationPath: "root"}, Input: stepInput})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.BeginEffect(ctx, EffectIntent{Identity: operation.Identity, Input: []byte(`{"amountCents":4300}`)}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("changed durable attempt input accepted: %v", err)
	}
	attempt, err := j.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, j = newJournalAtPath(t, path, Config{})
	defer db.Close()
	policy := inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true}, MaxPageSize: 10, MaxPayloadBytes: 1024}
	page, err := inspect.InspectSource(ctx, j, "alice", policy, inspection.Query{Version: inspection.Version, RunID: admission.RunID})
	if err != nil || len(page.Steps) != 2 {
		t.Fatalf("durable input page=%+v err=%v", page, err)
	}
	var scoped, attempted *inspection.Step
	for index := range page.Steps {
		switch page.Steps[index].ID {
		case "charge":
			scoped = &page.Steps[index]
		case "charge[root]":
			attempted = &page.Steps[index]
		}
	}
	if scoped == nil || string(scoped.Input) != string(stepInput) || attempted == nil || len(attempted.Attempts) != 1 || attempted.Attempts[0].ID != attempt.ID || string(attempted.Input) != string(stepInput) || string(attempted.Attempts[0].Input) != string(stepInput) {
		t.Fatalf("durable step/attempt input missing: steps=%+v", page.Steps)
	}
}

func TestJournalInspectionInputsAreBoundedBeforePersistence(t *testing.T) {
	db, j := newJournal(t, "inspect-input-limit.db", Config{})
	defer db.Close()
	ctx := context.Background()
	admission, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-input-limit", Workflow: "bounded", ArtifactDigest: "sha256:limit", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	oversized := []byte(`"` + strings.Repeat("x", MaxInspectionInputBytes) + `"`)
	identity := OperationIdentity{RunID: admission.RunID, ArtifactDigest: "sha256:limit", InvocationPath: "large", IterationPath: "root"}
	if _, err := j.BeginEffect(ctx, EffectIntent{Identity: identity, Input: oversized}); !errors.Is(err, ErrObservationLimit) {
		t.Fatalf("oversized effect input err=%v", err)
	}
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: admission.RunID, Path: "large", Kind: "node", Input: oversized}); !errors.Is(err, ErrObservationLimit) {
		t.Fatalf("oversized step input err=%v", err)
	}
	if _, err := j.Operation(ctx, identity.Key()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oversized input persisted operation: %v", err)
	}
	page, err := inspect.InspectSource(ctx, j, "alice", inspection.Policy{MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: admission.RunID})
	if err != nil || len(page.Steps) != 0 {
		t.Fatalf("oversized input left inspection row: page=%+v err=%v", page, err)
	}
}

func TestJournalRunFailureIsCanonicalAndSeparateFromAttemptRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "inspect-failure.db")
	db, j := newJournalAtPath(t, path, Config{})
	defer db.Close()
	ctx := context.Background()
	admission, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-failure", Workflow: "payment", ArtifactDigest: "sha256:failure", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := j.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: admission.RunID, ArtifactDigest: "sha256:failure", InvocationPath: "charge", IterationPath: "root"}, Input: []byte(`{"amount":12}`)})
	if err != nil {
		t.Fatal(err)
	}
	first, err := j.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.FailAttempt(ctx, operation.Key, first.ID, true, "temporary failure"); err != nil {
		t.Fatal(err)
	}
	second, err := j.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	activeScopeRun, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "inspect-failure-active-scope", Workflow: "payment", ArtifactDigest: "sha256:failure", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.StartScope(ctx, ScopeRecord{RunID: activeScopeRun.RunID, Path: "running-node", Kind: "node"}); err != nil {
		t.Fatal(err)
	}
	if err := j.FailRun(ctx, activeScopeRun.RunID, "execution_failed", "workflow"); !errors.Is(err, ErrRunActiveWork) {
		t.Fatalf("terminal failure accepted while a scope was still running: %v", err)
	}
	policy := inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true, inspection.FieldError: true}, MaxPageSize: 10, MaxPayloadBytes: 1024}
	page, err := inspect.InspectSource(ctx, j, "alice", policy, inspection.Query{Version: inspection.Version, RunID: admission.RunID})
	if err != nil || page.Run.Status != inspection.StatusRunning || len(page.Steps) != 1 || page.Steps[0].Status != inspection.StatusRunning || len(page.Steps[0].Attempts) != 2 || page.Steps[0].Attempts[0].Status != inspection.StatusFailed || page.Steps[0].Attempts[1].Status != inspection.StatusRunning {
		t.Fatalf("retry incorrectly terminalized the run: page=%+v err=%v", page, err)
	}
	if err := j.FailRun(ctx, admission.RunID, "provider_rejected", "provider"); !errors.Is(err, ErrRunActiveWork) {
		t.Fatalf("active attempt permitted terminal run failure: %v", err)
	}
	if err := j.FailAttempt(ctx, operation.Key, second.ID, false, "permanent provider rejection"); err != nil {
		t.Fatal(err)
	}
	page, err = inspect.InspectSource(ctx, j, "alice", policy, inspection.Query{Version: inspection.Version, RunID: admission.RunID})
	if err != nil || page.Run.Status != inspection.StatusRunning {
		t.Fatalf("attempt failure invented whole-run termination: page=%+v err=%v", page, err)
	}
	if err := j.FailRun(ctx, admission.RunID, "provider_rejected", "provider"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, j = newJournalAtPath(t, path, Config{})
	defer db.Close()
	page, err = inspect.InspectSource(ctx, j, "alice", policy, inspection.Query{Version: inspection.Version, RunID: admission.RunID})
	if err != nil || page.Run.Status != inspection.StatusFailed || page.Run.ErrorCode != "provider_rejected" || page.Run.ErrorClass != "provider" || page.Run.FinishedAt.IsZero() || len(page.Steps) != 1 || page.Steps[0].Status != inspection.StatusFailed || len(page.Steps[0].Attempts) == 0 || page.Steps[0].Attempts[len(page.Steps[0].Attempts)-1].ID != second.ID {
		t.Fatalf("canonical terminal failure projection=%+v err=%v", page, err)
	}
}

func TestJournalRejectsOversizedRunOutputBeforePersistence(t *testing.T) {
	db, j := newJournal(t, "oversized-output.db", Config{})
	defer db.Close()
	ctx := context.Background()
	admission, err := j.Admit(ctx, AdmissionRequest{Principal: "alice", RequestKey: "oversized-output", Workflow: "quote", ArtifactDigest: "sha256:output", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	output := []byte(`"` + strings.Repeat("x", MaxRunOutputBytes) + `"`)
	if err := j.CompleteRun(ctx, admission.RunID, output); !errors.Is(err, ErrRunOutputLimit) {
		t.Fatalf("oversized completed output error=%v, want ErrRunOutputLimit", err)
	}
	run, err := j.Run(ctx, admission.RunID)
	if err != nil || run.State != runAccepted || len(run.Output) != 0 {
		t.Fatalf("oversized output changed run: run=%+v err=%v", run, err)
	}
}
