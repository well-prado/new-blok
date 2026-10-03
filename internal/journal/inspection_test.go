package journal

import (
	"context"
	"errors"
	"fmt"
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
	if _, err = j.ScheduleWait(ctx, WaitRequest{RunID: waiting.RunID, WaitID: "wait-real", Name: "approval", DueAt: time.Now().Add(time.Hour)}); err != nil {
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
