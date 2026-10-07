package journal

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store/sqlite"
)

func TestWaitAndSignalSurviveRestartAndDuplicateSignalsAreStable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	database, journal := openWaitJournal(t, path)
	admission, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "wait-request", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{"value":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	due := time.Unix(100, 0)
	if _, err := journal.ScheduleWait(context.Background(), WaitRequest{RunID: admission.RunID, WaitID: "wait-1", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: due}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, journal = openWaitJournal(t, path)
	defer database.Close()
	record, err := journal.Wait(context.Background(), "wait-1")
	if err != nil || !record.DueAt.Equal(due.UTC()) || record.State != waitWaiting {
		t.Fatalf("record=%+v err=%v", record, err)
	}
	envelope := signal.Envelope{RunID: admission.RunID, SignalID: "signal-1", Name: "approval", Principal: "operator", Payload: []byte(`{"approved":true}`)}
	result, err := journal.Signal(context.Background(), envelope, true)
	if err != nil || !result.Accepted || !result.Resumed {
		t.Fatalf("signal=%+v err=%v", result, err)
	}
	duplicate, err := journal.Signal(context.Background(), envelope, true)
	if err != nil || !duplicate.Duplicate || !duplicate.Resumed {
		t.Fatalf("duplicate=%+v err=%v", duplicate, err)
	}
	record, err = journal.Wait(context.Background(), "wait-1")
	if err != nil || record.State != waitResumed || record.SignalID != "signal-1" {
		t.Fatalf("resumed=%+v err=%v", record, err)
	}
}

func TestUnauthorizedLateAndCancelSignalOutcomes(t *testing.T) {
	database, journal := openWaitJournal(t, filepath.Join(t.TempDir(), "journal.db"))
	defer database.Close()
	admission, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "signal-request", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	envelope := signal.Envelope{RunID: admission.RunID, SignalID: "bad", Name: "approval", Principal: "intruder", Payload: []byte(`{}`)}
	if _, err := journal.Signal(context.Background(), envelope, false); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized=%v", err)
	}
	if _, err := journal.ScheduleWait(context.Background(), WaitRequest{RunID: admission.RunID, WaitID: "wait-2", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: time.Unix(200, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := journal.CancelWait(context.Background(), "wait-2"); err != nil {
		t.Fatal(err)
	}
	envelope.SignalID = "late"
	result, err := journal.Signal(context.Background(), envelope, true)
	if err != nil || !result.Late {
		t.Fatalf("late=%+v err=%v", result, err)
	}
	claimed, err := journal.ClaimDueWaits(context.Background(), time.Unix(300, 0), 100)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
}

func TestSignalArrivingBeforeWaitIsConsumedAtomically(t *testing.T) {
	database, journal := openWaitJournal(t, filepath.Join(t.TempDir(), "journal.db"))
	defer database.Close()
	admission, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "prewait", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	envelope := signal.Envelope{RunID: admission.RunID, SignalID: "prewait-signal", Name: "approval", Principal: "operator", Payload: []byte(`{"approved":true}`)}
	result, err := journal.Signal(context.Background(), envelope, true)
	if err != nil || !result.Accepted || result.Resumed {
		t.Fatalf("pre-wait signal=%+v err=%v", result, err)
	}
	record, err := journal.ScheduleWait(context.Background(), WaitRequest{RunID: admission.RunID, WaitID: "prewait-wait", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: time.Unix(300, 0)})
	if err != nil {
		t.Fatal(err)
	}
	if record.State != waitResumed || record.SignalID != envelope.SignalID || string(record.Payload) != string(envelope.Payload) {
		t.Fatalf("wait did not consume pre-wait signal: %+v", record)
	}
	duplicate, err := journal.Signal(context.Background(), envelope, true)
	if err != nil || !duplicate.Duplicate || !duplicate.Accepted || !duplicate.Resumed {
		t.Fatalf("duplicate pre-wait signal=%+v err=%v", duplicate, err)
	}
}

func openWaitJournal(t *testing.T, path string) (interface{ Close() error }, *Journal) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	journal, err := New(context.Background(), database, Config{})
	if err != nil {
		database.Close()
		t.Fatal(err)
	}
	return database, journal
}
