package journal

import (
	"context"
	"errors"
	"fmt"
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
	if err != nil || record.State != waitFired || record.SignalID != "signal-1" {
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
	// A signal for the canceled wait is late (#332: one addressed by name
	// alone would be held for the run's next wait of the name).
	envelope.SignalID = "late"
	result, err := journal.SignalWait(context.Background(), envelope, WaitTarget{WaitID: "wait-2"}, true)
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
	if record.State != waitFired || record.SignalID != envelope.SignalID || string(record.Payload) != string(envelope.Payload) {
		t.Fatalf("wait did not consume pre-wait signal: %+v", record)
	}
	duplicate, err := journal.Signal(context.Background(), envelope, true)
	if err != nil || !duplicate.Duplicate || !duplicate.Accepted || !duplicate.Resumed {
		t.Fatalf("duplicate pre-wait signal=%+v err=%v", duplicate, err)
	}
}

// scheduleDueWait admits a run of its own and schedules one wait on it due
// at due. Separate runs keep each wait's run lease independent.
func scheduleDueWait(t *testing.T, journal *Journal, id string, due time.Time) {
	t.Helper()
	admission, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "req-" + id, Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.ScheduleWait(context.Background(), WaitRequest{RunID: admission.RunID, WaitID: id, Name: "timer", InvocationPath: "sleep", IterationPath: "root", DueAt: due}); err != nil {
		t.Fatal(err)
	}
}

func claimedIDs(t *testing.T, journal *Journal, now time.Time) []string {
	t.Helper()
	records, err := journal.ClaimDueWaits(context.Background(), now, 100)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, record := range records {
		ids = append(ids, record.WaitID)
	}
	return ids
}

// TestClaimDueWaitsNeverFiresBeforeDueAt pins the due boundary exactly:
// one nanosecond early fires nothing, the due instant fires the wait, and
// the wait fires once. A ClaimDueWaits that ignored due_at fails the first
// check (#336).
func TestClaimDueWaitsNeverFiresBeforeDueAt(t *testing.T) {
	database, journal := openWaitJournal(t, filepath.Join(t.TempDir(), "journal.db"))
	defer database.Close()
	due := time.Unix(1_000, 0)
	scheduleDueWait(t, journal, "wait-due", due)
	for _, early := range []time.Time{time.Unix(0, 0), due.Add(-time.Hour), due.Add(-time.Nanosecond)} {
		if got := claimedIDs(t, journal, early); len(got) != 0 {
			t.Fatalf("claimed %v at %v, before due %v", got, early, due)
		}
		if record, err := journal.Wait(context.Background(), "wait-due"); err != nil || record.State != waitWaiting {
			t.Fatalf("early claim changed the wait: %+v err=%v", record, err)
		}
	}
	if got := claimedIDs(t, journal, due); len(got) != 1 || got[0] != "wait-due" {
		t.Fatalf("claimed %v at the due instant, want [wait-due]", got)
	}
	if got := claimedIDs(t, journal, due.Add(time.Hour)); len(got) != 0 {
		t.Fatalf("fired again: %v", got)
	}
}

// TestClockJumpsNeverRefireOrFireEarly: the clock is a parameter, so a
// backward jump is a smaller now. Predeclared: after the 100s wait fires,
// stepping back to 50s fires nothing and re-fires nothing; the 200s and
// 300s waits stay closed until their own due time; jumping forward to
// 10000s fires exactly the two overdue ones, in due order, once.
func TestClockJumpsNeverRefireOrFireEarly(t *testing.T) {
	database, journal := openWaitJournal(t, filepath.Join(t.TempDir(), "journal.db"))
	defer database.Close()
	scheduleDueWait(t, journal, "w-100", time.Unix(100, 0))
	scheduleDueWait(t, journal, "w-200", time.Unix(200, 0))
	scheduleDueWait(t, journal, "w-300", time.Unix(300, 0))
	steps := []struct {
		name string
		now  int64
		want []string
	}{
		{"first is due", 100, []string{"w-100"}},
		{"backward jump", 50, []string{}},
		{"backward to epoch", 0, []string{}},
		{"forward but before second", 199, []string{}},
		{"forward jump over both", 10_000, []string{"w-200", "w-300"}},
		{"backward after all fired", 90, []string{}},
		{"forward again", 20_000, []string{}},
	}
	for _, step := range steps {
		if got := claimedIDs(t, journal, time.Unix(step.now, 0)); fmt.Sprint(got) != fmt.Sprint(step.want) {
			t.Fatalf("%s: claimed %v at %ds, want %v", step.name, got, step.now, step.want)
		}
	}
}

// TestWaitDueTimesCompareInUTCAcrossDaylightSaving: due times built in a
// zone that observes daylight saving are stored and compared as instants.
// In America/New_York 2026-11-01 01:30 happens twice (05:30Z EDT, 06:30Z
// EST) and 2026-03-08 02:30 never happens (07:30Z reads as 03:30 EDT).
// Predeclared: each wait fires at its UTC instant and not a second sooner.
func TestWaitDueTimesCompareInUTCAcrossDaylightSaving(t *testing.T) {
	zone, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	database, journal := openWaitJournal(t, filepath.Join(t.TempDir(), "journal.db"))
	defer database.Close()
	firstOne := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC).In(zone)
	secondOne := time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC).In(zone)
	if firstOne.Format("15:04") != "01:30" || secondOne.Format("15:04") != "01:30" || firstOne.Equal(secondOne) {
		t.Fatalf("fixture is not the repeated hour: %v %v", firstOne, secondOne)
	}
	springForward := time.Date(2026, 3, 8, 7, 30, 0, 0, time.UTC).In(zone)
	if springForward.Format("15:04") != "03:30" {
		t.Fatalf("fixture is not after the skipped hour: %v", springForward)
	}
	scheduleDueWait(t, journal, "fall-first", firstOne)
	scheduleDueWait(t, journal, "fall-second", secondOne)
	scheduleDueWait(t, journal, "spring", springForward)
	if record, err := journal.Wait(context.Background(), "fall-second"); err != nil || !record.DueAt.Equal(secondOne) || record.DueAt.Location() != time.UTC {
		t.Fatalf("stored due %+v err=%v", record, err)
	}
	// The clock is read in UTC and in the zone: the result is the same.
	steps := []struct {
		now  time.Time
		want []string
	}{
		{time.Date(2026, 3, 8, 7, 29, 59, 0, time.UTC).In(zone), []string{}},
		{time.Date(2026, 3, 8, 7, 30, 0, 0, time.UTC), []string{"spring"}},
		{time.Date(2026, 11, 1, 5, 29, 59, 0, time.UTC), []string{}},
		{time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC).In(zone), []string{"fall-first"}},
		// 01:30 wall clock a second time: the first repeat is not enough.
		{time.Date(2026, 11, 1, 6, 29, 59, 0, time.UTC).In(zone), []string{}},
		{time.Date(2026, 11, 1, 6, 30, 0, 0, time.UTC), []string{"fall-second"}},
	}
	for _, step := range steps {
		if got := claimedIDs(t, journal, step.now); fmt.Sprint(got) != fmt.Sprint(step.want) {
			t.Fatalf("claimed %v at %v, want %v", got, step.now, step.want)
		}
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
