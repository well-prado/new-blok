package journal

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store/sqlite"
)

// wakeupRows lists every wait as wait_id|state|signal_id|lease_owner|
// lease_until (nanoseconds after fixtureBase, or "" with no lease), then
// every signal as signal_id|state, each in insertion order.
func wakeupRows(t *testing.T, j *Journal) []string {
	t.Helper()
	rows := waitRows(t, j.database, fmt.Sprintf(`SELECT wait_id || '|' || state || '|' || signal_id || '|' || COALESCE(lease_owner, '') || '|' || COALESCE(lease_until - %d, '') FROM journal_waits ORDER BY rowid`, fixtureBase.UnixNano()))
	return append(rows, waitRows(t, j.database, `SELECT signal_id || '|' || state FROM journal_signals ORDER BY rowid`)...)
}

func waitIDs(records []WaitRecord) []string {
	ids := []string{}
	for _, record := range records {
		ids = append(ids, record.WaitID+"@"+record.LeaseOwner)
	}
	sort.Strings(ids)
	return ids
}

// TestClaimedWakeupSurvivesReopen is the E07 audit's D1 probe: a timer
// wait is claimed, the process goes away, the journal is reopened. On
// origin/main the wait was already "resumed", nothing could claim it
// again, and its run stayed accepted forever. Now the claim is a lease:
// while it lasts nobody else takes the run; once it lapses the run is
// listed for resumption, leased to the new holder, until the resumed step
// is acknowledged. A signalled wakeup has no lease and is listed at once.
func TestClaimedWakeupSurvivesReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "wakeup.db")
	database, j := newJournalAtPath(t, path, Config{Holder: "first"})
	runs := map[string]string{}
	for key, due := range map[string]time.Time{"timer": fixtureBase, "signal": fixtureBase.Add(time.Hour)} {
		admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: key, Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		runs[key] = admitted.RunID
		if _, err := j.ScheduleWait(ctx, WaitRequest{RunID: admitted.RunID, WaitID: key + "-wait", Name: key, InvocationPath: "wait", IterationPath: "root", DueAt: due}); err != nil {
			t.Fatal(err)
		}
	}
	if claimed, err := j.ClaimDueWaits(ctx, fixtureBase, 10); err != nil || !reflect.DeepEqual(waitIDs(claimed), []string{"timer-wait@first"}) {
		t.Fatalf("claimed=%v err=%v", waitIDs(claimed), err)
	}
	if result, err := j.Signal(ctx, signal.Envelope{RunID: runs["signal"], SignalID: "s1", Name: "signal", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil || result != delivered {
		t.Fatalf("signal=%+v err=%v", result, err)
	}
	database.Close()

	database, j = newJournalAtPath(t, path, Config{Holder: "second"})
	defer database.Close()
	for _, step := range []struct {
		at   time.Duration
		want []string
	}{
		{0, []string{"signal-wait@second"}},               // the claim's lease still lasts
		{31 * time.Second, []string{"timer-wait@second"}}, // it has lapsed
		{time.Hour, []string{}},                           // both acknowledged
	} {
		listed, err := j.PendingResumptions(ctx, fixtureBase.Add(step.at), 10)
		if err != nil || !reflect.DeepEqual(waitIDs(listed), step.want) {
			t.Fatalf("at +%v: listed=%v err=%v; want %v", step.at, waitIDs(listed), err, step.want)
		}
		for _, w := range listed {
			if err := j.AcknowledgeWait(ctx, w.WaitID); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, run := range runs {
		if got, err := j.Run(ctx, run); err != nil || got.State != runAccepted {
			t.Fatalf("run=%+v err=%v", got, err)
		}
	}
}

// TestAcknowledgeWait: a fired wait is acknowledged once (again is a
// no-op); a waiting or canceled one is ErrWaitNotFired; an unknown one is
// ErrNotFound.
func TestAcknowledgeWait(t *testing.T) {
	r := newSignalRig(t, ticking(fixtureBase))
	r.wait("open", "approve", "0")
	r.wait("canceled", "approve", "1")
	if err := r.journal.CancelWait(r.ctx, "canceled"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"open", "canceled"} {
		if err := r.journal.AcknowledgeWait(r.ctx, id); !errors.Is(err, ErrWaitNotFired) {
			t.Fatalf("%s: err=%v; want ErrWaitNotFired", id, err)
		}
	}
	if err := r.journal.AcknowledgeWait(r.ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: err=%v", err)
	}
	r.send("s1")
	for range 2 {
		if err := r.journal.AcknowledgeWait(r.ctx, "open"); err != nil {
			t.Fatal(err)
		}
	}
	if w, err := r.journal.Wait(r.ctx, "open"); err != nil || w.State != waitAcknowledged || w.SignalID != "s1" {
		t.Fatalf("open=%+v err=%v", w, err)
	}
}

// TestPendingResumptionsLeasesEachRunOnce: two holders list resumptions of
// 20 runs at once; each run, with both of its fired waits, goes to exactly
// one of them, and neither gets anything more while the leases last.
func TestPendingResumptionsLeasesEachRunOnce(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "leases.db")
	database, a := newJournalAtPath(t, path, Config{Holder: "a"})
	defer database.Close()
	second, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	b, err := New(ctx, second, Config{Holder: "b"})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 20 {
		admitted, err := a.Admit(ctx, AdmissionRequest{RequestKey: fmt.Sprint(i), Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		for _, branch := range []string{"left", "right"} {
			if _, err := a.ScheduleWait(ctx, WaitRequest{RunID: admitted.RunID, WaitID: fmt.Sprintf("%d-%s", i, branch), Name: branch, InvocationPath: branch, IterationPath: "root", DueAt: fixtureBase.Add(time.Hour)}); err != nil {
				t.Fatal(err)
			}
			if _, err := a.Signal(ctx, signal.Envelope{RunID: admitted.RunID, SignalID: branch, Name: branch, Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
				t.Fatal(err)
			}
		}
	}
	lists := make([][]WaitRecord, 2)
	var group sync.WaitGroup
	for i, holder := range []*Journal{a, b} {
		group.Go(func() {
			listed, err := holder.PendingResumptions(ctx, fixtureBase, 100)
			if err != nil {
				t.Error(err)
			}
			lists[i] = listed
		})
	}
	group.Wait()
	runs := map[string]string{}
	waits := 0
	for i, listed := range lists {
		holder := []string{"a", "b"}[i]
		for _, w := range listed {
			if w.LeaseOwner != holder || w.State != waitFired {
				t.Fatalf("%s listed %+v", holder, w)
			}
			if owner, seen := runs[w.RunID]; seen && owner != holder {
				t.Fatalf("run %s listed to both holders", w.RunID)
			}
			runs[w.RunID] = holder
			waits++
		}
	}
	if len(runs) != 20 || waits != 40 {
		t.Fatalf("listed %d runs, %d waits; want 20 runs with both waits each", len(runs), waits)
	}
	for _, holder := range []*Journal{a, b} {
		if again, err := holder.PendingResumptions(ctx, fixtureBase.Add(29*time.Second), 100); err != nil || len(again) != 0 {
			t.Fatalf("while leased: %d listed, err=%v", len(again), err)
		}
	}
}

// TestLegacyResumedWaitsBecomeFiredOrAcknowledged: a "resumed" wait, as
// origin/main and binaries from before #291 write it, is mapped on open to
// fired when its run is live, and listed for resumption, or to
// acknowledged when its run has ended. The aaf633c database's stranded
// claim and signal are listed at once.
func TestLegacyResumedWaitsBecomeFiredOrAcknowledged(t *testing.T) {
	ctx := context.Background()
	database, j := newJournalAtPath(t, decompress(t, waitFixture), Config{Holder: "upgraded"})
	defer database.Close()
	listed, err := j.PendingResumptions(ctx, fixtureBase.Add(48*time.Hour), 10)
	if err != nil || !reflect.DeepEqual(waitIDs(listed), []string{"claimed-timer@upgraded", "signaled-approval@upgraded"}) {
		t.Fatalf("listed=%v err=%v; want origin/main's stranded claim and signal", waitIDs(listed), err)
	}

	r := newSignalRig(t, ticking(fixtureBase))
	r.wait("live", "approve", "0")
	r.send("s-live")
	ended, err := r.journal.Admit(r.ctx, AdmissionRequest{RequestKey: "ended", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.journal.ScheduleWait(r.ctx, WaitRequest{RunID: ended.RunID, WaitID: "ended", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: fixtureBase}); err != nil {
		t.Fatal(err)
	}
	if _, err := r.journal.Signal(r.ctx, signal.Envelope{RunID: ended.RunID, SignalID: "s-ended", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
		t.Fatal(err)
	}
	if err := r.journal.CompleteRun(r.ctx, ended.RunID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	execAll(t, r.database, `UPDATE journal_waits SET state = 'resumed', fired_at = NULL`)
	if _, err := New(r.ctx, r.database, Config{}); err != nil {
		t.Fatal(err)
	}
	want := []string{"live|fired|1", "ended|acknowledged|1"}
	if got := waitRows(t, r.database, `SELECT wait_id || '|' || state || '|' || (fired_at IS NOT NULL) FROM journal_waits ORDER BY rowid`); !reflect.DeepEqual(got, want) {
		t.Fatalf("waits:\n got %q\nwant %q", got, want)
	}
	// A fired wait of an ended run is never listed, even if one is left.
	execAll(t, r.database, `UPDATE journal_waits SET state = 'fired' WHERE wait_id = 'ended'`)
	if listed, err := r.journal.PendingResumptions(r.ctx, fixtureBase, 10); err != nil || !reflect.DeepEqual(waitIDs(listed), []string{"live@" + r.journal.holder}) {
		t.Fatalf("listed=%v err=%v; want only the live run's wakeup", waitIDs(listed), err)
	}
}

// TestClaimLeavesARunAnotherHolderResumes: while holder a resumes a run
// under a lease, a second wait of that run falls due; holder b's timer
// claim fires it but does not take it, so b never resumes the run a is
// resuming. Once a's lease lapses the run is listed with both wakeups.
func TestClaimLeavesARunAnotherHolderResumes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "held.db")
	database, a := newJournalAtPath(t, path, Config{Holder: "a", WakeupLease: 5 * time.Minute})
	defer database.Close()
	second, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	b, err := New(ctx, second, Config{Holder: "b"})
	if err != nil {
		t.Fatal(err)
	}
	admitted, err := a.Admit(ctx, AdmissionRequest{RequestKey: "held", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for i, due := range []time.Time{fixtureBase, fixtureBase.Add(time.Minute)} {
		if _, err := a.ScheduleWait(ctx, WaitRequest{RunID: admitted.RunID, WaitID: fmt.Sprintf("branch-%d", i), Name: "timer", InvocationPath: "branch", IterationPath: fmt.Sprint(i), DueAt: due}); err != nil {
			t.Fatal(err)
		}
	}
	if claimed, err := a.ClaimDueWaits(ctx, fixtureBase, 10); err != nil || !reflect.DeepEqual(waitIDs(claimed), []string{"branch-0@a"}) {
		t.Fatalf("a claimed=%v err=%v", waitIDs(claimed), err)
	}
	if claimed, err := b.ClaimDueWaits(ctx, fixtureBase.Add(time.Minute), 10); err != nil || len(claimed) != 0 {
		t.Fatalf("b claimed %v from a run a holds; err=%v", waitIDs(claimed), err)
	}
	if w, err := b.Wait(ctx, "branch-1"); err != nil || w.State != waitFired || w.LeaseOwner != "" {
		t.Fatalf("branch-1=%+v err=%v; want fired, unleased", w, err)
	}
	if listed, err := b.PendingResumptions(ctx, fixtureBase.Add(5*time.Minute+time.Second), 10); err != nil || !reflect.DeepEqual(waitIDs(listed), []string{"branch-0@b", "branch-1@b"}) {
		t.Fatalf("after a's lease lapsed: listed=%v err=%v", waitIDs(listed), err)
	}
}
