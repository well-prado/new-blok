package journal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// wakeupRows lists every wait as wait_id|state|signal_id|lease_owner|
// lease_until of its run's lease (nanoseconds after fixtureBase, or "" with
// no lease), then every signal as signal_id|state, each in insertion order.
func wakeupRows(t *testing.T, j *Journal) []string {
	t.Helper()
	rows := waitRows(t, j.database, fmt.Sprintf(`SELECT w.wait_id || '|' || w.state || '|' || w.signal_id || '|' || COALESCE(r.lease_owner, '') || '|' || COALESCE(r.lease_until - %d, '') FROM journal_waits w JOIN journal_runs r ON r.run_id = w.run_id ORDER BY w.rowid`, fixtureBase.UnixNano()))
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
			if err := j.AcknowledgeWait(ctx, w.WaitID, w.LeaseToken); err != nil {
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

// TestAcknowledgeWait: a fired wait is acknowledged once, under its run's
// current lease (again is a no-op); without that lease it is ErrLeaseLost;
// a waiting or canceled one is ErrWaitNotFired; an unknown one is
// ErrNotFound.
func TestAcknowledgeWait(t *testing.T) {
	r := newSignalRig(t, ticking(fixtureBase))
	r.wait("open", "approve", "0")
	r.wait("canceled", "approve", "1")
	if err := r.journal.CancelWait(r.ctx, "canceled"); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"open", "canceled"} {
		if err := r.journal.AcknowledgeWait(r.ctx, id, 0); !errors.Is(err, ErrWaitNotFired) {
			t.Fatalf("%s: err=%v; want ErrWaitNotFired", id, err)
		}
	}
	if err := r.journal.AcknowledgeWait(r.ctx, "missing", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: err=%v", err)
	}
	r.send("s1")
	if err := r.journal.AcknowledgeWait(r.ctx, "open", 0); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("acknowledged without the run lease: err=%v", err)
	}
	token, err := r.journal.TakeRunLease(r.ctx, r.run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := r.journal.AcknowledgeWait(r.ctx, "open", token); err != nil {
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

// twoHolders opens two journals with their own handles on one file.
func twoHolders(t *testing.T, a, b Config) (*Journal, *Journal) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "holders.db")
	journals := make([]*Journal, 2)
	for i, config := range []Config{a, b} {
		database, err := (sqlite.Backend{}).Open(context.Background(), path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { database.Close() })
		if journals[i], err = New(context.Background(), database, config); err != nil {
			t.Fatal(err)
		}
	}
	return journals[0], journals[1]
}

func admitWaiting(t *testing.T, j *Journal, key string, waits map[string]time.Time) string {
	t.Helper()
	admitted, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: key, Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	for id, due := range waits {
		if _, err := j.ScheduleWait(context.Background(), WaitRequest{RunID: admitted.RunID, WaitID: id, Name: id, InvocationPath: id, IterationPath: "root", DueAt: due}); err != nil {
			t.Fatal(err)
		}
	}
	return admitted.RunID
}

// TestRunLeaseOutlivesAcknowledgement is Review R's two probes on #366: a
// holder that acknowledges a wakeup is still executing the run, so a timer
// claim or a resumption listing by another holder must not hand the run
// out again, whatever wakes it meanwhile. The lease is on the run: renewal
// extends it, only its holder renews or releases it, release hands the run
// on at once, and a lease left to lapse hands it on then.
func TestRunLeaseOutlivesAcknowledgement(t *testing.T) {
	ctx := context.Background()
	a, b := twoHolders(t, Config{Holder: "a", WakeupLease: 5 * time.Minute}, Config{Holder: "b", WakeupLease: 5 * time.Minute})
	run := admitWaiting(t, a, "running", map[string]time.Time{"w1": fixtureBase, "w2": fixtureBase.Add(time.Minute), "w3": fixtureBase.Add(time.Hour)})
	at := func(d time.Duration) time.Time { return fixtureBase.Add(d) }
	expect := func(label string, listed []WaitRecord, err error, want ...string) {
		t.Helper()
		if err != nil || !reflect.DeepEqual(waitIDs(listed), append([]string{}, want...)) {
			t.Fatalf("%s: %v err=%v; want %v", label, waitIDs(listed), err, want)
		}
	}
	claimed, err := a.ClaimDueWaits(ctx, at(0), 10)
	expect("a claims w1", claimed, err, "w1@a")
	token := claimed[0].LeaseToken
	if err := a.AcknowledgeWait(ctx, "w1", token); err != nil {
		t.Fatal(err)
	}
	claimed, err = b.ClaimDueWaits(ctx, at(time.Minute), 10)
	expect("b's claim while a still runs it", claimed, err)
	if result, err := b.Signal(ctx, signal.Envelope{RunID: run, SignalID: "s3", Name: "w3", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil || result != delivered {
		t.Fatalf("signal=%+v err=%v", result, err)
	}
	listed, err := b.PendingResumptions(ctx, at(time.Minute+time.Second), 10)
	expect("b's listing while a still runs it", listed, err)
	if err := a.RenewRunLease(ctx, run, token, at(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	listed, err = b.PendingResumptions(ctx, at(6*time.Minute), 10)
	expect("b's listing after a renewed past its first lease", listed, err)
	for _, change := range []func() error{
		func() error { return b.RenewRunLease(ctx, run, token, at(6*time.Minute)) },
		func() error { return b.ReleaseRunLease(ctx, run, token) },
	} {
		if err := change(); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("b changing a's lease: err=%v; want ErrLeaseLost", err)
		}
	}
	if err := a.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	listed, err = b.PendingResumptions(ctx, at(6*time.Minute), 10)
	expect("b's listing once a released", listed, err, "w2@b", "w3@b")
	listed, err = a.PendingResumptions(ctx, at(11*time.Minute+time.Second), 10)
	expect("a's listing once b's lease lapsed", listed, err, "w2@a", "w3@a")
}

// TestAnyLiveLeaseHoldsTheRun: a holder's own live lease holds a run too.
// Two journals sharing one holder name (a misconfiguration: Config.Holder
// must be unique per process) never both get a run, and inside one journal
// a resumption listing and a timer claim never both get it. On 2b92980 a
// claim ignored a lease under its own holder name.
func TestAnyLiveLeaseHoldsTheRun(t *testing.T) {
	ctx := context.Background()
	first, second := twoHolders(t, Config{Holder: "same"}, Config{Holder: "same"})
	run := admitWaiting(t, first, "run", map[string]time.Time{"s": fixtureBase.Add(time.Hour), "t": fixtureBase})
	if _, err := first.Signal(ctx, signal.Envelope{RunID: run, SignalID: "s1", Name: "s", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
		t.Fatal(err)
	}
	if listed, err := first.PendingResumptions(ctx, fixtureBase, 10); err != nil || !reflect.DeepEqual(waitIDs(listed), []string{"s@same"}) {
		t.Fatalf("first listing=%v err=%v", waitIDs(listed), err)
	}
	for label, j := range map[string]*Journal{"another journal under the same holder": second, "the journal that listed it": first} {
		if claimed, err := j.ClaimDueWaits(ctx, fixtureBase, 10); err != nil || len(claimed) != 0 {
			t.Fatalf("%s claimed %v; err=%v", label, waitIDs(claimed), err)
		}
		if listed, err := j.PendingResumptions(ctx, fixtureBase, 10); err != nil || len(listed) != 0 {
			t.Fatalf("%s listed %v; err=%v", label, waitIDs(listed), err)
		}
	}
}

// TestClaimSkipsEndedRuns: a due wait of a run that has ended (here
// uncertain, which does not require the run to be quiescent) is not
// fired, claimed or leased.
func TestClaimSkipsEndedRuns(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "ended.db", Config{Holder: "a"})
	defer database.Close()
	run := admitWaiting(t, j, "uncertain", map[string]time.Time{"w1": fixtureBase})
	if err := j.MarkRunUncertain(ctx, run, "timeout", "uncertain"); err != nil {
		t.Fatal(err)
	}
	if claimed, err := j.ClaimDueWaits(ctx, fixtureBase.Add(time.Minute), 10); err != nil || len(claimed) != 0 {
		t.Fatalf("claimed %v from an ended run; err=%v", waitIDs(claimed), err)
	}
	if got := oneRow(t, database, `SELECT w.state || '|' || COALESCE(r.lease_owner, '') FROM journal_waits w JOIN journal_runs r ON r.run_id = w.run_id`); got != "waiting|" {
		t.Fatalf("wait|lease=%s; want waiting and unleased", got)
	}
}

// TestPendingResumptionsIsFair: with room for one run per listing and two
// runs never acknowledged, listings a lease period apart alternate between
// them (least recently leased first), instead of the run that fired first
// every time.
func TestPendingResumptionsIsFair(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "fair.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	for _, key := range []string{"poison", "other"} {
		run := admitWaiting(t, j, key, map[string]time.Time{key: fixtureBase.Add(time.Hour)})
		if _, err := j.Signal(ctx, signal.Envelope{RunID: run, SignalID: key, Name: key, Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
			t.Fatal(err)
		}
	}
	var order []string
	for period := range 5 {
		listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Duration(period)*31*time.Second), 1)
		if err != nil || len(listed) != 1 {
			t.Fatalf("period %d: %v err=%v", period, waitIDs(listed), err)
		}
		order = append(order, listed[0].WaitID)
	}
	if want := []string{"poison", "other", "poison", "other", "poison"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("listed %v; want %v", order, want)
	}
}

// TestReopenDoesNotWaitForAWriter: reopening a migrated journal only reads,
// so another handle holding the write lock does not delay it.
func TestReopenDoesNotWaitForAWriter(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "busy.db")
	database, j := newJournalAtPath(t, path, Config{})
	defer database.Close()
	admitWaiting(t, j, "run", map[string]time.Time{"w": fixtureBase})
	held, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- database.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `UPDATE journal_runs SET run_id = run_id WHERE 0`); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	go func() { time.Sleep(1500 * time.Millisecond); close(release) }()
	start := time.Now()
	other, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := New(ctx, other, Config{}); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > 750*time.Millisecond {
		t.Fatalf("reopen took %v while another handle held the writer", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestLeaseTokenFencesStaleExecutions is Review R round 2's three probes:
// every acquisition of a run lease gets a new token, and renewing,
// releasing or acknowledging under an older one is ErrLeaseLost, even when
// the same holder has taken the run again. On 3258f8c each was accepted.
func TestLeaseTokenFencesStaleExecutions(t *testing.T) {
	ctx := context.Background()
	at := func(d time.Duration) time.Time { return fixtureBase.Add(d) }
	t.Run("own sweep retakes a stalled execution's run", func(t *testing.T) {
		database, a := newJournal(t, "stall.db", Config{Holder: "a"})
		defer database.Close()
		run := admitWaiting(t, a, "run", map[string]time.Time{"w1": fixtureBase})
		claimed, err := a.ClaimDueWaits(ctx, at(0), 10)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim=%v err=%v", waitIDs(claimed), err)
		}
		stale := claimed[0].LeaseToken
		swept, err := a.PendingResumptions(ctx, at(31*time.Second), 10)
		if err != nil || !reflect.DeepEqual(waitIDs(swept), []string{"w1@a"}) || swept[0].LeaseToken == stale {
			t.Fatalf("sweep=%+v err=%v; want w1 under a new token", swept, err)
		}
		if err := a.RenewRunLease(ctx, run, stale, at(32*time.Second)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("the stalled execution renewed: err=%v", err)
		}
		if err := a.AcknowledgeWait(ctx, "w1", stale); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("the stalled execution acknowledged: err=%v", err)
		}
		if err := a.AcknowledgeWait(ctx, "w1", swept[0].LeaseToken); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("retaken after another holder died", func(t *testing.T) {
		a, b := twoHolders(t, Config{Holder: "a"}, Config{Holder: "b"})
		run := admitWaiting(t, a, "run", map[string]time.Time{"w1": fixtureBase})
		claimed, err := a.ClaimDueWaits(ctx, at(0), 10)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("claim=%v err=%v", waitIDs(claimed), err)
		}
		if listed, err := b.PendingResumptions(ctx, at(31*time.Second), 10); err != nil || len(listed) != 1 {
			t.Fatalf("b=%v err=%v", waitIDs(listed), err)
		}
		retaken, err := a.PendingResumptions(ctx, at(62*time.Second), 10)
		if err != nil || len(retaken) != 1 {
			t.Fatalf("a=%v err=%v", waitIDs(retaken), err)
		}
		if err := a.RenewRunLease(ctx, run, claimed[0].LeaseToken, at(63*time.Second)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("a's first execution renewed: err=%v", err)
		}
		if err := a.RenewRunLease(ctx, run, retaken[0].LeaseToken, at(63*time.Second)); err != nil {
			t.Fatal(err)
		}
		// Renewal never shortens a lease.
		if err := a.RenewRunLease(ctx, run, retaken[0].LeaseToken, at(0)); err != nil {
			t.Fatal(err)
		}
		if w, err := a.Wait(ctx, "w1"); err != nil || !w.LeaseUntil.Equal(at(93*time.Second)) {
			t.Fatalf("lease until %v err=%v; want %v", w.LeaseUntil, err, at(93*time.Second))
		}
	})
	t.Run("two journals under one holder name", func(t *testing.T) {
		first, second := twoHolders(t, Config{Holder: "same"}, Config{Holder: "same"})
		run := admitWaiting(t, first, "run", nil)
		old, err := first.TakeRunLease(ctx, run, at(0))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := second.TakeRunLease(ctx, run, at(time.Second)); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("the other journal took a live lease: err=%v", err)
		}
		current, err := second.TakeRunLease(ctx, run, at(31*time.Second))
		if err != nil {
			t.Fatalf("the other journal could not take the lapsed lease: %v", err)
		}
		for _, change := range []func() error{
			func() error { return first.RenewRunLease(ctx, run, old, at(32*time.Second)) },
			func() error { return first.ReleaseRunLease(ctx, run, old) },
		} {
			if err := change(); !errors.Is(err, ErrLeaseLost) {
				t.Fatalf("the first journal's earlier token: err=%v; want ErrLeaseLost", err)
			}
		}
		for range 2 {
			if err := second.ReleaseRunLease(ctx, run, current); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// TestAcknowledgedRetryNeedsTheCurrentToken is Review R round 3's probe:
// acknowledging a wait that is already acknowledged is a no-op only under
// the run's current lease token. A claims w1, B takes the lapsed run and
// acknowledges it; A's stale token, a made-up one, and B's retry under
// any but its current token are ErrLeaseLost.
// On daff569 both returned nil.
func TestAcknowledgedRetryNeedsTheCurrentToken(t *testing.T) {
	ctx := context.Background()
	a, b := twoHolders(t, Config{Holder: "a"}, Config{Holder: "b"})
	admitWaiting(t, a, "run", map[string]time.Time{"w1": fixtureBase})
	claimed, err := a.ClaimDueWaits(ctx, fixtureBase, 10)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim=%v err=%v", waitIDs(claimed), err)
	}
	swept, err := b.PendingResumptions(ctx, fixtureBase.Add(31*time.Second), 10)
	if err != nil || len(swept) != 1 {
		t.Fatalf("sweep=%v err=%v", waitIDs(swept), err)
	}
	if err := b.AcknowledgeWait(ctx, "w1", swept[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
	for _, stale := range []int64{claimed[0].LeaseToken, 999} {
		if err := a.AcknowledgeWait(ctx, "w1", stale); !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("acknowledged again under token %d: err=%v; want ErrLeaseLost", stale, err)
		}
	}
	// The current holder too: only its current token makes the retry a no-op.
	if err := b.AcknowledgeWait(ctx, "w1", swept[0].LeaseToken+1); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("the current holder's retry under another token: err=%v; want ErrLeaseLost", err)
	}
	if err := b.AcknowledgeWait(ctx, "w1", swept[0].LeaseToken); err != nil {
		t.Fatalf("the current holder's retry: %v", err)
	}
}

// TestLeaseTokensAreJournalWide: lease tokens come from one sequence for
// the whole journal, so a token from one run never fences another. On
// daff569 each run counted from 1, and run x's token released run y.
func TestLeaseTokensAreJournalWide(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "tokens.db", Config{Holder: "a"})
	defer database.Close()
	x, y := admitWaiting(t, j, "x", nil), admitWaiting(t, j, "y", nil)
	tx, err := j.TakeRunLease(ctx, x, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	ty, err := j.TakeRunLease(ctx, y, fixtureBase)
	if err != nil || ty == tx {
		t.Fatalf("y token=%d x token=%d err=%v; want distinct", ty, tx, err)
	}
	if err := j.ReleaseRunLease(ctx, y, tx); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("released y with x's token: err=%v; want ErrLeaseLost", err)
	}
}

// TestRewokenRunRanksByItsNewWakeup is Review R round 3's fairness probe:
// run r was leased and released long ago; s wakes, then r wakes again. A
// run's turn is the later of its last lease and its first pending wakeup,
// so s, which woke first, is listed first. On daff569 r's old lease put it
// ahead.
func TestRewokenRunRanksByItsNewWakeup(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "rewoken.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	r := admitWaiting(t, j, "r", map[string]time.Time{"r": fixtureBase.Add(time.Hour)})
	s := admitWaiting(t, j, "s", map[string]time.Time{"s": fixtureBase.Add(time.Hour)})
	token, err := j.TakeRunLease(ctx, r, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.ReleaseRunLease(ctx, r, token); err != nil {
		t.Fatal(err)
	}
	for _, run := range []struct{ id, name string }{{s, "s"}, {r, "r"}} {
		if _, err := j.Signal(ctx, signal.Envelope{RunID: run.id, SignalID: run.name, Name: run.name, Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
			t.Fatal(err)
		}
	}
	if listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Minute), 1); err != nil || !reflect.DeepEqual(waitIDs(listed), []string{"s@a"}) {
		t.Fatalf("listed %v err=%v; want s, which woke first", waitIDs(listed), err)
	}
}

// TestTakeRunLeaseHoldsARunThatNeverSuspended is Review R round 2's S2
// probe: a run executing since admission takes its lease, so a timer it
// schedules while it runs is not claimed by another holder; once released
// it is. An ended run or a held one cannot be taken.
func TestTakeRunLeaseHoldsARunThatNeverSuspended(t *testing.T) {
	ctx := context.Background()
	a, b := twoHolders(t, Config{Holder: "a"}, Config{Holder: "b"})
	run := admitWaiting(t, a, "fresh", nil)
	token, err := a.TakeRunLease(ctx, run, fixtureBase)
	if err != nil || token == 0 {
		t.Fatalf("take=%d err=%v", token, err)
	}
	if _, err := b.TakeRunLease(ctx, run, fixtureBase); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("b took a held run: err=%v", err)
	}
	if _, err := a.ScheduleWait(ctx, WaitRequest{RunID: run, WaitID: "t", Name: "t", InvocationPath: "t", IterationPath: "root", DueAt: fixtureBase}); err != nil {
		t.Fatal(err)
	}
	if claimed, err := b.ClaimDueWaits(ctx, fixtureBase.Add(time.Second), 10); err != nil || len(claimed) != 0 {
		t.Fatalf("b claimed %v from a run a is executing; err=%v", waitIDs(claimed), err)
	}
	if err := a.ReleaseRunLease(ctx, run, token); err != nil {
		t.Fatal(err)
	}
	if listed, err := b.PendingResumptions(ctx, fixtureBase.Add(2*time.Second), 10); err != nil || !reflect.DeepEqual(waitIDs(listed), []string{"t@b"}) {
		t.Fatalf("after release: %v err=%v; want [t@b]", waitIDs(listed), err)
	}
	ended := admitWaiting(t, a, "ended", nil)
	if err := a.CompleteRun(ctx, ended, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.TakeRunLease(ctx, ended, fixtureBase); !errors.Is(err, ErrRunNotActive) {
		t.Fatalf("took an ended run: err=%v", err)
	}
	if _, err := a.TakeRunLease(ctx, "run:missing", fixtureBase); !errors.Is(err, ErrNotFound) {
		t.Fatalf("took an unknown run: err=%v", err)
	}
}

// TestNewWakeupsDoNotStarveALeasedRun: a run whose lease lapsed without
// acknowledgement waits its turn behind runs that woke before it was last
// leased, and ahead of runs that woke after. On 3258f8c never-leased runs
// always ranked first.
func TestNewWakeupsDoNotStarveALeasedRun(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "starve.db", Config{Holder: "a", Clock: ticking(fixtureBase)})
	defer database.Close()
	signal1 := func(key string) {
		run := admitWaiting(t, j, key, map[string]time.Time{key: fixtureBase.Add(time.Hour)})
		if _, err := j.Signal(ctx, signal.Envelope{RunID: run, SignalID: key, Name: key, Principal: "operator", Payload: []byte(`{}`)}, true); err != nil {
			t.Fatal(err)
		}
	}
	signal1("stuck")
	if listed, err := j.PendingResumptions(ctx, fixtureBase, 1); err != nil || len(listed) != 1 {
		t.Fatalf("%v err=%v", waitIDs(listed), err)
	}
	for _, key := range []string{"new-1", "new-2", "new-3"} {
		signal1(key)
	}
	var order []string
	for period := 1; period <= 4; period++ {
		listed, err := j.PendingResumptions(ctx, fixtureBase.Add(time.Duration(period)*31*time.Second), 1)
		if err != nil || len(listed) != 1 {
			t.Fatalf("period %d: %v err=%v", period, waitIDs(listed), err)
		}
		order = append(order, listed[0].WaitID)
	}
	if want := []string{"stuck", "new-1", "new-2", "new-3"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("listed %v; want %v", order, want)
	}
}
