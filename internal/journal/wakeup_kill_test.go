package journal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store/sqlite"
)

// TestSignalAndTimerRaceHasOneWinner: 50 times, a due wait is claimed by
// its timer on one handle while a signal for it arrives on another. Each
// time exactly one fires it: either the claim returns it and the signal is
// queued, or the signal resumes it and the claim does not return it.
func TestSignalAndTimerRaceHasOneWinner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "race.db")
	database, timer := newJournalAtPath(t, path, Config{Holder: "timer"})
	defer database.Close()
	second, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	signaller, err := New(ctx, second, Config{Holder: "signaller"})
	if err != nil {
		t.Fatal(err)
	}
	wins := map[string]int{}
	for round := range 50 {
		admitted, err := timer.Admit(ctx, AdmissionRequest{RequestKey: fmt.Sprint(round), Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		waitID := fmt.Sprintf("wait-%d", round)
		if _, err := timer.ScheduleWait(ctx, WaitRequest{RunID: admitted.RunID, WaitID: waitID, Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: fixtureBase}); err != nil {
			t.Fatal(err)
		}
		var claimed []WaitRecord
		var result SignalResult
		var claimErr, signalErr error
		start := make(chan struct{})
		var group sync.WaitGroup
		group.Go(func() { <-start; claimed, claimErr = timer.ClaimDueWaits(ctx, fixtureBase, 10) })
		group.Go(func() {
			<-start
			result, signalErr = signaller.Signal(ctx, signal.Envelope{RunID: admitted.RunID, SignalID: "s", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true)
		})
		close(start)
		group.Wait()
		if claimErr != nil || signalErr != nil {
			t.Fatalf("round %d: claim err=%v signal err=%v", round, claimErr, signalErr)
		}
		w, err := timer.Wait(ctx, waitID)
		if err != nil || w.State != waitFired {
			t.Fatalf("round %d: wait=%+v err=%v", round, w, err)
		}
		timerWon := len(claimed) == 1 && claimed[0].WaitID == waitID
		switch {
		case timerWon && result == pending && w.SignalID == "" && w.LeaseOwner == "timer":
			wins["timer"]++
		case !timerWon && len(claimed) == 0 && result == delivered && w.SignalID == "s" && w.LeaseOwner == "":
			wins["signal"]++
		default:
			t.Fatalf("round %d: claimed=%v signal=%+v wait=%+v; want exactly one winner", round, waitIDs(claimed), result, w)
		}
	}
	t.Logf("winners over 50 rounds: %v", wins)
}

// TestWakeupBarriersSurviveAKill kills a process (SIGKILL) parked just
// before and just after the commit of a wait's schedule, its timer claim
// and its signal, then recovers from the database it left: the rows are
// exactly the committed prefix, and the run is resumed exactly once.
func TestWakeupBarriersSurviveAKill(t *testing.T) {
	if os.Getenv("NEWBLOK_332_WAKEUP_CHILD") == "1" {
		runWakeupChild()
		return
	}
	at := fixtureBase.Add(time.Minute)
	lease := (time.Minute + 30*time.Second).Nanoseconds()
	for _, c := range []struct {
		barrier, phase string
		killed         []string
		recover        func(*testing.T, *Journal, string)
		listed         []string
		recovered      []string
	}{
		{"wait-schedule", "before", nil, nil, nil, nil},
		{"wait-schedule", "after", []string{"kill-wait|waiting|||"}, nil, nil, []string{"kill-wait|waiting|||"}},
		{"wait-claim", "before", []string{"kill-wait|waiting|||"}, func(t *testing.T, j *Journal, _ string) {
			if claimed, err := j.ClaimDueWaits(context.Background(), at, 10); err != nil || len(claimed) != 1 {
				t.Fatalf("claim after recovery=%v err=%v", waitIDs(claimed), err)
			}
		}, []string{"kill-wait@parent"}, []string{"kill-wait|acknowledged|||"}},
		{"wait-claim", "after", []string{fmt.Sprintf("kill-wait|fired||child|%d", lease)}, nil, []string{"kill-wait@parent"}, []string{"kill-wait|acknowledged|||"}},
		{"signal", "before", []string{"kill-wait|waiting|||"}, func(t *testing.T, j *Journal, run string) {
			if result, err := j.Signal(context.Background(), signal.Envelope{RunID: run, SignalID: "kill-signal", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil || result != delivered {
				t.Fatalf("signal after recovery=%+v err=%v", result, err)
			}
		}, []string{"kill-wait@parent"}, []string{"kill-wait|acknowledged|kill-signal||", "kill-signal|stored"}},
		{"signal", "after", []string{"kill-wait|fired|kill-signal||", "kill-signal|stored"}, func(t *testing.T, j *Journal, run string) {
			if result, err := j.Signal(context.Background(), signal.Envelope{RunID: run, SignalID: "kill-signal", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil || !result.Duplicate {
				t.Fatalf("signal retry after recovery=%+v err=%v", result, err)
			}
		}, []string{"kill-wait@parent"}, []string{"kill-wait|acknowledged|kill-signal||", "kill-signal|stored"}},
	} {
		t.Run(c.barrier+"/"+c.phase, func(t *testing.T) {
			directory := t.TempDir()
			path, marker := filepath.Join(directory, "journal.db"), filepath.Join(directory, "marker")
			command := exec.Command(os.Args[0], "-test.run=^TestWakeupBarriersSurviveAKill$")
			command.Env = append(os.Environ(), "NEWBLOK_332_WAKEUP_CHILD=1", "NEWBLOK_JOURNAL_TRANSITION="+c.barrier, "NEWBLOK_JOURNAL_PHASE="+c.phase, "NEWBLOK_JOURNAL_PATH="+path, "NEWBLOK_JOURNAL_MARKER="+marker)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForJournalMarker(t, marker)
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()

			database, j := newJournalAtPath(t, path, Config{Holder: "parent", Clock: ticking(at)})
			defer database.Close()
			if got := wakeupRows(t, j); !reflect.DeepEqual(got, c.killed) && !(len(got) == 0 && len(c.killed) == 0) {
				t.Fatalf("rows the kill left:\n got %q\nwant %q", got, c.killed)
			}
			run := oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = 'kill'`)
			if c.recover != nil {
				c.recover(t, j, run)
			}
			// The resumer: every fired wakeup is listed once its lease
			// lapses, resumed, and acknowledged; then nothing is left.
			listed, err := j.PendingResumptions(context.Background(), at.Add(time.Hour), 10)
			if err != nil || !reflect.DeepEqual(waitIDs(listed), append([]string{}, c.listed...)) {
				t.Fatalf("listed %v err=%v; want %v", waitIDs(listed), err, c.listed)
			}
			for _, w := range listed {
				if err := j.AcknowledgeWait(context.Background(), w.WaitID); err != nil {
					t.Fatal(err)
				}
			}
			if again, err := j.PendingResumptions(context.Background(), at.Add(2*time.Hour), 10); err != nil || len(again) != 0 {
				t.Fatalf("still pending after acknowledging: %v err=%v", waitIDs(again), err)
			}
			if got := wakeupRows(t, j); !reflect.DeepEqual(got, c.recovered) && !(len(got) == 0 && len(c.recovered) == 0) {
				t.Fatalf("rows after recovery:\n got %q\nwant %q", got, c.recovered)
			}
		})
	}
}

// runWakeupChild schedules a wait due at fixtureBase, then claims it at
// fixtureBase+1m or signals it, parking at the chosen barrier.
func runWakeupChild() {
	barrier, phase := os.Getenv("NEWBLOK_JOURNAL_TRANSITION"), os.Getenv("NEWBLOK_JOURNAL_PHASE")
	park := func(when string) func(string) {
		return func(name string) {
			if name == barrier && phase == when {
				journalMarkerAndWait()
			}
		}
	}
	database, j := newJournalAtPathForChild(os.Getenv("NEWBLOK_JOURNAL_PATH"), Config{Holder: "child", Hooks: Hooks{BeforeCommit: park("before"), AfterCommit: park("after")}})
	defer database.Close()
	ctx := context.Background()
	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "kill", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		panic(err)
	}
	if _, err := j.ScheduleWait(ctx, WaitRequest{RunID: admitted.RunID, WaitID: "kill-wait", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: fixtureBase}); err != nil {
		panic(err)
	}
	switch barrier {
	case "wait-claim":
		_, err = j.ClaimDueWaits(ctx, fixtureBase.Add(time.Minute), 10)
	case "signal":
		_, err = j.Signal(ctx, signal.Envelope{RunID: admitted.RunID, SignalID: "kill-signal", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true)
	}
	if err != nil {
		panic(err)
	}
}
