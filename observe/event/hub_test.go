package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newHub(t *testing.T, config Config) *Hub {
	t.Helper()
	hub, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return hub
}

func step(n int) Item {
	return Item{Name: "step.completed", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
}

func names(frames []*Frame) []string {
	out := make([]string, len(frames))
	for i, frame := range frames {
		out[i] = frame.Name()
	}
	return out
}

func gapReason(t *testing.T, frame *Frame) string {
	t.Helper()
	var gap Gap
	if !frame.Gap() || json.Unmarshal(frame.Data(), &gap) != nil {
		t.Fatalf("frame %s %s is not a gap", frame.Name(), frame.Data())
	}
	return gap.Reason
}

func TestConfigBoundsAreValidatedAndDefaulted(t *testing.T) {
	hub := newHub(t, Config{})
	got := hub.Config()
	if got.MaxRuns != DefaultMaxRuns || got.QueueDepth != DefaultQueueDepth || got.LateWindow != DefaultLateWindow || got.RunBytes != DefaultRunBytes {
		t.Fatalf("defaults=%+v", got)
	}
	for name, config := range map[string]Config{
		"negative":            {MaxRuns: -1},
		"runs over limit":     {MaxRuns: MaxRunsLimit + 1},
		"event over run":      {MaxEventBytes: 64 << 10, RunBytes: 64 << 10},
		"retained over 1GiB":  {MaxRuns: MaxRunsLimit, RunBytes: MaxRunBytesLimit},
		"queues over 256MiB":  {MaxSubscribers: MaxSubscribersCap, QueueDepth: MaxQueueDepth, SubscribersPerRun: 1, SubscribersPerPrincipal: 1},
		"replays over 256MiB": {MaxSubscribers: 1024, QueueDepth: 1, MaxEventBytes: 4096, SubscribersPerRun: 1, SubscribersPerPrincipal: 1},
		"late window":         {LateWindow: MaxLateWindow + time.Second},
		"negative window":     {LateWindow: -time.Second},
		"per-run over max":    {MaxSubscribers: 2, SubscribersPerRun: 3, SubscribersPerPrincipal: 1},
	} {
		if _, err := New(config); err == nil {
			t.Errorf("%s: config accepted: %+v", name, config)
		}
	}
}

// TestPublishNeverWaitsForAStalledSubscriber is the core telemetry rule: a
// reader that stops draining is disconnected, and the publisher (the run's
// own goroutine) is never held.
func TestPublishNeverWaitsForAStalledSubscriber(t *testing.T) {
	hub := newHub(t, Config{QueueDepth: 2})
	if _, err := hub.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true}); err != nil {
		t.Fatal(err)
	}
	replay, err := hub.Subscribe("run-1", "alice", "", nil)
	if err != nil || replay.Subscriber == nil {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			if _, err := hub.Publish("run-1", "alice", step(i)); err != nil {
				t.Errorf("publish %d: %v", i, err)
				return
			}
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("publication waited on a subscriber that never reads")
	}
	select {
	case <-replay.Subscriber.Done():
	default:
		t.Fatal("stalled subscriber was not disconnected")
	}
	if reason := replay.Subscriber.Reason(); reason != "slow_subscriber" {
		t.Fatalf("reason=%q", reason)
	}
	if stats := hub.Stats(); stats.SlowSubscribers != 1 || stats.Subscribers != 0 || stats.Published != 10001 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestDroppedObservationsAreInlineGapMarkers(t *testing.T) {
	hub := newHub(t, Config{MaxEventBytes: 4096})
	if _, err := hub.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true}); err != nil {
		t.Fatal(err)
	}
	replay, err := hub.Subscribe("run-1", "alice", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, item := range map[string]Item{
		"oversized": {Name: "step.log", Data: []byte(`"` + strings.Repeat("x", 5000) + `"`)},
		"not json":  {Name: "step.log", Data: []byte(`{`)},
		"bad name":  {Name: "Step\nlog", Data: []byte(`{}`)},
		"reserved":  {Name: GapName, Data: []byte(`{}`)},
	} {
		if _, err := hub.Publish("run-1", "alice", item); !errors.Is(err, ErrInvalidPayload) {
			t.Errorf("%s: err=%v", name, err)
		}
	}
	hub.Drop("run-1", "alice")
	hub.Drop("run-1", "mallory") // another owner cannot mark alice's run
	for i := 0; i < 5; i++ {
		frame := <-replay.Subscriber.Events()
		if reason := gapReason(t, frame); reason != GapDropped {
			t.Fatalf("live gap reason=%q", reason)
		}
	}
	select {
	case frame := <-replay.Subscriber.Events():
		t.Fatalf("unexpected frame %s", frame.Name())
	default:
	}
	again, err := hub.Subscribe("run-1", "alice", "", nil)
	if err != nil || len(again.Frames) != 6 || again.Gap != nil {
		t.Fatalf("replay=%v gap=%+v err=%v", names(again.Frames), again.Gap, err)
	}
	if stats := hub.Stats(); stats.Dropped != 6 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestCursorResumesRetentionGapsAndRejectsForeignCursors(t *testing.T) {
	hub := newHub(t, Config{EventsPerRun: 4})
	cursors := []string{}
	start, _ := hub.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	cursors = append(cursors, start)
	for i := 0; i < 9; i++ {
		cursor, err := hub.Publish("run-1", "alice", step(i))
		if err != nil {
			t.Fatal(err)
		}
		cursors = append(cursors, cursor)
	}
	// Resume from a retained position: exactly what follows, no gap.
	replay, err := hub.Subscribe("run-1", "alice", cursors[7], nil)
	if err != nil || replay.Gap != nil || len(replay.Frames) != 2 || replay.Frames[0].Cursor() != cursors[8] {
		t.Fatalf("resume replay=%v gap=%+v err=%v", names(replay.Frames), replay.Gap, err)
	}
	hub.Unsubscribe(replay.Subscriber, "test")
	// Resume from an evicted position: a counted retention gap first.
	replay, err = hub.Subscribe("run-1", "alice", cursors[1], nil)
	if err != nil || replay.Gap == nil || replay.Gap.Reason != GapRetention || replay.Gap.Missed != 4 || len(replay.Frames) != 4 {
		t.Fatalf("retention replay=%v gap=%+v err=%v", names(replay.Frames), replay.Gap, err)
	}
	if replay.GapCursor != cursors[5] {
		t.Fatalf("gap cursor=%s want %s", replay.GapCursor, cursors[5])
	}
	hub.Unsubscribe(replay.Subscriber, "test")
	// Reconnecting from the gap cursor continues without another gap.
	replay, err = hub.Subscribe("run-1", "alice", cursors[5], nil)
	if err != nil || replay.Gap != nil || len(replay.Frames) != 4 {
		t.Fatalf("gap-cursor replay=%v gap=%+v err=%v", names(replay.Frames), replay.Gap, err)
	}
	hub.Unsubscribe(replay.Subscriber, "test")
	other, _ := hub.Publish("run-2", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	head, _, _ := strings.Cut(cursors[9], ":")
	for name, cursor := range map[string]string{
		"malformed":     "nope",
		"ahead":         head + ":11",
		"other run":     other,
		"non canonical": head + ":09",
		"later inc":     strings.Replace(cursors[0], ".1.", ".9.", 1),
		"oversized":     strings.Repeat("a", MaxCursorBytes+1),
		"zero inc":      strings.Replace(cursors[0], ".1.", ".0.", 1),
	} {
		if _, err := hub.Subscribe("run-1", "alice", cursor, nil); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("%s cursor %q: err=%v", name, cursor, err)
		}
	}
	// A cursor from another process epoch is a restart gap.
	restarted := newHub(t, Config{})
	if _, err := restarted.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true}); err != nil {
		t.Fatal(err)
	}
	replay, err = restarted.Subscribe("run-1", "alice", cursors[3], nil)
	if err != nil || replay.Gap == nil || replay.Gap.Reason != GapRestart || len(replay.Frames) != 1 {
		t.Fatalf("restart replay=%v gap=%+v err=%v", names(replay.Frames), replay.Gap, err)
	}
}

func TestUnauthorizedReaderIsIndistinguishableFromUnknownRun(t *testing.T) {
	hub := newHub(t, Config{})
	if _, err := hub.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Subscribe("run-1", "mallory", "", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("other principal err=%v", err)
	}
	if _, err := hub.Subscribe("run-unknown", "mallory", "", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown run err=%v", err)
	}
	// The authorizer runs outside the hub lock: one that reads the hub
	// itself must not deadlock.
	reviewer := func(reader, owner string) error {
		_ = hub.Stats()
		if reader == "reviewer" && owner == "alice" {
			return nil
		}
		return ErrUnauthorized
	}
	if replay, err := hub.Subscribe("run-1", "reviewer", "", reviewer); err != nil || replay.Subscriber == nil {
		t.Fatalf("delegated reader err=%v", err)
	}
	if _, err := hub.Publish("run-1", "mallory", step(1)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("foreign publisher err=%v", err)
	}
}

func TestLateObservationsAreBoundedByTheLateWindow(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)}
	hub := newHub(t, Config{LateWindow: 2 * time.Second, Clock: clock.Now})
	hub.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	live, err := hub.Subscribe("run-1", "alice", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := hub.Publish("run-1", "alice", Item{Name: "run.completed", Data: []byte(`{}`), Terminal: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Publish("run-1", "alice", step(1)); !errors.Is(err, ErrRunFinished) {
		t.Fatalf("transition after terminal err=%v", err)
	}
	clock.Advance(time.Second)
	if _, err := hub.Publish("run-1", "alice", Item{Name: "step.log", Data: []byte(`{"m":"late"}`), Late: true}); err != nil {
		t.Fatalf("late log inside window err=%v", err)
	}
	got := names(live.Frames)
	for len(got) < 3 {
		select {
		case frame := <-live.Subscriber.Events():
			got = append(got, frame.Name())
		case <-time.After(5 * time.Second):
			t.Fatalf("live frames=%v", got)
		}
	}
	if strings.Join(got, ",") != "run.started,run.completed,step.log" {
		t.Fatalf("live frames=%v", got)
	}
	// A subscriber arriving inside the window is told when it closes.
	mid, err := hub.Subscribe("run-1", "alice", terminal, nil)
	if err != nil || mid.Subscriber == nil || !mid.CloseAt.Equal(clock.Now().Add(time.Second)) || len(mid.Frames) != 1 {
		t.Fatalf("mid-window replay=%+v err=%v", mid, err)
	}
	clock.Advance(2 * time.Second)
	for i := 0; i < 3; i++ {
		if _, err := hub.Publish("run-1", "alice", Item{Name: "step.log", Data: []byte(`{}`), Late: true}); !errors.Is(err, ErrLate) {
			t.Fatalf("late log after window err=%v", err)
		}
	}
	closed, err := hub.Subscribe("run-1", "alice", "", nil)
	if err != nil || closed.Subscriber != nil || !closed.Finished {
		t.Fatalf("closed run replay=%+v err=%v", closed, err)
	}
	if n := names(closed.Frames); strings.Join(n, ",") != "run.started,run.completed,step.log,gap" || gapReason(t, closed.Frames[3]) != GapLateDropped {
		t.Fatalf("closed frames=%v", n)
	}
	if stats := hub.Stats(); stats.LateDelivered != 1 || stats.LateDropped != 3 || stats.Rejected != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	// A reader at the last frame of a closed run has nothing to follow.
	end, err := hub.Subscribe("run-1", "alice", closed.LastCursor, nil)
	if err != nil || end.Subscriber != nil || len(end.Frames) != 0 || end.Gap != nil {
		t.Fatalf("end replay=%+v err=%v", end, err)
	}
}

func TestRunEvictionAndSaturationAreVisible(t *testing.T) {
	hub := newHub(t, Config{MaxRuns: 2})
	start := Item{Name: "run.started", Data: []byte(`{}`), Start: true}
	first, _ := hub.Publish("run-1", "alice", start)
	hub.Publish("run-2", "alice", start)
	followed, err := hub.Subscribe("run-2", "alice", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	// run-1 is least recently used and unfollowed: it is evicted.
	hub.Publish("run-3", "alice", start)
	if _, err := hub.Subscribe("run-1", "alice", "", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("evicted run err=%v", err)
	}
	// run-1 keeps running elsewhere in this process: its next observation
	// starts a new incarnation that opens with an eviction gap, and a reader
	// of the old incarnation gets a retention gap.
	hub.Publish("run-1", "alice", step(1))
	replay, err := hub.Subscribe("run-1", "alice", first, nil)
	if err != nil || replay.Gap == nil || replay.Gap.Reason != GapRetention || len(replay.Frames) != 2 || gapReason(t, replay.Frames[0]) != GapEvicted {
		t.Fatalf("re-created run replay=%v gap=%+v err=%v", names(replay.Frames), replay.Gap, err)
	}
	// Re-creating run-1 evicted run-3, the unfollowed one. run-1 and run-2
	// are both followed now, so a new run cannot be admitted, and that is
	// counted, never silent.
	if _, err := hub.Subscribe("run-3", "alice", "", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("run-3 should have been evicted: %v", err)
	}
	before := hub.Stats().Dropped
	if _, err := hub.Publish("run-4", "alice", start); !errors.Is(err, ErrSaturated) {
		t.Fatalf("saturated err=%v", err)
	}
	if stats := hub.Stats(); stats.Dropped != before+1 || stats.EvictedRuns != 2 {
		t.Fatalf("stats=%+v", stats)
	}
	// Once a reader leaves, its run can be evicted again.
	hub.Unsubscribe(followed.Subscriber, "test")
	if _, err := hub.Publish("run-4", "alice", start); err != nil {
		t.Fatalf("publish after a reader left: %v", err)
	}
}

func TestSubscriberLimitsAndShutdown(t *testing.T) {
	hub := newHub(t, Config{MaxSubscribers: 3, SubscribersPerRun: 2, SubscribersPerPrincipal: 2})
	start := Item{Name: "run.started", Data: []byte(`{}`), Start: true}
	hub.Publish("run-1", "alice", start)
	hub.Publish("run-2", "bob", start)
	share := func(reader, owner string) error { return nil }
	a1, _ := hub.Subscribe("run-1", "alice", "", nil)
	if _, err := hub.Subscribe("run-1", "alice", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Subscribe("run-1", "bob", "", share); !errors.Is(err, ErrSaturated) {
		t.Fatalf("per-run limit err=%v", err)
	}
	if _, err := hub.Subscribe("run-2", "alice", "", share); !errors.Is(err, ErrSaturated) {
		t.Fatalf("per-principal limit err=%v", err)
	}
	if _, err := hub.Subscribe("run-2", "bob", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Subscribe("run-2", "carol", "", share); !errors.Is(err, ErrSaturated) {
		t.Fatalf("global limit err=%v", err)
	}
	if stats := hub.Stats(); stats.RejectedSubscribers != 3 || stats.Subscribers != 3 {
		t.Fatalf("stats=%+v", stats)
	}
	hub.Close()
	<-a1.Subscriber.Done()
	if a1.Subscriber.Reason() != "shutdown" || hub.Stats().Subscribers != 0 {
		t.Fatalf("shutdown reason=%q", a1.Subscriber.Reason())
	}
	if _, err := hub.Subscribe("run-1", "alice", "", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe after close err=%v", err)
	}
	if _, err := hub.Publish("run-1", "alice", step(1)); err != nil {
		t.Fatalf("publication must keep working after close: %v", err)
	}
}

func TestStartOnFinishedRunSupersedesAndRecoveredRunsReportMissingHistory(t *testing.T) {
	hub := newHub(t, Config{})
	start := Item{Name: "run.started", Data: []byte(`{}`), Start: true}
	hub.Publish("run-1", "alice", start)
	old, _ := hub.Subscribe("run-1", "alice", "", nil)
	hub.Publish("run-1", "alice", Item{Name: "run.failed", Data: []byte(`{}`), Terminal: true})
	if _, err := hub.Publish("run-1", "alice", start); err != nil {
		t.Fatal(err)
	}
	<-old.Subscriber.Done()
	if old.Subscriber.Reason() != "superseded" {
		t.Fatalf("reason=%q", old.Subscriber.Reason())
	}
	if err := hub.AttachRecovered("run-1", "mallory", "mallory", false); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("recovered owner mismatch err=%v", err)
	}
	if err := hub.AttachRecovered("run-r", "alice", "alice", false); err != nil {
		t.Fatal(err)
	}
	replay, err := hub.Subscribe("run-r", "alice", "", nil)
	if err != nil || replay.Gap == nil || replay.Gap.Reason != GapUnavailable || replay.Subscriber == nil || !replay.Recovered {
		t.Fatalf("recovered replay=%+v err=%v", replay, err)
	}
	if err := hub.AttachRecovered("run-t", "alice", "alice", true); err != nil {
		t.Fatal(err)
	}
	if replay, err := hub.Subscribe("run-t", "alice", "", nil); err != nil || replay.Subscriber != nil || replay.Gap == nil {
		t.Fatalf("recovered terminal replay=%+v err=%v", replay, err)
	}
}

func TestRetainedMemoryStaysWithinConfiguredBound(t *testing.T) {
	config := Config{MaxRuns: 8, EventsPerRun: 64, RunBytes: 16 << 10, MaxEventBytes: 4 << 10}
	hub := newHub(t, config)
	payload := []byte(`"` + strings.Repeat("x", 1000) + `"`)
	for run := 0; run < 100; run++ {
		id := fmt.Sprintf("run-%d", run)
		hub.Publish(id, "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
		for i := 0; i < 200; i++ {
			hub.Publish(id, "alice", Item{Name: "step.log", Data: payload, Late: true})
		}
		hub.Publish(id, "alice", Item{Name: "run.completed", Data: []byte(`{}`), Terminal: true})
		if stats := hub.Stats(); stats.RetainedBytes > config.MaxRuns*config.RunBytes || stats.Runs > config.MaxRuns || stats.Frames > config.MaxRuns*config.EventsPerRun {
			t.Fatalf("bound exceeded after run %d: %+v", run, stats)
		}
	}
	stats := hub.Stats()
	t.Logf("retained=%dB frames=%d runs=%d evictedFrames=%d evictedRuns=%d bound=%dB", stats.RetainedBytes, stats.Frames, stats.Runs, stats.EvictedFrames, stats.EvictedRuns, config.MaxRuns*config.RunBytes)
	if stats.EvictedRuns != 92 {
		t.Fatalf("stats=%+v", stats)
	}
}

func TestConcurrentPublishersAndSubscribers(t *testing.T) {
	hub := newHub(t, Config{QueueDepth: 8, EventsPerRun: 128, LateWindow: time.Millisecond})
	var wg sync.WaitGroup
	for r := 0; r < 8; r++ {
		runID := fmt.Sprintf("run-%d", r)
		hub.Publish(runID, "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
		wg.Add(2)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				hub.Publish(runID, "alice", step(i))
			}
			hub.Publish(runID, "alice", Item{Name: "run.completed", Data: []byte(`{}`), Terminal: true})
		}()
		go func() {
			defer wg.Done()
			cursor := ""
			for attempt := 0; attempt < 200; attempt++ {
				replay, err := hub.Subscribe(runID, "alice", cursor, nil)
				if errors.Is(err, ErrSaturated) {
					continue
				}
				if err != nil {
					t.Errorf("subscribe: %v", err)
					return
				}
				for _, frame := range replay.Frames {
					cursor = frame.Cursor()
				}
				if replay.Subscriber == nil {
					return
				}
			read:
				for i := 0; i < 16; i++ {
					select {
					case frame := <-replay.Subscriber.Events():
						cursor = frame.Cursor()
					case <-replay.Subscriber.Done():
						break read
					case <-time.After(10 * time.Millisecond):
						break read
					}
				}
				hub.Unsubscribe(replay.Subscriber, "test")
			}
		}()
	}
	wg.Wait()
	if stats := hub.Stats(); stats.Subscribers != 0 {
		t.Fatalf("subscribers leaked: %+v", stats)
	}
}

// TestTrustedPublisherReclaimsARecoveredRunAttachedUnderAnotherOwner is the
// hub's defence for review finding F1: if a recovered run was attached
// under an owner other than the engine's trusted publisher, the publisher
// wins visibly: followers are detached to re-authorize, nothing is silently
// rejected, and the real owner can follow.
func TestTrustedPublisherReclaimsARecoveredRunAttachedUnderAnotherOwner(t *testing.T) {
	hub := newHub(t, Config{})
	if err := hub.AttachRecovered("run-1", "bob", "bob", false); err != nil {
		t.Fatal(err)
	}
	bob, err := hub.Subscribe("run-1", "bob", "", nil)
	if err != nil || bob.Subscriber == nil {
		t.Fatalf("bob=%+v err=%v", bob, err)
	}
	if _, err := hub.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true}); err != nil {
		t.Fatalf("trusted publication: %v", err)
	}
	<-bob.Subscriber.Done()
	if bob.Subscriber.Reason() != "reowned" {
		t.Fatalf("bob reason=%q", bob.Subscriber.Reason())
	}
	if _, err := hub.Subscribe("run-1", "bob", "", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("bob after reclaim err=%v", err)
	}
	alice, err := hub.Subscribe("run-1", "alice", "", nil)
	if err != nil || len(alice.Frames) != 1 || alice.Frames[0].Name() != "run.started" {
		t.Fatalf("alice replay=%v err=%v", names(alice.Frames), err)
	}
	if stats := hub.Stats(); stats.Reowned != 1 || stats.Rejected != 0 || stats.Published != 1 {
		t.Fatalf("stats=%+v", stats)
	}
	// A live run's owner is never reclaimed: that is a real conflict.
	if _, err := hub.Publish("run-1", "mallory", step(1)); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("conflicting live publisher err=%v", err)
	}
}

// TestRecoveredRunsRecycleTheirOwnBudgetAndNeverEvictLiveRuns is review
// finding F2 at the hub.
func TestRecoveredRunsRecycleTheirOwnBudgetAndNeverEvictLiveRuns(t *testing.T) {
	hub := newHub(t, Config{MaxRuns: 3, RecoveredPerPrincipal: 2})
	hub.Publish("live-a", "carol", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	for _, runID := range []string{"old-1", "old-2", "old-3", "old-4"} {
		if err := hub.AttachRecovered(runID, "mallory", "mallory", true); err != nil {
			t.Fatalf("%s: %v", runID, err)
		}
	}
	if _, err := hub.Subscribe("live-a", "carol", "", nil); err != nil {
		t.Fatalf("carol's live run was evicted by recovered reads: %v", err)
	}
	if _, err := hub.Subscribe("old-1", "mallory", "", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mallory's oldest recovered run should have been recycled: %v", err)
	}
	if stats := hub.Stats(); stats.Runs != 3 {
		t.Fatalf("stats=%+v", stats)
	}
	// With only live runs retained, a recovered read is refused, not served
	// by evicting one.
	full := newHub(t, Config{MaxRuns: 2})
	full.Publish("live-a", "carol", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	full.Publish("live-b", "dave", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	if err := full.AttachRecovered("old-1", "mallory", "mallory", false); !errors.Is(err, ErrSaturated) {
		t.Fatalf("recovered read evicted a live run: %v", err)
	}
	// A closed run may make room.
	full.Publish("live-b", "dave", Item{Name: "run.completed", Data: []byte(`{}`), Terminal: true})
	closedClock := newHub(t, Config{MaxRuns: 1, LateWindow: time.Millisecond})
	closedClock.Publish("done", "dave", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	closedClock.Publish("done", "dave", Item{Name: "run.completed", Data: []byte(`{}`), Terminal: true})
	time.Sleep(5 * time.Millisecond)
	if err := closedClock.AttachRecovered("old-1", "mallory", "mallory", true); err != nil {
		t.Fatalf("a closed run should make room: %v", err)
	}
}

// TestAdmissionBoundsEveryConnection is review finding F3 at the hub: a
// replay that will not follow live still holds its admission.
func TestAdmissionBoundsEveryConnection(t *testing.T) {
	hub := newHub(t, Config{MaxSubscribers: 2, SubscribersPerRun: 2, SubscribersPerPrincipal: 2})
	hub.Publish("run-1", "alice", Item{Name: "run.started", Data: []byte(`{}`), Start: true})
	hub.AttachRecovered("closed", "alice", "alice", true)
	first, err := hub.Admit("alice")
	if err != nil {
		t.Fatal(err)
	}
	replay, err := first.Subscribe("closed", "", nil)
	if err != nil || replay.Subscriber != nil {
		t.Fatalf("closed replay=%+v err=%v", replay, err)
	}
	second, _ := hub.Admit("alice")
	if _, err := hub.Admit("alice"); !errors.Is(err, ErrSaturated) {
		t.Fatalf("third connection err=%v", err)
	}
	if stats := hub.Stats(); stats.Readers != 2 || stats.Subscribers != 0 {
		t.Fatalf("stats=%+v", stats)
	}
	first.Release()
	first.Release()
	second.Release()
	if _, err := first.Subscribe("run-1", "", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("subscribe on a released admission err=%v", err)
	}
	if stats := hub.Stats(); stats.Readers != 0 {
		t.Fatalf("stats=%+v", stats)
	}
}
