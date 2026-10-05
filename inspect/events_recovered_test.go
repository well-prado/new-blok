package inspect_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
)

// countingOwnerSource is the journal's owner-only source, counting durable
// reads.
type countingOwnerSource struct {
	store *journal.Journal
	reads atomic.Int64
}

func (s *countingOwnerSource) ReadInspection(ctx context.Context, principal, runID, stepID string, offset, limit int, fields map[inspection.Field]bool, maxPayload int) (inspection.Run, []inspection.Step, int, error) {
	s.reads.Add(1)
	return s.store.ReadInspection(ctx, principal, runID, stepID, offset, limit, fields, maxPayload)
}

func (s *countingOwnerSource) RunOwner(ctx context.Context, reader, runID string) (string, error) {
	return s.store.RunOwner(ctx, reader, runID)
}

func snapshotPage(t *testing.T, frame sseFrame) inspection.Page {
	t.Helper()
	if frame.Event != "snapshot" {
		t.Fatalf("frame %q is not a snapshot", frame.Event)
	}
	var value struct {
		Page inspection.Page `json:"page"`
	}
	if err := json.Unmarshal([]byte(frame.Data), &value); err != nil {
		t.Fatalf("snapshot %q: %v", frame.Data, err)
	}
	return value.Page
}

// TestFollowedRecoveredRunEndsWhenItsSourceTurnsTerminal is #263: a reader
// following a run that no execution in this process publishes for (a
// cluster run, a crashed one) used to receive only heartbeats until
// MaxDuration while holding its admission. It now learns the run finished
// from the durable source within a bounded time, receives the final
// reconstruction and an end, and is released. While the run does not
// change, the reader is told nothing new, the durable reads stay bounded by
// the poll interval, and the field policy is evaluated once.
func TestFollowedRecoveredRunEndsWhenItsSourceTurnsTerminal(t *testing.T) {
	store, closeJournal := openJournal(t, filepath.Join(t.TempDir(), "recovered.db"))
	defer closeJournal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	admission, err := store.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "followed", Workflow: "quote", ArtifactDigest: "sha256:followed", Input: []byte(`{"sku":"coffee","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	source := &countingOwnerSource{store: store}
	var policies atomic.Int64
	live := newLiveApp(t, liveConfig{
		// Capture stays off: the policy grants every field, yet no payload
		// may reach the reader, from a live frame or a reconstruction.
		handler: inspect.EventHandlerConfig{Source: source, MaxDuration: time.Minute, Heartbeat: 250 * time.Millisecond, Policy: func(string) inspection.Policy {
			policies.Add(1)
			return allFields()
		}},
		nodes: map[string]node.Any{"test/gate": (&gateNode{}).node(t)},
	})
	url := live.eventsURL(admission.RunID)
	response, body, stream := openStream(t, ctx, url, "alice", "")
	if stream == nil {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
	defer stream.close()
	opening := stream.until(t, "snapshot", 5*time.Second)
	if got := strings.Join(frameNames(opening), ","); got != "gap,snapshot" {
		t.Fatalf("opening frames=%s", got)
	}
	if page := snapshotPage(t, opening[1]); page.Run.Status != inspection.StatusRunning {
		t.Fatalf("opening reconstruction=%+v", page.Run)
	}
	// Nothing changes for a while: no new frame, and polling is bounded.
	const idle = 3 * time.Second
	time.Sleep(idle)
	if reads := source.reads.Load(); reads > 1+int64(idle/time.Second) {
		t.Fatalf("%d durable reads in %s: polling is not bounded", reads, idle)
	}
	if err := store.CompleteRun(ctx, admission.RunID, json.RawMessage(`{"totalCents":1500}`)); err != nil {
		t.Fatal(err)
	}
	completed := time.Now()
	closing := stream.until(t, "end", 10*time.Second)
	elapsed := time.Since(completed)
	if got := strings.Join(frameNames(closing), ","); got != "snapshot,end" {
		t.Fatalf("frames after completion=%s: an unchanged run must not be resent", got)
	}
	final := snapshotPage(t, closing[0])
	if final.Run.Status != inspection.StatusCompleted || len(final.Run.Output) != 0 || strings.Contains(closing[0].Data, "1500") {
		t.Fatalf("final reconstruction=%+v data=%s", final.Run, closing[0].Data)
	}
	if rest := stream.rest(t, 5*time.Second); len(rest) != 0 {
		t.Fatalf("frames after end=%v", frameNames(rest))
	}
	waitFor(t, func() bool {
		stats := live.stream.Hub().Stats()
		return stats.Readers == 0 && stats.Subscribers == 0
	})
	if calls := policies.Load(); calls != 1 {
		t.Fatalf("policy evaluated %d times for one connection", calls)
	}
	// The attachment is closed: a later reader gets the reconstruction and
	// an end at once instead of following.
	laterStarted := time.Now()
	_, _, later := openStream(t, ctx, url, "alice", "")
	if later == nil {
		t.Fatal("later reader refused")
	}
	if got := strings.Join(frameNames(later.rest(t, 5*time.Second)), ","); got != "gap,snapshot,end" {
		t.Fatalf("later reader frames=%s", got)
	}
	// Well under one poll interval (2 s): it did not wait to rediscover it.
	if took := time.Since(laterStarted); took > 1500*time.Millisecond {
		t.Fatalf("later reader of a finished recovered run took %s", took)
	}
	t.Logf("end %s after the durable run completed (MaxDuration %s); %d durable reads", elapsed, time.Minute, source.reads.Load())
}

// TestIdleUnfinishedRunsDoNotKeepRecoveredReadsOut is the #263 addendum:
// runs that started here and never finished (suspended, or whose execution
// stopped) used to keep a saturated hub's recovered reads out forever, as a
// recovered read may not displace a live run. Past the hub's idle bound an
// unfollowed unfinished run becomes recyclable; before it, it is protected.
func TestIdleUnfinishedRunsDoNotKeepRecoveredReadsOut(t *testing.T) {
	store, closeJournal := openJournal(t, filepath.Join(t.TempDir(), "idle.db"))
	defer closeJournal()
	ctx := context.Background()
	admission, err := store.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "old", Workflow: "quote", ArtifactDigest: "sha256:old", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteRun(ctx, admission.RunID, json.RawMessage(`1`)); err != nil {
		t.Fatal(err)
	}
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	clock := func() time.Time { return time.Unix(0, now.Load()) }
	live := newLiveApp(t, liveConfig{
		stream:  inspect.EventStreamConfig{Hub: event.Config{MaxRuns: 2, Clock: clock}},
		handler: inspect.EventHandlerConfig{Source: store},
		nodes:   map[string]node.Any{"test/gate": (&gateNode{}).node(t)},
	})
	for i := 0; i < 2; i++ {
		live.stream.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: fmt.Sprintf("carol-idle-%d", i), Principal: "carol", At: clock()})
	}
	read := func() string {
		response, body, stream := openStream(t, ctx, live.eventsURL(admission.RunID), "alice", "")
		if stream == nil {
			return fmt.Sprintf("%d %s", response.StatusCode, strings.TrimSpace(body))
		}
		frames := stream.rest(t, 5*time.Second)
		if len(frames) == 0 {
			return strings.Join(stream.Comments(), ",")
		}
		return strings.Join(frameNames(frames), ",")
	}
	now.Add(int64(time.Minute))
	if got := read(); got != "saturated" {
		t.Fatalf("within the idle bound a recovered read displaced a live run: %s", got)
	}
	now.Add(int64(time.Hour))
	if got := read(); got != "gap,snapshot,end" {
		t.Fatalf("idle unfinished runs still keep the recovered read out: %s; stats=%+v", got, live.stream.Hub().Stats())
	}
}

// TestRecoveredPollIsBounded: the poll interval is an explicit bound with a
// floor, so no configuration can make a follower read durable state in a
// tight loop.
func TestRecoveredPollIsBounded(t *testing.T) {
	stream, err := inspect.NewEventStream(inspect.EventStreamConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, poll := range []time.Duration{-time.Second, inspect.MinEventRecoveredPoll - time.Millisecond, inspect.MaxEventRecoveredPoll + time.Second} {
		if _, err := inspect.NewEventHandler(stream, inspect.EventHandlerConfig{Authenticate: principalFromHeader, RecoveredPoll: poll}); err == nil {
			t.Errorf("RecoveredPoll %s accepted", poll)
		}
	}
	for _, poll := range []time.Duration{0, inspect.MinEventRecoveredPoll, inspect.MaxEventRecoveredPoll} {
		if _, err := inspect.NewEventHandler(stream, inspect.EventHandlerConfig{Authenticate: principalFromHeader, RecoveredPoll: poll}); err != nil {
			t.Errorf("RecoveredPoll %s refused: %v", poll, err)
		}
	}
}
