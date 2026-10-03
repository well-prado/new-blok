package sse_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/internal/drainprobe"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/sse"
)

// heldTracker holds the tracker's store read once armed.
type heldTracker struct {
	*memorySubmitter
	held  *drainprobe.Held
	armed atomic.Bool
}

func (h *heldTracker) Settled(ctx context.Context, key string) (bool, error) {
	if h.armed.Load() {
		if err := h.held.Run(ctx); err != nil {
			return false, err
		}
	}
	return h.memorySubmitter.Settled(ctx, key)
}

// unverifiedStream returns a stream whose subscriber must ask the tracker
// before following it: its work is running, but the stream was evicted and
// recreated while the tracker was unreachable.
func unverifiedStream(t *testing.T, f *fixture, clock *manualClock) string {
	t.Helper()
	stream := evicted(t, f, clock, "k", false)
	f.submit.trackErr.Store(true)
	if status, body := f.start(t, "alice", "k", `{"item":"book"}`); status != http.StatusServiceUnavailable || body["error"] != "unavailable" {
		t.Fatalf("repeat without the tracker: %d %v", status, body)
	}
	f.submit.trackErr.Store(false)
	return stream
}

// TestSubscribeHoldsTheApplicationThroughItsTrackerRead: a subscription's
// pre-stream store read holds the application open, so Shutdown does not
// close the store under it (#177).
func TestSubscribeHoldsTheApplicationThroughItsTrackerRead(t *testing.T) {
	probe := &drainprobe.Probe{}
	clock := &manualClock{now: t0}
	tracker := &heldTracker{held: drainprobe.NewHeld(t, probe)}
	f := newFixtureWith(t, probe.Config(10*time.Second), sse.HubConfig{MaxStreams: 2, Retention: time.Minute, Clock: clock.Now}, func(e *sse.Endpoint) { e.Tracker = tracker }, nil)
	tracker.memorySubmitter = f.submit
	stream := unverifiedStream(t, f, clock)
	tracker.armed.Store(true)
	subscribed := make(chan *eventStream, 1)
	go func() {
		s, _, _ := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
		subscribed <- s
	}()
	tracker.held.Wait(t)
	stopped := drainprobe.Shutdown(f.app)
	time.Sleep(200 * time.Millisecond)
	if probe.Closed() || f.app.State() != app.DrainingState {
		t.Fatalf("Shutdown closed the store under a subscription's tracker read (state %s)", f.app.State())
	}
	tracker.held.Release()
	if err := <-stopped; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if s := <-subscribed; s != nil {
		s.close()
	}
	if probe.ClosedUnderWork() {
		t.Fatal("the store was closed while the tracker read it")
	}
}

// TestDrainTimeoutCancelsTheTrackerRead: when the drain times out, the
// subscription's tracker read is canceled and stops before the store is
// closed, and the subscriber is asked to retry (#177).
func TestDrainTimeoutCancelsTheTrackerRead(t *testing.T) {
	probe := &drainprobe.Probe{}
	clock := &manualClock{now: t0}
	tracker := &heldTracker{held: drainprobe.NewHeld(t, probe)}
	f := newFixtureWith(t, probe.Config(50*time.Millisecond), sse.HubConfig{MaxStreams: 2, Retention: time.Minute, Clock: clock.Now}, func(e *sse.Endpoint) { e.Tracker = tracker }, nil)
	tracker.memorySubmitter = f.submit
	stream := unverifiedStream(t, f, clock)
	tracker.armed.Store(true)
	type answer struct {
		first, second frame
		ended         bool
		refused       *http.Response
	}
	answered := make(chan answer, 1)
	go func() {
		s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
		if err != nil || refused != nil {
			answered <- answer{refused: refused}
			return
		}
		defer s.close()
		first, _ := s.next(5*time.Second, true)
		second, _ := s.next(5*time.Second, true)
		answered <- answer{first: first, second: second, ended: s.ended(5 * time.Second)}
	}()
	drainprobe.Abort(t, f.app, probe, tracker.held)
	got := <-answered
	if got.refused != nil || got.first.Retry == "" || got.second.Comment != "unavailable" || !got.ended {
		t.Fatalf("subscriber answered %+v; want a retry hint for unavailable", got)
	}
}

// heldSubmitter holds a start's submission.
type heldSubmitter struct{ held *drainprobe.Held }

func (h heldSubmitter) Submit(ctx context.Context, _ trigger.Submission) (bool, error) {
	return true, h.held.Run(ctx)
}

// TestDrainTimeoutCancelsAStart: when the drain times out under a start's
// submission, the submission is canceled and stops before the store is
// closed, and the caller is told to retry (#177).
func TestDrainTimeoutCancelsAStart(t *testing.T) {
	probe := &drainprobe.Probe{}
	work := drainprobe.NewHeld(t, probe)
	f := newFixtureWith(t, probe.Config(50*time.Millisecond), sse.HubConfig{}, func(e *sse.Endpoint) {
		e.Submit = heldSubmitter{held: work}
		e.SubmitTimeout = 30 * time.Second
	}, nil)
	type answer struct {
		status int
		retry  string
		body   map[string]any
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		request, _ := http.NewRequest(http.MethodPost, f.http.URL+"/orders", strings.NewReader(`{"item":"book"}`))
		request.Header.Set("Authorization", "Bearer alice")
		request.Header.Set("Idempotency-Key", "k")
		response, err := f.client.Do(request)
		if err != nil {
			answered <- answer{err: err}
			return
		}
		defer response.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(response.Body).Decode(&body)
		answered <- answer{status: response.StatusCode, retry: response.Header.Get("Retry-After"), body: body}
	}()
	drainprobe.Abort(t, f.app, probe, work)
	got := <-answered
	if got.err != nil || got.status != http.StatusServiceUnavailable || got.retry != "1" || got.body["error"] != "unavailable" {
		t.Fatalf("start answered %+v; want 503 unavailable with Retry-After", got)
	}
}
