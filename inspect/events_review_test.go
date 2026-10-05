package inspect_test

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
)

// teamSource is a broader durable source than the owner-only journal: a
// teammate may read a run, and it can say who owns the run. It reads the
// real journal as the run's owner after its own team check.
type teamSource struct {
	store  *journal.Journal
	owners map[string]string // run → owner
	team   map[string]bool
}

func (s teamSource) ReadInspection(ctx context.Context, principal, runID, stepID string, offset, limit int, fields map[inspection.Field]bool, maxPayload int) (inspection.Run, []inspection.Step, int, error) {
	owner := s.owners[runID]
	if !s.team[principal] || !s.team[owner] {
		return inspection.Run{}, nil, 0, journal.ErrNotFound
	}
	return s.store.ReadInspection(ctx, owner, runID, stepID, offset, limit, fields, maxPayload)
}

// ownerTeamSource also reports the durable owner of a run it lets the
// reader see.
type ownerTeamSource struct{ teamSource }

func (s ownerTeamSource) RunOwner(_ context.Context, reader, runID string) (string, error) {
	owner := s.owners[runID]
	if !s.team[reader] || !s.team[owner] {
		return "", journal.ErrNotFound
	}
	return owner, nil
}

func teamAuthorize(reader, owner string) error {
	if (reader == "alice" || reader == "bob") && (owner == "alice" || owner == "bob") {
		return nil
	}
	return event.ErrUnauthorized
}

// TestRecoveredRunIsOwnedByItsDurableOwnerNotItsFirstReader is review
// finding F1: a reader attaching a run the hub does not hold must never
// become its owner. Otherwise the engine's trusted publications for that run
// are rejected (silent loss) and the real owner is locked out.
func TestRecoveredRunIsOwnedByItsDurableOwnerNotItsFirstReader(t *testing.T) {
	for _, item := range []struct {
		name      string
		source    func(teamSource) inspection.Source
		authorize event.Authorizer
		bobStatus int
		// bobFollows: bob's first connection follows the run live; otherwise
		// it gets the durable reconstruction only and must reconnect.
		bobFollows bool
	}{
		{"owner-reporting source, team authorization", func(s teamSource) inspection.Source { return ownerTeamSource{s} }, teamAuthorize, http.StatusOK, true},
		{"owner-reporting source, owner-only authorization", func(s teamSource) inspection.Source { return ownerTeamSource{s} }, nil, http.StatusNotFound, false},
		{"source that cannot name the owner", func(s teamSource) inspection.Source { return s }, teamAuthorize, http.StatusOK, false},
	} {
		t.Run(item.name, func(t *testing.T) {
			store, closeJournal := openJournal(t, filepath.Join(t.TempDir(), "team.db"))
			defer closeJournal()
			admission, err := store.Admit(context.Background(), journal.AdmissionRequest{Principal: "alice", RequestKey: "team", Workflow: "quote", ArtifactDigest: "sha256:team", Input: []byte(`{"sku":"held","quantity":1}`)})
			if err != nil {
				t.Fatal(err)
			}
			source := item.source(teamSource{store: store, owners: map[string]string{admission.RunID: "alice"}, team: map[string]bool{"alice": true, "bob": true}})
			gate := &gateNode{hold: "held", release: make(chan struct{}), entered: make(chan struct{}, 1)}
			live := newLiveApp(t, liveConfig{
				stream:  inspect.EventStreamConfig{Hub: event.Config{LateWindow: 10 * time.Millisecond}},
				handler: inspect.EventHandlerConfig{Source: source, Authorize: item.authorize},
				nodes:   map[string]node.Any{"test/gate": gate.node(t)},
			})
			release := sync.OnceFunc(func() { close(gate.release) })
			t.Cleanup(release)
			url := live.eventsURL(admission.RunID)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// bob, a teammate, reads alice's run before it publishes anything.
			response, _, bob := openStream(t, ctx, url, "bob", "")
			if response.StatusCode != item.bobStatus {
				t.Fatalf("bob status=%d want %d", response.StatusCode, item.bobStatus)
			}
			if bob != nil {
				if first, _ := bob.next(t, 5*time.Second); first.Event != event.GapName {
					t.Fatalf("bob first frame=%+v", first)
				}
			}
			result := live.start(admission.RunID, "alice", quote.Input{SKU: "held", Quantity: 1})
			select {
			case <-gate.entered:
			case got := <-result:
				t.Fatalf("run ended before its held step: %v", got)
			case <-time.After(10 * time.Second):
				t.Fatal("run did not reach its held step")
			}
			alice := awaitStream(t, ctx, url, "alice")
			if frames := alice.until(t, "step.processing", 5*time.Second); !strings.Contains(strings.Join(frameNames(frames), ","), "run.started") {
				t.Fatalf("alice did not receive her own run live: %v", frameNames(frames))
			}
			switch {
			case bob != nil && item.bobFollows:
				// bob attached the run under its durable owner, so the
				// engine's publications reach him live.
				if frames := bob.until(t, "step.processing", 5*time.Second); !strings.Contains(strings.Join(frameNames(frames), ","), "run.started") {
					t.Fatalf("bob frames=%v", frameNames(frames))
				}
			case bob != nil:
				// bob got the reconstruction only, ending explicitly; on
				// reconnect he follows the now-live run as a teammate.
				if frames := bob.rest(t, 5*time.Second); strings.Join(frameNames(frames), ",") != "snapshot,end" {
					t.Fatalf("bob durable-only frames=%v", frameNames(frames))
				}
				reconnected := awaitStream(t, ctx, url, "bob")
				reconnected.until(t, "step.processing", 5*time.Second)
			}
			release()
			awaitResult(t, result)
			if stats := live.stream.Hub().Stats(); stats.Rejected != 0 {
				t.Fatalf("the engine's trusted publications were rejected: %+v", stats)
			}
		})
	}
}

// TestRecoveredReadsCannotEvictOtherPrincipalsLiveRuns is review finding F2:
// one principal opening many of its own old runs recycles its own recovered
// slots; it never pushes another principal's live run out of the hub.
func TestRecoveredReadsCannotEvictOtherPrincipalsLiveRuns(t *testing.T) {
	store, closeJournal := openJournal(t, filepath.Join(t.TempDir(), "budget.db"))
	defer closeJournal()
	var old []string
	for i := 0; i < 6; i++ {
		admission, err := store.Admit(context.Background(), journal.AdmissionRequest{Principal: "mallory", RequestKey: fmt.Sprintf("old-%d", i), Workflow: "quote", ArtifactDigest: "sha256:old", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.CompleteRun(context.Background(), admission.RunID, []byte(`1`)); err != nil {
			t.Fatal(err)
		}
		old = append(old, admission.RunID)
	}
	live := newLiveApp(t, liveConfig{
		stream:  inspect.EventStreamConfig{Hub: event.Config{MaxRuns: 4}},
		handler: inspect.EventHandlerConfig{Source: store},
		nodes:   map[string]node.Any{"test/gate": (&gateNode{}).node(t)},
	})
	now := time.Now()
	for i := 0; i < 3; i++ {
		live.stream.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: fmt.Sprintf("carol-live-%d", i), Principal: "carol", At: now})
	}
	for _, runID := range old {
		_, _, stream := openStream(t, context.Background(), live.eventsURL(runID), "mallory", "")
		if stream == nil {
			continue
		}
		stream.rest(t, 5*time.Second)
	}
	for i := 0; i < 3; i++ {
		runID := fmt.Sprintf("carol-live-%d", i)
		ctx, cancel := context.WithCancel(context.Background())
		response, body, stream := openStream(t, ctx, live.eventsURL(runID), "carol", "")
		if stream == nil {
			cancel()
			t.Fatalf("carol's live run %s was pushed out: %d %s", runID, response.StatusCode, body)
		}
		if first, _ := stream.next(t, 5*time.Second); first.Event != "run.started" {
			t.Fatalf("carol's live run %s first frame=%+v", runID, first)
		}
		cancel()
		stream.close()
	}
}

// blockingSource counts concurrent durable reads and holds each one.
type blockingSource struct {
	inspection.Source
	active, peak atomic.Int64
	release      chan struct{}
	once         sync.Once
}

func (s *blockingSource) ReadInspection(ctx context.Context, principal, runID, stepID string, offset, limit int, fields map[inspection.Field]bool, maxPayload int) (inspection.Run, []inspection.Step, int, error) {
	current := s.active.Add(1)
	defer s.active.Add(-1)
	for {
		peak := s.peak.Load()
		if current <= peak || s.peak.CompareAndSwap(peak, current) {
			break
		}
	}
	select {
	case <-s.release:
	case <-ctx.Done():
	}
	return inspection.Run{}, nil, 0, journal.ErrNotFound
}

// TestReaderAdmissionPrecedesDurableReads is review finding F3: reader
// limits apply to every connection, including replay-only ones and those
// that need a durable read, and they are checked before that read.
func TestReaderAdmissionPrecedesDurableReads(t *testing.T) {
	source := &blockingSource{release: make(chan struct{})}
	live := newLiveApp(t, liveConfig{
		stream:  inspect.EventStreamConfig{Hub: event.Config{MaxSubscribers: 2, SubscribersPerRun: 2, SubscribersPerPrincipal: 2}},
		handler: inspect.EventHandlerConfig{Source: source},
		nodes:   map[string]node.Any{"test/gate": (&gateNode{}).node(t)},
	})
	var wg sync.WaitGroup
	statuses := make(chan string, 6)
	for i := 0; i < 6; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			response, body, stream := openStream(t, context.Background(), live.eventsURL(fmt.Sprintf("unknown-%d", i)), "alice", "")
			if stream != nil {
				stream.rest(t, 10*time.Second)
				statuses <- fmt.Sprintf("200 %v", stream.Comments())
				return
			}
			statuses <- fmt.Sprintf("%d %s", response.StatusCode, strings.TrimSpace(body))
		}(i)
	}
	waitFor(t, func() bool { return source.active.Load() >= 2 })
	time.Sleep(200 * time.Millisecond)
	source.once.Do(func() { close(source.release) })
	wg.Wait()
	close(statuses)
	var got []string
	for status := range statuses {
		got = append(got, status)
	}
	if peak := source.peak.Load(); peak > 2 {
		t.Fatalf("%d durable reads ran at once with a reader limit of 2; statuses=%v", peak, got)
	}
}

// TestErrorLabelsRequireTheErrorField is review finding F4: a failing run's
// classified error labels reach only a reader whose policy grants them.
func TestErrorLabelsRequireTheErrorField(t *testing.T) {
	readers := map[string]inspection.Policy{"alice": {Fields: map[inspection.Field]bool{inspection.FieldError: true}}, "reviewer": {}}
	live := newLiveApp(t, liveConfig{
		stream:  inspect.EventStreamConfig{Hub: event.Config{LateWindow: 10 * time.Millisecond}},
		handler: inspect.EventHandlerConfig{Policy: func(principal string) inspection.Policy { return readers[principal] }, Authorize: func(reader, owner string) error { return nil }},
		nodes:   map[string]node.Any{"test/gate": (&gateNode{}).node(t)},
	})
	awaitResult(t, live.start("failing-run", "alice", quote.Input{SKU: "failure", Quantity: 1}))
	for reader, want := range map[string]bool{"alice": true, "reviewer": false} {
		_, _, stream := openStream(t, context.Background(), live.eventsURL("failing-run"), reader, "")
		labelled := false
		for _, frame := range stream.rest(t, 5*time.Second) {
			if strings.Contains(frame.Data, "errorCode") || strings.Contains(frame.Data, "errorClass") {
				labelled = true
			}
		}
		if labelled != want {
			t.Fatalf("%s saw error labels=%v want %v", reader, labelled, want)
		}
	}
}
