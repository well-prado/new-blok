package sse

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/trigger"
)

type streamsFixture struct {
	CursorCases []struct {
		Name         string `json:"name"`
		RetainEvents int    `json:"retainEvents"`
		Incarnation  uint64 `json:"incarnation"`
		Published    int    `json:"published"`
		Finished     bool   `json:"finished"`
		Cursor       string `json:"cursor"`
		Expect       struct {
			Outcome string   `json:"outcome"`
			Gap     string   `json:"gap"`
			Missed  uint64   `json:"missed"`
			GapID   uint64   `json:"gapID"`
			Replay  []uint64 `json:"replay"`
		} `json:"expect"`
	} `json:"cursorCases"`
	EventCases []struct {
		Name      string `json:"name"`
		Type      string `json:"type"`
		Data      string `json:"data"`
		DataBytes int    `json:"dataBytes"`
		Wire      string `json:"wire"`
		Error     string `json:"error"`
	} `json:"eventCases"`
	Expected struct {
		Output int `json:"output"`
		Errors int `json:"errors"`
	} `json:"expected"`
}

var alice = trigger.Principal{ID: "alice"}

// TestStreamsFixture runs the predeclared cursor and framing cases: every
// cursor outcome (replay, gap with reason and missed count, finished,
// invalid) and every event shape (framed, or refused as invalid_event).
func TestStreamsFixture(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "sse", "streams.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f streamsFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	output, failures := 0, 0
	for _, tc := range f.CursorCases {
		hub, err := NewHub(HubConfig{RetainEvents: tc.RetainEvents})
		if err != nil {
			t.Fatal(err)
		}
		stream := StreamID("fixture")
		// An earlier incarnation was evicted: the hub has created streams
		// before this one.
		for hub.incarnations+1 < max(tc.Incarnation, 1) {
			hub.incarnations++
		}
		if _, err := hub.open(stream, alice, "key"); err != nil {
			t.Fatal(err)
		}
		hub.settle(stream, true)
		for i := 0; i < tc.Published; i++ {
			if _, err := hub.Publish(stream, Event{Type: "progress", Data: json.RawMessage(strconv.Itoa(i))}); err != nil {
				t.Fatal(err)
			}
		}
		if tc.Finished {
			if _, err := hub.Finish(stream, Event{Type: "result"}); err != nil {
				t.Fatal(err)
			}
		}
		cursor := tc.Cursor
		switch {
		case strings.HasPrefix(cursor, "seq:"):
			cursor = hub.epoch + "." + strconv.FormatUint(hub.streams[stream].incarnation, 10) + ":" + strings.TrimPrefix(cursor, "seq:")
		case strings.HasPrefix(cursor, "inc:"):
			incarnation, seq, _ := strings.Cut(strings.TrimPrefix(cursor, "inc:"), ":")
			cursor = hub.epoch + "." + incarnation + ":" + seq
		case strings.HasPrefix(cursor, "other:"):
			other := "0123456789abcdef"
			if other == hub.epoch {
				other = "fedcba9876543210"
			}
			cursor = other + ".1:" + strings.TrimPrefix(cursor, "other:")
		case strings.HasPrefix(cursor, "raw:"):
			cursor = strings.TrimPrefix(cursor, "raw:")
		}
		result, err := hub.subscribe(stream, alice, cursor, SamePrincipal, 8, 8, nil)
		if tc.Expect.Outcome == "invalid_cursor" {
			if !errors.Is(err, errInvalidCursor) {
				t.Fatalf("%s: %v, want invalid_cursor", tc.Name, err)
			}
			failures++
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", tc.Name, err)
		}
		var replay []uint64
		for _, event := range result.replay {
			replay = append(replay, event.seq)
		}
		gap, missed, gapID := "", uint64(0), ""
		if result.gap != nil {
			gap, missed, gapID = result.gap.Reason, result.gap.Missed, result.gapID
		}
		wantGapID := ""
		if tc.Expect.Gap != "" {
			wantGapID = hub.id(hub.streams[stream].incarnation, tc.Expect.GapID)
		}
		if fmt.Sprint(replay) != fmt.Sprint(tc.Expect.Replay) || gap != tc.Expect.Gap || missed != tc.Expect.Missed || gapID != wantGapID || result.done != (tc.Expect.Outcome == "finished") {
			t.Fatalf("%s: replay %v gap %q missed %d gapID %q done %v, want %+v", tc.Name, replay, gap, missed, gapID, result.done, tc.Expect)
		}
		if result.sub != nil {
			hub.unsubscribe(stream, result.sub, ReasonClientClosed)
		}
		output++
	}
	hub, err := NewHub(HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range f.EventCases {
		data := json.RawMessage(tc.Data)
		if tc.DataBytes > 0 {
			data = json.RawMessage(`"` + strings.Repeat("x", tc.DataBytes) + `"`)
		}
		event, err := hub.event(Event{Type: tc.Type, Data: data})
		if tc.Error != "" {
			if !errors.Is(err, ErrInvalidEvent) {
				t.Fatalf("%s: %v, want invalid_event", tc.Name, err)
			}
			failures++
			continue
		}
		if err != nil {
			t.Fatalf("%s: %v", tc.Name, err)
		}
		var frame bytes.Buffer
		writeEvent(&frame, "e:1", event.typ, event.data)
		if want := "id: e:1\n" + tc.Wire + "\n"; frame.String() != want {
			t.Fatalf("%s: wire %q, want %q", tc.Name, frame.String(), want)
		}
		output++
	}
	if output != f.Expected.Output || failures != f.Expected.Errors {
		t.Fatalf("output=%d errors=%d, want %+v", output, failures, f.Expected)
	}
}

type steppedClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *steppedClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *steppedClock) add(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }
func streamName(i int) string               { return StreamID(strconv.Itoa(i)) }

// TestEvictionOrder: a full hub evicts a finished stream past retention,
// then an unfinished stream idle past retention, then the least recently
// used finished stream; it never evicts an unfinished stream in use or a
// stream with a subscriber, and refuses the new stream instead.
func TestEvictionOrder(t *testing.T) {
	clock := &steppedClock{now: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)}
	hub, err := NewHub(HubConfig{MaxStreams: 3, Retention: time.Minute, Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	finish := func(i int) {
		if _, err := hub.Finish(streamName(i), Event{Type: "result"}); err != nil {
			t.Fatal(err)
		}
	}
	progress := func(i int) error {
		_, err := hub.Publish(streamName(i), Event{Type: "progress"})
		return err
	}
	// 0 and 1 finish, 2 is running; all three are recent.
	finish(0)
	clock.add(time.Second)
	finish(1)
	clock.add(time.Second)
	if err := progress(2); err != nil {
		t.Fatal(err)
	}
	present := func(want ...int) {
		t.Helper()
		var got []int
		for i := 0; i < 10; i++ {
			if _, ok := hub.streams[streamName(i)]; ok {
				got = append(got, i)
			}
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("streams %v, want %v", got, want)
		}
	}
	// No stream is stale: the least recently used finished one (0) goes.
	if err := progress(3); err != nil {
		t.Fatal(err)
	}
	present(1, 2, 3)
	// A subscriber protects 1; 2 and 3 are running and recent: refused.
	sub, err := hub.subscribe(streamName(1), alice, "", func(trigger.Principal, trigger.Principal) error { return nil }, 1, 1, nil)
	if !errors.Is(err, errUnknownStream) {
		t.Fatalf("a stream nobody opened is unknown to subscribers: %v %+v", err, sub)
	}
	for _, i := range []int{1, 3} {
		if _, err := hub.open(streamName(i), alice, "key"); err != nil {
			t.Fatal(err)
		}
		hub.settle(streamName(i), true)
	}
	sub, err = hub.subscribe(streamName(3), alice, "", SamePrincipal, 1, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := progress(4); err != nil {
		t.Fatalf("finished 1 should be evicted first: %v", err)
	}
	present(2, 3, 4)
	if err := progress(5); !errors.Is(err, ErrHubFull) {
		t.Fatalf("running streams were evicted: %v", err)
	}
	// Past retention, the running 2 is stale and goes; 3 has a subscriber.
	clock.add(2 * time.Minute)
	if err := progress(4); err != nil {
		t.Fatal(err)
	}
	if err := progress(5); err != nil {
		t.Fatal(err)
	}
	present(3, 4, 5)
	hub.unsubscribe(streamName(3), sub.sub, ReasonClientClosed)
	if stats := hub.Stats(); stats.EvictedStreams != 3 || stats.RefusedEvents != 1 {
		t.Fatalf("stats %+v", stats)
	}
}

// TestRetentionBytesAreBounded: however much is published, one stream
// retains at most RetainEvents events and RetainBytes bytes.
func TestRetentionBytesAreBounded(t *testing.T) {
	hub, err := NewHub(HubConfig{RetainEvents: 100, RetainBytes: 64 << 10, MaxEventBytes: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamName(0)
	for i := 0; i < 10000; i++ {
		size := 1 + i%(4<<10-2)
		if _, err := hub.Publish(stream, Event{Type: "progress", Data: json.RawMessage(`"` + strings.Repeat("x", size-2+1) + `"`)}); err != nil {
			t.Fatal(err)
		}
		s := hub.streams[stream]
		total := 0
		for _, e := range s.events {
			total += len(e.data)
		}
		if len(s.events) > 100 || s.bytes > 64<<10 || total != s.bytes {
			t.Fatalf("after %d events: %d retained, %d bytes (counted %d)", i+1, len(s.events), s.bytes, total)
		}
	}
	for _, bad := range []HubConfig{
		{MaxStreams: MaxStreamsLimit + 1},
		{RetainEvents: RetainEventsLimit + 1},
		{RetainBytes: RetainBytesLimit + 1},
		{MaxEventBytes: 2 << 20},
		{MaxEventBytes: 128 << 10, RetainBytes: 64 << 10},
		{MaxStreams: 1 << 16, RetainBytes: 1 << 20},
	} {
		if _, err := NewHub(bad); err == nil {
			t.Fatalf("%+v accepted", bad)
		}
	}
}

// TestOpenAndSettle: a stream is owned by one principal; a principal has a
// stream quota; a start that committed nothing removes only a stream no
// other start committed; and a stream with a start in flight is never
// evicted.
func TestOpenAndSettle(t *testing.T) {
	hub, err := NewHub(HubConfig{MaxStreams: 3, MaxStreamsPerOwner: 2})
	if err != nil {
		t.Fatal(err)
	}
	mallory := trigger.Principal{ID: "mallory"}
	if _, err := hub.open(streamName(0), alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(streamName(0), true)
	if _, err := hub.open(streamName(0), mallory, "key"); !errors.Is(err, errOwnerMismatch) {
		t.Fatalf("another principal opened alice's stream: %v", err)
	}
	if _, err := hub.open(streamName(1), alice, "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.open(streamName(2), alice, "key"); !errors.Is(err, errOwnerQuota) {
		t.Fatalf("alice exceeded her quota: %v", err)
	}
	// Two starts race on stream 1: one commits, the other fails.
	if _, err := hub.open(streamName(1), alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(streamName(1), true)
	hub.settle(streamName(1), false)
	if _, ok := hub.streams[streamName(1)]; !ok {
		t.Fatal("a failed start removed a stream another start committed")
	}
	// A start that commits nothing, alone, leaves no stream behind.
	if _, err := hub.open(streamName(2), mallory, "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Publish(streamName(9), Event{Type: "progress"}); !errors.Is(err, ErrHubFull) {
		t.Fatalf("a stream with a start in flight was evicted: %v", err)
	}
	hub.settle(streamName(2), false)
	if _, ok := hub.streams[streamName(2)]; ok || hub.owned["mallory"] != 0 {
		t.Fatalf("a failed start left its stream: %v %v", ok, hub.owned)
	}
	if err := SamePrincipal(trigger.Principal{}, trigger.Principal{}); err == nil {
		t.Fatal("an empty principal may follow a stream with an empty owner")
	}
}

// TestRecreatedStreamGivesAGap: a stream evicted and recreated in the same
// epoch starts a new incarnation, so a cursor from the old one gets a gap
// instead of silently skipping the new events.
func TestRecreatedStreamGivesAGap(t *testing.T) {
	clock := &steppedClock{now: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)}
	hub, err := NewHub(HubConfig{MaxStreams: 1, Retention: time.Minute, Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamName(0)
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(stream, true)
	var old string
	for i := 0; i < 5; i++ {
		if old, err = hub.Publish(stream, Event{Type: "progress"}); err != nil {
			t.Fatal(err)
		}
	}
	clock.add(2 * time.Minute)
	if _, err := hub.Publish(streamName(1), Event{Type: "progress"}); err != nil {
		t.Fatal(err)
	}
	clock.add(2 * time.Minute)
	// The worker keeps publishing: the stream comes back, then a repeated
	// start registers its owner again.
	for i := 0; i < 6; i++ {
		if _, err := hub.Publish(stream, Event{Type: "progress"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(stream, true)
	result, err := hub.subscribe(stream, alice, old, SamePrincipal, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.gap == nil || result.gap.Reason != GapRetention || len(result.replay) != 6 {
		t.Fatalf("cursor %s from the evicted incarnation: gap %+v, %d replayed; want a retention gap and all 6 new events", old, result.gap, len(result.replay))
	}
	hub.unsubscribe(stream, result.sub, ReasonClientClosed)
}

// TestRealOutcomeReplacesExpired: a repeated start may expire a stream
// between the job's commit and the worker's Finish; the real outcome then
// replaces the notice, and a subscriber that saw the notice gets it on
// reconnecting.
func TestRealOutcomeReplacesExpired(t *testing.T) {
	hub, err := NewHub(HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamName(0)
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(stream, true)
	hub.expire(stream)
	seen, err := hub.subscribe(stream, alice, "", SamePrincipal, 8, 8, nil)
	if err != nil || len(seen.replay) != 1 || seen.replay[0].typ != TypeExpired {
		t.Fatalf("expired: %+v %v", seen, err)
	}
	if _, err := hub.Finish(stream, Event{Type: "result", Data: json.RawMessage(`{"total":3}`)}); err != nil {
		t.Fatalf("the real outcome was refused after expired: %v", err)
	}
	if _, err := hub.Finish(stream, Event{Type: "result"}); !errors.Is(err, ErrFinished) {
		t.Fatalf("a second real outcome was accepted: %v", err)
	}
	again, err := hub.subscribe(stream, alice, seen.ids[0], SamePrincipal, 8, 8, nil)
	if err != nil || len(again.replay) != 1 || again.replay[0].typ != "result" || !again.done {
		t.Fatalf("reconnecting from expired: %+v %v", again, err)
	}
}

// TestFailedStartSendsAwayItsSubscribers: a subscriber that attached while
// a start was in flight does not keep the stream, or its owner's quota,
// once that start fails: the stream goes and the subscriber is told.
func TestFailedStartSendsAwayItsSubscribers(t *testing.T) {
	hub, err := NewHub(HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamName(0)
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	result, err := hub.subscribe(stream, alice, "", SamePrincipal, 8, 8, nil)
	if err != nil {
		t.Fatalf("a subscriber waiting on an in-flight start was refused: %v", err)
	}
	hub.settle(stream, false)
	select {
	case <-result.sub.done:
	default:
		t.Fatal("the subscriber was not told the start failed")
	}
	if _, ok := hub.streams[stream]; ok || hub.owned[alice.ID] != 0 || result.sub.reason != ReasonNoWork {
		t.Fatalf("a stream with no work outlived its failed start: owned=%v reason=%q", hub.owned, result.sub.reason)
	}
}

// TestUnverifiedStreams: a stream a repeated start recreated is checked with
// the tracker before anyone follows it, but not while a start is in flight
// (its key may not be committed yet); a publication or an accepted start
// verifies it; expire acts only on an unverified stream.
func TestUnverifiedStreams(t *testing.T) {
	hub, err := NewHub(HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamName(0)
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	if _, unverified := hub.unverified(stream, false); unverified {
		t.Fatal("a subscriber may check while a start is in flight")
	}
	if key, unverified := hub.unverified(stream, true); !unverified || key != "key" {
		t.Fatal("a committed start may not check its own stream")
	}
	hub.settle(stream, true)
	if _, err := hub.subscribe(stream, alice, "", SamePrincipal, 8, 8, nil); !errors.Is(err, errUnverified) {
		t.Fatalf("an unverified stream was followed unchecked: %v", err)
	}
	hub.verify(stream)
	hub.expire(stream)
	result, err := hub.subscribe(stream, alice, "", SamePrincipal, 8, 8, nil)
	if err != nil || result.done {
		t.Fatalf("a verified stream was expired or refused: %+v %v", result, err)
	}
	hub.unsubscribe(stream, result.sub, ReasonClientClosed)
	// A worker publishes before the start is repeated: the publication
	// shows someone will end the stream, so the repeat needs no check.
	other := streamName(1)
	if _, err := hub.Publish(other, Event{Type: "progress"}); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.open(other, alice, "key2"); err != nil {
		t.Fatal(err)
	}
	hub.settle(other, true)
	if _, unverified := hub.unverified(other, false); unverified {
		t.Fatal("a stream someone publishes to is unverified")
	}
}

// TestStreamsInUseAreNotEvicted: neither a start in flight nor a subscriber
// lets the hub evict a stream, even one stale or finished; once the
// subscriber leaves, the stale stream can go.
func TestStreamsInUseAreNotEvicted(t *testing.T) {
	clock := &steppedClock{now: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)}
	hub, err := NewHub(HubConfig{MaxStreams: 1, Retention: time.Minute, Clock: clock.Now})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamName(0)
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(stream, true)
	if _, err := hub.Finish(stream, Event{Type: "result"}); err != nil {
		t.Fatal(err)
	}
	clock.add(2 * time.Minute)
	// A repeated start is in flight on the stale, finished stream.
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.Publish(streamName(1), Event{Type: "progress"}); !errors.Is(err, ErrHubFull) {
		t.Fatalf("a stream with a start in flight was evicted: %v", err)
	}
	hub.settle(stream, true)
	// Running work with a subscriber, past retention.
	running := streamName(2)
	if _, err := hub.Publish(running, Event{Type: "progress"}); err != nil {
		t.Fatal(err)
	}
	if _, err := hub.open(running, alice, "key2"); err != nil {
		t.Fatal(err)
	}
	hub.settle(running, true)
	result, err := hub.subscribe(running, alice, "", SamePrincipal, 8, 8, nil)
	if err != nil {
		t.Fatal(err)
	}
	clock.add(2 * time.Minute)
	if _, err := hub.Publish(streamName(3), Event{Type: "progress"}); !errors.Is(err, ErrHubFull) {
		t.Fatalf("a stream with a subscriber was evicted: %v", err)
	}
	hub.unsubscribe(running, result.sub, ReasonClientClosed)
	clock.add(2 * time.Minute)
	if _, err := hub.Publish(streamName(3), Event{Type: "progress"}); err != nil {
		t.Fatalf("a stale stream its subscriber left was kept: %v", err)
	}
}

// TestReplacedExpiredIsNotAGap: the expired notice a real outcome replaced
// leaves a hole in the sequence, which is not a lost event.
func TestReplacedExpiredIsNotAGap(t *testing.T) {
	hub, err := NewHub(HubConfig{RetainEvents: 1})
	if err != nil {
		t.Fatal(err)
	}
	stream := streamName(0)
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(stream, true)
	hub.expire(stream)
	if _, err := hub.Finish(stream, Event{Type: "result"}); err != nil {
		t.Fatal(err)
	}
	for _, cursor := range []string{"", hub.id(1, 0)} {
		result, err := hub.subscribe(stream, alice, cursor, SamePrincipal, 8, 8, nil)
		if err != nil || result.gap != nil || len(result.replay) != 1 || result.replay[0].typ != "result" {
			t.Fatalf("cursor %q: %+v %v", cursor, result, err)
		}
	}
}
