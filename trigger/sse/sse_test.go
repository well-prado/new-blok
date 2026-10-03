package sse_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/sse"
)

const orderSchema = `{"type":"object","properties":{"item":{"type":"string"}},"required":["item"]}`

// memorySubmitter deduplicates like a durable queue, without a store. When
// hold is set, a submission waits for it and records whether its context
// was canceled meanwhile.
type memorySubmitter struct {
	mu       sync.Mutex
	payloads map[string]string
	settled  map[string]bool
	trackErr atomic.Bool
	tracked  atomic.Int64
	count    int
	hold     chan struct{}
	holdFor  string
	// entered, when set, is signaled as a held submission starts waiting.
	entered  chan struct{}
	canceled atomic.Bool
}

func (m *memorySubmitter) Submit(ctx context.Context, s trigger.Submission) (bool, error) {
	if m.hold != nil && (m.holdFor == "" || strings.Contains(string(s.Payload), m.holdFor)) {
		if m.entered != nil {
			m.entered <- struct{}{}
		}
		<-m.hold
		// Give the caller's disconnect time to reach the context, if it
		// is going to.
		select {
		case <-ctx.Done():
			m.canceled.Store(true)
		case <-time.After(300 * time.Millisecond):
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.count++
	if previous, ok := m.payloads[s.Key]; ok {
		if previous != string(s.Payload) {
			return false, trigger.ErrConflict
		}
		return false, nil
	}
	m.payloads[s.Key] = string(s.Payload)
	return true, nil
}

func (m *memorySubmitter) submissions() int { m.mu.Lock(); defer m.mu.Unlock(); return m.count }

// Settled reports the work under a key as settled when the test says so.
func (m *memorySubmitter) Settled(_ context.Context, key string) (bool, error) {
	m.tracked.Add(1)
	if m.trackErr.Load() {
		return false, errors.New("store unavailable")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.settled[key], nil
}

func (m *memorySubmitter) settle(key string) { m.mu.Lock(); defer m.mu.Unlock(); m.settled[key] = true }

// The bearer token is the principal id; "admin" also carries the admin role.
func authenticate(request *http.Request) (trigger.Principal, error) {
	token, ok := strings.CutPrefix(request.Header.Get("Authorization"), "Bearer ")
	if !ok || token == "" || token == "forged" {
		return trigger.Principal{}, errors.New("unauthenticated")
	}
	principal := trigger.Principal{ID: token}
	if token == "admin" {
		principal.Roles = []string{"admin"}
	}
	return principal, nil
}

type closed struct {
	mu      sync.Mutex
	reasons []string
}

func (c *closed) record(_, reason string) {
	c.mu.Lock()
	c.reasons = append(c.reasons, reason)
	c.mu.Unlock()
}

func (c *closed) wait(t *testing.T, reason string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, r := range c.reasons {
			if r == reason {
				c.mu.Unlock()
				return
			}
		}
		c.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	t.Fatalf("no subscription closed with %q; closed with %v", reason, c.reasons)
}

type fixture struct {
	app    *app.Application
	hub    *sse.Hub
	server *sse.Server
	http   *httptest.Server
	submit *memorySubmitter
	closed *closed
	client *http.Client
}

func newFixture(t *testing.T, config sse.HubConfig, configure func(*sse.Endpoint)) *fixture {
	return newFixtureWithListener(t, config, configure, nil)
}

func newFixtureWithListener(t *testing.T, config sse.HubConfig, configure func(*sse.Endpoint), listener net.Listener) *fixture {
	t.Helper()
	hub, err := sse.NewHub(config)
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	f := &fixture{app: application, hub: hub, submit: &memorySubmitter{payloads: map[string]string{}, settled: map[string]bool{}}, closed: &closed{}}
	endpoint := sse.Endpoint{Name: "orders", Path: "/orders", Kind: "order.build", Submit: f.submit, Tracker: f.submit, Authenticate: authenticate, InputSchema: []byte(orderSchema), OnClose: f.closed.record}
	if configure != nil {
		configure(&endpoint)
	}
	f.server, err = sse.New(application, hub, []sse.Endpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	f.http = httptest.NewUnstartedServer(f.server)
	if listener != nil {
		unusedListener := f.http.Listener
		unusedAddress := unusedListener.Addr().String()
		if err := unusedListener.Close(); err != nil {
			t.Fatalf("close unused httptest listener: %v", err)
		}
		probe, err := net.DialTimeout("tcp", unusedAddress, 100*time.Millisecond)
		if err == nil {
			_ = probe.Close()
			closeErr := unusedListener.Close()
			t.Fatalf("unused httptest listener still accepts connections; cleanup close: %v", closeErr)
		}
		f.http.Listener = listener
	}
	f.http.Start()
	f.client = &http.Client{Transport: &http.Transport{}}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = f.server.Shutdown(ctx)
		f.http.Close()
		f.client.CloseIdleConnections()
	})
	return f
}

func (f *fixture) start(t *testing.T, credential, key, body string) (int, map[string]any) {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, f.http.URL+"/orders", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if credential != "" {
		request.Header.Set("Authorization", "Bearer "+credential)
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var decoded map[string]any
	_ = json.NewDecoder(response.Body).Decode(&decoded)
	return response.StatusCode, decoded
}

// started starts work and returns its stream.
func (f *fixture) started(t *testing.T, principal, key string) string {
	t.Helper()
	status, body := f.start(t, principal, key, `{"item":"book"}`)
	if status != http.StatusAccepted {
		t.Fatalf("start %s: %d %v", key, status, body)
	}
	stream := body["stream"].(string)
	if want := sse.StreamID(sse.SubmissionKey("orders", trigger.Principal{ID: principal}, key)); stream != want {
		t.Fatalf("stream %s, want %s", stream, want)
	}
	return stream
}

func (f *fixture) url(stream string) string { return f.http.URL + "/orders/" + stream }

func publish(t *testing.T, hub *sse.Hub, stream string, n int, typ string) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		id, err := hub.Publish(stream, sse.Event{Type: typ, Data: json.RawMessage(fmt.Sprintf(`{"step":%d}`, i))})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	return ids
}

// TestWireFraming reads a whole finished stream and compares it byte for
// byte: the retry hint, then each event's id, type and data lines, each
// frame ended by a blank line. A reconnect from the final event's id gets
// 204, which tells EventSource to stop.
func TestWireFraming(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, func(e *sse.Endpoint) { e.Retry = 1500 * time.Millisecond })
	stream := f.started(t, "alice", "k1")
	first, err := f.hub.Publish(stream, sse.Event{Type: "progress", Data: json.RawMessage("{\n  \"percent\": 50,\n  \"note\": \"line\\nbreak\"\n}")})
	if err != nil {
		t.Fatal(err)
	}
	final, err := f.hub.Finish(stream, sse.Result(json.RawMessage(`{"total": 3}`)))
	if err != nil {
		t.Fatal(err)
	}
	request, _ := http.NewRequest(http.MethodGet, f.url(stream), nil)
	request.Header.Set("Authorization", "Bearer alice")
	response, err := f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || response.Header.Get("Content-Type") != "text/event-stream" || response.Header.Get("Cache-Control") != "no-cache" || response.Header.Get("X-Accel-Buffering") != "no" {
		t.Fatalf("status %d headers %v", response.StatusCode, response.Header)
	}
	want := "retry: 1500\n\n" +
		"id: " + first + "\nevent: progress\ndata: {\"percent\":50,\"note\":\"line\\nbreak\"}\n\n" +
		"id: " + final + "\nevent: result\ndata: {\"total\":3}\n\n"
	if string(body) != want {
		t.Fatalf("wire:\n%q\nwant:\n%q", body, want)
	}
	request.Header.Set("Last-Event-ID", final)
	response, err = f.client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("reconnect after the final event: %d, want 204", response.StatusCode)
	}
}

// TestHeartbeatKeepsSilentStreamsAlive: a silent subscription gets a
// comment every heartbeat interval, and an event resets the interval.
func TestHeartbeatKeepsSilentStreamsAlive(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, func(e *sse.Endpoint) { e.Heartbeat = 50 * time.Millisecond })
	stream := f.started(t, "alice", "k1")
	s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("subscribe: %v %v", err, refused)
	}
	defer s.close()
	if f, _ := s.next(time.Second, true); f.Retry == "" {
		t.Fatalf("first frame %+v, want the retry hint", f)
	}
	for i := 0; i < 3; i++ {
		if f, err := s.next(time.Second, true); err != nil || f.Comment != "heartbeat" {
			t.Fatalf("heartbeat %d: %+v %v", i, f, err)
		}
	}
	// Events closer together than the heartbeat interval keep resetting
	// it: none of the frames in between is a heartbeat.
	for i := 0; i < 8; i++ {
		publish(t, f.hub, stream, 1, "progress")
		got, err := s.next(time.Second, true)
		if err != nil || got.Type != "progress" {
			t.Fatalf("event %d: %+v %v", i, got, err)
		}
		time.Sleep(30 * time.Millisecond)
	}
}

// TestReconnectResumesAfterLastEventID: a subscriber that reconnects with
// the id of the last event it saw receives exactly the events after it.
func TestReconnectResumesAfterLastEventID(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, nil)
	stream := f.started(t, "alice", "k1")
	ids := publish(t, f.hub, stream, 5, "progress")
	s, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	var seen []string
	for len(seen) < 2 {
		frame, err := s.next(time.Second, false)
		if err != nil {
			t.Fatal(err)
		}
		if frame.Type == "progress" {
			seen = append(seen, frame.ID)
		}
	}
	s.close()
	ids = append(ids, publish(t, f.hub, stream, 3, "progress")...)
	resumed, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", seen[1])
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.close()
	var after []string
	for len(after) < 6 {
		frame, err := resumed.next(time.Second, false)
		if err != nil {
			t.Fatal(err)
		}
		if frame.Type == sse.TypeGap {
			t.Fatalf("unexpected gap %+v", frame)
		}
		if frame.Retry == "" {
			after = append(after, frame.ID)
		}
	}
	if fmt.Sprint(after) != fmt.Sprint(ids[2:]) {
		t.Fatalf("resumed %v, want %v", after, ids[2:])
	}
}

// readGapAndReplay reads a subscription's gap (if any) and its replayed
// event ids.
func readGapAndReplay(t *testing.T, s *eventStream, events int) (*frame, []string) {
	t.Helper()
	var gap *frame
	var ids []string
	for len(ids) < events {
		f, err := s.next(time.Second, false)
		if err != nil {
			t.Fatal(err)
		}
		switch {
		case f.Retry != "":
		case f.Type == sse.TypeGap:
			copied := f
			gap = &copied
		default:
			ids = append(ids, f.ID)
		}
	}
	return gap, ids
}

// TestCursorGapOutcomes: a cursor older than retention gets a retention gap
// with the number of events missed, a cursor from another epoch (a hub
// restart) a restart gap, and a malformed or future cursor is refused with
// 400 before any stream byte.
func TestCursorGapOutcomes(t *testing.T) {
	f := newFixture(t, sse.HubConfig{RetainEvents: 4}, nil)
	stream := f.started(t, "alice", "k1")
	ids := publish(t, f.hub, stream, 10, "progress")
	for _, tc := range []struct {
		name, cursor string
		reason       string
		missed       float64
	}{
		{"cursor older than retention", ids[1], sse.GapRetention, 4},
		{"no cursor, earliest events dropped", "", sse.GapRetention, 6},
		{"cursor from an earlier epoch", "0123456789abcdef.1:7", sse.GapRestart, 0},
	} {
		s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", tc.cursor)
		if err != nil || refused != nil {
			t.Fatalf("%s: %v %v", tc.name, err, refused)
		}
		gap, replay := readGapAndReplay(t, s, 4)
		s.close()
		if gap == nil {
			t.Fatalf("%s: no gap", tc.name)
		}
		var data map[string]any
		_ = json.Unmarshal([]byte(gap.Data), &data)
		if data["reason"] != tc.reason || (tc.missed > 0 && data["missed"] != tc.missed) || fmt.Sprint(replay) != fmt.Sprint(ids[6:]) {
			t.Fatalf("%s: gap %s replay %v, want %s missed %v replay %v", tc.name, gap.Data, replay, tc.reason, tc.missed, ids[6:])
		}
		// Resuming from the gap's id continues without another gap.
		again, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", gap.ID)
		if err != nil {
			t.Fatal(err)
		}
		gap, replay = readGapAndReplay(t, again, 4)
		again.close()
		if gap != nil || fmt.Sprint(replay) != fmt.Sprint(ids[6:]) {
			t.Fatalf("%s: resuming from the gap: gap %+v replay %v", tc.name, gap, replay)
		}
	}
	head, _, _ := strings.Cut(ids[0], ":")
	epoch, _, _ := strings.Cut(head, ".")
	for _, cursor := range []string{"garbage", head + ":11", head + ":-1", head + ":01", epoch + ".2:1", epoch + ":1", "ABCDEF0123456789.1:1"} {
		_, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", cursor)
		if err != nil || refused == nil || refused.StatusCode != http.StatusBadRequest {
			t.Fatalf("cursor %q: %v %+v", cursor, err, refused)
		}
		if body, _ := io.ReadAll(refused.Body); strings.Contains(string(body), "step") {
			t.Fatalf("cursor %q leaked events: %s", cursor, body)
		}
	}
}

// TestUnauthorizedSubscribersSeeNoPayload: no credential is 401; another
// principal gets the same 404 as an unknown stream, so a refusal does not
// reveal that the stream exists; each refusal is a JSON error with no
// stream bytes; a custom authorization can admit a role; one principal's
// subscription never carries another stream's events.
func TestUnauthorizedSubscribersSeeNoPayload(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, func(e *sse.Endpoint) {
		e.Authorize = func(reader, owner trigger.Principal) error {
			for _, role := range reader.Roles {
				if role == "admin" {
					return nil
				}
			}
			return sse.SamePrincipal(reader, owner)
		}
	})
	alice := f.started(t, "alice", "k1")
	bob := f.started(t, "bob", "k1")
	if alice == bob {
		t.Fatal("two principals' identical keys share a stream")
	}
	if _, err := f.hub.Publish(alice, sse.Event{Type: "progress", Data: json.RawMessage(`{"secret":"alice-only"}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.hub.Publish(bob, sse.Event{Type: "progress", Data: json.RawMessage(`{"secret":"bob-only"}`)}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, credential, stream string
		status                   int
		code                     string
	}{
		{"no credential", "", alice, http.StatusUnauthorized, "unauthorized"},
		{"forged credential", "forged", alice, http.StatusUnauthorized, "unauthorized"},
		{"another principal", "bob", alice, http.StatusNotFound, "not_found"},
		{"unknown stream", "alice", strings.Repeat("0", 32), http.StatusNotFound, "not_found"},
		{"malformed stream", "alice", "x", http.StatusNotFound, "not_found"},
	} {
		_, refused, err := subscribe(context.Background(), f.client, f.url(tc.stream), tc.credential, "")
		if err != nil || refused == nil {
			t.Fatalf("%s: admitted (%v)", tc.name, err)
		}
		body, _ := io.ReadAll(refused.Body)
		if refused.StatusCode != tc.status || refused.Header.Get("Content-Type") != "application/json" || !strings.Contains(string(body), tc.code) || strings.Contains(string(body), "only") {
			t.Fatalf("%s: %d %s %s", tc.name, refused.StatusCode, refused.Header.Get("Content-Type"), body)
		}
	}
	for _, tc := range []struct{ credential, stream, own, other string }{
		{"alice", alice, "alice-only", "bob-only"},
		{"bob", bob, "bob-only", "alice-only"},
		{"admin", alice, "alice-only", "bob-only"},
	} {
		s, refused, err := subscribe(context.Background(), f.client, f.url(tc.stream), tc.credential, "")
		if err != nil || refused != nil {
			t.Fatalf("%s: %v %v", tc.credential, err, refused)
		}
		frame, err := s.next(time.Second, false)
		for err == nil && frame.Retry != "" {
			frame, err = s.next(time.Second, false)
		}
		s.close()
		if err != nil || !strings.Contains(frame.Data, tc.own) || strings.Contains(frame.Data, tc.other) {
			t.Fatalf("%s on %s: %+v %v", tc.credential, tc.stream, frame, err)
		}
	}
}

// stalled opens a subscription over raw TCP and never reads it.
func stalled(t *testing.T, f *fixture, stream string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", strings.TrimPrefix(f.http.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.(*net.TCPConn).SetReadBuffer(4 << 10)
	fmt.Fprintf(conn, "GET /orders/%s HTTP/1.1\r\nHost: test\r\nAuthorization: Bearer alice\r\nAccept: text/event-stream\r\n\r\n", stream)
	status, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.Contains(status, "200") {
		t.Fatalf("stalled subscription: %q %v", status, err)
	}
	return conn
}

// flood publishes large events as fast as possible and returns the longest
// time a single Publish took.
func flood(t *testing.T, hub *sse.Hub, stream string, events int) time.Duration {
	t.Helper()
	data := json.RawMessage(`"` + strings.Repeat("x", 60<<10) + `"`)
	var longest time.Duration
	for i := 0; i < events; i++ {
		began := time.Now()
		if _, err := hub.Publish(stream, sse.Event{Type: "progress", Data: data}); err != nil {
			t.Fatal(err)
		}
		longest = max(longest, time.Since(began))
	}
	return longest
}

// TestSlowSubscriberIsDisconnectedNotBuffered: a subscriber that stops
// reading lets its bounded queue fill, and is then disconnected; publishing
// never waits for it, and the hub retains only its replay bound.
func TestSlowSubscriberIsDisconnectedNotBuffered(t *testing.T) {
	f := newFixture(t, sse.HubConfig{RetainBytes: 1 << 20, MaxEventBytes: 64 << 10}, func(e *sse.Endpoint) {
		e.QueueDepth = 4
		e.WriteTimeout = time.Minute
	})
	stream := f.started(t, "alice", "k1")
	conn := stalled(t, f, stream)
	defer conn.Close()
	longest := flood(t, f.hub, stream, 600)
	f.closed.wait(t, sse.ReasonSlow)
	if longest > publishBound {
		t.Fatalf("a publish waited %v for a slow subscriber", longest)
	}
	if stats := f.hub.Stats(); stats.SlowSubscribers != 1 || stats.EvictedEvents == 0 {
		t.Fatalf("stats %+v", stats)
	}
}

// TestBlockedWriteTimesOut: a subscriber whose socket stops draining is
// disconnected by the write deadline even while its queue has room.
func TestBlockedWriteTimesOut(t *testing.T) {
	transport, err := newWriteGateListener()
	if err != nil {
		t.Fatal(err)
	}
	f := newFixtureWithListener(t, sse.HubConfig{RetainBytes: 1 << 20, MaxEventBytes: 64 << 10}, func(e *sse.Endpoint) {
		e.QueueDepth = 256
		e.WriteTimeout = 100 * time.Millisecond
	}, transport)
	stream := f.started(t, "alice", "k1")
	conn := blockedSubscription(t, f, stream, transport)
	defer conn.Close()
	if _, err := f.hub.Publish(stream, sse.Event{Type: "progress", Data: json.RawMessage(`{"step":1}`)}); err != nil {
		t.Fatal(err)
	}
	transport.waitBlocked(t)
	f.closed.wait(t, sse.ReasonWriteTimeout)
	if stats := f.hub.Stats(); stats.SlowSubscribers != 0 {
		t.Fatalf("closed by the queue, not the write deadline: %+v", stats)
	}
}

// auditLog is a synchronous audit sink that can be made to fail.
type auditLog struct {
	mu      sync.Mutex
	records []sse.AuditRecord
	fail    atomic.Bool
}

func (a *auditLog) Record(_ context.Context, record sse.AuditRecord) error {
	if a.fail.Load() {
		return errors.New("disk full")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.records = append(a.records, record)
	return nil
}

func (a *auditLog) count() int { a.mu.Lock(); defer a.mu.Unlock(); return len(a.records) }

// TestAuditAndDropPoliciesStayDistinct: audit records are stored
// synchronously whatever happens to progress (retention drops it, a slow
// subscriber is cut, the hub has no room); a failing sink fails Record and
// publishes nothing, and never affects Publish.
func TestAuditAndDropPoliciesStayDistinct(t *testing.T) {
	audit := &auditLog{}
	f := newFixture(t, sse.HubConfig{RetainEvents: 2, MaxStreams: 1, Audit: audit}, nil)
	stream := f.started(t, "alice", "k1")
	for i := 0; i < 10; i++ {
		if _, err := f.hub.Record(context.Background(), stream, sse.Event{Type: "charge", Data: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))}); err != nil {
			t.Fatal(err)
		}
	}
	if audit.count() != 10 || f.hub.Stats().EvictedEvents != 8 {
		t.Fatalf("audit %d records, progress evicted %d; want 10 and 8", audit.count(), f.hub.Stats().EvictedEvents)
	}
	// No room for another stream: progress is refused, the audit record is
	// still stored.
	if _, err := f.hub.Publish(strings.Repeat("a", 32), sse.Event{Type: "progress"}); !errors.Is(err, sse.ErrHubFull) {
		t.Fatalf("publish to a full hub: %v", err)
	}
	if _, err := f.hub.Record(context.Background(), strings.Repeat("a", 32), sse.Event{Type: "charge"}); err != nil || audit.count() != 11 {
		t.Fatalf("record on a full hub: %v, %d records", err, audit.count())
	}
	audit.fail.Store(true)
	s, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	_, replay := readGapAndReplay(t, s, 2)
	if _, err := f.hub.Record(context.Background(), stream, sse.Event{Type: "charge", Data: json.RawMessage(`{"n":99}`)}); !errors.Is(err, sse.ErrAudit) {
		t.Fatalf("failing sink: %v", err)
	}
	id, err := f.hub.Publish(stream, sse.Event{Type: "progress"})
	if err != nil {
		t.Fatalf("publish while the sink fails: %v", err)
	}
	next, err := s.next(time.Second, false)
	if err != nil || next.ID != id || next.Type != "progress" {
		t.Fatalf("after a failed record the stream carried %+v (replay %v), want only the progress event %s", next, replay, id)
	}
}

// TestStartOrderAndOutcomes: authentication precedes the body read, the
// idempotency key precedes the body, the body is bounded, and the durable
// submission decides 202, 200 duplicate or 409 conflict.
func TestStartOrderAndOutcomes(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, func(e *sse.Endpoint) { e.MaxBodyBytes = 1024 })
	huge := `{"item":"` + strings.Repeat("x", 4096) + `"}`
	for _, tc := range []struct {
		name, credential, key, body string
		status                      int
		code                        string
	}{
		{"unauthenticated before body", "", "k", huge, http.StatusUnauthorized, "unauthorized"},
		{"key before body", "alice", "", huge, http.StatusBadRequest, "invalid_key"},
		{"key with spaces", "alice", "a b", `{"item":"x"}`, http.StatusBadRequest, "invalid_key"},
		{"body too large", "alice", "k", huge, http.StatusRequestEntityTooLarge, "too_large"},
		{"invalid input", "alice", "k", `{"item":7}`, http.StatusBadRequest, "invalid_input"},
		{"empty body", "alice", "k", ``, http.StatusBadRequest, "invalid_input"},
	} {
		if status, body := f.start(t, tc.credential, tc.key, tc.body); status != tc.status || body["error"] != tc.code {
			t.Fatalf("%s: %d %v", tc.name, status, body)
		}
	}
	if f.submit.submissions() != 0 {
		t.Fatalf("refused starts submitted %d times", f.submit.submissions())
	}
	first := f.started(t, "alice", "k")
	if status, body := f.start(t, "alice", "k", `{"item":"book"}`); status != http.StatusOK || body["duplicate"] != true || body["stream"] != first {
		t.Fatalf("duplicate: %d %v", status, body)
	}
	if status, body := f.start(t, "alice", "k", `{"item":"pen"}`); status != http.StatusConflict || body["error"] != "conflict" {
		t.Fatalf("conflict: %d %v", status, body)
	}
	if status, _ := f.start(t, "alice", "other", `{ "item" : "book" }`); status != http.StatusAccepted {
		t.Fatalf("second start: %d", status)
	}
	if f.submit.payloads[sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "other")] != `{"item":"book"}` {
		t.Fatalf("the submitted payload is not the normalized input: %v", f.submit.payloads)
	}
}

// TestCallerLeavingDoesNotCancelSubmission: a caller that disconnects while
// its start is being committed does not cancel the commit.
func TestCallerLeavingDoesNotCancelSubmission(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, nil)
	f.submit.hold = make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, f.http.URL+"/orders", strings.NewReader(`{"item":"book"}`))
	request.Header.Set("Authorization", "Bearer alice")
	request.Header.Set("Idempotency-Key", "k")
	done := make(chan error, 1)
	go func() {
		response, err := f.client.Do(request)
		if err == nil {
			response.Body.Close()
		}
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("the caller did not leave: %v", err)
	}
	close(f.submit.hold)
	deadline := time.Now().Add(5 * time.Second)
	for f.submit.submissions() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("the submission did not complete")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if f.submit.canceled.Load() {
		t.Fatal("the submission's context was canceled by the caller leaving")
	}
}

// TestLimitsRefuseWithoutSubmitting: a full hub refuses a new start before
// anything is submitted, and subscriber limits answer 503.
func TestLimitsRefuseWithoutSubmitting(t *testing.T) {
	f := newFixture(t, sse.HubConfig{MaxStreams: 1}, func(e *sse.Endpoint) { e.StreamSubscribers = 1; e.MaxSubscribers = 2 })
	stream := f.started(t, "alice", "k1")
	s, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if status, body := f.start(t, "alice", "k2", `{"item":"book"}`); status != http.StatusServiceUnavailable || body["error"] != "saturated" || f.submit.submissions() != 1 {
		t.Fatalf("start on a full hub: %d %v, %d submissions", status, body, f.submit.submissions())
	}
	retryHinted(t, f, stream, "saturated")
}

// retryHinted subscribes expecting a transient refusal: an empty event
// stream with the retry hint and a comment naming the reason, which an
// EventSource client retries instead of abandoning.
func retryHinted(t *testing.T, f *fixture, stream, reason string) {
	t.Helper()
	s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("refused with a status instead of a retry hint: %v %+v", err, refused)
	}
	defer s.close()
	first, _ := s.next(time.Second, true)
	second, _ := s.next(time.Second, true)
	if first.Retry == "" || second.Comment != reason || !s.ended(time.Second) {
		t.Fatalf("transient refusal: %+v then %+v, want the retry hint and %q, then the end", first, second, reason)
	}
}

// TestMaxDurationAndShutdownEndSubscriptions: a subscription ends at its
// maximum duration, Shutdown ends the rest and refuses new ones, and every
// handler returns.
func TestMaxDurationAndShutdownEndSubscriptions(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, func(e *sse.Endpoint) { e.MaxDuration = 100 * time.Millisecond })
	stream := f.started(t, "alice", "k1")
	s, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil {
		t.Fatal(err)
	}
	if !s.ended(5 * time.Second) {
		t.Fatal("the subscription outlived its maximum duration")
	}
	s.close()
	f.closed.wait(t, sse.ReasonMaxDuration)

	g := newFixture(t, sse.HubConfig{}, nil)
	stream = g.started(t, "alice", "k1")
	var streams []*eventStream
	for i := 0; i < 3; i++ {
		s, _, err := subscribe(context.Background(), g.client, g.url(stream), "alice", "")
		if err != nil {
			t.Fatal(err)
		}
		streams = append(streams, s)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, s := range streams {
		if !s.ended(5 * time.Second) {
			t.Fatal("a subscription outlived shutdown")
		}
		s.close()
	}
	retryHinted(t, g, stream, "unavailable")
}

// TestLongStreamsDoNotLeak runs many streams through start, subscription,
// progress and either completion or an abandoned subscriber, over a hub
// smaller than the number of streams: goroutines return to their baseline,
// the hub keeps at most its stream bound, and the heap does not grow with
// the number of streams.
func TestLongStreamsDoNotLeak(t *testing.T) {
	f := newFixture(t, sse.HubConfig{MaxStreams: 32, RetainEvents: 16}, func(e *sse.Endpoint) { e.Heartbeat = 20 * time.Millisecond })
	// measure waits (bounded) for goroutines to fall to target, then
	// reports them and the heap in use.
	measure := func(target int) (int, uint64) {
		f.client.CloseIdleConnections()
		deadline := time.Now().Add(5 * time.Second)
		count := runtime.NumGoroutine()
		for count > target && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
			runtime.GC()
			count = runtime.NumGoroutine()
		}
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return count, m.HeapInuse
	}
	round := func(from, to int) {
		for i := from; i < to; i++ {
			stream := f.started(t, "alice", fmt.Sprintf("k%d", i))
			s, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
			if err != nil {
				t.Fatal(err)
			}
			publish(t, f.hub, stream, 40, "progress")
			// Half the subscribers leave mid-stream; the work still
			// finishes.
			if i%2 == 1 {
				s.close()
			}
			if _, err := f.hub.Finish(stream, sse.Result(json.RawMessage(`{}`))); err != nil {
				t.Fatal(err)
			}
			if i%2 == 0 && !s.ended(5*time.Second) {
				t.Fatal("a finished stream did not end")
			}
			s.close()
		}
	}
	round(0, 50)
	time.Sleep(100 * time.Millisecond)
	baseline, before := measure(1 << 30)
	round(50, 450)
	after, heap := measure(baseline)
	t.Logf("400 streams: goroutines %d → %d, heap in use %d → %d bytes, %+v", baseline, after, before, heap, f.hub.Stats())
	if after > baseline {
		t.Fatalf("goroutines grew from %d to %d", baseline, after)
	}
	if growth := int64(heap) - int64(before); growth > 8<<20 {
		t.Fatalf("heap grew by %d bytes over 400 streams", growth)
	}
	if f.hub.Stats().EvictedStreams < 400-32 {
		t.Fatalf("the hub kept more than its stream bound: %+v", f.hub.Stats())
	}
	f.closed.wait(t, sse.ReasonClientClosed)
	f.closed.wait(t, sse.ReasonFinished)
}

// TestShutdownInterruptsABlockedWrite: a subscriber stuck in a write to a
// socket that stopped draining is ended by Shutdown at once, not after its
// write deadline.
func TestShutdownInterruptsABlockedWrite(t *testing.T) {
	transport, err := newWriteGateListener()
	if err != nil {
		t.Fatal(err)
	}
	f := newFixtureWithListener(t, sse.HubConfig{RetainBytes: 1 << 20, MaxEventBytes: 64 << 10}, func(e *sse.Endpoint) {
		e.QueueDepth = 256
		e.WriteTimeout = time.Minute
	}, transport)
	stream := f.started(t, "alice", "k1")
	conn := blockedSubscription(t, f, stream, transport)
	defer conn.Close()
	if _, err := f.hub.Publish(stream, sse.Event{Type: "progress", Data: json.RawMessage(`{"step":1}`)}); err != nil {
		t.Fatal(err)
	}
	transport.waitBlocked(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	began := time.Now()
	if err := f.server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown waited for a blocked write: %v", err)
	}
	f.closed.wait(t, sse.ReasonShutdown)
	t.Logf("shutdown ended a blocked subscriber in %v", time.Since(began))
}

// TestClosingTransportUnblocksABlockedWrite verifies that force-closing the
// server-side connection also releases the transport gate, as test cleanup
// and client disconnect handling may do independently of a write deadline.
func TestClosingTransportUnblocksABlockedWrite(t *testing.T) {
	transport, err := newWriteGateListener()
	if err != nil {
		t.Fatal(err)
	}
	f := newFixtureWithListener(t, sse.HubConfig{RetainBytes: 1 << 20, MaxEventBytes: 64 << 10}, func(e *sse.Endpoint) {
		e.QueueDepth = 256
		e.WriteTimeout = time.Minute
	}, transport)
	stream := f.started(t, "alice", "k1")
	conn := blockedSubscription(t, f, stream, transport)
	defer conn.Close()
	if _, err := f.hub.Publish(stream, sse.Event{Type: "progress", Data: json.RawMessage(`{"step":1}`)}); err != nil {
		t.Fatal(err)
	}
	transport.waitBlocked(t)

	f.http.CloseClientConnections()
	if err := transport.waitWriteExit(t); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("closing the server connection ended the blocked write with %v, want %v", err, net.ErrClosed)
	}
	f.closed.wait(t, sse.ReasonClientClosed)
}

// TestEndpointBoundsAreRefused: New refuses endpoints past their bounds,
// including a subscriber queue that could hold more than MaxQueueBytes.
func TestEndpointBoundsAreRefused(t *testing.T) {
	hub, err := sse.NewHub(sse.HubConfig{MaxEventBytes: 1 << 20, RetainBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	valid := sse.Endpoint{Name: "orders", Path: "/orders", Kind: "order.build", Submit: &memorySubmitter{}, Tracker: &memorySubmitter{}, Authenticate: authenticate, InputSchema: []byte(orderSchema), QueueDepth: 16}
	if _, err := sse.New(application, hub, []sse.Endpoint{valid}); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*sse.Endpoint){
		"queue bytes":           func(e *sse.Endpoint) { e.QueueDepth = 17 },
		"queue depth":           func(e *sse.Endpoint) { e.QueueDepth = sse.MaxQueueDepth + 1 },
		"body":                  func(e *sse.Endpoint) { e.MaxBodyBytes = sse.MaxBodyBytesLimit + 1 },
		"read timeout":          func(e *sse.Endpoint) { e.ReadTimeout = 2 * sse.MaxReadTimeout },
		"stream above endpoint": func(e *sse.Endpoint) { e.MaxSubscribers = 4; e.StreamSubscribers = 5 },
		"no authenticator":      func(e *sse.Endpoint) { e.Authenticate = nil },
		"no submitter":          func(e *sse.Endpoint) { e.Submit = nil },
		"no tracker":            func(e *sse.Endpoint) { e.Tracker = nil },
		"retry below the floor": func(e *sse.Endpoint) { e.Retry = time.Millisecond },
		"retry above the cap":   func(e *sse.Endpoint) { e.Retry = time.Hour },
		"bad name":              func(e *sse.Endpoint) { e.Name = "Orders" },
		"trailing slash":        func(e *sse.Endpoint) { e.Path = "/orders/" },
		"unparsable schema":     func(e *sse.Endpoint) { e.InputSchema = []byte(`{"type":"nope"}`) },
	} {
		endpoint := valid
		mutate(&endpoint)
		if _, err := sse.New(application, hub, []sse.Endpoint{endpoint}); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, err := sse.New(application, hub, []sse.Endpoint{valid, valid}); err == nil {
		t.Fatal("two endpoints with one name accepted")
	}
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *manualClock) Now() time.Time      { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *manualClock) add(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }

// TestRepeatedStartAfterEviction: once a stream is no longer retained, a
// repeated start of its work ends the new stream as expired if the work has
// settled (instead of leaving it open forever), and otherwise registers it
// again so the work's later progress reaches the subscriber, after a gap
// for the cursor of the evicted incarnation.
func TestRepeatedStartAfterEviction(t *testing.T) {
	clock := &manualClock{now: t0}
	f := newFixture(t, sse.HubConfig{MaxStreams: 2, Retention: time.Minute, Clock: clock.Now}, nil)
	settled := f.started(t, "alice", "settled")
	if _, err := f.hub.Finish(settled, sse.Result(json.RawMessage(`{"done":true}`))); err != nil {
		t.Fatal(err)
	}
	pending := f.started(t, "alice", "pending")
	old := publish(t, f.hub, pending, 3, "progress")
	// Both streams age past retention and are evicted by other work.
	clock.add(2 * time.Minute)
	for _, other := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
		if _, err := f.hub.Finish(other, sse.Result(nil)); err != nil {
			t.Fatal(err)
		}
	}
	f.submit.settle(sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "settled"))

	// A repeat that conflicts with the committed work ends the stream as
	// expired too, rather than bringing it back open.
	if status, _ := f.start(t, "alice", "settled", `{"item":"pen"}`); status != http.StatusConflict {
		t.Fatalf("conflicting repeat: %d", status)
	}
	conflicted, refused, err := subscribe(context.Background(), f.client, f.url(settled), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("after a conflicting repeat: %v %+v", err, refused)
	}
	var got frame
	for got.Type == "" {
		if got, err = conflicted.next(time.Second, false); err != nil {
			t.Fatal(err)
		}
	}
	if got.Type != sse.TypeExpired || !conflicted.ended(time.Second) {
		t.Fatalf("after a conflicting repeat: %+v, want the expired final event", got)
	}
	conflicted.close()
	if status, body := f.start(t, "alice", "settled", `{"item":"book"}`); status != http.StatusOK || body["stream"] != settled {
		t.Fatalf("repeated start of settled work: %d %v", status, body)
	}
	s, refused, err := subscribe(context.Background(), f.client, f.url(settled), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("%v %+v", err, refused)
	}
	var final frame
	for final.Type == "" {
		if final, err = s.next(time.Second, false); err != nil {
			t.Fatal(err)
		}
	}
	if final.Type != sse.TypeExpired || !s.ended(time.Second) {
		t.Fatalf("settled work's stream: %+v, want the expired final event and the end", final)
	}
	s.close()
	request, _ := http.NewRequest(http.MethodGet, f.url(settled), nil)
	request.Header.Set("Authorization", "Bearer alice")
	request.Header.Set("Last-Event-ID", final.ID)
	if response, err := f.client.Do(request); err != nil || response.StatusCode != http.StatusNoContent {
		t.Fatalf("reconnect after expired: %v %+v", err, response)
	} else {
		response.Body.Close()
	}

	// The pending work's worker publishes again; the stream comes back
	// without an owner until the start is repeated.
	clock.add(2 * time.Minute)
	fresh := publish(t, f.hub, pending, 2, "progress")
	if _, refused, _ := subscribe(context.Background(), f.client, f.url(pending), "alice", ""); refused == nil || refused.StatusCode != http.StatusNotFound {
		t.Fatalf("an unregistered stream was served: %+v", refused)
	}
	if status, _ := f.start(t, "alice", "pending", `{"item":"book"}`); status != http.StatusOK {
		t.Fatalf("repeated start of pending work: %d", status)
	}
	s, _, err = subscribe(context.Background(), f.client, f.url(pending), "alice", old[2])
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	gap, replay := readGapAndReplay(t, s, 2)
	if gap == nil || !strings.Contains(gap.Data, sse.GapRetention) || fmt.Sprint(replay) != fmt.Sprint(fresh) {
		t.Fatalf("old cursor %s on the new incarnation: gap %+v replay %v, want a gap then %v", old[2], gap, replay, fresh)
	}
}

// TestFailedStartKeepsAConcurrentlyAcceptedStream: a start that loses a
// race to a conflicting start with the same key does not remove the stream
// the winner was acknowledged for.
func TestFailedStartKeepsAConcurrentlyAcceptedStream(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, nil)
	f.submit.hold, f.submit.holdFor = make(chan struct{}), "pen"
	lost := make(chan int, 1)
	go func() {
		status, _ := f.start(t, "alice", "k", `{"item":"pen"}`)
		lost <- status
	}()
	time.Sleep(50 * time.Millisecond)
	stream := f.started(t, "alice", "k")
	close(f.submit.hold)
	if status := <-lost; status != http.StatusConflict {
		t.Fatalf("the losing start: %d", status)
	}
	s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("the accepted start's stream is gone: %v %+v", err, refused)
	}
	s.close()
}

// plainWriter hides the server's connection: it can flush but cannot take
// a write deadline.
type plainWriter struct{ http.ResponseWriter }

func (w plainWriter) Flush() { w.ResponseWriter.(http.Flusher).Flush() }

// TestWriterWithoutDeadlinesIsRefused: a subscription whose writes could
// not be bounded is refused before any stream byte.
func TestWriterWithoutDeadlinesIsRefused(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, nil)
	stream := f.started(t, "alice", "k")
	wrapped := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { f.server.ServeHTTP(plainWriter{w}, r) }))
	defer wrapped.Close()
	_, refused, err := subscribe(context.Background(), f.client, wrapped.URL+"/orders/"+stream, "alice", "")
	if err != nil || refused == nil || refused.StatusCode != http.StatusInternalServerError {
		t.Fatalf("a writer without deadlines: %v %+v", err, refused)
	}
}

// TestReplayIsNotCopiedPerSubscriber: subscribers that read a large replay
// and then wait do not each keep a copy of it.
func TestReplayIsNotCopiedPerSubscriber(t *testing.T) {
	f := newFixture(t, sse.HubConfig{MaxStreams: 16, RetainBytes: 8 << 20}, func(e *sse.Endpoint) { e.StreamSubscribers = 16; e.MaxSubscribers = 16 })
	stream := f.started(t, "alice", "k")
	data := json.RawMessage(`"` + strings.Repeat("x", 60<<10) + `"`)
	for i := 0; i < 128; i++ {
		if _, err := f.hub.Publish(stream, sse.Event{Type: "progress", Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapInuse
	}
	before := heap()
	var streams []*eventStream
	for i := 0; i < 16; i++ {
		s, _, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
		if err != nil {
			t.Fatal(err)
		}
		if _, replay := readGapAndReplay(t, s, 128); len(replay) != 128 {
			t.Fatal("short replay")
		}
		streams = append(streams, s)
	}
	growth := int64(heap()) - int64(before)
	for _, s := range streams {
		s.close()
	}
	t.Logf("16 idle subscribers after a 7.5 MiB replay: heap grew %d bytes", growth)
	if growth > 32<<20 {
		t.Fatalf("heap grew %d bytes: subscribers keep their replay", growth)
	}
}

// TestOwnerQuota: one principal cannot fill the hub with running streams
// and lock other principals out.
func TestOwnerQuota(t *testing.T) {
	f := newFixture(t, sse.HubConfig{MaxStreams: 4, MaxStreamsPerOwner: 2}, nil)
	f.started(t, "mallory", "k1")
	f.started(t, "mallory", "k2")
	if status, body := f.start(t, "mallory", "k3", `{"item":"book"}`); status != http.StatusServiceUnavailable || body["error"] != "saturated" {
		t.Fatalf("mallory past the quota: %d %v", status, body)
	}
	f.started(t, "alice", "k1")
}

// evicted starts work, finishes or abandons it as asked, and lets the hub
// evict its stream.
func evicted(t *testing.T, f *fixture, clock *manualClock, key string, finish bool) string {
	t.Helper()
	stream := f.started(t, "alice", key)
	publish(t, f.hub, stream, 1, "progress")
	if finish {
		if _, err := f.hub.Finish(stream, sse.Result(nil)); err != nil {
			t.Fatal(err)
		}
	}
	clock.add(2 * time.Minute)
	for i := 0; i < 2; i++ {
		filler := strings.Repeat(fmt.Sprint(i), 32)
		publish(t, f.hub, filler, 1, "progress")
		if _, err := f.hub.Finish(filler, sse.Result(nil)); err != nil {
			t.Fatal(err)
		}
	}
	return stream
}

func finalType(t *testing.T, f *fixture, stream string) string {
	t.Helper()
	s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("subscribe: %v %+v", err, refused)
	}
	defer s.close()
	for {
		got, err := s.next(time.Second, false)
		if err != nil {
			return "none: " + err.Error()
		}
		if got.Type != "" && got.Type != "progress" {
			if !s.ended(time.Second) {
				return got.Type + " (stream did not end)"
			}
			return got.Type
		}
	}
}

// TestDuplicateOfSettledWorkExpires: a duplicate start of settled work
// whose stream was evicted ends the recreated stream as expired.
func TestDuplicateOfSettledWorkExpires(t *testing.T) {
	clock := &manualClock{now: t0}
	f := newFixture(t, sse.HubConfig{MaxStreams: 2, Retention: time.Minute, Clock: clock.Now}, nil)
	stream := evicted(t, f, clock, "k", true)
	f.submit.settle(sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "k"))
	if status, _ := f.start(t, "alice", "k", `{"item":"book"}`); status != http.StatusOK {
		t.Fatalf("duplicate: %d", status)
	}
	if got := finalType(t, f, stream); got != sse.TypeExpired {
		t.Fatalf("final %s, want expired", got)
	}
}

// TestTrackerFailureIsCheckedLater: when the repeated start cannot reach the
// tracker, the stream stays unverified and the first subscriber's check
// decides it, so it never stays open with no work behind it.
func TestTrackerFailureIsCheckedLater(t *testing.T) {
	clock := &manualClock{now: t0}
	f := newFixture(t, sse.HubConfig{MaxStreams: 2, Retention: time.Minute, Clock: clock.Now}, nil)
	stream := evicted(t, f, clock, "k", true)
	f.submit.settle(sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "k"))
	f.submit.trackErr.Store(true)
	if status, body := f.start(t, "alice", "k", `{"item":"book"}`); status != http.StatusServiceUnavailable || body["error"] != "unavailable" {
		t.Fatalf("repeat without the tracker: %d %v", status, body)
	}
	// Still failing: a subscriber is asked to retry, not left waiting.
	retryHinted(t, f, stream, "unavailable")
	f.submit.trackErr.Store(false)
	if got := finalType(t, f, stream); got != sse.TypeExpired {
		t.Fatalf("final %s, want expired", got)
	}
}

// TestUnverifiedRunningWorkIsFollowed: a recreated stream whose work is
// still running is verified by the check and followed live.
func TestUnverifiedRunningWorkIsFollowed(t *testing.T) {
	clock := &manualClock{now: t0}
	f := newFixture(t, sse.HubConfig{MaxStreams: 2, Retention: time.Minute, Clock: clock.Now}, nil)
	stream := evicted(t, f, clock, "k", false)
	if status, _ := f.start(t, "alice", "k", `{"item":"book"}`); status != http.StatusOK {
		t.Fatalf("duplicate: %d", status)
	}
	checks := f.submit.tracked.Load()
	s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("%v %+v", err, refused)
	}
	defer s.close()
	if f.submit.tracked.Load() != checks {
		t.Fatal("a verified stream was checked again")
	}
	id, err := f.hub.Finish(stream, sse.Result(json.RawMessage(`{"ok":true}`)))
	if err != nil {
		t.Fatal(err)
	}
	for {
		got, err := s.next(time.Second, false)
		if err != nil {
			t.Fatal(err)
		}
		if got.Type == "result" {
			if got.ID != id {
				t.Fatalf("result id %s, want %s", got.ID, id)
			}
			return
		}
	}
}

// TestAcceptedStartNeedsNoCheck: the stream of freshly accepted work is
// followed without asking the tracker.
func TestAcceptedStartNeedsNoCheck(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, nil)
	stream := f.started(t, "alice", "k")
	s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("%v %+v", err, refused)
	}
	s.close()
	if n := f.submit.tracked.Load(); n != 0 {
		t.Fatalf("the tracker was asked %d times about accepted work", n)
	}
}

// TestLeftSubscriberDoesNotPinStream: a subscriber that leaves an
// unfinished stream no longer protects it, so once stale it can make room
// for new work.
func TestLeftSubscriberDoesNotPinStream(t *testing.T) {
	clock := &manualClock{now: t0}
	f := newFixture(t, sse.HubConfig{MaxStreams: 1, Retention: time.Minute, Clock: clock.Now}, nil)
	stream := f.started(t, "alice", "k1")
	s, refused, err := subscribe(context.Background(), f.client, f.url(stream), "alice", "")
	if err != nil || refused != nil {
		t.Fatalf("%v %+v", err, refused)
	}
	s.close()
	f.closed.wait(t, sse.ReasonClientClosed)
	clock.add(2 * time.Minute)
	if status, body := f.start(t, "alice", "k2", `{"item":"book"}`); status != http.StatusAccepted {
		t.Fatalf("a stream its subscriber left kept the hub full: %d %v", status, body)
	}
}

// TestStartHoldsTheApplicationUntilSubmitted: the application cannot stop
// while a start's durable submission is in flight; a start that arrives
// while it drains is refused without submitting, and an open subscription
// does not hold the application open.
func TestStartHoldsTheApplicationUntilSubmitted(t *testing.T) {
	f := newFixture(t, sse.HubConfig{}, nil)
	stream := f.started(t, "alice", "first")
	follow, err := http.NewRequest(http.MethodGet, f.url(stream), nil)
	if err != nil {
		t.Fatal(err)
	}
	follow.Header.Set("Authorization", "Bearer alice")
	subscription, err := f.client.Do(follow)
	if err != nil || subscription.StatusCode != http.StatusOK {
		t.Fatalf("subscription: %v %v", subscription, err)
	}
	defer subscription.Body.Close()
	f.submit.hold, f.submit.entered = make(chan struct{}), make(chan struct{}, 1)
	f.submit.holdFor = "pen"
	var release sync.Once
	// Cleanups run last-in first-out: this one frees a held submission
	// before the fixture shuts down, so a failure cannot hang the test.
	t.Cleanup(func() { release.Do(func() { close(f.submit.hold) }) })
	type reply struct {
		status int
		body   map[string]any
	}
	replies := make(chan reply, 1)
	go func() {
		status, body := f.start(t, "alice", "held", `{"item":"pen"}`)
		replies <- reply{status, body}
	}()
	select {
	case <-f.submit.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the held start never reached its submission")
	}
	drained := make(chan error, 1)
	go func() { drained <- f.app.Shutdown(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for f.app.State() == app.ReadyState {
		if time.Now().After(deadline) {
			t.Fatal("the application never began draining")
		}
		time.Sleep(time.Millisecond)
	}
	before := f.submit.submissions()
	late, err := http.NewRequest(http.MethodPost, f.http.URL+"/orders", strings.NewReader(`{"item":"book"}`))
	if err != nil {
		t.Fatal(err)
	}
	late.Header.Set("Authorization", "Bearer alice")
	late.Header.Set("Idempotency-Key", "late")
	refused, err := f.client.Do(late)
	if err != nil {
		t.Fatal(err)
	}
	var refusal map[string]any
	_ = json.NewDecoder(refused.Body).Decode(&refusal)
	refused.Body.Close()
	if refused.StatusCode != http.StatusServiceUnavailable || refused.Header.Get("Retry-After") != "1" || refusal["error"] != "unavailable" {
		t.Fatalf("a start while draining: %d Retry-After=%q %v", refused.StatusCode, refused.Header.Get("Retry-After"), refusal)
	}
	if got := f.submit.submissions(); got != before {
		t.Fatalf("a refused start submitted (%d submissions, was %d)", got, before)
	}
	if state := f.app.State(); state != app.DrainingState {
		t.Fatalf("the application is %s with a submission in flight; want draining", state)
	}
	release.Do(func() { close(f.submit.hold) })
	select {
	case r := <-replies:
		if r.status != http.StatusAccepted {
			t.Fatalf("the held start answered %d %v", r.status, r.body)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the held start never answered")
	}
	select {
	case err := <-drained:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("an open subscription kept the application from stopping")
	}
}
