package sse

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/trigger"
)

const (
	DefaultMaxStreams         = 1024
	MaxStreamsLimit           = 1 << 16
	DefaultMaxStreamsPerOwner = 64
	DefaultRetainEvents       = 256
	RetainEventsLimit         = 4096
	DefaultRetainBytes        = 256 << 10
	RetainBytesLimit          = 16 << 20
	DefaultMaxEventBytes      = 64 << 10
	MaxEventBytesLimit        = 1 << 20
	DefaultRetention          = 10 * time.Minute
	// MaxHubBytes bounds MaxStreams × RetainBytes, the event bytes one hub
	// may retain.
	MaxHubBytes = 1 << 30
)

// Gap reasons, carried by the "gap" event a subscriber receives when it
// cannot be given every event after its cursor.
const (
	// GapRetention: events after the cursor were dropped, by the stream's
	// retention bound or with an earlier incarnation of the stream.
	GapRetention = "retention"
	// GapRestart: the cursor belongs to an earlier epoch of the hub.
	GapRestart = "restart"
)

// Reserved event types.
const (
	// TypeGap is a gap notice.
	TypeGap = "gap"
	// TypeExpired is the final event of a stream whose work has settled
	// but whose progress and outcome are no longer retained.
	TypeExpired = "expired"
)

var (
	// ErrInvalidEvent refuses an event whose type or data is malformed or
	// too large.
	ErrInvalidEvent = errors.New("sse: invalid event")
	// ErrFinished refuses an event for a stream whose final event was
	// published.
	ErrFinished = errors.New("sse: stream is finished")
	// ErrHubFull refuses a new stream when every retained stream is in use.
	// Progress for it is dropped; the work it reports on is unaffected.
	ErrHubFull = errors.New("sse: hub is full")
	// ErrAudit wraps an audit sink failure: the record was not stored and
	// the event was not published.
	ErrAudit = errors.New("sse: audit record failed")
	// ErrNoAuditSink is returned by Record when the hub has no audit sink.
	ErrNoAuditSink = errors.New("sse: hub has no audit sink")

	errUnknownStream = errors.New("sse: unknown stream")
	errForbidden     = errors.New("sse: forbidden")
	errInvalidCursor = errors.New("sse: invalid cursor")
	errBusy          = errors.New("sse: subscriber limit reached")
	errOwnerMismatch = errors.New("sse: stream belongs to another principal")
	errOwnerQuota    = errors.New("sse: principal stream quota reached")
	errUnverified    = errors.New("sse: stream not yet known to have work behind it")
)

var eventType = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)

// Event is one progress event. Data is JSON; it is compacted before it is
// retained, so it never spans lines on the wire.
type Event struct {
	Type string
	Data json.RawMessage
}

// AuditRecord is what an audit sink stores.
type AuditRecord struct {
	Stream string
	Type   string
	Data   json.RawMessage
	At     time.Time
}

// AuditSink stores audit records synchronously. It must return only after
// the record is durable; an error means it was not stored.
type AuditSink interface {
	Record(context.Context, AuditRecord) error
}

// HubConfig bounds a hub. Zero values select the defaults.
type HubConfig struct {
	// MaxStreams bounds the streams retained at once, and
	// MaxStreamsPerOwner those one principal started.
	MaxStreams         int
	MaxStreamsPerOwner int
	// RetainEvents and RetainBytes bound one stream's replay buffer; the
	// oldest events are dropped first.
	RetainEvents int
	RetainBytes  int
	// MaxEventBytes bounds one event's data.
	MaxEventBytes int
	// Retention is how long a finished stream stays available for replay,
	// and how long an unfinished stream may stay idle before it can be
	// evicted.
	Retention time.Duration
	// Audit stores audit records; Record fails without it.
	Audit AuditSink
	Clock func() time.Time
}

// Hub holds the bounded, in-memory progress of streams. It is not durable:
// a restart starts a new epoch, and a subscriber whose cursor belongs to an
// earlier epoch is told so with a gap. Every stream the hub creates gets a
// new incarnation, so an id never names two events. Publishing never blocks
// on a subscriber.
type Hub struct {
	config       HubConfig
	epoch        string
	mu           sync.Mutex
	streams      map[string]*stream
	owned        map[string]int
	incarnations uint64
	stats        Stats
}

// Stats counts what a hub dropped, by cause.
type Stats struct {
	// EvictedEvents were dropped from a replay buffer to stay within its
	// bounds.
	EvictedEvents int
	// EvictedStreams were dropped to make room for a new stream.
	EvictedStreams int
	// RefusedEvents could not be retained (no room for their stream, or a
	// finished stream).
	RefusedEvents int
	// SlowSubscribers were disconnected because their queue was full.
	SlowSubscribers int
}

type stored struct {
	seq   uint64
	typ   string
	data  []byte
	final bool
	// synthetic marks the expired event the hub publishes for work whose
	// outcome it no longer holds; the work's real final event replaces it.
	synthetic bool
}

type stream struct {
	id          string
	incarnation uint64
	owner       *trigger.Principal
	events      []*stored
	bytes       int
	next        uint64
	finished    bool
	touched     time.Time
	subs        map[*subscriber]struct{}
	// starts counts start requests that opened the stream and have not
	// settled; committed is set once work exists for it (a start was
	// committed, or something was published to it).
	starts    int
	committed bool
	// verified is set once something shows the stream will end: an
	// accepted submission, a publication, or the tracker. A stream a
	// repeated start recreated is unverified until then, because its work
	// may have settled with no one left to finish it. key is the
	// submission key the tracker is asked about.
	verified bool
	key      string
	// hole is the sequence number of a replaced expired notice, which is
	// not a lost event.
	hole uint64
}

// first returns the sequence number of the oldest retained event, or next
// when nothing is retained.
func (s *stream) first() uint64 {
	if len(s.events) == 0 {
		return s.next
	}
	return s.events[0].seq
}

type subscriber struct {
	events chan *stored
	done   chan struct{}
	once   sync.Once
	reason string
	// interrupt aborts a write in progress, so a closed subscriber's
	// connection ends at once rather than at its write deadline.
	interrupt func()
}

// close ends a subscription. done is closed before the pending write is
// interrupted, so a writer that sets its own deadline and then finds done
// open is guaranteed to be interrupted.
func (s *subscriber) close(reason string) {
	s.once.Do(func() {
		s.reason = reason
		close(s.done)
		if s.interrupt != nil {
			s.interrupt()
		}
	})
}

// NewHub validates the bounds. It starts no goroutine.
func NewHub(config HubConfig) (*Hub, error) {
	if config.MaxStreams <= 0 {
		config.MaxStreams = DefaultMaxStreams
	}
	if config.MaxStreamsPerOwner <= 0 {
		config.MaxStreamsPerOwner = min(DefaultMaxStreamsPerOwner, config.MaxStreams)
	}
	if config.RetainEvents <= 0 {
		config.RetainEvents = DefaultRetainEvents
	}
	if config.RetainBytes <= 0 {
		config.RetainBytes = DefaultRetainBytes
	}
	if config.MaxEventBytes <= 0 {
		config.MaxEventBytes = DefaultMaxEventBytes
	}
	if config.Retention <= 0 {
		config.Retention = DefaultRetention
	}
	if config.Clock == nil {
		config.Clock = time.Now
	}
	if config.MaxStreams > MaxStreamsLimit || config.MaxStreamsPerOwner > config.MaxStreams || config.RetainEvents > RetainEventsLimit || config.RetainBytes > RetainBytesLimit || config.MaxEventBytes > MaxEventBytesLimit {
		return nil, errors.New("sse: hub exceeds the stream, owner, event or byte bound")
	}
	if config.MaxEventBytes > config.RetainBytes {
		return nil, errors.New("sse: MaxEventBytes exceeds RetainBytes")
	}
	if int64(config.MaxStreams)*int64(config.RetainBytes) > MaxHubBytes {
		return nil, fmt.Errorf("sse: MaxStreams × RetainBytes exceeds %d bytes", MaxHubBytes)
	}
	epoch := make([]byte, 8)
	if _, err := rand.Read(epoch); err != nil {
		return nil, err
	}
	return &Hub{config: config, epoch: hex.EncodeToString(epoch), streams: map[string]*stream{}, owned: map[string]int{}}, nil
}

// Stats returns the drop counters.
func (h *Hub) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.stats
}

func (h *Hub) event(e Event) (*stored, error) {
	if !eventType.MatchString(e.Type) || e.Type == TypeGap {
		return nil, fmt.Errorf("%w: type %q", ErrInvalidEvent, e.Type)
	}
	data := e.Data
	if len(bytes.TrimSpace(data)) == 0 {
		data = json.RawMessage("null")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, data); err != nil {
		return nil, fmt.Errorf("%w: data is not JSON", ErrInvalidEvent)
	}
	if compact.Len() > h.config.MaxEventBytes {
		return nil, fmt.Errorf("%w: data exceeds %d bytes", ErrInvalidEvent, h.config.MaxEventBytes)
	}
	return &stored{typ: e.Type, data: compact.Bytes()}, nil
}

// Publish appends a progress event to a stream, creating the stream if
// needed, and hands it to every subscriber without waiting. It returns the
// event id. Progress is best effort: retention may later drop it, and a
// subscriber too slow to take it is disconnected.
func (h *Hub) Publish(streamID string, e Event) (string, error) {
	event, err := h.event(e)
	if err != nil {
		return "", err
	}
	return h.append(streamID, event)
}

// Finish publishes a stream's final event. Subscribers receive it and their
// streams end; the stream is kept for replay for the retention period.
func (h *Hub) Finish(streamID string, e Event) (string, error) {
	event, err := h.event(e)
	if err != nil {
		return "", err
	}
	event.final = true
	return h.append(streamID, event)
}

// Record stores an audit record synchronously, then publishes the event as
// progress. Audit and progress are separate policies: the record is never
// dropped (a sink failure is returned and nothing is published), while the
// published event may still be dropped like any progress.
func (h *Hub) Record(ctx context.Context, streamID string, e Event) (string, error) {
	event, err := h.event(e)
	if err != nil {
		return "", err
	}
	if h.config.Audit == nil {
		return "", ErrNoAuditSink
	}
	if err := h.config.Audit.Record(ctx, AuditRecord{Stream: streamID, Type: event.typ, Data: append(json.RawMessage(nil), event.data...), At: h.config.Clock()}); err != nil {
		return "", fmt.Errorf("%w: %w", ErrAudit, err)
	}
	id, err := h.append(streamID, event)
	if err != nil {
		// The record is stored; only the progress copy was refused.
		return "", nil
	}
	return id, nil
}

func (h *Hub) append(streamID string, event *stored) (string, error) {
	id, slow, err := h.appendLocked(streamID, event)
	// Disconnecting a subscriber interrupts its write, which under HTTP/2
	// waits on the connection; it never happens under the hub's lock.
	for _, sub := range slow {
		sub.close(ReasonSlow)
	}
	return id, err
}

func (h *Hub) appendLocked(streamID string, event *stored) (string, []*subscriber, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, err := h.stream(streamID)
	if err != nil {
		h.stats.RefusedEvents++
		return "", nil, err
	}
	if s.finished {
		last := len(s.events) - 1
		if !event.final || last < 0 || !s.events[last].synthetic {
			h.stats.RefusedEvents++
			return "", nil, ErrFinished
		}
		// The work's real outcome replaces the expired notice published
		// while it was not known; a subscriber that saw the notice and
		// reconnects from its id receives the outcome.
		s.bytes -= len(s.events[last].data)
		s.hole = s.events[last].seq
		s.events[last] = nil
		s.events = s.events[:last]
		s.finished = false
	}
	s.committed, s.verified = true, true
	event.seq = s.next
	s.next++
	s.events = append(s.events, event)
	s.bytes += len(event.data)
	for len(s.events) > h.config.RetainEvents || s.bytes > h.config.RetainBytes {
		s.bytes -= len(s.events[0].data)
		s.events[0] = nil
		s.events = s.events[1:]
		h.stats.EvictedEvents++
	}
	s.touched = h.config.Clock()
	if event.final {
		s.finished = true
	}
	var slow []*subscriber
	for sub := range s.subs {
		select {
		case sub.events <- event:
		default:
			h.stats.SlowSubscribers++
			delete(s.subs, sub)
			slow = append(slow, sub)
		}
	}
	if event.final {
		s.subs = map[*subscriber]struct{}{}
	}
	return h.id(s.incarnation, event.seq), slow, nil
}

// expire ends an unverified stream whose work the tracker reports settled:
// its outcome is no longer held. The work's real final event, if it
// arrives, replaces the notice. A verified or finished stream is left as
// it is.
func (h *Hub) expire(streamID string) {
	h.mu.Lock()
	s, ok := h.streams[streamID]
	stale := ok && !s.verified && !s.finished
	h.mu.Unlock()
	if !stale {
		return
	}
	event, _ := h.event(Event{Type: TypeExpired, Data: json.RawMessage(`{"reason":"expired"}`)})
	event.final, event.synthetic = true, true
	_, _ = h.append(streamID, event)
}

// verify records that a stream will end: its work was accepted, or the
// tracker reports it still running.
func (h *Hub) verify(streamID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s, ok := h.streams[streamID]; ok {
		s.verified = true
	}
}

// unverified returns the submission key of a stream that must be checked
// with the tracker: unverified and unfinished. Only a start that knows its
// key is committed may check while starts are in flight; anyone else waits
// for them, because a key not yet committed reads as settled.
func (h *Hub) unverified(streamID string, committed bool) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.streams[streamID]
	if !ok || s.verified || s.finished || (s.starts > 0 && !committed) || s.key == "" {
		return "", false
	}
	return s.key, true
}

// id is an event id: the hub's epoch, the stream's incarnation and the
// event's sequence number within it.
func (h *Hub) id(incarnation, seq uint64) string {
	return h.epoch + "." + strconv.FormatUint(incarnation, 10) + ":" + strconv.FormatUint(seq, 10)
}

// stream returns a stream, creating it when there is room; the caller holds
// mu.
func (h *Hub) stream(id string) (*stream, error) {
	if s, ok := h.streams[id]; ok {
		return s, nil
	}
	if len(h.streams) >= h.config.MaxStreams && !h.evict("") {
		return nil, ErrHubFull
	}
	h.incarnations++
	s := &stream{id: id, incarnation: h.incarnations, next: 1, touched: h.config.Clock(), subs: map[*subscriber]struct{}{}}
	h.streams[id] = s
	return s, nil
}

// remove drops a stream; the caller holds mu.
func (h *Hub) remove(s *stream) {
	delete(h.streams, s.id)
	if s.owner != nil {
		if h.owned[s.owner.ID]--; h.owned[s.owner.ID] <= 0 {
			delete(h.owned, s.owner.ID)
		}
	}
}

// evict drops one stream nobody is reading or starting, to make room, in
// this order: a finished stream past its retention, an unfinished stream
// idle past its retention (its work was abandoned or its publisher is
// gone), then the least recently touched finished stream. An unfinished
// stream still in use is never evicted: the new stream is refused instead.
// With an owner, only that principal's streams are candidates. The caller
// holds mu.
func (h *Hub) evict(owner string) bool {
	now := h.config.Clock()
	rank := func(s *stream) int {
		stale := now.Sub(s.touched) >= h.config.Retention
		switch {
		case s.finished && stale:
			return 0
		case stale:
			return 1
		case s.finished:
			return 2
		default:
			return -1
		}
	}
	var victim *stream
	for _, s := range h.streams {
		r := rank(s)
		if len(s.subs) > 0 || s.starts > 0 || r < 0 || (owner != "" && (s.owner == nil || s.owner.ID != owner)) {
			continue
		}
		if victim == nil || r < rank(victim) || (r == rank(victim) && s.touched.Before(victim.touched)) {
			victim = s
		}
	}
	if victim == nil {
		return false
	}
	h.remove(victim)
	h.stats.EvictedStreams++
	return true
}

// open registers a start request with the stream its work will report to,
// recording the principal that owns it, and creates the stream if needed;
// created reports whether it did. A stream owned by another principal is
// refused, as is a principal past its stream quota. Every successful open
// is followed by settle.
func (h *Hub) open(streamID string, owner trigger.Principal, key string) (created bool, err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.streams[streamID]
	if ok && s.owner != nil {
		if s.owner.ID != owner.ID {
			return false, errOwnerMismatch
		}
		s.starts++
		s.key = key
		s.touched = h.config.Clock()
		return false, nil
	}
	if h.owned[owner.ID] >= h.config.MaxStreamsPerOwner && !h.evict(owner.ID) {
		return false, errOwnerQuota
	}
	if !ok {
		if s, err = h.stream(streamID); err != nil {
			return false, err
		}
	}
	s.owner = &owner
	h.owned[owner.ID]++
	s.starts++
	s.key = key
	s.touched = h.config.Clock()
	return !ok, nil
}

// settle ends a start request. committed reports whether its work exists
// (the submission was accepted or was a duplicate). A stream that no
// committed start and no publication ever used is dropped once its last
// start settles, so a failed start never removes a stream another start
// committed.
func (h *Hub) settle(streamID string, committed bool) {
	h.mu.Lock()
	s, ok := h.streams[streamID]
	if !ok {
		h.mu.Unlock()
		return
	}
	s.starts--
	s.committed = s.committed || committed
	var orphans []*subscriber
	if s.starts == 0 && !s.committed {
		// No work exists for the stream: it goes, and a subscriber that
		// attached meanwhile is sent away (it will find nothing).
		for sub := range s.subs {
			orphans = append(orphans, sub)
		}
		s.subs = map[*subscriber]struct{}{}
		h.remove(s)
	}
	h.mu.Unlock()
	for _, sub := range orphans {
		sub.close(ReasonNoWork)
	}
}

// Gap tells a subscriber it cannot be given every event after its cursor.
type Gap struct {
	Reason string `json:"reason"`
	// Missed is how many events were dropped, when the hub knows.
	Missed uint64 `json:"missed,omitempty"`
}

type subscription struct {
	// incarnation names the stream the subscriber follows, for the ids of
	// live events.
	incarnation uint64
	sub         *subscriber
	gap         *Gap
	gapID       string
	replay      []*stored
	ids         []string
	done        bool
}

type cursor struct {
	epoch       string
	incarnation uint64
	seq         uint64
}

func canonicalUint(text string) (uint64, bool) {
	n, err := strconv.ParseUint(text, 10, 64)
	return n, err == nil && strconv.FormatUint(n, 10) == text
}

// parseCursor reads a Last-Event-ID: epoch.incarnation:seq.
func parseCursor(text string) (cursor, error) {
	head, seq, ok := strings.Cut(text, ":")
	if !ok {
		return cursor{}, errInvalidCursor
	}
	epoch, incarnation, ok := strings.Cut(head, ".")
	if !ok || len(epoch) != 16 || strings.Trim(epoch, "0123456789abcdef") != "" {
		return cursor{}, errInvalidCursor
	}
	c := cursor{epoch: epoch}
	if c.incarnation, ok = canonicalUint(incarnation); !ok || c.incarnation == 0 {
		return cursor{}, errInvalidCursor
	}
	if c.seq, ok = canonicalUint(seq); !ok {
		return cursor{}, errInvalidCursor
	}
	return c, nil
}

// subscribe authorizes a reader and registers it, atomically with
// computing its replay, so no event falls between replay and live delivery.
// A reader that may not follow the stream is told it does not exist, so a
// refusal does not reveal that the stream exists.
func (h *Hub) subscribe(streamID string, reader trigger.Principal, text string, authorize func(reader, owner trigger.Principal) error, depth, maxPerStream int, interrupt func()) (subscription, error) {
	var c cursor
	hasCursor := text != ""
	if hasCursor {
		var err error
		if c, err = parseCursor(text); err != nil {
			return subscription{}, err
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	s, ok := h.streams[streamID]
	if !ok || s.owner == nil || authorize(reader, *s.owner) != nil {
		return subscription{}, errUnknownStream
	}
	if !s.verified && !s.finished && s.starts == 0 {
		return subscription{}, errUnverified
	}
	result := subscription{incarnation: s.incarnation}
	after := c.seq
	switch {
	case !hasCursor:
	case c.epoch != h.epoch:
		result.gap = &Gap{Reason: GapRestart}
		after = 0
	case c.incarnation > s.incarnation, c.incarnation == s.incarnation && c.seq >= s.next:
		return subscription{}, errInvalidCursor
	case c.incarnation < s.incarnation:
		// The cursor belongs to an earlier incarnation of the stream, which
		// was evicted with its events; how many were lost is unknown.
		result.gap = &Gap{Reason: GapRetention}
		after = 0
	}
	if first := s.first(); after+1 < first && result.gap == nil {
		missed := first - 1 - after
		if s.hole > after && s.hole < first {
			missed--
		}
		if missed > 0 {
			result.gap = &Gap{Reason: GapRetention, Missed: missed}
		}
	}
	if result.gap != nil {
		result.gapID = h.id(s.incarnation, s.first()-1)
	}
	for _, event := range s.events {
		if event.seq > after {
			result.replay = append(result.replay, event)
			result.ids = append(result.ids, h.id(s.incarnation, event.seq))
		}
	}
	if s.finished {
		result.done = true
		return result, nil
	}
	if len(s.subs) >= maxPerStream {
		return subscription{}, errBusy
	}
	result.sub = &subscriber{events: make(chan *stored, depth), done: make(chan struct{}), interrupt: interrupt}
	s.subs[result.sub] = struct{}{}
	s.touched = h.config.Clock()
	return result, nil
}

func (h *Hub) unsubscribe(streamID string, sub *subscriber, reason string) {
	h.mu.Lock()
	if s, ok := h.streams[streamID]; ok {
		delete(s.subs, sub)
		s.touched = h.config.Clock()
	}
	h.mu.Unlock()
	sub.close(reason)
}
