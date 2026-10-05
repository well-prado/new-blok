// Package event is a bounded, best-effort, process-local fanout of
// development observations, keyed by run and owned by the run's trusted
// principal. It imports no transport, store or UI: an adapter such as
// inspect.NewEventHandler frames it for a client.
//
// Nothing here is reliable state or audit. Publishing never waits for a
// subscriber, so a slow reader can never hold up the run that produces the
// observations, nor the journal transitions that run commits. Loss is never
// silent: every dropped, evicted or rejected observation is counted in Stats
// and becomes visible to a reader as a gap.
package event

import (
	"bytes"
	"container/list"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Defaults and hard limits. Every bound has a saturation policy, documented on
// Config.
const (
	DefaultMaxRuns                 = 256
	DefaultEventsPerRun            = 1024
	DefaultRunBytes                = 256 << 10
	DefaultMaxEventBytes           = 32 << 10
	DefaultMaxSubscribers          = 64
	DefaultSubscribersPerRun       = 8
	DefaultSubscribersPerPrincipal = 16
	DefaultQueueDepth              = 32
	DefaultLateWindow              = 2 * time.Second

	MaxRunsLimit       = 1 << 14
	MaxEventsPerRun    = 1 << 14
	MaxRunBytesLimit   = 16 << 20
	MaxEventBytesLimit = 1 << 20
	MaxSubscribersCap  = 1 << 12
	MaxQueueDepth      = 1024
	MaxLateWindow      = 30 * time.Second
	// MaxRetainedBytes bounds MaxRuns × RunBytes.
	MaxRetainedBytes = 1 << 30
	// MaxQueuedBytes bounds MaxSubscribers × QueueDepth × MaxEventBytes: the
	// most that subscriber queues can pin beyond the retained history.
	MaxQueuedBytes = 256 << 20
	// MaxCursorBytes bounds a cursor before it is parsed.
	MaxCursorBytes = 128
	// MaxIdentityBytes bounds run IDs and principals.
	MaxIdentityBytes = 256
)

// frameOverhead bounds a frame's accounted name and cursor bytes.
const frameOverhead = 256

// GapName is the reserved frame name of a loss marker.
const GapName = "gap"

// Gap reasons. A reader that sees a gap knows observations are missing at
// that position, and why.
const (
	// GapDropped: an observation could not be represented within the
	// configured bounds and was dropped at publication.
	GapDropped = "dropped"
	// GapEvicted: this process forgot the run (MaxRuns) while it was still
	// producing observations; what came before is gone.
	GapEvicted = "evicted"
	// GapRetention: the reader's cursor is older than the retained history.
	GapRetention = "retention"
	// GapRestart: the reader's cursor is from another process epoch.
	GapRestart = "restart"
	// GapUnavailable: the run is known only from durable state; this
	// process has none of its live history.
	GapUnavailable = "history_unavailable"
	// GapLateDropped: a late observation arrived after the run's late
	// window closed.
	GapLateDropped = "late_dropped"
)

var (
	ErrNotFound       = errors.New("observe/event: run not found")
	ErrUnauthorized   = errors.New("observe/event: principal does not own the run")
	ErrInvalidCursor  = errors.New("observe/event: invalid cursor")
	ErrSaturated      = errors.New("observe/event: capacity saturated")
	ErrRunFinished    = errors.New("observe/event: run already finished")
	ErrLate           = errors.New("observe/event: late window closed")
	ErrInvalidPayload = errors.New("observe/event: invalid or oversized observation")
	ErrClosed         = errors.New("observe/event: hub closed")
)

// Config bounds a Hub. A zero field takes its default; a field above its hard
// limit is refused by New.
type Config struct {
	// MaxRuns runs are retained. A new run evicts the least recently used
	// run that nobody follows; when every retained run is followed the new
	// run's observations are dropped and counted (Stats.Dropped).
	MaxRuns int
	// EventsPerRun and RunBytes bound one run's history; the oldest frames
	// are evicted first (Stats.EvictedFrames) and a reader whose cursor is
	// older sees a retention gap.
	EventsPerRun int
	RunBytes     int
	// MaxEventBytes bounds one frame's data. A larger observation is
	// dropped and replaced by a gap marker.
	MaxEventBytes int
	// MaxSubscribers, SubscribersPerRun and SubscribersPerPrincipal bound
	// readers; a subscription beyond them is refused with ErrSaturated.
	MaxSubscribers          int
	SubscribersPerRun       int
	SubscribersPerPrincipal int
	// QueueDepth frames may wait for one subscriber. A publication that
	// finds the queue full disconnects that subscriber ("slow_subscriber")
	// instead of waiting; it resumes from its cursor.
	QueueDepth int
	// LateWindow is how long after a run's terminal frame a Late
	// observation (a worker log delivered off the result path) is still
	// accepted and delivered. A later one is dropped and marked.
	LateWindow time.Duration
	// Clock is for tests.
	Clock func() time.Time
}

func (c Config) withDefaults() (Config, error) {
	defaults := []struct {
		value *int
		def   int
		max   int
		name  string
	}{
		{&c.MaxRuns, DefaultMaxRuns, MaxRunsLimit, "MaxRuns"},
		{&c.EventsPerRun, DefaultEventsPerRun, MaxEventsPerRun, "EventsPerRun"},
		{&c.RunBytes, DefaultRunBytes, MaxRunBytesLimit, "RunBytes"},
		{&c.MaxEventBytes, DefaultMaxEventBytes, MaxEventBytesLimit, "MaxEventBytes"},
		{&c.MaxSubscribers, DefaultMaxSubscribers, MaxSubscribersCap, "MaxSubscribers"},
		{&c.SubscribersPerRun, DefaultSubscribersPerRun, MaxSubscribersCap, "SubscribersPerRun"},
		{&c.SubscribersPerPrincipal, DefaultSubscribersPerPrincipal, MaxSubscribersCap, "SubscribersPerPrincipal"},
		{&c.QueueDepth, DefaultQueueDepth, MaxQueueDepth, "QueueDepth"},
	}
	for _, item := range defaults {
		if *item.value < 0 {
			return c, fmt.Errorf("observe/event: %s is negative", item.name)
		}
		if *item.value == 0 {
			*item.value = item.def
		}
		if *item.value > item.max {
			return c, fmt.Errorf("observe/event: %s %d exceeds hard limit %d", item.name, *item.value, item.max)
		}
	}
	if c.LateWindow < 0 || c.LateWindow > MaxLateWindow {
		return c, fmt.Errorf("observe/event: LateWindow must be within [0, %s]", MaxLateWindow)
	}
	if c.LateWindow == 0 {
		c.LateWindow = DefaultLateWindow
	}
	if c.MaxEventBytes+frameOverhead > c.RunBytes {
		return c, errors.New("observe/event: MaxEventBytes plus frame overhead exceeds RunBytes")
	}
	if int64(c.MaxRuns)*int64(c.RunBytes) > MaxRetainedBytes {
		return c, fmt.Errorf("observe/event: MaxRuns × RunBytes exceeds %d bytes", MaxRetainedBytes)
	}
	if int64(c.MaxSubscribers)*int64(c.QueueDepth)*int64(c.MaxEventBytes) > MaxQueuedBytes {
		return c, fmt.Errorf("observe/event: MaxSubscribers × QueueDepth × MaxEventBytes exceeds %d bytes", MaxQueuedBytes)
	}
	if c.SubscribersPerRun > c.MaxSubscribers || c.SubscribersPerPrincipal > c.MaxSubscribers {
		return c, errors.New("observe/event: a per-run or per-principal subscriber limit exceeds MaxSubscribers")
	}
	if c.Clock == nil {
		c.Clock = time.Now
	}
	return c, nil
}

// Stats is counters only; it never exposes contents or principals.
type Stats struct {
	Runs, Frames, RetainedBytes, Subscribers int
	Published                                uint64
	// Dropped observations: invalid or oversized, or no run slot.
	Dropped uint64
	// Rejected publications: wrong owner, or a non-late observation after
	// the run's terminal frame.
	Rejected            uint64
	EvictedFrames       uint64
	EvictedRuns         uint64
	SlowSubscribers     uint64
	RejectedSubscribers uint64
	LateDelivered       uint64
	LateDropped         uint64
}

// Frame is one immutable, already-projected observation. Its bytes are
// shared by every reader and must not be modified.
type Frame struct {
	seq        uint64
	name       string
	data       []byte
	terminal   bool
	late       bool
	cursor     string
	finishedAt time.Time
}

func (f *Frame) Name() string { return f.name }

// Data returns the frame's compact JSON; callers must not modify it.
func (f *Frame) Data() []byte   { return f.data }
func (f *Frame) Cursor() string { return f.cursor }
func (f *Frame) Terminal() bool { return f.terminal }
func (f *Frame) Gap() bool      { return f.name == GapName }
func (f *Frame) Late() bool     { return f.late }
func (f *Frame) Seq() uint64    { return f.seq }
func (f *Frame) Size() int      { return len(f.data) + len(f.name) + len(f.cursor) }

// FinishedAt is when a terminal frame finished its run; zero otherwise.
func (f *Frame) FinishedAt() time.Time { return f.finishedAt }

// Item is one observation to publish.
type Item struct {
	// Name is the frame type, ^[a-z][a-z0-9_.-]{0,63}$; "gap" is reserved.
	Name string
	// Data is one JSON value. It is compacted, so a frame is one line.
	Data []byte
	// Start marks a run's first observation. A run created by any other
	// observation begins with an "evicted" gap. A Start on a finished run
	// begins a new incarnation.
	Start bool
	// Terminal marks the run's final transition.
	Terminal bool
	// Late marks an observation that may legitimately arrive after the
	// terminal frame, within LateWindow.
	Late bool
}

type run struct {
	id, owner  string
	inc        uint64
	next       uint64
	frames     []*Frame
	bytes      int
	subs       map[*Subscriber]struct{}
	finished   bool
	finishedAt time.Time
	recovered  bool
	element    *list.Element
}

// Hub is safe for concurrent use. Its lock is held only for bounded,
// in-memory work; no callback, I/O or channel wait happens under it.
type Hub struct {
	cfg         Config
	epoch       string
	mu          sync.Mutex
	runs        map[string]*run
	lru         *list.List // of *run, least recently used first
	nextInc     uint64
	subscribers int
	byReader    map[string]int
	stats       Stats
	retained    int
	closed      bool
}

// New validates config and starts a new epoch: cursors from another Hub,
// including one from before a restart, are recognized as another epoch.
func New(config Config) (*Hub, error) {
	config, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	var raw [8]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, fmt.Errorf("observe/event: create epoch: %w", err)
	}
	return &Hub{cfg: config, epoch: hex.EncodeToString(raw[:]), runs: map[string]*run{}, lru: list.New(), byReader: map[string]int{}}, nil
}

// Config returns the effective configuration.
func (h *Hub) Config() Config { return h.cfg }

func (h *Hub) Stats() Stats {
	h.mu.Lock()
	defer h.mu.Unlock()
	stats := h.stats
	stats.Runs = len(h.runs)
	stats.RetainedBytes = h.retained
	stats.Subscribers = h.subscribers
	for _, item := range h.runs {
		stats.Frames += len(item.frames)
	}
	return stats
}

func validName(name string) bool {
	if len(name) == 0 || len(name) > 64 || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for i := 1; i < len(name); i++ {
		c := name[i]
		if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '_' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

func validIdentity(value string) bool { return value != "" && len(value) <= MaxIdentityBytes }

// Publish appends one observation of runID, owned by the trusted principal
// owner, and offers it to the run's subscribers without waiting. It returns
// the frame's cursor.
func (h *Hub) Publish(runID, owner string, item Item) (string, error) {
	if !validIdentity(runID) || !validIdentity(owner) {
		h.mu.Lock()
		h.stats.Dropped++
		h.mu.Unlock()
		return "", ErrInvalidPayload
	}
	var data []byte
	valid := validName(item.Name) && item.Name != GapName && len(item.Data) <= h.cfg.MaxEventBytes && json.Valid(item.Data)
	if valid {
		var compact bytes.Buffer
		if json.Compact(&compact, item.Data) != nil || compact.Len() > h.cfg.MaxEventBytes {
			valid = false
		} else {
			data = compact.Bytes()
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if !valid {
		h.dropLocked(runID, owner, GapDropped)
		return "", ErrInvalidPayload
	}
	now := h.cfg.Clock()
	current := h.runs[runID]
	if current != nil && current.owner != owner {
		h.stats.Rejected++
		return "", ErrUnauthorized
	}
	if current != nil && current.finished {
		switch {
		case item.Start:
			h.removeLocked(current, "superseded")
			current = nil
		case item.Late && now.Before(current.finishedAt.Add(h.cfg.LateWindow)):
			h.stats.LateDelivered++
		case item.Late:
			h.stats.LateDropped++
			if last := lastFrame(current); last == nil || !last.Gap() || !bytes.Contains(last.data, []byte(GapLateDropped)) {
				h.appendLocked(current, h.gapFrame(current, GapLateDropped))
			}
			return "", ErrLate
		default:
			h.stats.Rejected++
			return "", ErrRunFinished
		}
	}
	if current == nil && item.Late {
		// A late observation of a run this process does not hold has nobody
		// to reach and nothing that would end it.
		h.stats.Dropped++
		return "", ErrNotFound
	}
	if current == nil {
		created, err := h.createLocked(runID, owner, now)
		if err != nil {
			h.stats.Dropped++
			return "", err
		}
		current = created
		if !item.Start {
			h.appendLocked(current, h.gapFrame(current, GapEvicted))
		}
	}
	frame := &Frame{seq: current.next, name: item.Name, data: data, terminal: item.Terminal, late: item.Late}
	frame.cursor = h.cursor(current, frame.seq)
	if item.Terminal {
		current.finished, current.finishedAt = true, now
		frame.finishedAt = now
	}
	h.appendLocked(current, frame)
	h.stats.Published++
	return frame.cursor, nil
}

// Drop records an observation of runID that could not be represented. A
// retained run owned by owner receives a gap marker in sequence.
func (h *Hub) Drop(runID, owner string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dropLocked(runID, owner, GapDropped)
}

func (h *Hub) dropLocked(runID, owner, reason string) {
	h.stats.Dropped++
	if current := h.runs[runID]; current != nil && current.owner == owner {
		h.appendLocked(current, h.gapFrame(current, reason))
	}
}

func lastFrame(item *run) *Frame {
	if len(item.frames) == 0 {
		return nil
	}
	return item.frames[len(item.frames)-1]
}

func (h *Hub) gapFrame(item *run, reason string) *Frame {
	data, _ := json.Marshal(struct {
		Reason string `json:"reason"`
	}{reason})
	frame := &Frame{seq: item.next, name: GapName, data: data}
	frame.cursor = h.cursor(item, frame.seq)
	return frame
}

// appendLocked consumes the frame's sequence number, retains it within the
// run's bounds and offers it to every subscriber without blocking.
func (h *Hub) appendLocked(item *run, frame *Frame) {
	item.next = frame.seq + 1
	size := frame.Size()
	for len(item.frames) > 0 && (len(item.frames) >= h.cfg.EventsPerRun || item.bytes+size > h.cfg.RunBytes) {
		old := item.frames[0]
		item.frames[0] = nil
		item.frames = item.frames[1:]
		item.bytes -= old.Size()
		h.retained -= old.Size()
		h.stats.EvictedFrames++
	}
	item.frames = append(item.frames, frame)
	item.bytes += size
	h.retained += size
	for sub := range item.subs {
		select {
		case sub.events <- frame:
		default:
			h.stats.SlowSubscribers++
			h.detachLocked(item, sub, "slow_subscriber")
		}
	}
	h.touchLocked(item)
}

func (h *Hub) touchLocked(item *run) {
	if item.element != nil {
		h.lru.MoveToBack(item.element)
	}
}

func (h *Hub) createLocked(runID, owner string, now time.Time) (*run, error) {
	if len(h.runs) >= h.cfg.MaxRuns {
		var victim *run
		for element := h.lru.Front(); element != nil; element = element.Next() {
			candidate := element.Value.(*run)
			if len(candidate.subs) == 0 {
				victim = candidate
				break
			}
		}
		if victim == nil {
			return nil, ErrSaturated
		}
		h.removeLocked(victim, "evicted")
		h.stats.EvictedRuns++
	}
	h.nextInc++
	item := &run{id: runID, owner: owner, inc: h.nextInc, next: 1, subs: map[*Subscriber]struct{}{}}
	item.element = h.lru.PushBack(item)
	h.runs[runID] = item
	return item, nil
}

func (h *Hub) removeLocked(item *run, reason string) {
	for sub := range item.subs {
		h.detachLocked(item, sub, reason)
	}
	h.retained -= item.bytes
	if item.element != nil {
		h.lru.Remove(item.element)
		item.element = nil
	}
	delete(h.runs, item.id)
}

func (h *Hub) detachLocked(item *run, sub *Subscriber, reason string) {
	if _, ok := item.subs[sub]; !ok {
		return
	}
	delete(item.subs, sub)
	h.subscribers--
	if h.byReader[sub.reader]--; h.byReader[sub.reader] <= 0 {
		delete(h.byReader, sub.reader)
	}
	sub.close(reason)
}

// AttachRecovered makes a run known from durable state followable after an
// authorized durable read established that owner owns it. The run has no
// live history: a reader without a cursor of this incarnation is told so by
// a gap. A terminal run is closed at once.
func (h *Hub) AttachRecovered(runID, owner string, terminal bool) error {
	if !validIdentity(runID) || !validIdentity(owner) {
		return ErrNotFound
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return ErrClosed
	}
	if current := h.runs[runID]; current != nil {
		if current.owner != owner {
			return ErrUnauthorized
		}
		return nil
	}
	item, err := h.createLocked(runID, owner, h.cfg.Clock())
	if err != nil {
		return err
	}
	item.recovered = true
	if terminal {
		// Closed immediately: the late window belongs to the process that
		// saw the terminal transition.
		item.finished, item.finishedAt = true, time.Time{}
	}
	return nil
}

// Gap is a loss a reader must be told about before its replay.
type Gap struct {
	Reason string `json:"reason"`
	// Missed is the number of frames known to be missing; zero means
	// unknown.
	Missed uint64 `json:"missed,omitempty"`
}

// Replay is the outcome of a subscription: an optional leading gap, the
// retained frames after the cursor, and a live Subscriber unless the run is
// closed.
type Replay struct {
	Gap       *Gap
	GapCursor string
	Frames    []*Frame
	// Subscriber is nil when the run is closed (finished and past its late
	// window).
	Subscriber *Subscriber
	Finished   bool
	// CloseAt is when a finished run's late window closes.
	CloseAt time.Time
	// LastCursor is the cursor of the run's last frame, or of its start.
	LastCursor string
	Recovered  bool
}

// Subscriber is a bounded queue of frames for one reader.
type Subscriber struct {
	events chan *Frame
	done   chan struct{}
	once   sync.Once
	reason string
	runID  string
	reader string
	inc    uint64
}

func (s *Subscriber) Events() <-chan *Frame { return s.events }
func (s *Subscriber) Done() <-chan struct{} { return s.done }

// Reason is valid after Done is closed.
func (s *Subscriber) Reason() string { <-s.done; return s.reason }
func (s *Subscriber) close(reason string) {
	s.once.Do(func() { s.reason = reason; close(s.done) })
}

// Authorizer decides whether reader may follow a run owned by owner.
type Authorizer func(reader, owner string) error

// SameOwner allows only the run's own principal.
func SameOwner(reader, owner string) error {
	if reader != "" && reader == owner {
		return nil
	}
	return ErrUnauthorized
}

// Subscribe authorizes reader before any retained data is selected, then
// selects the replay after cursor and registers the live queue under one
// lock, so no frame falls between them. An unauthorized reader gets
// ErrNotFound, exactly as for an unknown run. authorize runs outside the
// hub's lock.
func (h *Hub) Subscribe(runID, reader, cursor string, authorize Authorizer) (Replay, error) {
	if !validIdentity(runID) || !validIdentity(reader) {
		return Replay{}, ErrNotFound
	}
	if authorize == nil {
		authorize = SameOwner
	}
	if len(cursor) > MaxCursorBytes {
		return Replay{}, ErrInvalidCursor
	}
	var parsed cursorValue
	if cursor != "" {
		var err error
		if parsed, err = parseCursor(cursor); err != nil {
			return Replay{}, err
		}
	}
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return Replay{}, ErrClosed
	}
	current := h.runs[runID]
	if current == nil {
		h.mu.Unlock()
		return Replay{}, ErrNotFound
	}
	owner, incarnation := current.owner, current.inc
	h.mu.Unlock()
	if authorize(reader, owner) != nil {
		return Replay{}, ErrNotFound
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return Replay{}, ErrClosed
	}
	current = h.runs[runID]
	if current == nil || current.owner != owner || current.inc != incarnation {
		return Replay{}, ErrNotFound
	}
	result := Replay{Finished: current.finished, Recovered: current.recovered, LastCursor: h.cursor(current, current.next-1)}
	after := uint64(0)
	switch {
	case cursor == "":
		if current.recovered {
			result.Gap = &Gap{Reason: GapUnavailable}
		}
	case parsed.runHash != runHash(runID):
		return Replay{}, ErrInvalidCursor
	case parsed.epoch != h.epoch:
		result.Gap = &Gap{Reason: GapRestart}
	case parsed.inc > current.inc:
		return Replay{}, ErrInvalidCursor
	case parsed.inc < current.inc:
		result.Gap = &Gap{Reason: GapRetention}
	case parsed.seq >= current.next:
		return Replay{}, ErrInvalidCursor
	default:
		after = parsed.seq
	}
	first := current.next
	if len(current.frames) > 0 {
		first = current.frames[0].seq
	}
	if result.Gap == nil && after+1 < first {
		result.Gap = &Gap{Reason: GapRetention, Missed: first - 1 - after}
	}
	if result.Gap != nil {
		// A reader that reconnects from the gap continues without another.
		result.GapCursor = h.cursor(current, max(after, first-1))
	}
	for _, frame := range current.frames {
		if frame.seq > after {
			result.Frames = append(result.Frames, frame)
		}
	}
	now := h.cfg.Clock()
	if current.finished {
		result.CloseAt = current.finishedAt.Add(h.cfg.LateWindow)
		if current.finishedAt.IsZero() || !now.Before(result.CloseAt) {
			result.CloseAt = time.Time{}
			return result, nil
		}
	}
	if len(current.subs) >= h.cfg.SubscribersPerRun || h.subscribers >= h.cfg.MaxSubscribers || h.byReader[reader] >= h.cfg.SubscribersPerPrincipal {
		h.stats.RejectedSubscribers++
		return Replay{}, ErrSaturated
	}
	sub := &Subscriber{events: make(chan *Frame, h.cfg.QueueDepth), done: make(chan struct{}), runID: runID, reader: reader, inc: current.inc}
	current.subs[sub] = struct{}{}
	h.subscribers++
	h.byReader[reader]++
	h.touchLocked(current)
	result.Subscriber = sub
	return result, nil
}

// Unsubscribe removes sub. Frames already queued stay readable.
func (h *Hub) Unsubscribe(sub *Subscriber, reason string) {
	if sub == nil {
		return
	}
	h.mu.Lock()
	if current := h.runs[sub.runID]; current != nil && current.inc == sub.inc {
		h.detachLocked(current, sub, reason)
	}
	h.mu.Unlock()
	sub.close(reason)
}

// Close ends every subscription ("shutdown") and refuses new ones.
// Publication keeps working so a run in flight is never affected.
func (h *Hub) Close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	for _, item := range h.runs {
		for sub := range item.subs {
			h.detachLocked(item, sub, "shutdown")
		}
	}
}

// Cursor format: <epoch 16 hex>.<incarnation>.<run hash 8 hex>:<seq>. It
// binds a position to one run, one incarnation and one process epoch.
type cursorValue struct {
	epoch   string
	inc     uint64
	runHash uint32
	seq     uint64
}

func runHash(runID string) uint32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(runID))
	return hash.Sum32()
}

func (h *Hub) cursor(item *run, seq uint64) string {
	return h.epoch + "." + strconv.FormatUint(item.inc, 10) + "." + fmt.Sprintf("%08x", runHash(item.id)) + ":" + strconv.FormatUint(seq, 10)
}

func canonicalUint(text string) (uint64, bool) {
	n, err := strconv.ParseUint(text, 10, 64)
	return n, err == nil && strconv.FormatUint(n, 10) == text
}

func lowerHex(text string, length int) bool {
	return len(text) == length && strings.Trim(text, "0123456789abcdef") == ""
}

func parseCursor(text string) (cursorValue, error) {
	head, seq, ok := strings.Cut(text, ":")
	if !ok {
		return cursorValue{}, ErrInvalidCursor
	}
	parts := strings.Split(head, ".")
	if len(parts) != 3 || !lowerHex(parts[0], 16) || !lowerHex(parts[2], 8) {
		return cursorValue{}, ErrInvalidCursor
	}
	inc, ok := canonicalUint(parts[1])
	if !ok || inc == 0 {
		return cursorValue{}, ErrInvalidCursor
	}
	hash, err := strconv.ParseUint(parts[2], 16, 32)
	if err != nil {
		return cursorValue{}, ErrInvalidCursor
	}
	position, ok := canonicalUint(seq)
	if !ok {
		return cursorValue{}, ErrInvalidCursor
	}
	return cursorValue{epoch: parts[0], inc: inc, runHash: uint32(hash), seq: position}, nil
}
