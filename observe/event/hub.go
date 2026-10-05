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
	DefaultRecoveredPerPrincipal   = 16
	DefaultIdleTimeout             = 10 * time.Minute

	MaxRunsLimit       = 1 << 14
	MaxEventsPerRun    = 1 << 14
	MaxRunBytesLimit   = 16 << 20
	MaxEventBytesLimit = 1 << 20
	MaxSubscribersCap  = 1 << 12
	MaxQueueDepth      = 1024
	MaxLateWindow      = 30 * time.Second
	MaxIdleTimeout     = 2 * time.Hour
	// MaxRetainedBytes bounds MaxRuns × RunBytes.
	MaxRetainedBytes = 1 << 30
	// MaxQueuedBytes bounds MaxSubscribers × (QueueDepth × MaxEventBytes +
	// RunBytes): the most that readers can pin beyond the retained history,
	// a live queue plus one run's replay each.
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
	// GapReowned: the run had been attached from durable state under an
	// owner its trusted publisher contradicted; it restarted under the
	// publisher, and what the old attachment held is not carried over.
	GapReowned = "reowned"
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
	// MaxSubscribers and SubscribersPerPrincipal bound admitted readers
	// (Admit): every connection, whether it follows live, only replays, or
	// first needs a durable read. SubscribersPerRun bounds live followers of
	// one run. A reader beyond them is refused with ErrSaturated.
	MaxSubscribers          int
	SubscribersPerRun       int
	SubscribersPerPrincipal int
	// RecoveredPerPrincipal bounds the runs one reader principal has
	// attached from durable state (AttachRecovered). At the budget its own
	// least recently used unfollowed recovered run is recycled; a recovered
	// run never evicts a run that is still live.
	RecoveredPerPrincipal int
	// QueueDepth frames may wait for one subscriber. A publication that
	// finds the queue full disconnects that subscriber ("slow_subscriber")
	// instead of waiting; it resumes from its cursor.
	QueueDepth int
	// LateWindow is how long after a run's terminal frame a Late
	// observation (a worker log delivered off the result path) is still
	// accepted and delivered. A later one is dropped and marked.
	LateWindow time.Duration
	// IdleTimeout is how long an unfinished run may go without a
	// publication or a follower before a recovered read may recycle it
	// (#263). Below it, a recovered run never displaces a live run; past
	// it, an unfollowed unfinished run (one suspended, or one whose
	// execution in this process stopped without a terminal transition) is
	// as recyclable as a closed one, so a saturated hub holding only idle
	// runs cannot keep durable reads out indefinitely. A publisher that
	// later resumes a recycled run re-creates it behind an "evicted" gap.
	IdleTimeout time.Duration
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
		{&c.RecoveredPerPrincipal, DefaultRecoveredPerPrincipal, MaxRunsLimit, "RecoveredPerPrincipal"},
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
	if c.IdleTimeout < 0 || c.IdleTimeout > MaxIdleTimeout {
		return c, fmt.Errorf("observe/event: IdleTimeout must be within [0, %s]", MaxIdleTimeout)
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = DefaultIdleTimeout
	}
	if c.MaxEventBytes+frameOverhead > c.RunBytes {
		return c, errors.New("observe/event: MaxEventBytes plus frame overhead exceeds RunBytes")
	}
	if int64(c.MaxRuns)*int64(c.RunBytes) > MaxRetainedBytes {
		return c, fmt.Errorf("observe/event: MaxRuns × RunBytes exceeds %d bytes", MaxRetainedBytes)
	}
	if int64(c.MaxSubscribers)*(int64(c.QueueDepth)*int64(c.MaxEventBytes)+int64(c.RunBytes)) > MaxQueuedBytes {
		return c, fmt.Errorf("observe/event: MaxSubscribers × (QueueDepth × MaxEventBytes + RunBytes) exceeds %d bytes", MaxQueuedBytes)
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
	// Readers are admitted connections; Subscribers are live queues.
	Runs, Frames, RetainedBytes, Readers, Subscribers int
	Published                                         uint64
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
	// Reowned counts recovered runs whose attached owner differed from the
	// trusted publisher; the publisher won (see Publish).
	Reowned uint64
	// IdleRecycled counts unfinished runs a recovered read recycled after
	// IdleTimeout without a publication or a follower (also counted in
	// EvictedRuns).
	IdleRecycled uint64
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
	// attachedBy is the reader whose durable read attached a recovered run.
	attachedBy string
	// reownedFrom is the incarnation a publisher reclaimed this run from.
	reownedFrom uint64
	// active is the last publication, subscription or departure of a
	// follower; IdleTimeout is measured from it.
	active  time.Time
	element *list.Element
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
	readers     int
	byReader    map[string]int
	recoveredBy map[string]int
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
	return &Hub{cfg: config, epoch: hex.EncodeToString(raw[:]), runs: map[string]*run{}, lru: list.New(), byReader: map[string]int{}, recoveredBy: map[string]int{}}, nil
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
	stats.Readers = h.readers
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
	var reownedFrom uint64
	if current != nil && current.owner != owner && current.recovered {
		// The owner of a recovered run came from a durable read; the
		// publisher is the engine's trusted invocation. The publisher wins:
		// followers are detached so they re-authorize against the real
		// owner, and the new incarnation opens with a gap.
		h.stats.Reowned++
		reownedFrom = current.inc
		h.removeLocked(current, "reowned")
		current = nil
	}
	if current != nil && current.owner == owner && current.recovered {
		// The trusted owner is publishing: the run is live now. It stops
		// counting against the reader that attached it, may no longer be
		// displaced as a recovered run, and has live history from here.
		current.recovered = false
		h.releaseAttachLocked(current)
	}
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
		created, err := h.createLocked(runID, owner, now, false)
		if err != nil {
			h.stats.Dropped++
			return "", err
		}
		current = created
		current.reownedFrom = reownedFrom
		// Starting with the run's first observation loses nothing; otherwise
		// the earlier observations are gone, and the marker says why.
		switch {
		case item.Start:
		case reownedFrom != 0:
			h.appendLocked(current, h.gapFrame(current, GapReowned))
		default:
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
	item.active = h.cfg.Clock()
	if item.element != nil {
		h.lru.MoveToBack(item.element)
	}
}

func (h *Hub) createLocked(runID, owner string, now time.Time, recovered bool) (*run, error) {
	if len(h.runs) >= h.cfg.MaxRuns {
		var victim *run
		idle := false
		for element := h.lru.Front(); element != nil; element = element.Next() {
			candidate := element.Value.(*run)
			if len(candidate.subs) != 0 {
				continue
			}
			// A recovered run may displace only another recovered run, a
			// closed one, or one idle past IdleTimeout, never a run that is
			// still live.
			if !recovered || candidate.recovered || h.closedLocked(candidate, now) {
				victim = candidate
				break
			}
			if h.idleLocked(candidate, now) {
				victim, idle = candidate, true
				break
			}
		}
		if victim == nil {
			return nil, ErrSaturated
		}
		h.removeLocked(victim, "evicted")
		h.stats.EvictedRuns++
		if idle {
			h.stats.IdleRecycled++
		}
	}
	h.nextInc++
	item := &run{id: runID, owner: owner, inc: h.nextInc, next: 1, subs: map[*Subscriber]struct{}{}, active: now}
	item.element = h.lru.PushBack(item)
	h.runs[runID] = item
	return item, nil
}

// releaseAttachLocked returns a recovered run's slot to the budget of the
// reader that attached it.
func (h *Hub) releaseAttachLocked(item *run) {
	if item.attachedBy == "" {
		return
	}
	if h.recoveredBy[item.attachedBy]--; h.recoveredBy[item.attachedBy] <= 0 {
		delete(h.recoveredBy, item.attachedBy)
	}
	item.attachedBy = ""
}

// idleLocked reports an unfinished run without a publication, a new
// follower or a departing one for IdleTimeout.
func (h *Hub) idleLocked(item *run, now time.Time) bool {
	return !item.finished && !now.Before(item.active.Add(h.cfg.IdleTimeout))
}

// closedLocked reports a run that is finished and past its late window.
func (h *Hub) closedLocked(item *run, now time.Time) bool {
	return item.finished && (item.finishedAt.IsZero() || !now.Before(item.finishedAt.Add(h.cfg.LateWindow)))
}

func (h *Hub) removeLocked(item *run, reason string) {
	for sub := range item.subs {
		h.detachLocked(item, sub, reason)
	}
	h.releaseAttachLocked(item)
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
	item.active = h.cfg.Clock()
	sub.close(reason)
}

// AttachRecovered makes a run known from durable state followable. owner
// must be the run's durable owner as the durable source reports it, never
// the reader; reader is whose read attached it, for its recovered budget.
// The run has no live history: a reader without a cursor of this incarnation
// is told so by a gap. A terminal run is closed at once.
func (h *Hub) AttachRecovered(runID, owner, reader string, terminal bool) error {
	if !validIdentity(runID) || !validIdentity(owner) || !validIdentity(reader) {
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
	if h.recoveredBy[reader] >= h.cfg.RecoveredPerPrincipal {
		var own *run
		for element := h.lru.Front(); element != nil; element = element.Next() {
			candidate := element.Value.(*run)
			if candidate.attachedBy == reader && len(candidate.subs) == 0 {
				own = candidate
				break
			}
		}
		if own == nil {
			h.stats.RejectedSubscribers++
			return ErrSaturated
		}
		h.removeLocked(own, "evicted")
		h.stats.EvictedRuns++
	}
	item, err := h.createLocked(runID, owner, h.cfg.Clock(), true)
	if err != nil {
		h.stats.RejectedSubscribers++
		return err
	}
	item.recovered, item.attachedBy = true, reader
	h.recoveredBy[reader]++
	if terminal {
		// Closed immediately: the late window belongs to the process that
		// saw the terminal transition.
		item.finished, item.finishedAt = true, time.Time{}
	}
	return nil
}

// Recovered reports whether sub still follows a recovered attachment: the
// run it subscribed to is retained, in the same incarnation, and no trusted
// publisher has claimed it. Only such a run has nothing that would ever end
// it, so only its followers need to watch the durable source (#263).
func (h *Hub) Recovered(sub *Subscriber) bool {
	if sub == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.runs[sub.runID]
	return current != nil && current.inc == sub.inc && current.recovered
}

// FinishRecovered closes the recovered attachment sub follows, because its
// durable source reports the run terminal; a later reader then gets the
// reconstruction and an end instead of following. Like AttachRecovered for a
// terminal run, it is closed at once: the late window belongs to the process
// that saw the terminal transition. It reports false, and changes nothing,
// when the run is no longer that recovered attachment (a publisher claimed
// it, or it was recycled).
func (h *Hub) FinishRecovered(sub *Subscriber) bool {
	if sub == nil {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	current := h.runs[sub.runID]
	if current == nil || current.inc != sub.inc || !current.recovered {
		return false
	}
	if !current.finished {
		current.finished, current.finishedAt = true, time.Time{}
	}
	return true
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
	events    chan *Frame
	done      chan struct{}
	once      sync.Once
	reason    string
	runID     string
	reader    string
	inc       uint64
	admission *Admission
	// ownsAdmission: Hub.Subscribe admitted it, so Unsubscribe releases it.
	ownsAdmission bool
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

// Admission is one admitted reader connection. Every connection is admitted
// before it does any work, including a durable read or a replay that will
// not follow live, so reader limits bound all of them.
type Admission struct {
	hub      *Hub
	reader   string
	once     sync.Once
	released bool
}

// Admit admits one connection of reader within MaxSubscribers and
// SubscribersPerPrincipal.
func (h *Hub) Admit(reader string) (*Admission, error) {
	if !validIdentity(reader) {
		return nil, ErrNotFound
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, ErrClosed
	}
	if h.readers >= h.cfg.MaxSubscribers || h.byReader[reader] >= h.cfg.SubscribersPerPrincipal {
		h.stats.RejectedSubscribers++
		return nil, ErrSaturated
	}
	h.readers++
	h.byReader[reader]++
	return &Admission{hub: h, reader: reader}, nil
}

// Release ends the admission. It is idempotent.
func (a *Admission) Release() {
	if a == nil {
		return
	}
	a.once.Do(func() {
		h := a.hub
		h.mu.Lock()
		a.released = true
		h.readers--
		if h.byReader[a.reader]--; h.byReader[a.reader] <= 0 {
			delete(h.byReader, a.reader)
		}
		h.mu.Unlock()
	})
}

// Subscribe admits reader and subscribes it; Unsubscribe releases both.
// It is Admit followed by Admission.Subscribe.
func (h *Hub) Subscribe(runID, reader, cursor string, authorize Authorizer) (Replay, error) {
	admission, err := h.Admit(reader)
	if err != nil {
		return Replay{}, err
	}
	replay, err := admission.Subscribe(runID, cursor, authorize)
	if err != nil || replay.Subscriber == nil {
		admission.Release()
	} else {
		replay.Subscriber.ownsAdmission = true
	}
	return replay, err
}

// Subscribe authorizes the admitted reader before any retained data is selected, then
// selects the replay after cursor and registers the live queue under one
// lock, so no frame falls between them. An unauthorized reader gets
// ErrNotFound, exactly as for an unknown run. authorize runs outside the
// hub's lock.
func (a *Admission) Subscribe(runID, cursor string, authorize Authorizer) (Replay, error) {
	h, reader := a.hub, a.reader
	if !validIdentity(runID) {
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
	if h.closed || a.released {
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
	case parsed.inc < current.inc && parsed.inc == current.reownedFrom:
		result.Gap = &Gap{Reason: GapReowned}
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
	if len(current.subs) >= h.cfg.SubscribersPerRun {
		h.stats.RejectedSubscribers++
		return Replay{}, ErrSaturated
	}
	sub := &Subscriber{events: make(chan *Frame, h.cfg.QueueDepth), done: make(chan struct{}), runID: runID, reader: reader, inc: current.inc, admission: a}
	current.subs[sub] = struct{}{}
	h.subscribers++
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
	if sub.ownsAdmission {
		sub.admission.Release()
	}
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
