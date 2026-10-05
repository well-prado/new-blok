package inspect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/observe/event"
)

// Event handler bounds.
const (
	DefaultEventMaxDuration   = 30 * time.Minute
	MaxEventMaxDuration       = 2 * time.Hour
	DefaultEventHeartbeat     = 15 * time.Second
	MaxEventHeartbeat         = time.Minute
	DefaultEventWriteTimeout  = 5 * time.Second
	MaxEventWriteTimeout      = time.Minute
	DefaultEventSourceTimeout = 5 * time.Second
	MaxEventSourceTimeout     = time.Minute
	DefaultEventRetry         = time.Second
	DefaultEventSnapshotBytes = 64 << 10
	MaxEventSnapshotBytes     = 1 << 20
	MinEventRetry             = 100 * time.Millisecond
	MaxEventRetry             = 10 * time.Minute
	DefaultEventRecoveredPoll = 2 * time.Second
	MinEventRecoveredPoll     = 100 * time.Millisecond
	MaxEventRecoveredPoll     = time.Minute
	defaultEventSnapshotPage  = 20
)

// EventHandlerConfig composes the SSE endpoint for an EventStream.
// Authentication and policy belong to the application: the handler never
// takes a principal, field selection or capture choice from the request.
type EventHandlerConfig struct {
	// Authenticate returns the verified reader principal. Required.
	Authenticate func(*http.Request) (string, error)
	// Authorize decides whether reader may follow a run owned by owner.
	// Nil allows only the owner. A refused reader is answered exactly like
	// an unknown run.
	Authorize event.Authorizer
	// Policy selects the optional fields a reader may see. Nil, or a field
	// left false, withholds it. A field the stream did not capture is
	// withheld whatever the policy says.
	Policy func(principal string) inspection.Policy
	// Source optionally reconstructs a run this process has no live
	// history of (after a restart, or eviction) from durable state. It is
	// read as the reader, so it authorizes again.
	Source inspection.Source
	// MaxDuration bounds one subscription; the client resumes from its
	// cursor. Heartbeat keeps an idle subscription alive. WriteTimeout
	// bounds every write. SourceTimeout bounds a durable read.
	MaxDuration   time.Duration
	Heartbeat     time.Duration
	WriteTimeout  time.Duration
	SourceTimeout time.Duration
	// Retry is the reconnect hint sent to EventSource.
	Retry time.Duration
	// RecoveredPoll is how often a reader following a recovered run (one
	// known only from Source, that no execution in this process publishes,
	// such as a cluster run or a crashed one) re-reads it from Source. A
	// changed reconstruction is sent as a new snapshot; a terminal one
	// ends the stream. Each poll is one durable read, bounded by
	// SourceTimeout, so one connection makes at most MaxDuration /
	// RecoveredPoll of them.
	RecoveredPoll time.Duration
	// SnapshotBytes bounds a reconstructed snapshot.
	SnapshotBytes int
}

type eventHandler struct {
	stream *EventStream
	cfg    EventHandlerConfig
}

// NewEventHandler returns a read-only Server-Sent Events endpoint serving
// GET .../runs/{run}/events. Mount it under any prefix.
func NewEventHandler(stream *EventStream, config EventHandlerConfig) (http.Handler, error) {
	if stream == nil || stream.hub == nil || config.Authenticate == nil {
		return nil, errors.New("inspection: an event stream and an authenticator are required")
	}
	if config.Authorize == nil {
		config.Authorize = event.SameOwner
	}
	bounds := []struct {
		value             *time.Duration
		def, minimum, max time.Duration
		name              string
	}{
		{&config.MaxDuration, DefaultEventMaxDuration, 0, MaxEventMaxDuration, "MaxDuration"},
		{&config.Heartbeat, DefaultEventHeartbeat, 0, MaxEventHeartbeat, "Heartbeat"},
		{&config.WriteTimeout, DefaultEventWriteTimeout, 0, MaxEventWriteTimeout, "WriteTimeout"},
		{&config.SourceTimeout, DefaultEventSourceTimeout, 0, MaxEventSourceTimeout, "SourceTimeout"},
		{&config.Retry, DefaultEventRetry, MinEventRetry, MaxEventRetry, "Retry"},
		{&config.RecoveredPoll, DefaultEventRecoveredPoll, MinEventRecoveredPoll, MaxEventRecoveredPoll, "RecoveredPoll"},
	}
	for _, bound := range bounds {
		if *bound.value < 0 {
			return nil, errors.New("inspection: event handler " + bound.name + " is negative")
		}
		if *bound.value == 0 {
			*bound.value = bound.def
		}
		if *bound.value > bound.max || *bound.value < bound.minimum {
			return nil, errors.New("inspection: event handler " + bound.name + " is outside its bound")
		}
	}
	if config.SnapshotBytes < 0 || config.SnapshotBytes > MaxEventSnapshotBytes {
		return nil, errors.New("inspection: event handler SnapshotBytes is outside its bound")
	}
	if config.SnapshotBytes == 0 {
		config.SnapshotBytes = DefaultEventSnapshotBytes
	}
	return &eventHandler{stream: stream, cfg: config}, nil
}

// fields is the effective field selection: the reader's policy, limited to
// what the stream captured. Error labels are classified, not sensitive
// content, and are not capture-gated.
func (h *eventHandler) fields(principal string) map[inspection.Field]bool {
	var policy inspection.Policy
	if h.cfg.Policy != nil {
		policy = h.cfg.Policy(principal)
	}
	capture := h.stream.capture
	return map[inspection.Field]bool{
		inspection.FieldInput:  policy.Fields[inspection.FieldInput] && capture.Inputs,
		inspection.FieldOutput: policy.Fields[inspection.FieldOutput] && capture.Outputs,
		inspection.FieldLogs:   policy.Fields[inspection.FieldLogs] && capture.Logs,
		inspection.FieldError:  policy.Fields[inspection.FieldError],
	}
}

func (h *eventHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodGet {
		writer.Header().Set("Allow", http.MethodGet)
		writeEventError(writer, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	principal, err := h.cfg.Authenticate(request)
	if err != nil || principal == "" || len(principal) > event.MaxIdentityBytes {
		writeEventError(writer, http.StatusUnauthorized, "unauthorized")
		return
	}
	runID, ok := parseEventPath(request.URL.Path)
	if !ok {
		writeEventError(writer, http.StatusNotFound, "not_found")
		return
	}
	cursor := request.Header.Get("Last-Event-ID")
	if len(cursor) > event.MaxCursorBytes {
		writeEventError(writer, http.StatusBadRequest, "invalid_cursor")
		return
	}
	control := http.NewResponseController(writer)
	// Every write must be bounded and interruptible.
	if err := control.SetWriteDeadline(time.Time{}); err != nil {
		writeEventError(writer, http.StatusInternalServerError, "stream_writer_unsupported")
		return
	}
	fields := h.fields(principal)
	hub := h.stream.hub
	// Admission comes first: every connection counts against the reader
	// limits before it replays, follows or reads durable state.
	admission, err := hub.Admit(principal)
	var replay event.Replay
	if err == nil {
		defer admission.Release()
		replay, err = admission.Subscribe(runID, cursor, h.cfg.Authorize)
	}
	var snapshot *inspection.Page
	var notes []string
	if errors.Is(err, event.ErrNotFound) && h.cfg.Source != nil {
		page, pageNotes, sourceErr := h.snapshot(request.Context(), principal, runID, fields)
		if sourceErr == nil {
			snapshot, notes = &page, pageNotes
			if owners, ok := h.cfg.Source.(RunOwnerSource); ok {
				// Follow live only under the run's durable owner, so the
				// engine's trusted publications reach it and the reader is
				// authorized against the real owner.
				var owner string
				if owner, err = h.owner(request.Context(), owners, principal, runID); err != nil {
					err = event.ErrNotFound
				} else if err = hub.AttachRecovered(runID, owner, principal, terminalStatus(page.Run.Status)); err == nil {
					replay, err = admission.Subscribe(runID, cursor, h.cfg.Authorize)
				}
			} else {
				// The source cannot name the owner: serve the reconstruction
				// once, without following live.
				replay, err = event.Replay{Gap: &event.Gap{Reason: event.GapUnavailable}, Finished: true}, nil
			}
		}
	}
	switch {
	case err == nil:
	case errors.Is(err, event.ErrNotFound), errors.Is(err, event.ErrUnauthorized):
		writeEventError(writer, http.StatusNotFound, "not_found")
		return
	case errors.Is(err, event.ErrInvalidCursor):
		writeEventError(writer, http.StatusBadRequest, "invalid_cursor")
		return
	case errors.Is(err, event.ErrSaturated), errors.Is(err, event.ErrClosed):
		// Transient: an empty stream with a retry hint, so EventSource
		// tries again rather than giving up on an error status.
		reason := "saturated"
		if errors.Is(err, event.ErrClosed) {
			reason = "shutdown"
		}
		writeStreamHeader(writer)
		_, _ = io.WriteString(writer, "retry: "+strconv.FormatInt(h.cfg.Retry.Milliseconds(), 10)+"\n: "+reason+"\n\n")
		_ = control.Flush()
		return
	default:
		writeEventError(writer, http.StatusInternalServerError, "internal")
		return
	}
	sub := replay.Subscriber
	if sub != nil {
		defer hub.Unsubscribe(sub, "client_closed")
	}
	if replay.Gap != nil && snapshot == nil && h.cfg.Source != nil {
		if page, pageNotes, sourceErr := h.snapshot(request.Context(), principal, runID, fields); sourceErr == nil {
			snapshot, notes = &page, pageNotes
		}
	}
	session := &eventSession{writer: writer, control: control, timeout: h.cfg.WriteTimeout, seen: cursor}
	var planned []plannedFrame
	// sent is the last reconstruction this reader received, so a poll only
	// sends one that changed.
	var sent []byte
	if replay.Gap != nil {
		gap, _ := json.Marshal(replay.Gap)
		planned = append(planned, plannedFrame{id: replay.GapCursor, name: event.GapName, data: gap})
		if snapshot != nil {
			if data, ok := h.snapshotFrame(*snapshot, notes); ok {
				planned = append(planned, plannedFrame{id: replay.GapCursor, name: "snapshot", data: data})
				sent = data
			}
		}
		session.seen = replay.GapCursor
	}
	terminal := replay.Finished
	for _, frame := range replay.Frames {
		if name, data, ok := project(frame, fields); ok {
			planned = append(planned, plannedFrame{id: frame.Cursor(), name: name, data: data})
		} else {
			planned = append(planned, plannedFrame{id: frame.Cursor()})
		}
	}
	if sub == nil && countVisible(planned) == 0 {
		// Nothing more for this reader, and nothing more will come.
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	stopWatching := func() {}
	if sub != nil {
		// Disconnection by the hub (slow, shutdown, eviction) interrupts a
		// write in progress instead of waiting out its deadline.
		finished, exited := make(chan struct{}), make(chan struct{})
		go func() {
			defer close(exited)
			select {
			case <-sub.Done():
				_ = control.SetWriteDeadline(time.Now())
			case <-finished:
			}
		}()
		var once sync.Once
		stopWatching = func() { once.Do(func() { close(finished); <-exited }) }
		defer stopWatching()
	}
	writeStreamHeader(writer)
	if session.raw("retry: "+strconv.FormatInt(h.cfg.Retry.Milliseconds(), 10)+"\n\n") != nil {
		return
	}
	for _, frame := range planned {
		if session.frame(frame) != nil {
			return
		}
	}
	if session.flush() != nil {
		return
	}
	lateWindow := hub.Config().LateWindow
	end := func() {
		data, _ := json.Marshal(struct {
			LateWindowMs int64 `json:"lateWindowMs"`
		}{lateWindow.Milliseconds()})
		id := session.seen
		if id == "" {
			id = replay.LastCursor
		}
		_ = session.frame(plannedFrame{id: id, name: "end", data: data})
		_ = session.flush()
	}
	if sub == nil {
		end()
		return
	}
	// Late observations are worth waiting for only when this reader may
	// see them.
	followLate := fields[inspection.FieldLogs]
	var closeTimer <-chan time.Time
	if terminal {
		if !followLate {
			end()
			return
		}
		closeTimer = time.After(time.Until(replay.CloseAt))
	}
	lifetime := time.NewTimer(h.cfg.MaxDuration)
	defer lifetime.Stop()
	heartbeat := time.NewTicker(h.cfg.Heartbeat)
	defer heartbeat.Stop()
	// A recovered run has no publisher that would ever end it here, so its
	// follower watches the durable source at a bounded rate (#263). The
	// field selection is the connection's, computed once above.
	var poll <-chan time.Time
	if replay.Recovered && h.cfg.Source != nil {
		ticker := time.NewTicker(h.cfg.RecoveredPoll)
		defer ticker.Stop()
		poll = ticker.C
	}
	deliver := func(frame *event.Frame) error {
		if name, data, ok := project(frame, fields); ok {
			heartbeat.Reset(h.cfg.Heartbeat)
			return session.frame(plannedFrame{id: frame.Cursor(), name: name, data: data})
		}
		session.seen = frame.Cursor()
		return nil
	}
	for {
		select {
		case <-request.Context().Done():
			return
		case <-lifetime.C:
			return
		case <-heartbeat.C:
			if session.raw(": heartbeat\n\n") != nil || session.flush() != nil {
				return
			}
		case <-sub.Done():
			return
		case <-poll:
			if !hub.Recovered(sub) {
				// A trusted publisher claimed the run: it is live now and
				// ends with its own terminal frame.
				poll = nil
				continue
			}
			page, pageNotes, err := h.snapshot(request.Context(), principal, runID, fields)
			if err != nil {
				if h.refused(err) {
					// The source no longer lets this reader see the run:
					// close the connection now rather than at MaxDuration.
					// A reconnect gets the not-found answer.
					stopWatching()
					hub.Unsubscribe(sub, "revoked")
					return
				}
				// Unavailable for now; the next poll tries again, and
				// MaxDuration still bounds the subscription.
				continue
			}
			terminal := terminalStatus(page.Run.Status)
			// The durable run is over: close the attachment, so later
			// readers get the reconstruction and an end. If a publisher
			// claimed the run during the read, it is live and ends with its
			// own terminal frame instead.
			if terminal && !hub.FinishRecovered(sub) {
				poll = nil
				continue
			}
			if data, ok := h.snapshotFrame(page, pageNotes); ok && !bytes.Equal(data, sent) {
				if session.frame(plannedFrame{id: session.seen, name: "snapshot", data: data}) != nil || session.flush() != nil {
					return
				}
				sent = data
				heartbeat.Reset(h.cfg.Heartbeat)
			}
			if terminal {
				// Release this reader, and hand over anything a publisher
				// queued meanwhile before the end.
				stopWatching()
				hub.Unsubscribe(sub, "recovered_terminal")
			drainRecovered:
				for {
					select {
					case frame := <-sub.Events():
						if deliver(frame) != nil {
							return
						}
					default:
						break drainRecovered
					}
				}
				end()
				return
			}
		case <-closeTimer:
			// The late window has closed: stop following, then hand over
			// everything the hub queued before it did.
			// Stop the interrupt first: this unsubscription is ours, and the
			// end frame must still be written.
			stopWatching()
			hub.Unsubscribe(sub, "late_window_closed")
		drain:
			for {
				select {
				case frame := <-sub.Events():
					if deliver(frame) != nil {
						return
					}
				default:
					break drain
				}
			}
			end()
			return
		case frame := <-sub.Events():
			if deliver(frame) != nil || session.flush() != nil {
				return
			}
			if frame.Terminal() {
				if !followLate {
					end()
					return
				}
				closeTimer = time.After(time.Until(frame.FinishedAt().Add(lateWindow)))
			}
		}
	}
}

type plannedFrame struct {
	id, name string
	data     []byte
}

func countVisible(frames []plannedFrame) int {
	count := 0
	for _, frame := range frames {
		if frame.name != "" {
			count++
		}
	}
	return count
}

type eventSession struct {
	writer  http.ResponseWriter
	control *http.ResponseController
	timeout time.Duration
	// seen is the cursor of the last frame this reader has passed, written
	// or withheld by policy.
	seen string
}

func (s *eventSession) raw(text string) error {
	if err := s.control.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return err
	}
	_, err := io.WriteString(s.writer, text)
	return err
}

// frame writes one event. A frame without a name was withheld by policy; it
// only advances the reader's position.
func (s *eventSession) frame(frame plannedFrame) error {
	if frame.name == "" {
		s.seen = frame.id
		return nil
	}
	var builder strings.Builder
	builder.Grow(len(frame.id) + len(frame.name) + len(frame.data) + 24)
	if frame.id != "" {
		builder.WriteString("id: ")
		builder.WriteString(frame.id)
		builder.WriteByte('\n')
	}
	builder.WriteString("event: ")
	builder.WriteString(frame.name)
	builder.WriteString("\ndata: ")
	builder.Write(frame.data)
	builder.WriteString("\n\n")
	if err := s.raw(builder.String()); err != nil {
		return err
	}
	if frame.id != "" {
		s.seen = frame.id
	}
	return nil
}

func (s *eventSession) flush() error {
	if err := s.control.SetWriteDeadline(time.Now().Add(s.timeout)); err != nil {
		return err
	}
	return s.control.Flush()
}

// RunOwnerSource is an inspection.Source that can also name the durable
// owner of a run it lets reader see. With it, a run this process has no live
// history of is followed live under its real owner; without it, the reader
// gets the reconstruction only.
type RunOwnerSource interface {
	inspection.Source
	RunOwner(ctx context.Context, reader, runID string) (string, error)
}

func (h *eventHandler) owner(parent context.Context, source RunOwnerSource, principal, runID string) (string, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.SourceTimeout)
	defer cancel()
	owner, err := source.RunOwner(ctx, principal, runID)
	if err == nil && (owner == "" || len(owner) > event.MaxIdentityBytes) {
		err = event.ErrNotFound
	}
	return owner, err
}

type recoveredSnapshot struct {
	Source        string          `json:"source"`
	Reconstructed bool            `json:"reconstructed"`
	Page          inspection.Page `json:"page"`
	Unavailable   []string        `json:"unavailable"`
}

// snapshotFrame encodes a reconstruction with what it could not include, or
// reports that it does not fit SnapshotBytes.
func (h *eventHandler) snapshotFrame(page inspection.Page, notes []string) ([]byte, bool) {
	unavailable := append([]string{"transient transitions", "logs", "payloads the journal does not keep"}, notes...)
	data, err := json.Marshal(recoveredSnapshot{Source: "journal", Reconstructed: true, Page: page, Unavailable: unavailable})
	return data, err == nil && len(data) <= h.cfg.SnapshotBytes
}

// refused reports a failed source read that a RefusingSource classifies as
// the reader no longer being allowed to see the run.
func (h *eventHandler) refused(err error) bool {
	refusing, ok := h.cfg.Source.(RefusingSource)
	var failed *sourceReadError
	return ok && errors.As(err, &failed) && refusing.Refused(failed.cause)
}

func (h *eventHandler) snapshot(parent context.Context, principal, runID string, fields map[inspection.Field]bool) (inspection.Page, []string, error) {
	ctx, cancel := context.WithTimeout(parent, h.cfg.SourceTimeout)
	defer cancel()
	policy := inspection.Policy{Fields: fields, MaxPageSize: defaultEventSnapshotPage, MaxResponseBytes: h.cfg.SnapshotBytes - 512}
	return inspectSource(ctx, h.cfg.Source, principal, policy, inspection.Query{Version: inspection.Version, RunID: runID, Limit: defaultEventSnapshotPage})
}

func terminalStatus(status inspection.Status) bool {
	switch status {
	case inspection.StatusCompleted, inspection.StatusFailed, inspection.StatusCanceled, inspection.StatusUncertain:
		return true
	}
	return false
}

// project applies the reader's field selection to one frame. Gap markers
// pass unchanged. A log the reader may not see is withheld.
func project(frame *event.Frame, fields map[inspection.Field]bool) (string, []byte, bool) {
	if frame.Gap() {
		return frame.Name(), frame.Data(), true
	}
	if frame.Name() == string(inspection.StepLog) && !fields[inspection.FieldLogs] {
		return "", nil, false
	}
	if fields[inspection.FieldInput] && fields[inspection.FieldOutput] && fields[inspection.FieldError] {
		return frame.Name(), frame.Data(), true
	}
	var value inspection.Event
	if err := json.Unmarshal(frame.Data(), &value); err != nil {
		return "", nil, false
	}
	if !fields[inspection.FieldInput] {
		value.Input = nil
	}
	if !fields[inspection.FieldOutput] {
		value.Output = nil
	}
	if !fields[inspection.FieldError] {
		value.ErrorCode, value.ErrorClass = "", ""
	}
	data, err := json.Marshal(value)
	if err != nil {
		return "", nil, false
	}
	return frame.Name(), data, true
}

// parseEventPath accepts .../runs/{run}/events with a run ID of the
// characters run IDs use.
func parseEventPath(path string) (string, bool) {
	const suffix = "/events"
	if !strings.HasSuffix(path, suffix) {
		return "", false
	}
	rest := strings.TrimSuffix(path, suffix)
	index := strings.LastIndex(rest, "/runs/")
	if index < 0 {
		return "", false
	}
	runID := rest[index+len("/runs/"):]
	if runID == "" || len(runID) > event.MaxIdentityBytes {
		return "", false
	}
	for _, char := range runID {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || strings.ContainsRune("_.:-", char)) {
			return "", false
		}
	}
	return runID, true
}

func writeStreamHeader(writer http.ResponseWriter) {
	header := writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache, no-transform")
	header.Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
}

func writeEventError(writer http.ResponseWriter, status int, code string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("Cache-Control", "no-store")
	writer.WriteHeader(status)
	_, _ = io.WriteString(writer, `{"error":"`+code+`"}`+"\n")
}
