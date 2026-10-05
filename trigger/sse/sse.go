// Package sse starts durable work over HTTP and streams its progress as
// Server-Sent Events.
//
// A start request (POST) is authenticated, validated and submitted through
// trigger.Submitter under the caller's idempotency key; it is acknowledged
// only once the submission is committed, and answers with the stream that
// will carry the work's progress. A subscription (GET, the EventSource
// protocol) is authenticated and authorized for that stream before any byte
// of it is written, then replays the stream from its Last-Event-ID and
// follows it live. Progress lives in a bounded in-memory Hub: it is not a
// durable signal. When a subscriber cannot be given every event after its
// cursor (retention dropped them, or the hub restarted) it receives an
// explicit gap event first. A subscriber that falls behind is disconnected
// rather than buffered, and closing a subscription never cancels the work.
// Audit records take a separate, synchronous path that never drops.
package sse

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the SSE contract: a start is acknowledged once its work is
// committed durably, a subscriber that goes away stops waiting without
// cancelling that work, and every caller is authenticated.
var Declaration = trigger.Declaration{Kind: trigger.SSE, Adapter: "trigger/sse", Completion: trigger.Durable, Disconnect: trigger.StopWaiting, Authentication: trigger.Caller}

const (
	DefaultMaxBodyBytes      = 1 << 20
	MaxBodyBytesLimit        = 16 << 20
	DefaultReadTimeout       = 10 * time.Second
	MaxReadTimeout           = time.Minute
	DefaultSubmitTimeout     = 10 * time.Second
	DefaultHeartbeat         = 15 * time.Second
	DefaultWriteTimeout      = 10 * time.Second
	DefaultRetry             = 3 * time.Second
	MinRetry                 = 100 * time.Millisecond
	MaxRetry                 = 10 * time.Minute
	DefaultMaxDuration       = 30 * time.Minute
	DefaultQueueDepth        = 64
	MaxQueueDepth            = 1024
	DefaultMaxSubscribers    = 1024
	MaxSubscribersLimit      = 1 << 16
	DefaultStreamSubscribers = 8
	// MaxQueueBytes bounds QueueDepth × the hub's MaxEventBytes, the event
	// bytes one subscriber may hold beyond its stream's retention. An
	// endpoint holds at most MaxSubscribers times that.
	MaxQueueBytes = 16 << 20
	// replayChunk is how many replay bytes are written before a flush, and
	// the largest write buffer a subscription keeps between writes.
	replayChunk = 64 << 10
)

// Reasons a subscription ended, reported to Endpoint.OnClose.
const (
	ReasonClientClosed = "client_closed"
	ReasonFinished     = "finished"
	ReasonSlow         = "slow_subscriber"
	ReasonWriteTimeout = "write_timeout"
	ReasonMaxDuration  = "max_duration"
	ReasonShutdown     = "shutdown"
	// ReasonNoWork: the start the subscriber was waiting on failed, so no
	// work stands behind the stream.
	ReasonNoWork = "no_work"
)

// Endpoint binds a start route and its subscription routes: POST Path starts
// work, GET Path/{stream} subscribes to its progress.
type Endpoint struct {
	// Name scopes submission keys; it is part of every stream identity.
	Name string
	Path string
	// Kind is the submission kind the work is queued under.
	Kind   string
	Submit trigger.Submitter
	// Tracker reports whether the work submitted under a key has settled.
	// A repeated start whose stream is no longer retained uses it to end
	// that stream as expired instead of leaving it open forever.
	Tracker Tracker
	// Authenticate establishes the caller of a start or a subscription.
	Authenticate func(*http.Request) (trigger.Principal, error)
	// Authorize decides whether reader may follow a stream started by
	// owner. The default allows only the same principal.
	Authorize   func(reader, owner trigger.Principal) error
	InputSchema []byte
	// OnClose, when set, is told why each subscription ended.
	OnClose func(stream, reason string)

	MaxBodyBytes  int64
	ReadTimeout   time.Duration
	SubmitTimeout time.Duration
	// Heartbeat is how long a subscription may stay silent before a comment
	// keeps it alive.
	Heartbeat time.Duration
	// WriteTimeout bounds every write to a subscriber.
	WriteTimeout time.Duration
	// Retry is the reconnection delay sent to EventSource clients.
	Retry time.Duration
	// MaxDuration ends a subscription; the client reconnects with its
	// cursor.
	MaxDuration time.Duration
	// QueueDepth bounds the events waiting for one subscriber; a
	// subscriber whose queue is full is disconnected.
	QueueDepth int
	// MaxSubscribers bounds this endpoint's open subscriptions, and
	// StreamSubscribers those of one stream.
	MaxSubscribers    int
	StreamSubscribers int
}

// Tracker reports whether the work submitted under a key has settled
// (finished or failed for good). worker.Queue implements it.
type Tracker interface {
	Settled(ctx context.Context, key string) (bool, error)
}

type endpoint struct {
	Endpoint
	input schema.Schema
}

type Server struct {
	application *app.Application
	hub         *Hub
	endpoints   map[string]*endpoint
	mu          sync.Mutex
	closing     bool
	open        map[*endpoint]int
	subs        map[*subscriber]string
	group       sync.WaitGroup
}

var (
	endpointName  = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,63}$`)
	idempotency   = regexp.MustCompile(`^[\x21-\x7e]{1,256}$`)
	streamPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)
)

// New validates the endpoints. It opens no listener and starts no
// goroutine.
func New(application *app.Application, hub *Hub, endpoints []Endpoint) (*Server, error) {
	if application == nil || hub == nil || len(endpoints) == 0 {
		return nil, errors.New("sse: an application, a hub and at least one endpoint are required")
	}
	s := &Server{application: application, hub: hub, endpoints: map[string]*endpoint{}, open: map[*endpoint]int{}, subs: map[*subscriber]string{}}
	names := map[string]bool{}
	for _, e := range endpoints {
		if !endpointName.MatchString(e.Name) || !strings.HasPrefix(e.Path, "/") || strings.HasSuffix(e.Path, "/") || e.Kind == "" || e.Submit == nil || e.Tracker == nil || e.Authenticate == nil {
			return nil, fmt.Errorf("sse: endpoint %q needs a name, path, kind, submitter, tracker and authenticator", e.Name)
		}
		if names[e.Name] || s.endpoints[e.Path] != nil {
			return nil, fmt.Errorf("sse: endpoint %q reuses a name or path", e.Name)
		}
		names[e.Name] = true
		parsed, err := schema.Parse(e.InputSchema)
		if err != nil {
			return nil, fmt.Errorf("sse: endpoint %s schema: %w", e.Name, err)
		}
		if e.Authorize == nil {
			e.Authorize = SamePrincipal
		}
		for _, d := range []struct {
			value    *time.Duration
			fallback time.Duration
		}{{&e.ReadTimeout, DefaultReadTimeout}, {&e.SubmitTimeout, DefaultSubmitTimeout}, {&e.Heartbeat, DefaultHeartbeat}, {&e.WriteTimeout, DefaultWriteTimeout}, {&e.Retry, DefaultRetry}, {&e.MaxDuration, DefaultMaxDuration}} {
			if *d.value <= 0 {
				*d.value = d.fallback
			}
		}
		if e.MaxBodyBytes <= 0 {
			e.MaxBodyBytes = DefaultMaxBodyBytes
		}
		if e.QueueDepth <= 0 {
			e.QueueDepth = DefaultQueueDepth
		}
		if e.MaxSubscribers <= 0 {
			e.MaxSubscribers = DefaultMaxSubscribers
		}
		if e.StreamSubscribers <= 0 {
			e.StreamSubscribers = DefaultStreamSubscribers
		}
		if e.MaxBodyBytes > MaxBodyBytesLimit || e.ReadTimeout > MaxReadTimeout || e.QueueDepth > MaxQueueDepth || e.MaxSubscribers > MaxSubscribersLimit || e.StreamSubscribers > e.MaxSubscribers || e.Retry < MinRetry || e.Retry > MaxRetry {
			return nil, fmt.Errorf("sse: endpoint %s exceeds the body, timeout, queue, subscriber or retry bound", e.Name)
		}
		if int64(e.QueueDepth)*int64(hub.config.MaxEventBytes) > MaxQueueBytes {
			return nil, fmt.Errorf("sse: endpoint %s QueueDepth × MaxEventBytes exceeds %d bytes per subscriber", e.Name, MaxQueueBytes)
		}
		s.endpoints[e.Path] = &endpoint{Endpoint: e, input: parsed}
	}
	return s, nil
}

// SamePrincipal is the default authorization: only the principal that
// started a stream may follow it.
func SamePrincipal(reader, owner trigger.Principal) error {
	if reader.ID == "" || reader.ID != owner.ID {
		return errForbidden
	}
	return nil
}

// SubmissionKey is the durable identity of a start: the endpoint, the
// caller and the caller's idempotency key. Including the caller keeps one
// principal's keys out of another's.
func SubmissionKey(endpoint string, principal trigger.Principal, key string) string {
	digest := sha256.Sum256([]byte(principal.ID))
	return "sse:" + endpoint + ":" + hex.EncodeToString(digest[:]) + ":" + key
}

// StreamID is the stream that carries the progress of the work submitted
// under a submission key. The worker that runs the work publishes to it.
func StreamID(submissionKey string) string {
	digest := sha256.Sum256([]byte(submissionKey))
	return hex.EncodeToString(digest[:16])
}

// Result is the final event of work that completed.
func Result(output json.RawMessage) Event { return Event{Type: "result", Data: output} }

// Failure is the final event of work that failed, of type "failed" (not
// "error", which EventSource reserves for connection errors). Only a stable
// code is published: a classified error's code, "saturated", or
// "internal".
func Failure(err error) Event {
	code := "internal"
	if errors.Is(err, trigger.ErrSaturated) {
		code = "saturated"
	} else if classified, _, ok := trigger.Classify(err); ok {
		code = classified
	}
	data, _ := json.Marshal(map[string]string{"code": code})
	return Event{Type: "failed", Data: data}
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if e, ok := s.endpoints[request.URL.Path]; ok {
		if request.Method != http.MethodPost {
			writer.Header().Set("Allow", http.MethodPost)
			respond(writer, http.StatusMethodNotAllowed, "error", "method_not_allowed")
			return
		}
		s.start(writer, request, e)
		return
	}
	cut := strings.LastIndexByte(request.URL.Path, '/')
	if e, ok := s.endpoints[request.URL.Path[:max(cut, 0)]]; ok && cut > 0 {
		if request.Method != http.MethodGet {
			writer.Header().Set("Allow", http.MethodGet)
			respond(writer, http.StatusMethodNotAllowed, "error", "method_not_allowed")
			return
		}
		s.subscribe(writer, request, e, request.URL.Path[cut+1:])
		return
	}
	respond(writer, http.StatusNotFound, "error", "not_found")
}

// admit takes an application lease for a start: new work is refused while
// the application is not ready or is draining, and the application cannot
// stop until the start, its durable submission included, has answered.
func (s *Server) admit(writer http.ResponseWriter) (*app.Lease, bool) {
	lease, err := s.application.Begin()
	if err != nil {
		writer.Header().Set("Retry-After", "1")
		respond(writer, http.StatusServiceUnavailable, "error", "unavailable")
		return nil, false
	}
	return lease, true
}

// start runs a start request's steps in a fixed order: admission,
// authentication, idempotency key, bounded body read, input validation, a
// stream slot in the hub, and the durable submission.
func (s *Server) start(writer http.ResponseWriter, request *http.Request, e *endpoint) {
	lease, admitted := s.admit(writer)
	if !admitted {
		return
	}
	defer lease.Release()
	// Authentication and the body read also stop if the application's
	// drain times out.
	original := request.Context()
	bound, unbindRequest := lease.Bind(original)
	defer unbindRequest()
	request = request.WithContext(bound)
	principal, err := e.Authenticate(request)
	if err != nil || strings.TrimSpace(principal.ID) == "" {
		respond(writer, http.StatusUnauthorized, "error", "unauthorized")
		return
	}
	key := request.Header.Get("Idempotency-Key")
	if !idempotency.MatchString(key) {
		respond(writer, http.StatusBadRequest, "error", "invalid_key")
		return
	}
	_ = http.NewResponseController(writer).SetReadDeadline(time.Now().Add(e.ReadTimeout))
	body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, e.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			respond(writer, http.StatusRequestEntityTooLarge, "error", "too_large")
		} else {
			respond(writer, http.StatusRequestTimeout, "error", "read_timeout")
		}
		return
	}
	if len(bytes.TrimSpace(body)) == 0 {
		body = []byte("null")
	}
	payload, err := e.input.Normalize(body)
	if err != nil {
		respond(writer, http.StatusBadRequest, "error", "invalid_input")
		return
	}
	submissionKey := SubmissionKey(e.Name, principal, key)
	streamID := StreamID(submissionKey)
	_, err = s.hub.open(streamID, principal, submissionKey)
	switch {
	case errors.Is(err, ErrHubFull), errors.Is(err, errOwnerQuota):
		writer.Header().Set("Retry-After", "1")
		respond(writer, http.StatusServiceUnavailable, "error", "saturated")
		return
	case err != nil:
		respond(writer, http.StatusInternalServerError, "error", "internal")
		return
	}
	// The submission runs under a context the caller cannot cancel: once
	// started, its outcome is decided by the store, not by the connection.
	submitting, unbind := lease.Bind(context.WithoutCancel(original))
	defer unbind()
	ctx, cancel := context.WithTimeout(submitting, e.SubmitTimeout)
	defer cancel()
	accepted, err := e.Submit.Submit(ctx, trigger.Submission{Key: submissionKey, Kind: e.Kind, Payload: payload, Principal: principal})
	committed := err == nil || errors.Is(err, trigger.ErrConflict)
	switch {
	case err == nil && accepted:
		s.hub.verify(streamID)
	case committed:
		// A repeat of committed work (a duplicate or a conflict). If its
		// stream is unverified (recreated after eviction or a restart),
		// the tracker decides whether it can still end.
		if checkErr := s.check(ctx, e, streamID, true); checkErr != nil {
			s.hub.settle(streamID, true)
			writer.Header().Set("Retry-After", "1")
			respond(writer, http.StatusServiceUnavailable, "error", "unavailable")
			return
		}
	}
	s.hub.settle(streamID, committed)
	switch {
	case err == nil && accepted:
		writeJSON(writer, http.StatusAccepted, map[string]any{"stream": streamID})
		return
	case err == nil:
		writeJSON(writer, http.StatusOK, map[string]any{"stream": streamID, "duplicate": true})
		return
	}
	switch {
	case errors.Is(err, trigger.ErrConflict):
		respond(writer, http.StatusConflict, "error", "conflict")
	case errors.Is(err, trigger.ErrInvalidInput):
		respond(writer, http.StatusBadRequest, "error", "invalid_input")
	case app.Aborted(ctx):
		// The drain timed out under the submission: a retry with the same
		// key is deduplicated if it did commit.
		writer.Header().Set("Retry-After", "1")
		respond(writer, http.StatusServiceUnavailable, "error", "unavailable")
	case errors.Is(err, trigger.ErrSaturated):
		writer.Header().Set("Retry-After", "1")
		respond(writer, http.StatusServiceUnavailable, "error", "saturated")
	default:
		respond(writer, http.StatusInternalServerError, "error", "internal")
	}
}

// subscribe authenticates and authorizes the reader, resolves its cursor
// and registers it before writing the response status, so a refused reader
// receives no byte of the stream. A permanent refusal is a JSON error
// (EventSource then stops); a transient one (limits, draining) is an empty
// stream carrying only the retry hint, so EventSource tries again.
func (s *Server) subscribe(writer http.ResponseWriter, request *http.Request, e *endpoint, streamID string) {
	lease, err := s.application.Begin()
	if err != nil {
		retryLater(writer, e, "unavailable")
		return
	}
	// The lease covers everything decided before the status, the tracker's
	// store read included, and is released before following the stream: a
	// subscription is long-lived and does not hold the application open.
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(lease.Release) }
	defer release()
	// Authentication also stops if the application's drain times out.
	principal, err := func() (trigger.Principal, error) {
		authenticating, unbind := lease.Bind(request.Context())
		defer unbind()
		return e.Authenticate(request.WithContext(authenticating))
	}()
	if err != nil || strings.TrimSpace(principal.ID) == "" {
		respond(writer, http.StatusUnauthorized, "error", "unauthorized")
		return
	}
	if !streamPattern.MatchString(streamID) {
		respond(writer, http.StatusNotFound, "error", "not_found")
		return
	}
	// Writes must be bounded and interruptible. A writer that cannot take
	// a deadline is refused rather than allowed to block without bound.
	control := http.NewResponseController(writer)
	if err := control.SetWriteDeadline(time.Now().Add(e.WriteTimeout)); err != nil {
		respond(writer, http.StatusInternalServerError, "error", "internal")
		return
	}
	if refused := s.reserve(e); refused != "" {
		retryLater(writer, e, refused)
		return
	}
	defer s.unreserve(e)
	interrupt := func() { _ = control.SetWriteDeadline(time.Now()) }
	cursor := request.Header.Get("Last-Event-ID")
	result, err := s.hub.subscribe(streamID, principal, cursor, e.Authorize, e.QueueDepth, e.StreamSubscribers, interrupt)
	if errors.Is(err, errUnverified) {
		// Nothing has shown that this stream will ever end: ask the
		// tracker before following it. The read stops, too, if the
		// application's drain times out.
		checkErr := func() error {
			bound, unbind := lease.Bind(request.Context())
			defer unbind()
			ctx, cancel := context.WithTimeout(bound, e.SubmitTimeout)
			defer cancel()
			return s.check(ctx, e, streamID, false)
		}()
		if checkErr != nil {
			retryLater(writer, e, "unavailable")
			return
		}
		result, err = s.hub.subscribe(streamID, principal, cursor, e.Authorize, e.QueueDepth, e.StreamSubscribers, interrupt)
	}
	switch {
	case errors.Is(err, errInvalidCursor):
		respond(writer, http.StatusBadRequest, "error", "invalid_cursor")
		return
	case errors.Is(err, errUnknownStream):
		respond(writer, http.StatusNotFound, "error", "not_found")
		return
	case errors.Is(err, errUnverified):
		retryLater(writer, e, "unavailable")
		return
	case errors.Is(err, errBusy):
		retryLater(writer, e, "saturated")
		return
	case err != nil:
		respond(writer, http.StatusInternalServerError, "error", "internal")
		return
	}
	// follow owns the replay; nothing here keeps it reachable while the
	// subscription lasts.
	sub := result.sub
	release()
	reason := s.follow(writer, control, request, e, streamID, result)
	if sub != nil {
		s.hub.unsubscribe(streamID, sub, reason)
		s.mu.Lock()
		delete(s.subs, sub)
		s.mu.Unlock()
	}
	if e.OnClose != nil {
		e.OnClose(streamID, reason)
	}
}

// check asks the tracker about an unverified stream: settled work ends the
// stream as expired, running work verifies it.
//
// committed is set by a start whose own key is known to be committed.
func (s *Server) check(ctx context.Context, e *endpoint, streamID string, committed bool) error {
	key, unverified := s.hub.unverified(streamID, committed)
	if !unverified {
		return nil
	}
	settled, err := e.Tracker.Settled(ctx, key)
	if err != nil {
		return err
	}
	if settled {
		s.hub.expire(streamID)
	} else {
		s.hub.verify(streamID)
	}
	return nil
}

// retryLater answers a transient refusal as an empty event stream: the
// retry hint and a comment naming the reason. EventSource reconnects after
// the hint, where an error status would make it give up.
func retryLater(writer http.ResponseWriter, e *endpoint, reason string) {
	header := writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(writer, "retry: "+strconv.FormatInt(e.Retry.Milliseconds(), 10)+"\n\n: "+reason+"\n\n")
}

// reserve takes a subscription slot, or names why there is none.
func (s *Server) reserve(e *endpoint) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.closing:
		return "unavailable"
	case s.open[e] >= e.MaxSubscribers:
		return "saturated"
	}
	s.open[e]++
	s.group.Add(1)
	return ""
}

func (s *Server) unreserve(e *endpoint) {
	s.mu.Lock()
	s.open[e]--
	s.mu.Unlock()
	s.group.Done()
}

// follow writes the stream: the retry hint, a gap if any, the replay, then
// live events and heartbeats until the stream ends or the subscription is
// closed. It returns why the subscription ended.
func (s *Server) follow(writer http.ResponseWriter, control *http.ResponseController, request *http.Request, e *endpoint, streamID string, result subscription) string {
	if result.sub != nil {
		s.mu.Lock()
		if s.closing {
			s.mu.Unlock()
			retryLater(writer, e, "unavailable")
			return ReasonShutdown
		}
		s.subs[result.sub] = streamID
		s.mu.Unlock()
	}
	if result.done && result.gap == nil && len(result.replay) == 0 {
		// Nothing follows the cursor of a finished stream. 204 tells an
		// EventSource client to stop reconnecting.
		writer.WriteHeader(http.StatusNoContent)
		return ReasonFinished
	}
	header := writer.Header()
	header.Set("Content-Type", "text/event-stream")
	header.Set("Cache-Control", "no-cache")
	header.Set("X-Accel-Buffering", "no")
	writer.WriteHeader(http.StatusOK)
	var frame bytes.Buffer
	var writeErr error
	write := func() bool {
		if err := control.SetWriteDeadline(time.Now().Add(e.WriteTimeout)); err != nil && !errors.Is(err, http.ErrNotSupported) {
			return false
		}
		// Closed after its deadline was set: an interrupt may have been
		// overwritten, so stop here instead of writing.
		if result.sub != nil {
			select {
			case <-result.sub.done:
				return false
			default:
			}
		}
		_, writeErr = writer.Write(frame.Bytes())
		frame.Reset()
		if frame.Cap() > replayChunk {
			frame = bytes.Buffer{}
		}
		if writeErr == nil {
			writeErr = control.Flush()
		}
		return writeErr == nil
	}
	frame.WriteString("retry: " + strconv.FormatInt(e.Retry.Milliseconds(), 10) + "\n\n")
	if result.gap != nil {
		data, _ := json.Marshal(result.gap)
		writeEvent(&frame, result.gapID, TypeGap, data)
	}
	// failed names why a write failed: the subscription was closed (slow,
	// shutdown), the write ran past its deadline, or the client went away.
	failed := func() string {
		if result.sub != nil {
			select {
			case <-result.sub.done:
				return result.sub.reason
			default:
			}
		}
		if errors.Is(writeErr, os.ErrDeadlineExceeded) {
			return ReasonWriteTimeout
		}
		return ReasonClientClosed
	}
	// The replay is written in chunks, so a subscription never holds a
	// copy of its whole replay.
	for index, event := range result.replay {
		writeEvent(&frame, result.ids[index], event.typ, event.data)
		if event.final {
			if !write() {
				return failed()
			}
			return ReasonFinished
		}
		if frame.Len() >= replayChunk && !write() {
			return failed()
		}
	}
	result.replay, result.ids = nil, nil
	if !write() {
		return failed()
	}
	if result.sub == nil {
		return ReasonFinished
	}
	heartbeat := time.NewTimer(e.Heartbeat)
	defer heartbeat.Stop()
	deadline := time.NewTimer(e.MaxDuration)
	defer deadline.Stop()
	for {
		select {
		case event := <-result.sub.events:
			writeEvent(&frame, s.hub.id(result.incarnation, event.seq), event.typ, event.data)
			if !write() {
				return failed()
			}
			if event.final {
				return ReasonFinished
			}
			heartbeat.Reset(e.Heartbeat)
		case <-heartbeat.C:
			frame.WriteString(": heartbeat\n\n")
			if !write() {
				return failed()
			}
			heartbeat.Reset(e.Heartbeat)
		case <-result.sub.done:
			return result.sub.reason
		case <-deadline.C:
			return ReasonMaxDuration
		case <-request.Context().Done():
			return ReasonClientClosed
		}
	}
}

// writeEvent frames one event: its id, its type and its data, one data
// line per line of data, ended by a blank line.
func writeEvent(frame *bytes.Buffer, id, typ string, data []byte) {
	frame.WriteString("id: " + id + "\nevent: " + typ + "\n")
	for _, line := range bytes.Split(data, []byte("\n")) {
		frame.WriteString("data: ")
		frame.Write(bytes.TrimSuffix(line, []byte("\r")))
		frame.WriteByte('\n')
	}
	frame.WriteByte('\n')
}

// Shutdown ends every subscription and refuses new ones, then waits for the
// subscription handlers to return. It does not touch submitted work.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	subs := make([]*subscriber, 0, len(s.subs))
	for sub := range s.subs {
		subs = append(subs, sub)
	}
	s.mu.Unlock()
	for _, sub := range subs {
		sub.close(ReasonShutdown)
	}
	done := make(chan struct{})
	go func() { s.group.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func respond(writer http.ResponseWriter, status int, field, value string) {
	writeJSON(writer, status, map[string]string{field: value})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
