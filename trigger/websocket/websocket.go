// Package websocket binds WebSocket connections and messages to workflows.
//
// The upgrade request is authenticated, origin-checked and admitted before
// the protocol switches; the server then assigns the connection identity, and
// no message field can change it. Each connection reads frames into a bounded
// queue and runs one message workflow at a time, in arrival order. The
// policies for every slow or misbehaving peer are explicit: an inbound flood
// that overflows the queue, a reply that cannot be written within the write
// timeout, or a missed pong closes the connection; an oversized frame closes
// it with 1009 and a binary frame with 1003; a malformed text frame gets an
// error reply and the connection continues. A disconnect, whatever its cause,
// cancels the in-flight message and is reported to the disconnect workflow
// exactly once.
package websocket

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the WebSocket contract: replies carry the completed result
// in band, a lost connection cancels its in-flight work, and every connection
// is authenticated at the upgrade.
var Declaration = trigger.Declaration{Kind: trigger.WebSocket, Adapter: "trigger/websocket", Completion: trigger.Memory, Disconnect: trigger.CancelWork, Authentication: trigger.Caller}

const (
	DefaultMaxMessageBytes = 64 << 10
	MaxMessageBytesLimit   = 1 << 20
	DefaultMaxConnections  = 1024
	MaxConnectionsLimit    = 1 << 16
	DefaultQueueDepth      = 16
	MaxQueueDepth          = 1024
	DefaultWriteTimeout    = 5 * time.Second
	DefaultPingInterval    = 30 * time.Second
	DefaultPongTimeout     = 10 * time.Second
	DefaultMessageTimeout  = 30 * time.Second
	DefaultMaxReplyBytes   = 1 << 20
	MaxReplyBytesLimit     = 16 << 20
	// MaxQueueBytes bounds QueueDepth × MaxMessageBytes, the frames one
	// connection may hold waiting. With MaxConnections it bounds the whole
	// endpoint: MaxConnections × (MaxQueueBytes + 2 × MaxMessageBytes).
	MaxQueueBytes = 16 << 20
)

// Disconnect reasons reported to OnDisconnect.
const (
	ReasonClientClosed    = "client_closed"
	ReasonBackpressure    = "backpressure"
	ReasonSlowReader      = "slow_reader"
	ReasonPingTimeout     = "ping_timeout"
	ReasonTooLarge        = "too_large"
	ReasonUnsupportedData = "unsupported_data"
	ReasonRejected        = "rejected"
	ReasonShutdown        = "shutdown"
)

// Connection is what the connection workflow sees: the identity the server
// assigned and the principal authenticated at the upgrade.
type Connection struct {
	ID        string
	Principal trigger.Principal
}

// Message is one validated message on a connection.
type Message struct {
	Connection Connection
	RequestID  string
	Input      json.RawMessage
}

// Disconnected is reported once per connection.
type Disconnected struct {
	Connection Connection
	Reason     string
}

// Endpoint binds one route to the connection, message and disconnect
// workflows.
type Endpoint struct {
	Path string
	// Origins lists allowed browser origin host patterns. Empty allows only
	// same-host requests.
	Origins      []string
	Authenticate func(*http.Request) (trigger.Principal, error)
	// OnConnect may refuse a connection; the client is then closed with
	// 1008 and the error's stable code as the reason.
	OnConnect func(context.Context, Connection) error
	// OnMessage handles one message; its output is the reply.
	OnMessage func(context.Context, Message) (json.RawMessage, error)
	// OnDisconnect runs exactly once per accepted connection.
	OnDisconnect    func(context.Context, Disconnected)
	InputSchema     []byte
	MaxMessageBytes int64
	// MaxReplyBytes bounds one reply; a larger output is answered with
	// reply_too_large instead.
	MaxReplyBytes  int
	MaxConnections int
	QueueDepth     int
	WriteTimeout   time.Duration
	PingInterval   time.Duration
	PongTimeout    time.Duration
	MessageTimeout time.Duration
}

type Server struct {
	application *app.Application
	endpoint    Endpoint
	input       schema.Schema
	mu          sync.Mutex
	closing     bool
	conns       map[string]*conn
	group       sync.WaitGroup
	// beforeAttach, when set, runs between the upgrade and attaching the
	// socket. Tests use it to hold a connection in that window.
	beforeAttach func(*conn)
	// beforeWrite, when set, runs immediately before a reply is written.
	beforeWrite func()
}

// New validates the endpoint. It opens no listener and starts no goroutine.
func New(application *app.Application, e Endpoint) (*Server, error) {
	if application == nil || !strings.HasPrefix(e.Path, "/") || e.Authenticate == nil || e.OnMessage == nil {
		return nil, errors.New("websocket: endpoint needs an application, path, authenticator and message handler")
	}
	parsed, err := schema.Parse(e.InputSchema)
	if err != nil {
		return nil, fmt.Errorf("websocket: schema: %w", err)
	}
	defaults := []struct {
		value    *time.Duration
		fallback time.Duration
	}{{&e.WriteTimeout, DefaultWriteTimeout}, {&e.PingInterval, DefaultPingInterval}, {&e.PongTimeout, DefaultPongTimeout}, {&e.MessageTimeout, DefaultMessageTimeout}}
	for _, d := range defaults {
		if *d.value <= 0 {
			*d.value = d.fallback
		}
	}
	if e.MaxMessageBytes <= 0 {
		e.MaxMessageBytes = DefaultMaxMessageBytes
	}
	if e.MaxConnections <= 0 {
		e.MaxConnections = DefaultMaxConnections
	}
	if e.QueueDepth <= 0 {
		e.QueueDepth = DefaultQueueDepth
	}
	if e.MaxReplyBytes <= 0 {
		e.MaxReplyBytes = DefaultMaxReplyBytes
	}
	if e.MaxMessageBytes > MaxMessageBytesLimit || e.MaxConnections > MaxConnectionsLimit || e.QueueDepth > MaxQueueDepth || e.MaxReplyBytes > MaxReplyBytesLimit {
		return nil, errors.New("websocket: endpoint exceeds the message, connection, queue or reply bound")
	}
	if int64(e.QueueDepth)*e.MaxMessageBytes > MaxQueueBytes {
		return nil, fmt.Errorf("websocket: QueueDepth × MaxMessageBytes exceeds %d bytes per connection", MaxQueueBytes)
	}
	for _, pattern := range e.Origins {
		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("websocket: origin pattern %q: %w", pattern, err)
		}
	}
	return &Server{application: application, endpoint: e, input: parsed, conns: map[string]*conn{}}, nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != s.endpoint.Path {
		http.Error(writer, `{"error":"not_found"}`, http.StatusNotFound)
		return
	}
	lease, err := s.application.Begin()
	if err != nil {
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, `{"error":"unavailable"}`, http.StatusServiceUnavailable)
		return
	}
	lease.Release()
	// Cheap checks first: a foreign origin and a full endpoint are refused
	// before the authenticator runs, so neither can drive it without bound.
	if !originAllowed(request, s.endpoint.Origins) {
		http.Error(writer, `{"error":"forbidden_origin"}`, http.StatusForbidden)
		return
	}
	c, ok := s.reserve()
	if !ok {
		writer.Header().Set("Retry-After", "1")
		http.Error(writer, `{"error":"saturated"}`, http.StatusServiceUnavailable)
		return
	}
	principal, err := s.endpoint.Authenticate(request)
	if err != nil || strings.TrimSpace(principal.ID) == "" {
		s.release(c)
		http.Error(writer, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return
	}
	c.info.Principal = principal
	socket, err := websocket.Accept(writer, request, &websocket.AcceptOptions{OriginPatterns: s.endpoint.Origins})
	if err != nil {
		// Accept has already written the refusal (for example 403 for a
		// foreign origin).
		s.release(c)
		return
	}
	socket.SetReadLimit(s.endpoint.MaxMessageBytes)
	if s.beforeAttach != nil {
		s.beforeAttach(c)
	}
	c.attach(socket)
	c.serve()
}

// originAllowed applies the browser origin policy: a request without an
// Origin header (a non-browser client) is allowed, as is the same host or a
// host matching one of the patterns. Accept checks the same rule again.
func originAllowed(request *http.Request, patterns []string) bool {
	origin := request.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	if strings.EqualFold(u.Host, request.Host) {
		return true
	}
	for _, pattern := range patterns {
		if matched, _ := path.Match(strings.ToLower(pattern), strings.ToLower(u.Host)); matched {
			return true
		}
	}
	return false
}

// reserve admits a connection under the bound and registers it, so Shutdown
// sees every connection that might upgrade.
func (s *Server) reserve() (*conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing || len(s.conns) >= s.endpoint.MaxConnections {
		return nil, false
	}
	var random [12]byte
	_, _ = rand.Read(random[:])
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{server: s, info: Connection{ID: "ws-" + hex.EncodeToString(random[:])}, ctx: ctx, cancel: cancel, queue: make(chan []byte, s.endpoint.QueueDepth), closed: make(chan struct{})}
	s.conns[c.info.ID] = c
	s.group.Add(1)
	return c, true
}

func (s *Server) release(c *conn) {
	s.mu.Lock()
	delete(s.conns, c.info.ID)
	s.mu.Unlock()
	s.group.Done()
}

// Connections reports how many connections are open.
func (s *Server) Connections() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.conns) }

// Shutdown refuses new connections, closes every open one with 1001 (going
// away), and waits until each has run its disconnect workflow and released
// its goroutines, or ctx ends.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	open := make([]*conn, 0, len(s.conns))
	for _, c := range s.conns {
		open = append(open, c)
	}
	s.mu.Unlock()
	for _, c := range open {
		c.close(websocket.StatusGoingAway, ReasonShutdown)
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

type conn struct {
	server *Server
	info   Connection
	// socket is set once by attach; mu orders that against a concurrent
	// close (a shutdown racing the upgrade).
	mu     sync.Mutex
	socket *websocket.Conn
	// ctx is the connection's work context: closing cancels it, which
	// cancels the in-flight message.
	ctx        context.Context
	cancel     context.CancelFunc
	queue      chan []byte
	reason     string
	stopOnce   sync.Once
	closed     chan struct{}
	notifyOnce sync.Once
}

// close ends the connection once; the first cause becomes the disconnect
// reason. Work is canceled at once. Writes and pings do not use the work
// context, so canceling it does not tear the socket down: the close
// handshake, which carries the status code to the peer and then unblocks the
// reader, runs separately so a peer that does not read cannot hold the
// caller. A peer that has stopped reading cannot receive the close frame.
func (c *conn) close(code websocket.StatusCode, reason string) {
	c.stopOnce.Do(func() {
		c.reason = reason
		c.cancel()
		go func() {
			defer close(c.closed)
			c.mu.Lock()
			socket := c.socket
			c.mu.Unlock()
			if socket != nil {
				_ = socket.Close(code, reason)
			}
		}()
	})
}

// attach gives the connection its upgraded socket. If the connection was
// already closed while upgrading (a shutdown raced it), the socket is closed
// here with 1001, because the close handshake ran before there was a socket.
// This runs on the connection's own goroutine, which Shutdown waits for.
func (c *conn) attach(socket *websocket.Conn) {
	c.mu.Lock()
	c.socket = socket
	stopped := c.ctx.Err() != nil
	c.mu.Unlock()
	if stopped {
		_ = socket.Close(websocket.StatusGoingAway, ReasonShutdown)
	}
}

func (c *conn) serve() {
	s := c.server
	defer s.release(c)
	e := s.endpoint
	if c.ctx.Err() != nil {
		// Closed before the upgrade finished; attach already closed the
		// socket.
		<-c.closed
		c.disconnected()
		return
	}
	if e.OnConnect != nil {
		if err := c.connect(); err != nil {
			code := "rejected"
			if classified, _, ok := trigger.Classify(err); ok {
				code = classified
			}
			c.stopOnce.Do(func() {
				c.reason = ReasonRejected
				c.cancel()
				_ = c.socket.Close(websocket.StatusPolicyViolation, code)
				close(c.closed)
			})
			// If a shutdown won the race, wait for its handshake instead.
			<-c.closed
			c.disconnected()
			return
		}
	}
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); c.read() }()
	go func() { defer workers.Done(); c.keepalive() }()
	c.process()
	workers.Wait()
	// Every exit path went through close; wait for its handshake to end.
	<-c.closed
	_ = c.socket.CloseNow()
	c.disconnected()
}

// read moves frames into the bounded queue. Overflow is the inbound
// backpressure policy: the connection is closed with 1008.
// Reads are not tied to the work context: closing the socket is what ends
// them, after the close frame has been sent.
func (c *conn) read() {
	defer close(c.queue)
	for {
		kind, data, err := c.socket.Read(context.Background())
		if err != nil {
			switch {
			case c.ctx.Err() != nil:
			case errors.Is(err, websocket.ErrMessageTooBig):
				c.close(websocket.StatusMessageTooBig, ReasonTooLarge)
			default:
				c.close(websocket.StatusNormalClosure, ReasonClientClosed)
			}
			return
		}
		if kind != websocket.MessageText {
			c.close(websocket.StatusUnsupportedData, ReasonUnsupportedData)
			return
		}
		select {
		case c.queue <- data:
		default:
			c.close(websocket.StatusPolicyViolation, ReasonBackpressure)
			return
		}
	}
}

// keepalive pings at PingInterval; a pong that does not arrive within
// PongTimeout closes the connection.
func (c *conn) keepalive() {
	e := c.server.endpoint
	ticker := time.NewTicker(e.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), e.PongTimeout)
			err := c.socket.Ping(ctx)
			cancel()
			if err != nil && c.ctx.Err() == nil {
				c.close(websocket.StatusPolicyViolation, ReasonPingTimeout)
				return
			}
		}
	}
}

type request struct {
	ID    string          `json:"id"`
	Input json.RawMessage `json:"input"`
}

type reply struct {
	ID     string          `json:"id,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
}

var requestID = regexp.MustCompile(`^[\x21-\x7e]{0,64}$`)

// process runs one message at a time, in arrival order.
func (c *conn) process() {
	for data := range c.queue {
		if c.ctx.Err() != nil {
			return
		}
		r, code := c.decode(data)
		if code != "" {
			c.write(reply{ID: r.ID, Error: code})
			continue
		}
		answer := c.handle(r)
		if c.ctx.Err() != nil {
			return
		}
		c.write(answer)
	}
}

func (c *conn) decode(data []byte) (request, string) {
	var r request
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&r); err != nil || decoder.More() || !requestID.MatchString(r.ID) {
		// A request id is echoed only if it is itself well formed.
		if !requestID.MatchString(r.ID) {
			r.ID = ""
		}
		return r, "invalid_frame"
	}
	candidate := r.Input
	if len(candidate) == 0 {
		candidate = json.RawMessage("null")
	}
	if _, err := c.server.input.Normalize(candidate); err != nil {
		return r, "invalid_input"
	}
	return r, ""
}

func (c *conn) handle(r request) reply {
	s := c.server
	lease, err := s.application.Begin()
	if err != nil {
		return reply{ID: r.ID, Error: "unavailable"}
	}
	defer lease.Release()
	ctx, cancel := context.WithTimeout(c.ctx, s.endpoint.MessageTimeout)
	defer cancel()
	output, err := s.endpoint.OnMessage(ctx, Message{Connection: c.info, RequestID: r.ID, Input: append(json.RawMessage(nil), r.Input...)})
	switch {
	case err == nil:
		return reply{ID: r.ID, Output: output}
	case errors.Is(err, trigger.ErrSaturated):
		return reply{ID: r.ID, Error: "saturated"}
	case errors.Is(err, context.DeadlineExceeded) && c.ctx.Err() == nil:
		return reply{ID: r.ID, Error: "timeout"}
	}
	if code, _, ok := trigger.Classify(err); ok {
		return reply{ID: r.ID, Error: code}
	}
	return reply{ID: r.ID, Error: "internal"}
}

// write sends one reply within WriteTimeout; a peer that does not read in
// time is a slow reader and is disconnected.
func (c *conn) write(r reply) {
	data, err := json.Marshal(r)
	if err != nil {
		data = []byte(`{"error":"internal"}`)
	}
	if len(data) > c.server.endpoint.MaxReplyBytes {
		data, _ = json.Marshal(reply{ID: r.ID, Error: "reply_too_large"})
	}
	// The write does not use the work context, so a close during the write
	// does not cut the socket before the close frame. The timer records the
	// slow-reader reason first; the write's own deadline (twice as long)
	// only guarantees the goroutine cannot block forever.
	timeout := c.server.endpoint.WriteTimeout
	timer := time.AfterFunc(timeout, func() {
		c.close(websocket.StatusPolicyViolation, ReasonSlowReader)
	})
	defer timer.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*timeout)
	defer cancel()
	if c.server.beforeWrite != nil {
		c.server.beforeWrite()
	}
	if err := c.socket.Write(ctx, websocket.MessageText, data); err != nil && c.ctx.Err() == nil {
		c.close(websocket.StatusPolicyViolation, ReasonSlowReader)
	}
}

// connect runs the connection workflow under an application lease and the
// message timeout.
func (c *conn) connect() error {
	e := c.server.endpoint
	lease, err := c.server.application.Begin()
	if err != nil {
		return err
	}
	defer lease.Release()
	ctx, cancel := context.WithTimeout(c.ctx, e.MessageTimeout)
	defer cancel()
	return e.OnConnect(ctx, c.info)
}

// disconnected runs the disconnect workflow once, bounded by the message
// timeout and under an application lease when the application is still
// admitting work. It must run even while the application drains, so a
// refused lease does not skip it; hosts shut the WebSocket server down before
// the application so its dependencies outlive these handlers.
func (c *conn) disconnected() {
	c.notifyOnce.Do(func() {
		e := c.server.endpoint
		if e.OnDisconnect == nil {
			return
		}
		if lease, err := c.server.application.Begin(); err == nil {
			defer lease.Release()
		}
		ctx, cancel := context.WithTimeout(context.Background(), e.MessageTimeout)
		defer cancel()
		e.OnDisconnect(ctx, Disconnected{Connection: c.info, Reason: c.reason})
	})
}
