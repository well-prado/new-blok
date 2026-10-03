package websocket_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/trigger"
	blokws "github.com/well-prado/new-blok/trigger/websocket"
)

type fixture struct {
	InputSchema     json.RawMessage `json:"inputSchema"`
	MaxMessageBytes int64           `json:"maxMessageBytes"`
	Frames          []struct {
		Name    string `json:"name"`
		Frame   string `json:"frame"`
		Reply   string `json:"reply"`
		Code    string `json:"code"`
		ID      string `json:"id"`
		Effects int    `json:"effects"`
		Spoof   string `json:"spoof"`
	} `json:"frames"`
	Closing []struct {
		Name   string `json:"name"`
		Kind   string `json:"kind"`
		Status int    `json:"status"`
		Reason string `json:"reason"`
	} `json:"closing"`
	Expected map[string]int `json:"expected"`
}

func loadFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "websocket", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

type domainError struct{}

func (domainError) Error() string      { return "out_of_stock" }
func (domainError) ErrorCode() string  { return "out_of_stock" }
func (domainError) ErrorClass() string { return "validation" }

// recorder is the workflow side: it counts effects and disconnects and
// echoes the identity the server assigned.
type recorder struct {
	mu          sync.Mutex
	active      int
	peak        int
	unbounded   int
	effects     int
	disconnects map[string][]string
	connects    int
}

func newRecorder() *recorder { return &recorder{disconnects: map[string][]string{}} }

func (r *recorder) onMessage(ctx context.Context, m blokws.Message) (json.RawMessage, error) {
	var input struct {
		SKU      string `json:"sku"`
		Quantity int    `json:"quantity"`
	}
	_ = json.Unmarshal(m.Input, &input)
	switch input.SKU {
	case "out-of-stock":
		return nil, domainError{}
	case "explode":
		return nil, errors.New("synthetic-secret-detail: dsn=postgres://u:p@db")
	case "busy":
		return nil, trigger.ErrSaturated
	case "big":
		return json.Marshal(map[string]string{"blob": strings.Repeat("b", 16<<10)})
	case "huge":
		return json.Marshal(map[string]string{"blob": strings.Repeat("h", 1<<20)})
	case "jitter":
		r.mu.Lock()
		r.active++
		if r.active > r.peak {
			r.peak = r.active
		}
		r.mu.Unlock()
		time.Sleep(time.Duration(input.Quantity%7) * time.Millisecond)
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
	case "slow":
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	r.mu.Lock()
	r.effects++
	r.mu.Unlock()
	return json.Marshal(map[string]any{"connection": m.Connection.ID, "principal": m.Connection.Principal.ID, "quantity": input.Quantity})
}

func (r *recorder) onConnect(context.Context, blokws.Connection) error {
	r.mu.Lock()
	r.connects++
	r.mu.Unlock()
	return nil
}

func (r *recorder) onDisconnect(ctx context.Context, d blokws.Disconnected) {
	r.mu.Lock()
	r.disconnects[d.Connection.ID] = append(r.disconnects[d.Connection.ID], d.Reason)
	if _, ok := ctx.Deadline(); !ok {
		r.unbounded++
	}
	r.mu.Unlock()
}

func (r *recorder) effectCount() int { r.mu.Lock(); defer r.mu.Unlock(); return r.effects }

// reasons returns a copy of every disconnect reason reported so far.
func (r *recorder) reasons() map[string][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := make(map[string][]string, len(r.disconnects))
	for id, list := range r.disconnects {
		copied[id] = append([]string(nil), list...)
	}
	return copied
}

func (r *recorder) disconnectCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, reasons := range r.disconnects {
		total += len(reasons)
	}
	return total
}

func authenticate(request *http.Request) (trigger.Principal, error) {
	token := strings.TrimPrefix(request.Header.Get("Authorization"), "Bearer ")
	if !strings.HasPrefix(token, "user-") {
		return trigger.Principal{}, errors.New("unauthenticated")
	}
	return trigger.Principal{ID: token}, nil
}

type env struct {
	server   *blokws.Server
	http     *httptest.Server
	app      *app.Application
	recorder *recorder
}

func newEnv(t *testing.T, f fixture, configure func(*blokws.Endpoint)) *env {
	t.Helper()
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	r := newRecorder()
	endpoint := blokws.Endpoint{Path: "/ws", Authenticate: authenticate, OnConnect: r.onConnect, OnMessage: r.onMessage, OnDisconnect: r.onDisconnect, InputSchema: f.InputSchema, MaxMessageBytes: f.MaxMessageBytes}
	if configure != nil {
		configure(&endpoint)
	}
	server, err := blokws.New(application, endpoint)
	if err != nil {
		t.Fatal(err)
	}
	listener := httptest.NewServer(server)
	e := &env{server: server, http: listener, app: application, recorder: r}
	t.Cleanup(func() {
		_ = server.Shutdown(context.Background())
		listener.Close()
		_ = application.Shutdown(context.Background())
	})
	return e
}

func (e *env) dial(t *testing.T, token string, header http.Header) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	if header == nil {
		header = http.Header{}
	}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return websocket.Dial(ctx, "ws"+strings.TrimPrefix(e.http.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: header})
}

type reply struct {
	ID     string          `json:"id"`
	Output json.RawMessage `json:"output"`
	Error  string          `json:"error"`
}

func roundTrip(t *testing.T, conn *websocket.Conn, frame string) reply {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var r reply
	if err := json.Unmarshal(data, &r); err != nil {
		t.Fatalf("reply %q: %v", data, err)
	}
	return r
}

func waitFor(t *testing.T, what string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFramesOverRealSocket(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, nil)
	conn, _, err := e.dial(t, "user-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	output, failures := 0, 0
	var connectionID string
	for _, tc := range f.Frames {
		frame := strings.Replace(tc.Frame, "LONGID", strings.Repeat("x", 65), 1)
		before := e.recorder.effectCount()
		r := roundTrip(t, conn, frame)
		if r.ID != tc.ID || (tc.Reply == "output") != (len(r.Output) > 0) || r.Error != tc.Code || e.recorder.effectCount()-before != tc.Effects {
			t.Fatalf("%s: reply=%+v effects=%d, want %s %q id=%q effects=%d", tc.Name, r, e.recorder.effectCount()-before, tc.Reply, tc.Code, tc.ID, tc.Effects)
		}
		if strings.Contains(string(r.Output)+r.Error, "synthetic-secret-detail") {
			t.Fatalf("%s leaked an internal error", tc.Name)
		}
		if len(r.Output) > 0 {
			output++
			var echoed struct {
				Connection string `json:"connection"`
				Principal  string `json:"principal"`
			}
			_ = json.Unmarshal(r.Output, &echoed)
			if connectionID == "" {
				connectionID = echoed.Connection
			}
			if echoed.Connection != connectionID || echoed.Connection == tc.Spoof || !strings.HasPrefix(echoed.Connection, "ws-") || echoed.Principal != "user-1" {
				t.Fatalf("%s: handler saw connection %q principal %q", tc.Name, echoed.Connection, echoed.Principal)
			}
		} else {
			failures++
		}
	}
	closed := 0
	for _, tc := range f.Closing {
		c, _, err := e.dial(t, "user-2", nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		switch tc.Kind {
		case "binary":
			err = c.Write(ctx, websocket.MessageBinary, []byte{1, 2, 3})
		case "oversized":
			err = c.Write(ctx, websocket.MessageText, []byte(`{"id":"big","input":{"sku":"`+strings.Repeat("c", int(f.MaxMessageBytes))+`","quantity":1}}`))
		}
		if err != nil {
			t.Fatalf("%s: write: %v", tc.Name, err)
		}
		_, _, err = c.Read(ctx)
		cancel()
		if got := websocket.CloseStatus(err); int(got) != tc.Status {
			t.Fatalf("%s: close status %d (%v), want %d", tc.Name, got, err, tc.Status)
		}
		closed++
		c.CloseNow()
	}
	waitFor(t, "disconnect events", func() bool { return e.recorder.disconnectCount() == 2 })
	reasons := map[string]bool{}
	for _, list := range e.recorder.reasons() {
		for _, reason := range list {
			reasons[reason] = true
		}
	}
	for _, tc := range f.Closing {
		if !reasons[tc.Reason] {
			t.Fatalf("disconnect reasons %v, want %s", e.recorder.reasons(), tc.Reason)
		}
	}
	if output != f.Expected["output"] || failures != f.Expected["errors"] || e.recorder.effectCount() != f.Expected["effects"] || closed != f.Expected["closed"] {
		t.Fatalf("output=%d errors=%d effects=%d closed=%d, want %v", output, failures, e.recorder.effectCount(), closed, f.Expected)
	}
}

func TestHandshakeOriginAuthenticationAndPing(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, func(endpoint *blokws.Endpoint) {
		endpoint.Origins = []string{"app.example.test"}
		endpoint.PingInterval = 50 * time.Millisecond
		endpoint.PongTimeout = 100 * time.Millisecond
	})
	for _, tc := range []struct {
		name, token, origin string
		status              int
	}{
		{"no credential", "", "", http.StatusUnauthorized},
		{"forged credential", "admin", "", http.StatusUnauthorized},
		{"foreign origin", "user-1", "https://evil.example", http.StatusForbidden},
	} {
		header := http.Header{}
		if tc.origin != "" {
			header.Set("Origin", tc.origin)
		}
		_, response, err := e.dial(t, tc.token, header)
		if err == nil || response == nil || response.StatusCode != tc.status {
			status := 0
			if response != nil {
				status = response.StatusCode
			}
			t.Fatalf("%s: status=%d err=%v, want %d", tc.name, status, err, tc.status)
		}
	}
	allowed := http.Header{"Origin": {"https://app.example.test"}}
	conn, _, err := e.dial(t, "user-1", allowed)
	if err != nil {
		t.Fatalf("allowed origin refused: %v", err)
	}
	// A reading client answers pings and stays connected well past several
	// ping intervals.
	readCtx, stopReading := context.WithCancel(context.Background())
	go func() {
		for {
			if _, _, err := conn.Read(readCtx); err != nil {
				return
			}
		}
	}()
	time.Sleep(400 * time.Millisecond)
	if e.server.Connections() != 1 {
		t.Fatalf("a client answering pings was disconnected")
	}
	stopReading()
	conn.CloseNow()
	// A client that never reads never answers a ping and is closed.
	silent, _, err := e.dial(t, "user-2", allowed)
	if err != nil {
		t.Fatal(err)
	}
	defer silent.CloseNow()
	waitFor(t, "ping timeout", func() bool {
		e.recorder.mu.Lock()
		defer e.recorder.mu.Unlock()
		for _, reasons := range e.recorder.disconnects {
			for _, reason := range reasons {
				if reason == blokws.ReasonPingTimeout {
					return true
				}
			}
		}
		return false
	})
}

func TestMultiClientIsolationAndSpoofedIDs(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, nil)
	const clients, messages = 16, 20
	var group sync.WaitGroup
	seen := make([]map[string]bool, clients)
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			conn, _, err := e.dial(t, fmt.Sprintf("user-%d", i), nil)
			if err != nil {
				errs <- err
				return
			}
			defer conn.CloseNow()
			seen[i] = map[string]bool{}
			for m := 0; m < messages; m++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				// Every client claims to be connection "ws-victim".
				frame := fmt.Sprintf(`{"id":"c%d-m%d","input":{"sku":"coffee","quantity":%d,"connectionId":"ws-victim"}}`, i, m, m%100+1)
				if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
					cancel()
					errs <- err
					return
				}
				_, data, err := conn.Read(ctx)
				cancel()
				if err != nil {
					errs <- err
					return
				}
				var r struct {
					ID     string `json:"id"`
					Output struct {
						Connection string `json:"connection"`
						Principal  string `json:"principal"`
						Quantity   int    `json:"quantity"`
					} `json:"output"`
				}
				if err := json.Unmarshal(data, &r); err != nil {
					errs <- err
					return
				}
				if r.ID != fmt.Sprintf("c%d-m%d", i, m) || r.Output.Principal != fmt.Sprintf("user-%d", i) || r.Output.Quantity != m%100+1 || r.Output.Connection == "ws-victim" {
					errs <- fmt.Errorf("client %d received %s", i, data)
					return
				}
				seen[i][r.Output.Connection] = true
			}
		}(i)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	ids := map[string]int{}
	for i, set := range seen {
		if len(set) != 1 {
			t.Fatalf("client %d saw connection ids %v; one connection must keep one id", i, set)
		}
		for id := range set {
			ids[id]++
		}
	}
	if len(ids) != clients {
		t.Fatalf("connection ids %v are not distinct per client", ids)
	}
}

func TestSlowClientsFollowDeclaredPolicy(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, func(endpoint *blokws.Endpoint) {
		endpoint.QueueDepth = 4
		endpoint.WriteTimeout = 200 * time.Millisecond
		endpoint.PingInterval = time.Hour
	})
	// An inbound flood while a message is in flight overflows the bounded
	// queue and is disconnected with 1008.
	flood, _, err := e.dial(t, "user-flood", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer flood.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = flood.Write(ctx, websocket.MessageText, []byte(`{"id":"slow","input":{"sku":"slow","quantity":1}}`))
	for i := 0; i < 50; i++ {
		if err := flood.Write(ctx, websocket.MessageText, []byte(`{"id":"f","input":{"sku":"coffee","quantity":1}}`)); err != nil {
			break
		}
	}
	_, _, err = flood.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("flood close status %d (%v), want 1008", websocket.CloseStatus(err), err)
	}
	waitFor(t, "backpressure disconnect", func() bool {
		e.recorder.mu.Lock()
		defer e.recorder.mu.Unlock()
		for _, reasons := range e.recorder.disconnects {
			if len(reasons) > 0 && reasons[0] == blokws.ReasonBackpressure {
				return true
			}
		}
		return false
	})
	for id, reasons := range e.recorder.reasons() {
		if len(reasons) != 1 || reasons[0] != blokws.ReasonBackpressure {
			t.Fatalf("connection %s reported %v, want exactly backpressure", id, reasons)
		}
	}
}

func TestDisconnectRacesReportOnce(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, nil)
	const clients = 40
	var conns []*websocket.Conn
	for i := 0; i < clients; i++ {
		conn, _, err := e.dial(t, fmt.Sprintf("user-%d", i), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = conn.Write(ctx, websocket.MessageText, []byte(`{"id":"s","input":{"sku":"slow","quantity":1}}`))
		cancel()
		conns = append(conns, conn)
	}
	waitFor(t, "connections", func() bool { return e.server.Connections() == clients })
	// Half the clients vanish while their message runs, at the same moment
	// the server shuts down.
	var group sync.WaitGroup
	for i, conn := range conns {
		if i%2 == 0 {
			group.Add(1)
			go func(c *websocket.Conn) { defer group.Done(); c.CloseNow() }(conn)
		}
	}
	shutdown := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdown <- e.server.Shutdown(ctx)
	}()
	group.Wait()
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	for _, conn := range conns {
		conn.CloseNow()
	}
	if e.recorder.disconnectCount() != clients || len(e.recorder.reasons()) != clients {
		t.Fatalf("disconnects=%d over %d connections, want exactly %d", e.recorder.disconnectCount(), len(e.recorder.reasons()), clients)
	}
	if e.recorder.effectCount() != 0 {
		t.Fatalf("effects=%d: canceled work completed", e.recorder.effectCount())
	}
	e.recorder.mu.Lock()
	unbounded := e.recorder.unbounded
	e.recorder.mu.Unlock()
	if unbounded != 0 {
		t.Fatalf("%d disconnect workflows ran without a deadline", unbounded)
	}
	if _, response, err := e.dial(t, "user-late", nil); err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("a connection was accepted after shutdown")
	}
}

func TestShutdownDrainsWithBoundedGoroutinesAndMemory(t *testing.T) {
	f := loadFixture(t)
	runtime.GC()
	baseline := runtime.NumGoroutine()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	rssBefore := rss()
	e := newEnv(t, f, nil)
	const clients = 200
	var conns []*websocket.Conn
	for i := 0; i < clients; i++ {
		conn, _, err := e.dial(t, fmt.Sprintf("user-%d", i), nil)
		if err != nil {
			t.Fatal(err)
		}
		roundTrip(t, conn, `{"id":"x","input":{"sku":"coffee","quantity":1}}`)
		conns = append(conns, conn)
	}
	runtime.GC()
	var open runtime.MemStats
	runtime.ReadMemStats(&open)
	perConnGoroutines := float64(runtime.NumGoroutine()-baseline) / clients
	perConnHeap := float64(int64(open.HeapInuse)-int64(before.HeapInuse)) / clients
	perConnRSS := float64(rss()-rssBefore) / clients
	t.Logf("open: %.1f goroutines, %.0f heap bytes and %.0f RSS bytes per connection (client and server sides together)", perConnGoroutines, perConnHeap, perConnRSS)
	// The server runs 3 goroutines per connection (connection, reader,
	// keepalive) and the client side 0 in steady state; a single leaked
	// goroutine per connection would exceed this bound.
	if perConnGoroutines > 3.5 || perConnHeap > 96<<10 || perConnRSS > rssBoundPerConnection {
		t.Fatalf("per-connection footprint %.1f goroutines / %.0f heap / %.0f RSS bytes exceeds the bound", perConnGoroutines, perConnHeap, perConnRSS)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, conn := range conns {
		readCtx, stop := context.WithTimeout(context.Background(), time.Second)
		_, _, err := conn.Read(readCtx)
		stop()
		if websocket.CloseStatus(err) != websocket.StatusGoingAway {
			t.Fatalf("client close status %d (%v), want 1001", websocket.CloseStatus(err), err)
		}
		conn.CloseNow()
	}
	if e.recorder.disconnectCount() != clients || e.server.Connections() != 0 {
		t.Fatalf("disconnects=%d open=%d", e.recorder.disconnectCount(), e.server.Connections())
	}
	e.http.CloseClientConnections()
	e.http.Close()
	var settled atomic.Bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= baseline+2 {
			settled.Store(true)
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !settled.Load() {
		t.Fatalf("goroutines did not drain: %d > baseline %d", runtime.NumGoroutine(), baseline)
	}
}

func TestEndpointConfigurationIsBounded(t *testing.T) {
	application, _ := app.New(app.Config{})
	valid := blokws.Endpoint{Path: "/ws", Authenticate: authenticate, OnMessage: newRecorder().onMessage, InputSchema: []byte(`{"type":"object"}`)}
	for name, mutate := range map[string]func(*blokws.Endpoint){
		"no authenticator":   func(e *blokws.Endpoint) { e.Authenticate = nil },
		"no message handler": func(e *blokws.Endpoint) { e.OnMessage = nil },
		"relative path":      func(e *blokws.Endpoint) { e.Path = "ws" },
		"invalid schema":     func(e *blokws.Endpoint) { e.InputSchema = []byte(`{"type":"nope"}`) },
		"unbounded frames":   func(e *blokws.Endpoint) { e.MaxMessageBytes = blokws.MaxMessageBytesLimit + 1 },
		"unbounded conns":    func(e *blokws.Endpoint) { e.MaxConnections = blokws.MaxConnectionsLimit + 1 },
		"unbounded queue":    func(e *blokws.Endpoint) { e.QueueDepth = blokws.MaxQueueDepth + 1 },
		"queue byte budget": func(e *blokws.Endpoint) {
			e.QueueDepth = blokws.MaxQueueDepth
			e.MaxMessageBytes = blokws.MaxMessageBytesLimit
		},
		"unbounded replies":  func(e *blokws.Endpoint) { e.MaxReplyBytes = blokws.MaxReplyBytesLimit + 1 },
		"bad origin pattern": func(e *blokws.Endpoint) { e.Origins = []string{"["} },
	} {
		endpoint := valid
		mutate(&endpoint)
		if _, err := blokws.New(application, endpoint); err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
	if _, err := blokws.New(application, valid); err != nil {
		t.Fatal(err)
	}
}

func TestConnectionLimitAndRejectedConnect(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, func(endpoint *blokws.Endpoint) {
		endpoint.MaxConnections = 2
		endpoint.OnConnect = func(_ context.Context, c blokws.Connection) error {
			if c.Principal.ID == "user-banned" {
				return domainError{}
			}
			return nil
		}
	})
	a, _, err := e.dial(t, "user-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer a.CloseNow()
	b, _, err := e.dial(t, "user-b", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer b.CloseNow()
	if _, response, err := e.dial(t, "user-c", nil); err == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("a connection beyond the bound was accepted")
	}
	a.CloseNow()
	waitFor(t, "slot release", func() bool { return e.server.Connections() == 1 })
	banned, _, err := e.dial(t, "user-banned", nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = banned.Read(ctx)
	var closeErr websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.StatusPolicyViolation || closeErr.Reason != "out_of_stock" {
		t.Fatalf("refused connection closed with %v, want 1008 out_of_stock", err)
	}
}

// TestShutdownRacingUpgradesDrains starts upgrades and a shutdown at the same
// time, many times. Every connection, whether the shutdown reached it before
// or after its upgrade, must be closed and reported once, and Shutdown must
// drain instead of waiting on a reader nobody will close.
func TestShutdownRacingUpgradesDrains(t *testing.T) {
	f := loadFixture(t)
	for round := 0; round < 20; round++ {
		e := newEnv(t, f, func(endpoint *blokws.Endpoint) { endpoint.PingInterval = time.Hour })
		var group sync.WaitGroup
		var accepted atomic.Int32
		for i := 0; i < 8; i++ {
			group.Add(1)
			go func(i int) {
				defer group.Done()
				conn, _, err := e.dial(t, fmt.Sprintf("user-%d", i), nil)
				if err != nil {
					return
				}
				accepted.Add(1)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, _, _ = conn.Read(ctx)
				conn.CloseNow()
			}(i)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := e.server.Shutdown(ctx)
		cancel()
		if err != nil {
			t.Fatalf("round %d: shutdown did not drain: %v", round, err)
		}
		group.Wait()
		if e.server.Connections() != 0 {
			t.Fatalf("round %d: %d connections left open", round, e.server.Connections())
		}
		for id, reasons := range e.recorder.reasons() {
			if len(reasons) != 1 {
				t.Fatalf("round %d: connection %s reported %d disconnects", round, id, len(reasons))
			}
		}
		if got := e.recorder.disconnectCount(); got < int(accepted.Load()) {
			t.Fatalf("round %d: %d accepted connections but %d disconnects", round, accepted.Load(), got)
		}
	}
}

// TestMessagesOnAConnectionRunInOrderOneAtATime pipelines frames without
// waiting for replies; handlers take varying time. Replies must come back in
// send order and at most one handler may run per connection.
func TestMessagesOnAConnectionRunInOrderOneAtATime(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, nil)
	conn, _, err := e.dial(t, "user-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const messages = 12
	for i := 0; i < messages; i++ {
		frame := fmt.Sprintf(`{"id":"m%02d","input":{"sku":"jitter","quantity":%d}}`, i, 13-i)
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < messages; i++ {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var r reply
		if err := json.Unmarshal(data, &r); err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("m%02d", i); r.ID != want || r.Error != "" {
			t.Fatalf("reply %d = %+v, want %s in order", i, r, want)
		}
	}
	e.recorder.mu.Lock()
	peak := e.recorder.peak
	e.recorder.mu.Unlock()
	if peak != 1 {
		t.Fatalf("peak concurrent handlers on one connection = %d, want 1", peak)
	}
}

// TestSlowReaderIsDisconnected: the queue is deep enough that inbound
// backpressure cannot fire; the client asks for large replies and never reads
// them. Only the write timeout can end this, and it must.
func TestSlowReaderIsDisconnected(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, func(endpoint *blokws.Endpoint) {
		endpoint.QueueDepth = blokws.MaxQueueDepth
		endpoint.MaxReplyBytes = 4 << 20
		endpoint.WriteTimeout = 200 * time.Millisecond
		endpoint.PingInterval = time.Hour
	})
	conn, _, err := e.dial(t, "user-slow", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 16; i++ {
		if err := conn.Write(ctx, websocket.MessageText, []byte(`{"id":"h","input":{"sku":"huge","quantity":1}}`)); err != nil {
			break
		}
	}
	waitFor(t, "slow reader disconnect", func() bool { return e.recorder.disconnectCount() == 1 })
	for _, reasons := range e.recorder.reasons() {
		if len(reasons) != 1 || reasons[0] != blokws.ReasonSlowReader {
			t.Fatalf("disconnect reasons %v, want exactly slow_reader", reasons)
		}
	}
}

// rss is the process resident set in bytes from /proc (Linux), or 0.
func rss() int64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(data), "\n") {
		if fields := strings.Fields(line); len(fields) >= 2 && fields[0] == "VmRSS:" {
			kb, _ := strconv.ParseInt(fields[1], 10, 64)
			return kb << 10
		}
	}
	return 0
}

func TestOversizedReplyIsReplacedByError(t *testing.T) {
	f := loadFixture(t)
	e := newEnv(t, f, func(endpoint *blokws.Endpoint) { endpoint.MaxReplyBytes = 1024 })
	conn, _, err := e.dial(t, "user-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	if r := roundTrip(t, conn, `{"id":"b","input":{"sku":"big","quantity":1}}`); r.Error != "reply_too_large" || r.ID != "b" {
		t.Fatalf("reply=%+v", r)
	}
	if r := roundTrip(t, conn, `{"id":"ok","input":{"sku":"coffee","quantity":1}}`); r.Error != "" {
		t.Fatalf("connection did not continue: %+v", r)
	}
}

// TestAdmissionChecksPrecedeAuthentication: a foreign origin and a full
// endpoint are refused before the authenticator runs.
func TestAdmissionChecksPrecedeAuthentication(t *testing.T) {
	f := loadFixture(t)
	var calls atomic.Int32
	e := newEnv(t, f, func(endpoint *blokws.Endpoint) {
		endpoint.MaxConnections = 1
		endpoint.Authenticate = func(r *http.Request) (trigger.Principal, error) {
			calls.Add(1)
			return authenticate(r)
		}
	})
	if _, response, err := e.dial(t, "", http.Header{"Origin": {"https://evil.example"}}); err == nil || response.StatusCode != http.StatusForbidden {
		t.Fatal("foreign origin was not refused with 403")
	}
	held, _, err := e.dial(t, "user-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer held.CloseNow()
	before := calls.Load()
	if _, response, err := e.dial(t, "", nil); err == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("a full endpoint did not answer 503")
	}
	if calls.Load() != before || before != 1 {
		t.Fatalf("authenticator calls=%d (before full=%d), want 1: only the admitted connection", calls.Load(), before)
	}
}
