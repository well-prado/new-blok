package websocket

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/trigger"
)

// TestShutdownBetweenUpgradeAndAttach holds a connection after its upgrade
// but before its socket is attached, until Shutdown has closed it. The socket
// attached afterwards must still be closed with 1001 and the connection
// reported, so Shutdown drains instead of waiting on a reader nobody closes.
func TestShutdownBetweenUpgradeAndAttach(t *testing.T) {
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	disconnects := make(chan Disconnected, 1)
	server, err := New(application, Endpoint{
		Path: "/ws", InputSchema: []byte(`{"type":"object"}`), PingInterval: time.Hour,
		Authenticate: func(*http.Request) (trigger.Principal, error) { return trigger.Principal{ID: "user-1"}, nil },
		OnMessage:    func(context.Context, Message) (json.RawMessage, error) { return json.RawMessage(`{}`), nil },
		OnDisconnect: func(_ context.Context, d Disconnected) { disconnects <- d },
	})
	if err != nil {
		t.Fatal(err)
	}
	held := make(chan struct{})
	server.beforeAttach = func(c *conn) {
		close(held)
		deadline := time.Now().Add(5 * time.Second)
		for c.ctx.Err() == nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	listener := httptest.NewServer(server)
	defer listener.Close()
	closed := make(chan websocket.StatusCode, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(listener.URL, "http")+"/ws", nil)
		if err != nil {
			closed <- -2
			return
		}
		defer conn.CloseNow()
		_, _, err = conn.Read(ctx)
		closed <- websocket.CloseStatus(err)
	}()
	<-held
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown did not drain a connection closed before its socket was attached: %v", err)
	}
	if status := <-closed; status != websocket.StatusGoingAway {
		t.Fatalf("client close status %d, want 1001", status)
	}
	select {
	case d := <-disconnects:
		if d.Reason != ReasonShutdown {
			t.Fatalf("disconnect reason %q", d.Reason)
		}
	case <-time.After(time.Second):
		t.Fatal("the connection was never reported")
	}
}

// TestShutdownDuringReplyStillSendsCloseFrame starts a shutdown while a reply
// far larger than the transport's buffers is being written to a client that
// has not started reading. The write must not be cut: the client gets the
// whole reply and then the 1001 close.
func TestShutdownDuringReplyStillSendsCloseFrame(t *testing.T) {
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	giant := []byte(`{"blob":"` + strings.Repeat("g", 12<<20) + `"}`)
	server, err := New(application, Endpoint{
		Path: "/ws", InputSchema: []byte(`{"type":"object"}`), PingInterval: time.Hour,
		MaxReplyBytes: MaxReplyBytesLimit, WriteTimeout: 10 * time.Second,
		Authenticate: func(*http.Request) (trigger.Principal, error) { return trigger.Principal{ID: "user-1"}, nil },
		OnMessage:    func(context.Context, Message) (json.RawMessage, error) { return giant, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	writing := make(chan struct{})
	server.beforeWrite = func() { close(writing) }
	listener := httptest.NewServer(server)
	defer listener.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(listener.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(16 << 20)
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"id":"g","input":{}}`)); err != nil {
		t.Fatal(err)
	}
	<-writing
	time.Sleep(100 * time.Millisecond) // the write is now blocked on the full transport
	shutdown := make(chan error, 1)
	go func() { shutdown <- server.Shutdown(ctx) }()
	time.Sleep(100 * time.Millisecond)
	_, data, err := conn.Read(ctx)
	if err != nil || !strings.Contains(string(data[:min(len(data), 64)]), `"id":"g"`) || len(data) < 12<<20 {
		t.Fatalf("reply cut: %d bytes err=%v", len(data), err)
	}
	_, _, err = conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusGoingAway {
		t.Fatalf("close status %d (%v), want 1001", websocket.CloseStatus(err), err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
}
