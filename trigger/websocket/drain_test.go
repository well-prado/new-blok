package websocket_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/well-prado/new-blok/internal/drainprobe"
	"github.com/well-prado/new-blok/trigger"
	blokws "github.com/well-prado/new-blok/trigger/websocket"
)

// TestDrainTimeoutCancelsAMessage: when the drain times out under a
// message, its handler is canceled and stops before the application closes
// its dependencies, and the reply says the service is unavailable (#177).
func TestDrainTimeoutCancelsAMessage(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	sockets, err := blokws.New(application, blokws.Endpoint{Path: "/ws", InputSchema: []byte(`{"type":"object"}`), MessageTimeout: 30 * time.Second, Authenticate: func(request *http.Request) (trigger.Principal, error) {
		if request.Header.Get("Authorization") != "Bearer alice" {
			return trigger.Principal{}, errors.New("unauthenticated")
		}
		return trigger.Principal{ID: "alice"}, nil
	}, OnMessage: func(ctx context.Context, _ blokws.Message) (json.RawMessage, error) {
		return nil, work.Run(ctx)
	}})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(sockets)
	defer web.Close()
	ctx := context.Background()
	socket, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(web.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer alice"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.CloseNow()
	if err := socket.Write(ctx, websocket.MessageText, []byte(`{"id":"1","input":{}}`)); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		reply map[string]any
		err   error
	}
	answered := make(chan answer, 1)
	go func() {
		_, frame, err := socket.Read(ctx)
		var reply map[string]any
		_ = json.Unmarshal(frame, &reply)
		answered <- answer{reply: reply, err: err}
	}()
	drainprobe.Abort(t, application, probe, work)
	if got := <-answered; got.err != nil || got.reply["id"] != "1" || got.reply["error"] != "unavailable" {
		t.Fatalf("message answered %+v; want unavailable", got)
	}
	socket.CloseNow()
	stop, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = sockets.Shutdown(stop)
}

func aliceOnly(request *http.Request) (trigger.Principal, error) {
	if request.Header.Get("Authorization") != "Bearer alice" {
		return trigger.Principal{}, errors.New("unauthenticated")
	}
	return trigger.Principal{ID: "alice"}, nil
}

// TestDrainTimeoutCancelsAConnectionWorkflow: when the drain times out
// under OnConnect, it is canceled and stops before the application closes
// its dependencies, and the client is closed with "try again later" (#177).
func TestDrainTimeoutCancelsAConnectionWorkflow(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	sockets, err := blokws.New(application, blokws.Endpoint{Path: "/ws", InputSchema: []byte(`{"type":"object"}`), MessageTimeout: 30 * time.Second, Authenticate: aliceOnly,
		OnConnect: func(ctx context.Context, _ blokws.Connection) error { return work.Run(ctx) },
		OnMessage: func(context.Context, blokws.Message) (json.RawMessage, error) { return json.RawMessage(`{}`), nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(sockets)
	defer web.Close()
	ctx := context.Background()
	socket, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(web.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer alice"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer socket.CloseNow()
	closed := make(chan error, 1)
	go func() {
		_, _, err := socket.Read(ctx)
		closed <- err
	}()
	drainprobe.Abort(t, application, probe, work)
	var closing websocket.CloseError
	if err := <-closed; !errors.As(err, &closing) || closing.Code != websocket.StatusTryAgainLater || closing.Reason != "unavailable" {
		t.Fatalf("connection closed with %v; want 1013 unavailable", err)
	}
	stop, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = sockets.Shutdown(stop)
}

// TestDrainTimeoutCancelsADisconnectWorkflow: OnDisconnect, run under a
// lease, also stops when the drain times out, before the application
// closes its dependencies (#177).
func TestDrainTimeoutCancelsADisconnectWorkflow(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	sockets, err := blokws.New(application, blokws.Endpoint{Path: "/ws", InputSchema: []byte(`{"type":"object"}`), MessageTimeout: 30 * time.Second, Authenticate: aliceOnly,
		OnMessage:    func(context.Context, blokws.Message) (json.RawMessage, error) { return json.RawMessage(`{}`), nil },
		OnDisconnect: func(ctx context.Context, _ blokws.Disconnected) { _ = work.Run(ctx) },
	})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(sockets)
	defer web.Close()
	ctx := context.Background()
	socket, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(web.URL, "http")+"/ws", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer alice"}}})
	if err != nil {
		t.Fatal(err)
	}
	_ = socket.Close(websocket.StatusNormalClosure, "bye")
	drainprobe.Abort(t, application, probe, work)
	stop, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	_ = sockets.Shutdown(stop)
}
