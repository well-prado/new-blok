package sse

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
	"weak"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/trigger"
)

type acceptAll struct{}

func (acceptAll) Submit(context.Context, trigger.Submission) (bool, error) { return true, nil }
func (acceptAll) Settled(context.Context, string) (bool, error)            { return false, nil }

// TestReplayIsNotPinnedBySubscription: once the hub drops the events a
// subscriber was replayed, an open subscription does not keep them alive.
func TestReplayIsNotPinnedBySubscription(t *testing.T) {
	hub, err := NewHub(HubConfig{RetainEvents: 16})
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
	server, err := New(application, hub, []Endpoint{{Name: "orders", Path: "/orders", Kind: "order", Submit: acceptAll{}, Tracker: acceptAll{}, InputSchema: []byte(`{"type":"object"}`),
		Authenticate: func(*http.Request) (trigger.Principal, error) { return alice, nil }}})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(server)
	defer web.Close()
	defer func() { _ = server.Shutdown(context.Background()) }()
	stream := StreamID(SubmissionKey("orders", alice, "k"))
	if _, err := hub.open(stream, alice, "key"); err != nil {
		t.Fatal(err)
	}
	hub.settle(stream, true)
	data := json.RawMessage(`"` + strings.Repeat("x", 4096) + `"`)
	var replayed []weak.Pointer[stored]
	for i := 0; i < 16; i++ {
		if _, err := hub.Publish(stream, Event{Type: "progress", Data: data}); err != nil {
			t.Fatal(err)
		}
	}
	hub.mu.Lock()
	for _, event := range hub.streams[stream].events {
		replayed = append(replayed, weak.Make(event))
	}
	hub.mu.Unlock()
	response, err := web.Client().Get(web.URL + "/orders/" + stream)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	// Read the whole replay (16 events of about 4 KiB).
	if _, err := io.ReadFull(response.Body, make([]byte, 16*4096)); err != nil {
		t.Fatal(err)
	}
	// The hub moves on and drops every replayed event.
	for i := 0; i < 16; i++ {
		if _, err := hub.Publish(stream, Event{Type: "progress"}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		alive := 0
		for _, pointer := range replayed {
			if pointer.Value() != nil {
				alive++
			}
		}
		if alive == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of 16 replayed events are still held by the open subscription", alive)
		}
		time.Sleep(20 * time.Millisecond)
	}
	response.Body.Close()
	// The ended subscription leaves nothing behind in the server.
	for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		server.mu.Lock()
		open := len(server.subs)
		server.mu.Unlock()
		if open == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d ended subscriptions are still tracked by the server", open)
		}
	}
}
