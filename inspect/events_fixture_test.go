package inspect_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
)

// TestLiveEventStreamFixtureCases runs every synthetic fixture case against
// an actual HTTP-triggered run and compares the predeclared outcome: the
// run's HTTP status, the stream's status and frames, the error code, the
// number of effects, how many frames carried payloads, and that no frame
// leaked the owner or a credential.
func TestLiveEventStreamFixtureCases(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "inspection", "events", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SchemaVersion int    `json:"schemaVersion"`
		Owner         string `json:"owner"`
		Contract      string `json:"contract"`
		Note          string `json:"note"`
		Cases         []struct {
			ID   string `json:"id"`
			Kind string `json:"kind"`
			Run  struct {
				Principal string `json:"principal"`
				SKU       string `json:"sku"`
			} `json:"run"`
			Read struct {
				Principal string `json:"principal"`
				Cursor    string `json:"cursor"`
			} `json:"read"`
			Capture  string `json:"capture"`
			Expected struct {
				RunHTTP       string   `json:"runHTTP"`
				StreamStatus  int      `json:"streamStatus"`
				Frames        []string `json:"frames"`
				Effects       int64    `json:"effects"`
				ErrorCode     string   `json:"errorCode"`
				PayloadFrames int      `json:"payloadFrames"`
				SecretLeaks   int      `json:"secretLeaks"`
			} `json:"expected"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	accepted, rejected := 0, 0
	for _, item := range fixture.Cases {
		switch item.Kind {
		case "accepted":
			accepted++
		case "rejected":
			rejected++
		}
	}
	if fixture.SchemaVersion != 1 || fixture.Owner != "E15-T02" || accepted < 4 || rejected < 4 || accepted+rejected != len(fixture.Cases) {
		t.Fatalf("fixture header=%d %s accepted=%d rejected=%d cases=%d", fixture.SchemaVersion, fixture.Owner, accepted, rejected, len(fixture.Cases))
	}
	for _, item := range fixture.Cases {
		t.Run(item.ID, func(t *testing.T) {
			capture := inspect.Capture{}
			if item.Capture == "full" {
				capture = fullCapture
			} else if item.Capture != "none" {
				t.Fatalf("capture %q", item.Capture)
			}
			live := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: capture, Hub: event.Config{LateWindow: 20 * time.Millisecond}}, nodes: map[string]node.Any{"test/gate": (&gateNode{}).node(t)}})
			got := awaitResult(t, live.start(item.ID, item.Run.Principal, quote.Input{SKU: item.Run.SKU, Quantity: 2}))
			if got[0] != item.Expected.RunHTTP {
				t.Fatalf("run HTTP=%v want %s", got, item.Expected.RunHTTP)
			}
			cursor := item.Read.Cursor
			if cursor == "malformed" {
				cursor = "not-a-cursor"
			}
			response, body, stream := openStream(t, context.Background(), live.eventsURL(item.ID), item.Read.Principal, cursor)
			if response.StatusCode != item.Expected.StreamStatus {
				t.Fatalf("stream status=%d body=%s want %d", response.StatusCode, body, item.Expected.StreamStatus)
			}
			var frames []sseFrame
			if stream != nil {
				frames = stream.rest(t, 5*time.Second)
			}
			if names := frameNames(frames); strings.Join(names, ",") != strings.Join(item.Expected.Frames, ",") {
				t.Fatalf("frames=%v\nwant  =%v", names, item.Expected.Frames)
			}
			payloads, leaks, code := 0, 0, ""
			for _, frame := range frames {
				if strings.Contains(frame.Data, "synthetic-token-value") || strings.Contains(frame.Data, `"alice"`) {
					leaks++
				}
				if frame.Event == "end" {
					continue
				}
				value := frame.decode(t)
				if value.Input != nil || value.Output != nil || value.LogAttrs != nil {
					payloads++
				}
				if strings.HasPrefix(frame.Event, "run.") && value.ErrorCode != "" {
					code = value.ErrorCode
				}
			}
			if payloads != item.Expected.PayloadFrames || leaks != item.Expected.SecretLeaks || code != item.Expected.ErrorCode || live.catalog.calls.Load() != item.Expected.Effects {
				t.Fatalf("payloadFrames=%d leaks=%d errorCode=%q effects=%d; want %d %d %q %d", payloads, leaks, code, live.catalog.calls.Load(), item.Expected.PayloadFrames, item.Expected.SecretLeaks, item.Expected.ErrorCode, item.Expected.Effects)
			}
		})
	}
}
