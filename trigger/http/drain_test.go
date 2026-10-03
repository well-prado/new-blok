package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/drainprobe"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
)

// TestDrainTimeoutCancelsARequest: when the drain times out under a
// request, its handler is canceled and stops before the application closes
// its dependencies, and the caller is told to retry (#177).
func TestDrainTimeoutCancelsARequest(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	server, err := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/work", Timeout: 30 * time.Second, Handle: func(ctx context.Context, _ blokhttp.Input) (any, error) {
		return nil, work.Run(ctx)
	}}})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(server)
	defer web.Close()
	type answer struct {
		status int
		retry  string
		body   map[string]any
		err    error
	}
	answered := make(chan answer, 1)
	go func() {
		response, err := web.Client().Post(web.URL+"/work", "application/json", strings.NewReader(`{}`))
		if err != nil {
			answered <- answer{err: err}
			return
		}
		defer response.Body.Close()
		var body map[string]any
		_ = json.NewDecoder(response.Body).Decode(&body)
		answered <- answer{status: response.StatusCode, retry: response.Header.Get("Retry-After"), body: body}
	}()
	drainprobe.Abort(t, application, probe, work)
	got := <-answered
	if got.err != nil || got.status != http.StatusServiceUnavailable || got.retry != "1" || got.body["error"] != "application unavailable" {
		t.Fatalf("request answered %+v; want 503 application unavailable with Retry-After", got)
	}
}
