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
// its dependencies. It is answered as canceled (504), not as a refusal to
// retry: it may have committed. A request after that is refused with
// Retry-After (#177).
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
	if got.err != nil || got.status != http.StatusGatewayTimeout || got.retry != "" {
		t.Fatalf("request answered %+v; want 504 without Retry-After", got)
	}
	response, err := web.Client().Post(web.URL+"/work", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("Retry-After") != "1" {
		t.Fatalf("a request after shutdown: %d Retry-After %q; want 503 with Retry-After", response.StatusCode, response.Header.Get("Retry-After"))
	}
}

// TestDrainTimeoutCancelsAuthentication: authentication runs under the
// request's lease, so a drain timeout cancels it too (#177).
func TestDrainTimeoutCancelsAuthentication(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	server, err := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/work",
		Authenticate: func(request *http.Request) (blokhttp.Principal, error) {
			return blokhttp.Principal{ID: "alice"}, work.Run(request.Context())
		},
		Handle: func(context.Context, blokhttp.Input) (any, error) { return map[string]any{}, nil },
	}})
	if err != nil {
		t.Fatal(err)
	}
	web := httptest.NewServer(server)
	defer web.Close()
	answered := make(chan int, 1)
	go func() {
		response, err := web.Client().Post(web.URL+"/work", "application/json", strings.NewReader(`{}`))
		if err != nil {
			answered <- 0
			return
		}
		response.Body.Close()
		answered <- response.StatusCode
	}()
	drainprobe.Abort(t, application, probe, work)
	if status := <-answered; status != http.StatusUnauthorized {
		t.Fatalf("aborted authentication answered %d; want 401", status)
	}
}
