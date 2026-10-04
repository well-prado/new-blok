package webhook_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/drainprobe"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/webhook"
)

type acceptAll struct{}

func (acceptAll) Verify(request webhook.Request) (webhook.Verified, error) {
	return webhook.Verified{EventID: "event-1", Timestamp: request.Received, KeyID: "k"}, nil
}

type heldSubmitter struct{ work *drainprobe.Held }

func (h heldSubmitter) Submit(ctx context.Context, _ trigger.Submission) (bool, error) {
	return true, h.work.Run(ctx)
}

// TestDrainTimeoutCancelsADelivery: when the drain times out under a
// delivery's submission, the submission is canceled and stops before the
// application closes its dependencies, and the provider is told to
// redeliver (#177).
func TestDrainTimeoutCancelsADelivery(t *testing.T) {
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	server, err := webhook.New(application, nil, []webhook.Endpoint{{Path: "/hooks", Provider: "acme", Principal: trigger.Principal{ID: "acme"}, Kind: "event", Verifier: acceptAll{}, Submit: heldSubmitter{work: work}, InputSchema: []byte(`{"type":"object"}`), SubmitTimeout: 30 * time.Second}})
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
		response, err := web.Client().Post(web.URL+"/hooks", "application/json", strings.NewReader(`{}`))
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
	if got.err != nil || got.status != http.StatusServiceUnavailable || got.retry != "1" || got.body["error"] != "unavailable" {
		t.Fatalf("delivery answered %+v; want 503 unavailable with Retry-After", got)
	}
}
