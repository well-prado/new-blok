package deploy

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/deployment"
	"github.com/well-prado/new-blok/internal/tooling/promrule"
	"github.com/well-prado/new-blok/observe/slo"
)

var monitoring = filepath.Join("..", "..", "examples", "monitoring", "testdata")

func scrape(t *testing.T, d *Deployment) string {
	t.Helper()
	w := httptest.NewRecorder()
	d.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if w.Code != http.StatusOK || w.Header().Get("Content-Type") != slo.TextContentType {
		t.Fatalf("/metrics %d %q", w.Code, w.Header().Get("Content-Type"))
	}
	return w.Body.String()
}

func parse(t *testing.T, text string) []promrule.ExpositionSample {
	t.Helper()
	samples, err := promrule.ParseText(strings.NewReader(text))
	if err != nil {
		t.Fatalf("exposition: %v\n%s", err, text)
	}
	return samples
}

// checkScenario asserts the predeclared deltas of scenario id against the
// expositions this test observed and, with BLOK_RECORD_FIXTURES=1, records
// them as the fixture the monitoring rules are validated on.
func checkScenario(t *testing.T, id, source, before, after string) {
	t.Helper()
	scenarios, err := promrule.LoadScenarios(filepath.Join(monitoring, "scenarios.json"))
	if err != nil {
		t.Fatal(err)
	}
	scenario, ok := scenarios.Find(id)
	if !ok {
		t.Fatalf("scenario %s is not declared", id)
	}
	if err := scenario.Check(parse(t, before), parse(t, after)); err != nil {
		t.Fatalf("predeclared signals not observed:\n%v\nafter:\n%s", err, after)
	}
	if os.Getenv("BLOK_RECORD_FIXTURES") == "1" {
		path := filepath.Join(monitoring, "recorded", id+".prom")
		if err := promrule.WriteRecording(path, promrule.Recording{Scenario: id, Source: source, Before: before, After: after}); err != nil {
			t.Fatal(err)
		}
		t.Logf("recorded %s", path)
	}
}

// TestOverloadScenarioSignals drives real bounded admission past capacity:
// two requests hold both slots, four more are refused with 503. The
// exposition must show saturation and capacity rejections, and nothing that
// pages.
func TestOverloadScenarioSignals(t *testing.T) {
	a, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer a.Shutdown(context.Background())
	entered, release := make(chan struct{}, 8), make(chan struct{})
	d, err := NewDeployment(a, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 2, DrainTimeout: time.Second}, DeploymentChecks{Artifact: func(context.Context) error { return nil }},
		http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			<-release
			_, _ = io.WriteString(w, "ok")
		}))
	if err != nil {
		t.Fatal(err)
	}
	before := scrape(t, d)
	var held sync.WaitGroup
	for i := 0; i < 2; i++ {
		held.Add(1)
		go func() {
			defer held.Done()
			w := httptest.NewRecorder()
			d.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/orders", nil))
		}()
		<-entered
	}
	for i := 0; i < 4; i++ {
		w := httptest.NewRecorder()
		d.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/orders", nil))
		if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "1" {
			t.Fatalf("overflow request %d: %d", i, w.Code)
		}
	}
	after := scrape(t, d)
	close(release)
	held.Wait()
	// The pre-catalogue lines stay for existing scrapes.
	if !strings.Contains(after, "blok_active 2\n") || !strings.Contains(after, "blok_admission_rejected_total 4\n") {
		t.Fatalf("legacy lines changed:\n%s", after)
	}
	checkScenario(t, "overload", "app/deploy /metrics (standard library exposition)", before, after)

	// Draining refusals are a different reason from capacity.
	d.closeAdmission()
	w := httptest.NewRecorder()
	d.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/orders", nil))
	drained := parse(t, scrape(t, d))
	for _, want := range []struct {
		reason string
		value  float64
	}{{"capacity", 4}, {"draining", 1}, {"not_ready", 0}} {
		var got float64 = -1
		for _, s := range drained {
			if s.Labels["__name__"] == "blok_admission_requests_total" && s.Labels["blok_reason"] == want.reason {
				got = s.Value
			}
		}
		if got != want.value {
			t.Fatalf("rejected{%s} = %v, want %v", want.reason, got, want.value)
		}
	}
}

// TestMetricsEndpointRendersOperationalSources composes extra sources on
// /metrics: every rendered family is catalogued, a failing or stuck source
// is counted and does not hide the others, and probe errors never appear.
func TestMetricsEndpointRendersOperationalSources(t *testing.T) {
	a, _ := app.New(app.Config{})
	_ = a.Start(context.Background())
	defer a.Shutdown(context.Background())
	stuck := make(chan struct{})
	defer close(stuck)
	sources := []slo.Source{
		func(context.Context) (slo.Snapshot, error) {
			return slo.Snapshot{Workers: []slo.Worker{{Name: "node", Ready: true, InFlight: 3, Capacity: 64}}, Storage: []slo.Storage{{Name: "journal", Used: 4096, Budget: 1 << 30}}}, nil
		},
		func(context.Context) (slo.Snapshot, error) { return slo.Snapshot{}, errors.New("private-token") },
		func(ctx context.Context) (slo.Snapshot, error) { <-stuck; return slo.Snapshot{}, nil },
	}
	d, err := NewDeployment(a, deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 1, DrainTimeout: time.Second, StoreRequired: true},
		DeploymentChecks{Artifact: func(context.Context) error { return nil }, Store: func(context.Context) error { return errors.New("private-token") }, Operational: sources}, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	text := scrape(t, d)
	if elapsed := time.Since(start); elapsed > operationalSampleTimeout+time.Second {
		t.Fatalf("a stuck source held /metrics for %v", elapsed)
	}
	if strings.Contains(text, "private-token") {
		t.Fatal("a probe or source error reached /metrics")
	}
	catalogue := map[string]slo.Metric{}
	for _, m := range slo.Catalogue() {
		catalogue[m.Prometheus] = m
	}
	values := map[string]float64{}
	for _, s := range parse(t, text) {
		name := s.Labels["__name__"]
		if name == "blok_active" || name == "blok_admission_rejected_total" {
			continue
		}
		m, ok := catalogue[name]
		if !ok || m.Path != slo.PathSnapshot {
			t.Fatalf("uncatalogued series %s", s.Labels)
		}
		key := name
		for _, l := range m.Labels {
			key += "," + s.Labels[l.Prometheus]
		}
		values[key] = s.Value
	}
	for key, want := range map[string]float64{
		"blok_ready": 0, "blok_dependency_ready,store": 0, "blok_dependency_ready,artifact": 1,
		"blok_worker_in_flight,node": 3, "blok_worker_capacity,node": 64, "blok_storage_used_bytes,journal": 4096,
		"blok_storage_budget_bytes,journal": 1 << 30, "blok_operational_sample_failures_total": 2,
	} {
		if got, ok := values[key]; !ok || got != want {
			t.Errorf("%s = %v (present %v), want %v", key, got, ok, want)
		}
	}
	// The stuck source is still running: the next scrape skips it (counted)
	// instead of starting a second goroutine for it.
	if again := parse(t, scrape(t, d)); len(again) == 0 {
		t.Fatal("empty second scrape")
	}
	if failures := d.sampler.Failures(); failures != 4 {
		t.Fatalf("sample failures %d, want 4 (two per scrape)", failures)
	}
}
