package otel_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/tooling/promrule"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/otel"
	"github.com/well-prado/new-blok/observe/slo"
)

var monitoringDir = filepath.Join("..", "..", "examples", "monitoring")

// exposition renders the latest cumulative OTLP points of a metric pipeline
// as Prometheus text under the catalogue's translation (slo.PrometheusName),
// the same names the example collector config produces; the actual-collector
// test checks that equivalence.
func exposition(points []metricPoint) string {
	latest := map[string]metricPoint{}
	var order []string
	for _, p := range points {
		k := p.name + "|" + key(p.attrs)
		if _, ok := latest[k]; !ok {
			order = append(order, k)
		}
		latest[k] = p
	}
	sort.Strings(order)
	labels := func(attrs map[string]string, extra ...string) string {
		var parts []string
		for k, v := range attrs {
			parts = append(parts, strings.ReplaceAll(k, ".", "_")+"="+strconv.Quote(v))
		}
		for i := 0; i+1 < len(extra); i += 2 {
			parts = append(parts, extra[i]+"="+strconv.Quote(extra[i+1]))
		}
		sort.Strings(parts)
		if len(parts) == 0 {
			return ""
		}
		return "{" + strings.Join(parts, ",") + "}"
	}
	var b strings.Builder
	typed := map[string]bool{}
	for _, k := range order {
		p := latest[k]
		kind := slo.Gauge
		switch {
		case p.kind == "histogram":
			kind = slo.Histogram
		case p.kind == "sum" && p.monotonic:
			kind = slo.Counter
		}
		name := slo.PrometheusName(p.name, p.unit, kind)
		if !typed[name] {
			fmt.Fprintf(&b, "# TYPE %s %s\n", name, kind)
			typed[name] = true
		}
		if kind != slo.Histogram {
			fmt.Fprintf(&b, "%s%s %v\n", name, labels(p.attrs), p.value)
			continue
		}
		var cumulative uint64
		for i, count := range p.buckets {
			cumulative += count
			le := "+Inf"
			if i < len(p.bounds) {
				le = strconv.FormatFloat(p.bounds[i], 'g', -1, 64)
			}
			fmt.Fprintf(&b, "%s_bucket%s %d\n", name, labels(p.attrs, "le", le), cumulative)
		}
		fmt.Fprintf(&b, "%s_sum%s %v\n%s_count%s %d\n", name, labels(p.attrs), p.value, name, labels(p.attrs), p.count)
	}
	return b.String()
}

// scenarioTarget is where a scenario's metrics go and how they are scraped:
// the in-process OTLP receiver rendered by exposition, or an actual
// collector's Prometheus exporter.
type scenarioTarget interface {
	metrics(t *testing.T) otel.Config
	scrape(t *testing.T, service string) string
	outage(t *testing.T, down bool)
}

type inProcessTarget struct{ c *collector }

func (p *inProcessTarget) metrics(t *testing.T) otel.Config {
	p.c = newCollector(t)
	return p.c.exporters(t, signals{metrics: true})
}
func (p *inProcessTarget) scrape(*testing.T, string) string {
	_, points, _ := p.c.snapshot()
	return exposition(points)
}
func (p *inProcessTarget) outage(_ *testing.T, down bool) {
	if down {
		p.c.setMode("unavailable")
	} else {
		p.c.setMode("")
	}
}

// slowLedger is the charge step's payment store; every write first waits
// delay: the slow-store fault. (The SQLite backend is not used here to keep
// it out of this module's requirements; the delay is what is under test.)
type slowLedger struct {
	mu    sync.Mutex
	delay time.Duration
	rows  []string
}

func (l *slowLedger) write(ctx context.Context, sku string) error {
	select {
	case <-time.After(l.delay):
	case <-ctx.Done():
		return ctx.Err()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rows = append(l.rows, sku)
	return nil
}

type scenarioRecorder struct {
	t         *testing.T
	scenarios promrule.Scenarios
	record    bool
	source    string
}

func newScenarioRecorder(t *testing.T, source string) *scenarioRecorder {
	t.Helper()
	scenarios, err := promrule.LoadScenarios(filepath.Join(monitoringDir, "testdata", "scenarios.json"))
	if err != nil {
		t.Fatal(err)
	}
	return &scenarioRecorder{t: t, scenarios: scenarios, record: os.Getenv("BLOK_RECORD_FIXTURES") == "1", source: source}
}

func (r *scenarioRecorder) check(t *testing.T, id, before, after string) {
	t.Helper()
	scenario, ok := r.scenarios.Find(id)
	if !ok {
		t.Fatalf("scenario %s is not declared", id)
	}
	parse := func(text string) []promrule.ExpositionSample {
		samples, err := promrule.ParseText(strings.NewReader(text))
		if err != nil {
			t.Fatalf("%s: %v\n%s", id, err, text)
		}
		return samples
	}
	if err := scenario.Check(parse(before), parse(after)); err != nil {
		t.Fatalf("predeclared signals not observed:\n%v\nafter:\n%s", err, after)
	}
	if r.record {
		path := filepath.Join(monitoringDir, "testdata", "recorded", id+".prom")
		if err := promrule.WriteRecording(path, promrule.Recording{Scenario: id, Source: r.source, Before: before, After: after}); err != nil {
			t.Fatal(err)
		}
		t.Logf("recorded %s", path)
	}
}

func scenarioExporter(t *testing.T, target scenarioTarget, service string, tune func(*otel.Config)) *otel.Exporter {
	t.Helper()
	config := target.metrics(t)
	config.ServiceName = service
	config.TenantLabels = []string{"tenant-a"}
	config.ExportTimeout = 500 * time.Millisecond
	if tune != nil {
		tune(&config)
	}
	exporter, err := otel.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = exporter.Shutdown(ctx)
	})
	return exporter
}

func flushed(t *testing.T, exporter *otel.Exporter) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = exporter.Flush(ctx)
}

// orderRuns runs the harness order workflow once per SKU, concurrently.
func orderRuns(t *testing.T, runner *execution.Runner, program contract.InternalProgram, prefix string, skus []string) {
	t.Helper()
	var wg sync.WaitGroup
	for i, sku := range skus {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = runner.Run(context.Background(), program, orderInput{SKU: sku, Quantity: 1}, inspection.Invocation{RunID: fmt.Sprintf("%s-%d", prefix, i), Principal: principalSentinel, Tenant: "tenant-a"})
		}()
	}
	wg.Wait()
}

func repeat(sku string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = sku
	}
	return out
}

// runEventScenarios drives the event-path scenarios through target and
// checks each against its predeclared deltas.
func runEventScenarios(t *testing.T, target scenarioTarget, recorder *scenarioRecorder) {
	t.Run("slow-store", func(t *testing.T) {
		exporter := scenarioExporter(t, target, "blok-slow-store", nil)
		slow := &slowLedger{delay: 1200 * time.Millisecond}
		validate := node.MustDefine("shop/validate", "1.0.0", func(_ context.Context, in orderInput) (orderInput, error) { return in, nil },
			node.Description("Synthetic order validation"), node.Schemas(orderInputSchema, orderInputSchema), node.Pure())
		charge := node.MustDefine("shop/charge", "1.0.0", func(ctx context.Context, in orderInput) (orderOutput, error) {
			err := slow.write(ctx, in.SKU)
			return orderOutput{TotalCents: 1500, Receipt: "receipt-" + in.SKU}, err
		}, node.Description("Charge recorded in a slow store"), node.Schemas(orderInputSchema, orderOutputSchema), node.Effects("payments:charge"))
		definition, err := flow.Define(flow.Spec{Name: "shop/order", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[orderInput]) flow.Ref[orderOutput] {
			return flow.Call(b, "charge", charge, flow.Call(b, "validate", validate, in))
		})
		if err != nil {
			t.Fatal(err)
		}
		program, err := definition.Lower()
		if err != nil {
			t.Fatal(err)
		}
		application, err := app.New(app.Config{Inspection: exporter})
		if err != nil || application.Start(context.Background()) != nil {
			t.Fatal(err)
		}
		runner := execution.NewRunner(application, map[string]node.Any{"shop/validate": validate.Any(), "shop/charge": charge.Any()})
		flushed(t, exporter)
		before := target.scrape(t, "blok-slow-store")
		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := runner.Run(context.Background(), program, orderInput{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: fmt.Sprintf("slow-%d", i), Principal: principalSentinel, Tenant: "tenant-a"}); err != nil {
					t.Error(err)
				}
			}()
		}
		wg.Wait()
		flushed(t, exporter)
		recorder.check(t, "slow-store", before, target.scrape(t, "blok-slow-store"))
	})
	order := func(t *testing.T, id string, skus []string) {
		exporter := scenarioExporter(t, target, "blok-"+id, nil)
		h := &harness{exporter: exporter}
		runner, program := orderRunnerWith(t, h, exporter)
		flushed(t, exporter)
		before := target.scrape(t, "blok-"+id)
		orderRuns(t, runner, program, id, skus)
		flushed(t, exporter)
		recorder.check(t, id, before, target.scrape(t, "blok-"+id))
	}
	t.Run("uncertain-distinct", func(t *testing.T) {
		order(t, "uncertain-distinct", append(repeat("coffee", 18), "uncertain", "uncertain"))
	})
	t.Run("errors", func(t *testing.T) {
		order(t, "errors", append(repeat("coffee", 15), repeat("declined", 5)...))
	})
	t.Run("uncertain-single-run", func(t *testing.T) {
		exporter := scenarioExporter(t, target, "blok-uncertain-single-run", nil)
		h := &harness{exporter: exporter}
		runner, program := orderRunnerWith(t, h, exporter)
		orderRuns(t, runner, program, "single-ok", repeat("coffee", 5))
		flushed(t, exporter)
		before := target.scrape(t, "blok-uncertain-single-run")
		orderRuns(t, runner, program, "single-uncertain", []string{"uncertain"})
		flushed(t, exporter)
		recorder.check(t, "uncertain-single-run", before, target.scrape(t, "blok-uncertain-single-run"))
	})
	t.Run("uncertain-first-run", func(t *testing.T) {
		exporter := scenarioExporter(t, target, "blok-uncertain-first-run", nil)
		h := &harness{exporter: exporter}
		runner, program := orderRunnerWith(t, h, exporter)
		flushed(t, exporter)
		before := target.scrape(t, "blok-uncertain-first-run")
		orderRuns(t, runner, program, "first-uncertain", []string{"uncertain"})
		flushed(t, exporter)
		recorder.check(t, "uncertain-first-run", before, target.scrape(t, "blok-uncertain-first-run"))
	})
	t.Run("collector-outage", func(t *testing.T) {
		exporter := scenarioExporter(t, target, "blok-collector-outage", func(c *otel.Config) { c.MetricInterval = 100 * time.Millisecond })
		h := &harness{exporter: exporter}
		runner, program := orderRunnerWith(t, h, exporter)
		flushed(t, exporter)
		before := target.scrape(t, "blok-collector-outage")
		orderRuns(t, runner, program, "outage-up", repeat("coffee", 2))
		flushed(t, exporter)
		target.outage(t, true)
		deadline := time.Now().Add(20 * time.Second)
		for exporter.Stats().MetricFailures < 3 && time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
		}
		orderRuns(t, runner, program, "outage-down", repeat("coffee", 2))
		failures := exporter.Stats().MetricFailures
		if failures < 3 {
			t.Fatalf("metric export failures %d during the outage", failures)
		}
		target.outage(t, false)
		flushed(t, exporter)
		after := target.scrape(t, "blok-collector-outage")
		recorder.check(t, "collector-outage", before, after)
		// The loss is reported by the next successful export: the counter
		// carries at least every failure counted before the collector
		// returned, and runs were never affected.
		samples, _ := promrule.ParseText(strings.NewReader(after))
		for _, s := range samples {
			if s.Labels["__name__"] == "blok_telemetry_dropped_total" && s.Labels["reason"] == "metric_exports_failed" && s.Value < float64(failures) {
				t.Fatalf("exported %v metric export failures, counted %d", s.Value, failures)
			}
		}
		if h.effects.Load() != 4 {
			t.Fatalf("effects %d, want 4", h.effects.Load())
		}
	})
}

// TestScenarioSignalsInProcess runs the event-path scenarios through the real
// OTLP/HTTP exporter into the in-process receiver; no container is needed.
func TestScenarioSignalsInProcess(t *testing.T) {
	recorder := newScenarioRecorder(t, "")
	recorder.record = false // recordings come from the actual collector
	runEventScenarios(t, &inProcessTarget{}, recorder)
}

var errScrape = errors.New("scrape failed")

func execCommand(name string, args ...string) (string, error) {
	output, err := exec.Command(name, args...).CombinedOutput()
	return string(output), err
}

func httpGet(url string) (string, error) {
	response, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK {
		return "", errors.Join(errScrape, err)
	}
	return string(body), nil
}

// exampleCollector is an actual collector container running the example
// configuration examples/monitoring/otel-collector.yaml unchanged.
type exampleCollector struct {
	name, otlp, prometheus, health string
}

func startExampleCollector(t *testing.T) *exampleCollector {
	t.Helper()
	image := os.Getenv("BLOK_OTEL_COLLECTOR_IMAGE")
	if image == "" {
		t.Skip("set BLOK_OTEL_COLLECTOR_IMAGE (for example otel/opentelemetry-collector:0.162.0) to run against an actual collector container")
	}
	config, err := filepath.Abs(filepath.Join(monitoringDir, "otel-collector.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	// The example config validates, and a broken copy does not: the
	// validation can fail.
	docker(t, "run", "--rm", "-v", config+":/etc/otelcol/config.yaml:ro", image, "validate", "--config=/etc/otelcol/config.yaml")
	data, _ := os.ReadFile(config)
	broken := filepath.Join(t.TempDir(), "broken.yaml")
	_ = os.WriteFile(broken, []byte(strings.Replace(string(data), "exporters: [prometheus]", "exporters: [prometheus, missing]", 1)), 0o644)
	if output, err := execCommand("docker", "run", "--rm", "-v", broken+":/etc/otelcol/config.yaml:ro", image, "validate", "--config=/etc/otelcol/config.yaml"); err == nil {
		t.Fatalf("a config naming an undefined exporter validated:\n%s", output)
	}
	suffix := strconv.FormatInt(time.Now().UnixNano()%1_000_000, 36)
	c := &exampleCollector{name: "blok-otel-e16t03-" + suffix}
	otlpPort, promPort, healthPort := freePort(t), freePort(t), freePort(t)
	c.otlp = fmt.Sprintf("http://127.0.0.1:%d", otlpPort)
	c.prometheus = fmt.Sprintf("http://127.0.0.1:%d/metrics", promPort)
	c.health = fmt.Sprintf("http://127.0.0.1:%d/", healthPort)
	docker(t, "run", "-d", "--name", c.name, "-p", fmt.Sprintf("127.0.0.1:%d:4318", otlpPort), "-p", fmt.Sprintf("127.0.0.1:%d:8889", promPort), "-p", fmt.Sprintf("127.0.0.1:%d:13133", healthPort), "-v", config+":/etc/otelcol/config.yaml:ro", image, "--config", "/etc/otelcol/config.yaml")
	t.Cleanup(func() { _, _ = execCommand("docker", "rm", "-f", c.name) })
	c.waitReady(t)
	return c
}

func (c *exampleCollector) waitReady(t *testing.T) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if _, err := httpGet(c.health); err == nil {
			return
		}
	}
	t.Fatalf("collector %s never became healthy", c.name)
}

func (c *exampleCollector) metrics(t *testing.T) otel.Config {
	t.Helper()
	exporter, err := otlpmetrichttp.New(context.Background(), otlpmetrichttp.WithEndpointURL(c.otlp+"/v1/metrics"), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}))
	if err != nil {
		t.Fatal(err)
	}
	return otel.Config{Metrics: exporter, MetricInterval: 10 * time.Minute}
}

// scrape returns the collector's exposition restricted to one service's
// series (job = service.name), with every TYPE comment. The example config
// batches for up to 5s, so it polls until the series have not changed for
// longer than that.
func (c *exampleCollector) scrape(t *testing.T, service string) string {
	t.Helper()
	var last string
	for deadline, stable := time.Now().Add(40*time.Second), 0; time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		text, err := httpGet(c.prometheus)
		if err != nil {
			continue
		}
		var b strings.Builder
		for _, line := range strings.Split(text, "\n") {
			if strings.HasPrefix(line, "# TYPE ") || strings.Contains(line, `job="`+service+`"`) {
				b.WriteString(line + "\n")
			}
		}
		if current := b.String(); current == last {
			if stable++; stable >= 13 {
				return current
			}
		} else {
			last, stable = current, 0
		}
	}
	return last
}

func (c *exampleCollector) outage(t *testing.T, down bool) {
	t.Helper()
	if down {
		docker(t, "stop", c.name)
		return
	}
	docker(t, "start", c.name)
	c.waitReady(t)
}

// TestScenarioSignalsThroughActualCollector runs the event-path scenarios
// against an actual collector running the example configuration, scrapes its
// Prometheus exporter, and asserts the predeclared deltas on what a
// Prometheus would see. With BLOK_RECORD_FIXTURES=1 the scrapes become the
// recorded fixtures examples/monitoring validates its rules on. It also
// checks that the collector exposes snapshot metrics under exactly the
// catalogue names and labels slo.WriteText uses.
func TestScenarioSignalsThroughActualCollector(t *testing.T) {
	collector := startExampleCollector(t)
	recorder := newScenarioRecorder(t, "observe/otel over OTLP/HTTP to "+os.Getenv("BLOK_OTEL_COLLECTOR_IMAGE")+" running examples/monitoring/otel-collector.yaml, Prometheus exporter scrape")
	runEventScenarios(t, collector, recorder)

	t.Run("snapshot-names", func(t *testing.T) {
		full := slo.Snapshot{
			Readiness:  &slo.Readiness{Ready: true, Dependencies: []slo.Dependency{{Name: "artifact", Ready: true}}},
			Admission:  &slo.Admission{Active: 1, Capacity: 2, Accepted: 3, Rejected: map[slo.RejectReason]uint64{slo.RejectCapacity: 1}},
			Work:       []slo.Work{{Source: "orders", Stalled: 1, OldestPending: 2 * time.Second}},
			Timers:     []slo.Timers{{Source: "cluster", Overdue: 1, Lag: time.Second}},
			Workers:    []slo.Worker{{Name: "node", Ready: true, InFlight: 1, Capacity: 64}},
			Partitions: &slo.Partitions{Total: 4, Owned: 3},
			Storage:    []slo.Storage{{Name: "journal", Used: 10, Budget: 100}},
		}
		exporter := scenarioExporter(t, collector, "blok-snapshot", func(c *otel.Config) {
			c.Operational = []slo.Source{slo.Func("static", func(context.Context) (slo.Snapshot, error) { return full, nil })}
		})
		flushed(t, exporter)
		scraped, err := promrule.ParseText(strings.NewReader(collector.scrape(t, "blok-snapshot")))
		if err != nil {
			t.Fatal(err)
		}
		var text strings.Builder
		expected := full
		expected.Sources = []slo.SourceStatus{{Name: "static", Up: true, Pages: true}}
		if err := slo.WriteText(&text, expected); err != nil {
			t.Fatal(err)
		}
		written, _ := promrule.ParseText(strings.NewReader(text.String()))
		shape := func(samples []promrule.ExpositionSample, extra map[string]bool) map[string]float64 {
			out := map[string]float64{}
			for _, s := range samples {
				if !strings.HasPrefix(s.Labels["__name__"], "blok_") {
					continue
				}
				var names []string
				for k, v := range s.Labels {
					if !extra[k] && k != "__name__" {
						names = append(names, k+"="+v)
					}
				}
				sort.Strings(names)
				out[s.Labels["__name__"]+"{"+strings.Join(names, ",")+"}"] = s.Value
			}
			return out
		}
		collectorLabels := map[string]bool{"job": true, "instance": true, "otel_scope_name": true, "otel_scope_version": true, "otel_scope_schema_url": true}
		got, want := shape(scraped, collectorLabels), shape(written, nil)
		for series, value := range want {
			if got[series] != value {
				t.Errorf("collector exposes %s = %v (present %v), slo.WriteText %v", series, got[series], hasKey(got, series), value)
			}
		}
		eventPath := map[string]bool{}
		for _, m := range slo.Catalogue() {
			if m.Path == slo.PathEvents {
				eventPath[m.Prometheus] = true
			}
		}
		for series := range got {
			if eventPath[series[:strings.IndexByte(series, '{')]] {
				continue // the exporter's own event-path metrics
			}
			if _, ok := want[series]; !ok {
				t.Errorf("collector exposes %s, which slo.WriteText does not", series)
			}
		}
	})
}

func hasKey(m map[string]float64, k string) bool {
	_, ok := m[k]
	return ok
}
