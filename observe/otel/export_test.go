package otel_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/observe/otel"
	"github.com/well-prado/new-blok/observe/slo"
)

type fixtureCase struct {
	ID       string     `json:"id"`
	Ratio    float64    `json:"ratio"`
	Tenant   string     `json:"tenant"`
	Input    orderInput `json:"input"`
	Expected struct {
		Outputs     int               `json:"outputs"`
		Errors      int               `json:"errors"`
		Effects     int64             `json:"effects"`
		ErrorCode   string            `json:"errorCode"`
		RunOutcome  string            `json:"runOutcome"`
		Spans       int               `json:"spans"`
		ErrorSpans  int               `json:"errorSpans"`
		Logs        int               `json:"logs"`
		TenantLabel string            `json:"tenantLabel"`
		Steps       map[string]string `json:"steps"`
	} `json:"expected"`
}

func loadCases(t *testing.T) []fixtureCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Cases []fixtureCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	if len(file.Cases) < 6 {
		t.Fatalf("fixture lost cases: %d", len(file.Cases))
	}
	return file.Cases
}

// errorCode reads the classified code of a run error, as triggers do.
func errorCode(err error) string {
	var classified interface{ ErrorCode() string }
	if errors.As(err, &classified) {
		return classified.ErrorCode()
	}
	return ""
}

// TestFixtureCasesExportThroughRealOTLP runs every synthetic case through the
// real engine and the real OTLP/HTTP exporters, and requires the business
// result, the effect count and what the collector received to match the
// predeclared expectation.
func TestFixtureCasesExportThroughRealOTLP(t *testing.T) {
	for _, tc := range loadCases(t) {
		t.Run(tc.ID, func(t *testing.T) {
			h := newHarness(t, harnessOptions{ratio: tc.Ratio})
			result, err := h.run(t, "run-"+tc.ID, tc.Tenant, tc.Input)
			outputs, errorsSeen := 0, 0
			if err == nil {
				outputs = 1
				if out, ok := result.Output.(orderOutput); !ok || out.TotalCents != int64(tc.Input.Quantity)*1500 {
					t.Fatalf("output %#v", result.Output)
				}
			} else {
				errorsSeen = 1
			}
			if outputs != tc.Expected.Outputs || errorsSeen != tc.Expected.Errors || h.effects.Load() != tc.Expected.Effects || errorCode(err) != tc.Expected.ErrorCode {
				t.Fatalf("outputs=%d errors=%d effects=%d code=%q err=%v", outputs, errorsSeen, h.effects.Load(), errorCode(err), err)
			}
			h.flush(t)
			spans, _, logs := h.collector.snapshot()
			if len(spans) != tc.Expected.Spans || len(logs) != tc.Expected.Logs {
				t.Fatalf("spans=%d logs=%d, want %d/%d", len(spans), len(logs), tc.Expected.Spans, tc.Expected.Logs)
			}
			errorSpans := 0
			for _, span := range spans {
				if span.Status.GetCode() == tracepb.Status_STATUS_CODE_ERROR {
					errorSpans++
				}
			}
			if errorSpans != tc.Expected.ErrorSpans {
				t.Fatalf("error spans=%d, want %d", errorSpans, tc.Expected.ErrorSpans)
			}
			if tc.Expected.Spans > 0 {
				assertOneTree(t, spans, "run-"+tc.ID, tc.Expected.RunOutcome, tc.Expected.Steps)
			}
			runs := h.collector.latest(otel.MetricRuns)
			want := key(map[string]string{otel.AttrWorkflow: "shop/order", otel.AttrOutcome: tc.Expected.RunOutcome, otel.AttrTenant: tc.Expected.TenantLabel})
			// Every outcome of the workflow and tenant exists from its first
			// run, at zero (ADR 0022); exactly one is incremented.
			counted := 0
			for _, point := range runs {
				if point.value != 0 {
					counted++
				}
			}
			if len(runs) != len(slo.RunOutcomes) || counted != 1 || runs[want].value != 1 {
				t.Fatalf("runs metric %+v, want every outcome and one point %s=1", runs, want)
			}
			steps := h.collector.latest(otel.MetricSteps)
			if len(steps) != len(tc.Expected.Steps) {
				t.Fatalf("step series %+v, want %v", steps, tc.Expected.Steps)
			}
			for step, outcome := range tc.Expected.Steps {
				found := false
				for _, point := range steps {
					if point.attrs[otel.AttrStep] == step && point.attrs[otel.AttrOutcome] == outcome && point.value == 1 {
						found = true
					}
				}
				if !found {
					t.Fatalf("no %s=%s step point in %+v", step, outcome, steps)
				}
			}
			for _, record := range logs {
				attributes := attrs(record.Attributes)
				if record.Body.GetStringValue() != "charging" || attributes["sku"] != tc.Input.SKU || attributes[otel.AttrStep] != "charge" || len(record.TraceId) != 16 || len(record.SpanId) != 8 {
					t.Fatalf("log record %+v", record)
				}
				if _, leaked := attributes["card_token"]; leaked {
					t.Fatal("an attribute outside LogAttributes was exported")
				}
			}
			for _, sentinel := range []string{principalSentinel, payloadSentinel, cardTokenSentinel, `"quantity"`} {
				if h.collector.contains(sentinel) {
					t.Fatalf("collector received %q", sentinel)
				}
			}
			if stats := h.exporter.Stats(); stats.Dropped != 0 || stats.SpansFailed != 0 || stats.LogsFailed != 0 || stats.Panics != 0 || stats.Orphaned != 0 {
				t.Fatalf("stats %+v", stats)
			}
		})
	}
}

// assertOneTree checks the exported spans form one trace rooted at the run.
func assertOneTree(t *testing.T, spans []*tracepb.Span, runID, outcome string, steps map[string]string) {
	t.Helper()
	var root *tracepb.Span
	for _, span := range spans {
		if len(span.ParentSpanId) == 0 {
			if root != nil {
				t.Fatal("two root spans")
			}
			root = span
		}
	}
	if root == nil || root.Name != "run shop/order" || attrs(root.Attributes)[otel.AttrRunID] != runID || attrs(root.Attributes)[otel.AttrOutcome] != outcome {
		t.Fatalf("root %+v", root)
	}
	for _, span := range spans {
		if hex.EncodeToString(span.TraceId) != hex.EncodeToString(root.TraceId) {
			t.Fatal("span outside the run's trace")
		}
		if span == root {
			continue
		}
		step := attrs(span.Attributes)[otel.AttrStep]
		if hex.EncodeToString(span.ParentSpanId) != hex.EncodeToString(root.SpanId) || span.Name != "step "+step || attrs(span.Attributes)[otel.AttrOutcome] != steps[step] {
			t.Fatalf("step span %s: %+v", step, span)
		}
		if span.StartTimeUnixNano > span.EndTimeUnixNano || span.StartTimeUnixNano < root.StartTimeUnixNano || span.EndTimeUnixNano > root.EndTimeUnixNano {
			t.Fatalf("step span %s is not inside its run", step)
		}
	}
}

// TestUnselectedSignalsAreNeverExported selects metrics only: no trace or log
// request reaches the collector even with full sampling.
func TestUnselectedSignalsAreNeverExported(t *testing.T) {
	h := newHarness(t, harnessOptions{ratio: 1, signals: signals{metrics: true}})
	if _, err := h.run(t, "run-metrics-only", "tenant-a", orderInput{SKU: "coffee", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	h.flush(t)
	if h.collector.requests("/v1/traces") != 0 || h.collector.requests("/v1/logs") != 0 || h.collector.requests("/v1/metrics") == 0 {
		t.Fatalf("requests traces=%d logs=%d metrics=%d", h.collector.requests("/v1/traces"), h.collector.requests("/v1/logs"), h.collector.requests("/v1/metrics"))
	}
	if _, err := otel.New(otel.Config{}); !errors.Is(err, otel.ErrConfig) {
		t.Fatalf("an exporter selecting nothing was built: %v", err)
	}
}

// TestMetricLabelsAreBoundedAndAllowlisted drives more distinct step and
// tenant values than the series bound and checks every exported label key is
// from the fixed vocabulary, unlisted tenants collapse to "other", and the
// overflow is counted.
func TestMetricLabelsAreBoundedAndAllowlisted(t *testing.T) {
	const bound = 5
	h := newHarness(t, harnessOptions{ratio: 1, signals: signals{metrics: true}, tune: func(c *otel.Config) { c.MaxSeries = bound }})
	for i := 0; i < 40; i++ {
		tenant := "tenant-" + strings.Repeat("z", i%20+1) // 20 distinct unlisted tenants
		if i%2 == 0 {
			tenant = "tenant-a"
		}
		sku := []string{"coffee", "declined", "uncertain"}[i%3]
		_, _ = h.run(t, "run-cardinality-"+string(rune('a'+i%26))+strings.Repeat("x", i), tenant, orderInput{SKU: sku, Quantity: 1})
	}
	// A user-supplied tenant that is not a valid label is still only "other".
	_, _ = h.run(t, "run-cardinality-email", "user@example.test", orderInput{SKU: "coffee", Quantity: 1})
	h.flush(t)
	allowed := map[string]bool{otel.AttrWorkflow: true, otel.AttrStep: true, otel.AttrOutcome: true, otel.AttrTenant: true, otel.AttrErrorClass: true, otel.AttrOverflow: true, otel.AttrDropReason: true}
	_, points, _ := h.collector.snapshot()
	series := map[string]map[string]bool{}
	for _, point := range points {
		for key, value := range point.attrs {
			if !allowed[key] {
				t.Fatalf("metric %s exported label %q", point.name, key)
			}
			if key == otel.AttrTenant && value != "tenant-a" && value != "other" {
				t.Fatalf("tenant label %q escaped the allowlist", value)
			}
			if strings.Contains(value, "run-") || strings.Contains(value, "@") {
				t.Fatalf("unbounded value %q exported as a label", value)
			}
		}
		if series[point.name] == nil {
			series[point.name] = map[string]bool{}
		}
		series[point.name][key(point.attrs)] = true
	}
	for name, sets := range series {
		if name != otel.MetricDropped && len(sets) > bound+1 {
			t.Fatalf("%s exported %d series, bound %d plus overflow", name, len(sets), bound)
		}
	}
	if h.exporter.Stats().Overflowed == 0 {
		t.Fatal("the series bound was never reached; the check is vacuous")
	}
	overflow := h.collector.latest(otel.MetricRuns)[key(map[string]string{otel.AttrOverflow: "true"})]
	if overflow.value == 0 {
		t.Fatal("overflowed points were not recorded under otel.metric.overflow")
	}
	if h.effects.Load() == 0 || h.collector.contains(principalSentinel) {
		t.Fatal("cardinality run did no work or leaked the principal")
	}
}

// TestConfigBoundsAreRefused pins the hard limits.
func TestConfigBoundsAreRefused(t *testing.T) {
	c := newCollector(t)
	base := c.exporters(t, signals{metrics: true})
	for name, tune := range map[string]func(*otel.Config){
		"queue":          func(c *otel.Config) { c.QueueSize = otel.MaxQueueSize + 1 },
		"negative queue": func(c *otel.Config) { c.QueueSize = -1 },
		"open runs":      func(c *otel.Config) { c.MaxOpenRuns = otel.MaxOpenRuns + 1 },
		"series":         func(c *otel.Config) { c.MaxSeries = otel.MaxSeries + 1 },
		"batch":          func(c *otel.Config) { c.BatchSize = otel.MaxBatchSize + 1 },
		"timeout":        func(c *otel.Config) { c.ExportTimeout = otel.MaxExportTimeout + time.Second },
		"tenant label":   func(c *otel.Config) { c.TenantLabels = []string{"user@example.test"} },
		"other tenant":   func(c *otel.Config) { c.TenantLabels = []string{"other"} },
		"tenants":        func(c *otel.Config) { c.TenantLabels = make([]string, otel.MaxTenantLabels+1) },
		"log attribute":  func(c *otel.Config) { c.LogAttributes = []string{"blok.run.id"} },
	} {
		config := base
		tune(&config)
		if _, err := otel.New(config); !errors.Is(err, otel.ErrConfig) {
			t.Fatalf("%s: err=%v", name, err)
		}
	}
	exporter, err := otel.New(base)
	if err != nil {
		t.Fatal(err)
	}
	if exporter.ObservesPayloads() {
		t.Fatal("the exporter must declare that it reads no payloads")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := exporter.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := exporter.Shutdown(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
	if !errors.Is(observe.TracePolicy{Ratio: 2}.Validate(), observe.ErrInvalidTracePolicy) {
		t.Fatal("trace policy bound")
	}
}
