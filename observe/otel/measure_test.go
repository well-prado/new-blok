package otel_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/otel"
	"github.com/well-prado/new-blok/observe/slo"
)

type batchReport struct {
	P50, P95, P99, Max int64   // nanoseconds
	RSSKiB             int64   // ps -o rss= after the batch
	HeapInuseKiB       uint64  // after runtime.GC
	Samples            []int64 `json:",omitempty"`
}

type modeReport struct {
	Mode, Endpoint, GoVersion, GOOS, GOARCH string
	GOMAXPROCS, Warmup, BatchRuns           int
	Ratio                                   float64
	StartedAt, FinishedAt                   time.Time
	WarmupRSSKiB                            int64
	Batches                                 []batchReport
	PooledP50, PooledP95, PooledP99         int64
	Effects                                 int64
	Stats                                   otel.Stats
}

func rssKiB(t *testing.T) int64 {
	t.Helper()
	output, err := exec.Command("ps", "-o", "rss=", "-p", strconv.Itoa(os.Getpid())).Output()
	if err != nil {
		t.Fatal(err)
	}
	value, err := strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func percentile(sorted []int64, p float64) int64 {
	index := int(float64(len(sorted)-1) * p)
	return sorted[index]
}

// TestMeasureMode records serial per-run latency and RSS for one mode in a
// fresh process: off (no observer, no tracing), sampled (ratio 0.1) or full
// (ratio 1), the latter two exporting traces, metrics and logs over
// OTLP/HTTP to BLOK_OTEL_ENDPOINT (an actual collector) or, without it, to
// the in-process collector. It is skipped unless BLOK_OBSERVE_MEASURE names
// the mode; the report goes to BLOK_OBSERVE_REPORT.
func TestMeasureMode(t *testing.T) {
	mode := os.Getenv("BLOK_OBSERVE_MEASURE")
	if mode == "" {
		t.Skip("set BLOK_OBSERVE_MEASURE=off|sampled|full|metrics|slo (and BLOK_OBSERVE_REPORT) to measure")
	}
	// metrics and slo export metrics only, with tracing off: slo adds the
	// ADR 0022 operational sources (sampled every metric interval), so
	// slo minus metrics is the operational overhead.
	ratios := map[string]float64{"off": 0, "sampled": 0.1, "full": 1, "metrics": 0, "slo": 0}
	ratio, ok := ratios[mode]
	if !ok {
		t.Fatalf("unknown mode %q", mode)
	}
	const warmup, batches, perBatch = 500, 5, 2000
	// The charge node logs every run. Without an observer node.Logger falls
	// back to slog.Default; discard it so "off" measures no stderr I/O.
	slog.SetDefault(slog.New(slog.DiscardHandler))
	h := &harness{}
	validate, charge, program := orderDefinitions(t, h)
	config := app.Config{}
	endpoint := os.Getenv("BLOK_OTEL_ENDPOINT")
	var exporter *otel.Exporter
	if mode != "off" {
		if endpoint == "" {
			c := newCollector(t)
			endpoint = c.server.URL
		}
		ctx := context.Background()
		traces, _ := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint+"/v1/traces"), otlptracehttp.WithRetry(otlptracehttp.RetryConfig{Enabled: false}))
		metrics, _ := otlpmetrichttp.New(ctx, otlpmetrichttp.WithEndpointURL(endpoint+"/v1/metrics"), otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{Enabled: false}))
		logs, _ := otlploghttp.New(ctx, otlploghttp.WithEndpointURL(endpoint+"/v1/logs"), otlploghttp.WithRetry(otlploghttp.RetryConfig{Enabled: false}))
		var err error
		exportConfig := otel.Config{Traces: traces, Metrics: metrics, Logs: logs, MetricInterval: time.Second, LogAttributes: []string{"sku"}, TenantLabels: []string{"tenant-a"}}
		if mode == "metrics" || mode == "slo" {
			// Collect every 10ms so the metric reader (and, for slo, every
			// operational source) runs many times while runs are measured.
			exportConfig.Traces, exportConfig.Logs, exportConfig.LogAttributes = nil, nil, nil
			exportConfig.MetricInterval = 10 * time.Millisecond
		}
		if mode == "slo" {
			exportConfig.Operational = measureSources()
		}
		exporter, err = otel.New(exportConfig)
		if err != nil {
			t.Fatal(err)
		}
		config = app.Config{Inspection: exporter, Trace: observe.TracePolicy{Ratio: ratio}}
	}
	application, err := app.New(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{"shop/validate": validate.Any(), "shop/charge": charge.Any()})
	report := modeReport{Mode: mode, Endpoint: endpoint, GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), Warmup: warmup, BatchRuns: perBatch, Ratio: ratio, StartedAt: time.Now().UTC()}
	sequence := 0
	one := func() int64 {
		sequence++
		start := time.Now()
		result, err := runner.Run(context.Background(), program, orderInput{SKU: "coffee", Quantity: 2}, inspection.Invocation{RunID: fmt.Sprintf("run-%s-%d", mode, sequence), Principal: "measure", Tenant: "tenant-a"})
		elapsed := time.Since(start).Nanoseconds()
		if err != nil || result.Output.(orderOutput).TotalCents != 3000 {
			t.Fatalf("run %d: %v", sequence, err)
		}
		return elapsed
	}
	for i := 0; i < warmup; i++ {
		one()
	}
	report.WarmupRSSKiB = rssKiB(t)
	var pooled []int64
	for b := 0; b < batches; b++ {
		samples := make([]int64, perBatch)
		for i := range samples {
			samples[i] = one()
		}
		pooled = append(pooled, samples...)
		sorted := slices.Clone(samples)
		slices.Sort(sorted)
		var memory runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&memory)
		report.Batches = append(report.Batches, batchReport{P50: percentile(sorted, 0.5), P95: percentile(sorted, 0.95), P99: percentile(sorted, 0.99), Max: sorted[len(sorted)-1], RSSKiB: rssKiB(t), HeapInuseKiB: memory.HeapInuse / 1024, Samples: samples})
	}
	slices.Sort(pooled)
	report.PooledP50, report.PooledP95, report.PooledP99 = percentile(pooled, 0.5), percentile(pooled, 0.95), percentile(pooled, 0.99)
	report.Effects = h.effects.Load()
	if exporter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := exporter.Shutdown(ctx); err != nil {
			t.Logf("shutdown: %v", err)
		}
		report.Stats = exporter.Stats()
	}
	report.FinishedAt = time.Now().UTC()
	if report.Effects != int64(warmup+batches*perBatch) {
		t.Fatalf("effects %d", report.Effects)
	}
	t.Logf("%s: p50=%dns p95=%dns p99=%dns rss=%dKiB stats=%+v", mode, report.PooledP50, report.PooledP95, report.PooledP99, report.Batches[len(report.Batches)-1].RSSKiB, report.Stats)
	if path := os.Getenv("BLOK_OBSERVE_REPORT"); path != "" {
		encoded, err := json.MarshalIndent(report, "", " ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// measureSources are representative operational sources for the slo mode: a
// deployment-sized readiness and admission snapshot, and a census that
// classifies 1000 in-memory work items on every sample.
func measureSources() []slo.Source {
	items := make([]slo.Observation, 1000)
	for i := range items {
		items[i] = slo.Observation{Claimed: i%3 == 0, OwnerLive: i%50 != 0, Waiting: i%7 == 0}
	}
	return []slo.Source{
		func(context.Context) (slo.Snapshot, error) {
			return slo.Snapshot{
				Readiness: &slo.Readiness{Ready: true, Dependencies: []slo.Dependency{{Name: "artifact", Ready: true}, {Name: "store", Ready: true}}},
				Admission: &slo.Admission{Active: 1, Capacity: 32, Accepted: 10, Rejected: map[slo.RejectReason]uint64{slo.RejectCapacity: 1}},
				Workers:   []slo.Worker{{Name: "node", Ready: true, InFlight: 1, Capacity: 64}},
				Storage:   []slo.Storage{{Name: "journal", Used: 1 << 20, Budget: 1 << 30}},
			}, nil
		},
		func(context.Context) (slo.Snapshot, error) {
			work := slo.Work{Source: "orders"}
			for _, item := range items {
				work.Add(slo.Classify(item))
			}
			return slo.Snapshot{Work: []slo.Work{work}, Timers: []slo.Timers{{Source: "orders"}}, Partitions: &slo.Partitions{Total: 8, Owned: 8}}, nil
		},
	}
}
