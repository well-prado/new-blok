package otel_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/well-prado/new-blok/observe/otel"
	"github.com/well-prado/new-blok/observe/slo"
)

// TestCardinalityBudgetHoldsForEveryInstrument drives 300 distinct tenants
// through every run outcome and checks each exported instrument against the
// budget ADR 0022 publishes: the tenant label appears only on the run
// instruments and takes at most the allowlist plus "other"; no other
// instrument varies with the tenant; snapshot instruments stay within their
// catalogued series bound.
func TestCardinalityBudgetHoldsForEveryInstrument(t *testing.T) {
	full := slo.Snapshot{
		Readiness: &slo.Readiness{Ready: true}, Admission: &slo.Admission{Capacity: 2},
		Work: []slo.Work{{Source: "orders"}, {Source: "cluster"}}, Timers: []slo.Timers{{Source: "cluster"}},
		Workers: []slo.Worker{{Name: "node"}}, Partitions: &slo.Partitions{Total: 2, Owned: 2}, Storage: []slo.Storage{{Name: "journal", Budget: 1}},
	}
	h := newHarness(t, harnessOptions{signals: signals{metrics: true}, tune: func(c *otel.Config) {
		c.Operational = []slo.Source{slo.Func("static", func(context.Context) (slo.Snapshot, error) { return full, nil })}
	}})
	skus := []string{"coffee", "declined", "uncertain"}
	for i := 0; i < 300; i++ {
		if _, err := h.run(t, fmt.Sprintf("run-budget-%d", i), fmt.Sprintf("user-%d", i), orderInput{SKU: skus[i%3], Quantity: 1}); err != nil && i%3 == 0 {
			t.Fatal(err)
		}
	}
	h.flush(t)
	_, points, _ := h.collector.snapshot()
	series := map[string]map[string]bool{}
	for _, p := range points {
		if series[p.name] == nil {
			series[p.name] = map[string]bool{}
		}
		series[p.name][key(p.attrs)] = true
		if _, tenanted := p.attrs[otel.AttrTenant]; tenanted && p.name != otel.MetricRuns && p.name != otel.MetricRunDuration {
			t.Errorf("%s carries the tenant label", p.name)
		}
		if tenant, ok := p.attrs[otel.AttrTenant]; ok && tenant != "tenant-a" && tenant != "other" {
			t.Errorf("%s exported tenant %q outside the allowlist", p.name, tenant)
		}
	}
	// Three outcomes x (allowlist of one + other) for the run counter.
	if got, budget := len(series[otel.MetricRuns]), 3*(1+1); got > budget {
		t.Errorf("%s has %d series, budget %d", otel.MetricRuns, got, budget)
	}
	for name, sets := range series {
		m, ok := slo.Lookup(name)
		if !ok {
			t.Errorf("uncatalogued instrument %s", name)
			continue
		}
		if m.SeriesBound > 0 && len(sets) > m.SeriesBound {
			t.Errorf("%s has %d series, catalogue bound %d", name, len(sets), m.SeriesBound)
		}
		if len(sets) > 1000 {
			t.Errorf("%s has %d series, over the default MaxSeries", name, len(sets))
		}
	}
	if len(series[otel.MetricExternalCalls]) != len(slo.StepOutcomes) || len(series[slo.MetricWorkItems]) != 2*len(slo.Livenesses) {
		t.Fatalf("external calls %d series, work items %d series", len(series[otel.MetricExternalCalls]), len(series[slo.MetricWorkItems]))
	}
}

// TestExternalCallsAreOnlyEffectfulSteps: the order workflow's charge step
// declares an effect, validate does not. Every run makes one external call,
// whatever its outcome, and an unconfirmed charge is an uncertain external
// call, not a failed one.
func TestExternalCallsAreOnlyEffectfulSteps(t *testing.T) {
	h := newHarness(t, harnessOptions{signals: signals{metrics: true}})
	for i, sku := range []string{"coffee", "coffee", "declined", "uncertain"} {
		_, _ = h.run(t, fmt.Sprintf("run-ext-%d", i), "tenant-a", orderInput{SKU: sku, Quantity: 1})
	}
	h.flush(t)
	byOutcome := map[string]float64{}
	for _, p := range h.collector.latest(otel.MetricExternalCalls) {
		if p.attrs[otel.AttrStep] != "charge" {
			t.Fatalf("external call recorded for step %q", p.attrs[otel.AttrStep])
		}
		byOutcome[p.attrs[otel.AttrOutcome]] += p.value
	}
	if byOutcome["completed"] != 2 || byOutcome["failed"] != 1 || byOutcome["uncertain"] != 1 {
		t.Fatalf("external calls by outcome %v, want completed 2, failed 1, uncertain 1", byOutcome)
	}
	steps := 0.0
	for _, p := range h.collector.latest(otel.MetricSteps) {
		steps += p.value
	}
	if steps <= 4 {
		t.Fatalf("steps %v: every run has more steps than external calls", steps)
	}
	var durations uint64
	for _, p := range h.collector.latest(otel.MetricExternalDuration) {
		durations += p.count
		if len(p.bounds) != len(slo.DurationBuckets) || p.bounds[len(p.bounds)-1] != 300 {
			t.Fatalf("external call buckets %v, want the catalogue's seconds buckets", p.bounds)
		}
	}
	if durations != 4 {
		t.Fatalf("external call durations %d, want 4", durations)
	}
}

// TestOperationalSourcesAreSampledPerCollection: sources are read on the
// metric reader's goroutine once per collection, a failing source is counted
// in Stats and on blok.operational.sample.failures, and Operational without a
// metric exporter is refused.
func TestOperationalSourcesAreSampledPerCollection(t *testing.T) {
	calls := 0
	h := newHarness(t, harnessOptions{signals: signals{metrics: true}, tune: func(c *otel.Config) {
		c.SampleTimeout = 100 * time.Millisecond
		c.Operational = []slo.Source{
			slo.Func("orders", func(context.Context) (slo.Snapshot, error) {
				calls++
				return slo.Snapshot{Work: []slo.Work{{Source: "orders", Stalled: 2, Waiting: 5}}}, nil
			}),
			slo.Func("cluster", func(context.Context) (slo.Snapshot, error) { return slo.Snapshot{}, errors.New("census unavailable") }),
		}
	}})
	h.flush(t)
	h.flush(t)
	if calls != 2 {
		t.Fatalf("source sampled %d times for two collections", calls)
	}
	if stats := h.exporter.Stats(); stats.SampleFailures != 2 {
		t.Fatalf("sample failures %d, want 2", stats.SampleFailures)
	}
	work := map[string]float64{}
	for _, p := range h.collector.latest(slo.MetricWorkItems) {
		work[p.attrs[slo.AttrLiveness]] = p.value
	}
	if work["stalled"] != 2 || work["waiting"] != 5 || work["uncertain"] != 0 {
		t.Fatalf("work items %v", work)
	}
	for _, p := range h.collector.latest(slo.MetricSampleFailures) {
		if p.value != 2 {
			t.Fatalf("exported sample failures %v", p.value)
		}
	}
	// The failing census is reported down, not dropped: a rule can page on it.
	up := map[string]float64{}
	for _, p := range h.collector.latest(slo.MetricSourceUp) {
		up[p.attrs[slo.AttrSource]+","+p.attrs[slo.AttrPages]] = p.value
	}
	if up["orders,true"] != 1 || up["cluster,true"] != 0 || len(up) != 2 {
		t.Fatalf("source up %v", up)
	}
	c := newCollector(t)
	config := c.exporters(t, signals{traces: true})
	config.Operational = []slo.Source{slo.Func("empty", func(context.Context) (slo.Snapshot, error) { return slo.Snapshot{}, nil })}
	if _, err := otel.New(config); !errors.Is(err, otel.ErrConfig) {
		t.Fatalf("Operational without Metrics: %v", err)
	}
}

// TestOutcomeSeriesAreCreatedAtZero: a workflow's first run creates every run
// outcome at zero, and an external step's first call every external-call
// outcome, so the first uncertain outcome after them is an increase a rate
// rule sees, not a series born at one.
func TestOutcomeSeriesAreCreatedAtZero(t *testing.T) {
	h := newHarness(t, harnessOptions{signals: signals{metrics: true}})
	if _, err := h.run(t, "run-zero-1", "tenant-a", orderInput{SKU: "coffee", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	h.flush(t)
	runs := map[string]float64{}
	for _, p := range h.collector.latest(otel.MetricRuns) {
		runs[p.attrs[otel.AttrOutcome]] = p.value
	}
	external := map[string]float64{}
	for _, p := range h.collector.latest(otel.MetricExternalCalls) {
		external[p.attrs[otel.AttrOutcome]] = p.value
	}
	if len(runs) != len(slo.RunOutcomes) || runs["uncertain"] != 0 || runs["completed"] != 1 {
		t.Fatalf("run outcomes after one run: %v", runs)
	}
	if len(external) != len(slo.StepOutcomes) || external["uncertain"] != 0 || external["completed"] != 1 {
		t.Fatalf("external outcomes after one call: %v", external)
	}
}
