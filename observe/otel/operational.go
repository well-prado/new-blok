package otel

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/well-prado/new-blok/observe/slo"
)

// operationalInstruments registers the ADR 0022 snapshot instruments and one
// callback that samples every operational source once per collection. The
// callback runs on the metric reader's goroutine: a slow source delays a
// collection (bounded by SampleTimeout), never a run. Snapshot labels are
// bounded by slo.Snapshot.Validate (at most slo.MaxNamed names per list, each
// a bounded label), so they bypass the per-instrument series map, which only
// the export goroutine may touch.
func (e *Exporter) operationalInstruments(meter metric.Meter) error {
	gauges := map[string]metric.Float64ObservableGauge{}
	counters := map[string]metric.Float64ObservableCounter{}
	var observables []metric.Observable
	for _, m := range slo.Catalogue() {
		if m.Path != slo.PathSnapshot {
			continue
		}
		options := []metric.InstrumentOption{metric.WithDescription(m.Description)}
		if m.Unit != "" {
			options = append(options, metric.WithUnit(m.Unit))
		}
		switch m.Kind {
		case slo.Gauge:
			g, err := meter.Float64ObservableGauge(m.Name, toGaugeOptions(options)...)
			if err != nil {
				return err
			}
			gauges[m.Name] = g
			observables = append(observables, g)
		case slo.Counter:
			c, err := meter.Float64ObservableCounter(m.Name, toCounterOptions(options)...)
			if err != nil {
				return err
			}
			counters[m.Name] = c
			observables = append(observables, c)
		}
	}
	_, err := meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		snapshot := e.sampler.Sample(ctx)
		gauge := func(name string, v float64, attrs ...attribute.KeyValue) {
			o.ObserveFloat64(gauges[name], v, metric.WithAttributes(attrs...))
		}
		counter := func(name string, v float64, attrs ...attribute.KeyValue) {
			o.ObserveFloat64(counters[name], v, metric.WithAttributes(attrs...))
		}
		observeSnapshot(snapshot, gauge, counter)
		return nil
	}, observables...)
	return err
}

func toGaugeOptions(options []metric.InstrumentOption) []metric.Float64ObservableGaugeOption {
	out := make([]metric.Float64ObservableGaugeOption, len(options))
	for i, o := range options {
		out[i] = o
	}
	return out
}

func toCounterOptions(options []metric.InstrumentOption) []metric.Float64ObservableCounterOption {
	out := make([]metric.Float64ObservableCounterOption, len(options))
	for i, o := range options {
		out[i] = o
	}
	return out
}

func flag(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// observeSnapshot reports s with the same series slo.WriteText renders:
// every closed label value, zeros included.
func observeSnapshot(s slo.Snapshot, gauge, counter func(string, float64, ...attribute.KeyValue)) {
	if r := s.Readiness; r != nil {
		gauge(slo.MetricReady, flag(r.Ready))
		gauge(slo.MetricDraining, flag(r.Draining))
		for _, d := range r.Dependencies {
			gauge(slo.MetricDependencyReady, flag(d.Ready), attribute.String(slo.AttrDependency, d.Name))
		}
	}
	if a := s.Admission; a != nil {
		gauge(slo.MetricAdmissionActive, float64(a.Active))
		gauge(slo.MetricAdmissionCap, float64(a.Capacity))
		counter(slo.MetricAdmissionReqs, float64(a.Accepted), attribute.String(slo.AttrResult, "accepted"), attribute.String(slo.AttrReason, "none"))
		for _, reason := range slo.RejectReasons {
			counter(slo.MetricAdmissionReqs, float64(a.Rejected[reason]), attribute.String(slo.AttrResult, "rejected"), attribute.String(slo.AttrReason, string(reason)))
		}
	}
	truncated := map[string]bool{}
	for _, w := range s.Work {
		source := attribute.String(slo.AttrSource, w.Source)
		for _, l := range slo.Livenesses {
			gauge(slo.MetricWorkItems, float64(w.Count(l)), source, attribute.String(slo.AttrLiveness, string(l)))
		}
		gauge(slo.MetricWorkOldest, w.OldestPending.Seconds(), source)
		gauge(slo.MetricWorkDead, float64(w.DeadLetters), source)
		truncated[w.Source] = w.Truncated
	}
	for _, t := range s.Timers {
		source := attribute.String(slo.AttrSource, t.Source)
		gauge(slo.MetricTimersOverdue, float64(t.Overdue), source)
		gauge(slo.MetricTimerLag, t.Lag.Seconds(), source)
		if _, ok := truncated[t.Source]; !ok {
			truncated[t.Source] = t.Truncated
		}
	}
	for source, value := range truncated {
		gauge(slo.MetricCensusTruncated, flag(value), attribute.String(slo.AttrSource, source))
	}
	for _, w := range s.Workers {
		worker := attribute.String(slo.AttrWorker, w.Name)
		gauge(slo.MetricWorkerReady, flag(w.Ready), worker)
		gauge(slo.MetricWorkerInFlight, float64(w.InFlight), worker)
		gauge(slo.MetricWorkerCapacity, float64(w.Capacity), worker)
	}
	if p := s.Partitions; p != nil {
		gauge(slo.MetricPartitions, float64(p.Owned), attribute.String(slo.AttrOwned, "true"))
		gauge(slo.MetricPartitions, float64(p.Total-p.Owned), attribute.String(slo.AttrOwned, "false"))
	}
	for _, st := range s.Storage {
		store := attribute.String(slo.AttrStore, st.Name)
		gauge(slo.MetricStorageUsed, float64(st.Used), store)
		if st.Budget > 0 {
			gauge(slo.MetricStorageBudget, float64(st.Budget), store)
		}
	}
	counter(slo.MetricSampleFailures, float64(s.SampleFailures))
}
