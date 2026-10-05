package slo

import (
	"slices"
	"strings"
)

// Kind is a metric instrument kind.
type Kind string

const (
	Counter   Kind = "counter"
	Gauge     Kind = "gauge"
	Histogram Kind = "histogram"
)

// Counts names what a metric counts. The four request-path counts are kept
// apart on purpose: one accepted request can complete one run made of many
// steps, a few of which call out of the process (ADR 0022).
type Counts string

const (
	CountsAdmission  Counts = "admission"      // requests offered at the edge, accepted or rejected
	CountsCompletion Counts = "completion"     // runs reaching an outcome
	CountsSteps      Counts = "steps"          // step attempts
	CountsExternal   Counts = "external_calls" // attempts of steps whose node declares effects
	CountsState      Counts = "state"          // a point-in-time census or level
	CountsTelemetry  Counts = "telemetry"      // the monitoring pipeline itself
)

// Path is where a metric is produced.
type Path string

const (
	// PathSnapshot metrics come from a Snapshot: app/deploy renders them on
	// /metrics with the standard library, and observe/otel exports them.
	PathSnapshot Path = "snapshot"
	// PathEvents metrics are derived from engine observation events by the
	// optional observe/otel module only.
	PathEvents Path = "events"
)

// Label is one metric attribute. Values is its closed vocabulary when it has
// one; otherwise Bound is the most distinct values it can take, and zero
// means application-defined names capped by the exporter's per-instrument
// series bound.
type Label struct {
	Name       string   `json:"name"`
	Prometheus string   `json:"prometheus"`
	Values     []string `json:"values,omitempty"`
	Bound      int      `json:"bound,omitempty"`
	Note       string   `json:"note,omitempty"`
}

// Metric is one catalogued instrument.
type Metric struct {
	Name        string  `json:"name"`
	Prometheus  string  `json:"prometheus"`
	Kind        Kind    `json:"kind"`
	Unit        string  `json:"unit"`
	Signal      string  `json:"signal"`
	Counts      Counts  `json:"counts"`
	Path        Path    `json:"path"`
	Labels      []Label `json:"labels"`
	Description string  `json:"description"`
	// SeriesBound is the largest number of attribute sets; zero means the
	// set is bounded only by the exporter's MaxSeries (plus one overflow set).
	SeriesBound int `json:"seriesBound"`
}

// DurationBuckets are the explicit histogram boundaries, in seconds, of every
// duration instrument. The SDK default boundaries are tuned for milliseconds
// and would put every run under five seconds in one bucket.
var DurationBuckets = []float64{0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}

// Metric names. Event-path names are the observe/otel instruments.
const (
	MetricReady            = "blok.ready"
	MetricDraining         = "blok.draining"
	MetricDependencyReady  = "blok.dependency.ready"
	MetricAdmissionActive  = "blok.admission.active"
	MetricAdmissionCap     = "blok.admission.capacity"
	MetricAdmissionReqs    = "blok.admission.requests"
	MetricWorkItems        = "blok.work.items"
	MetricWorkOldest       = "blok.work.oldest_pending.age"
	MetricWorkDead         = "blok.work.dead_letters"
	MetricCensusTruncated  = "blok.census.truncated"
	MetricTimersOverdue    = "blok.timers.overdue"
	MetricTimerLag         = "blok.timer.lag"
	MetricWorkerReady      = "blok.worker.ready"
	MetricWorkerInFlight   = "blok.worker.in_flight"
	MetricWorkerCapacity   = "blok.worker.capacity"
	MetricPartitions       = "blok.partitions"
	MetricStorageUsed      = "blok.storage.used"
	MetricStorageBudget    = "blok.storage.budget"
	MetricSampleFailures   = "blok.operational.sample.failures"
	MetricSourceUp         = "blok.source.up"
	MetricSourceAge        = "blok.source.age"
	MetricCensusTakeover   = "blok.census.takeover"
	MetricRuns             = "blok.runs"
	MetricRunDuration      = "blok.run.duration"
	MetricSteps            = "blok.steps"
	MetricStepDuration     = "blok.step.duration"
	MetricExternalCalls    = "blok.external.calls"
	MetricExternalDuration = "blok.external.call.duration"
	MetricTelemetryDropped = "blok.telemetry.dropped"
)

// Attribute keys.
const (
	AttrSource     = "blok.source"
	AttrLiveness   = "blok.liveness"
	AttrWorker     = "blok.worker"
	AttrStore      = "blok.store"
	AttrDependency = "blok.dependency"
	AttrOwned      = "blok.owned"
	AttrResult     = "blok.result"
	AttrReason     = "blok.reason"
	AttrPages      = "blok.pages"
	AttrWorkflow   = "blok.workflow"
	AttrStep       = "blok.step"
	AttrOutcome    = "blok.outcome"
	AttrTenant     = "blok.tenant"
	AttrErrorClass = "blok.error.class"
	AttrDropReason = "reason"
)

// Outcome values of run, step and external-call counters. Uncertain is its
// own outcome: it is never folded into failed.
var (
	RunOutcomes  = []string{"completed", "failed", "canceled", "uncertain", "suspended"}
	StepOutcomes = []string{"completed", "failed", "canceled", "uncertain"}
	DropReasons  = []string{"queue_full", "closed", "abandoned", "orphaned", "series_overflow", "spans_failed", "logs_failed", "metric_exports_failed"}
)

// TenantBound is the tenant label bound: observe/otel's allowlist (64) plus
// "other".
const TenantBound = 65

func names[T ~string](values []T) []string {
	out := make([]string, len(values))
	for i, v := range values {
		out[i] = string(v)
	}
	return out
}

func label(name string, values []string, bound int, note string) Label {
	return Label{Name: name, Prometheus: strings.ReplaceAll(name, ".", "_"), Values: values, Bound: bound, Note: note}
}

// Catalogue returns the metric catalogue, the single source of truth for
// names, units, labels and cardinality bounds. examples/monitoring/
// catalogue.json is generated from it and checked for drift.
func Catalogue() []Metric {
	source := label(AttrSource, nil, MaxNamed, "census source declared by the application (a queue name, cluster)")
	liveness := label(AttrLiveness, names(Livenesses), 0, "")
	workflow := label(AttrWorkflow, nil, 0, "registered workflow name; invalid names are exported as invalid")
	step := label(AttrStep, nil, 0, "step id within the workflow")
	errorClass := label(AttrErrorClass, nil, 0, "node error class; empty when the step did not fail")
	tenant := label(AttrTenant, nil, TenantBound, "allowlisted tenant (at most 64) or other; never a raw tenant on metrics")
	worker := label(AttrWorker, nil, MaxNamed, "worker runtime name declared by the application")
	store := label(AttrStore, nil, MaxNamed, "store name declared by the application")
	metrics := []Metric{
		{Name: MetricReady, Kind: Gauge, Signal: "readiness", Counts: CountsState, Path: PathSnapshot, Description: "1 when the deployment is ready for admission"},
		{Name: MetricDraining, Kind: Gauge, Signal: "readiness", Counts: CountsState, Path: PathSnapshot, Description: "1 while the deployment drains; not-ready while draining is expected"},
		{Name: MetricDependencyReady, Kind: Gauge, Signal: "readiness", Counts: CountsState, Path: PathSnapshot, Labels: []Label{label(AttrDependency, nil, MaxNamed, "artifact, store, worker, secrets")}, Description: "1 when the readiness dependency is ready"},
		{Name: MetricAdmissionActive, Kind: Gauge, Unit: "{request}", Signal: "admission_saturation", Counts: CountsState, Path: PathSnapshot, Description: "requests holding an admission slot"},
		{Name: MetricAdmissionCap, Kind: Gauge, Unit: "{request}", Signal: "admission_saturation", Counts: CountsState, Path: PathSnapshot, Description: "admission slots configured"},
		{Name: MetricAdmissionReqs, Kind: Counter, Unit: "{request}", Signal: "admission_saturation", Counts: CountsAdmission, Path: PathSnapshot, Labels: []Label{label(AttrResult, []string{"accepted", "rejected"}, 0, ""), label(AttrReason, append([]string{"none"}, names(RejectReasons)...), 0, "none when accepted")}, Description: "requests offered at the edge, by admission result"},
		{Name: MetricWorkItems, Kind: Gauge, Unit: "{item}", Signal: "queue_depth", Counts: CountsState, Path: PathSnapshot, Labels: []Label{source, liveness}, Description: "unfinished work by liveness; stalled pages, waiting never does"},
		{Name: MetricWorkOldest, Kind: Gauge, Unit: "s", Signal: "queue_depth", Counts: CountsState, Path: PathSnapshot, Labels: []Label{source}, Description: "age of the oldest pending item (backlog lag)"},
		{Name: MetricWorkDead, Kind: Gauge, Unit: "{item}", Signal: "errors", Counts: CountsState, Path: PathSnapshot, Labels: []Label{source}, Description: "dead-lettered work retained for an operator"},
		{Name: MetricCensusTruncated, Kind: Gauge, Signal: "queue_depth", Counts: CountsState, Path: PathSnapshot, Labels: []Label{label(AttrSource, nil, 2*MaxNamed, "a work source or a timer source")}, Description: "1 when the census hit its read bound; counts are lower bounds"},
		{Name: MetricCensusTakeover, Kind: Gauge, Unit: "s", Signal: "queue_depth", Counts: CountsState, Path: PathSnapshot, Labels: []Label{source}, Description: "longest normal takeover or reclaim before the census reports lost ownership; the stall alert window must cover it"},
		{Name: MetricTimersOverdue, Kind: Gauge, Unit: "{timer}", Signal: "timer_lag", Counts: CountsState, Path: PathSnapshot, Labels: []Label{source}, Description: "timers past their due time and not yet fired"},
		{Name: MetricTimerLag, Kind: Gauge, Unit: "s", Signal: "timer_lag", Counts: CountsState, Path: PathSnapshot, Labels: []Label{source}, Description: "how far past due the oldest unfired timer is"},
		{Name: MetricWorkerReady, Kind: Gauge, Signal: "worker_availability", Counts: CountsState, Path: PathSnapshot, Labels: []Label{worker}, Description: "1 when the worker runtime is negotiated and accepting calls"},
		{Name: MetricWorkerInFlight, Kind: Gauge, Unit: "{call}", Signal: "worker_availability", Counts: CountsState, Path: PathSnapshot, Labels: []Label{worker}, Description: "worker calls in flight"},
		{Name: MetricWorkerCapacity, Kind: Gauge, Unit: "{call}", Signal: "worker_availability", Counts: CountsState, Path: PathSnapshot, Labels: []Label{worker}, Description: "worker concurrent-call capacity"},
		{Name: MetricPartitions, Kind: Gauge, Unit: "{partition}", Signal: "worker_availability", Counts: CountsState, Path: PathSnapshot, Labels: []Label{label(AttrOwned, []string{"true", "false"}, 0, "")}, Description: "durable partitions by live ownership; an unowned partition claims nothing and fires no timer"},
		{Name: MetricStorageUsed, Kind: Gauge, Unit: "By", Signal: "storage_growth", Counts: CountsState, Path: PathSnapshot, Labels: []Label{store}, Description: "bytes used by the durable store"},
		{Name: MetricStorageBudget, Kind: Gauge, Unit: "By", Signal: "storage_growth", Counts: CountsState, Path: PathSnapshot, Labels: []Label{store}, Description: "operator-declared storage budget; absent when none was declared"},
		{Name: MetricSampleFailures, Kind: Counter, Unit: "{sample}", Signal: "exporter_loss", Counts: CountsTelemetry, Path: PathSnapshot, Description: "operational source samples that failed, timed out or were invalid"},
		{Name: MetricSourceUp, Kind: Gauge, Signal: "freshness", Counts: CountsTelemetry, Path: PathSnapshot, Labels: []Label{label(AttrSource, nil, MaxSources, "operational source name"), label(AttrPages, []string{"true", "false"}, 0, "false only for informational sources")}, Description: "1 when the source answered this sample; every source is always reported"},
		{Name: MetricSourceAge, Kind: Gauge, Unit: "s", Signal: "freshness", Counts: CountsTelemetry, Path: PathSnapshot, Labels: []Label{label(AttrSource, nil, MaxSources, "operational source name"), label(AttrPages, []string{"true", "false"}, 0, "")}, Description: "seconds since the source last answered; 0 while it answers"},
		{Name: MetricRuns, Kind: Counter, Unit: "{run}", Signal: "errors", Counts: CountsCompletion, Path: PathEvents, Labels: []Label{workflow, label(AttrOutcome, RunOutcomes, 0, "uncertain is never folded into failed; every outcome is created at zero on a workflow and tenant's first run"), tenant}, Description: "workflow runs by outcome"},
		{Name: MetricRunDuration, Kind: Histogram, Unit: "s", Signal: "latency", Counts: CountsCompletion, Path: PathEvents, Labels: []Label{workflow, label(AttrOutcome, RunOutcomes, 0, ""), tenant}, Description: "observed run duration"},
		{Name: MetricSteps, Kind: Counter, Unit: "{step}", Signal: "errors", Counts: CountsSteps, Path: PathEvents, Labels: []Label{workflow, step, label(AttrOutcome, StepOutcomes, 0, "uncertain is never folded into failed"), errorClass}, Description: "step attempts by outcome"},
		{Name: MetricStepDuration, Kind: Histogram, Unit: "s", Signal: "latency", Counts: CountsSteps, Path: PathEvents, Labels: []Label{workflow, step, label(AttrOutcome, StepOutcomes, 0, "")}, Description: "observed step attempt duration"},
		{Name: MetricExternalCalls, Kind: Counter, Unit: "{call}", Signal: "uncertainty", Counts: CountsExternal, Path: PathEvents, Labels: []Label{workflow, step, label(AttrOutcome, StepOutcomes, 0, "uncertain: the effect may have happened; every outcome is created at zero on the step's first call")}, Description: "attempts of steps whose node declares effects (error classes are on blok.steps)"},
		{Name: MetricExternalDuration, Kind: Histogram, Unit: "s", Signal: "latency", Counts: CountsExternal, Path: PathEvents, Labels: []Label{workflow, step, label(AttrOutcome, StepOutcomes, 0, "")}, Description: "external call duration"},
		{Name: MetricTelemetryDropped, Kind: Counter, Unit: "{event}", Signal: "exporter_loss", Counts: CountsTelemetry, Path: PathEvents, Labels: []Label{label(AttrDropReason, DropReasons, 0, "")}, Description: "telemetry the exporter discarded, by reason"},
	}
	for i := range metrics {
		m := &metrics[i]
		m.Prometheus = PrometheusName(m.Name, m.Unit, m.Kind)
		if m.Labels == nil {
			m.Labels = []Label{}
		}
		m.SeriesBound = seriesBound(m.Labels)
	}
	return metrics
}

func seriesBound(labels []Label) int {
	bound := 1
	for _, l := range labels {
		switch {
		case len(l.Values) > 0:
			bound *= len(l.Values)
		case l.Bound > 0:
			bound *= l.Bound
		default:
			return 0
		}
	}
	return bound
}

// Lookup returns the catalogued metric with the OpenTelemetry name.
func Lookup(name string) (Metric, bool) {
	catalogue := Catalogue()
	i := slices.IndexFunc(catalogue, func(m Metric) bool { return m.Name == name })
	if i < 0 {
		return Metric{}, false
	}
	return catalogue[i], true
}

// PrometheusName is the exposition name of an instrument under the
// OpenTelemetry-to-Prometheus compatibility rules the example collector
// config selects (translation_strategy UnderscoreEscapingWithSuffixes):
// dots become underscores, a unit in braces is dropped, s and By add
// _seconds and _bytes, and a monotonic counter adds _total. A histogram's
// name is the base of its _bucket, _sum and _count series.
func PrometheusName(name, unit string, kind Kind) string {
	base := strings.ReplaceAll(name, ".", "_")
	switch unit {
	case "s":
		base += "_seconds"
	case "By":
		base += "_bytes"
	}
	if kind == Counter {
		base += "_total"
	}
	return base
}
