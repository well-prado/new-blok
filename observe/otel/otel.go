// Package otel exports engine observations as OpenTelemetry traces, metrics
// and structured logs (ADR 0020).
//
// It lives in its own Go module so that an application that does not import
// it carries none of the OpenTelemetry SDK, its exporters or the dependency
// versions they require. The application selects each signal independently
// by passing an OpenTelemetry SDK exporter (for example otlptracehttp); a nil
// exporter means that signal is not exported. Nothing here registers global
// providers, reads ambient configuration for its pipeline, or opens a
// connection: connections belong to the exporters the application passes.
//
// An Exporter is an inspection.Observer. The engine calls Observe on the
// run's goroutine; Observe only copies a bounded event into a fixed queue and
// never blocks. A single export goroutine turns events into spans, metric
// points and log records, and exports batches synchronously with a bounded
// timeout. When the queue is full the newest event is dropped and counted;
// when an export fails its batch is dropped and counted. No configuration can
// make telemetry block, retry into, or fail a run.
package otel

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/observe/redact"
	"github.com/well-prado/new-blok/observe/slo"
)

// Bounds. A zero Config field takes the default; a value above the hard
// limit is refused by New.
const (
	DefaultQueueSize      = 4096
	MaxQueueSize          = 65536
	DefaultMaxOpenRuns    = 4096
	MaxOpenRuns           = 65536
	DefaultMaxSeries      = 1000
	MaxSeries             = 10000
	DefaultBatchSize      = 512
	MaxBatchSize          = 4096
	DefaultFlushInterval  = time.Second
	DefaultMetricInterval = 10 * time.Second
	DefaultExportTimeout  = 5 * time.Second
	MaxExportTimeout      = 30 * time.Second
	MaxTenantLabels       = 64
	MaxLogAttributes      = 32
	maxStepsPerRun        = 64
	maxLogBodyBytes       = 1024
	maxLogValueBytes      = 256
	scopeName             = "github.com/well-prado/new-blok/observe/otel"
	otherTenant           = "other"
	invalidLabel          = "invalid"
)

// Metric instrument names.
const (
	MetricRuns         = "blok.runs"
	MetricRunDuration  = "blok.run.duration"
	MetricSteps        = "blok.steps"
	MetricStepDuration = "blok.step.duration"
	MetricDropped      = "blok.telemetry.dropped"
	// MetricExternalCalls and MetricExternalDuration count and time attempts
	// of steps whose node declares effects (ADR 0022).
	MetricExternalCalls    = slo.MetricExternalCalls
	MetricExternalDuration = slo.MetricExternalDuration
	AttrWorkflow           = "blok.workflow"
	AttrStep               = "blok.step"
	AttrOutcome            = "blok.outcome"
	AttrTenant             = "blok.tenant"
	AttrErrorCode          = "blok.error.code"
	AttrErrorClass         = "blok.error.class"
	AttrRunID              = "blok.run.id"
	AttrAttempt            = "blok.attempt"
	AttrAttemptID          = "blok.attempt.id"
	AttrParentRun          = "blok.parent.run.id"
	AttrParentStep         = "blok.parent.step"
	AttrStartObserved      = "blok.start_observed"
	AttrOverflow           = "otel.metric.overflow"
	AttrDropReason         = "reason"
	defaultServiceName     = "blok"
	logSeverityFallback    = otellog.SeverityInfo
)

var ErrConfig = errors.New("otel: invalid configuration")

// Config selects the exported signals and bounds the pipeline.
type Config struct {
	// Traces, Metrics and Logs are each optional and independent.
	Traces  sdktrace.SpanExporter
	Metrics sdkmetric.Exporter
	Logs    sdklog.Exporter
	// Resource describes the process; nil uses service.name=ServiceName
	// (default "blok"). No ambient environment is read for it.
	Resource    *resource.Resource
	ServiceName string

	// QueueSize bounds events waiting between run goroutines and the export
	// goroutine. A full queue drops the newest event (Stats.Dropped).
	QueueSize int
	// MaxOpenRuns bounds runs whose terminal event has not arrived. At the
	// bound the oldest open run is abandoned (Stats.Abandoned).
	MaxOpenRuns int
	// MaxSeries bounds distinct attribute sets per metric instrument. Later
	// sets are recorded under otel.metric.overflow=true (Stats.Overflowed).
	MaxSeries int
	// BatchSize and FlushInterval bound span and log batches.
	BatchSize     int
	FlushInterval time.Duration
	// MetricInterval is the periodic metric export interval.
	MetricInterval time.Duration
	// ExportTimeout bounds every export call.
	ExportTimeout time.Duration
	// TenantLabels allowlists tenants exported as blok.tenant on metrics,
	// spans and logs; any other tenant is exported as "other". At most
	// MaxTenantLabels.
	TenantLabels []string
	// RawTenants opts in to exporting every tenant that is a valid label
	// as-is on spans and logs (never on metrics, which always use the
	// allowlist). Leave it false unless tenant identifiers are not
	// personal data and the trace backend can index their cardinality.
	RawTenants bool
	// LogAttributes allowlists structured log attribute keys exported with
	// a step log; all other attributes are dropped. At most MaxLogAttributes.
	LogAttributes []string

	// Operational lists state sources (deployment readiness and admission, a
	// queue census, the cluster census, worker availability, storage size)
	// exported as the ADR 0022 snapshot metrics. They are sampled once per
	// metric collection, on the metric reader's goroutine, never on a run's;
	// each source is bounded by SampleTimeout and a failing one is counted
	// (blok.operational.sample.failures), never fatal. Requires Metrics. At
	// most slo.MaxSources.
	Operational []slo.Source
	// SampleTimeout bounds each operational source (default 1s).
	SampleTimeout time.Duration
}

// Stats counts what the pipeline did. Every offered event is accepted or
// counted as dropped; after Shutdown returns, every accepted event is either
// processed or counted in DroppedShutdown; every exported item is either
// exported or failed.
type Stats struct {
	Accepted        uint64 // events queued
	Dropped         uint64 // events refused because the queue was full
	DroppedClosed   uint64 // events refused after Shutdown began
	DroppedShutdown uint64 // accepted events discarded unprocessed because Shutdown's context expired
	Queued          uint64 // accepted events still waiting (Accepted = Processed + Queued + DroppedShutdown, give or take one in hand)
	Processed       uint64 // events the export goroutine handled
	Abandoned       uint64 // open runs evicted at MaxOpenRuns or steps left open at run end
	Orphaned        uint64 // terminal events whose start was never observed
	Overflowed      uint64 // metric points recorded under the overflow attribute set
	SpansExported   uint64
	SpansFailed     uint64
	LogsExported    uint64
	LogsFailed      uint64
	MetricExports   uint64
	MetricFailures  uint64
	Panics          uint64 // recovered SDK panics; the event is dropped
	SampleFailures  uint64 // operational source samples that failed (ADR 0022)
}

// Exporter is an inspection.Observer that exports to OpenTelemetry.
type Exporter struct {
	config   Config
	queue    chan inspection.Event
	stop     chan struct{}
	flushReq chan chan struct{}
	done     chan struct{}
	closed   atomic.Bool
	once     sync.Once
	// producers counts Observe calls between their closed check and their
	// send, so Shutdown can wait them out and no event is queued after the
	// loop has drained.
	producers atomic.Int64
	// exportCtx is the parent of every export call; Shutdown cancels it
	// when its own context expires, and aborted then stops the loop from
	// doing anything but counting what is left.
	exportCtx     context.Context
	cancelExports context.CancelFunc
	aborted       atomic.Bool
	// One in-flight call per signal: an exporter that ignores its context
	// leaves at most one abandoned goroutine per signal, never one per
	// batch. While it is stuck, later batches fail fast (counted).
	traceSlot, logSlot, metricSlot chan struct{}

	providersOnce sync.Once
	shutdownErr   error

	tracerProvider *sdktrace.TracerProvider
	tracer         oteltrace.Tracer
	spans          *spanBuffer
	meterProvider  *sdkmetric.MeterProvider
	metrics        *countingMetrics
	loggerProvider *sdklog.LoggerProvider
	logger         otellog.Logger
	records        *logBuffer

	runsCounter      metric.Int64Counter
	runDuration      metric.Float64Histogram
	stepsCounter     metric.Int64Counter
	stepDuration     metric.Float64Histogram
	externalCounter  metric.Int64Counter
	externalDuration metric.Float64Histogram
	sampler          *slo.Sampler
	series           map[string]map[string]struct{}
	zeroed           map[string]struct{}
	tenants          map[string]bool
	logAttributes    map[string]bool

	runs  map[string]*list.Element
	order *list.List

	accepted, dropped, droppedClosed, droppedShutdown, processed, abandoned, orphaned   atomic.Uint64
	overflowed                                                                          atomic.Uint64
	spansExported, spansFailed, logsExported, logsFailed, metricExports, metricFailures atomic.Uint64
	panics                                                                              atomic.Uint64
}

type runState struct {
	id       string
	start    time.Time
	workflow string
	tenant   string
	span     oteltrace.Span
	steps    map[string]*stepState
}

type stepState struct {
	start time.Time
	span  oteltrace.Span
}

// New validates config and builds the selected pipelines. It opens no
// connection itself; the exporters passed in decide when they connect.
func New(config Config) (*Exporter, error) {
	if err := normalize(&config); err != nil {
		return nil, err
	}
	res := config.Resource
	if res == nil {
		name := config.ServiceName
		if name == "" {
			name = defaultServiceName
		}
		res = resource.NewSchemaless(attribute.String("service.name", name))
	}
	e := &Exporter{
		config:   config,
		queue:    make(chan inspection.Event, config.QueueSize),
		stop:     make(chan struct{}),
		flushReq: make(chan chan struct{}),
		done:     make(chan struct{}),
		series:   map[string]map[string]struct{}{},
		zeroed:   map[string]struct{}{},
		tenants:  map[string]bool{},
		runs:     map[string]*list.Element{},
		order:    list.New(),

		traceSlot:  make(chan struct{}, 1),
		logSlot:    make(chan struct{}, 1),
		metricSlot: make(chan struct{}, 1),
	}
	e.exportCtx, e.cancelExports = context.WithCancel(context.Background())
	for _, tenant := range config.TenantLabels {
		e.tenants[tenant] = true
	}
	if len(config.LogAttributes) > 0 {
		e.logAttributes = map[string]bool{}
		for _, key := range config.LogAttributes {
			e.logAttributes[key] = true
		}
	}
	if config.Traces != nil {
		e.spans = &spanBuffer{exporter: config.Traces, owner: e}
		e.tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			// Sampling is decided once per trace by the engine's
			// TracePolicy; only sampled observations reach the tracer.
			sdktrace.WithSampler(sdktrace.AlwaysSample()),
			sdktrace.WithIDGenerator(engineIDs{}),
			sdktrace.WithSpanProcessor(e.spans),
		)
		e.tracer = e.tracerProvider.Tracer(scopeName)
	}
	if config.Logs != nil {
		e.records = &logBuffer{exporter: config.Logs, owner: e}
		e.loggerProvider = sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(e.records))
		e.logger = e.loggerProvider.Logger(scopeName)
	}
	if len(config.Operational) > 0 {
		sampler, err := slo.NewSampler(config.SampleTimeout, config.Operational...)
		if err != nil {
			e.cancelExports()
			return nil, fmt.Errorf("%w: %v", ErrConfig, err)
		}
		e.sampler = sampler
	}
	if config.Metrics != nil {
		e.metrics = &countingMetrics{Exporter: config.Metrics, owner: e}
		reader := sdkmetric.NewPeriodicReader(e.metrics, sdkmetric.WithInterval(config.MetricInterval), sdkmetric.WithTimeout(config.ExportTimeout))
		e.meterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
		if err := e.instruments(); err != nil {
			_ = e.meterProvider.Shutdown(context.Background())
			e.cancelExports()
			return nil, err
		}
	}
	go e.loop()
	return e, nil
}

func normalize(c *Config) error {
	if c.Traces == nil && c.Metrics == nil && c.Logs == nil {
		return fmt.Errorf("%w: select at least one of Traces, Metrics or Logs", ErrConfig)
	}
	type bound struct {
		name              string
		value             *int
		fallback, ceiling int
	}
	for _, b := range []bound{{"QueueSize", &c.QueueSize, DefaultQueueSize, MaxQueueSize}, {"MaxOpenRuns", &c.MaxOpenRuns, DefaultMaxOpenRuns, MaxOpenRuns}, {"MaxSeries", &c.MaxSeries, DefaultMaxSeries, MaxSeries}, {"BatchSize", &c.BatchSize, DefaultBatchSize, MaxBatchSize}} {
		if *b.value == 0 {
			*b.value = b.fallback
		}
		if *b.value < 1 || *b.value > b.ceiling {
			return fmt.Errorf("%w: %s %d outside 1..%d", ErrConfig, b.name, *b.value, b.ceiling)
		}
	}
	if c.FlushInterval == 0 {
		c.FlushInterval = DefaultFlushInterval
	}
	if c.MetricInterval == 0 {
		c.MetricInterval = DefaultMetricInterval
	}
	if c.ExportTimeout == 0 {
		c.ExportTimeout = DefaultExportTimeout
	}
	if c.FlushInterval < time.Millisecond || c.FlushInterval > time.Minute || c.MetricInterval < time.Millisecond || c.MetricInterval > 10*time.Minute {
		return fmt.Errorf("%w: flush interval %v or metric interval %v out of range", ErrConfig, c.FlushInterval, c.MetricInterval)
	}
	if c.ExportTimeout < time.Millisecond || c.ExportTimeout > MaxExportTimeout {
		return fmt.Errorf("%w: export timeout %v outside 1ms..%v", ErrConfig, c.ExportTimeout, MaxExportTimeout)
	}
	if len(c.Operational) > 0 && c.Metrics == nil {
		return fmt.Errorf("%w: Operational sources require a Metrics exporter", ErrConfig)
	}
	if len(c.TenantLabels) > MaxTenantLabels || len(c.LogAttributes) > MaxLogAttributes {
		return fmt.Errorf("%w: at most %d tenant labels and %d log attributes", ErrConfig, MaxTenantLabels, MaxLogAttributes)
	}
	for _, tenant := range c.TenantLabels {
		if !observe.ValidLabel(tenant) || tenant == otherTenant {
			return fmt.Errorf("%w: tenant label %q", ErrConfig, tenant)
		}
	}
	for _, key := range c.LogAttributes {
		if !observe.ValidLabel(key) || strings.HasPrefix(key, "blok.") {
			return fmt.Errorf("%w: log attribute %q", ErrConfig, key)
		}
	}
	c.TenantLabels = append([]string(nil), c.TenantLabels...)
	c.LogAttributes = append([]string(nil), c.LogAttributes...)
	return nil
}

func (e *Exporter) instruments() error {
	meter := e.meterProvider.Meter(scopeName)
	var err error
	if e.runsCounter, err = meter.Int64Counter(MetricRuns, metric.WithUnit("{run}"), metric.WithDescription("Workflow runs by terminal outcome")); err != nil {
		return err
	}
	buckets := metric.WithExplicitBucketBoundaries(slo.DurationBuckets...)
	if e.runDuration, err = meter.Float64Histogram(MetricRunDuration, metric.WithUnit("s"), metric.WithDescription("Observed workflow run duration"), buckets); err != nil {
		return err
	}
	if e.stepsCounter, err = meter.Int64Counter(MetricSteps, metric.WithUnit("{step}"), metric.WithDescription("Step attempts by outcome")); err != nil {
		return err
	}
	if e.stepDuration, err = meter.Float64Histogram(MetricStepDuration, metric.WithUnit("s"), metric.WithDescription("Observed step attempt duration"), buckets); err != nil {
		return err
	}
	if e.externalCounter, err = meter.Int64Counter(MetricExternalCalls, metric.WithUnit("{call}"), metric.WithDescription("Attempts of steps whose node declares effects, by outcome")); err != nil {
		return err
	}
	if e.externalDuration, err = meter.Float64Histogram(MetricExternalDuration, metric.WithUnit("s"), metric.WithDescription("External call duration"), buckets); err != nil {
		return err
	}
	if e.sampler != nil {
		if err := e.operationalInstruments(meter); err != nil {
			return err
		}
	}
	_, err = meter.Int64ObservableCounter(MetricDropped, metric.WithUnit("{event}"), metric.WithDescription("Telemetry the pipeline discarded, by reason"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			for reason, value := range map[string]*atomic.Uint64{"queue_full": &e.dropped, "closed": &e.droppedClosed, "abandoned": &e.abandoned, "orphaned": &e.orphaned, "series_overflow": &e.overflowed, "spans_failed": &e.spansFailed, "logs_failed": &e.logsFailed, "metric_exports_failed": &e.metricFailures} {
				observer.Observe(int64(value.Load()), metric.WithAttributes(attribute.String(AttrDropReason, reason)))
			}
			return nil
		}))
	return err
}

// ObservesPayloads reports that the exporter never reads Input or Output, so
// the engine does not serialize them for it (observe.PayloadObserver).
func (e *Exporter) ObservesPayloads() bool { return false }

// Observe queues a copy of event without blocking. Payloads and the
// principal are discarded here, before anything is retained.
func (e *Exporter) Observe(event inspection.Event) {
	if e == nil {
		return
	}
	e.producers.Add(1)
	defer e.producers.Add(-1)
	if e.closed.Load() {
		e.droppedClosed.Add(1)
		return
	}
	if event.Kind == inspection.StepLog && (e.logger == nil || !event.Trace.Sampled()) {
		return
	}
	event.Input, event.Output, event.Principal = nil, nil, ""
	if event.Kind != inspection.StepLog || e.logAttributes == nil {
		event.LogAttrs = nil
	}
	select {
	case e.queue <- event:
		e.accepted.Add(1)
	default:
		e.dropped.Add(1)
	}
}

// Stats returns a snapshot of the pipeline counters.
func (e *Exporter) Stats() Stats {
	return Stats{
		Accepted: e.accepted.Load(), Dropped: e.dropped.Load(), DroppedClosed: e.droppedClosed.Load(), DroppedShutdown: e.droppedShutdown.Load(), Queued: uint64(len(e.queue)), Processed: e.processed.Load(),
		Abandoned: e.abandoned.Load(), Orphaned: e.orphaned.Load(), Overflowed: e.overflowed.Load(),
		SpansExported: e.spansExported.Load(), SpansFailed: e.spansFailed.Load(), LogsExported: e.logsExported.Load(), LogsFailed: e.logsFailed.Load(),
		MetricExports: e.metricExports.Load(), MetricFailures: e.metricFailures.Load(), Panics: e.panics.Load(), SampleFailures: e.sampler.Failures(),
	}
}

// Flush exports everything queued so far, including a metric collection,
// bounded by ctx. It is for drain and tests; runs never wait on it.
func (e *Exporter) Flush(ctx context.Context) error {
	reply := make(chan struct{})
	select {
	case e.flushReq <- reply:
	case <-e.done:
		return errors.New("otel: exporter is shut down")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-reply:
	case <-ctx.Done():
		return ctx.Err()
	}
	if e.meterProvider != nil {
		return e.meterProvider.ForceFlush(ctx)
	}
	return nil
}

// Shutdown stops accepting events, exports what is queued and shuts the
// selected providers and exporters down, all bounded by ctx. When ctx
// expires first, in-flight exports are canceled, events still queued are
// discarded and counted (Stats.DroppedShutdown), and the providers are shut
// down anyway with the expired ctx; Shutdown then returns ctx's error. Open
// runs are abandoned, not exported as if complete. An exporter call that
// ignores its context is abandoned: its goroutine ends when the exporter
// returns. It is safe to call more than once.
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.once.Do(func() {
		e.closed.Store(true)
		// Producers past their closed check are doing a non-blocking send;
		// wait them out so nothing is queued after the loop drains.
		for e.producers.Load() != 0 {
			runtime.Gosched()
		}
		close(e.stop)
	})
	var expired error
	select {
	case <-e.done:
	case <-ctx.Done():
		e.aborted.Store(true)
		e.cancelExports()
		// After abort the loop does only in-memory work, so this is short.
		<-e.done
		expired = ctx.Err()
	}
	e.providersOnce.Do(func() {
		// Whatever export the providers' own shutdown starts (the metric
		// reader's final collection) is canceled when ctx expires, and each
		// provider shutdown is abandoned at ctx rather than waited out.
		stop := context.AfterFunc(ctx, e.cancelExports)
		defer stop()
		var errs []error
		if e.tracerProvider != nil {
			errs = append(errs, e.boundedCall(ctx, nil, e.tracerProvider.Shutdown))
		}
		if e.loggerProvider != nil {
			errs = append(errs, e.boundedCall(ctx, nil, e.loggerProvider.Shutdown))
		}
		if e.meterProvider != nil {
			errs = append(errs, e.boundedCall(ctx, nil, e.meterProvider.Shutdown))
		}
		// The SDK skips its processors once ctx has expired; every selected
		// exporter is still told to shut down (each at most once).
		if e.spans != nil {
			errs = append(errs, e.spans.Shutdown(ctx))
		}
		if e.records != nil {
			errs = append(errs, e.records.Shutdown(ctx))
		}
		if e.metrics != nil {
			errs = append(errs, e.metrics.Shutdown(ctx))
		}
		e.cancelExports()
		e.shutdownErr = errors.Join(errs...)
	})
	if expired != nil {
		return expired
	}
	return e.shutdownErr
}

func (e *Exporter) loop() {
	defer close(e.done)
	ticker := time.NewTicker(e.config.FlushInterval)
	defer ticker.Stop()
	for {
		select {
		case event := <-e.queue:
			e.handle(event)
			if e.spans.pending() >= e.config.BatchSize || e.records.pending() >= e.config.BatchSize {
				e.flush()
			}
		case <-ticker.C:
			e.flush()
		case reply := <-e.flushReq:
			e.drain()
			e.flush()
			close(reply)
		case <-e.stop:
			e.drain()
			for _, element := range e.runs {
				e.abandonRun(element.Value.(*runState))
			}
			e.runs, e.order = map[string]*list.Element{}, list.New()
			e.flush()
			return
		}
	}
}

func (e *Exporter) drain() {
	for {
		if e.aborted.Load() {
			e.discardQueued()
			return
		}
		select {
		case event := <-e.queue:
			e.handle(event)
			if e.spans.pending() >= e.config.BatchSize || e.records.pending() >= e.config.BatchSize {
				e.flush()
			}
		default:
			return
		}
	}
}

// discardQueued counts what is left in the queue once Shutdown gave up.
func (e *Exporter) discardQueued() {
	for {
		select {
		case <-e.queue:
			e.droppedShutdown.Add(1)
		default:
			return
		}
	}
}

func (e *Exporter) flush() {
	if e.spans != nil {
		if batch := e.spans.take(); len(batch) > 0 {
			err := e.export(e.traceSlot, context.Background(), func(ctx context.Context) error { return e.config.Traces.ExportSpans(ctx, batch) })
			if err != nil {
				e.spansFailed.Add(uint64(len(batch)))
			} else {
				e.spansExported.Add(uint64(len(batch)))
			}
		}
	}
	if e.records != nil {
		if batch := e.records.take(); len(batch) > 0 {
			err := e.export(e.logSlot, context.Background(), func(ctx context.Context) error { return e.config.Logs.Export(ctx, batch) })
			if err != nil {
				e.logsFailed.Add(uint64(len(batch)))
			} else {
				e.logsExported.Add(uint64(len(batch)))
			}
		}
	}
}

var (
	errAborted  = errors.New("otel: export abandoned at shutdown")
	errBusy     = errors.New("otel: previous export call has not returned")
	errPanicked = errors.New("otel: exporter panicked")
)

// export runs one export call bounded by ExportTimeout, by parent, and by
// Shutdown giving up. The call runs on its own goroutine so the pipeline
// never waits past those bounds, even on an exporter that ignores its
// context; slot admits one such call per signal at a time. A panicking
// exporter is contained and counted: telemetry never takes the process down.
func (e *Exporter) export(slot chan struct{}, parent context.Context, call func(context.Context) error) error {
	if e.aborted.Load() {
		return errAborted
	}
	ctx, cancel := context.WithTimeout(parent, e.config.ExportTimeout)
	defer cancel()
	stop := context.AfterFunc(e.exportCtx, cancel)
	defer stop()
	return e.boundedCall(ctx, slot, call)
}

// boundedCall runs call(ctx) and returns when it does or when ctx ends.
func (e *Exporter) boundedCall(ctx context.Context, slot chan struct{}, call func(context.Context) error) error {
	if slot != nil {
		select {
		case slot <- struct{}{}:
		default:
			return errBusy
		}
	}
	result := make(chan error, 1)
	go func() {
		if slot != nil {
			defer func() { <-slot }()
		}
		defer func() {
			if recover() != nil {
				e.panics.Add(1)
				result <- errPanicked
			}
		}()
		result <- call(ctx)
	}()
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *Exporter) handle(event inspection.Event) {
	defer func() {
		if recover() != nil {
			e.panics.Add(1)
		}
	}()
	e.processed.Add(1)
	switch event.Kind {
	case inspection.RunStarted:
		e.runStarted(event)
	case inspection.StepProcessing:
		e.stepStarted(event)
	case inspection.StepCompleted, inspection.StepFailed, inspection.StepCanceled, inspection.StepUncertain:
		e.stepEnded(event)
	case inspection.StepLog:
		e.stepLog(event)
	case inspection.RunCompleted, inspection.RunFailed, inspection.RunCanceled, inspection.RunUncertain, inspection.RunSuspended:
		e.runEnded(event)
	}
}

func (e *Exporter) runStarted(event inspection.Event) {
	if element, ok := e.runs[event.RunID]; ok {
		e.abandonRun(element.Value.(*runState))
		e.order.Remove(element)
		delete(e.runs, event.RunID)
	}
	if len(e.runs) >= e.config.MaxOpenRuns {
		oldest := e.order.Front()
		state := oldest.Value.(*runState)
		e.abandonRun(state)
		e.order.Remove(oldest)
		delete(e.runs, state.id)
	}
	e.zeroRuns(event.Workflow, event.Tenant)
	state := &runState{id: event.RunID, start: event.At, workflow: event.Workflow, tenant: event.Tenant, steps: map[string]*stepState{}}
	if e.tracer != nil && event.Trace.Sampled() {
		state.span = e.startSpan(event, "run "+label(event.Workflow), e.runAttributes(event))
	}
	e.runs[event.RunID] = e.order.PushBack(state)
}

func (e *Exporter) stepStarted(event inspection.Event) {
	state := e.run(event.RunID)
	if state == nil || event.AttemptID == "" {
		return
	}
	if len(state.steps) >= maxStepsPerRun {
		for key, step := range state.steps {
			e.abandonStep(step)
			delete(state.steps, key)
		}
	}
	if event.External {
		e.zeroExternal(state.workflow, event.StepID)
	}
	step := &stepState{start: event.At}
	if e.tracer != nil && event.Trace.Sampled() {
		step.span = e.startSpan(event, "step "+label(event.StepID), e.stepAttributes(event))
	}
	state.steps[event.AttemptID] = step
}

func (e *Exporter) stepEnded(event inspection.Event) {
	outcome := stepOutcome(event.Kind)
	state := e.run(event.RunID)
	var step *stepState
	if state != nil && event.AttemptID != "" {
		step = state.steps[event.AttemptID]
		delete(state.steps, event.AttemptID)
	}
	workflow := event.Workflow
	if state != nil {
		workflow = state.workflow
	}
	attrs := []attribute.KeyValue{attribute.String(AttrWorkflow, label(workflow)), attribute.String(AttrStep, label(event.StepID)), attribute.String(AttrOutcome, outcome)}
	if e.stepsCounter != nil {
		counted := append(attrs, attribute.String(AttrErrorClass, optionalLabel(event.ErrorClass)))
		e.stepsCounter.Add(context.Background(), 1, metric.WithAttributeSet(e.bounded(MetricSteps, counted)))
		if step != nil && !step.start.IsZero() && !event.At.Before(step.start) {
			e.stepDuration.Record(context.Background(), event.At.Sub(step.start).Seconds(), metric.WithAttributeSet(e.bounded(MetricStepDuration, attrs)))
		}
		if event.External {
			e.zeroExternal(workflow, event.StepID)
			e.externalCounter.Add(context.Background(), 1, metric.WithAttributeSet(e.bounded(MetricExternalCalls, attrs)))
			if step != nil && !step.start.IsZero() && !event.At.Before(step.start) {
				e.externalDuration.Record(context.Background(), event.At.Sub(step.start).Seconds(), metric.WithAttributeSet(e.bounded(MetricExternalDuration, attrs)))
			}
		}
	}
	if e.tracer == nil || !event.Trace.Sampled() {
		return
	}
	span := oteltrace.Span(nil)
	if step != nil {
		span = step.span
	}
	if span == nil {
		e.orphaned.Add(1)
		span = e.startSpan(event, "step "+label(event.StepID), append(e.stepAttributes(event), attribute.Bool(AttrStartObserved, false)))
	}
	endSpan(span, event, outcome)
}

// zeroRuns creates every run outcome of a workflow and tenant at zero the
// first time it is seen, and zeroExternal every outcome of an external call.
// A counter series born at its first increment has no increase() in
// Prometheus, so without the zeros the first uncertain run of a workflow
// that already ran would not be seen by a rate rule (ADR 0022). The created
// sets count towards MaxSeries like any other.
func (e *Exporter) zeroRuns(workflow, tenant string) {
	if e.runsCounter == nil {
		return
	}
	key := "runs|" + label(workflow) + "|"
	var tenantAttr []attribute.KeyValue
	if tenant != "" {
		tenantAttr = []attribute.KeyValue{attribute.String(AttrTenant, e.tenantLabel(tenant))}
		key += e.tenantLabel(tenant)
	}
	if !e.markZeroed(key) {
		return
	}
	for _, outcome := range slo.RunOutcomes {
		attrs := append([]attribute.KeyValue{attribute.String(AttrWorkflow, label(workflow)), attribute.String(AttrOutcome, outcome)}, tenantAttr...)
		e.runsCounter.Add(context.Background(), 0, metric.WithAttributeSet(e.bounded(MetricRuns, attrs)))
	}
}

func (e *Exporter) zeroExternal(workflow, step string) {
	if e.externalCounter == nil || !e.markZeroed("external|"+label(workflow)+"|"+label(step)) {
		return
	}
	for _, outcome := range slo.StepOutcomes {
		attrs := []attribute.KeyValue{attribute.String(AttrWorkflow, label(workflow)), attribute.String(AttrStep, label(step)), attribute.String(AttrOutcome, outcome)}
		e.externalCounter.Add(context.Background(), 0, metric.WithAttributeSet(e.bounded(MetricExternalCalls, attrs)))
	}
}

// markZeroed records key and reports whether it is new; the record is
// bounded by MaxSeries.
func (e *Exporter) markZeroed(key string) bool {
	if _, ok := e.zeroed[key]; ok || len(e.zeroed) >= e.config.MaxSeries {
		return false
	}
	e.zeroed[key] = struct{}{}
	return true
}

func (e *Exporter) runEnded(event inspection.Event) {
	outcome := runOutcome(event.Kind)
	var state *runState
	if element, ok := e.runs[event.RunID]; ok {
		state = element.Value.(*runState)
		e.order.Remove(element)
		delete(e.runs, event.RunID)
	}
	if state != nil {
		for key, step := range state.steps {
			e.abandonStep(step)
			delete(state.steps, key)
		}
	}
	tenant, workflow := event.Tenant, event.Workflow
	if state != nil {
		tenant, workflow = state.tenant, state.workflow
	}
	attrs := []attribute.KeyValue{attribute.String(AttrWorkflow, label(workflow)), attribute.String(AttrOutcome, outcome)}
	if tenant != "" {
		attrs = append(attrs, attribute.String(AttrTenant, e.tenantLabel(tenant)))
	}
	if e.runsCounter != nil {
		e.runsCounter.Add(context.Background(), 1, metric.WithAttributeSet(e.bounded(MetricRuns, attrs)))
		if state != nil && !state.start.IsZero() && !event.At.Before(state.start) {
			e.runDuration.Record(context.Background(), event.At.Sub(state.start).Seconds(), metric.WithAttributeSet(e.bounded(MetricRunDuration, attrs)))
		}
	}
	if e.tracer == nil || !event.Trace.Sampled() {
		return
	}
	var span oteltrace.Span
	if state != nil {
		span = state.span
	}
	if span == nil {
		e.orphaned.Add(1)
		span = e.startSpan(event, "run "+label(workflow), append(e.runAttributes(event), attribute.Bool(AttrStartObserved, false)))
	}
	endSpan(span, event, outcome)
}

func (e *Exporter) stepLog(event inspection.Event) {
	if e.logger == nil || !event.Trace.Sampled() {
		return
	}
	var record otellog.Record
	record.SetTimestamp(event.At)
	record.SetObservedTimestamp(time.Now())
	severity, text := logSeverity(event.LogLevel)
	record.SetSeverity(severity)
	record.SetSeverityText(text)
	record.SetBody(attribute.StringValue(redact.Message(truncate(event.LogMessage, maxLogBodyBytes))))
	record.AddAttributes(attribute.String(AttrRunID, identity(event.RunID)), attribute.String(AttrWorkflow, label(event.Workflow)), attribute.String(AttrStep, label(event.StepID)))
	if tenant := e.spanTenant(event.Tenant); tenant != "" {
		record.AddAttributes(attribute.String(AttrTenant, tenant))
	}
	record.AddAttributes(e.allowedLogAttributes(event.LogAttrs)...)
	e.logger.Emit(spanContext(event.Trace, event.Trace.SpanID, false), record)
}

// allowedLogAttributes decodes only allowlisted top-level scalar attributes.
// Allowlisting a key selects it for export; it never exports a secret: a
// sensitive key's value and any credential-shaped string, encoded ones
// included, are exported as the redaction marker (ADR 0021).
func (e *Exporter) allowedLogAttributes(raw json.RawMessage) []attribute.KeyValue {
	if e.logAttributes == nil || len(raw) == 0 {
		return nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var values map[string]any
	if decoder.Decode(&values) != nil {
		return nil
	}
	out := []attribute.KeyValue{}
	for _, key := range e.config.LogAttributes {
		value, ok := values[key]
		if !ok {
			continue
		}
		if redact.Key(key) {
			out = append(out, attribute.String(key, redact.Marker))
			continue
		}
		switch typed := value.(type) {
		case string:
			out = append(out, attribute.String(key, redact.String(truncate(typed, maxLogValueBytes))))
		case bool:
			out = append(out, attribute.Bool(key, typed))
		case json.Number:
			if integer, err := typed.Int64(); err == nil {
				out = append(out, attribute.Int64(key, integer))
			} else if float, err := typed.Float64(); err == nil {
				out = append(out, attribute.Float64(key, float))
			}
		}
	}
	return out
}

func (e *Exporter) run(id string) *runState {
	if element, ok := e.runs[id]; ok {
		return element.Value.(*runState)
	}
	return nil
}

func (e *Exporter) abandonRun(state *runState) {
	for key, step := range state.steps {
		e.abandonStep(step)
		delete(state.steps, key)
	}
	e.abandoned.Add(1)
	// An abandoned span is never ended, so it is never exported as if it
	// had completed.
	state.span = nil
}

func (e *Exporter) abandonStep(step *stepState) {
	e.abandoned.Add(1)
	step.span = nil
}

// bounded returns attrs as a set, or the overflow set when the instrument
// already holds MaxSeries distinct sets.
func (e *Exporter) bounded(instrument string, attrs []attribute.KeyValue) attribute.Set {
	set := attribute.NewSet(attrs...)
	key := set.Encoded(attribute.DefaultEncoder())
	seen := e.series[instrument]
	if seen == nil {
		seen = map[string]struct{}{}
		e.series[instrument] = seen
	}
	if _, ok := seen[key]; ok {
		return set
	}
	if len(seen) >= e.config.MaxSeries {
		e.overflowed.Add(1)
		return attribute.NewSet(attribute.Bool(AttrOverflow, true))
	}
	seen[key] = struct{}{}
	return set
}

func (e *Exporter) tenantLabel(tenant string) string {
	if e.tenants[tenant] {
		return tenant
	}
	return otherTenant
}

// spanTenant is the tenant on spans and logs: the allowlisted label, or the
// raw valid label only when the application opted in with RawTenants.
func (e *Exporter) spanTenant(tenant string) string {
	if tenant == "" {
		return ""
	}
	if e.config.RawTenants && observe.ValidLabel(tenant) {
		return tenant
	}
	return e.tenantLabel(tenant)
}

// identity bounds an identifier exported on spans and logs: a value that is
// a valid label is kept for correlation; anything else (longer than 64
// bytes or outside the label alphabet) becomes "h:" and 16 hex digits of
// its SHA-256, which a reader can compute from the inspection id. A value
// that itself starts with "h:" is always hashed, so a raw id can never equal
// another id's digest.
func identity(value string) string {
	if value == "" || observe.ValidLabel(value) && !strings.HasPrefix(value, "h:") {
		return value
	}
	sum := sha256.Sum256([]byte(value))
	return "h:" + hex.EncodeToString(sum[:8])
}

func (e *Exporter) startSpan(event inspection.Event, name string, attrs []attribute.KeyValue) oteltrace.Span {
	ctx := context.WithValue(spanContext(event.Trace, event.Trace.Parent, true), idsKey{}, ids{trace: oteltrace.TraceID(event.Trace.TraceID), span: oteltrace.SpanID(event.Trace.SpanID)})
	_, span := e.tracer.Start(ctx, name, oteltrace.WithTimestamp(event.At), oteltrace.WithSpanKind(oteltrace.SpanKindInternal), oteltrace.WithAttributes(attrs...))
	return span
}

// spanContext returns a context whose active span is (trace, id), or the
// background context when id is zero (a root).
func spanContext(span observe.Span, id observe.SpanID, remote bool) context.Context {
	ctx := context.Background()
	if !id.IsValid() {
		return ctx
	}
	config := oteltrace.SpanContextConfig{TraceID: oteltrace.TraceID(span.TraceID), SpanID: oteltrace.SpanID(id), TraceFlags: oteltrace.TraceFlags(span.Flags & observe.FlagSampled), Remote: remote}
	if state, err := oteltrace.ParseTraceState(span.State); err == nil {
		config.TraceState = state
	}
	sc := oteltrace.NewSpanContext(config)
	if remote {
		return oteltrace.ContextWithRemoteSpanContext(ctx, sc)
	}
	return oteltrace.ContextWithSpanContext(ctx, sc)
}

func endSpan(span oteltrace.Span, event inspection.Event, outcome string) {
	span.SetAttributes(attribute.String(AttrOutcome, outcome))
	if code := optionalLabel(event.ErrorCode); code != "" {
		span.SetAttributes(attribute.String(AttrErrorCode, code))
	}
	if class := optionalLabel(event.ErrorClass); class != "" {
		span.SetAttributes(attribute.String(AttrErrorClass, class))
	}
	switch outcome {
	case "failed", "uncertain":
		span.SetStatus(codes.Error, optionalLabel(event.ErrorCode))
	case "completed":
		span.SetStatus(codes.Ok, "")
	}
	span.End(oteltrace.WithTimestamp(event.At))
}

func (e *Exporter) runAttributes(event inspection.Event) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String(AttrWorkflow, label(event.Workflow)), attribute.String(AttrRunID, identity(event.RunID))}
	if event.ParentRun != "" {
		attrs = append(attrs, attribute.String(AttrParentRun, identity(event.ParentRun)), attribute.String(AttrParentStep, label(event.ParentStep)))
	}
	if tenant := e.spanTenant(event.Tenant); tenant != "" {
		attrs = append(attrs, attribute.String(AttrTenant, tenant))
	}
	return attrs
}

func (e *Exporter) stepAttributes(event inspection.Event) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String(AttrWorkflow, label(event.Workflow)), attribute.String(AttrStep, label(event.StepID)), attribute.Int(AttrAttempt, event.Attempt), attribute.String(AttrRunID, identity(event.RunID))}
	if event.AttemptID != "" {
		attrs = append(attrs, attribute.String(AttrAttemptID, identity(event.AttemptID)))
	}
	if tenant := e.spanTenant(event.Tenant); tenant != "" {
		attrs = append(attrs, attribute.String(AttrTenant, tenant))
	}
	return attrs
}

func runOutcome(kind inspection.Kind) string {
	switch kind {
	case inspection.RunCompleted:
		return "completed"
	case inspection.RunCanceled:
		return "canceled"
	case inspection.RunUncertain:
		return "uncertain"
	case inspection.RunSuspended:
		return "suspended"
	default:
		return "failed"
	}
}

func stepOutcome(kind inspection.Kind) string {
	switch kind {
	case inspection.StepCompleted:
		return "completed"
	case inspection.StepCanceled:
		return "canceled"
	case inspection.StepUncertain:
		return "uncertain"
	default:
		return "failed"
	}
}

func label(value string) string {
	if observe.ValidLabel(value) {
		return value
	}
	return invalidLabel
}

func optionalLabel(value string) string {
	if value == "" {
		return ""
	}
	return label(value)
}

func logSeverity(level string) (otellog.Severity, string) {
	switch level {
	case "DEBUG":
		return otellog.SeverityDebug, level
	case "INFO":
		return otellog.SeverityInfo, level
	case "WARN":
		return otellog.SeverityWarn, level
	case "ERROR":
		return otellog.SeverityError, level
	default:
		return logSeverityFallback, "INFO"
	}
}

func truncate(value string, max int) string {
	if len(value) <= max {
		return value
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut]
}

// engineIDs makes the SDK use the span ids the engine already allocated and
// propagated, so exported spans match what workers and child runs saw.
type engineIDs struct{}
type idsKey struct{}
type ids struct {
	trace oteltrace.TraceID
	span  oteltrace.SpanID
}

func (engineIDs) NewIDs(ctx context.Context) (oteltrace.TraceID, oteltrace.SpanID) {
	value, _ := ctx.Value(idsKey{}).(ids)
	return value.trace, value.span
}

func (engineIDs) NewSpanID(ctx context.Context, _ oteltrace.TraceID) oteltrace.SpanID {
	value, _ := ctx.Value(idsKey{}).(ids)
	return value.span
}

// spanBuffer is the span processor: ended spans wait here until the export
// goroutine exports them synchronously. It holds at most one batch beyond
// BatchSize because the loop flushes at that size.
type spanBuffer struct {
	mu       sync.Mutex
	batch    []sdktrace.ReadOnlySpan
	exporter sdktrace.SpanExporter
	owner    *Exporter
	shutdown onceErr
}

// onceErr runs a shutdown at most once and remembers its result.
type onceErr struct {
	once sync.Once
	err  error
}

func (o *onceErr) do(call func() error) error {
	o.once.Do(func() { o.err = call() })
	return o.err
}

func (b *spanBuffer) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (b *spanBuffer) OnEnd(span sdktrace.ReadOnlySpan) {
	b.mu.Lock()
	b.batch = append(b.batch, span)
	b.mu.Unlock()
}
func (b *spanBuffer) Shutdown(ctx context.Context) error {
	return b.shutdown.do(func() error { return b.owner.boundedCall(ctx, nil, b.exporter.Shutdown) })
}
func (b *spanBuffer) ForceFlush(context.Context) error { return nil }
func (b *spanBuffer) pending() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.batch)
}
func (b *spanBuffer) take() []sdktrace.ReadOnlySpan {
	b.mu.Lock()
	defer b.mu.Unlock()
	batch := b.batch
	b.batch = nil
	return batch
}

// logBuffer is the log processor, with the same contract as spanBuffer.
type logBuffer struct {
	mu       sync.Mutex
	batch    []sdklog.Record
	exporter sdklog.Exporter
	owner    *Exporter
	shutdown onceErr
}

func (b *logBuffer) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (b *logBuffer) OnEmit(_ context.Context, record *sdklog.Record) error {
	b.mu.Lock()
	b.batch = append(b.batch, record.Clone())
	b.mu.Unlock()
	return nil
}
func (b *logBuffer) Shutdown(ctx context.Context) error {
	return b.shutdown.do(func() error { return b.owner.boundedCall(ctx, nil, b.exporter.Shutdown) })
}
func (b *logBuffer) ForceFlush(context.Context) error { return nil }
func (b *logBuffer) pending() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.batch)
}
func (b *logBuffer) take() []sdklog.Record {
	b.mu.Lock()
	defer b.mu.Unlock()
	batch := b.batch
	b.batch = nil
	return batch
}

// countingMetrics counts metric export outcomes. Metrics are cumulative, so
// a collection exported after a failed one still carries every observation.
type countingMetrics struct {
	sdkmetric.Exporter
	owner    *Exporter
	shutdown onceErr
}

func (c *countingMetrics) Export(ctx context.Context, data *metricdata.ResourceMetrics) error {
	err := c.owner.export(c.owner.metricSlot, ctx, func(ctx context.Context) error { return c.Exporter.Export(ctx, data) })
	if err != nil {
		c.owner.metricFailures.Add(1)
	} else {
		c.owner.metricExports.Add(1)
	}
	return err
}

func (c *countingMetrics) ForceFlush(ctx context.Context) error {
	return c.owner.boundedCall(ctx, nil, c.Exporter.ForceFlush)
}

func (c *countingMetrics) Shutdown(ctx context.Context) error {
	return c.shutdown.do(func() error { return c.owner.boundedCall(ctx, nil, c.Exporter.Shutdown) })
}
