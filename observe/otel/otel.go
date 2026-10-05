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
	"encoding/json"
	"errors"
	"fmt"
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
	MetricRuns          = "blok.runs"
	MetricRunDuration   = "blok.run.duration"
	MetricSteps         = "blok.steps"
	MetricStepDuration  = "blok.step.duration"
	MetricDropped       = "blok.telemetry.dropped"
	AttrWorkflow        = "blok.workflow"
	AttrStep            = "blok.step"
	AttrOutcome         = "blok.outcome"
	AttrTenant          = "blok.tenant"
	AttrErrorCode       = "blok.error.code"
	AttrErrorClass      = "blok.error.class"
	AttrRunID           = "blok.run.id"
	AttrAttempt         = "blok.attempt"
	AttrAttemptID       = "blok.attempt.id"
	AttrParentRun       = "blok.parent.run.id"
	AttrParentStep      = "blok.parent.step"
	AttrStartObserved   = "blok.start_observed"
	AttrOverflow        = "otel.metric.overflow"
	AttrDropReason      = "reason"
	defaultServiceName  = "blok"
	logSeverityFallback = otellog.SeverityInfo
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
	// TenantLabels allowlists tenants used as metric labels; any other
	// tenant is labeled "other". Tenants on spans and logs must still be
	// valid labels. At most MaxTenantLabels.
	TenantLabels []string
	// LogAttributes allowlists structured log attribute keys exported with
	// a step log; all other attributes are dropped. At most MaxLogAttributes.
	LogAttributes []string
}

// Stats counts what the pipeline did. Every event is either processed or
// counted as dropped; every exported item is either exported or failed.
type Stats struct {
	Accepted       uint64 // events queued
	Dropped        uint64 // events refused because the queue was full
	DroppedClosed  uint64 // events refused after Shutdown began
	Processed      uint64 // events the export goroutine handled
	Abandoned      uint64 // open runs evicted at MaxOpenRuns or steps left open at run end
	Orphaned       uint64 // terminal events whose start was never observed
	Overflowed     uint64 // metric points recorded under the overflow attribute set
	SpansExported  uint64
	SpansFailed    uint64
	LogsExported   uint64
	LogsFailed     uint64
	MetricExports  uint64
	MetricFailures uint64
	Panics         uint64 // recovered SDK panics; the event is dropped
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

	providersOnce sync.Once
	shutdownErr   error

	tracerProvider *sdktrace.TracerProvider
	tracer         oteltrace.Tracer
	spans          *spanBuffer
	meterProvider  *sdkmetric.MeterProvider
	loggerProvider *sdklog.LoggerProvider
	logger         otellog.Logger
	records        *logBuffer

	runsCounter   metric.Int64Counter
	runDuration   metric.Float64Histogram
	stepsCounter  metric.Int64Counter
	stepDuration  metric.Float64Histogram
	series        map[string]map[string]struct{}
	tenants       map[string]bool
	logAttributes map[string]bool

	runs  map[string]*list.Element
	order *list.List

	accepted, dropped, droppedClosed, processed, abandoned, orphaned, overflowed        atomic.Uint64
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
		tenants:  map[string]bool{},
		runs:     map[string]*list.Element{},
		order:    list.New(),
	}
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
		e.spans = &spanBuffer{exporter: config.Traces}
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
		e.records = &logBuffer{exporter: config.Logs}
		e.loggerProvider = sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(e.records))
		e.logger = e.loggerProvider.Logger(scopeName)
	}
	if config.Metrics != nil {
		reader := sdkmetric.NewPeriodicReader(&countingMetrics{Exporter: config.Metrics, owner: e}, sdkmetric.WithInterval(config.MetricInterval), sdkmetric.WithTimeout(config.ExportTimeout))
		e.meterProvider = sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(reader))
		if err := e.instruments(); err != nil {
			_ = e.meterProvider.Shutdown(context.Background())
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
	if e.runDuration, err = meter.Float64Histogram(MetricRunDuration, metric.WithUnit("s"), metric.WithDescription("Observed workflow run duration")); err != nil {
		return err
	}
	if e.stepsCounter, err = meter.Int64Counter(MetricSteps, metric.WithUnit("{step}"), metric.WithDescription("Step attempts by outcome")); err != nil {
		return err
	}
	if e.stepDuration, err = meter.Float64Histogram(MetricStepDuration, metric.WithUnit("s"), metric.WithDescription("Observed step attempt duration")); err != nil {
		return err
	}
	_, err = meter.Int64ObservableCounter(MetricDropped, metric.WithUnit("{event}"), metric.WithDescription("Telemetry the pipeline discarded, by reason"),
		metric.WithInt64Callback(func(_ context.Context, observer metric.Int64Observer) error {
			for reason, value := range map[string]*atomic.Uint64{"queue_full": &e.dropped, "closed": &e.droppedClosed, "abandoned": &e.abandoned, "orphaned": &e.orphaned, "series_overflow": &e.overflowed, "spans_failed": &e.spansFailed, "logs_failed": &e.logsFailed} {
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
		Accepted: e.accepted.Load(), Dropped: e.dropped.Load(), DroppedClosed: e.droppedClosed.Load(), Processed: e.processed.Load(),
		Abandoned: e.abandoned.Load(), Orphaned: e.orphaned.Load(), Overflowed: e.overflowed.Load(),
		SpansExported: e.spansExported.Load(), SpansFailed: e.spansFailed.Load(), LogsExported: e.logsExported.Load(), LogsFailed: e.logsFailed.Load(),
		MetricExports: e.metricExports.Load(), MetricFailures: e.metricFailures.Load(), Panics: e.panics.Load(),
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
// selected providers and exporters down, bounded by ctx. Open runs are
// abandoned, not exported as if complete. It is safe to call more than once.
func (e *Exporter) Shutdown(ctx context.Context) error {
	e.once.Do(func() {
		e.closed.Store(true)
		close(e.stop)
	})
	select {
	case <-e.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	e.providersOnce.Do(func() {
		var errs []error
		if e.tracerProvider != nil {
			errs = append(errs, e.tracerProvider.Shutdown(ctx))
		}
		if e.loggerProvider != nil {
			errs = append(errs, e.loggerProvider.Shutdown(ctx))
		}
		if e.meterProvider != nil {
			errs = append(errs, e.meterProvider.Shutdown(ctx))
		}
		e.shutdownErr = errors.Join(errs...)
	})
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

func (e *Exporter) flush() {
	if e.spans != nil {
		if batch := e.spans.take(); len(batch) > 0 {
			err := e.export(func(ctx context.Context) error { return e.config.Traces.ExportSpans(ctx, batch) })
			if err != nil {
				e.spansFailed.Add(uint64(len(batch)))
			} else {
				e.spansExported.Add(uint64(len(batch)))
			}
		}
	}
	if e.records != nil {
		if batch := e.records.take(); len(batch) > 0 {
			err := e.export(func(ctx context.Context) error { return e.config.Logs.Export(ctx, batch) })
			if err != nil {
				e.logsFailed.Add(uint64(len(batch)))
			} else {
				e.logsExported.Add(uint64(len(batch)))
			}
		}
	}
}

// export runs one bounded export call. A panicking exporter is contained and
// counted: telemetry must never take the process down.
func (e *Exporter) export(call func(context.Context) error) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), e.config.ExportTimeout)
	defer cancel()
	defer func() {
		if recover() != nil {
			e.panics.Add(1)
			err = errors.New("otel: exporter panicked")
		}
	}()
	return call(ctx)
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
	state := &runState{id: event.RunID, start: event.At, workflow: event.Workflow, tenant: event.Tenant, steps: map[string]*stepState{}}
	if e.tracer != nil && event.Trace.Sampled() {
		state.span = e.startSpan(event, "run "+label(event.Workflow), runAttributes(event))
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
	step := &stepState{start: event.At}
	if e.tracer != nil && event.Trace.Sampled() {
		step.span = e.startSpan(event, "step "+label(event.StepID), stepAttributes(event))
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
		span = e.startSpan(event, "step "+label(event.StepID), append(stepAttributes(event), attribute.Bool(AttrStartObserved, false)))
	}
	endSpan(span, event, outcome)
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
		span = e.startSpan(event, "run "+label(workflow), append(runAttributes(event), attribute.Bool(AttrStartObserved, false)))
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
	record.SetBody(attribute.StringValue(observe.RedactLogMessage(truncate(event.LogMessage, maxLogBodyBytes))))
	record.AddAttributes(attribute.String(AttrRunID, event.RunID), attribute.String(AttrWorkflow, label(event.Workflow)), attribute.String(AttrStep, label(event.StepID)))
	if tenant := optionalLabel(event.Tenant); tenant != "" && observe.ValidLabel(event.Tenant) {
		record.AddAttributes(attribute.String(AttrTenant, event.Tenant))
	}
	record.AddAttributes(e.allowedLogAttributes(event.LogAttrs)...)
	e.logger.Emit(spanContext(event.Trace, event.Trace.SpanID, false), record)
}

// allowedLogAttributes decodes only allowlisted top-level scalar attributes.
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
		switch typed := value.(type) {
		case string:
			out = append(out, attribute.String(key, observe.RedactLogMessage(truncate(typed, maxLogValueBytes))))
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

func runAttributes(event inspection.Event) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String(AttrWorkflow, label(event.Workflow)), attribute.String(AttrRunID, event.RunID)}
	if event.ParentRun != "" {
		attrs = append(attrs, attribute.String(AttrParentRun, event.ParentRun), attribute.String(AttrParentStep, label(event.ParentStep)))
	}
	if observe.ValidLabel(event.Tenant) {
		attrs = append(attrs, attribute.String(AttrTenant, event.Tenant))
	}
	return attrs
}

func stepAttributes(event inspection.Event) []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String(AttrWorkflow, label(event.Workflow)), attribute.String(AttrStep, label(event.StepID)), attribute.Int(AttrAttempt, event.Attempt), attribute.String(AttrRunID, event.RunID)}
	if event.AttemptID != "" {
		attrs = append(attrs, attribute.String(AttrAttemptID, event.AttemptID))
	}
	if observe.ValidLabel(event.Tenant) {
		attrs = append(attrs, attribute.String(AttrTenant, event.Tenant))
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
}

func (b *spanBuffer) OnStart(context.Context, sdktrace.ReadWriteSpan) {}
func (b *spanBuffer) OnEnd(span sdktrace.ReadOnlySpan) {
	b.mu.Lock()
	b.batch = append(b.batch, span)
	b.mu.Unlock()
}
func (b *spanBuffer) Shutdown(ctx context.Context) error { return b.exporter.Shutdown(ctx) }
func (b *spanBuffer) ForceFlush(context.Context) error   { return nil }
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
}

func (b *logBuffer) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (b *logBuffer) OnEmit(_ context.Context, record *sdklog.Record) error {
	b.mu.Lock()
	b.batch = append(b.batch, record.Clone())
	b.mu.Unlock()
	return nil
}
func (b *logBuffer) Shutdown(ctx context.Context) error { return b.exporter.Shutdown(ctx) }
func (b *logBuffer) ForceFlush(context.Context) error   { return nil }
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
	owner *Exporter
}

func (c *countingMetrics) Export(ctx context.Context, data *metricdata.ResourceMetrics) (err error) {
	defer func() {
		if recover() != nil {
			c.owner.panics.Add(1)
			c.owner.metricFailures.Add(1)
			err = errors.New("otel: metric exporter panicked")
		}
	}()
	err = c.Exporter.Export(ctx, data)
	if err != nil {
		c.owner.metricFailures.Add(1)
	} else {
		c.owner.metricExports.Add(1)
	}
	return err
}
