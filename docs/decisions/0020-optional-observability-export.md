# ADR 0020: Optional observability export and trace propagation

- Status: implementation in review for E16-T01 (#79)
- Date: 2026-10-05
- Roadmap: E16-T01 (#79); builds on ADR 0016 (E15-T01 #76, E15-T02 #77) and
  ADR 0004 (worker protocol, hardened by E08-T04 #53)
- Owners: observation port (`contract/observe`), engine instrumentation
  (`internal/engine`), OpenTelemetry exporter module (`observe/otel`)

## Context

Production operators need traces, metrics and structured logs. The framework
must provide them without the engine importing a telemetry SDK, without an
application that does not want them paying for them, and without telemetry
ever deciding, delaying or failing a run. ADR 0016 already gives the engine
one observation port, `inspection.Observer`, which the recorder and the live
stream consume; this decision reuses it rather than adding a second one.

## Decision

### One port, extended; no second observer

- `contract/observe` (standard library only) holds W3C trace context values
  (`TraceID`, `SpanID`, `TraceFlags`, `TraceContext`, `ParseTraceparent`),
  the context carrier `WithTrace`/`TraceFrom`, the head-sampling
  `TracePolicy`, the label shape rule `ValidLabel`, the credential redaction
  shared with inspection (`RedactLogMessage`) and the optional
  `PayloadObserver` declaration.
- `inspection.Event` gains two in-process fields, `Tenant` and `Trace`
  (`observe.Span`), both `json:"-"`: they are not part of the inspection/v1
  wire projection and the recorder and stream ignore them.
  `inspection.Invocation` gains `Tenant` and an optional trusted parent
  `Trace`. These are additive.
- An exporter is an ordinary `inspection.Observer` selected through
  `app.Config.Inspection` (alone or with `inspect.CombineObservers`).
- An observer may implement `observe.PayloadObserver` and return false; when
  every selected observer does, the engine skips serializing `Input` and
  `Output` (a combined observer reports true if any member reads them or does
  not declare). The exporter declares false and also discards payloads and
  the principal in `Observe` before anything is queued, so a production
  exporter can neither pay for nor leak business payloads.

### The engine allocates spans; exporters only translate

Tracing is on only when an observer is selected **and**
`app.Config.Trace` (`observe.TracePolicy{Ratio}`) is non-zero; `app.New`
rejects an invalid ratio and a policy without an observer. Then the engine
allocates the run span when the run starts and one span per step attempt,
puts them on the events, and gives each invoked node a context carrying its
step's `TraceContext`. Allocating in the engine (instead of in the exporter)
is what lets the same identity reach a worker or a child run before any
exporter has seen an event, and keeps the engine free of the SDK. The
exporter makes the OpenTelemetry SDK use exactly these ids (a custom
`IDGenerator`), so what a worker saw and what the collector stores agree.

### Trace context crosses three boundaries

1. **Go nodes.** `observe.TraceFrom(ctx)` inside a node returns its step
   attempt's context, for outbound propagation.
2. **Workers.** `runtime/worker.Define` copies it into the worker call as
   `Call.traceparent`/`Call.tracestate` (proto fields 12 and 13). See the
   protocol decision below. The Node SDK exposes it as the optional,
   frozen `ctx.trace = {traceparent, tracestate}`; a malformed value is
   dropped, never a reason to fail a call.
3. **Child runs.** A run started through `execution.Runner` from inside a
   step (with `ParentRun`/`ParentStep`) has no explicit parent, so the engine
   takes the step's context from `ctx`: the child run span is a child of the
   step that started it, in the same trace. An explicit `Invocation.Trace`
   (for example an inbound `traceparent` the application chose to trust)
   wins. The agent catalog's child workflow tools run an unobserved nested
   engine; they emit no spans of their own, but the dispatching step's
   context reaches their tools and workers unchanged.

### Worker protocol: additive field, no minor bump

ADR 0004 requires a client to reject a worker whose minor version is lower.
Bumping to 1.2 would make every 1.1 worker incompatible for data that cannot
change execution. The trace fields are therefore additive inside 1.1:
proto3 peers ignore unknown fields (the Node frame validator only inspects
field 6), a worker that ignores them is conformant, and they never change a
call's outcome. Go validates them before sending (`Call.Validate`: canonical
version-00 traceparent, tracestate at most 256 printable bytes, no
tracestate without traceparent) and counts them in the frame size. If adding
them would push an otherwise admissible call over the negotiated frame
ceiling they are omitted, so tracing never changes whether a call is
dispatched.

### Sampling, labels, queue and failure policy

| Policy | Statement |
| --- | --- |
| Sampling | Head sampling, decided once per trace. A root run is sampled when its trace id falls under `Ratio`, deterministically (OpenTelemetry `TraceIDRatioBased` arithmetic). A run with a parent keeps the parent's flag. Unsampled traces still propagate with flag 00. Zero disables tracing (no ids, no propagation). |
| What sampling affects | Spans and step logs only. Metrics count every run and step. Validation, approval, journaling and outcomes never read it. |
| Span attributes | `blok.workflow`, `blok.step`, `blok.attempt`, `blok.attempt.id`, `blok.run.id`, `blok.parent.run.id`, `blok.parent.step`, `blok.tenant` (only if a valid label), `blok.outcome`, `blok.error.code`, `blok.error.class` (bounded label shapes). Never the principal, inputs, outputs or provider messages. |
| Metric labels | Fixed vocabulary: `blok.workflow`, `blok.step`, `blok.outcome`, `blok.error.class`, `blok.tenant`. Run, attempt and parent ids are never metric labels. `blok.tenant` is the tenant only when it is in `TenantLabels` (at most 64), otherwise `other`. Each instrument keeps at most `MaxSeries` (default 1000, hard 10000) attribute sets; later sets go to `otel.metric.overflow=true` and are counted. |
| Logs | Exported only for sampled traces, correlated by trace/span id. Body is the message bounded to 1 KiB and passed through the shared credential redaction. Attributes are run id, workflow, step, valid tenant and only the keys in `LogAttributes` (at most 32), scalar values bounded to 256 bytes. |
| Run path | `Observe` copies a bounded event into a fixed queue (`QueueSize`, default 4096, hard 65536) with a non-blocking send. It performs no I/O and never waits. |
| Backpressure | Drop newest: a full queue drops the event and counts it (`Stats.Dropped`). Open runs are bounded by `MaxOpenRuns` (default 4096); at the bound the oldest is abandoned and counted. An abandoned span is never ended, so it is never exported as if it completed. |
| Export | One goroutine converts events and exports span and log batches synchronously (`BatchSize` 512, `FlushInterval` 1 s), each call bounded by `ExportTimeout` (default 5 s, hard 30 s). Metrics use a periodic reader with the same timeout. |
| Exporter failure | Optional, always: a failed or timed-out batch is dropped and counted (`SpansFailed`, `LogsFailed`, `MetricFailures`), never retried by the pipeline and never surfaced to a run. Applications should disable or bound their exporter's own retry. Cumulative metrics carry every count in the next successful export. A timed-out export may still be delivered late by the network; it is counted as failed and never resent. No configuration can make export block or fail a run. |
| Shutdown | `Exporter.Shutdown` stops accepting (counted as `DroppedClosed`), drains the queue, abandons open runs and shuts the providers down within its context. Compose it as an `app.Dependency` `Close` so drain flushes telemetry after admitted work. |

`Stats()` reports every counter, and `blok.telemetry.dropped{reason}` exports
the losses as a metric.

### Gates stay authoritative

`Observe` returns nothing and runs after the engine's decisions; the exporter
recovers its own panics. Schema validation, the agent catalog's approval gate
(`tool.Gate`) and evidence checks never consult telemetry or trace context.
Trace context is correlation data: a node that rewrites it (for example a
forged `tracestate`) changes no admission, and exported spans come from the
engine's own allocation, never from a context a node rewrote.

### Module placement: `observe/otel` is a separate Go module

The OpenTelemetry bridge lives in `observe/otel` with its own `go.mod`
(`github.com/well-prado/new-blok/observe/otel`). A package inside the root
module was rejected because Go resolves requirements per module, not per
imported package: the bridge's dependencies would raise every root-module
build's versions even when nothing imports it. Measured with Go 1.27.1 and
OpenTelemetry Go v1.47.0: the root module pins `google.golang.org/grpc`
v1.76.0, `google.golang.org/protobuf` v1.36.10 and `golang.org/x/net`
v0.58.0 (the versions ADR 0004's generated worker bindings record); the
exporter module resolves them to v1.84.0, v1.36.12 and v0.59.0. In the root
module that upgrade would reach the worker transport of applications that
never select telemetry. As a separate module it reaches only applications
that import `observe/otel`.

The bridge package itself imports the SDK (`otel`, `sdk`, `sdk/metric`,
`sdk/log`, `log`, `trace`, `metric`) and no exporter: applications pass the
exporters they choose (`otlptracehttp`, `otlpmetrichttp`, `otlploghttp`, a
gRPC exporter, stdout...), each signal independently, nil meaning not
exported. Nothing registers global providers or reads ambient configuration
for the pipeline; the resource defaults to `service.name=blok`. The SDK's
resource and propagation packages do link `net/http` and `os/exec` into an
exporter-selecting binary; they open nothing.

Pinned versions (`observe/otel/go.mod`, verified with `go mod verify`):
`go.opentelemetry.io/otel`, `otel/trace`, `otel/metric`, `otel/log`,
`otel/sdk`, `otel/sdk/metric`, `otel/sdk/log` v1.47.0; test-only
`otel/exporters/otlp/otlptrace/otlptracehttp` and
`otlpmetric/otlpmetrichttp` v1.47.0, `otlplog/otlploghttp` v0.23.0,
`go.opentelemetry.io/proto/otlp` v1.11.1. During development the module
replaces the root module with `../..`; a release must tag the root first and
require that tag.

## Compatibility

Additive throughout. `inspection.Event` and `Invocation` gain fields with no
wire form; `app.Config` gains `Trace` (zero keeps today's behaviour);
`engine.Result` gains `Trace`; the worker `Call` gains two optional fields
inside protocol 1.1, and the Node SDK's context gains an optional `trace`.
The engine's behaviour without a trace policy and without a payload-free
observer is unchanged. The proto change regenerates the Go bindings (Buf
v1.57.2, protoc-gen-go v1.36.10, protoc-gen-go-grpc v1.5.1) and the Node
bindings and digest (`npm run generate`), which a worker build must pick up.
The Node fixture catalog gains `fixture/trace`, which changes its catalog
digest; tests compute it from discovery.

## Evidence

- No-exporter footprint: `contract/observe/footprint_test.go` builds a Go-only
  application with an observer and full tracing and requires no `net`,
  `crypto/tls`, gRPC, OpenTelemetry or `observe/*` package to be linked, and
  the root `go.mod` to require no OpenTelemetry module;
  `observe/otel/footprint_test.go` shows the same application with the OTLP
  exporters selected does link them, so the check can fail.
- Real exporter integration: `observe/otel` tests use the real OTLP/HTTP
  exporters against an in-process OTLP receiver that decodes the protobuf
  requests (`testdata/cases.json` declares outputs, errors, effects, spans,
  error spans, logs and labels per case, including invalid input, a business
  rejection, an uncertain effect, an unsampled run and an unlisted tenant),
  and, with `BLOK_OTEL_COLLECTOR_IMAGE`, against an actual OpenTelemetry
  Collector container that is stopped, paused and restarted.
- Cardinality, queue saturation and collector outage; trace lineage across Go
  steps, the actual Node worker and a child workflow; approval-gate and
  validation independence; and per-mode latency/RSS (`TestMeasureMode`) are
  recorded with commands and raw figures in the PR.

## Limits

- No trigger extracts an inbound `traceparent` yet; an application that
  trusts one passes it as `Invocation.Trace`.
- Journaled and cluster runs emit no observation events (ADR 0016, #263), so
  they are neither traced nor counted by the exporter.
- Agent catalog child workflows produce no spans of their own; only the
  dispatching step's context reaches their tools.
- The Node worker receives and exposes the context but emits no spans; Node
  logs are correlated through the Go step that dispatched the call.
- The live event stream does not yet declare `PayloadObserver`, so selecting
  it still makes the engine serialize payloads.
- One export goroutine per exporter: at full sampling on the measuring
  machine it could not keep up with ~40,000 serial runs per second and
  dropped about a quarter of events, as the policy requires. Sampled mode
  dropped nothing. These are one developer machine's figures, not capacity
  claims.
- Windows is vetted (`GOOS=windows go vet`) but not executed.
