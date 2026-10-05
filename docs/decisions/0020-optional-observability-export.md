# ADR 0020: Optional observability export and trace propagation

- Status: implementation in review for E16-T01 (#79)
- Date: 2026-10-05
- Roadmap: E16-T01 (#79); builds on ADR 0016 (E15-T01 #76, E15-T02 #77) and
  ADR 0004 (worker protocol, hardened by E08-T04 #53). Inbound trace context
  at the trigger boundary: #276.
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
   dropped, never a reason to fail a call. "Never changes the outcome" is a
   framework guarantee: the engine, the worker adapter and the protocol make
   no decision from trace context. A node that reads `ctx.trace` and returns
   or acts on it (like the `fixture/trace` test node, which is therefore
   declared non-deterministic) makes its own output depend on random trace
   ids; that is the node's choice, not the framework's.
3. **Child runs.** A run started through `execution.Runner` from inside a
   step (with `ParentRun`/`ParentStep`) has no explicit parent, so the engine
   takes the step's context from `ctx`: the child run span is a child of the
   step that started it, in the same trace. An explicit `Invocation.Trace`
   (for example an inbound `traceparent` the application chose to trust)
   wins. This boundary is met for runner child runs only. The agent
   catalog's child workflow tools run an unobserved nested engine (#260's
   guard deliberately keeps observers out of it); they emit no spans of
   their own (#275), though the dispatching step's context reaches their
   tools and workers unchanged.

### Inbound trace context at the trigger boundary (#276)

A fourth boundary: an upstream service's `traceparent`/`tracestate`. Every
adapter that has a carrier can read it, through one shared policy,
`trigger.TraceIngress{Extract, Sampling}`, over one parser,
`observe.ExtractTrace`, and one sampling rule, `TracePolicy.Inbound`.

**Default: off.** The zero `TraceIngress` reads nothing. The alternative,
on by default with the inbound sampled flag ignored, was rejected for three
reasons. An inbound trace id is caller data: an edge application that joins
it lets any caller place its runs inside a trace of the caller's choosing
(or collide with someone else's), which is the application's decision to
make, not the framework's; `Invocation.Trace` already meant "a parent the
application chose to trust", and opt-in keeps that meaning. It keeps
today's behaviour for every existing application (a run behind a trigger is
a root). And "on, ignore sampling" still lets the caller pick the trace id;
only "off" means nothing the caller sent reaches telemetry. Extraction is
also a no-op when the application does not trace (`TracePolicy` zero), so
"zero disables tracing, no propagation" stays true for inbound context.

**Sampling policy.** `Sampling` is explicit:

| `observe.InboundSampling` | The run's sampled flag |
| --- | --- |
| `IgnoreInboundSampling` (zero, the default once `Extract` is set) | Decided locally: sampled with probability `Ratio`, from a fresh random draw. The run still joins the inbound trace id and parent span. |
| `HonorInboundSampling` | The caller's flag, as OpenTelemetry `ParentBased` does. For callers trusted to decide how much the application records. |

Under the ignore policy the draw is deliberately **not** `Samples(inbound
trace id)`: that arithmetic is deterministic in an id the caller chooses, so
a caller could pick ids under the ratio and be sampled every time. The cost
is that two Blok ingresses that see the same foreign trace may decide
differently; under the honor policy they agree. This answers the #271 review
note: an inbound sampled flag cannot force 100% sampling unless the
application chose `HonorInboundSampling`. An inbound `00` cannot opt out of
the local ratio either.

**What a carrier must look like** (`observe.ExtractTrace`). It never
fails; anything it cannot use is ignored and the work proceeds as a root
run.

- Exactly one `traceparent` value (one header line, metadata value or
  message header). None, two (even identical ones) or a comma-joined pair
  yields no context: W3C defines a single field, and picking one of several
  would let an intermediary choose. Values are never merged.
- At most `MaxTraceparentBytes` (256) of printable ASCII, then
  `ParseTraceparent`: version `ff`, upper-case hex, zero ids and a version-00
  value that is not exactly 55 bytes are rejected; a later version's extra
  fields are ignored.
- Exactly one `tracestate` value of at most 256 printable bytes is kept.
  More than one, an oversized or a non-printable one is dropped and the
  `traceparent` is still used, as W3C allows.

**Where each adapter reads it, and where it goes.** Always after routing,
admission, authentication, authorization and input validation, so it
cannot change any of them:

| Trigger | Carrier | Run parent travels via |
| --- | --- | --- |
| HTTP | request headers | the handler's context (`observe.WithTrace`); the engine already joins `TraceFrom(ctx)` when `Invocation.Trace` is not set |
| gRPC | incoming metadata | the handler's context |
| WebSocket | upgrade request headers, resolved once per connection | the context of `OnConnect`, every `OnMessage` and `OnDisconnect` |
| MCP | headers of the HTTP request carrying the `tools/call` | the context given to `Catalog.Invoke` |
| Webhook | request headers (not covered by the provider signature) | `Submission.Trace` → job record |
| SSE start | request headers | `Submission.Trace` → job record |
| Pub/sub | message headers, when the driver's message implements `pubsub.TraceCarrier` (`natsjs` does, collecting every spelling of the case-sensitive NATS names) | `Submission.Trace` → job record. The consumer has no application, so `Subscription.TracePolicy` carries the policy |
| Worker | `EnqueueRequest.Trace` / `Submission.Trace`, stored with the job | the handler's context (`observe.TraceFrom`) and `Job.Trace` |
| Cron | none: an occurrence has no upstream | deliberately nothing. A tick's own context is never captured; the run is a root |

**Queue: the trace travels with the job record.** `worker.New` adds two
columns, `traceparent` and `tracestate` (`TEXT NOT NULL DEFAULT ''`), with
the same idempotent in-place migration as `principal_json` and
`enqueue_seq` (ADR 0006; `migration.Retry`). Jobs from before the columns
have no trace. The trace is correlation, not identity: it is not in the
payload digest or the duplicate comparison, so a provider redelivery with
a new `traceparent` is a duplicate (never a conflict), and the first
committed trace is kept. An invalid context is not stored, an invalid
tracestate is dropped, and a stored value that does not parse is ignored
when the job runs. The queue keeps the stored sampled flag: its producers
are trusted application code (ADR 0005), and an ingress adapter in front of
it has already applied its policy.

**Never authority.** Admission, authorization, idempotency keys, payload
digests and routing are decided before the carrier is read, and none of
them reads it. `trace_ingress_test.go` checks this differentially: the same
request with and without a forged context (a crafted sampled id with
`tracestate: admin=true,role=root`, an unsampled flag, duplicated
traceparents, an oversized tracestate) is answered the same, runs the same
input to the same output under the same principal, is refused the same when
its credentials are wrong, gets the same durable key, and a repeat that
differs only in its trace is a duplicate that changes nothing.

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
| Span attributes | `blok.workflow`, `blok.step`, `blok.attempt`, `blok.attempt.id`, `blok.run.id`, `blok.parent.run.id`, `blok.parent.step`, `blok.tenant`, `blok.outcome`, `blok.error.code`, `blok.error.class`. Every string value is a bounded label shape: names and codes that are not valid labels become `invalid`; identifiers (run, parent run, attempt ids) are kept when they are valid labels (at most 64 bytes of `[A-Za-z0-9._/:-]`) and otherwise exported as `h:` plus 16 hex digits of their SHA-256, which a reader computes from the inspection id. An id that itself starts with `h:` is always hashed, so a raw id can never equal another id's digest. Never the principal, inputs, outputs or provider messages. |
| Tenants | One allowlist (`TenantLabels`, at most 64) for metrics, spans and logs: any other tenant is exported as `other`. `RawTenants: true` is an explicit opt-in that exports valid-label tenants as-is on spans and logs only; metrics always use the allowlist. The default is the privacy-safe one because tenant identifiers can be personal data. |
| Metric labels | Fixed vocabulary: `blok.workflow`, `blok.step`, `blok.outcome`, `blok.error.class`, `blok.tenant`. Run, attempt and parent ids are never metric labels. `blok.tenant` is the tenant only when it is in `TenantLabels` (at most 64), otherwise `other`. Each instrument keeps at most `MaxSeries` (default 1000, hard 10000) attribute sets; later sets go to `otel.metric.overflow=true` and are counted. |
| Logs | Exported only for sampled traces, correlated by trace/span id. Body is the message bounded to 1 KiB and passed through the shared credential redaction. Attributes are the bounded run id, workflow, step, the tenant per the tenant policy, and only the keys in `LogAttributes` (at most 32), scalar values bounded to 256 bytes. |
| Run path | `Observe` copies a bounded event into a fixed queue (`QueueSize`, default 4096, hard 65536) with a non-blocking send. It performs no I/O and never waits. |
| Backpressure | Drop newest: a full queue drops the event and counts it (`Stats.Dropped`). Open runs are bounded by `MaxOpenRuns` (default 4096); at the bound the oldest is abandoned and counted. An abandoned span is never ended, so it is never exported as if it completed. |
| Export | One goroutine converts events and exports span and log batches (`BatchSize` 512, `FlushInterval` 1 s). Each export call runs on its own goroutine and the pipeline waits for it at most `ExportTimeout` (default 5 s, hard 30 s) or until Shutdown gives up, even if the exporter ignores its context. One call per signal may be in flight: while an abandoned call is still inside an exporter, later batches of that signal fail fast and are counted, so a stuck exporter costs at most one goroutine per signal. Metrics use a periodic reader whose exports go through the same bound. |
| Exporter failure | Optional, always: a failed or timed-out batch is dropped and counted (`SpansFailed`, `LogsFailed`, `MetricFailures`), never retried by the pipeline and never surfaced to a run. Applications should disable or bound their exporter's own retry. Cumulative metrics carry every count in the next successful export. A timed-out export may still be delivered late by the network; it is counted as failed and never resent. No configuration can make export block or fail a run. |
| Shutdown | `Exporter.Shutdown(ctx)` stops accepting (`DroppedClosed`), drains the queue, abandons open runs, then shuts the providers and every selected exporter down, all bounded by `ctx`. If `ctx` expires first, in-flight exports are canceled, the export loop exits at once, events still queued are discarded and counted (`DroppedShutdown`), the providers and exporters are still shut down (each exporter at most once, with the expired `ctx`), and `ctx`'s error is returned. After it returns, `Accepted = Processed + DroppedShutdown` and no pipeline goroutine remains except calls still inside an exporter that ignores its context, which end when that exporter returns. Compose it as an `app.Dependency` `Close` so drain flushes telemetry after admitted work. |

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
`go.opentelemetry.io/proto/otlp` v1.11.1.

During development the module requires the root at the placeholder `v0.0.0`
and replaces it with `../..`. That works only inside this repository: an
external `go get` of `observe/otel` fails with `unknown revision v0.0.0`.
`TestModuleReleaseReadiness` requires the placeholder and the replace to
change together and fails under `BLOK_RELEASE=1` while either is present.
Release checklist for `observe/otel`:

1. Tag the root module (`vX.Y.Z`).
2. In `observe/otel/go.mod`, require `github.com/well-prado/new-blok vX.Y.Z`
   and delete the `replace`; `go mod tidy && go mod verify`.
3. Run the module's gates with `BLOK_RELEASE=1` (the readiness test must pass).
4. Tag the module as `observe/otel/vX.Y.Z`.

## Compatibility

Additive throughout. `inspection.Event` and `Invocation` gain fields with no
wire form; `app.Config` gains `Trace` (zero keeps today's behaviour);
`engine.Result` gains `Trace`; the worker `Call` gains two optional fields
inside protocol 1.1, and the Node SDK's context gains an optional `trace`.
#276 adds, all additively: `observe.ExtractTrace`, `MaxTraceparentBytes`,
`InboundSampling` and `TracePolicy.Inbound`; `trigger.TraceIngress` and the
`Trace` field on every adapter's endpoint, binding, config or subscription
(zero keeps today's behaviour); `trigger.Submission.Trace`,
`worker.EnqueueRequest.Trace` and `worker.Job.Trace` with two queue columns
(ADR 0006); and the optional `pubsub.TraceCarrier`, which existing drivers
need not implement.
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
- Bounded shutdown (`shutdown_test.go`): a stalled exporter that honours and
  one that ignores its context, BatchSize 8, ExportTimeout 2 s, 32 runs and a
  100 ms Shutdown: Shutdown returns by its deadline, the loop is gone, the
  exporter is shut down once, counters reconcile, and stack-counted pipeline
  goroutines return to baseline (after the stuck exporter returns, for the
  ignoring case); a stuck exporter never accumulates goroutines.
- Tenant allowlist on spans and logs and bounded identities
  (`privacy_test.go`, including a run id spelled as another run's digest),
  the Observe/Shutdown race (`handshake_test.go`: four producers against
  Shutdown with expired, 1 ms, 10 ms and unbounded deadlines, 300 times), and
  the release-readiness check (`release_test.go`).
- Inbound trace context (#276): `trigger/trace_ingress_test.go` drives a
  real request through HTTP, gRPC, WebSocket, MCP, webhook, SSE and pub/sub
  (durable ones through the real `worker.Queue`) into `execution.Runner`
  with an observer and a `TracePolicy`, and checks the engine's root span:
  it is a child of the inbound span with its tracestate; a fresh root with
  extraction off, with no `TracePolicy`, for cron and for every malformed
  carrier (duplicated, comma-joined, version `ff`, upper-case, zero ids,
  non-ASCII, oversized, truncated, empty, tracestate alone; an oversized,
  duplicated or non-ASCII tracestate drops only the tracestate). Under the
  ignore policy a forced-sampled crafted id at ratio 1e-12 is never sampled
  and 400 forced-sampled requests at ratio 0.25 sample about a quarter; the
  honor policy samples all of them, so the check can fail. The differential
  check is described above. `contract/observe/ingress_test.go` covers the
  parser and the policy, and `trigger/worker/trace_test.go` the job record,
  its identity, malformed stored values and the migration of a pre-#276
  queue.
- Cardinality, queue saturation and collector outage; trace lineage across Go
  steps, the actual Node worker and a child workflow; approval-gate and
  validation independence; and per-mode latency/RSS (`TestMeasureMode`) are
  recorded with commands and raw figures in the PR.

## Limits

- Inbound trace extraction is opt-in per endpoint (#276). MCP reads the
  carrying HTTP request's headers, not a `traceparent` in the call's
  `_meta`. A job enqueued from inside a traced step does not capture that
  step's context by itself; the producer passes `EnqueueRequest.Trace`.
  Workers from before #276 on the same store neither write nor read the
  trace columns, so jobs they enqueue start root runs.
- The NATS driver's header reading is tested against a fake JetStream
  message; the end-to-end pub/sub ingress test uses the in-memory source,
  because no NATS server was available where #276 was measured.
- Journaled and cluster runs emit no observation events (ADR 0016, #263), so
  they are neither traced nor counted by the exporter.
- Agent catalog child workflows produce no spans of their own (#275); only
  the dispatching step's context reaches their tools.
- `observe/otel` is not consumable outside this repository until the release
  checklist above is followed.
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
