# ADR 0005: Shared trigger adapter contract and conformance

- Status: accepted
- Date: 2026-10-02
- Roadmap: E09-T01 ([#54](https://github.com/well-prado/new-blok/issues/54))
- Consumers: E09-T02–T08 (#55–#60, #101) and any third-party adapter

## Context

HTTP (E06-T02) and the durable worker (E07-T03) were built before a shared
adapter contract existed. Each made its own choices about completion,
disconnects, error mapping and validation, and nothing stopped a new adapter
from importing the interpreter, retrying failed invocations or reading a
principal from caller data. Seven more protocols are about to be added.

## Decision

### The `trigger` package

`trigger` is a small public package with no protocol, store or engine
dependency. It defines:

- `Kind`: the nine protocols.
- `Declaration`: every adapter package publishes one, stating `Completion`
  (`memory` or `durable`), `Disconnect` (`cancel`, `detach` or `redeliver`)
  and `Authentication` (`caller` or `trusted-producer`). `Validate` enforces
  the pairs each kind may declare:

  | Kind | Allowed completion / disconnect |
  | --- | --- |
  | http, grpc, sse, websocket, mcp | memory / cancel, durable / detach |
  | webhook | durable / redeliver, durable / detach (redeliver added by [ADR 0006](0006-durable-submission-and-webhooks.md)) |
  | worker, cron, pubsub | durable / redeliver |

  Memory work always cancels when its caller goes away. Durable work never
  does: a caller-facing durable adapter stops waiting (`detach`) and a queued
  source redelivers what was not acknowledged (`redeliver`).

  `trusted-producer` is permitted only where application code produces the
  delivery: worker and cron, and pubsub, which may also authenticate its
  publisher as a `caller`. Every other kind must authenticate its caller, so
  a caller-facing adapter cannot opt out of the authentication cases.
- `Principal`: produced only by an adapter's authenticator.
- `TraceIngress`: an adapter's opt-in policy for an inbound W3C trace
  context (#276, ADR 0020). It is read only after admission,
  authentication and validation, and never feeds a principal, a key or a
  route.
- `ErrSaturated` (the same value as `capacity.ErrSaturated`): returned by an
  admission handler without capacity, or by anything a handler calls that
  ran out of capacity: a busy store's `store.ErrBusy` matches it (#190). It
  means the operation that failed committed nothing; it does not mean the
  handler did nothing, so the engine hides it once an earlier step's
  declared effect has committed (ADR 0003). Adapters translate it into
  protocol backpressure and never retry it themselves. An error that
  matches both `ErrSaturated` and a context deadline may be answered as
  either (HTTP and WebSocket check saturation first, gRPC and MCP the
  deadline); both answers are retryable. Once the engine hides
  saturation, the error matches the deadline alone, if it carries one.
- `Classified` / `Classify`: a stable public `ErrorCode()`/`ErrorClass()` pair.
  `internal/engine.Error` and `node.DomainError` implement it, so adapters map
  domain errors without importing the engine. Codes are source-visible, so
  `Classify` reports a code only if it is a short lowercase identifier
  (`^[a-z][a-z0-9_.]{0,63}$`); anything else, such as a code built from
  caller or secret data, falls back to the adapter's generic error.
- `Binding` / `CheckBindings`: several bindings, of one or several kinds, may
  target the same workflow. A binding's `InputSchema` is the value it hands the
  workflow *after* mapping, not its protocol schema. Startup rejects a binding
  that targets another workflow, uses an unknown kind or duplicates an id, and
  any binding for which it cannot prove that every value the binding can
  produce is accepted by the workflow schema: same type and wire form,
  matching nested properties and array items, bounds inside the workflow's
  bounds, no nullable value into a non-nullable field, no field (or open
  object) the workflow's closed object rejects, every workflow-required field
  required, and every `anyOf` branch accepted. Optional workflow fields may be
  omitted.

### Retry ownership

"No adapter owns a retry engine" means an adapter invokes the workflow handler
exactly once per delivery or claim and never loops on a failed or saturated
invocation. Step and attempt retries belong to the engine/journal. A queued
source (worker, pubsub, cron) does own *delivery* transfer: lease, redelivery
of unacknowledged work, an attempt budget and dead letter, as the architecture
assigns. Conformance distinguishes the two by counting dispatches per delivery.

Redelivery is bounded. In the worker, a handler failure consumes an attempt;
saturation and a lost consumer consume a separate deferral budget instead
(`MaxDeferrals` = 16, backoff 1s doubling to a 1-minute cap), after which the
job is dead-lettered with `deferral_budget_exhausted`. Saturation that names
the write domain the job's own claim holds is not backpressure but a nested
submission, and fails the job (ADR 0006, #207).

### Drain timeout

A caller-facing adapter (HTTP, webhook, SSE, gRPC, WebSocket, MCP) holds an
application lease (`app.Application.Begin`) for the work it admits, and
`app.Shutdown` waits for every lease up to `DrainTimeout` before it closes
the application's dependencies. The queued sources (worker, pubsub, cron)
take no leases: they are loops driven by their context
(`worker.Queue.ProcessOnce`, `pubsub.Consumer.Run`, `cron.Scheduler.Run`),
and they stop with the application only if the host runs each as an
`app.Dependency` whose `Close` cancels that context and waits for the
loop. Work can outlive that wait.
Shutdown then cancels it rather than closing the store under it (#177):

- Every such adapter derives the admitted work's context from its lease
  (`Lease.Bind`) right after admission, so authentication is covered too.
  When the drain times out, that context is canceled with cause
  `app.ErrDrainTimeout`; work bound after that starts canceled.
- `Shutdown` then waits up to `AbortGrace` (1 s by default) for the canceled
  work to release its leases, and only then closes the dependencies; both
  waits end early if `Shutdown`'s own ctx does, so a caller's deadline also
  bounds the grace. Work that ignores its context beyond that still meets
  closed dependencies: it fails with their error, and nothing it was
  writing commits.
- `app/deploy.Deployment.Run` has two bounded phases. Its configured
  `deployment.Config.DrainTimeout` bounds HTTP server drain. If that expires,
  it cancels request contexts with `app.ErrDrainTimeout`, closes the listener
  and connections, then gives the application at most its configured
  `AbortGrace` for canceled handlers to release their leases. The overall
  handler-drain bound is therefore the deployment drain timeout plus the
  application abort grace. A handler that ignores cancellation beyond that
  grace may still be running when dependencies close; native application code
  is trusted code, so the host cannot guarantee it stops. Dependencies must
  honor their close context. `Deployment.Run` treats its `ctx` cancellation
  or signal as the shutdown trigger; the configured two phases bound shutdown
  after that trigger. Direct `app.Shutdown(ctx)` instead keeps the caller's
  context as the bound for both of its waits.
  Only admitted work holds the HTTP drain open. Once admission is closed,
  `Run` closes every connection that has not yet delivered a request (a
  client's spare or speculative dial): it carries no admitted work, and
  `http.Server.Shutdown` alone would keep it for five seconds, timing out a
  drain whose real work had finished (#194). Such a connection gets no
  answer; its request was never admitted, so retrying it is safe.
- Aborted work may already have committed something, so it is answered as
  a retry invitation only where a retry is harmless. The durable starts
  (webhook, SSE) are keyed, so they answer 503 `unavailable` with
  `Retry-After`; a final answer the submission already had (conflict,
  invalid input) still wins. The in-band adapters have no key, and answer
  as the cancellation it is, which clients do not retry by default (the
  rule #190 set for a failure after an effect): HTTP 504, gRPC `Canceled`,
  MCP and WebSocket `canceled` (HTTP 504 when the handler returns the
  context's error; any other unclassified error stays 500). A handler that
  returned success is still answered with it. An aborted WebSocket
  `OnConnect` closes with 1013 (try again later); the disconnect workflow
  that follows starts canceled.

The alternative considered was to require `DrainTimeout` to cover every
endpoint's read and submit bounds. It was rejected: it ties the
application's configuration to every adapter's knobs, and it still fails
for a dependency that hangs.

### Host shutdown order

A host that runs several adapters over one application stops them in this
order, which `trigger/nine_test.go` drives with work in flight (#173):

1. **The adapters.** `sse.Server.Shutdown` ends subscriptions,
   `websocket.Server.Shutdown` closes connections as going away (1001),
   `mcp.Server.Shutdown` refuses new sessions and calls and waits for the
   calls in flight and their answers (#197), the gRPC server's `GracefulStop` finishes its calls, and
   the HTTP server's `Shutdown` finishes its requests (HTTP, webhook, SSE
   starts). The order matters: SSE subscriptions keep their connections
   active, so the HTTP server's `Shutdown` would
   wait on them until its deadline if they were not ended first. The
   application stays ready meanwhile, so work they hold completes and is
   answered, and until the HTTP server's `Shutdown` runs, HTTP, webhook and
   SSE starts are still admitted (an SSE start admitted then is followed
   from another instance, or after the restart). A WebSocket message in
   flight is not answered: closing the connection cancels it, and the
   client resends; the websocket package's own shutdown tests cover that.
2. **The application.** `app.Shutdown` drains what is left. The queued
   sources (cron, pubsub, the worker pool) are registered as its
   dependencies, after the store if the store is one, so they stop before
   it: a worker job in flight is canceled with its consumer, rolled back and
   left pending without spending an attempt.
3. **The store and broker connections**, closed last, by the application as
   its first dependencies or by the host after `app.Shutdown` returns.

The queued sources stop with the application only because the host
registers them as dependencies; nothing else stops them.

### Conformance harness

`contract/conformance.RunTrigger` runs a versioned, embedded corpus
(`LoadTriggerCorpus`, version 1, 19 cases) against a `TriggerDriver` written by
the adapter author. The supplied corpus must contain every embedded case
unchanged (an adapter may add cases, never drop or weaken one), and a run in
which no case applies fails. It imports only `trigger` and the standard library, so an
adapter outside this module can run it. The harness owns the workflow handler
and the authenticator; the driver must reach them through the real protocol.

It checks, in order: a valid declaration; the corpus; required capabilities
(durable adapters: `Restarter` and `EffectLedger`; redeliver adapters:
`Recoverer`; trusted-producer adapters with a configured principal:
`PrincipalSource`, added by [ADR 0007](0007-pubsub-delivery-transfer.md));
no goroutine or listener created by construction; per case the
outcomes, stable codes, absence of internal error text, in-band output for
memory completion, verified principal on every dispatch (none for trusted
producers), dispatched input equal to a delivered payload, cancellation as
declared, that an effect is committed by the time durable completion is
reported, and predeclared output/error/effect/dispatch/duplicate counts; after
`Stop`, no listener, no dispatch and no goroutine started during the run still
alive. Goroutines are tracked by identity and stack, not by count, so an
unrelated goroutine exiting cannot hide a leaked one, and the failure names
the leaked goroutine's stack.

The acknowledgment check is a state check, not an ordering proof: it
observes committed effects when `Deliver` returns. An adapter that
acknowledges in one transaction and commits in a later one before returning
is caught only by crash evidence such as the worker's process-kill test.

A case whose `applies` filter does not match the declaration is reported as
not applicable with a reason, never as passed. Failures are
`*TriggerFailure` with stable codes (`invalid_declaration`,
`construction_side_effect`, `listener_before_start`, `outcome_mismatch`,
`code_mismatch`, `error_leak`, `output_mismatch`, `principal_mismatch`,
`input_mismatch`, `cancellation_mismatch`, `ack_before_commit`,
`<count>_count_mismatch`, `dispatch_after_stop`, `goroutine_leak`,
`capability_missing`, `dispatch_outlived_delivery`, `recover_failed`,
`corpus_mismatch`, `corpus_incomplete`, `no_applicable_cases`).

### Static modularity checks

`internal/tooling/graphcheck` now also runs against this repository
(`TestRepositoryGraph`) and reports:

- `trigger_import_forbidden` / `trigger_transitive_import_forbidden`: a
  package under `trigger/` reaches `internal/engine`, `internal/compile`,
  `internal/program`, `internal/journal` or `flowtest`.
- `trigger_declaration_missing`: an adapter package (every package under
  `trigger/`, at any depth, except `trigger` itself and `internal`
  helpers) has no top-level `var Declaration` of type `trigger.Declaration`.
- `trigger_conformance_missing`: its tests never *call*
  `conformance.RunTrigger` (aliased and dot imports are recognized; a bare
  reference does not count).

### The not-found class (#306)

A record the caller may not see has to be answered without saying why: a
record that does not exist and one that belongs to another principal must
look the same. Before #306 the only way to say that was class `validation`
with code `not_found`, which HTTP answers 400 ("you sent a bad request")
and gRPC `InvalidArgument`. Clients, caches and retry logic branch on that
status, so they took the wrong path.

`node.ClassNotFound` and `trigger.ClassNotFound` are the class
`not_found`. Two constants carry the one wire string because `trigger`
does not import `node` (adapters map classified errors without the
authoring packages); a test pins that they agree. The class sits beside the
existing ones (`validation`, `admission`, `cancellation`, `configuration`,
`persistence`, `uncertain`, ...) and means "this record is not visible to
you". It does not mean "the input is malformed" (that stays `validation`),
and it is not an admission or configuration refusal.

**Indistinguishability.** The framework guarantees that a not-found answer
carries the status its adapter gives the class and the error's stable code,
and nothing else: never the error's text or its wrapped cause, and no
header, detail or field that depends on them. A node returns the same
error, code and class, for "does not exist" and "is not yours"; the two are
then byte-identical on every adapter apart from the per-request id HTTP adds
to every error body. The code is the node's, so an entity code such as
`order_not_found` is kept; `not_found` is the recommended one. A code that
is not a stable identifier falls back to the adapter's generic error, as for
every class.

| Adapter | Not-found from the workflow | Why |
| --- | --- | --- |
| HTTP | 404, `{"error":<code>,"requestId":…}`, no `Retry-After` | HTTP's not-found. The router's own 404 for an unknown route keeps its `{"error":"not found"}` body; route existence is public |
| gRPC | `NotFound`, message and `ErrorInfo` reason `<code>` | gRPC's not-found ([ADR 0013](0013-grpc-bindings.md)) |
| WebSocket | reply `{"id":…,"error":<code>}`; an `OnConnect` refusal closes 1008 with the code | the reply protocol has no status. There is no not-found close code, and the adapter does not invent one in the private 4000–4999 range |
| MCP | `isError` tool result `{"code":<code>}` | MCP reports a tool's own failure in the result. JSON-RPC errors are for protocol failures, and the spec's -32002 is for `resources/read`, not `tools/call` |
| SSE | final `failed` event `{"code":<code>}` | the start answers the committed submission before the workflow runs; its status cannot carry the outcome |
| Webhook (and pub/sub, cron, worker) | the provider already got 202; the job ends dead after its one attempt with the code as its error | the event is acknowledged when committed; the workflow's outcome is the job's |

A `Submitter` (webhook, SSE) that itself returns a not-found error is not a
sentinel those adapters map: it is answered 500 `internal`, as any other
unexpected admission failure, and a provider redelivers.

**Retry semantics.** The class is terminal and never invites a retry. HTTP
answers a 4xx without `Retry-After`, never the 5xx that clients and proxies
retry; `NotFound` is not in the transient set gRPC retry policies list. The
worker retries only a `HandlerError` the handler marks retryable, so the
class does not make a job retryable. The in-band runner writes `FailRun`
with the code and class, never `MarkRunUncertain`. The distributed runtime
leaves a run replayable only for a non-overflow `persistence` failure or a
context error, so a not-found run ends failed. One existing rule still
applies: in a journaled (distributed) run, a failure of a node that declares
effects is `effect_outcome_uncertain`, whatever its class, because the
engine cannot take the node's word that nothing happened (ADR 0003). A
lookup that should answer not-found there must not declare effects.

**Telemetry and redaction.** The class reaches inspection events, the
journal, the OTel span and the `blok.error.class` label of `blok.steps`
unchanged as `not_found`. It is a valid bounded label, `redact.Sensitive`
does not match it, and the inspection projection (#80) keeps it. No metric
vocabulary changes: `blok.error.class` was already an open, series-bounded
label (ADR 0022).

**Remote nodes.** The runtime protocol's `ErrorClass` enum has no
not-found value, so a node running in an external runtime cannot return
this class yet: its failure arrives as one of the enum's classes. Adding the
value is a wire change of its own.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| New `trigger` package, `conformance.RunTrigger` and corpus | additive | none |
| `Classified` methods on `engine.Error`, `node.DomainError` | additive | none |
| `http.Principal` is an alias of `trigger.Principal` | source-compatible | none |
| `http.New` rejects an unparsable endpoint schema | behavioral | previously every request returned 500; fix the schema |
| HTTP validates an empty body as `null` when a schema is set | behavioral (fix) | required fields can no longer be bypassed by omitting the body |
| HTTP maps `trigger.ErrSaturated` to 503 + `Retry-After` | additive | none |
| `worker.Queue.RegisterKind`, `ErrInvalidPayload` | additive | unregistered kinds keep accepting any JSON |
| Worker job id derived from the request key | behavioral (fix) | ids are opaque; existing rows keep theirs. Clock-derived ids collided for equal payloads under a coarse clock |
| Worker defers `ErrSaturated` and lost consumers against a bounded deferral budget instead of attempts | behavioral (fix) | saturation was dead-lettered. Adds `Job.Deferrals` and a `deferrals` column; `worker.New` migrates existing queues in place |
| Worker consumer loss returns `ErrConsumerLost` after a synchronous rollback | behavioral (fix) | the claim was aborted by `database/sql` in the background; an immediate redelivery hit `SQLITE_BUSY` in 7 of 400 contended conformance runs (0 of 400 after the fix) |
| Worker dead-letter text may be a classified error code | behavioral | only codes `Classify` accepts as stable identifiers; never arbitrary error text |
| `trigger.ErrSaturated` is `capacity.ErrSaturated`; `store.ErrBusy` matches it (#190) | behavioral | a busy store's error, reaching an in-band trigger, is answered as saturation instead of an internal error. Code that read `errors.Is(err, ErrSaturated)` as "refused before admission, nothing happened" now also sees "a store transaction committed nothing" |
| The engine hides saturation after an earlier step's declared effect (#190) | behavioral | such a failure keeps its code and matches everything else it carried, but no longer `ErrSaturated`; its text gains `(after step "<id>" committed its effects)`. It is answered as a failure, never as a retryable refusal. A worker hosting such a workflow fails or dead-letters the job instead of deferring it |
| Agent catalog workflow tools' dispatch steps declare their child's effects (#190) | behavioral | lets the engine hide saturation after a child's effect; dispatch steps are internal, so nothing else observes it |
| `store.ErrBusy` text is `store: busy` | behavioral | log text only; match it with `errors.Is` |
| `app.Lease.Context`/`Bind`, `app.Aborted`, `Config.AbortGrace` | additive | none |
| `app.Application.AbortGrace()` | additive | none; reports the effective configured grace for host shutdown orchestration |
| A drain timeout cancels admitted work, waits up to `AbortGrace`, then closes the dependencies (#177) | behavioral (fix) | work that outlived `DrainTimeout` used to run on into closed dependencies. Handlers should observe their context |
| HTTP's draining 503 carries `Retry-After` | additive | none |
| `Deployment.Run` closes request-less connections when drain begins (#194) | behavioral (fix) | a connection opened but not yet carrying a request is closed instead of answered. Its request was never admitted. A drain that used to report `application_drain_timeout` because of such a connection now ends when the admitted work does |
| `node.ClassNotFound`, `trigger.ClassNotFound` (#306) | additive | none |
| HTTP answers class `not_found` 404 with its code (#306) | behavioral | it was a 500 `internal error` that hid the code. No code in this repository used the class; code `not_found` with class `validation` still answers 400 until it switches class |
| gRPC answers class `not_found` `NotFound` (#306) | behavioral | it was `FailedPrecondition` with the same reason |

## Limits

- Only HTTP (memory/cancel/caller) and worker (durable/redeliver/trusted
  producer) exist. The `detach` cases are exercised by a synthetic reference
  adapter until E09-T02 adds a real durable caller-facing adapter.
- The worker had no producer principal; [ADR 0006](0006-durable-submission-and-webhooks.md)
  adds one, persisted with each job.
- Goroutine checks are process-wide; `RunTrigger` must not run in parallel with
  other tests. A goroutine started and finished during construction is not
  detected.
- `listener_before_start` relies on the driver's `Endpoint` report. The
  selection binaries prove at link level that an unselected adapter, `net`
  and `net/http` are absent.
- Acknowledgment is checked as committed state when completion is reported,
  through a committed effect ledger; it is not an ordering proof.
- HTTP validates the body against the schema but hands the handler the raw
  body, so schema defaults and wire forms are not applied by the adapter.
- graphcheck matches `Declaration` syntactically (by import path and type
  name); it does not type-check.
- The worker's claim transaction no longer observes the consumer's context;
  only the handler does. A claim statement is bounded by the store's busy
  timeout rather than by consumer cancellation. The consumer-loss lock race is
  evidenced statistically under contention, not by a deterministic
  reproduction.
- The corpus does not cover TLS, load, slow clients or protocol-specific
  framing; each E09 task adds its own protocol integration evidence.
