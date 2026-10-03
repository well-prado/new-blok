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
job is dead-lettered with `deferral_budget_exhausted`.

### Drain timeout

A caller-facing adapter (HTTP, webhook, SSE, gRPC, WebSocket, MCP) holds an
application lease (`app.Application.Begin`) for the work it admits, and
`app.Shutdown` waits for every lease up to `DrainTimeout` before it closes
the application's dependencies. The queued sources (worker, pubsub, cron)
run as dependencies instead, and stop their work in their own `Close`. Work can outlive that wait.
Shutdown then cancels it rather than closing the store under it (#177):

- Every such adapter derives the admitted work's context from its lease
  (`Lease.Bind`). When the drain times out, that context is canceled with
  cause `app.ErrDrainTimeout`.
- `Shutdown` then waits up to `AbortGrace` (1 s by default) for the canceled
  work to release its leases, and only then closes the dependencies. Work
  that ignores its context beyond that grace still meets closed
  dependencies: it fails with their error, and nothing it was writing
  commits.
- An adapter answers aborted work (`app.Aborted(ctx)`) as *unavailable*, a
  retryable answer: HTTP 503 `application unavailable` and webhook and SSE
  start 503 `unavailable`, all with `Retry-After`; gRPC `Unavailable
  unavailable`; WebSocket and MCP `unavailable`, and an aborted WebSocket
  `OnConnect` closes with 1013. An answer the work already had that is
  final (conflict, invalid input) still wins.

The alternative considered was to require `DrainTimeout` to cover every
endpoint's read and submit bounds. It was rejected: it ties the
application's configuration to every adapter's knobs, and it still fails
for a dependency that hangs.

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
| A drain timeout cancels admitted work, waits up to `AbortGrace`, then closes the dependencies (#177) | behavioral (fix) | work that outlived `DrainTimeout` used to run on into closed dependencies. Handlers should observe their context |
| HTTP's draining 503 carries `Retry-After` | additive | none |

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
