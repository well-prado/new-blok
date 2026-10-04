# E20-T01 executable parity evidence

This package compares executable behavior, not equal instruction counts. The
old side is the published Blok 2.5.0 `runWorkflow()` path through its actual
`Configuration` and `Runner`; the new side calls `flow.Define`, `flow.Call`,
`Lower`, and `internal/engine.Engine.Run`. Both call a per-test loopback HTTP
provider fixture. No external provider, credentials, paid API, or GitHub Action
is involved.

## Pinned environment

- Old framework: Blok tag `v2.5.0`, commit
  `7611e434f716a5a8efbed26613e546893d5bcba7`, run from a clean detached isolated
  worktree; the user's primary old checkout is not used or changed.
- The isolated old source checkout passed the required root `bun run build`
  (33 Nx projects plus `scripts/fix-esm-extensions.ts`). The first attempt
  exposed an incomplete Hono install; installing `hono@4.11.7` with npm
  `--no-save --no-package-lock` repaired only that worktree's ignored dependency
  tree. No old source or manifest was changed; its tracked status remains clean.
- Local machine for the focused checks: macOS 27.0 arm64, Node `v24.21.0`, Bun
  `1.3.14`, Go `1.27.1 darwin/arm64`; the new-framework source base is
  `0baf071a7136e9c1f4371bc9a8b85da41eeedb8e` (current `origin/main`, including
  #71 package resolution). These are not a production deployment topology.
- Published old packages: `@blokjs/core`, `@blokjs/runner`, `@blokjs/shared`,
  `@blokjs/trigger-sse`, `@blokjs/trigger-webhook`, and
  `@blokjs/trigger-worker` all exactly `2.5.0`; transitive `@blokjs/helper`
  `2.5.5`; `hono` `4.11.7`; `zod` `3.25.76`; Node `24.21.0`. The separately
  gated durable-recovery process also pins `pg-boss` `10.4.2` and `pg` `8.23.1`,
  and records the PostgreSQL server version queried read-only.
- Local new framework: Go module `github.com/well-prado/new-blok`, current
  issue branch revision recorded by `git rev-parse HEAD`; Go version recorded
  by `go version`. The parity harness is in-process for the native engine.
- Fixtures: `testdata/parity/contracts.json`; all provider calls target an
  ephemeral `httptest` loopback listener and all expected calls/effects are
  declared before execution.

## Reproduce

From the new repository root:

```sh
GOMAXPROCS=2 go test -p=1 ./migration -count=1 -v
GOMAXPROCS=2 go test -p=1 ./benchmarks/parity -count=1 -v
BLOK_PARITY_PERF=1 GOMAXPROCS=2 go test -p=1 ./benchmarks/parity -run '^TestPersistentApplicationDistributions$' -count=1 -v
BLOK_PARITY_LIMITED_PERF=1 GOMAXPROCS=2 go test -p=1 ./benchmarks/parity -run '^TestLimitedHarnessPerformanceDistributions$' -count=1 -v
BLOK_PARITY_RECOVERY=1 BLOK_PARITY_POSTGRES_URL=postgres://...@127.0.0.1:5432/parity108_test GOMAXPROCS=2 go test -p=1 ./benchmarks/parity -run '^TestPersistentDurableRecoveryDistributions$' -count=1 -v
GOMAXPROCS=2 go test -p=1 ./trigger/worker -run '^TestProcessKillRollsBackBusinessWriteAndAcknowledgment$' -count=1 -v
```

Do not run the parity/recovery commands while a parent-owned full gate is live;
coordinate their execution window first. On 2026-10-04, with the gate window
cleared, the migration test, full focused parity package command, and focused
process-kill worker test and historical limited performance sampler passed on
the recorded macOS/arm64 toolchain. The new persistent-process and
durable-recovery samplers above have not yet run; they remain pending final
gate coordination and, for recovery, an explicitly supplied disposable
loopback PostgreSQL database.
The worker retry path uses the actual old `WorkerTrigger`/`InMemoryAdapter`
and new SQLite queue plus real native engine.

The parity command prints raw JSON old/new envelopes, provider calls/effects,
wall times, and recovery state. The worker command runs the existing worker API
crash test: it starts a real child process, kills it while a SQLite transaction
contains an uncommitted handler write and acknowledgment, reopens the database,
and verifies redelivery commits exactly one write. It is a focused test, not a
full repository or load gate.

From `benchmarks/parity/old-engine`, the old package lockfile fixes dependency
resolution; install with `npm ci --ignore-scripts`, then run the same Go command
from the repository root. The npm dependency tree is excluded by the narrow root
ignore rule `/benchmarks/parity/old-engine/node_modules/`; it is not part of the
evidence or deliverable. The harness's old-runner wall time includes a fresh
Node process and the published test runner's referenced timeout timer. The new
wall time is in-process. These are harness observations, not a fair latency
comparison and not production performance claims.

## Contract results and boundaries

`contracts.json` is the machine-readable expected-value ledger. Quote success
must match the exact response. Invalid SKU produces one provider call and no
committed effect; the old test-runner boundary retains only the message,
whereas the new engine retains stable `unknown_sku` classification. Repeated
order invocation is deliberately labelled invocation replay: both engines
call the provider twice, while the provider's shared idempotency ledger commits
one effect. This does not claim that either HTTP trigger deduplicates delivery.
The retry fixture expects old runner retry to make two attempts and complete;
the current native call engine makes one attempt and returns the predeclared
temporary error. No transparent workflow-level retry is asserted for the new
call engine. Separate worker probes run the published `WorkerTrigger` with its
real `InMemoryAdapter`: the synthetic first 503 triggers actual queue redelivery,
two provider calls, and one committed effect. The corresponding new probe uses
the SQLite-backed `trigger/worker.Queue` and executes the real native engine
program per delivery; it also produces two provider calls and one committed
effect after its actual durable queue retry.

Recovery probes use real worker APIs. The old published `InMemoryAdapter`
accepts a job but loses it when the adapter is disconnected/recreated. The new
`trigger/worker.Queue` persists a job in SQLite, reopens that database, and
processes it once. The process-kill test separately proves that transactional
worker handler writes and acknowledgment roll back together and are redelivered
after the consumer process dies. Neither behavior implies automatic replay of
an arbitrary workflow journal. Old distributed broker adapters are not
exercised here and are not described as having the in-memory adapter's loss
guarantee.

The webhook case invokes the actual old `WebhookTrigger` (Svix signature,
workflow dispatch, replay cache) and the actual new webhook adapter plus
durable `worker.Queue`; both execute the synthetic business effect once. Their
acknowledgments differ: old `200 {status: ok}` then `200 {status: duplicate}`;
new `202 {status: accepted}` then `200 {status: duplicate}`. SSE invokes both
actual trigger APIs and asserts the event/data bytes. Its lifecycle deliberately
differs: old `GET /sse/orders` opens and runs one workflow and the workflow sees
client disconnect cancellation; new `POST /sse/orders` durably accepts work,
`GET /sse/orders/{stream}` subscribes to it, and disconnecting that subscriber
does not cancel the accepted worker job. The new hub's transport cursor is not
the legacy event ID, so only event type/data are compared across engines.

The `stream-event-and-disconnect` fixture declares those distinctions. The
Inertia SPA protocol is explicitly out-of-scope for business-workflow parity
and requires its own client/server contract. No production topology, provider
throughput, cold-start, idle utilization, or load/recovery SLO is inferred from
these focused tests. Bounded historical local distributions are recorded in
[`performance-samples-2026-10-04.json`](../../testdata/parity/performance-samples-2026-10-04.json);
their process topologies do not match. The new opt-in application sampler
instead keeps both processes alive, reuses each resolved engine across
requests, and samples per-process CPU/RSS during a declared idle window. The
separate recovery sampler kills each accepting process after durable commit,
then measures fresh-consumer redelivery. Until both commands are run and their
raw outputs reviewed, full startup/idle/load/recovery acceptance remains open.
None of these local harnesses establish production performance, capacity,
throughput, or SLO claims.

The bounded public migration subset, compatibility classification, strict
source/Export handling, and rejected cases are specified by
[`ADR 0019`](../../docs/decisions/0019-bounded-blok-v2-structural-migration.md).
The migration fixtures reject duplicate object keys, nested unknown `$ref`
members, expressions, explicit-null inputs, trigger configuration and Export
documents whose output/reference/binding semantics would otherwise be dropped.
Earlier-call path projections are preserved; self/forward references are
rejected at Export even if a manually constructed document passes
`Document.Validate`.

## Performance acceptance remains open

The retained sample artifact is limited harness evidence and satisfies none of
the required startup/idle/load/recovery distribution acceptance by itself.
A replacement must launch both old and new applications as persistent
processes, use the same synthetic provider and schema/delivery contract, and
measure equivalent work under declared startup, sustained-idle, load, and
process-recovery scenarios. Idle evidence must include process CPU/RSS over a
fixed observation window, not queue-inspection call latency. Recovery must
redeliver accepted work from durable storage on both sides with predeclared
expected outputs, statuses, and effect counts; losing an in-memory old job is
not a matched recovery sample. Keep raw samples, exact versions, commands,
topology, durability settings, and limitations with the result. Do not relabel
the existing mismatched topology samples as parity or production performance.

The persistent recovery gate is separately opt-in because the published
`PgBossAdapter` requires a disposable loopback PostgreSQL database. It pins
`pg-boss` 10.4.2, accepts only loopback endpoints and a database name beginning
`parity108_test`, uses a run-specific schema, kills each producer only after the
old adapter's `send` or new SQLite `Enqueue` reports accepted, and then starts a
fresh actual old `WorkerTrigger`/`Runner` or native-engine/SQLite consumer. The
synthetic provider fails the first attempt and then succeeds; each recovered
job must report one retry, the predeclared output, two provider calls, and one
committed effect. PostgreSQL and SQLite are different local backends, so these
samples establish only single-host accepted-job recovery, not disk-loss,
multi-host, or production failover parity. The recovery command is destructive
only within the explicitly supplied local disposable database: pg-boss creates
its queue schema and leaves it there; never point it at a production database.

## Current raw sample scope

The focused parity tests emit raw quote/order/job, webhook, SSE, and recovery
envelopes and call/effect counts. The persistent app sampler emits bounded raw
startup and idle/load CPU/RSS, plus ten-second concurrent quote distributions;
the separate durable recovery sampler emits kill-to-first-completion
distributions and verifies stable post-retry queue settlement.
Rerun only after coordinating around parent-owned full gates, and retain exact
versions, command, topology, and raw samples.

For the persistent quote sampler, old and new load requests use disjoint
idempotency-key prefixes so both execute the same fresh synthetic-provider
effect path; only the separately labelled warmup intentionally shares a key.
The load windows run sequentially (published process first, native process
second), so the JSON records this order and labels the distributions descriptive,
not a controlled performance comparison. Process CPU uses macOS `ps` cumulative
`cputime` with one-second resolution; one-second windows are quantized and can
produce coarse or zero deltas. Resident memory is sampled from `ps` RSS.
