# E20-T01 executable parity evidence

This package runs the published Blok 2.5.0 engine and the new native engine on
the same synthetic business contracts and compares what each one *does*:
business outputs, errors, provider calls, committed effects, retries,
cancellation, duplicate delivery and recovery after a process is killed. It
never compares step counts or source shape.

Every provider call goes to a per-test loopback `httptest` server that keeps
an effect ledger keyed by idempotency key. No external provider, credential,
paid API or GitHub Action is involved. Expected values are predeclared in
[`testdata/parity/contracts.json`](../../testdata/parity/contracts.json)
before anything runs.

## Pinned versions

| Side | Pin |
|---|---|
| Old framework | Blok tag `v2.5.0` (`7611e434f716a5a8efbed26613e546893d5bcba7`), consumed as published npm packages |
| Old packages | `@blokjs/core`, `runner`, `shared`, `trigger-sse`, `trigger-webhook`, `trigger-worker` all `2.5.0`; `hono` `4.11.7`; `zod` `3.25.76`; `pg-boss` `10.4.2`; `pg` `8.23.1`; lockfile `old-engine/package-lock.json` |
| Old runtime | Node `24.21.0` (`engines.node`) |
| New framework | this repository at the commit recorded in each raw sample; Go `1.27.1` |
| Durable backends | PostgreSQL `17.11` from `postgres:17` (`postgres@sha256:d74eeac9a635390a49bc21bd49fccd973de707e2a53a76ac49b552b8712ec46f`) on a disposable loopback container; SQLite `3.53.4` WAL `synchronous=FULL` |

The pins are enforced, not just documented: with the gate set, the suite
fails before measuring anything if `node --version` or any installed
dependency differs from `old-engine/package.json`
(`TestVerifyPinnedOldEngineRejectsDrift` covers the check itself).

## Running it

The old-engine tests are an explicit opt-in gate, like the repository's other
external-runtime gates, so a fresh checkout's `go test ./...` does not depend
on an npm install. Without the gate they skip and say why; **a skipped run is
not evidence**.

```sh
(cd benchmarks/parity/old-engine && npm ci --ignore-scripts)

# Functional parity (about 10 s)
BLOK_PARITY_OLD_ENGINE=1 GOMAXPROCS=2 go test -p=1 ./benchmarks/parity -count=1 -v
BLOK_PARITY_OLD_ENGINE=1 GOMAXPROCS=2 go test -race -p=1 ./benchmarks/parity -count=1

# Persistent-process startup / idle / load distributions (about 35 s, never with -race)
BLOK_PARITY_OLD_ENGINE=1 BLOK_PARITY_PERF=1 BLOK_PARITY_RAW_DIR=<dir> GOMAXPROCS=2 \
  go test -p=1 ./benchmarks/parity -run '^TestPersistentApplicationDistributions$' -count=1 -v

# Durable recovery + mid-execution kill (about 3 min) against a fresh disposable PostgreSQL
docker run --rm -d --name p108-pg-<unique> --tmpfs /var/lib/postgresql/data:rw,size=512m \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_USER=parity108 -e POSTGRES_DB=parity108_test_ephemeral \
  -p 127.0.0.1::5432 postgres:17
docker port p108-pg-<unique> 5432/tcp
BLOK_PARITY_OLD_ENGINE=1 BLOK_PARITY_RECOVERY=1 BLOK_PARITY_RAW_DIR=<dir> \
  BLOK_PARITY_POSTGRES_URL=postgres://parity108@127.0.0.1:<port>/parity108_test_ephemeral \
  BLOK_PARITY_POSTGRES_CONTAINER=p108-pg-<unique> \
  GOMAXPROCS=2 go test -p=1 ./benchmarks/parity -count=1 -v -timeout 15m \
  -run '^(TestPersistentDurableRecoveryDistributions|TestDurableMidExecutionKillRedelivers)$'
docker stop p108-pg-<unique>
```

The samplers read the server identity themselves: `docker inspect` on
`BLOK_PARITY_POSTGRES_CONTAINER` gives the container and image IDs, the
image's repository digests and the tmpfs mount, and the run fails unless that
container publishes the loopback port in `BLOK_PARITY_POSTGRES_URL`. Each raw
file also records `git rev-parse HEAD`, `git status --porcelain` (clean or the
dirty paths), the test binary's arguments, the `BLOK_PARITY_*` environment,
installed old package versions and the host load average.

The recovery gate refuses any non-loopback host and any database whose name
does not start with `parity108_test`. pg-boss creates its schema in that
database; the tmpfs-backed container discards it on stop. The performance and
recovery samplers refuse to run under `-race` because the native child process
would be instrumented and the old side would not.

## What is compared

Workloads come in two classes (`contracts.json` → `workloadClasses`):

- **business-logic** (`businessWorkloads`): every business value — subtotal,
  bulk discount, tax, total — is computed by workflow nodes on each engine and
  carried between steps (quote: catalog lookup → pricing; order: validate →
  reserve → pricing → commit). The provider only returns catalog unit prices
  and records effects, plus the total each commit carried. The old nodes
  (`old-engine/run.mjs`, `parity-business-*`) and the native ones
  (`business_test.go`) are separate implementations of the same written rules.
  Cases cover a plain quote, the bulk-discount boundary (9 vs 10 vs 12 units),
  unknown SKU at lookup and at reserve, and an invalid quantity rejected before
  any provider call. Mutating the native discount threshold or tax rounding,
  or the old workflow's discount threshold or commit wiring, turns
  `TestBusinessLogicWorkflowsMatch` red.
- **plumbing** (`workloads`): one-step pass-through workflows whose value is
  computed by the provider. They prove input mapping, idempotency headers,
  error classification, retry, cancellation, delivery and recovery mechanics,
  not business-logic equivalence; `order-duplicate-delivery`'s single effect,
  for example, comes from the provider's idempotency ledger.

Which runtime path each comparison exercises:

| Path | Old side | New side | Used by |
|---|---|---|---|
| Test runner | `@blokjs/core/testing` `runWorkflow()` in a fresh Node process, 100 ms timeout (`run.mjs`) | `engine.Run` in the test process | `TestExecutableOldAndNewEngineWorkloads` (quote, invalid SKU, order duplicate, in-process job retry) |
| Long-lived HTTP apps | one Node process; `Configuration` + `Runner` reused across requests | one Go process; `engine.Run` reused across requests | business-logic workloads, cancellation, persistent load sampler |
| Runner on JSON source | `Configuration.init` on the JSON + `Runner` | `migration.Convert` → `internal/compile` → `engine.Run` | migrated workflows |
| Real triggers / queues | `WorkerTrigger` (`InMemoryAdapter`, `PgBossAdapter`), `WebhookTrigger`, `SSETrigger` | `trigger/worker.Queue`, webhook and SSE adapters, `engine.Run` per delivery | worker retry, webhook, SSE, durable recovery, mid-execution kill |

| Workload (contract id) | Old (published 2.5.0) | New (native) | Result |
|---|---|---|---|
| `businessWorkloads` (9 cases) | long-lived Runner app, node-computed totals | long-lived engine app, node-computed totals | identical outputs and error codes, predeclared calls/effects, provider-recorded commit totals equal |
| `quote-success` (plumbing) | `runWorkflow` through the real `Configuration`/`Runner` | `flow` + `engine.Run` | identical output, 1 call, 1 effect |
| `quote-invalid-sku` | provider 422 → failed run | provider 422 → `unknown_sku` | 1 call, 0 effects on both; old keeps only the message, new keeps the stable code |
| `order-duplicate-delivery` | invoked twice | invoked twice | 2 calls, 1 effect on both (provider idempotency; no HTTP-trigger dedupe is claimed) |
| `job-retry` (in-process) | step retry: 2 calls, completes | call engine: 1 call, returns the temporary error | **differs by design**; new retries at the queue (next row) |
| `job-retry` (worker) | `WorkerTrigger` + `InMemoryAdapter` redelivers | `trigger/worker.Queue` (SQLite) redelivers | 2 calls, 1 effect on both |
| `webhook-duplicate` | `WebhookTrigger` (Svix): `200 ok`, then `200 duplicate` | webhook adapter + durable queue: `202 accepted`, then `200 duplicate` | 1 call, 1 effect on both; acknowledgments differ |
| `stream-event-and-disconnect` | `GET /sse/orders` runs the workflow; disconnect cancels it | `POST` admits durably, `GET` subscribes; disconnect keeps the job | same event type/data; lifecycle **differs by design** |
| `order-reserve-commit-success` | long-lived Node HTTP app, two-step workflow | long-lived Go HTTP app, same two steps | identical output, 2 calls, 2 effects |
| `order-cancel-in-flight` | caller disconnects while `reserve` is held: `ctx.signal` aborts the fetch | caller disconnects: `request.Context()` cancels the node call | on both: reserve aborted at the provider, `commit` never called, 0 effects, process keeps serving |
| `durableRecovery` (5 samples) | producer SIGKILLed after two `PgBossAdapter.addJob` with one `jobId`; fresh consumer | producer SIGKILLed after two `Enqueue` with one request key; fresh consumer | same output; old stores **2 jobs** (pg-boss `singletonKey` is not a dedupe key on a standard queue), runs both: 3 calls, 1 effect; new stores 1: 2 calls, 1 effect |
| `midExecutionKill` (`attempts: 1` pins today's behaviour; #245) | consumer SIGKILLed mid-call; pg-boss expires the job (`expireInSeconds=5`) and retries it at its next maintenance pass (120 s default, not configurable through `PgBossAdapter` 2.5.0) | consumer SIGKILLed mid-call; claim, handler writes and ack share one SQLite transaction, so the claim rolls back and a fresh consumer claims at once | on both: killed call aborted, redelivered once, 2 calls, 1 effect, same output |
| `migratedWorkflows` | the Blok v2 JSON source as given | `migration.Convert` → canonical compiler → engine | supported source: identical output, 1 call, 1 effect; `@trigger.body` source runs on old but is refused with `unsupported_trigger_projection` |

Two findings about the engines themselves came out of executing them:

- `flow.Definition.Lower()` dropped call input references, so a later call
  silently received the workflow input (#244, fixed on `main` by #246). The
  native reserve→commit and business-logic programs are authored with `flow`
  and lowered with `Lower`, so these workloads now also exercise that fix.
- A root `@trigger` in published Blok 2.x is the whole request envelope
  (`ctx.request`), not the body. ADR 0019 and the converter's remediation now
  say so; a converted workflow must be bound to that same envelope.

Inertia/SPA is out of scope for business-workflow parity and needs its own
client/server protocol contract (`contracts.json` → `inertia`).

## Raw distributions

[`testdata/parity/raw/`](../../testdata/parity/raw/) holds the samplers' own
output files, copied byte for byte from `BLOK_PARITY_RAW_DIR` (no wrapper):
`persistent-application.json` (startup, idle CPU/RSS, load latency and
CPU/RSS), `durable-recovery.json` (five samples per engine) and
`mid-execution-kill.json` (one sample per engine). Each carries its own
`provenance` block, and the recovery files a `postgresContainer` block.

How to read them:

- **Startup** is process start to readiness line, five fresh processes each.
- **Idle** is five 1-second windows of `ps` CPU time and RSS on a process that
  has served nothing. `ps` CPU time has 10 ms resolution here, so idle reads
  as 0 %.
- **Load** is a paced open loop: 4 workers × 50 requests/s for 10 s (2,000
  requests per engine, identical offered rate), every response checked against
  the predeclared output and every request a fresh provider effect.
  `loadLateStarts` counts requests that could not start on schedule. The old
  window runs first and the new one second; nothing is interleaved or
  counterbalanced, so these are **descriptive distributions, not a
  controlled performance comparison**, and the offered load is far below
  saturation, so no throughput or capacity is measured.
- **Recovery** is kill-to-first-completion and is dominated by each queue's
  retry and orphan policy, not engine speed.
- The native application is the re-executed Go test binary, not a
  release-built binary; the host is a shared developer laptop (macOS 27.0,
  arm64, 10 logical CPUs, `GOMAXPROCS=2`).

The older
[`performance-samples-2026-10-04.json`](../../testdata/parity/performance-samples-2026-10-04.json)
came from the earlier limited harness (`BLOK_PARITY_LIMITED_PERF=1`, a fresh
Node process per old call versus an in-process engine). Its topologies do not
match and it is kept only as history.

None of this is production performance, capacity, throughput or SLO evidence,
and none of it covers multi-host, disk-loss or broker-failover recovery.
