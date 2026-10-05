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
docker image inspect postgres:17 --format '{{index .RepoDigests 0}}'
BLOK_PARITY_OLD_ENGINE=1 BLOK_PARITY_RECOVERY=1 BLOK_PARITY_RAW_DIR=<dir> \
  BLOK_PARITY_POSTGRES_URL=postgres://parity108@127.0.0.1:<port>/parity108_test_ephemeral \
  BLOK_PARITY_POSTGRES_IMAGE=postgres:17 BLOK_PARITY_POSTGRES_IMAGE_DIGEST=postgres@sha256:<digest> \
  GOMAXPROCS=2 go test -p=1 ./benchmarks/parity -count=1 -v -timeout 15m \
  -run '^(TestPersistentDurableRecoveryDistributions|TestDurableMidExecutionKillRedelivers)$'
docker stop p108-pg-<unique>
```

The recovery gate refuses any non-loopback host and any database whose name
does not start with `parity108_test`. pg-boss creates its schema in that
database; the tmpfs-backed container discards it on stop. The performance and
recovery samplers refuse to run under `-race` because the native child process
would be instrumented and the old side would not.

## What is compared

| Workload (contract id) | Old (published 2.5.0) | New (native) | Result |
|---|---|---|---|
| `quote-success` | `runWorkflow` through the real `Configuration`/`Runner` | `flow` + `engine.Run` | identical output, 1 call, 1 effect |
| `quote-invalid-sku` | provider 422 → failed run | provider 422 → `unknown_sku` | 1 call, 0 effects on both; old keeps only the message, new keeps the stable code |
| `order-duplicate-delivery` | invoked twice | invoked twice | 2 calls, 1 effect on both (provider idempotency; no HTTP-trigger dedupe is claimed) |
| `job-retry` (in-process) | step retry: 2 calls, completes | call engine: 1 call, returns the temporary error | **differs by design**; new retries at the queue (next row) |
| `job-retry` (worker) | `WorkerTrigger` + `InMemoryAdapter` redelivers | `trigger/worker.Queue` (SQLite) redelivers | 2 calls, 1 effect on both |
| `webhook-duplicate` | `WebhookTrigger` (Svix): `200 ok`, then `200 duplicate` | webhook adapter + durable queue: `202 accepted`, then `200 duplicate` | 1 call, 1 effect on both; acknowledgments differ |
| `stream-event-and-disconnect` | `GET /sse/orders` runs the workflow; disconnect cancels it | `POST` admits durably, `GET` subscribes; disconnect keeps the job | same event type/data; lifecycle **differs by design** |
| `order-reserve-commit-success` | long-lived Node HTTP app, two-step workflow | long-lived Go HTTP app, same two steps | identical output, 2 calls, 2 effects |
| `order-cancel-in-flight` | caller disconnects while `reserve` is held: `ctx.signal` aborts the fetch | caller disconnects: `request.Context()` cancels the node call | on both: reserve aborted at the provider, `commit` never called, 0 effects, process keeps serving |
| `durableRecovery` (5 samples) | producer SIGKILLed after two `PgBossAdapter.addJob` with one `jobId`; fresh consumer | producer SIGKILLed after two `Enqueue` with one request key; fresh consumer | same output; old stores **2 jobs** (pg-boss `singletonKey` is not a dedupe key on a standard queue), runs both: 3 calls, 1 effect; new stores 1: 2 calls, 1 effect |
| `midExecutionKill` | consumer SIGKILLed mid-call; pg-boss expires the job (`expireInSeconds=5`) and retries it at its next maintenance pass (120 s default, not configurable through `PgBossAdapter` 2.5.0) | consumer SIGKILLed mid-call; claim, handler writes and ack share one SQLite transaction, so the claim rolls back and a fresh consumer claims at once | on both: killed call aborted, redelivered once, 2 calls, 1 effect, same output |
| `migratedWorkflows` | the Blok v2 JSON source as given | `migration.Convert` → canonical compiler → engine | supported source: identical output, 1 call, 1 effect; `@trigger.body` source runs on old but is refused with `unsupported_trigger_projection` |

Two findings about the engines themselves came out of executing them:

- `flow.Definition.Lower()` drops call input references, so a later call
  silently receives the workflow input (#244). The reserve→commit program is
  therefore built through the canonical document compiler with an explicit
  reference until #244 is fixed.
- A root `@trigger` in published Blok 2.x is the whole request envelope
  (`ctx.request`), not the body. ADR 0019 and the converter's remediation now
  say so; a converted workflow must be bound to that same envelope.

Inertia/SPA is out of scope for business-workflow parity and needs its own
client/server protocol contract (`contracts.json` → `inertia`).

## Raw distributions

[`performance-samples-2026-10-05.json`](../../testdata/parity/performance-samples-2026-10-05.json)
holds the samplers' own raw JSON (written with `BLOK_PARITY_RAW_DIR`, not
transcribed from logs): startup, idle CPU/RSS, load latency and CPU/RSS, five
durable-recovery samples per engine, and one mid-execution-kill sample per
engine, each with the measured source revision and versions.

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
