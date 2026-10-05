# ADR 0019: Durable distributed workflow execution

Status: accepted for E18-T02 implementation; production support remains gated
on the issue's real-backend failure and load evidence.

## Context

ADR 0017 deliberately stopped at an isolated etcd/S3 fencing spike. The Go
application had no durable execution composition path: HTTP deployment accepts
a caller-supplied handler, the engine runs synchronously in memory, and the
existing journal is coupled to SQLite transactions. Recording only an
accepted-run event would not make workflow execution recoverable.

## Decision

- Keep etcd v3 transactions and partition leases as the authority described by
  ADR 0017. Run admission records and capacity slots are committed before HTTP
  acknowledgment. A storage/quorum failure rejects ingress with retryable
  unavailability; no uncommitted acceptance is acknowledged.
- Route each tenant by stable FNV-1a modulo an immutable partition count.
  Partition count and queue capacities are established in the cluster
  incarnation and mismatched replicas fail closed. Online resharding is not
  included; it remains E18-T03 (#86).
- Bound outstanding accepted work with fixed etcd-backed global and per-tenant
  slots. One current owner worker executes each partition; thus execution is
  bounded by configured partition count and a tenant is serialized within its
  partition. Selection rotates among tenants with pending work; its cursor is
  an owner-fenced durable projection, so takeover does not reset the rotation.
- Select runnable work from the bounded admission-slot projection, not by
  replaying the immutable event history. Due timers use a bounded ordered
  index. There is no implicit event-history migration for pre-existing timer
  records; operators must drain and migrate/rotate an older incarnation before
  deploying this version. The runtime does not perform or claim that migration.
- Add a backend-neutral engine `StepJournal` callback boundary. It carries the
  run, artifact digest, resolved input digest, stable operation key and a
  distinct attempt ID. The adapter validates the exact persisted run identity
  before replay. Every completed step output is fenced and committed before
  the next step runs. A dispatched effect without a committed result becomes
  uncertain after takeover and is not automatically invoked again.
- Journaled execution emits no engine inspection events. A durable run spans
  several engine calls (suspension at a wait, replay of committed steps after
  takeover), and per-attempt step events would report a suspension as a failed
  step and a restored output as a fresh completion; durable runs are inspected
  through their committed journal.
- Expose typed checkpoint decoding on `node.Any`; persisted raw JSON is schema
  validated before typed decoding, and interface-valued numbers retain
  `json.Number` precision. Decoding does not invoke a node or establish trust.
- Compose through an explicit app HTTP admission handler and application
  worker dependency. Tenant identity is supplied by an authenticated resolver,
  never inferred from request JSON.
- External effects are not exactly once. A lost result commit remains
  uncertain until an effect provider's idempotency or reconciliation resolves
  it. No automatic retry crosses an uncertain effect boundary.

## Declared storage-outage and ownership-change behavior

Every transition below was exercised against a real three-voter etcd cluster
(two voters paused, an owner process killed, stopped, or network-partitioned).

| Transition | Outage before the transaction is sent | Transaction in flight when quorum is lost |
|---|---|---|
| Admission | Reject, never acknowledged (`ErrUnavailable`, HTTP 503 + `Retry-After`); nothing durable | Not acknowledged; a retry with the same request key reconciles to exactly one accepted run |
| Signal delivery | Reject (`ErrUnavailable`, 503); wait stays open | Not acknowledged; a retry with the same signal ID is accepted or reported as a duplicate, never twice |
| Due timer | Error; the timer index entry is kept for the next poll | Error; the next poll fires it or finds it already fired, never twice |
| Pure step result | The run stays `running` (claimed by the failed owner) and keeps its slots; the worker leaves the partition; the next owner re-claims the run and re-dispatches the pure step under a new attempt | Same |
| Effectful step result | The run is reported uncertain unless recovery finds the committed result; the effect is never invoked again automatically | Same |
| Finish | The run keeps its slots and non-terminal state | Terminal state and slot release commit together or not at all; a later owner finishes without re-invoking committed steps |

Capacity exhaustion is a definite rejection (`ErrAdmissionFull`, HTTP 429 +
`Retry-After`), reported only from one linearizable read that shows no free
partition or tenant slot (`CapacityError` carries that read). Losing a chosen
slot to a concurrent admission is contention: admission picks a random free
slot, re-reads and retries, and after a bounded number of attempts with free
slots still visible reports retryable unavailability (HTTP 503), never 429. A request key already committed with a different input is a
definite conflict (`ErrRequestConflict`, HTTP 409), including when two ingress
nodes race. A standby worker retries partition acquisition at a bounded
interval. A worker whose processing fails (for example because its
registered artifact does not match an accepted run) releases its exact
partition fence at once, so a healthy worker can take over without waiting
for lease expiry, and backs off exponentially up to the owner TTL before it
tries to acquire again. Such a run still blocks a partition for as long as
every worker that acquires it fails the same way; mismatched artifacts are an
operator error the runtime reports but does not repair.

Fencing rejects a stale owner's durable writes, not its external calls. A
paused or partitioned owner that resumes after a takeover may still perform
the effect of a step it had already dispatched; its result is rejected and the
successor has already marked that run uncertain. Effect providers that need
stronger guarantees must check the step's operation key or the owner fence.

## Compatibility and limits

Ordinary in-memory `Engine.Run` remains unchanged and uses no journal. Durable
execution requires stable workflow artifact digests, a typed input decoder,
and an application-composed cluster runtime. Existing SQL journal APIs remain
available and are not replaced by a speculative storage interface. Only the
partition owner leases and the immutable runtime settings are keyed by the
cluster incarnation (`/blok/v1/incarnations/<incarnation>/...`). Events, run,
step and wait projections, admission slots and the timer index are keyed by
partition (`/blok/v1/partitions/<partition>/...`) and survive an incarnation
rotation, which is what lets a restored cluster keep its data under a fresh
fence. Every read and write of them is guarded by a compare on the current
incarnation, and each event records the incarnation and fence it was
committed under. Data retention and online resharding are separate concerns.

This decision does not imply independent-host or multi-region durability,
production S3 replication, general exactly-once effects, automatic artifact
publication, or fleet-scale capacity. The runtime verifies the registered
program digest before dispatch; it does not publish/fetch program artifacts
from S3. Operators must compose the exact artifact registry with each runtime,
and a missing or mismatched artifact fails closed. The S3 integration test
proves the existing content-addressed blob adapter across takeover, not an
artifact-registry integration.

Timer and signal records are persisted as fenced, tenant-scoped transitions;
signal/timer races have one committed winner. A journaled wait now suspends the
run after its committed prefix, then a signal or due timer atomically records
the outcome and re-admits the same run for replay/resumption. A signal arriving
before its wait is committed is not buffered. Signal delivery still does not
invoke an external consumer.

The immutable transition log has no per-event compactor in this slice. Its
retention contract is therefore explicit but not size-bounded: events remain
available for the lifetime of the cluster incarnation and must not be pruned
individually, because they are the stable identities used to reconcile
ambiguous commits. Operators are responsible for retaining snapshots and audit
records through their required horizon, then retiring the partition keyspace
explicitly: rotating the incarnation fences old owners but does not delete
partition data. This implementation provides no automated retirement, backup
inventory, or compaction. Bounded admission and indexed scheduling do not
bound this append-only history, so long-lived or high-volume production use is
not claimed. A bounded event-retention/compaction contract remains required
before such a capacity claim.
