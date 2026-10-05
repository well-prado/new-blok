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

Records are bounded after encoding (#254). The edge bound `MaxInputBytes`
counts request bytes, but `encoding/json` HTML-escapes `<`, `>` and `&` to six
bytes each, so a body under the edge bound can encode into a much larger
record. Three things are bounded, all before a transaction is sent:

- **Each record.** Every encoded state projection and event payload is at most
  `distributed.MaxPayloadBytes` (512 KiB).
- **Each transaction.** The whole etcd request (keys, values, compares and
  protobuf framing, estimated from above) is at most the smaller of etcd's
  `--max-request-bytes` and the client's `MaxCallSendMsgSize`. Both default to
  etcd's own defaults (1.5 MiB and 2 MiB). A deployment that changes either
  must pass its values with `distributed.WithRequestLimits`. `distributed.New`
  refuses limits below `MinRequestBytes` (two records at the bound plus
  64 KiB), so every two-record transaction (admission, claim, suspension,
  finish) fits whenever its records do. Only a transaction with three or more
  records (a signal or timer that re-admits its run) can exceed the request
  bound. If etcd itself still answers "request is too large", or gRPC
  answers ResourceExhausted for a message over its size limit, the error is
  reported the same way. etcd's NOSPACE alarm is not a size rejection and
  stays an ordinary error.
- **Each run's growth.** Admission accepts a run only if its record still fits
  with the largest metadata the runtime adds before a terminal transition:
  the longest non-terminal state and an owner ID of 180 bytes (the store's
  name limit) at its worst encoding (six bytes each), plus a maximal fence.
  Claim, suspension, takeover by a longer owner ID, and signal or timer
  re-admission therefore never outgrow the bound. Terminal output and error
  codes are not reserved; a terminal record that does not fit falls back to
  a smaller one instead (#265, below).

The store reports each case as `distributed.ErrRecordTooLarge`, as a
`*RecordTooLargeError` that names the record (a state ID, the event payload,
or the whole request). The runtime classifies it by whose record it is:

| Over-bound part | Classification | HTTP |
|---|---|---|
| Admission input, or a run record that leaves no metadata headroom | `ErrInvalid` | 400, no `Retry-After` |
| A signal's wait record, its event, or its late-signal record | `ErrInvalid` | 400, no `Retry-After` |
| A signal transaction over the request bound | `ErrInvalid`: the run record alone fits with room to spare, so only a large signal pushes it over, and a smaller signal is accepted | 400, no `Retry-After` |
| A run record the runtime grew, on a signal (unreachable for runs admitted with headroom; possible for a run stored without it) | `ErrRecordOverflow`: definite, and not the caller's fault | 500, no `Retry-After` |
| A genuine storage outage | `ErrUnavailable` | 503 + `Retry-After` (unchanged) |

400 rather than 413, because both handlers already answer a body over the
edge bound with 400 and map `ErrInvalid` to 400. "Too large" then has one
status, whichever layer detects it. In each case nothing durable was
written, and retrying the same request can never succeed. The effective bound
for HTML-heavy input is therefore about one sixth of `MaxInputBytes`, minus
the run-metadata headroom (about 1.1 KiB).

Compatibility classification for #254: behavioral, and additive in the API.
- Requests that encode over a bound used to get a permanent 503 +
  `Retry-After`, or a 202 for a run that could never be claimed. They now get
  a definite 400. A tiny signal to a run stored without headroom gets a 500
  instead of a misleading 400. Admission accepts about 1.1 KiB less encoded
  input than before.
- Additive exports: `ErrRecordTooLarge`, `RecordTooLargeError`,
  `RequestLimits`, `WithRequestLimits`, `CheckRecord`, `MinRequestBytes`, the
  two default limits, and `cluster.ErrRecordOverflow`. `distributed.New`
  gains variadic options and stays source compatible.
- Persisted bytes are unchanged. Records are still encoded with HTML
  escaping, so stored run, wait and event bytes are unaffected. So are the
  canonical input digest that matches a retried request key, the
  byte-for-byte event comparison that reconciles an ambiguous late-signal or
  admission commit, and the `testdata/distributed` fixtures. Mixed-version
  replicas agree on every record.
- Encoding with `SetEscapeHTML(false)` was considered and rejected. It would
  let such payloads through, but it changes the stored bytes and the input
  digest of any input containing those characters. A request or late signal
  retried across a rolling upgrade could then be reported as a conflict.

### Runtime-grown records inside a run (#265)

A run's own records can outgrow the bound after admission: a step's committed
output, a step's dispatch record (it carries the node's declared effects,
which the descriptor does not bound), the terminal run record (input plus
output), and, for a run stored without the headroom above, its run record
once a claim or a timer re-admission writes a longer owner ID. Retrying any of
these can never fit, so each one ends the run as a terminal failure with
error code `record_too_large`, releases its admission slots in the same
transaction, and frees the tenant's place for its next run. It used to stay
`running`, re-execute its node and block the partition indefinitely.

| Over-bound record | Outcome |
|---|---|
| Pure step output (committed record) | Run `failed`, `record_too_large`; the node ran once and its step is left `retryable` |
| Step dispatch record | Run `failed`, `record_too_large`, before the node is invoked |
| Effectful step output | Run `uncertain`, `record_too_large`: the effect ran, so it is not reported as a plain failure |
| Terminal run record | Fallback below; a completed run whose output does not fit becomes `failed`, `record_too_large` |
| Run record at claim (stored without headroom) | Run `failed`, `record_too_large`, without executing |
| Run record at timer re-admission (stored without headroom) | Run `failed`, `record_too_large`; the wait closes `timed_out` and leaves the due-timer index |

The journal reports a size rejection of a step record as `ErrRecordOverflow`.
The engine still wraps it in its `persistence` class, which is the class the
runtime retries; the runtime treats a `persistence` error caused by
`ErrRecordOverflow` as terminal and every other `persistence` error (a quorum
loss, a timeout, a fenced-out write) as retryable, exactly as before. Only
`distributed.ErrRecordTooLarge` produces `ErrRecordOverflow`, and the store
reports it only for a size decided before sending or etcd's or gRPC's own size
rejection, never for an outage.

A run must still be failed when its own run record can no longer be written.
`finish` therefore tries, in one fenced transition, the intended terminal
record, then (for a completed run) a failure without output, then that record
without its input. The dropped input is stored as `null`; the input digest
stays, and it is what a retried request key is compared with. A terminal run is
never replayed, so nothing reads the input again. Every field left in the
minimal record is a fixed-size digest, a name the store bounds at 180 bytes,
the request key (bounded at admission) or the registered workflow name, so it
fits unless that name alone approaches the bound, which is reported as
`ErrRecordOverflow`. The state of a failed or uncertain run is never rewritten;
only its input can be dropped. A run admitted with headroom keeps its input
unless the error code or output it ends with exceeds that headroom.

A timer that re-admits a run stored without headroom first fails the run, then
closes the wait in a second transition. If the owner stops between them, the
next poll finds the wait still due and its run terminal, and closes the wait
alone, so the due-timer index never keeps an entry for a terminal run.

Limits: a signal to a run stored without headroom keeps #254's behavior (500,
nothing written, the run stays waiting). If its wait has a timeout, the timer
then fails the run; a wait without one keeps the run and its slots until an
operator intervenes. The overflow depends on the current owner's ID, so a
deployment with shorter owner IDs may claim a run another would fail; this
decision fails it rather than wait for a different owner.

Compatibility classification for #265: behavioral, no API change. Runs whose
records exceed the bound now end `failed` (or `uncertain` for an effectful
step) with `record_too_large` instead of staying `running`. A terminal record
may carry `"input": null` when the input does not fit beside its terminal
metadata. Persisted bytes of every record that fits are unchanged, and so are
the retry decisions for every other error.

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
