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
  partition. Selection rotates among tenants with pending work.
- Add a backend-neutral engine `StepJournal` callback boundary. It carries the
  run, artifact digest, resolved input digest, stable operation key and a
  distinct attempt ID. The adapter validates the exact persisted run identity
  before replay. Every completed step output is fenced and committed before
  the next step runs. A dispatched effect without a committed result becomes
  uncertain after takeover and is not automatically invoked again.
- Expose typed checkpoint decoding on `node.Any`; persisted raw JSON is schema
  validated before typed decoding, and interface-valued numbers retain
  `json.Number` precision. Decoding does not invoke a node or establish trust.
- Compose through an explicit app HTTP admission handler and application
  worker dependency. Tenant identity is supplied by an authenticated resolver,
  never inferred from request JSON.
- External effects are not exactly once. A lost result commit remains
  uncertain until an effect provider's idempotency or reconciliation resolves
  it. No automatic retry crosses an uncertain effect boundary.

## Compatibility and limits

Ordinary in-memory `Engine.Run` remains unchanged and uses no journal. Durable
execution requires stable workflow artifact digests, a typed input decoder,
and an application-composed cluster runtime. Existing SQL journal APIs remain
available and are not replaced by a speculative storage interface. The
distributed state records are incarnation-scoped; data retention and online
resharding are separate concerns.

This decision does not imply independent-host or multi-region durability,
production S3 replication, general exactly-once effects, automatic artifact
publication, or fleet-scale capacity. The runtime verifies the registered
program digest before dispatch; it does not publish/fetch program artifacts
from S3. Operators must compose the exact artifact registry with each runtime,
and a missing or mismatched artifact fails closed. The S3 integration test
proves the existing content-addressed blob adapter across takeover, not an
artifact-registry integration.

Timer and signal records are persisted as fenced, tenant-scoped transitions;
signal/timer races have one committed winner. These APIs do not yet suspend and
resume an engine continuation, buffer a signal that arrives before its wait,
or deliver a signal to an external consumer. Event history also has no
compactor/retention policy here, so bounded active admission is not a bound on
total retained history. Those behaviors need their own execution/recovery and
retention contracts rather than being inferred from this commit primitive.
