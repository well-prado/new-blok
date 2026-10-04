# ADR 0017: Distributed persistence and partition ownership spike

Status: spike accepted for evaluation only; not a production backend selection

## Decision under test

Use etcd v3.6.5 as the executable coordination and small-record persistence
candidate for a distributed journal; keep immutable blob bytes in a separate
S3-compatible service. The repository prototype in `store/distributed` is an
isolated conformance spike, not an implementation of `store.Database` and not
wired into the app or engine. It establishes the owner/commit rule against a
real etcd cluster and measures a local three-member topology. It does not
complete E18, select a fleet topology, or establish production support.

The prototype uses `go.etcd.io/etcd/client/v3` v3.6.5 and
`github.com/minio/minio-go/v7` v7.0.95. Local failure tests use etcd v3.6.5
and SeaweedFS v4.47 containers. All published ports bind to
127.0.0.1. `spike-access` / `spike-secret-only-local` are synthetic local test
credentials only; they are not credentials, defaults, or examples for a
deployment. The single SeaweedFS volume is only an S3 API availability fixture
and supplies no replication evidence.

## Authoritative fencing rule

An owner is identified by `(cluster incarnation, partition, owner ID,
owner-key create revision)`. The create revision is the fence within one etcd
history. Every durable event transaction compares all of the following in the
same etcd transaction that writes the event:

1. the cluster-incarnation key still equals the owner's incarnation;
2. the partition owner key's create revision still equals the owner's fence;
3. the partition owner key still contains the same owner ID; and
4. the event ID has not already been committed.

Acquisition compares both the current cluster-incarnation key and the absent
owner key in one linearizable transaction, then attaches the owner key to an
etcd lease. A client created before an incarnation rotation therefore cannot
acquire a lease in its stale namespace. A paused process cannot renew while
paused. After lease expiry and takeover, its old create revision/owner ID no
longer compare, so its write cannot commit. A renewal cannot recreate or revive
an expired owner. Successful event commits are acknowledged only after etcd
reports the transaction committed. If the response is lost or times out, the
outcome is unknown: after quorum returns, read the stable event ID, compare the
stored partition, kind, owner fence and payload with the intended operation,
and retry only if absent. Event IDs are unique keys, so retrying the same
operation cannot create a second record. This does not make external effects
exactly once; those effects remain uncertain until their own idempotency or
reconciliation establishes the result.

The revision is not globally monotonic across snapshot restore. etcd restore
creates a new cluster history and may reuse lower revisions. The fence is
therefore the pair `(incarnation UUID, create revision)`, never the revision
alone. Restore procedure: keep every worker and admission path stopped, restore
the snapshot into an isolated cluster, use the recovery operator to compare
and rotate `/blok/v1/cluster-incarnation` to a fresh UUID, validate retained
artifacts and blob references, then route workers. Every commit compares this
key, so a pre-restore owner is fenced after the rotation even if its numeric
revision and owner ID recur. Starting workers before rotation is unsupported
and would leave a stale-writer window. The spike's `RotateIncarnation` is the
primitive for this barrier; it is not a restore orchestrator.

## Replication and acknowledgment matrix

| Operation | Authority and acknowledgment | Failure behavior / limit |
| --- | --- | --- |
| Acquire / renew owner | etcd lease plus linearizable transaction / lease response | No quorum means no confirmed acquisition or renewal. Lease expiry makes the old owner unusable. |
| Commit state, timer, signal, or outbox identity | One etcd transaction compares incarnation and owner fence and writes record; acknowledge only on confirmed transaction success | Majority is required. Timeout or disconnect can leave outcome unknown; reconcile the same stable ID and verify the full record before retry. No external side effect is included in the transaction. |
| Read state | Linearizable etcd transaction guarded by current incarnation | No quorum means unavailable; stale/serializable reads are not used for ownership or resume decisions. |
| Store blob bytes | External S3-compatible object service; upload, read back and verify digest before committing reference | Not atomic with etcd. Missing/unavailable object blocks publication/resume; successful metadata commit alone does not replicate or preserve bytes. |
| Backup / restore | etcd snapshot plus a retention-consistent object inventory and immutable blob copies | etcd snapshot alone is incomplete for blob-backed state. Restore stays isolated until incarnation rotation and artifact/blob verification finish. |
| Migrate ownership partition | Change owner for the same stable partition; timers, signals, journal entries and references retain that partition key | No row-copy is implied. In-flight work is claimed only by the current fence. Resharding key formats and migration cutover remain later implementation work. |
| Migrate blob provider/location | Copy by digest, verify bytes, then update the reference location under the partition fence; retain old copy through active references, audit and backup horizons | No cross-provider atomic transaction; orphaned copies are safe and collected only after the retention horizon. |

Three etcd voters tolerate one unavailable voter for writes when the remaining
two can communicate and form a majority. Two unavailable voters stop writes.
This is a quorum statement about etcd's Raft group, not about object storage,
application effects, disks, regions, or arbitrary network topologies.

## Ownership of delayed and external data

Partition assignment is derived from immutable run identity. Timer rows,
signal inbox/deduplication rows, event records and blob references carry that
same partition identity. Timer claims and signal acceptance must use the same
fenced transaction rule as ordinary state commits; delivery is not ownership.
This spike tests timer and signal records remaining addressable under the same
stable partition after a lease takeover, and verifies a referenced blob remains
readable after that takeover. It does not implement a timer scheduler, signal
authorization, compactor, retention inventory, or resharder. Partition and blob
provider migration are documented rules, not implemented online migration
operations.

Blob names are content-addressed and immutable. Stage object, read/verify
digest, then commit its reference. A crash between object write and reference
commit leaves an orphan eligible for collection only after a grace period
longer than the maximum upload/commit retry and backup inventory interval.
Compaction may remove a reference only after the active-run, audit, and backup
retention checks pass. Backups must retain all blob digests referenced by the
consistent journal cut; restore fails closed if any required digest is absent.

## Cost, topology and measurements

The dependency cost is the etcd client package and its etcd API/client-pkg,
zap, gateway/protobuf, and MinIO Go SDK v7.0.95 support modules. Both concrete
adapters are confined to `store/distributed`; neither is imported by engine,
app, or runtime packages. Constructors perform no network I/O, while bucket
provisioning is explicit. Adapter operations use caller contexts. The local
topology is one Docker host with three etcd v3.6.5 voting members, each with a
separate named volume, and one SeaweedFS v4.47 S3 endpoint with a single named
volume. This tests replication across processes and volumes, not independent
machines, disks, racks, zones, or regions.

The bounded runner is `go run ./benchmarks/distributed`; it requires explicit
endpoints, incarnation and S3 credentials, never starts services, and writes
raw samples as JSON. It records sequential and concurrent etcd commit/read
latencies, verified S3 put/get latencies, snapshot export and offline restore
durations, payload/workload settings, runtime, image identifiers and
non-standard package counts. The committed raw artifact will identify the
exact local run. Offline restore duration does not include serving readiness.
These measurements are local design-spike evidence only. They are not fleet
RPS, multi-region capacity, comparable durability across providers, or an SLA.

## Evidence and remaining limits

`store/distributed` tests the real etcd API for concurrent acquisition, lease
expiry/takeover, stale-owner rejection, a voter network partition and catch-up,
one-member pause/catch-up, and loss of two of three members. A failed
no-quorum acknowledgment is treated as unknown; after recovery the test
reconciles by the stable event ID and accepts either the already-committed
matching record or one retry if absent. The S3 test stages and verifies a
synthetic object before reference commit, rejects a commit while S3 is
unavailable, and blocks reads while a committed reference's object is
unavailable. Snapshot/restore checks that the snapshot retained the old
owner-key create revision, rotates the incarnation while isolated, then
proves both the old owner commit and stale-store acquisition are rejected.
This exercises the epoch barrier against a still-present old token; it is not
a synthetic numeric revision-rewind test.

No log-only failover claim is made. The spike does not prove disk-loss
survival, independent-host failure, geographic partition behavior, general
object-store durability, production backup orchestration, timer scheduling,
signal authorization, retention/compaction, online partition/blob migration,
engine integration, full journal semantics, fleet throughput, or release
readiness. Native Windows execution remains unverified even when the test
package cross-compiles. Full vet, repository-wide race tests and build remain
required before merge.
