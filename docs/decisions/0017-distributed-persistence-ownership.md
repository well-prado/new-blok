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

There are no package-level connections or network effects in `init`. Creating
the S3 adapter is local configuration only; bucket provisioning is an explicit
network operation. `distributed.New(ctx, client, incarnation)` is an explicit
network initialization call: it atomically establishes the cluster-incarnation
key if absent, otherwise reads and validates it. It uses the supplied etcd
client and caller context, so a canceled or deadline context bounds
initialization. The application and engine do not construct or import these
adapters.

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
longer compare, so its write cannot commit. Renewal requires a positive lease
keepalive and a linearizable comparison of incarnation, owner-key create
revision and owner ID; a stale-incarnation lease is not authority even if it
still reports time-to-live. Renewal cannot recreate or revive an expired
owner. Successful event commits are acknowledged only after etcd
reports the transaction committed. If the response is lost or times out, the
outcome is unknown: after quorum returns, read the stable event ID, compare the
stored partition, kind, owner fence and payload with the intended operation,
and retry only if absent. Event IDs are unique keys, so retrying the same
operation cannot create a second record. This does not make external effects
exactly once; those effects remain uncertain until their own idempotency or
reconciliation establishes the result.

The revision is not globally monotonic across snapshot restore. etcd restore
creates a new cluster history and may reuse lower revisions. The fence is
therefore the pair `(incarnation identifier, create revision)`, never the
revision alone. Restore procedure: keep every worker and admission path
stopped, restore the snapshot into an isolated cluster, use the recovery
operator to compare and rotate `/blok/v1/cluster-incarnation` to a fresh,
unique identifier, validate retained
artifacts and blob references, then route workers. Every commit compares this
key, so a pre-restore owner is fenced after the rotation even if its numeric
revision and owner ID recur. Starting workers before rotation is unsupported
and would leave a stale-writer window. The spike's `RotateIncarnation` is the
primitive for this barrier; it is not a restore orchestrator.

## Replication and acknowledgment matrix

| Operation | Authority and acknowledgment | Failure behavior / limit |
| --- | --- | --- |
| Acquire / renew owner | Acquisition atomically compares incarnation and absent owner key before attaching an etcd lease. Renewal requires a positive keepalive and a linearizable comparison of incarnation, owner-key create revision and owner ID. | No quorum means no confirmed acquisition or renewal. A lease response alone is not authority. Lease expiry makes the old owner unusable. |
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
stable partition after a lease takeover. It then attempts timer-claim and
signal-delivery commits with both the old and new owner: the old fence is
rejected and the current owner's fence is recorded. It also verifies the
referenced blob after takeover. This exercises durable ownership of claim
records, not timer scheduling, signal authorization or delivery to external
consumers. It does not implement a compactor, retention inventory, or
resharder. Partition and blob-provider migration are documented rules, not
implemented online migration operations.

Blob names are content-addressed and immutable. Stage object, read/verify
digest, then commit its reference. A crash between object write and reference
commit leaves an orphan eligible for collection only after a grace period
longer than the maximum upload/commit retry and backup inventory interval.
Compaction may remove a reference only after the active-run, audit, and backup
retention checks pass. Backups must retain all blob digests referenced by the
consistent journal cut; restore fails closed if any required digest is absent.

## Cost, topology and measurements

The direct Go dependencies are etcd client v3.6.5 (coordination, leases,
transactions, snapshots) and MinIO Go SDK v7.0.95 (S3-compatible blob I/O and
digest verification). Their transitive graph includes etcd API/client support,
gRPC/protobuf, zap, MinIO checksums/JSON/crypto, and `golang.org/x` support
packages. The raw run measured 2 non-standard Go packages under `./store`
versus 192 under `./store/distributed` (an incremental package-graph count of
190; this is not a binary-size or runtime-memory measurement). Both concrete
adapters are confined to `store/distributed`; neither is imported by engine,
app, or runtime packages. S3 client configuration is local; etcd `New` and
bucket provisioning are explicit caller-context network operations. Adapter
operations use caller contexts. The local
topology is one Docker host with three etcd v3.6.5 voting members, each with a
separate named volume, and one SeaweedFS v4.47 S3 endpoint with a single named
volume. This tests replication across processes and volumes, not independent
machines, disks, racks, zones, or regions.

The bounded runner is `go run ./benchmarks/distributed`; it requires explicit
endpoints, incarnation and S3 credentials, never starts services, and writes
raw samples as JSON. The [raw synthetic run](../../testdata/distributed/results-2026-10-04-go1.27.1-darwin-arm64.json)
contains five repetitions of 100 sequential commits, reads and four-worker
concurrent commits (500 latency observations per operation), verified S3
put/get, and five snapshot export/offline restore observations. On this Go
1.27.1 darwin/arm64 host, measured p50/p95/p99 were 3.22/5.76/9.56 ms for
sequential commit, 0.77/1.58/2.14 ms for read, 6.67/22.05/29.64 ms for
four-worker commit, 0.96/1.42/1.83 ms for S3 put+verify, and
0.34/0.70/1.08 ms for S3 get+verify. Snapshot export was 11.96 ms p50 and
offline `etcdutl` restore was 267.83 ms p50; snapshot size was 839,712 bytes.
The artifact records workload, exact local image IDs/digests, endpoints,
runtime and dependency package counts. Offline restore duration excludes
member startup and serving readiness. These are local design-spike measurements,
not fleet RPS, multi-region capacity, comparable durability across providers,
or an SLA.

## Evidence and remaining limits

`store/distributed` tests the real etcd API for twelve simultaneous
acquisitions on an unowned partition (exactly one owner wins and commits;
every other result must be `ErrOwnershipLost`), competing acquisition while an
owner is live, lease expiry/takeover, stale-owner rejection, a voter network
partition and catch-up, one-member pause/catch-up, and loss of two of three
members. A failed
no-quorum acknowledgment is treated as unknown; after recovery the test
reconciles by the stable event ID and accepts either the already-committed
matching record or one retry if absent. The S3 test stages and verifies a
synthetic object before reference commit, rejects a commit while S3 is
unavailable, and blocks reads while a committed reference's object is
unavailable. Snapshot/restore checks that the snapshot retained the old
owner-key create revision, advances the source revision beyond the snapshot,
and proves the restored cluster issues a lower numeric token. It then proves
old-incarnation commit and renewal, construction with the stale incarnation,
and stale-store acquisition are rejected; a fresh-incarnation owner commits.
This executes the epoch barrier against a measured numeric revision rewind.

No log-only failover claim is made. The spike does not prove disk-loss
survival, independent-host failure, geographic partition behavior, general
object-store durability, production backup orchestration, timer scheduling,
signal authorization, retention/compaction, online partition/blob migration,
engine integration, full journal semantics, fleet throughput, or release
readiness. Native Windows execution remains unverified even when the test
package cross-compiles. Full vet, repository-wide race tests and build remain
required before merge.
