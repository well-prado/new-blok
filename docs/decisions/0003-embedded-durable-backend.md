# ADR 0003: Select SQLite for the embedded durable backend

Status: accepted for M2

## Decision

New Blok selects `modernc.org/sqlite` v1.58.0 as the first embedded durable
backend. It is exposed through the replaceable `store.Backend` / `store.Database`
port; the engine does not import `store/sqlite` or any database driver.

The SQLite configuration used by the port is:

- WAL journal mode;
- `synchronous=FULL`;
- a 5-second busy timeout on every pooled connection;
- foreign-key enforcement enabled.

`Database.WithTx` returns only after the transaction commit succeeds. Durable
callers may acknowledge accepted work only after that return. This is a local
filesystem durability guarantee subject to the operating system and storage
device honoring flushes; it is not disk-loss survival or multi-host failover.

Transactions start deferred, so reads stay concurrent with a writer under WAL.
The busy timeout only helps a transaction that has not read yet: one that
reads and then writes fails at once with `SQLITE_BUSY` if another writer holds
the write lock or has committed since its read began, because SQLite does not
invoke the busy handler when it upgrades a transaction that has already read.
A transaction that may write under contention therefore writes first. Today
that holds for the worker's job claim, one `UPDATE … RETURNING` (#176), and
cron's cursor, inserted before it is read (#169); the journal and provider
paths that still read first are tracked in #179. Starting every transaction
with `BEGIN IMMEDIATE` was rejected: `WithTx` is also the read path
(`Get`, `Settled`, journal reads), so every read would wait behind the
long write transaction a worker handler holds.

Write-first removes the immediate failure, not the single writer. A worker's
handler runs inside its claim's write transaction, so concurrent workers run
handlers one at a time, and every other writer (submissions, cron, the
journal) waits for them under one busy-timeout budget per statement. A writer
that waits longer than 5 seconds, behind one slow handler or several queued
ones, still fails with `SQLITE_BUSY`; handlers must stay short. A worker
whose consumer is canceled while it waits reports `ErrConsumerLost`, after
up to the busy timeout, because the driver does not interrupt a busy wait.

Measured with 4 workers draining 200 instant jobs, 5 samples each
(`NEWBLOK_MEASURE_CLAIM=1 go test -run TestMeasureClaimContention -v
./trigger/worker/`, `golang:1.27.1`, linux/arm64): read-first claim
2,720–7,516 busy errors in 161–368 ms; write-first claim 0 errors in
148–344 ms.

## Alternatives considered

The executable spike compares SQLite with `go.etcd.io/bbolt` v1.5.0. bbolt is
pure Go, has durable transactions and a supported hot-backup operation, but it
is a low-level key/value store with one writer and would make the journal's
deduplication, attempts, uncertainty and outbox relationships application
indexes rather than transactional relational constraints. SQLite provides the
required relational transaction model, WAL read concurrency, integrity checks
and `VACUUM INTO` consistent backup path while remaining CGo-free through the
selected driver.

The comparison is not a general performance claim. It uses synthetic rows and
the exact settings recorded in the raw report. The report was produced with:

```text
go run ./cmd/store-spike -operations=100 -workers=4
```

on Docker Go 1.27.1, Linux arm64, with the runtime temporary directory. Raw
results are in
[`testdata/store/spike-linux-arm64-go1.27.1.json`](../../testdata/store/spike-linux-arm64-go1.27.1.json).

## Recovery and backup evidence

`store/sqlite/crash_test.go` launches the actual test binary as a child process
and kills it before commit, at a commit-boundary race, and after commit. It
verifies recovered row counts and integrity after reopening the same database.
The same test truncates a closed database and requires opening or integrity
checking to fail closed. `sqlite_test.go` creates a backup with
`VACUUM INTO`, reopens it through the selected backend, runs an integrity check,
and reads the committed row.

The synthetic expected outcomes and material limitations are recorded in
[`testdata/store/fixtures.json`](../../testdata/store/fixtures.json). These
tests do not prove disk-loss survival, network filesystems, multi-host locking,
application-level throughput, or universal exactly-once external effects.

## Compatibility and implementation boundary

The backend port uses `database/sql` transaction callbacks so E07-T02 can add
journal transitions without coupling the engine to SQLite. A later backend can
implement the same port, but it must reproduce the transaction, integrity,
backup and acknowledgment semantics before selection changes.
