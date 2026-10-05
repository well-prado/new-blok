# ADR 0003: Select SQLite for the embedded durable backend

Status: accepted for M2

## Decision

New Blok selects `modernc.org/sqlite` v1.58.0 as the first embedded durable
backend. It is exposed through the replaceable `store.Backend` / `store.Database`
port; the engine does not import `store/sqlite` or any database driver.

The SQLite configuration used by the port is:

- WAL journal mode;
- `synchronous=FULL`;
- a 5-second busy timeout on every pooled connection, configurable through
  `sqlite.Backend.BusyTimeout`, that also bounds the writer queue (#214);
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
ones, still fails with `SQLITE_BUSY`; handlers must stay short. Such a
failure is saturation, not a fault: every transaction error caused by
`SQLITE_BUSY` matches `store.ErrBusy` (#184), so callers can retry it.
`store.ErrBusy` also matches `capacity.ErrSaturated`, which is
`trigger.ErrSaturated` (#190): an in-band trigger that receives it answers
with its saturation response (HTTP 503 with `Retry-After`, gRPC
`ResourceExhausted`, WebSocket and MCP `saturated`). The sentinel lives in
the leaf package `contract/capacity`, so the store does not depend on the
triggers. Saturation promises only that the *failing* transaction
committed nothing. Three things narrow it:

- The engine hides it once an earlier step that declared effects has
  completed: retrying the workflow would repeat that effect, so the
  trigger answers the step's own failure instead (its classified code,
  `node_error` unless a domain error names one). The failure still matches
  whatever else it carries, such as a deadline, except anything that is
  itself saturation (`store.ErrBusy` included), and its text names the
  step that committed. An agent catalog workflow tool's dispatch steps
  declare their child's effects, so the same holds for them.
- Past an agent action's dispatch barrier, a busy store leaves the effect
  uncertain, and the policy reports `ErrExecution`, never saturation.
- A worker handler's busy store defers the job when it is another store,
  and fails it as a nested submission when the busy error names the write
  domain the handler's own claim holds (ADR 0006, #207).

Not covered: a node that commits more than one transaction itself, a step
whose writes are not declared as effects, and work outside the engine (a
handler or catalog that writes before or around `engine.Run`, or a custom
`tool.Gate` whose `Publish` reads a busy store after a native tool's
effect). Retrying such work is safe only if its writes are idempotent. A worker
whose consumer is canceled while it waits reports `ErrConsumerLost`, after
up to the busy timeout, because its claim transaction is deliberately not
canceled with the consumer.

Measured with 4 workers draining 200 instant jobs, 5 samples per run
(`NEWBLOK_MEASURE_CLAIM=1 go test -run TestMeasureClaimContention -v
./trigger/worker/`, `golang:1.27.1`, linux/arm64). The write-first claim:
0 busy errors in 148–195 ms. The same harness with the read-first claim of
`996f184` restored: 2,720–7,516 busy errors in 161–368 ms in one run, and
5,851–8,866 in 322–485 ms in an independent reviewer's run.

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

## Writer reservation policy (#179)

SQLite cannot invoke its busy handler when upgrading a previously read
transaction to a writer. Mutation callbacks must reserve the writer before
their first read. Journal `withTx` therefore begins with an empty
`UPDATE journal_runs SET run_id = run_id WHERE 0`: it obtains the write
reservation without changing rows, invoking row triggers, or replaying the
callback. Schema initialization is the exception: its first statement is
`CREATE TABLE`, before `journal_runs` exists. `Records.Execute` uses the same
empty-update rule on `provider_records`; its constructor starts with DDL.

All journal mutation paths follow this rule: admission, replay, effect intent,
attempt start/failure, effect commit/uncertainty, run completion/cancellation,
wait scheduling/claim/cancellation, signals, checkpoint save, scope
start/completion/cancellation, child and join recording, artifact registration,
reconciliation, and compaction. Worker claims and cron occurrence admission
already begin with writes in their respective adapters. No change is made to
`Database.WithTx` globally: journal Run, Operation, Wait, AuditCount, Recover,
RequireArtifact, PlanUpgrade and RetainedArtifacts remain read-only and can
read committed WAL snapshots while a writer is active. Backup remains the
backend's separate consistent-backup operation.

`internal/journal/contention_test.go` covers all ten formerly read-first
mutation callbacks against a held writer on another connection, checking that
a real Run read still completes before releasing that writer. Reconciliation
is tested at its single-transaction boundary, so its outer retry cannot hide
the defect. `provider/database_contention_test.go` verifies the same contention
and then verifies duplicate/conflicting operation behavior and exactly one
business row plus one outbox row. These tests fail on the pre-fix callbacks.
The existing 5-second busy timeout still bounds waiting: an exhausted timeout
may legitimately return an error. This is not a promise of unlimited
contention tolerance.

## Fair writer queue (#214)

SQLite's busy handler is not a queue. A writer that finds the lock taken
sleeps for 1, 2, 5, … up to 100 ms between polls, so the writers that have
waited longest poll least often, and writers that just arrived keep taking
the lock first. Measured with 200 submissions (16 in flight) and 4 workers on
one handle (macOS arm64, go1.27.1), no transaction held the write lock for
1 ms (p50 43 µs). Yet the longest wait reached the full 5 s busy timeout, and
submissions failed `store.ErrBusy` in 5 of 8 runs. A writer was starved, not
slowed.

The framework's own writers therefore take turns.
- **Marked writers:** a transaction whose context is marked `store.Writer` is
  queued per `sqlite` handle, first come first served, before it begins.
- **Who marks:** every journal transition, the worker's submission, claim and
  claim-accounting writes, provider records, cron cursor writes and approval
  records. All of these write first (#176, #179, above), so holding the turn
  from begin to commit is holding the write lock.
- **Bound:** the wait is bounded by the busy timeout, and fails as SQLite's
  would: `store.ErrBusy` annotated with the handle's write domain. A handler
  that submits to the store its own claim holds therefore still fails after
  one busy wait (#207).
- **`:memory:`:** every `:memory:` handle shares one database and one queue.

On the same harness (`NEWBLOK_MEASURE_WRITER_WAITS=1 go test -run
TestMeasureWriterWaits -count=8 -v ./trigger/worker/`) with the queue, there
were no busy failures in 16 runs:

| build | longest wait |
|---|---|
| plain | 2.2–3.0 ms |
| `-race` | 19–27 ms |

That is what an ordered queue predicts: contenders × hold time.

**Unmarked transactions are not queued**, and behave exactly as before.
- **Reads:** they run beside a writer on a committed snapshot (WAL; `:memory:`
  uses its memdb lock instead). That includes a read nested inside a marked
  writer's own callback on the same handle, such as a worker handler reading
  its store.
- **Callbacks that do work before writing:** they hold no turn while they do
  it.
- **Contention:** unmarked writers contend in SQLite's busy handler, as do
  writers on other handles or in other processes.

An earlier draft queued every unmarked transaction instead. Independent review
showed that a handler's ordinary read of its own store then waited out the
busy timeout, and the job was dead-lettered as a nested submission.

**Limits**
- **Opting in:** mark only a callback that writes first and does no slow work,
  since it holds the turn as long as it holds the lock.
- **Unenforced:** the mark is not enforced. An unmarked framework write would
  silently lose its ordering, so tests assert the marks on each writer path.
- **Two waits:** a marked writer that then meets an unmarked or out-of-handle
  writer in SQLite waits up to the busy timeout again there. Its total wait
  can therefore reach twice the timeout.

| Change | Class | Migration |
| --- | --- | --- |
| `store.Writer` / `store.IsWriter` | additive | Optional; unmarked transactions are unchanged |
| `sqlite.Backend.BusyTimeout` | additive | Zero keeps 5 s |
| Marked `sqlite` writers on one handle take turns | behavioral | Same bound and `store.ErrBusy`; a wrapper that lowered `PRAGMA busy_timeout` inside a transaction no longer shortens a marked writer's wait for its turn, so set `BusyTimeout` instead |
