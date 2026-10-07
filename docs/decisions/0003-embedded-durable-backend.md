# ADR 0003: Select SQLite for the embedded durable backend

Status: accepted for M2

## Decision

New Blok selects `modernc.org/sqlite` v1.58.0 as the first embedded durable
backend. It is exposed through the replaceable `store.Backend` / `store.Database`
port; the engine does not import `store/sqlite` or any database driver.

The SQLite configuration used by the port is:

- WAL journal mode, set once per file when it is opened (#320, below);
- `synchronous=FULL`;
- a 5-second busy timeout on every pooled connection, configurable through
  `sqlite.Backend.BusyTimeout`, that also bounds the writer queue (#214);
- foreign-key enforcement enabled;
- `secure_delete=ON` on every pooled connection (#281, below).

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
5,851–8,866 in 322–485 ms in an independent reviewer's run. Since #245 a job
commits two write transactions (the attempt's start, then the handler with
its acknowledgment; ADR 0006): 29–32 ms against 22–28 ms on the same macOS
host, 0 busy errors on both.

## Transactions that do not return (#267)

`Database.WithTx` rolls the transaction back on every exit that does not
reach COMMIT, including a callback that panics or calls `runtime.Goexit`,
and a panic in the commit hook the crash tests install. It does not
recover the panic: the rollback runs as the panic unwinds, and the caller
receives the original value with its original stack. A panic after COMMIT
leaves the commit in place. A marked writer's turn in the writer queue
(#214) is released after the rollback, on every exit, so the next writer
never takes its turn while the lock is still held. The busy and
write-domain annotations (#184, #207) apply only to returned errors; a
panic is never converted into one.

Before #267 `WithTx` rolled back only when its callback returned an error.
`database/sql` rolls back a transaction left open only when its context is
canceled, and the worker's handle transaction runs on a context that is
deliberately not cancelable (ADR 0006). A handler panic that a supervisor
recovered therefore kept the write lock and a pooled connection for the
life of the process: every later writer on the file, on any handle, failed
`store.ErrBusy` after the busy timeout, and on `:memory:` no other
connection could even read. A read transaction left open by a panicking
`RetainedArtifacts` visitor kept its connection, and eight of them spent the
pool, after which every call waited for a connection forever.

The rollback covers every caller of the port: the worker, the journal and
the engine through it, cron, provider records, approval records, and the
examples. A panic inside the driver's own COMMIT is not reachable through
this package: `database/sql` marks the transaction done before it calls the
driver, so a rollback could not undo it there.

| Change | Class | Migration |
| --- | --- | --- |
| `WithTx` rolls back when its callback, or the commit hook, panics or calls `runtime.Goexit`, then lets the panic continue (#267) | behavioral (bug fix) | None. Callbacks that return are unaffected. A caller that recovered a panic from `WithTx` and relied on the transaction staying open, which no API exposed, now finds it rolled back. Other `store.Database` implementations must do the same |

## Secure deletion and purge (#281)

Journal compaction is the framework's erasure (ADR 0021 §7), and a deleted
SQLite row is not erased: its bytes stay in the page's free space, on freed
pages, and in older write-ahead-log frames until something overwrites them.
Measured on this backend (`store/sqlite/erasure_test.go`): rows inserted,
updated and deleted with the previous settings were still readable in the
file after a truncating checkpoint.

- **`secure_delete=ON`** is set through the DSN (`_pragma`), so every pooled
  connection has it, not only the one that ran `configure`. SQLite then
  zeroes deleted content in the page and on freed and overflow pages. It is
  on for the whole database, since every component shares it. Measured in
  review over 500 operations, three interleaved samples, without and with
  it: worker queue 295–342 vs 301–397 ms, journal 89–112 vs 87–121 ms,
  journal compaction 27–29 vs 31–33 ms (about +12%).
- **`store.Purger`** is a new optional `Database` capability. `PurgeLog`
  runs `PRAGMA wal_checkpoint(TRUNCATE)` in the handle's writer turn, on a
  connection whose busy timeout is at most 100 ms: the checkpoint holds
  writers while it waits for readers, so it must not wait the store's full
  timeout. It returns `store.ErrBusy` when a reader still needs the log,
  having truncated nothing; the journal persists the pending purge and
  retries it (ADR 0021 §7). `PurgeFree` runs `VACUUM` and then `PurgeLog`, removing
  free space written before secure deletion was on; it rewrites the whole
  database and needs free space of its size. `:memory:` has no log, so
  `PurgeLog` is a no-op there.
- `Backup` (`VACUUM INTO`) already writes a fresh file without free space or
  log, so a backup taken after an erasure holds none of the erased content.

| Change | Class | Migration |
| --- | --- | --- |
| `secure_delete=ON` on every connection (#281) | behavioral | None. Deletes and updates write zeros over freed space; content deleted before the upgrade stays until `PurgeFree` |
| `store.Purger`, `store.PurgerOf`; `sqlite` implements it | additive | Optional; other `store.Database` implementations need not purge, and the journal then reports `LogPurged = false` |

## Schema versions (#291)

Several components migrate their tables in place when they open, and some
of those migrations are one-way: #281 rebuilt the journal's
reconciliations and dropped its legacy tombstone table, #286 gave
reconciliations a tenant, #290 gave the worker queue tombstones. Before
#291 nothing recorded which shape a database had, so an older binary
opened a migrated database as if it were its own: a pre-#281 journal
recreated an empty legacy tombstone table and failed its reconcile on a
`NOT NULL` constraint, a pre-#286 journal inserted reconciliations without
a tenant, and a pre-#290 worker accepted and ran again a duplicate of a
job compacted to a tombstone (shown with real binaries in the #291 PR).

**The stamp.** Every component that owns tables in the shared database
stamps its schema version in `blok_schema_versions`, one row per
component: `(component, version, upgraded_from)`. One row per component,
not `PRAGMA user_version`, because one file holds several components that
each binary opens independently, in whatever combination it composes, and
that migrate on their own schedules; a single number would make the
journal's version and the queue's indistinguishable, and an application
may use `user_version` itself. The components and the versions this
release supports:

| Component | Owner | Version | History |
| --- | --- | --- | --- |
| `journal` | `internal/journal` | 4 | 1 before #281; 2 with #281's erasure tables; 3 with #286's reconciliation tenant; 4 with #332's wait identity (ADR 0027) |
| `audit` | `contract/audit` | 1 | the #80 tables, unchanged since |
| `worker` | `trigger/worker` | 2 | 1 before #290; 2 with #290's `worker_compacted` and `worker_meta` |
| `approval` | `contract/approval` | 1 | `approval_decisions_v1` as #75 introduced it |
| `cron` | `trigger/cron` | 1 | `cron_cursors` as introduced |
| `provider` | `provider` (`Records`) | 1 | `provider_records`, `provider_outbox` as introduced |

`internal/migration.Apply` is the one implementation, run inside each
component's existing schema transaction:

1. **Read.** The component's row is read. A database with no row (written
   before #291, or a component opened for the first time) is classified by
   its tables' shape instead: 0 when they do not exist, otherwise the
   version they show (the journal by its reconciliations' `tenant` and
   `erased_at` columns, the queue by `worker_compacted`, the others by
   their tables' presence).
2. **Refuse.** A version newer than the highest this binary supports is
   refused before anything else runs, with `store.NewerSchemaError`
   (matching `store.ErrNewerSchema`) naming the component, the stamped
   version and the supported one, for example `store: the journal schema
   in this database is version 4, newer than version 3, the highest this
   binary supports; refusing to open it: run a binary that supports
   version 4, or restore a backup taken before the upgrade`. Nothing is
   written; the open fails and the constructor returns no component. It is
   never transient, and `migration.Retry` does not retry it. `provider`
   returns it unredacted: it holds a component name and two numbers, no
   cause text.
3. **Migrate.** The component's migration runs with the version found.
   The steps that predate the stamp (journal 1–3, queue 1–2) keep their
   shape guards and run on every open, as before: a binary from before
   #291 cannot see the stamp, so it can still bring an older shape back
   into a stamped database (a pre-#281 journal recreates `journal_audit`,
   a pre-#286 one inserts an untenanted reconciliation), and the next open
   must keep repairing it. A step added from now on runs when the version
   found is older than its own.
4. **Stamp.** The row is raised to the supported version, with the version
   found as `upgraded_from`, in the same transaction as the migration. A
   crash leaves neither (shown with a process killed inside the journal's
   schema transaction), and the next open does both. A database already at
   the supported version is not written, so reopening changes nothing.

The same or an older version is migrated forward; an equal one opens
unchanged. Concurrent first opens of an existing database read the stamp
before writing it, so they race for the write lock; every component's
schema transaction now runs under `migration.Retry` (#235), which the
journal and the queue already did, and audit, approval, cron and provider
now do too.

**Raising a version** is part of any change an older binary would misread:
the change bumps the component's `schemaVersion`, adds its migration step
gated on the version found, extends the shape classification only if the
change can meet unstamped databases (it cannot, from #291 on), and
records the version in the table above.

**Application tables** are the application's. The shop recipe keeps its
own ordered `shop_schema_migrations` and now refuses a database whose
latest migration is newer than the ones it knows, with the same error
(component `shop`); its `Teardown` also removes the queue's stamp with the
queue's tables. The deployment example's `deployment_format` row already
refuses an incompatible format.

| Change | Class | Migration |
| --- | --- | --- |
| `blok_schema_versions` table; each component stamps its version on open (#291) | schema, additive | Created on the first open by this release, inside each component's schema transaction. An unstamped database is classified by shape and stamped; no existing table changes |
| Opening a database stamped newer than the binary supports fails with `store.NewerSchemaError` / `store.ErrNewerSchema` (#291) | behavioral (breaking for downgrades) | None for upgrades. A downgrade below a release that raised a component's version is refused at open; restore a backup taken before the upgrade, as ADR 0021 §7 already required for #281 |
| Audit, approval, cron and provider schema transactions retried while busy (#291) | behavioral | None; a concurrent first open that failed `store.ErrBusy` now waits its turn, bounded as in #235 |
| `store.ErrNewerSchema`, `store.NewerSchemaError` | additive | None |

**Limits.**
- Only binaries from this release on refuse. Binaries built before it have
  no check and still open a stamped database as before, with the effects
  above; they are not made safe retroactively. The shape-guarded repairs
  keep cleaning up after them on the next open by a current binary, and
  ADR 0006's "one version per store" still applies to them.
- A component checks its own stamp, and the stamp of any other component
  whose tables it reads without composing it (#321, below). A
  composed component checked its stamp when it was opened.
- `store/distributed` stores (ADR 0019) have no tables of these components
  and are not stamped.
- A newer binary's migration is not reversible by an older one: the
  refusal tells the operator to restore a pre-upgrade backup, and a backup
  carries the stamp with it (`VACUUM INTO` copies the table).

### Cross-component reads (#321)

A component reads another's tables in one of two ways. Through a
**composed** component (the `*audit.Journal` in `journal.Config.Audit`, the
`audit.Owner` and `audit.RunActivity` values passed to `Verify` and
`Prune`), which refused a newer stamp of its own when its constructor ran.
Or through a **bare transaction**, with no such object: then the reader
checks the other component's stamp itself, in the same transaction, before
it reads those tables or infers anything from them, including whether they
exist. The owner of the tables exports the check, so the supported version
stays the owner's: `audit.CheckSchema` returns audit's
`store.NewerSchemaError`, the one `audit.NewJournal` returns.

| Reader | Reads | Route | Stamp checked |
| --- | --- | --- | --- |
| `internal/journal` tenant repair (`journal.New`, schema transaction, #286) | `audit_records_v1` (`audit.StoredTenant`), `audit_pruned_v1` (`audit.Pruned`), and whether `audit_records_v1` exists | bare transaction | audit's, by `audit.CheckSchema`, before any of them (#321) |
| `internal/journal` `Reconcile`, `DecideUpgrade`, compaction's record backfill | audit's tables (`Append`, `Recorded`, and `audit.Pruned` only when audit is composed) | composed `*audit.Journal` | by `audit.NewJournal` |
| `contract/approval` decisions | audit's tables (`Append`) | composed `*audit.Journal` | by `audit.NewJournal` |
| `contract/audit` `Verify` | `journal_reconciliations`, `approval_decisions_v1` (`AuditedIDs`) | composed `audit.Owner` (the journal, the approval store) | by `journal.New`, by the approval store's constructor |
| `contract/audit` `Prune` | the journal's run states (`ActiveRuns`) | composed `audit.RunActivity` (the journal) | by `journal.New` |
| `examples/recipes/shop` `Teardown` | drops `worker_jobs`, `worker_compacted`, `worker_meta` and deletes the `worker` stamp row | bare transaction | none: it removes the worker shape it knows (reported in #321, not changed) |

**Refuse when the repair would read audit.** With audit stamped newer
than this binary understands, `journal.New` refuses with audit's
`store.NewerSchemaError` whenever the tenant repair finds a reconciliation
without a tenant, and gives no row a tenant. The check runs before the
repair reads any audit record, and before it asks whether
`audit_records_v1` exists: a newer audit may keep its records elsewhere,
and "no audit table" would hand every such row to the system tenant. The
#321 PR shows each order separately. A mutant that checks the stamp only
after the table probe fails the test whose newer audit moved its records
out of `audit_records_v1`; that shows the check must precede the probe,
and says nothing about the record and tombstone reads, which it still
precedes. A tripwire test counts every read of a record's content
(`audit_records_v1.record`, which `audit.StoredTenant` verifies and reads
the tenant from) and of a tombstone's kind (`audit_pruned_v1.kind`, which
`audit.Pruned` matches on), a read whose result is discarded included.
Under the newer stamp the refused opens count zero of either; with the
stamp restored, the same open counts the read the repair needs: the
record, and the row gets its tenant, or, when the record was pruned, the
tombstone, and the row stays unowned. It fails under a mutant that reads
every record and discards the result before the check, under one that
looks up every tombstone before the check, and under one that reads the
record first and checks the stamp only before writing; that last mutant
also fails the test of rows the repair leaves unowned, whose opens then
never reach the check and succeed. The tripwire counts those two columns
only: a read that touches neither, such as a row count or a lookup by id
alone, is not counted. A stamp the check cannot read refuses the open
with the read's error, naming audit's schema version, and gives no row a
tenant; a check that took an unreadable stamp for no stamp fails that
test, where the repair would give the row its record's tenant.

It refuses rather than skipping the repair because #291 refuses every
stamp it does not understand, and a composed `audit.Journal` would already
refuse the same database. The journal also has no channel for a non-fatal
diagnostic, and a skip reported nowhere is the unchecked read this
replaces.

When the refusal applies. The check cannot tell rows apart without
reading audit, so it refuses for any row without a tenant. What an open
the stamp does not refuse does with that row differs (ADR 0021 §8):

- a row a binary from before #286 wrote whose verified audit record is
  still there: the repair gives it that record's tenant, once;
- such a row with no audit record and no prune tombstone (it predates
  audit, or audit was never composed): the repair gives it the system
  tenant `""`, once;
- such a row whose record was pruned (a tombstone proves it had a
  tenant), or whose record fails verification: the repair gives it no
  tenant, by design (owned by nobody, ADR 0021 §8), and tries it again on
  every open. Nothing in the framework repairs it: a pruned record never
  comes back, and an unverifiable one is repaired only if it verifies
  again. The journal never deletes a reconciliation row either;
  compaction erases its content in place and keeps the row, tenant still
  `NULL`.

So a database with a row of the last kind refuses every journal-only
open under a newer audit stamp, permanently: every open of a binary whose
audit support is older than the stamp, on every restart, for as long as
the row exists, which in the framework is for good. A binary that
supports that audit version opens the database, and with this release's
repair it leaves the row unowned too: it ends the refusal only for
itself. The ways out are that binary, a backup taken before the audit
upgrade, or the out-of-framework tenant assignment ADR 0021 §8 describes
for an operator who knows the deciding tenant. Before #321 these opens
succeeded. A database with no row without a tenant reads nothing of
audit's and is never refused for audit's stamp, so a journal-only binary
(`examples/deploy` composes no audit) on such a database is not stopped
by an audit-only upgrade. The #321 PR pins the permanent refusal in a
test, red under a mutant that refuses only for a row it would write.

This release keeps the wider refusal rather than narrowing it. To tell a
row the repair already gave up on from one an older binary wrote since,
the journal would have to record "repair attempted, unrepairable"
itself: a new journal-owned marker, so a schema change and a version
bump. It would also stop retrying a row whose record fails verification,
which changes #286's behaviour. Hitting the refusal takes both residue
from before #286 and a binary older than the database's audit schema.
No audit version 2 exists yet, and ADR 0006 already says one version per
store. The refusal names audit and both versions. The remedy is the one
#291 gives for any downgrade below a raised version: run a binary that
supports that audit version, or restore a backup taken before the
upgrade; for a row of the last kind above, nothing else in the framework
ends it. If a future audit version makes this reachable, the marker is
the change to make with it.

| Change | Class | Migration |
| --- | --- | --- |
| `journal.New` refuses with `store.NewerSchemaError{Component: "audit"}` when the tenant repair finds a reconciliation without a tenant (from before #286: unrepaired, or with a pruned or unverifiable record) and audit is stamped newer than supported (#321) | behavioral (breaking for downgrades) | None for a database this release's audit understands. Otherwise run a binary that supports that audit version, or restore a backup taken before the upgrade. That binary gives a row its verified record's tenant, or `""` when it has no record and no tombstone; a row whose record was pruned or fails verification stays unowned, so every open by the older binary stays refused for good |
| `audit.CheckSchema` | additive | None |

## Switching a new file to WAL (#320)

WAL is a property of the file, stored in its header. Before #320 the DSN
asked every pooled connection to switch to WAL as it connected, and
`configure` ran `PRAGMA journal_mode=WAL` once more without reading its
answer. That switch reads page 1 and then writes it, and the write is the
read-then-write upgrade described under Decision: SQLite skips the busy
handler for it. When several handles or processes opened the same brand-new
file at once, the losers failed `database is locked` at once, with none of
their busy timeout used, and `Open` failed with an error that was not
`store.ErrBusy`.

`Open` now switches a file path to WAL on a connection of its own, which is
discarded afterwards. It retries the switch while it fails busy, pausing
between attempts (1 ms doubling to 50 ms), and gives up when the busy timeout
is spent, so the whole switch takes at most one busy timeout. It reads the
mode SQLite answers and fails unless it is `wal`. Pooled connections no
longer switch on connect: they open the file in the WAL mode it already
stores. `:memory:` is unchanged (`journal_mode=MEMORY`, no switch).

| Change | Class | Migration |
| --- | --- | --- |
| `Open` retries the WAL switch while it fails busy, bounded by `BusyTimeout`, and then fails with `store.ErrBusy` (#320) | behavioral (bug fix) | None. Concurrent first opens of one new file now succeed. A first open that still cannot switch, because another connection holds the file for the whole busy timeout, fails `store.ErrBusy` after that timeout instead of an immediate non-`ErrBusy` `database is locked` error. A cancelled `ctx` ends the wait at once with `ctx.Err()` joined to the busy error. Errors other than busy, such as an unwritable path, still return at once |
| `Open` fails unless the file ends up in WAL (#320) | behavioral | None for files this backend creates. Before, the answer of `PRAGMA journal_mode=WAL` was ignored, so a file SQLite would not switch opened in its old mode; it is now refused with an error naming the mode it stayed in |
| Pooled connections no longer request WAL when they connect; they rely on the mode persisted in the file (#320) | behavioral (internal) | None. `Open` switches the file before it returns a handle, and SQLite keeps WAL in the file across connections and restarts |

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

A killed process cannot tell a flushed commit from one still in the
operating system's cache, so the crash test stays green with
`synchronous=OFF`. `pragmas_test.go` (#337) therefore reads the settings back
on all eight pooled connections held at once: `synchronous=2`,
`journal_mode=wal`, `foreign_keys=1`, the configured busy timeout and
`secure_delete=1`. Each connection applies them from the DSN when it
connects; `configure` repeats synchronous, busy timeout and foreign keys on
one connection only.

`internal/journal`'s `TestRealDiskFullFailsClosedAndReopensIntact` (#337)
fills a real size-limited filesystem with admissions until the commit fails
`database or disk is full`. The failed admission is not acknowledged and
nothing of it is visible; every acknowledged one is whole; once space is freed
the file reopens, passes `PRAGMA integrity_check` and accepts the refused
admission. It runs only on Linux with `BLOK_TEST_SMALL_FS` naming a directory
on a small tmpfs (for example `docker run --tmpfs /mnt/small:size=4m`), and
skips elsewhere. Freeing space must leave room for SQLite to checkpoint the
whole write-ahead log into the database file. On reopen, `Open`'s WAL-switch
connection is the file's only connection when it is discarded, and SQLite
checkpoints the log as the last connection closes. With less room (1 MiB
freed against a 3 MiB log, in development of the test) the checkpoint stops
part way: the committed rows stay intact and the file passes its integrity
check, but the reopened store fails `database or disk is full` again.

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
`CREATE TABLE`, before `journal_runs` exists. Adding a column to an
existing journal reads the schema before it alters it, so concurrent
openers of a journal that needs one race: without help, all but one fail
`store.ErrBusy` at once (50 of 60 opens in a six-handle probe). `journal.New`
therefore reruns the idempotent schema transaction while it fails busy,
through the same `internal/migration.Retry` the worker queue uses (#233,
#235). `Records.Execute` uses the same
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
- **Who marks:** every journal transition (not schema creation, which only
  reads when the journal is reopened), the worker's submission, claim and
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
  silently lose its ordering, so tests assert the marks on each writer path:
  journal transitions, worker Enqueue, claim, `chargeLostClaim` and
  `deferLost`, provider `Execute`, cron `Add` and tick flush, approval
  `Record`.
- **Two waits:** a marked writer that then meets an unmarked or out-of-handle
  writer in SQLite waits up to the busy timeout again there. Its total wait
  can therefore reach twice the timeout.

| Change | Class | Migration |
| --- | --- | --- |
| `store.Writer` / `store.IsWriter` | additive | Optional; unmarked transactions are unchanged |
| `sqlite.Backend.BusyTimeout` | additive | Zero keeps 5 s |
| Marked `sqlite` writers on one handle take turns | behavioral | Same bound and `store.ErrBusy`; a wrapper that lowered `PRAGMA busy_timeout` inside a transaction no longer shortens a marked writer's wait for its turn, so set `BusyTimeout` instead |
