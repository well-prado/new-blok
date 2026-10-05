# ADR 0006: Durable submission port and signed webhook admission

- Status: accepted
- Date: 2026-10-02
- Amended: #188 (worker nested-store diagnostics and saturation), #207
  (undetected self-submits fail after one busy wait), #245 (a started
  attempt is committed before its handler runs), #267 (a panicking handler
  fails its attempt and releases the write lock), #290 (retention of
  finished jobs and the dedupe window)
- Roadmap: E09-T02 ([#55](https://github.com/well-prado/new-blok/issues/55))
- Amends: [ADR 0005](0005-trigger-adapter-contract.md) (webhook declaration)
- Consumers: webhook now; cron (#56) and pubsub (#57) are expected to submit
  through the same port

## Context

A webhook provider resends an event until it gets a 2xx response. So the adapter must not answer
before the event is owned durably, and it must answer a resend without
creating a second run. The repository has one durable admission path, the
worker queue, but an adapter may not import a store or another adapter. The
worker also carried no principal, so a provider's verified identity could not
reach the workflow.

## Decision

### Shared durable submission

`trigger.Submitter` is the durable admission port:
`Submit(ctx, Submission{Key, Kind, Payload, Principal}) (accepted, err)`. It
returns only after the submission is committed. `accepted=false` means the key
was already committed with the same kind, payload and principal (a duplicate).
`trigger.ErrConflict` means the key was reused with different content, which is
never silently deduplicated. `trigger.ErrInvalidInput` and
`trigger.ErrSaturated` refuse the submission before acceptance. A store too
busy to take the submission in time, because the write lock or a connection
was not available before the busy timeout or the submit deadline, is
reported as `trigger.ErrSaturated` (#184), and nothing is committed: webhook
and SSE answer 503 `saturated` with `Retry-After`, pubsub naks with a delay
without spending its delivery budget, and cron keeps the occurrence for its
next attempt. Worker handlers also treat saturation as backpressure: a busy
other store or a spent submit deadline defers the job without consuming an
attempt. Before submitting, `Queue.Enqueue` checks the write domain attached
to the handler context. If that is the queue's own domain, it immediately
returns `worker.ErrNestedSubmission`, with guidance to use the handler's `Tx`
for atomic writes; this is a failed handler result, not saturation to defer.

The optional `store.WriteDomainProvider` capability supplies lock identity.
SQLite compares file-backed domains using the filesystem's same-file identity,
including separate handles and filesystem aliases to the same database file;
every `:memory:` open is one shared database with one stable domain token.
Wrappers that share a write lock must forward the underlying token.

The context check cannot see a nested submission whose handler discarded the
supplied context (for example by submitting with `context.Background()`) or
that goes through a wrapper hiding `WriteDomainProvider`. Such a submission
waits out the store's busy timeout once, because the claim holds the lock it
needs, and returns saturation that names the write domain it waited on
(#207). The store names it: SQLite annotates its busy errors with its domain
(`store.WithWriteDomain`), which survives wrappers that pass errors through;
`Queue.Enqueue` adds its own queue's domain when the cause carries none.
`ProcessOnce` compares every domain the handler's error names
(`store.ErrorWriteDomains`, which also reads each branch of an
`errors.Join`) with its claim's while the claim is still held. Checking only
the first would let a handler that joins another store's saturation ahead of
its own hide the self-submit. A match means the claim itself was the
contention, so deferring would only repeat the wait: the job fails as
`worker.ErrNestedSubmission`, is not retried (even when joined with a
retryable `HandlerError`, #225), and dead-letters as `nested
submission to claimed store; use worker.Tx for atomic writes`. Saturation
naming a different domain, or no domain, is backpressure and still defers.
The comparison needs the claiming queue's own domain: a queue opened on a
wrapper that hides it cannot tell its claim apart and defers, as before.
Direct database writes that bypass `Queue.Enqueue` are likewise outside this
diagnostic; handlers should use `worker.Tx`.

SQLite opens `:memory:` on its memdb VFS (`file:/new-blok-memory?vfs=memdb`),
not in shared-cache mode. In shared-cache mode a writer blocked by another
connection's write transaction gets `SQLITE_LOCKED_SHAREDCACHE`, which the
driver waits out with `sqlite3_unlock_notify`: no busy timeout applies, the
context cannot interrupt it, and SQLite reports a deadlock only when the
blocking connection is itself waiting for an unlock. A claim waiting for its
handler is not, so an undetected self-submit on `:memory:` blocked forever
(#207). memdb locks report `SQLITE_BUSY` and honor the busy timeout like a
file, so the same submission fails after one wait. The trade-off is ordinary
locking without WAL: while a write transaction is open (a worker claim holds
one for its whole handler), no other connection can even start a read; it
waits out the busy timeout and fails with `store.ErrBusy` (measured: 5.05 s,
where shared cache answered an unrelated read in 0.3 ms). Each connection
also refreshes its schema at the start of a transaction rather than sharing
one cache, and memdb caps the database at 1 GiB. `:memory:` is a test store;
deployments open a file.

A worker handler writes through `worker.Tx`, the claim's own transaction,
so its writes commit only if the job is acknowledged. SQLite can end that
transaction under the handler: it rolls the whole transaction back when a
statement is interrupted (its context was canceled or ran out: the consumer
was lost, or the handler's own deadline passed), fails for want of space, memory or I/O, or hits a
constraint declared `ON CONFLICT ROLLBACK` or a trigger's
`RAISE(ROLLBACK)`. `database/sql` cannot see that, and every later
statement would then commit on its own, outside the claim, and again on
redelivery (#180). `Tx` therefore fails closed:

- When a statement fails, `Tx` reads the job on the claim's own
  connection. While the transaction lives, it sees its own uncommitted
  lease; once SQLite has rolled back, it sees the job as it was before the
  claim. If the lease is gone, the claim is lost: that statement returns
  its own error, and every later one returns `worker.ErrClaimLost` and runs
  nothing.
- One lock covers each statement and the check after it, so a handler
  using `Tx` from several goroutines cannot start a statement on a
  transaction that has already ended.
- A statement that begins with a transaction-control keyword (`BEGIN`,
  `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`, after any leading
  whitespace, comments and empty statements such as a lone `;`) is refused with `worker.ErrTransactionControl`: the claim owns
  its transaction. A multi-statement string that hides one after another
  statement is not detected; handlers must not issue them.
- Statements still run under the handler's context, so cancellation and
  deadlines keep working. `ProcessOnce` routes its own post-claim
  statements through the same check.

A claim lost to consumer cancellation is deferred as before. A claim lost
any other way is a failed attempt: the job is retried after a backoff, or
dead-lettered once `MaxAttempts` is spent, with error `claim transaction
ended`. Like any failed attempt, `ProcessOnce` reports it as processed
without an error; the job's error records why.

**Started attempts survive a crash (#245).** Before #245 the claim counted
its attempt inside the handler's transaction. A worker process that died
mid-handler (SIGKILL, OOM, a runtime crash) rolled that count back with the
handler's writes, and the job was claimed again at once as if the attempt
had never happened. A handler that kills its process every time was
redelivered forever: a subprocess test restarted such a worker 10 times
against a job with `MaxAttempts` 3, and the job stayed `pending` at attempt
0 through 10 handler runs.

`ProcessOnce` now uses two write transactions, both write-first (#176) and
marked `store.Writer` (#214):

1. **Start.** The claim leases the job for the queue's lease
   (`worker.DefaultLease`, 30 s, or `worker.WithLease`), counts the attempt,
   and commits. Nothing else is written.
2. **Handle.** A second transaction takes the lease over (its first
   statement rewrites the lease to a value only this transaction holds,
   and fails if another worker has claimed the job since), runs the
   handler, and commits the handler's writes with the job's outcome. The
   business writes and the acknowledgment are still atomic: a crash rolls
   both back. The fail-closed `Tx` checks for this transaction's own lease
   value, so a rollback SQLite performs under it is still detected (#180).

A worker that dies after the start leaves the lease and the counted attempt
behind. Once the lease expires, the claim takes the job again as its next
attempt. A claim that finds the job's attempts already spent (every one
started and none finished) does not run the handler: it dead-letters the
job with error `claim_abandoned` (`worker.ClaimAbandoned`) at attempt
`MaxAttempts`. The same subprocess test now sees attempts 1, 2, 3 and then
`dead`, after exactly `MaxAttempts` handler runs, with no handler write
committed; a worker restarted before the lease expires claims nothing.

**One started attempt per write domain in a process.** A started attempt is
charged if the process dies before its outcome commits, so an attempt must
not be started that the process cannot run at once. A first version let
several workers in one process start attempts while one handler held the
write lock. The review reproduced the cost: a poison job's crashes
dead-lettered an innocent job started beside it, as `claim_abandoned`
at attempt 3, without its handler ever running. A worker whose handle
transaction then waited out the busy timeout behind a slow handler also
failed to give its attempt back for the same reason, and left its job leased
for 30 s with an attempt charged.

`ProcessOnce` therefore takes its write domain's claim turn before it starts
an attempt, and holds it until the attempt's outcome, including any
accounting transaction, has committed or rolled back. Every `Queue` the
process opens on the same write domain shares the turn
(`store.SameWriteDomain`). The cost is near zero: the turn holder's handler
holds the write lock, so another worker could not have run its own handler
meanwhile anyway. The wait for the turn is bounded by the store's busy timeout
(`store.BusyTimeoutProvider`, 5 s when the store does not report one). It
is not interrupted by the consumer's context, like the store's own write
turn (#214). It fails as the store would, with `store.ErrBusy` naming the
write domain, so the worker starts nothing and its job is untouched, as when
a claim waited behind a handler before #245. A handler that calls
`ProcessOnce` on the store its own claim holds therefore fails after one busy
wait instead of deadlocking (#207). Workers in other processes on the same
database file do not share the turn (see Limits).

How each ending treats the started attempt:

| Ending | Attempt | Job |
| --- | --- | --- |
| Handler succeeds, fails, or is saturated | as before (saturation and nested submission unchanged, #188/#207/#225) | committed with the handler's outcome |
| Consumer lost while the handler runs (graceful cancel) | given back | deferred, one deferral charged, as before |
| Consumer lost after the start and before the handler runs | given back | released at once, no deferral charged; the handler does not run |
| Handle transaction fails before the handler runs (for example busy, from a writer outside the process) | given back by a separate transaction; if that also fails, it stays counted | released at once; if the give-back failed, redelivered when the lease expires |
| Lease taken by another worker before the handler starts | stays counted | left to that worker; `ProcessOnce` returns `worker.ErrClaimLost` |
| Claim lost under the handler (#180), or the handle transaction fails to commit | stays counted | retried after a backoff, or dead (`claim transaction ended`) |
| Process dies | stays counted | redelivered when the lease expires, or dead (`claim_abandoned`) once attempts are spent |
| Handler panics or calls `runtime.Goexit`, and the caller recovers (#267) | stays counted | retried after a backoff, or dead (`handler_panicked`) once attempts are spent; the panic reaches the caller |

**A handler that panics (#267).** Before #267 a handler panic left its
handle transaction open: the store did not roll back a callback that
panicked, and the transaction's context is deliberately not cancelable, so
database/sql never rolled it back either. A supervisor that recovered the
panic kept a process in which every write to the file, from any handle,
failed busy until it exited. The store now rolls back every transaction
whose callback does not return (ADR 0003). `ProcessOnce` does not recover
the panic: it reaches the caller with its own value, and an unrecovered
panic still kills the process.

On the way out, a separate write-first transaction counts the attempt as
failed, matched on the start's lease like the other accounting below, while
the worker still holds the claim turn. It is the row for a handle
transaction that rolled back after its handler ran: retried after the usual
backoff of one second per attempt, or dead once `MaxAttempts` is spent. The
error is `handler_panicked` (`worker.HandlerPanicked`); the panic value is
not recorded, because arbitrary text never reaches the dead-letter record.
No deferral is charged. A `runtime.Goexit` in the handler is the same: the
handler did not return. If that accounting cannot commit, the attempt stays
counted and the job is redelivered when the lease expires, as after a
crash.

Why a failed, retried attempt and not the "handler fails" row: a panic
carries no `HandlerError`, so there is no retryable or terminal verdict to
read, and the "handler fails" row would dead-letter a plain error at once.
The outcome should not depend on whether a supervisor recovers the panic.
Unrecovered, the process dies, and the job gets `MaxAttempts` runs before it
is dead (`claim_abandoned`). Recovered, it gets the same `MaxAttempts`
runs, without waiting out the lease each time, and a dead-letter code that
says what happened.

The accounting after a rolled-back handle transaction (give back, defer,
charge) is its own write-first transaction, matched on the start's lease, so
it leaves alone a job another worker has claimed since that lease expired.
If the process dies before it commits, the job keeps the counted attempt and
is redelivered when the lease expires; it is never acknowledged.

**The lease.** `worker.New` takes options; `worker.WithLease` sets the lease,
and `worker.DefaultLease` is 30 s. `New` refuses a lease that is not longer
than twice the store's busy timeout, including the default on a store
configured with a busy timeout of 15 s or more. Between the start and the
handler, a worker may wait for its turn in the store's writer queue and then
for the write lock (a writer on another handle or in another process), each up
to the busy timeout. A lease that ran out meanwhile would be claimed by another
worker, and the attempt lost (one of the "Lease taken" endings above). The
lease is not renewed while the handler runs, and need not be: the handler
holds the write lock, so no other worker can claim its job until its
outcome commits, however long it runs. What the lease bounds is how long a
job waits after its worker dies. 30 s is six times the default 5 s busy
timeout, three times the worst legitimate gap of two busy timeouts. It is
also the lease the claim already wrote before #245, and the default
visibility timeout of comparable queues. A shorter lease recovers faster
after a crash but leaves less margin over a long busy timeout; it must still
be more than twice the busy timeout.

`worker.Queue` implements the port. It persists the principal established by the
trusted producer (`EnqueueRequest.Principal`, `Job.Principal`) and makes it part
of request identity. `worker.New` migrates existing queues in place by adding a
`principal_json` column, with empty for existing jobs.

**Claim order (#217).** A worker claims the available job with the oldest
`created_at` first. Jobs that share a `created_at` are claimed in the order
they were enqueued. Before #217 they went in `job_id` (hash) order, which was
effectively random. Ties are common on Windows: Go's clock there advances in
steps of about 2 ms (0.3–12.7 ms measured on the #156 host), against about
1 µs on macOS.

The order is kept in an `enqueue_seq` column. Each enqueue sets it to one
past the highest so far, inside its write transaction, so it follows commit
order within one `created_at`: `created_at` is read before the writer
waits for the lock, so producers on different clock ticks are ordered by
timestamp. It is stored rather than taken from SQLite's `rowid`, which SQLite
documents `VACUUM` may renumber for tables without an `INTEGER PRIMARY KEY`.

This is not a strict FIFO:
- `created_at` is the enqueuing process's wall clock, so producers with
  skewed clocks, or a clock stepped back, are ordered by timestamp, not
  commit.
- A retried or deferred job keeps its `created_at` and competes again once
  `available_at` passes.
- Concurrent workers run their handlers one at a time, but a later job can
  finish first if an earlier one fails and is retried.

Jobs enqueued before the column existed keep 0 and fall back to `job_id`
among themselves.

### Retention of finished jobs (#290)

A job row holds everything its producer handed the queue: the request key,
the payload, the principal, the trace context, and for a failed job the
handler's error text. Before #290 nothing ever deleted a completed or dead
job, so all of it was kept forever, and the dedupe record above lived
exactly as long as the row. `Queue.Compact(ctx, worker.Retention{...})` is
the queue's erasure, built like journal compaction (ADR 0021 §7).

**What is erased.** `Retention.Completed` and `Retention.Dead` are separate
cutoffs: a completed or dead job whose last update (its finish) is strictly
before its state's cutoff is erased. A zero cutoff erases nothing of that
state, so dead letters, which ADR 0022 counts as retained for an operator,
stay until the application gives them a cutoff of their own. Pending and
processing jobs (waiting on a backoff, under a live lease, or stalled under
an expired one) are never read by `Compact`, however old. Each erased job's
row is deleted and replaced, in the same transaction, by a tombstone in
`worker_compacted`: the SHA-256 of its request key, its job id, kind,
state, attempt and max-attempt counts, creation and finish times, the time
it was compacted, and one *identity digest* over its kind, payload digest
and encoded principal. Payload, principal, trace context, error text and
the request key itself are gone with the row. The request key is digested
because it is application-chosen and may carry personal data, as the
journal's tombstones and audit prune tombstones digest theirs; the
principal is folded into the identity digest rather than digested alone, so
it cannot be confirmed from the tombstone without the exact payload too.

**The dedupe window.** The submission contract above says `accepted=false`
means the key was already committed with the same kind, payload and
principal, and a key reused with other content is a conflict, never silently
deduplicated. Erasing the row must not quietly turn a provider's late resend
into a second run. So a compacted job keeps its identity, and only its
identity, for a dedupe window that the application ends explicitly:

- Within the window, a submission of a compacted job's key with the same
  kind, payload and principal is a duplicate: `Enqueue` returns
  `Accepted=false` and a `Job` with `Compacted=true`, its ID, kind, state
  and attempt counts, and no payload, principal, trace or error (erased
  content is never returned again, as for erased reconciliations).
  `Submit` returns `false`; nothing is inserted and nothing runs. The trace
  is not identity, so a duplicate with another trace is still a duplicate.
- Within the window, the same key with another kind, payload or principal
  is `worker.ErrRequestConflict` (`trigger.ErrConflict`), as it was while
  the row lived.
- `Get` returns the tombstone (`Job.Compacted`), and `Settled` stays true.
- `Retention.Tombstones` ends the window: tombstones of jobs that finished
  strictly before it are deleted. A key whose tombstone is gone is unknown
  again: `Get` reports `ErrNotFound`, `Settled` is true, and a new
  submission under it is accepted as a new job. The zero time keeps every
  tombstone, which is the default and the only setting that keeps "a key
  once committed is never run twice" without a horizon. An application
  that ends the window must end it later than any producer can resend:
  for a webhook, later than the provider's redelivery horizon.

`Enqueue` checks the tombstone inside its insert: the insert is
`INSERT … SELECT … WHERE NOT EXISTS (tombstone) ON CONFLICT(request_key) DO
NOTHING`, one statement, so the transaction still writes first (#176) and
a concurrent `Compact` cannot slip between the check and the insert (both
hold the write lock).

**Legal hold and minimum.** `worker.WithRetentionHold` and
`worker.WithMinRetention` are queue options, not `Compact` arguments, so
whoever schedules compaction cannot bypass them, as with
`journal.Config.Hold` and `MinRetention`. The hold sees each candidate
(`RetainedJob`: id, request key, kind, state, principal, finish time) and
keeps it, with all of its content, when it returns true. A hold that panics
keeps the job (fails closed) and the pass goes on. The minimum clamps each
cutoff to `now − minimum`, so no job younger than it is erased whatever
cutoff is passed; `New` refuses a negative minimum. Held jobs are counted
in `CompactionReport.Held` and stay eligible on every later pass.

The hold is asked outside any write transaction (#313 review). A hold is
typically a lookup in a legal-hold service, and the first version asked it
inside the batch's write transaction: with a 25 ms hold and the default
batch of 256 the review measured the write lock held for 6.7 s, and a
concurrent `Enqueue` failed `admission_saturated … no write turn within
5s`. Now each batch is read in a read transaction, the hold is asked about
every candidate with no lock held, and only then does a write transaction
erase the ones it released. A slow hold delays only `Compact`; a job whose
verdict has not come back is simply not erased yet, and a panic still keeps
it. The write transaction deletes a row only if it is still the finished
job that was read (same id, state and finish time), so a job erased in the
meantime by another `Compact` is skipped. A hold placed in the service
after its verdict was given and before the erasure commits is not seen; the
window is one batch, and the same race exists for any hold decided before
the delete.

**Bounded batches.** `Compact` works in write transactions of at most
`Retention.Batch` rows (`worker.DefaultCompactBatch` = 256 when zero,
refused above `worker.MaxCompactBatch` = 4096) and of at most a tenth of
the store's busy timeout (500 ms by default): a transaction that has run
that long commits after its current row and the rest of the batch goes in
the next one, so it always erases at least one job and no writer queued
behind it waits anywhere near the busy timeout. Rows bound the work, time
bounds the lock. Each is marked `store.Writer` (#214) and writes first:
its first statement makes sure the erasure counter row exists. Candidates are read with a keyset cursor on
(`updated_at`, `job_id`) through a new partial index,
`worker_jobs_finished ON worker_jobs (state, updated_at, job_id) WHERE
state IN ('completed','dead')`, so each batch costs its own rows and never
rescans the history before it, and a held job never stalls the cursor. The
write lock is released between batches, so other writers, claims and
submissions take their turns in the store's writer queue. Tombstone expiry
is batched the same way through an index on `finished_at`. Measured on the
author's macOS arm64 host under a load average of about 5 to 16, with the
hold outside the lock: compacting 200,000 completed jobs with the default
batch took 782 write transactions and 13–14 s in three runs; the median
write transaction (including its wait for the writer turn and its durable
commit) took 13–15 ms, the longest 35–91 ms. With a 30 ms hold, and with a
300 ms hold (longer than the busy timeout on its own), an `Enqueue` loop
beside the compaction on a store with a 200 ms busy timeout never failed
and waited at most about 15 ms (`TestSlowHoldNeverBlocksWriters`); with the
hold inside the transaction the same test fails `admission_saturated`.
Unfinished jobs are not in the index, so claims do not maintain it; every
job that finishes adds one entry. Twelve interleaved runs of 2,000 trivial
jobs on the same host gave median enqueue rates of about 13.6 k/s on
`origin/main` against 12.6 k/s with #290, and processing 2.29 k/s against
2.19 k/s. The spread within each arm was larger than that difference, so
it is not resolved.

**Bytes, not just rows.** Erasure relies on the store-wide
`secure_delete=ON` (#281) to zero the deleted cells, index entries and
overflow pages, then purges the write-ahead log with the store's optional
`store.Purger.PurgeLog`, under the journal's pending-purge pattern: each
batch that erased a job increments `erasure_generation` in `worker_meta` in
the same transaction, a successful purge records the generation it covered
as `purged_generation`, and every later `Compact`, even one that erases
nothing and even in a restarted process, retries the purge while the debt
is unpaid. `CompactionReport.LogPurged` and `PurgePending` report it as the
journal's report does. A reader that keeps the log in use blocks the purge
for that pass only.

**The census.** `Census` (#105, ADR 0022) still reads only unfinished and
dead jobs through `worker_jobs_unfinished`. Compaction never changes its
pending, waiting, active and stalled counts or the backlog lag. Its
`DeadLetters` counts dead jobs still retained for an operator: a dead job
`Compact` erased is no longer one (its payload is gone; it cannot be
inspected or replayed), while a held dead job still is.

**Schema.** `worker.New` creates `worker_compacted`, its `finished_at`
index, `worker_meta` and `worker_jobs_finished` in place, with `CREATE …
IF NOT EXISTS` inside the existing retried schema transaction (#233), so
reopening changes nothing. No existing column changes.

### Webhook admission

`trigger/webhook` declares durable / redeliver / caller: an event is
acknowledged only after `Submit` commits, and an event that was not
acknowledged (lost connection, crash, refusal) is resent by the provider. ADR
0005's table now allows webhook `{durable, redeliver}` alongside
`{durable, detach}`.

Each request goes through these steps in a fixed order:
1. route and method (404 / 405);
2. application admission (503 with `Retry-After`);
3. a body read capped by `MaxBodyBytes`, 1 MiB by default and 16 MiB at most (413), and by a `ReadTimeout` deadline, 10 s by default and 1 minute at most (408), so a slow unauthenticated caller cannot hold an admission slot;
4. provider verification over the original header and exact body bytes (401);
5. the replay window `Tolerance`, 5 minutes by default and 24 hours at most, past and future (400 `stale_event`);
6. input validation against the endpoint schema (400 `invalid_input`);
7. durable submission, under a context the provider cannot cancel, bounded by `SubmitTimeout`. It is canceled only if the application's drain times out first; the delivery is then answered 503 `unavailable` with `Retry-After`, and the provider redelivers (ADR 0005, #177).

The window compares instants (`ts < received−tolerance || ts > received+tolerance`),
never durations, so a far-future timestamp cannot overflow past it. The
adapter enforces it for every verifier. A verifier may also reject an
out-of-window timestamp before hashing, by returning `ErrStale`, which maps to
the same 400.

The submission's outcome sets the response: 202 accepted, 200 duplicate, 409
conflict, 503 saturated (with `Retry-After`), or 500 without detail. The body
is never parsed before verification.

- **Deduplication** key: `webhook:<provider>:<event id>` (`SubmissionKey`).
  The `webhook:` prefix keeps webhook events out of other producers' key
  space on a shared queue. Provider names are validated identifiers, and
  `New` refuses two endpoints with the same provider, so tenants on one
  server cannot claim each other's event ids. Uniqueness across separate
  `Server`s that share a queue is the application's responsibility. Event ids
  are bounded to 256 bytes.
- **Verification** is pluggable through `Verifier`. Every failure returns
  `ErrUnverified`, and every response to one is the same 401, so a caller
  cannot tell which check failed.
- **Built-in scheme:** `StandardWebhooks`, the Standard Webhooks headers,
  with HMAC-SHA256 over `id.timestamp.body` compared in constant time.
  - Bounds: at most 8 `v1` signatures per header and `MaxKeys` = 4 keys per
    verifier. Verification computes one HMAC per active key, not one per
    key × signature.
  - Timestamps must be canonical positive integers no larger than
    `MaxTimestamp`, and are mandatory: there is no untimestamped mode.
  - Keys have validity windows, so a rotation overlaps by giving the old and
    new keys overlapping windows.
  - The signature covers `id.timestamp.body`, not the endpoint, so reusing
    one secret across endpoints lets a captured request be accepted on each
    of them. Give every endpoint its own key.
- **Principal:** the endpoint's `Principal` becomes the submission principal
  for every verified request. Keys carry none, so a retry signed with a
  rotated key deduplicates instead of conflicting. Payload fields never set
  it.
- **Secrets:** a `Secret` keeps its bytes behind a pointer. It renders as
  `[redacted]` through `fmt`, `json`, text and `slog`. Even printers that
  bypass its formatting (`%+v` of a struct holding it in an unexported field)
  print only an address. Custom verifiers use it only through `HMACSHA256`.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| `trigger.Submission`, `Submitter`, `ErrConflict`, `ErrInvalidInput` | additive | none |
| Webhook may declare durable / redeliver | additive (ADR 0005 table) | none |
| `worker.Queue.Submit`, `EnqueueRequest.Principal`, `Job.Principal` | additive | none |
| Same request key with a different principal now conflicts | behavioral | producers that passed no principal are unaffected |
| `worker.ErrInvalidPayload` / `ErrRequestConflict` wrap the trigger sentinels | additive (error chains) | `errors.Is` on the worker sentinels keeps working |
| `store.WriteDomainProvider` and `worker.ErrNestedSubmission` | additive | Stores/wrappers may expose lock identity; handlers should use `worker.Tx` for same-store atomic writes |
| Worker handler receives saturation from another store | behavioral | The job is deferred within its existing deferral budget without consuming an attempt |
| `store.WithWriteDomain` / `store.ErrorWriteDomain`; SQLite busy errors and worker saturation name their write domain | additive (error chains) | `errors.Is` on `store.ErrBusy` / `trigger.ErrSaturated` keeps working; other stores may annotate their busy errors |
| `store.ErrorWriteDomains` reports the outermost annotation on every branch of an error tree | additive | `store.ErrorWriteDomain` is unchanged and still reports the first |
| Worker handler returns saturation naming its own claim's domain, alone or joined with other failures in any order | behavioral | The job fails as `worker.ErrNestedSubmission` after one busy wait instead of being deferred to `deferral_budget_exhausted` |
| SQLite `:memory:` uses the memdb VFS instead of shared cache | behavioral | Same shared database per process; writer conflicts are `store.ErrBusy` within the busy timeout instead of an unbounded wait |
| `principal_json` column | schema | added by `worker.New` |
| A started attempt is committed before its handler runs (#245): `worker.DefaultLease`, `worker.WithLease`, `worker.ClaimAbandoned` | behavioral, additive | No schema change or migration: the start reuses `state`, `attempt` and `lease_until`. A job whose worker died mid-handler is redelivered when its lease expires instead of at once, counts that attempt, and dead-letters as `claim_abandoned` once its attempts are spent. A handle transaction that rolls back after the handler ran now counts its attempt |
| `worker.New(ctx, database, clock, opts ...Option)` | additive (source-compatible) | Existing three-argument calls compile unchanged. `New` now fails on a store whose busy timeout is 15 s or more unless `WithLease` sets a lease longer than twice it |
| One started attempt per write domain per process (claim turn) | behavioral | A worker that cannot take the turn within the busy timeout fails with `store.ErrBusy` naming the write domain and starts nothing, where it used to wait for the write lock with the same timeout and outcome |
| `store.BusyTimeoutProvider` / `store.BusyTimeoutOf`; SQLite reports its busy timeout | additive | Stores and wrappers may expose it; without it the worker assumes 5 s |
| Workers from before #245 on the same store (mixed versions) | compatibility limit | No migration is needed and both claim only pending jobs and expired leases, so an old worker skips a new worker's live lease. An old worker's own crashed attempts stay uncounted. An old worker that claims a job whose attempts new workers' crashes have spent does not dead-letter it at the claim, as new workers do: it runs the handler once more, at attempt `MaxAttempts`+1, and dead-letters it only if that attempt fails. Run one version per store |
| A handler that panics or calls `runtime.Goexit` fails its attempt (#267): `worker.HandlerPanicked` | behavioral, additive | No schema change. The panic still propagates from `ProcessOnce` unchanged. Where a recovered panic left the job leased with its handle transaction open, and the store busy for every writer until the process exited, the job is now pending after a backoff with error `handler_panicked`, or dead once its attempts are spent |
| Jobs tied on `created_at` are claimed in enqueue order; `enqueue_seq` column and index | behavioral, schema | added by `worker.New`; existing jobs keep 0 and stay in `job_id` order among themselves |
| `trigger.Submission.Trace`, `worker.EnqueueRequest.Trace`, `worker.Job.Trace`; `traceparent` and `tracestate` columns (#276, ADR 0020) | additive, schema | added by `worker.New` like `principal_json`; existing jobs carry no trace. The trace is not part of request identity: the same key with another trace is a duplicate that keeps the first. Workers from before #276 on the same store ignore the columns, so their jobs start root runs |
| `worker.Queue.Compact`, `Retention`, `CompactionReport`, `RetainedJob`, `WithRetentionHold`, `WithMinRetention`, `DefaultCompactBatch`, `MaxCompactBatch`, `Job.Compacted` (#290) | additive | none. Nothing is erased until the application calls `Compact`; existing `New` calls compile unchanged |
| `worker_compacted` and `worker_meta` tables; `worker_jobs_finished` and `worker_compacted_finished` indexes (#290) | schema | created by `worker.New` in place and idempotently. The first open of an existing queue indexes every finished job once, inside the schema transaction |
| A key whose job was compacted answers from its tombstone (#290) | behavioral | only after `Compact` ran: a duplicate is `Accepted=false` with `Job.Compacted`, other content conflicts, `Get` returns the tombstone. Once `Retention.Tombstones` passes, the key is new again |
| Workers from before #290 on the same store | compatibility limit | they do not read tombstones: a duplicate of a compacted job submitted through an old binary is accepted and runs again, and its `Get` reports not found. Run one version per store, as #245 already requires |
| New package `trigger/webhook` | additive | none |

## Limits

- The workflow runs when the queue processes the job, not in the request.
  Webhook conformance therefore exercises the composed webhook → queue →
  workflow path, and its disconnect cases model a lost queue consumer. HTTP
  caller loss and crashes around acknowledgment are covered by the webhook
  crash tests instead.
- Constant-time comparison relies on `hmac.Equal`; timing is not measured.
- The worker sorts and de-duplicates principal roles before comparing.
  Adding a column must read the schema before it writes, and SQLite cannot
  make such a transaction wait for another writer. So when several
  processes open a queue that needs a column, all but one fail busy at once:
  50 of 60 concurrent opens failed before #233. `worker.New` therefore
  retries the idempotent migration while it fails busy, with a short growing
  pause, starting new attempts for up to 10 s (so `New` can take 10 s plus
  one busy timeout), and the same probe now has no failures. An opener that
  still loses fails startup as before; nothing is corrupted.
- Only the Standard Webhooks scheme is built in. Provider-specific schemes
  (with their own header formats and key distribution) are written against
  `Verifier`, as the tests do for a synthetic provider.
- Secret values come from application configuration; there is no secret
  provider integration yet (E10).
- The deduplication record lives as long as the job's tombstone (#290):
  forever unless the application sets `Retention.Tombstones`. Tombstones
  are small (digests, counts and times) but they do accumulate, one per
  compacted job, for as long as the window lasts.
- The queue does not schedule its own compaction: the application calls
  `Compact` with its cutoffs, for example from a cron trigger.
- Tombstone digests are pseudonymous, not anonymous: anyone holding the
  database can confirm a guessed low-entropy request key, or a guessed
  exact payload and principal, by hashing it. The kind, attempt counts and
  times are kept in clear. A hold slower than its callers expect slows only
  `Compact`, which then takes as long as the hold needs.
- Erasure reaches the database file and its log, not copies: a backup taken
  before `Compact` still holds the content, and content deleted before
  secure deletion was on stays in free space until `store.Purger.PurgeFree`
  (ADR 0021 §7 applies unchanged). Run `Compact` on a restored database
  before serving it.
- Each job now commits two write transactions instead of one. The #245
  review measured worker throughput on trivial jobs about 20 % lower:
  5.5–7.1 k jobs/s on `origin/main` against 4.9–5.7 k jobs/s with #245,
  on macOS with WAL and `synchronous=FULL`. The author's own interleaved
  runs of `TestMeasureClaimContention` (4 workers, 200 instant jobs, 5
  samples per round) on the same shared macOS arm64 host were 22–28 ms
  against 29–32 ms before the claim turn was added. With it, at a load
  average of about 13, they spread 51–192 ms against 55–458 ms, too noisy to
  resolve the difference. All runs had 0 busy errors. macOS `fsync` does not
  flush the drive cache. On Linux, where `synchronous=FULL` flushes it, the
  second commit is one more durable flush per job, so the cost there is
  likely higher; it has not been measured.
- Recovery after a crash waits for the lease: by default up to 30 s, where it
  was immediate. The parity mid-execution kill sample
  (`testdata/parity/raw/mid-execution-kill.json`) went from 11 ms to
  30.04 s kill-to-completion, recovering as attempt 2.
- The claim turn serializes started attempts only within one process.
  Workers in other processes on the same database file can still start an
  attempt while a handler here holds the write lock. A crash of either
  process then charges the attempt the dead process had started, even if
  its handler never ran. Enough such crashes can dead-letter a job whose own
  handler never failed. Within a process, only the job whose handler was
  running is charged.
- One attempt is lost when the lease expires before the handle transaction
  takes it over. `New`'s lease bound makes that possible only if the
  writer-queue and lock waits between the two transactions together exceed
  the lease, which each busy-timeout bound rules out, or the queue's clock
  steps forward by more than the lease meanwhile. The claim turn entries live for the life of the process, one
  per write domain opened.
