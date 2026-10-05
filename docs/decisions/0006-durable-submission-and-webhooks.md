# ADR 0006: Durable submission port and signed webhook admission

- Status: accepted
- Date: 2026-10-02
- Amended: #188 (worker nested-store diagnostics and saturation), #207
  (undetected self-submits fail after one busy wait), #245 (a started
  attempt is committed before its handler runs), #267 (a panicking handler
  fails its attempt and releases the write lock)
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
- The deduplication record lives as long as the job row; retention is the
  queue's (E07).
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
