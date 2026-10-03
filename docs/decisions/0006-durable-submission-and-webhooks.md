# ADR 0006: Durable submission port and signed webhook admission

- Status: accepted
- Date: 2026-10-02
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
next attempt. A worker handler that returns a busy store or a spent submit
deadline fails rather than deferring: the usual cause is the handler's own
claim, which holds the write lock it waited for (it wrote outside `tx`, for
example by submitting to the same store), and deferring would repeat the
deadlock. This also treats a busy *other* store as a failure; telling the
two apart is tracked separately.

A worker handler writes through `worker.Tx`, the claim's own transaction,
so its writes commit only if the job is acknowledged. SQLite can end that
transaction under the handler: it rolls the whole transaction back when a
statement is interrupted (the consumer was lost and the statement observed
the canceled context), fails for want of space, memory or I/O, or hits a
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
  comments) is refused with `worker.ErrTransactionControl`: the claim owns
  its transaction. A multi-statement string that hides one after another
  statement is not detected; handlers must not issue them.
- Statements still run under the handler's context, so cancellation and
  deadlines keep working. `ProcessOnce` routes its own post-claim
  statements through the same check.

A claim lost to consumer cancellation is deferred as before. A claim lost
any other way is a failed attempt, charged in a fresh transaction: the job
is retried after a backoff, or dead-lettered once `MaxAttempts` is spent,
with error `claim transaction ended`. Like any failed attempt, `ProcessOnce`
reports it as processed without an error; the job's error records why.

`worker.Queue` implements the port. It persists the principal established by the
trusted producer (`EnqueueRequest.Principal`, `Job.Principal`) and makes it part
of request identity. `worker.New` migrates existing queues in place by adding a
`principal_json` column, with empty for existing jobs.

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
7. durable submission, under a context the provider cannot cancel, bounded by `SubmitTimeout`.

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
| `principal_json` column | schema | added by `worker.New` |
| New package `trigger/webhook` | additive | none |

## Limits

- The workflow runs when the queue processes the job, not in the request.
  Webhook conformance therefore exercises the composed webhook → queue →
  workflow path, and its disconnect cases model a lost queue consumer. HTTP
  caller loss and crashes around acknowledgment are covered by the webhook
  crash tests instead.
- Constant-time comparison relies on `hmac.Equal`; timing is not measured.
- The worker sorts and de-duplicates principal roles before comparing.
  Concurrent first opens of a pre-migration queue may fail one opener with
  `SQLITE_BUSY` (startup fails; nothing is corrupted). This is not
  reproduced.
- Only the Standard Webhooks scheme is built in. Provider-specific schemes
  (with their own header formats and key distribution) are written against
  `Verifier`, as the tests do for a synthetic provider.
- Secret values come from application configuration; there is no secret
  provider integration yet (E10).
- The deduplication record lives as long as the job row; retention is the
  queue's (E07).
