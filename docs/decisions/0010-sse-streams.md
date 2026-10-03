# ADR 0010: SSE streams with replay cursors and a slow-client policy

- Status: accepted
- Date: 2026-10-03
- Roadmap: E09-T06 ([#59](https://github.com/well-prado/new-blok/issues/59))
- Builds on: [ADR 0005](0005-trigger-adapter-contract.md), [ADR 0006](0006-durable-submission-and-webhooks.md)

## Context

Server-Sent Events let a browser follow work as it happens over a plain HTTP
response. The connection drops often: on mobile networks, behind proxies,
when a tab sleeps. EventSource reconnects on its own and sends the id of the
last event it saw. Without explicit rules, a stream silently loses events
across a reconnect. A slow reader then makes the server buffer without bound,
a subscriber reads another user's progress, or closing a tab cancels work
the user meant to keep. The issue also requires that progress is not
treated as a durable signal, and that dropping progress is a different
policy from keeping an audit trail.

## Decision

`trigger/sse` declares durable / detach / caller. Work is started and
followed with two requests:

### Start: `POST <path>` with an `Idempotency-Key` header

Its steps run in a fixed order:
1. route and method (404/405);
2. application admission (503). The start holds its application lease
   until it has answered, so the application cannot stop while the durable
   submission below is in flight (#175); a start that arrives while the
   application drains is refused;
3. authentication (401), before the body is read;
4. the key, 1–256 printable ASCII characters (400 `invalid_key`);
5. a body read bounded by `MaxBodyBytes` (413) and `ReadTimeout` (408);
6. validation against the endpoint schema (400 `invalid_input`);
7. a stream slot in the hub (503 `saturated` when the hub is full or the
   caller is at its stream quota);
8. the durable submission through `trigger.Submitter`, under a context the
   caller cannot cancel.

The submission key is `sse:<endpoint>:<sha256 of the principal>:<key>`, with
the normalized input and the caller's principal. Including the principal keeps
one principal's keys out of another's. The stream id is derived from the
submission key (`StreamID`), so the worker that runs the work knows which
stream to publish to.

The outcome is the response: 202 accepted, 200 duplicate, 409 conflict, 503
saturated, or 500 without detail. Both 202 and 200 carry the stream id.

Starts that are in flight for a stream are counted. A stream that no start
committed and no worker published to is dropped when its last start settles.
A start that fails therefore never removes a stream that a concurrent start
was acknowledged for: a conflict means the key is committed, so its stream
belongs to that work. When the last start of a stream that no work stands
behind fails, the stream goes, and a subscriber that attached meanwhile is
sent away (`no_work`).

**A stream must be able to end.** A stream is *verified* once something
shows that its work will end it: an accepted submission, a publication, or the
endpoint's required `Tracker`, which worker.Queue implements. A stream that a
repeated start recreated (its old one was evicted, or the process restarted)
is *unverified*, because its work may have settled with nobody left to finish
it. An unverified stream is checked with the tracker before anyone follows
it:
- **Who checks.** A repeated start checks it, whether it is a duplicate or a
  conflict, since its key is known to be committed. A subscriber checks it
  only while no start is in flight, because a key that is not yet committed
  reads as settled.
- **Settled:** the work's progress and outcome are gone, so the stream ends
  at once with an `expired` final event instead of staying open forever. If
  the worker's real `Finish` arrives later (the check landed between the job's
  commit and its `Finish`), it replaces the expired notice. A subscriber that
  reconnects from the notice's id then receives it. The replaced notice
  leaves a hole in the sequence, which is not counted as a lost event.
- **Still running:** the stream is verified and followed.
- **Tracker failure:** the start answers 503 `unavailable` and the stream
  stays unverified; a subscriber is answered with the retry hint, and the
  next check decides.

### Subscribe: `GET <path>/<stream>`, the EventSource protocol

Everything is decided before the response status, in this order:
admission (a subscription is long-lived, so it is admitted but does not hold
the application open); authentication (401); the stream id format; whether the writer
supports write deadlines (500 if not, because writes must be bounded and
interruptible; the small JSON refusals before this point are written
without one); subscriber limits; the `Last-Event-ID` cursor (400
`invalid_cursor`); the stream's existence; and `Authorize(reader, owner)`.
The default authorization allows only the principal that started the
stream. A reader who is not allowed gets the same 404 as for an unknown
stream, so a refusal does not reveal that the stream exists.

Permanent refusals are JSON errors, after which EventSource stops. Transient
refusals (subscriber limits, a draining application, shutdown) are answered
as an empty event stream: the `retry:` hint and a comment naming the reason,
so EventSource tries again. EventSource gives up on any error status.

### Progress is a bounded, in-memory hub

The `Hub` holds each stream's recent events.

- **Not durable.** It is memory and starts a new random epoch at each
  restart.
- **Ids.** Every stream the hub creates gets a new incarnation, and event ids
  are `<epoch>.<incarnation>:<seq>`, with `seq` counting from 1. No id ever
  names two events, even when a stream is evicted and recreated in the same
  epoch.
- **Bounds.**
  - Per stream: `RetainEvents` (256) events and `RetainBytes` (256 KiB);
    the oldest events are dropped first.
  - Per event: `MaxEventBytes` (64 KiB).
  - Per hub: `MaxStreams` (1024) streams. `MaxStreams × RetainBytes` is at
    most 1 GiB.
  - Per principal: `MaxStreamsPerOwner` (64).
- **Stream eviction.** A full hub evicts a stream that nobody reads or is
  starting, in this order:
  1. a finished stream past `Retention` (10 min);
  2. an unfinished stream idle past `Retention`, whose work was abandoned or
     whose publisher is gone;
  3. the least recently used finished stream.

  An unfinished stream still in use is never evicted; a new start is then
  refused with 503 before anything is submitted. A principal at its quota
  first evicts one of its own streams by the same rules, so one principal
  cannot lock others out.
- **Finishing.** `Finish` publishes the final event. Subscribers receive it,
  their streams end, and later events are refused.
- **Publishing never blocks** on a subscriber.

### Cursors and gaps

A subscription first sends `retry: <ms>`, then replays what follows its
cursor, then follows live. Replay and registration happen under one lock, so
no event falls between them.

| Cursor | Outcome |
| --- | --- |
| none | replay everything retained; a `gap` first if early events were dropped |
| this incarnation, still retained | replay the events after it |
| this incarnation, older than retention | `gap` with reason `retention` and the number missed, then replay what is retained |
| an earlier incarnation of the stream | `gap` with reason `retention`, the number missed unknown, then replay what is retained |
| another epoch (a restart) | `gap` with reason `restart`, then replay what is retained |
| ahead of the stream, a later incarnation, or malformed | 400 `invalid_cursor`, no stream bytes |
| at a finished stream's final event | 204, which stops EventSource reconnecting |

A gap event carries the id just before the first replayed event, so a
client that reconnects right after it continues without another gap.

### Framing and liveness

- **Framing.** Each event is `id:`, `event:` and `data:` lines and a blank
  line.
  - Data is JSON, compacted, so it never spans lines.
  - Types match `^[a-z][a-z0-9_.-]{0,63}$`, so a type cannot inject a field.
  - `gap` is reserved.
  - Work ends with `result`, `failed` or `expired`. `failed` is used rather
    than `error`, because EventSource also dispatches its own connection
    errors as `error`.
- **Headers.** `text/event-stream`, `Cache-Control: no-cache` and
  `X-Accel-Buffering: no`.
- **Retry hint.** Between 100 ms and 10 minutes.
- **Heartbeat.** A silent subscription gets a `: heartbeat` comment every
  `Heartbeat` (15 s). An event resets the interval.
- **Duration.** A subscription ends after `MaxDuration` (30 min), and the
  client resumes from its cursor.

### Slow clients and memory

- **Queue.** Each subscriber has a queue of `QueueDepth` (64) events. A
  publish that finds it full disconnects that subscriber (`slow_subscriber`)
  instead of buffering or waiting.
- **Write deadline.** Every write has a deadline of `WriteTimeout` (10 s);
  a socket that stops draining is closed (`write_timeout`).
- **Interrupting a blocked write.** Disconnecting a subscriber, by the
  queue rule or by `Shutdown`, also interrupts a write in progress by moving
  its deadline to now. The subscription therefore ends at once, not at its
  write deadline. The interrupt never runs under the hub's or the server's
  lock: under HTTP/2 it waits on the connection.
- **Replay.** The replay is written and flushed in 64 KiB chunks, and a
  write buffer larger than that is dropped after use. Once written, the
  replay is not referenced, so events the hub drops are freed even while the
  subscription stays open.
- **Per-subscriber bound.** Beyond its stream's retained events, which are
  shared and not copied, a subscriber holds at most `QueueDepth` events. `New`
  refuses `QueueDepth × MaxEventBytes` above 16 MiB; with the defaults that is
  4 MiB.
- **Endpoint bound.** The worst case for an endpoint is `MaxSubscribers`
  times that, plus the hub's retention.
- **Limits.** `StreamSubscribers` (8 per stream) and `MaxSubscribers`
  (1024 per endpoint).

### Disconnect and durable work

- **A subscriber leaving** stops following; the work is not the
  subscriber's.
- **A caller leaving** during the start does not cancel the submission.
- **The work** runs in the worker. The application's worker wiring
  publishes progress and calls `Finish` with `Result(output)` or
  `Failure(err)` after the job's transaction commits. `Failure` publishes
  only a stable code: a classified error's code, `saturated` or `internal`.
- **Saturation.** By default the worker defers a saturated job. An
  application that wants an interactive stream to fail fast finishes it with
  `Failure(trigger.ErrSaturated)` and fails the job instead; the conformance
  wiring does this.

### Drop policy and audit policy are separate

- **`Publish`** is progress. It is best effort and may be dropped by
  retention, eviction or a full hub. The drops are counted in `Stats`, and a
  subscriber sees them as gaps.
- **`Record`** writes an `AuditRecord` to the hub's `AuditSink`
  synchronously, outside the hub's lock and before anything is published. A
  sink failure is returned (`ErrAudit`) and nothing is published. A stored
  record is never affected by retention, eviction, a full hub or a slow
  subscriber, although its progress copy may be.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| New package `trigger/sse` | additive | none |
| The HTTP selection binary must not link `trigger/sse` | test | none |

## Limits

- Progress does not survive a restart. Subscribers see a `restart` gap, and a
  stream is followed again only after its start is repeated, which is a
  duplicate. Durable progress would need a store-backed hub, which is not
  provided.
- One hub per process. Subscribers connected to another replica see nothing
  from this one; multi-replica fan-out needs a shared hub (E18).
- Conformance exercises the composed path SSE start → worker queue →
  workflow → hub → stream. As with webhooks (ADR 0006), the workflow runs in
  the queue's consumer, not in the request.
- Authorization compares principals, through the default or the endpoint's
  `Authorize`. Field-level redaction inside an event is the publisher's
  responsibility: the adapter isolates streams, not fields.
- `ReadTimeout` on the start, and the write deadlines, rely on
  `http.ResponseController`. A subscription through middleware that hides it
  is refused; a start through it is bounded only by the server's own
  timeouts.
- Stream ids are derived from the principal and the idempotency key. They
  are not secrets: authorization, not the id, protects a stream.
- A subscriber that reconnects from an expired notice in the moment between
  the check and the worker's real `Finish` gets 204 and stops; a later
  subscriber receives the real outcome. A stream evicted after it expired
  comes back without an owner if the real `Finish` arrives, and is followed
  again only after the start is repeated.
