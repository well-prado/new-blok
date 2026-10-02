# ADR 0007: Pub/sub delivery transfer and the NATS JetStream driver

- Status: accepted
- Date: 2026-10-02
- Roadmap: E09-T04 ([#57](https://github.com/well-prado/new-blok/issues/57))
- Builds on: [ADR 0005](0005-trigger-adapter-contract.md), [ADR 0006](0006-durable-submission-and-webhooks.md)

## Context

A broker delivers each message at least once and redelivers anything that is
not acknowledged. If Blok ran the workflow inside the broker's delivery,
retries would belong to the broker and a crash between the effect and the
ack would run the workflow twice. The architecture asks for an explicit
transfer: the broker owns a message until Blok owns it durably.

## Decision

### Transfer model (`trigger/pubsub`)

`pubsub.Consumer` moves messages from a broker `Source` into the shared
durable submission port (`trigger.Submitter`, ADR 0006). For each message it
checks, in order:

1. size (`MaxMessageBytes`, 1 MiB by default, at most 16 MiB);
2. a stable identity, bounded to 256 printable bytes;
3. the input schema.

Then it submits under key `pubsub:<subscription>:<message id>` and the
subscription's configured principal, and acknowledges the broker only after
`Submit` returns. Messages a fetch returns alongside an error are still
processed, every message in a batch gets its turn even if a broker call
fails for an earlier one, and broker calls run under a bounded context that
shutdown does not cancel.

- **Ack after commit.** A crash or lost connection before the ack leaves the
  message with the broker. Its redelivery is a duplicate submission
  (acknowledged without a second run), because the identity is stable across
  redeliveries.
- **Execution retries belong to the durable queue**, not to the broker.
- **Saturation** is backpressure: the message is valid, so it is nak'ed with
  exponential backoff (1 s doubling to 1 minute) and never dead-lettered for
  it, however many deliveries that takes.
- **An unavailable store** naks with the same backoff until the delivery
  budget (`MaxDeliver`, 5 by default, at most 100) is spent, then
  dead-letters with `delivery_budget_exhausted`. The budget is checked only
  after a failed submission, so a redelivered message that is already
  committed is acknowledged as a duplicate, never falsely dead-lettered.
- **Poison and conflicts.** A message that can never be admitted (too large,
  unusable id, invalid input, conflicting reuse of an id, budget spent) is
  recorded on the dead-letter destination and terminated only after that
  record is durable. If the dead-letter write fails, the message is nak'ed
  and retried. Because the broker never caps deliveries (below), a
  dead-letter outage can delay the record but cannot drop the message.
- **Lost acks.** If the ack itself fails after commit, the consumer reports
  `ack_failed` and relies on redelivery, which deduplicates.

`Run` processes batches until its context ends, then returns
`ctx.Err()`. Any other error ends it so the caller can reconnect; nothing
unacknowledged is lost.

The core imports no broker client. The engine-boundary check now refuses any
import outside the standard library and this module, directly
(`engine_external_import_forbidden`) or through module packages
(`engine_transitive_import_forbidden`).

### Cursor and ack durability (NATS JetStream, `trigger/pubsub/natsjs`)

The driver consumes a durable pull consumer with explicit ack:

- **Cursor.** The broker's acknowledgment floor is the durable cursor. A
  restarted consumer resumes after acknowledged messages and receives
  unacknowledged ones again after `AckWait`, 30 s by default and between
  1 s and 12 h.
- **Ack.** An ack is a double ack: it returns only after the broker
  confirms it.
- **Identity.** A message's id is `h:<publisher id>` from the configured
  header (`Nats-Msg-Id` by default) or, without one, the stored position
  `s:<stream>@<incarnation>:<sequence>`. The incarnation is the stream's
  creation time, so a deleted and recreated stream's restarted sequences
  cannot collide with old identities, and the prefixes keep the two id
  spaces disjoint. With `Nats-Msg-Id`, NATS also drops a republished id
  inside the stream's duplicate window before delivery, including one with
  a changed payload. Outside that window, Blok's store detects the conflict
  and dead-letters it.
- **Flow control.** The consumer is created with
  `MaxAckPending = MaxRequestBatch = MaxInFlight`. The broker therefore never
  hands out more unacknowledged messages than the bound, whatever the
  consumers' speed. The bound applies to the durable consumer as a whole,
  shared by every instance, not per process.
- **Budget.** The broker's `MaxDeliver` is -1 (unlimited). The consumer owns
  the budget, so the broker can never give up on a message before it is
  admitted or recorded as a dead letter.
- **Rebalance.** Consumers sharing the durable name split the stream. A lost
  instance's unacknowledged messages go to its peers after `AckWait`.
- **Validation before consuming.** `NewConsumer` refuses to fetch anything
  unless all of these hold:
  - the subscription is valid, with a principal and bounds;
  - the subject is stored by the named stream;
  - the dead-letter subject is literal (no wildcards), is stored by some
    stream, and is not matched by the consumer's own filter;
  - an existing durable consumer matches on every delivery-relevant setting:
    ack policy and wait, deliver policy and start position, filter
    subject(s), `MaxDeliver`, back-off, replay policy, `MaxAckPending`,
    request batch, expiry and byte limits, headers-only, rate limit,
    inactivity, pause, priority groups, and push delivery. A difference is
    refused, never silently changed, and the error names the fields.

### Principal

Publishers are trusted through the broker's access control, so pubsub
declares `trusted-producer`, and every submission carries the
*subscription's* configured principal. The conformance harness gains an
optional `PrincipalSource` capability so a trusted-producer driver can
declare that principal. Every dispatch must carry exactly it, and a payload
field can never set it.

### Dependency

`github.com/nats-io/nats.go` v1.54.0 (Apache-2.0) is added for the driver,
with its transitive modules:

| Module | Version | License |
| --- | --- | --- |
| `github.com/nats-io/nkeys` | v0.4.16 | Apache-2.0 |
| `github.com/nats-io/nuid` | v1.0.1 | Apache-2.0 |
| `github.com/klauspost/compress` | v1.20.0 | BSD-3-Clause / Apache-2.0 |
| `golang.org/x/crypto` | v0.57.0 | BSD-3-Clause |
| `golang.org/x/sys` | v0.47.0 → v0.48.0 (bump) | BSD-3-Clause |

Only an application that imports `natsjs` links them; the selection binaries
assert that the no-trigger, HTTP-only and worker-only binaries do not.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| `trigger/pubsub`, `trigger/pubsub/natsjs` | additive | none |
| `conformance.PrincipalSource` | additive | none |
| graphcheck `engine_external_import_forbidden` | additive rule | the engine already imported only stdlib and module packages |
| go.mod dependencies above | additive (`x/sys` minor bump) | `go mod verify` passes |

## Limits

- **One driver.** NATS JetStream is the only one; other brokers implement
  `pubsub.Source`.
- **Gated integration suite.** It needs a real broker
  (`NEWBLOK_NATS_URL`). Without one, those tests report SKIP. The core is
  always tested against a broker model.
- **Ordering.** Messages in a batch are processed in order, but redelivery
  and rebalancing do not preserve global order.
- **Trusted publishers.** A publisher can choose its message id; publishers
  are trusted, by declaration.
- **Dedup lifetime** is the queue job's (E07 retention).
- **Term is not confirmed by the broker.** A lost Term causes a redelivery
  and a second dead-letter publish. That second publish is deduplicated only
  within the dead-letter stream's duplicate window, so a dead-letter stream
  whose duplicate window is shorter than `AckWait` can hold duplicate
  records (never a missing one).
- **A permanently failing dead-letter destination** keeps a poison message
  with the broker, retried at most once a minute. It is an operational
  alarm, not data loss.
- **`Subscription.Name` must be unique per queue.** Publisher ids are
  namespaced by it, not by stream.
- **Conformance disconnect** cancels the queue consumer. Broker-connection
  loss is covered by the TCP-cut test instead.
- **Server version.** The broker was NATS server 2.11.17 (`nats:2.11-alpine`,
  `sha256:e4bf19f15fd3218814a4e3c9e0064e1334bd8aa20d5984b9f1a0afd084f8cc00`);
  other server versions are untested.
