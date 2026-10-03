# ADR 0008: WebSocket connection and message bindings

- Status: accepted
- Date: 2026-10-03
- Roadmap: E09-T07 ([#60](https://github.com/well-prado/new-blok/issues/60))
- Builds on: [ADR 0005](0005-trigger-adapter-contract.md)

## Context

A WebSocket is long-lived and two-way. A client can send faster than
workflows run, stop reading its replies, go silent, send frames that are too
large or not text, claim another connection's identity in a message, or
disappear while a message is running. Each of these needs a declared policy,
not an accident of buffering.

## Decision

`trigger/websocket` declares memory / cancel / caller. One endpoint binds
three workflows: `OnConnect` (may refuse the connection), `OnMessage` (the
reply is its output) and `OnDisconnect` (runs exactly once per accepted
connection).

| Concern | Policy |
| --- | --- |
| Admission | Before the protocol switches, cheapest check first: application admission (503); browser origin (403: the listed patterns or the same host; a request without an `Origin` header, i.e. a non-browser client, is allowed); `MaxConnections`, 1024 by default and 65 536 at most (503); then authentication of the upgrade request (401). A foreign origin or a full endpoint never reaches the authenticator. |
| Identity | The server assigns a random connection id. The frame envelope admits only `id` (a request id of up to 64 printable bytes, echoed in the reply) and `input`. Any other envelope field is an `invalid_frame` error, and a connection id inside `input` is plain data. |
| Validation | `input` is validated against the endpoint schema (`invalid_input`). A malformed text frame gets an `invalid_frame` reply and the connection continues. |
| Frame limits | A text frame larger than `MaxMessageBytes` (64 KiB by default, 1 MiB at most) closes the connection with 1009. A binary frame closes it with 1003. |
| Ordering | One message at a time per connection, in arrival order; replies come back in the same order. Connections are independent of each other. |
| Inbound flood | Frames wait in a bounded per-connection queue (`QueueDepth`, 16 by default and 1024 at most; `QueueDepth × MaxMessageBytes` at most 16 MiB). Overflow closes the connection with 1008 (`backpressure`). |
| Memory | Each connection holds at most its queue plus one frame being read and one being processed, so an endpoint holds at most `MaxConnections × (16 MiB + 2 × MaxMessageBytes)`, about 1.1 GiB at the defaults' worst case. Replies are capped by `MaxReplyBytes` (1 MiB by default, 16 MiB at most); a larger output is answered with `reply_too_large`. |
| Slow reader | Each reply must be written within `WriteTimeout` (5 s); otherwise the connection is closed (`slow_reader`). A timer records that reason before anything else can, so it is never misreported as the client closing. |
| Liveness | The server pings every `PingInterval` (30 s); a missing pong within `PongTimeout` (10 s) closes the connection (`ping_timeout`). |
| Workflow time and lifecycle | `MessageTimeout` (30 s) bounds every `OnConnect`, `OnMessage` and `OnDisconnect` call. `OnConnect` and `OnMessage` run under an application lease. `OnDisconnect` takes a lease when the application still admits work, but runs even while it drains. Hosts call `Server.Shutdown` before `app.Shutdown`, so dependencies outlive these handlers. |
| Disconnect | Any close cancels the in-flight message. The first cause becomes the reason, and `OnDisconnect` runs exactly once, even when a client drop and a server shutdown race. |
| Errors | Classified codes pass through `trigger.Classify`; saturation becomes `saturated`; anything else becomes `internal`, without its text. |
| Shutdown | `Shutdown` refuses new upgrades, closes every connection with 1001, and waits until each has run `OnDisconnect` and released its goroutines. |

Cancelling a connection's work does not tear down the socket: writes and
pings use contexts independent of the work context. The close handshake
carries the status code to the peer, even after a reply that was being
written when the close began, and is what unblocks the reader. So a client
that is still reading always sees why it was closed. A peer that has stopped
reading cannot receive a close frame.

### Dependency

`github.com/coder/websocket` v1.8.15 (ISC license, no transitive
dependencies) implements RFC 6455. Only applications that import
`trigger/websocket` link it; the selection binaries assert the others do
not.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| New package `trigger/websocket` | additive | none |
| `go.mod`: `github.com/coder/websocket` v1.8.15 | additive | none |

## Limits

- **Close handshake.** A peer that never reads holds a close handshake for
  up to the library's 5 s timeout, so shutting down non-reading clients
  takes up to 5 s.
- **Measured footprint.** On the development host (linux/arm64 container,
  without the race detector): 3.0 goroutines, about 35 KB of heap and about
  66 KB of RSS per open connection, counting client and server sides
  together. The test bounds are 3.5 goroutines, 96 KB of heap and 256 KB of
  RSS (2 MiB under the race detector). This is a regression bound, not a
  capacity claim.
- **Per-connection concurrency.** Messages on one connection are never
  concurrent. Workflows that need parallelism should fan out themselves.
- **Not covered.** No WebSocket compression, subprotocol negotiation or
  resumable sessions. A reconnecting client is a new connection with a new
  id.
