# ADR 0014: MCP tool exposure over the reviewed catalog

- Status: accepted
- Date: 2026-10-03
- Roadmap: E09-T08 ([#101](https://github.com/well-prado/new-blok/issues/101))
- Builds on: [ADR 0005](0005-trigger-adapter-contract.md), the reviewed
  catalog and durable approvals of E14-T01/E14-T02 (#74, #75)

## Context

The Model Context Protocol lets an agent host discover tools and call them.
Blok already has typed tools: nodes and composed workflows registered in the
agent catalog, with capability manifests, budgets and, for effects, durable
reviewed approvals. MCP has to reach those tools without becoming a second
way around them. The risks are concrete: a model that sees tools nobody chose
to expose, a caller who names an approval and is treated as having one, a
session reused by another caller, a client that goes away and leaves its work
running, an error string that carries a connection string to the model, and
a flood of calls or sessions that is held without bound.

## Decision

`trigger/mcp` declares memory / cancel / caller: a call completes in band; a
client that cancels it, drops the HTTP request that carries it, or ends its
session cancels its work; every request is authenticated.

### Transport and protocol

The adapter serves Streamable HTTP through the official MCP Go SDK,
`github.com/modelcontextprotocol/go-sdk` v1.8.0, in stateful mode. A client
negotiates one of 2025-11-25, 2025-06-18, 2025-03-26 or 2024-11-05. A request
whose `Mcp-Protocol-Version` header names a version older than 2026-07-28
that the server does not support is refused with 400 (the SDK does not
refuse newer, unknown header values on every request). Calls under the draft
2026-07-28 protocol, which carry no session, are refused (`-32022`): a
session-less call would escape the session that binds a caller and ends its
work. The SDK client probes 2026-07-28 with `server/discover` first and falls
back to `initialize`.

The adapter offers no standalone SSE stream. It never sends a message
outside the request it answers: no list-changed or resource-updated
notifications (a view's tools and resources are fixed before any session
uses it, and nothing subscribes), no logging, no keepalive pings, and no
server-to-client requests (sampling, elicitation, roots). A call's own
messages travel on the POST that carries it. A GET therefore has nothing to
carry, and the adapter refuses it, after authentication, with 405 and
`Allow: POST, DELETE`, as the specification lets a server that does not
offer the stream do; any other method but POST and DELETE gets the same.
Under 2026-07-28 the GET stream is gone from the protocol anyway.

### Authentication and sessions

Every HTTP request carries a bearer token. The application's `Authenticator`
resolves it to a `tool.Principal` (id, capabilities, maximum depth). Any
failure, including an outage of whatever the authenticator consults, and a
blank principal id are 401; the token is never echoed. The token's user id is
the principal id, so the SDK binds a session to the principal that opened it
and answers 403 when anyone else uses its session id, on POST and DELETE.

A call runs as the principal **its own request** authenticated, not the one
that opened the session: capabilities revoked while a session stays open
apply to the next call. Discovery (`tools/list`, `resources/list`,
`resources/read`) is the session's snapshot from when it opened; a session
opened after a revocation sees the narrower set.

### What is visible

A tool is visible only when all three hold:
1. the endpoint's `Expose` names it as `name@version`;
2. the catalog lists it for the principal (capabilities filter it);
3. its input schema is an object, and its output schema, if any, parses.

The MCP name is the tool name with `/` as `.` and `_v<version>` appended
(`orders/charge@1.0.0` → `orders.charge_v1.0.0`); two exposed tools that map
to one name are refused at `New`. Each visible tool's descriptor (name,
version, description, schemas, effects) is also a resource,
`newblok://tools/<name>/<version>`. A tool without effects is announced
read-only. Nodes and composed workflows are both catalog tools; exposing a
workflow does not expose its nodes.

A view (the tools of one principal and capability set) is built only by a
request that opens a session; the SDK asks for a server on every request, and
every other request uses a cached view or none. Up to `MaxPrincipals` views
are kept (oldest evicted; its sessions keep their server until they end or
`Shutdown` closes them through the session registry). A panic while listing refuses that session only.

### A call, in order

1. The principal of the call's request must be the session's (else tool
   error `unauthorized`; the SDK already refuses another principal with 403).
2. A deadline from `Timeout` (≤ 5 min), also the budget's deadline. The call
   is canceled when the client sends `notifications/cancelled`, when the HTTP
   request carrying it ends (without an event store a dropped response stream
   cannot be resumed, so its result would be lost anyway), and when its
   owner ends the session.
3. Server closing → tool error `unavailable`.
4. The principal's calls in flight (`MaxCallsPerPrincipal`), then a global
   slot (`MaxConcurrency`); none free → tool error `saturated` at once, never
   a queue.
5. An application lease for the call itself, so the application cannot stop
   under it even after its request is gone; draining → `unavailable`. If the
   drain times out first, the call is canceled and answered `canceled`, not
   as a refusal to retry: it may have committed (ADR 0005, #177).
6. The arguments are normalized against the input schema; failure is the
   tool error `invalid_input`, before the catalog sees the call. It is a tool
   error, not a protocol error, so the model can correct its arguments.
7. `_meta["newblok.dev/approval"]`, if present, must be 1–256 printable ASCII
   characters; otherwise the protocol error `-32602 invalid_approval` (the
   `_meta` is the client's, not the model's). It is passed on as a **name**.
   Naming a decision grants nothing: the catalog looks it up.
8. `Catalog.Invoke` with the current principal. A panic in it is a tool error
   `internal`.
9. A catalog that returns after the deadline or cancel has failed the call
   (`deadline_exceeded` / `canceled`), as in the gRPC adapter. An effect it
   committed is recorded by the journal, not by the reply.
10. Output larger than the budget's `MaxOutputBytes` is `budget_exceeded`;
    output outside the output schema is `invalid_output`. Neither is
    returned.

An unknown, hidden or unexposed tool is the SDK's protocol error `-32602`.

### Errors

A failing call is a tool error result (`isError: true`) whose only content is
`{"code": "<stable code>"}`. The model sees the code, never the error text:

| Cause | Code |
|---|---|
| deadline | `deadline_exceeded` |
| client cancel, dropped request or session end | `canceled` (seldom seen: the client has gone) |
| a call whose principal differs from the session's | `unauthorized` |
| server closing or application draining | `unavailable` |
| no slot, or `trigger.ErrSaturated` from the catalog | `saturated` |
| arguments outside the input schema | `invalid_input` |
| `mcp.ErrApprovalStale` (missing, rejected, expired or for another call) | `approval_stale` |
| `mcp.ErrEvidenceRequired` | `evidence_required` |
| `mcp.ErrConflict` | `conflict` |
| `mcp.ErrDenied` | `denied` |
| `tool.ErrBudget`, or output over the budget | `budget_exceeded` |
| output outside the output schema | `invalid_output` |
| a `trigger.Classified` error whose class is not `configuration` and whose code is a stable identifier | its code |
| anything else, including a panic | `internal` |

### The catalog port

`trigger/` may not import `agent` or `agent/policy` (they reach the engine and
the journal), and an adapter may not import a store. `contract/approval`
carries the journal-backed decision store, so the adapter does not import it
either: it defines its own refusal sentinels (`ErrDenied`,
`ErrApprovalStale`, `ErrEvidenceRequired`, `ErrConflict`) and a `Catalog`
port with `List` and `Invoke`. A test fails if `trigger/mcp` links
`database/sql`, `store`, `agent`, `internal/journal`, `internal/engine` or
`contract/approval`.

The application implements `Catalog` over `agent.Catalog` or, when effects
need review, `policy.CatalogGate`, and maps their errors onto the sentinels.
It also chooses the invocation identity a reviewer approves. A decision
covers one proposal — action, tool digest, input digest, run, invocation path
and scope — and **carries no principal**, so binding it to the caller is the
application's job. The worked example in the tests derives the invocation
path from the principal, the tool and the input digest: a decision then
cannot be used by another principal or for another input, and an approved
call runs at most once (a replay is refused by the journal and surfaces as
`internal` unless the application classifies it).

### Cancellation and sessions

A client cancels a call with `notifications/cancelled`; the SDK cancels the
handler's context. The token info is built per HTTP request, so it carries
that request's context too, and the call is canceled when the request ends.
A client ends its session with DELETE. The SDK's DELETE handler closes the
session before it answers, and that close waits for in-flight handlers, so
canceling after the DELETE would wait for the work it was meant to stop. The
adapter cancels the session's calls **before** the transport handles the
DELETE, and only when the authenticated caller owns the session; anyone
else's DELETE cancels nothing and gets 403. An ended session is remembered
until it closes, so a call that was still queued is canceled as it starts.

Sessions are bounded: a request that would open more than `MaxSessions`
sessions, or more than `MaxSessionsPerPrincipal` for its principal, is
refused with 503 and `Retry-After`. A session is counted from the request
that opens it until it closes (DELETE, `SessionTimeout` or `Shutdown`); a
request that opens none frees its place before its response starts. The
opening finds its session on the very server the transport used for it, so
evicting views meanwhile cannot leave a session uncounted, and every counted
session is kept in a registry `Shutdown` closes.

### Lifecycle

Each request takes an application lease; a draining application answers 503
with `Retry-After`. Every request holds its lease until it is answered, and
each call also holds its own lease while it runs.
A drain timeout cancels a request's own work (its authentication and
building the session's view) and every call (#177); the request's response
itself is not canceled.
`Shutdown(ctx)` refuses new sessions and calls, waits for calls in flight
and for the request carrying each of them to end, then closes every session
(the registry, which holds every counted session even when its view was
evicted, and every cached view's) and waits for their bookkeeping; each step
returns `ctx.Err()` when ctx ends first. The SDK writes a call's answer on
its request after the tool handler returns, and a session being closed
refuses that write, so closing as soon as the calls end lost the answer to
work that was done (#197). A call admitted before `Shutdown` began therefore
has its answer written and flushed before its session closes.
The request's context ends when the adapter's `ServeHTTP` returns, whatever
server hosts it. A call refused because `Shutdown` has begun is not waited
for: it did nothing, and its refusal may not arrive.
Closing a session waits for its in-flight requests, and an opening settles
while writing its own first response, so an opening never closes its
session in place: one that settles before `Shutdown` takes its sessions is
closed by `Shutdown`, one that settles after is closed on its own goroutine,
which `Shutdown` does not wait for: that close waits only for the initialize
already being answered, every other request is refused with 503, and the
opening request's own lease keeps the application from stopping until it
finishes.
A call that is already canceled when it would reach the catalog (its
session ended or its request is gone) is refused without reaching it.

### Bounds

| Setting | Default | Limit |
|---|---|---|
| `Timeout` | 30 s | 5 min |
| `MaxConcurrency` | 32 | 1024 |
| `MaxCallsPerPrincipal` | min(8, `MaxConcurrency`) | `MaxConcurrency` |
| `MaxRequestBytes` | 256 KiB | 1 MiB (the schema payload limit) |
| `Budget.MaxOutputBytes` | required | 1 MiB |
| `MaxSessions` | 256 | 65536 |
| `MaxSessionsPerPrincipal` | min(16, `MaxSessions`) | `MaxSessions` |
| `SessionTimeout` | 10 min | — |
| `MaxPrincipals` | 1024 | 65536 |

### Dependency

The SDK and its transitive modules are linked only by binaries that import
`trigger/mcp`; the selection tests forbid them in the no-trigger, HTTP-only
and worker-only binaries.

| Module | Version | License |
|---|---|---|
| github.com/modelcontextprotocol/go-sdk | v1.8.0 | Apache-2.0, with not-yet-relicensed contributions under MIT (both texts in its LICENSE) |
| github.com/google/jsonschema-go | v0.4.3 | MIT |
| github.com/segmentio/encoding | v0.5.4 | MIT |
| github.com/segmentio/asm | v1.1.3 | MIT |
| github.com/yosida95/uritemplate/v3 | v3.0.2 | BSD-3-Clause |
| golang.org/x/oauth2 | v0.35.0 | BSD-3-Clause |
| golang.org/x/sync | v0.23.0 | BSD-3-Clause |
| golang.org/x/time | v0.15.0 | BSD-3-Clause |

Writing our own protocol layer was rejected: session management,
negotiation, cancellation and the SSE stream framing are the parts most
likely to be subtly wrong, and the official SDK tracks the specification.

## Consequences

- An application exposes a tool by naming it; nothing in the catalog becomes
  visible to a model by accident.
- An effect that needs review stays blocked until a decision recorded for
  that exact proposal exists; the evidence test drives the real decision
  store.
- Clients that only speak the draft 2026-07-28 protocol cannot connect until
  the adapter handles session-less calls with request-scoped cancellation.
- A client that loses its connection loses its call: there is no event store
  and no resumption.
- `Shutdown` delivers the answer to a call in flight, and a client reads it,
  whatever its retry policy. Were the adapter to offer a standalone GET
  stream, closing a session would end it, and the Go SDK client (v1.8.0)
  reads that end and the answer on separate connections in no fixed order:
  a client that treats the end of the stream as fatal at once (`MaxRetries`
  below zero) fails the connection, and the call with it, even with the
  answer already on the wire (73 of 100 calls in the first probe; 21 to 55
  of 100 across regression-test runs against a server that still served the
  stream).
  Upstream declined to change the client (modelcontextprotocol/go-sdk#1175).
  Refusing the GET with 405 leaves Shutdown no stream to end, so the flaw
  has nothing to act on for any client of this server, the SDK's or
  anyone else's; the
  regression test's client disables retries and receives every answer.
- A client sees its session closed by `Shutdown` only at its next request,
  which the closing adapter refuses with 503 (and a stopped listener does
  not accept): without a standalone stream nothing tells it sooner. The
  adapter never had anything else to send it there.
- Prompts, sampling, elicitation, subscriptions and resource templates are
  not offered.

## Evidence

`trigger/mcp` tests run the real adapter on a real listener against the
official client: the shared trigger conformance corpus, the predeclared
fixture `testdata/mcp/cases.json` (23 calls and 3 discovery views with
expected outcomes, catalog invocation counts and effect counts), approval
evidence on the real E14-T02 stack, a composed workflow, revocation, hijack,
protocol versions, deadline, client cancel, a dropped request, a bare DELETE,
a late-queued call, a canceled call before the catalog, overload, per-principal slots, session bounds, output
size, draining, a call outliving its request, shutdown, a call answered on the
wire and received when shutdown begins while it runs (100 trials, with the
answer held after the handler returns, by a client that disables retries),
the standalone GET refused with 405 and `Allow`, a session opened during
and after shutdown, evicted views, view
building, a panic while listing, the dependency rule and goroutine bounds.
Every response byte, and the server's error log, is checked for the
synthetic secret.
