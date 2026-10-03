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
session reused by another caller, a cancel that leaves the work running, an
error string that carries a connection string to the model, and a flood of
calls that queues without bound.

## Decision

`trigger/mcp` declares memory / cancel / caller: a call completes in band, a
client that cancels it or ends its session cancels its work, and every
request is authenticated.

### Transport and protocol

The adapter serves Streamable HTTP through the official MCP Go SDK,
`github.com/modelcontextprotocol/go-sdk` v1.8.0, in stateful mode. A client
negotiates one of 2025-11-25, 2025-06-18, 2025-03-26 or 2024-11-05. A request
whose `Mcp-Protocol-Version` header names an unsupported version is refused
with 400. The draft 2026-07-28 protocol, whose calls carry no session, is
refused (`-32022`); a session-less call would escape the session that binds a
caller and ends its work.

### Authentication and sessions

Every HTTP request carries a bearer token. The application's `Authenticator`
resolves it to a `tool.Principal` (id, capabilities, maximum depth); failure
is 401 and the token is never echoed. The token's user id is the principal
id, so the SDK binds a session to the principal that opened it and answers
403 when anyone else uses its session id, including on DELETE.

A call runs as the principal **its own request** authenticated, not the one
that opened the session. Capabilities revoked while a session stays open
apply to the next call.

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

The view of one principal and capability set is built once and cached, up to
`MaxPrincipals` views (oldest evicted; its sessions keep their server until
they end or `Shutdown` closes them).

### A call, in order

1. Server closing → tool error `unavailable`.
2. A concurrency slot (`MaxConcurrency`); none free → tool error `saturated`
   at once, never a queue.
3. The arguments are normalized against the input schema; failure is the
   protocol error `-32602 invalid_input`, before the catalog sees the call.
4. `_meta["newblok.dev/approval"]`, if present, must be 1–256 printable ASCII
   characters (`-32602 invalid_approval` otherwise). It is passed on as a
   **name**. Naming a decision grants nothing: the catalog looks it up.
5. A deadline from `Timeout` (≤ 5 min), also set as the budget's deadline.
6. `Catalog.Invoke` with the current principal. A panic in it is a tool
   error `internal`.
7. A catalog that returns after the call's deadline or cancel has failed the
   call (`deadline_exceeded` / `canceled`), as in the gRPC adapter. An effect
   it committed is recorded by the journal, not by the reply.
8. Output is normalized against the output schema; failure is tool error
   `invalid_output`, and the output is never returned.

An unknown, hidden or unexposed tool is the SDK's protocol error `-32602`.

### Errors

A failing call is a tool error result (`isError: true`) whose only content is
`{"code": "<stable code>"}`. The model sees the code, never the error text:

| Cause | Code |
|---|---|
| deadline | `deadline_exceeded` |
| client cancel or session end | `canceled` |
| `trigger.ErrSaturated`, `approval.ErrCapacity` | `saturated` |
| `approval.ErrStale` (missing, rejected, expired or mismatched decision) | `approval_stale` |
| `approval.ErrEvidence` | `evidence_required` |
| `approval.ErrConflict` | `conflict` |
| `approval.ErrDenied` | `denied` |
| `tool.ErrBudget` | `budget_exceeded` |
| a `trigger.Classified` error of any class but `configuration` | its code |
| anything else, including a panic | `internal` |

### The catalog port

`trigger/` may not import `agent` or `agent/policy` (they reach the engine and
the journal). The adapter therefore defines `Catalog` with `List` and
`Invoke`, and the application implements it over `agent.Catalog` or, when
effects need review, `policy.CatalogGate`. The application maps an MCP call
to the run and invocation identity a reviewer prepares a decision for, and
maps `agent.ErrDenied` / `ErrNotAgentSafe` / `ErrCapacity` to the approval
sentinels above. The tests carry both adapters as worked examples.

### Cancellation

A client cancels a call with `notifications/cancelled`; the SDK cancels the
handler's context. A client also ends its session with DELETE. The SDK's
DELETE handler closes the session before it answers, and that close waits for
in-flight handlers, so canceling after the DELETE would wait for the work it
was meant to stop. The adapter therefore cancels the session's calls
**before** the transport handles the DELETE, and only when the authenticated
caller owns the session. Anyone else's DELETE cancels nothing and gets 403.

### Lifecycle

Each request takes an application lease; a draining application answers 503
with `Retry-After`. A GET is the session's standing stream, so it is admitted
but releases its lease at once; POSTs hold theirs while the call runs.
`Shutdown(ctx)` refuses new calls, waits for calls in flight (returning
`ctx.Err()` if they outlast it) and then closes every session, including
those of evicted views.

### Bounds

| Setting | Default | Limit |
|---|---|---|
| `Timeout` | 30 s | 5 min |
| `MaxConcurrency` | 32 | 1024 |
| `MaxRequestBytes` | 256 KiB | 1 MiB (the schema payload limit) |
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
  that exact call exists; the evidence test drives the real decision store.
- Clients that only speak the draft 2026-07-28 protocol cannot connect until
  the adapter handles session-less calls with request-scoped cancellation.
- The invocation identity a reviewer approves is the application's choice;
  the adapter does not mint one. A replayed approved call is refused by the
  journal, and surfaces as `internal` unless the application classifies it.
- Prompts, sampling, elicitation, subscriptions and resource templates are
  not offered.

## Evidence

`trigger/mcp` tests run the real adapter on a real listener against the
official client: the shared trigger conformance corpus, the predeclared
fixture `testdata/mcp/cases.json` (23 calls and 3 discovery views with
expected outcomes, catalog invocation counts and effect counts), approval
evidence on the real E14-T02 stack, a composed workflow, revocation, hijack,
protocol versions, deadline, client cancel, a bare DELETE, overload, draining,
shutdown, evicted views and goroutine bounds. Every response byte is
recorded and checked for the synthetic secret.
