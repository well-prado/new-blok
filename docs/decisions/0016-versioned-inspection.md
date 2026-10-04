# ADR 0016: Versioned run and step inspection projections

- Status: accepted for the application-composed Go and journal inspection slices
- Date: 2026-10-03
- Roadmap: E15-T01 (#76)

## Context

Notebook consumers need stable read-only run and step data without access to
mutable engine state. Authentication belongs to the application/adapter; the
engine must report trusted ownership supplied by that boundary. Payload fields
must be redacted, bounded, and authorized independently of execution input.

## Decision

The additive wire contract is `inspection/v1` in `contract/inspection`. Public
application composition selects an optional observer through `app.Config` and
the public `execution.NewRunner`; application-owned modules do not import the
internal interpreter. Ordinary `Engine.Run` does not JSON-serialize observation payloads.
Observed calls require a trusted run and principal, preserve supplied event
times, and have a unique execution-attempt identity; each step attempt has its
own identity and immutable input/output snapshots. The engine also captures
structured Go node logs only when inspection is selected. Diagnostic labels
are bounded and allowlisted; provider error messages are not projected.

`inspect.Recorder` is a process-local development projection. It authorizes by
exact principal, returns the same not-found result for unknown and unauthorized
IDs, supports versioned cursor pagination and field allowlists, redacts
sensitive JSON keys, and bounds payloads and responses. Hard defaults cap it at
1,024 runs, 100,000 accepted events, 64 MiB accounted retained event data,
4,096 steps per run, 100 attempts and 100 logs per step, and 64 KiB per event.
Saturation drops later observations, increments `Stats().DroppedEvents`, and
sets a visible `truncated` projection flag for retained runs. The recorder does
not evict history or promise restart retention. Page reads select the requested
page before projection. Each response projects only the five most recent
attempts and four most recent logs per step, with per-step truncation flags.
Payload fields have a 64-byte minimum projection envelope; an explicit smaller
`MaxPayloadBytes` is rejected, never raised. The effective per-payload ceiling
is also reduced as needed to fit the aggregate payload slots under the response
limit (256 KiB default, 1 MiB hard maximum). Oversized response envelopes fail
closed rather than violating the response ceiling.

`inspection.Source` is a narrow bounded read port. The journal implements it
without exposing the store implementation: principal authorization and SQL
pagination precede materialization, payload reads are capped to the computed
per-payload page budget, and attempts are SQL-limited before materialization.
Reads derive waiting/resumed status, uncertain operation status,
attempt IDs/history, and same-principal child lineage from actual journal rows.
Existing journal databases are migrated with an owner field; old ownerless
runs remain inaccessible through inspection. `InspectSource` applies the same
redaction and response projection. Blob retrieval is a separate opt-in path:
the caller must allow it, provide a principal-bound authenticated reader, and
set a hard maximum no greater than 1 MiB; digest, size, session, and principal
are checked before bytes are returned.

## Compatibility and limits

The contract is additive. Unsupported versions and malformed/cross-query
cursors fail closed. Returned byte slices are independent snapshots. The
journal source is an inspection view of the existing journal facts, not a new
general-purpose event store or promise that every workflow transition is
durable. Node.js SDK logging uses an explicit call-scoped logger and optional
protocol 1.1 log frames tagged with call, attempt, and generation; process-wide
stdout/stderr are never attributed to a run. Protocol 1.0 clients can use a
1.1 worker without logs. A 1.1 client rejects a 1.0 worker and requires its
upgrade. Logs have per-record and per-call bounds, use the same field selection
and attribute-key redaction as Go logs, and redact common credential-shaped
free-form text. Pattern matching cannot identify arbitrary secrets embedded in
ordinary prose, so applications must keep free-form log messages free of
credentials/customer payloads and put sensitive values only in structured
attributes. There is no concrete-trigger integration or universal API server
in this decision.

## Evidence

Executable scenarios run the real Go engine and real Node worker; compare an
actual engine projection with a checked-in golden response; exercise
two-principal isolation, redaction, immutable projections, pagination,
unsupported versions, recorder saturation, and bounded authenticated blob
retrieval; read real SQLite journal wait/resume, uncertain-attempt, and
child-lineage facts; and assert an actual Node worker log appears in an
authorized projection with its synthetic token attribute redacted. Node
integration requires `BLOK_NODE_INTEGRATION_ROOT` and the built runtime worker.
This evidence does not imply durable retention for recorder data.
