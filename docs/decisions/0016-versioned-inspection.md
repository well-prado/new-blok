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
Durable terminal projection is separately opt-in through the application-owned
`RunOutcomePort`: `execution.Runner` records completed, failed, canceled, or
explicitly uncertain outcomes after the real engine returns. When this port is
configured, run-terminal observation is deferred until the durable outcome
attempt resolves; a failed terminal write is projected uncertain, never briefly
completed. The journal
provides a principal-checking adapter for that port without being imported by
the engine. The trusted trigger/application boundary supplies the run ID and
principal together; the adapter verifies their ownership match before any
terminal mutation. If the application does not configure the port, the runner
does not claim durable terminal state.
Observed calls require a trusted run and principal, preserve supplied event
times, and have a unique execution-attempt identity; each step attempt has its
own identity and immutable input/output snapshots. The engine also captures
structured Go node logs only when inspection is selected. Diagnostic labels
are bounded and allowlisted; provider error messages are not projected.
Observer payload capture is also bounded before serialization: each input or
output is limited to 32 KiB, 8,192 visited values, and 32 nested levels. Larger
or unsupported values become `{"$truncated":true}`. Capture structurally copies
JSON-compatible fields and does not invoke caller-defined `MarshalJSON` methods.
Custom JSON/text marshalers, named-byte slices, anonymous embedded fields, and `string`/`omitzero` JSON tags
are explicitly truncated rather than approximated. Ordinary nil slices retain
JSON null and `omitempty` retains standard empty-collection semantics. These
capture limits affect diagnostic snapshots only, never business values.
Go 1.27 custom `MarshalJSONTo` and `AppendText` representations and unsupported
quoted/invalid JSON field tags likewise truncate without invoking application
methods. Map keys with custom representations also truncate rather than
exposing their underlying value.
Duplicate projected struct field names truncate before `omitempty` selection;
capture never exposes a value that standard JSON would discard as ambiguous.
Invalid standard-library time representations also truncate.
Raw JSON also uses a bounded token scan for depth and visited items before
retention; its serialized form does not bypass the structural capture limits.

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
Encoded cursors are limited to 1,024 bytes before base64 decoding.

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

New durable step-scope and effect-intent inputs are capped at 64 KiB each,
checked before JSON validation and before any SQL write. Attempt input is copied
from its already-bounded immutable effect intent, not accepted independently.
The additive migration leaves historical scope/operation/attempt input columns
NULL; recovery never fills those unknown values from a later invocation. Run
admission input remains its pre-existing journal contract and is not silently
reclassified by this inspection-input limit. A terminal failed-run fact is an
explicit `FailRun` transition from the execution owner, not an inference from
`FailAttempt`; it is rejected while operations, waits, or scopes remain active,
and uncertain effects remain uncertain rather than being converted to failure.
New successful-run output writes are capped at 1 MiB before JSON validation or
SQL persistence.
The runner binds execution to its application lease, so shutdown abort reaches
the node before dependencies close. Terminal writes use a bounded app-abort
context detached from request cancellation so the outcome can commit before
the lease is released. If terminal persistence fails after execution, the
runner returns an explicit uncertain result with the run ID for reconciliation;
it never presents the completed effect as a safely retryable generic failure.

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

A worker log is delivered off the call's result path (`Call.OnLog`), so it
may reach inspection after its step, and its run, have completed. That can
happen even without queue pressure: the result can simply win the race. A
log the runtime received before its call's result is still attributed to its
step and appears in later projections, with the time it was delivered rather
than the time it was emitted. A log frame received after the result, or one
that arrives while the log queue is full, is dropped as before. A reader
that needs a step's logs therefore reads after its completion rather than at
it (#226). A consumer that stops at a run's terminal event,
such as a live stream (#77), can miss such logs unless it keeps reading for
a bound after that event.

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
