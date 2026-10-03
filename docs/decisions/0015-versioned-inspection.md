# ADR 0015: Versioned run and step inspection projections

- Status: accepted for the process-local native execution slice
- Date: 2026-10-03
- Roadmap: E15-T01 (#76)

## Context

Notebook consumers need stable run and step data without access to mutable
engine state. Authentication belongs to the application/adapter; the engine
must report trusted execution ownership supplied by that boundary. Inspection
must not turn request payload fields into principals or expose secret values
by default.

## Decision

The additive wire contract is `inspection/v1` in
`contract/inspection`. The native interpreter emits immutable JSON snapshots
through an optional observer. `inspect.Recorder` builds process-local
projections and authorizes each read by exact owner-principal match. Unknown
and unauthorized run IDs have the same result. Field allowlists default to
omitting input, output and error detail. Permitted JSON payloads have sensitive
key values redacted and per-field and whole-response byte bounds. Step history
uses a run- and filter-bound cursor. Unknown API versions and malformed cursors
are rejected.

The contract distinguishes completed, failed, canceled, suspended and
uncertain statuses. The native interpreter produces real completed, failed,
canceled and uncertain events in this slice; no suspension transition exists
in the current interpreter. The recorder is not durable. It does not provide
logs, blob retrieval, Node.js worker workflow observation, or restart
retention. Those are not represented as passing evidence by this decision.

## Compatibility

This is additive to the engine's existing `Run` method. `RunObserved` uses the
same interpreter and requires trusted invocation metadata for recording. The
version string is explicit and unsupported versions fail closed. The recorder
does not alter committed node values; returned projections own their byte
slices.

## Evidence

Executable tests run the production Go engine with the quote node and assert
completed, failed, canceled and uncertain projections, two-principal
isolation, payload redaction/truncation, cursor pagination, version rejection
and returned-projection immutability. This is development inspection evidence,
not durable or production API evidence. Node worker inspection and suspended
run observation remain unverified.
