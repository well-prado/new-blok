# ADR 0019: Bounded structural migration from Blok v2 workflows

- Status: accepted for the E20-T01 implementation; parity completion pending
- Date: 2026-10-04
- Roadmap: E20-T01 ([#108](https://github.com/well-prado/new-blok/issues/108))

## Context

E20-T01 requires actual old/new application evidence and honest migration
limits. A converter that evaluates legacy mapper strings, drops trigger/job
options, or serializes a target document back into an apparently equivalent
source workflow would misstate behavior. Migration input and the node inventory
are caller-supplied and must also have explicit resource bounds.

## Decision

`migration.Convert` accepts only one bounded Blok schema-version-2 JSON
workflow and a caller-supplied inventory of immutable node descriptors. It
records a target document; it does not resolve packages, execute nodes, call a
provider, or make an old runtime's delivery guarantees durable in the new
runtime.

The accepted subset is deliberately structural:

- One workflow, at most one trigger kind, and at least one step.
- Step IDs are unique and satisfy the target identifier grammar.
- The target workflow ID is the normalized name plus a stable SHA-256 prefix,
  so distinct names that normalize to the same slug do not silently collide.
- Each `use` resolves to an exact inventory descriptor carrying its version,
  digest, input schema and output schema. Different descriptors may not
  collide on one target node ID.
- A call may omit `inputs`, or pass one whole-value structural `$ref` to
  `@trigger` or a completed earlier step. A structural path is preserved.
- Trigger configuration must be an empty object. Provider routes, auth,
  middleware, acknowledgments and delivery policy are configured and proven in
  application composition, not inferred by the converter.
- Source workflow/step fields, expressions, templates, null-vs-omitted input
  ambiguity, duplicate JSON object members, forward references, and unsupported
  trigger aliases/options fail with bounded actionable diagnostics. No
  expression is evaluated or converted to a literal.

`Export` is the inverse only for the exact subset emitted by `Convert`: calls
followed by one final output of the final call, at most one representable
binding, and at most one reference per call. It rejects additional bindings,
references, non-final/different output edges, source spans and other target-only
metadata instead of silently dropping them. Round-trip means the represented
workflow graph and pinned schemas survive; it does not restore provider
configuration or executable node source.

Limits: source JSON is at most 1 MiB, JSON nesting at most
`contract.MaxJSONDepth`, at most 131,072 JSON value tokens, 256 steps and 4,096
inventory entries. The inventory's total serialized identity/schema material
is bounded to 1 MiB. The encoded target document and exported source are each
bounded to `contract.MaxDocumentBytes` (1 MiB) before they are returned.

## Compatibility classification

This is an additive public Go API in the pre-M1 compatibility period, linked
to issue #108. It introduces no new workflow-document wire version and changes
no existing runtime behavior. Its behavioral compatibility promise is limited
to the subset above: accepted structural references and schema bytes are
preserved; unsupported input is rejected with remediation. `Export` does not
claim lossless source-code or configuration round-tripping. Changes to this
subset, identifiers, diagnostics or limits require an issue-linked decision
and updated valid/invalid fixtures under ADR 0001.

## Executable evidence

- `testdata/parity/migration-fixtures.json` contains accepted and rejected
  source documents, including structural refs, mapper/template input, nested
  unknown `$ref` metadata, duplicate keys, retry options and trigger config.
- The same fixture contains Export mutations for output-edge changes, extra
  call references and multiple bindings; each must fail with
  `unsupported_export_document`.
- `migration/blokv2_test.go` verifies structural round-trip, schema preservation,
  size/step/inventory bounds, identity collisions, forward refs and bounded
  diagnostics.
- Full #108 completion remains open until equivalent raw startup, idle, load
  and recovery distributions, independent Review R and the issue's remaining
  gates are recorded. A passing converter test is not application parity.
