# ADR 0001: Public API boundaries and evidence policy

- Status: accepted for implementation planning
- Date: 2026-10-01
- Roadmap: E01-T02 (#22)
- Owners: framework maintainers; implementation ownership is recorded in
  [`contract-ownership.json`](contract-ownership.json)

## Context

New Blok is an application-owned Go framework. The design baseline describes
the intended API, but the repository currently contains only the CLI bootstrap.
This record makes the boundary and evidence rules explicit without presenting
planned packages or commands as available code.

The framework repository owns the engine, authoring contracts, CLI, package
client, runtime SDK/worker contracts and conformance suites. Studio, the hosted
registry service and BLOK Cloud are separate products. This repository owns
only the versioned contracts and consumer fixtures they need.

## Decisions

### Public package responsibilities

The public surface is intentionally small and responsibility-based:

| Responsibility | Planned public owner | Delivery owner | Current status |
| --- | --- | --- | --- |
| Go application developer CLI | `cmd/blok` | E11-T01–T04 | Bootstrap only (`help`, `version`) |
| Typed node definitions, descriptors and registration | `node` | E03-T01 | Planned |
| Structural workflow authoring and typed references | `flow` | E03-T02–T04 | Planned |
| Application composition and lifecycle | `app` | E06-T01 | Planned |
| Versioned workflow, binding, schema and descriptor documents | `contract` | E02-T01–T04 | Planned |
| Protocol-specific trigger bindings | `trigger/<kind>` | E06-T02, E09-T01–T08 | Planned |
| Selected embedded execution store port/implementation | `store` | E07-T01–T07 | Planned |
| Persistent worker protocol and integrations | `worker` | E08-T01–T04, E19-T01–T14 | Planned |
| Node/workflow package client and lock contracts | `package` | E13-T01–T04 | Planned |
| Optional telemetry and inspection contracts | `observe` / `inspect` | E15-T01–T03, E16-T01–T03 | Planned |
| In-process node/workflow test helpers | `testing` | E05-T04 | Planned |

These names are ownership labels, not a commitment to create empty packages.
An implementation issue may choose a more precise package name through the
change process below. Applications own their module and executable and import
only the public packages and selected adapters they need.

The following remain internal implementation details unless a later ADR makes
them public: compiler/lowering, interpreter/executor, journal internals,
admission queues, artifact hashing internals, discovery, generated-code
implementation, and provider-specific clients. The engine must not import
concrete triggers, stores, providers, frontend assets, ORM packages or AI
provider SDKs.

### Versioning and compatibility

Before M1, public Go packages are pre-release and may change only through an
ADR or an issue-linked compatibility decision. A contract change must include:

1. the old and new contract or API shape;
2. compatibility classification (additive, source-breaking, wire-breaking,
   behavioral, or removal);
3. migration guidance and, where needed, a versioned adapter or rejection
   diagnostic;
4. valid and invalid fixtures, including expected diagnostics and effect
   counts; and
5. focused tests plus the applicable race, protocol, crash, fuzz or load
   evidence required by the owning issue.

Wire, journal, artifact and worker contracts are versioned independently where
their compatibility guarantees differ. A version string never substitutes for
content identity. Immutable artifacts cannot be replaced under the same
version. Unknown versions and instructions fail closed with actionable
diagnostics.

### Initial schema boundary

#### Strictly earlier instruction references (#218)

The public document boundary rejects self, forward, missing and empty
instruction references with `invalid_reference` before parsing returns a
document, compilation returns a program, or canonical publication returns
bytes. This applies to every supported instruction kind. Valid references to
earlier instructions retain their field paths, and duplicate instruction IDs
retain the `duplicate_id` diagnostic.

This is a behavioral validation tightening linked to
[#218](https://github.com/well-prado/new-blok/issues/218), not a wire-shape or
document-version change. Previously the validator registered the current ID
before checking references and accidentally accepted self-references. Such
documents have no valid earlier result to read. Remove cyclic/forward edges
and use supported structural control flow; the framework never silently drops
or rewrites an invalid edge. The synthetic self-reference fixture and public
Validate/Parse/Compile/Canonical tests prove rejection without executing
business effects. This correction does not certify arbitrary control-flow
programs or change typed authoring APIs.

#### Field references select encoding/json keys (#241)

A reference path segment selects a member of the JSON object its value
encodes to, as `encoding/json` encodes it. The same path therefore resolves
the same way whether the value is still a Go struct a native node returned or
has crossed a JSON boundary (a checkpoint, a foreign runtime, a child
workflow) as decoded maps.

- **Old contract:** on a struct, a segment matched a field's json tag name
  *or* its exact Go field name. Embedded structs were not flattened (the
  embedded type's name returned the whole struct), `json:"-"` fields were
  reachable under their Go name and under `-`, `omitempty`/`omitzero` fields
  always existed, `,string` fields kept their native type, custom
  `MarshalJSON`/`MarshalText` output was ignored, and a map whose key type was
  not exactly `string` panicked the run.
- **New contract:** the key set is the one `encoding/json` emits for the
  value: promoted fields flattened under Go's embedding rules (shallowest
  wins, a tagged field breaks a tie, a remaining tie drops the key; a field
  promoted through a nil embedded pointer is absent), no `-` fields, a tagged
  field only under its tag name, empty `omitempty` and zero `omitzero` fields
  absent, `,string` scalars as their quoted text, values with their own
  encoder (`MarshalJSON`, `MarshalJSONTo`, `MarshalText` or `AppendText`, by
  value or pointer receiver) as that method writes them, map keys as their
  encoded text, and duplicate names resolved to the last as a JSON decoder
  would. A null value, including a nil map or slice, fails with
  `cannot read "<key>" from null`; a non-object fails with
  `cannot read "<key>" from a JSON <array|string|number|boolean>`;
  descending into a value `encoding/json` cannot encode (a func, a map with
  `bool` keys) fails with `value has no JSON encoding`.
- **Work is bounded by the selected member.** A struct resolves through a key
  index built once per type; siblings are never encoded. Each tag's key name
  is delegated to `encoding/json` through a one-field struct carrying the same
  json tag, because unusual names are not fixed: Go 1.27's default (v2-backed)
  `encoding/json` encodes a field tagged `json:"it's"` under `"it"` and a
  `string`-kind map key with `MarshalText` under its text, while its v1
  implementation (`GOEXPERIMENT=nojsonv2`) falls back to the Go field name
  and uses the raw string. The supported and tested implementation is Go
  1.27's default; the v1 mode is untested, because this repository does not
  build in it (`internal/engine/observation.go` imports `encoding/json/v2`).
  `omitempty` and `omitzero` are decided from the
  selected field alone, with `encoding/json`'s definitions (v1's emptiness,
  which the v2-backed implementation keeps for `encoding/json`, and a type's
  own `IsZero`). The only member ever encoded is a `,string` scalar. Map keys are
  named without encoding values. A container whose own type has one of the
  four encoders is the one case encoded whole, because its keys exist
  nowhere else. Of the tag options Go 1.27's `encoding/json` parses, `embed`
  (an unnamed `embed` field is inlined) and `format:…` (encoding fails) are
  not modelled, so a struct using either also resolves against its whole
  encoding. Every other option, including `case:…` (decoding only), unknown
  options such as `required`, and look-alikes such as `omitEmpty`, is ignored
  by `encoding/json` and here.
- **Typed values stay typed.** A selected field is handed on as its Go value,
  so native nodes receive their declared input types whether the source node
  returned a `T` or a `*T`. Only a `,string` member, and members of a
  container with its own encoder, are handed on decoded. Consequence: a
  field whose type marshals through a pointer receiver, selected from inside
  a pointer output, is handed on as its Go value, whose by-value encoding
  differs from the member `encoding/json` wrote in place. Navigating *through*
  such a field still follows the in-place encoding.
- **Values JSON cannot carry.** NaN and ±Inf floats, `map[bool]…` and funcs
  have no JSON form, so no decoded value exists to compare with. They do not
  affect references to their siblings, and selecting such a field returns its
  Go value as before.

**Compatibility: behavioral.** No wire, document or journal format changes.
A hand-written reference that relied on the old struct-only behavior now
fails with `invalid_output_reference` / `invalid_input_reference`: a Go field
name where a json tag renames the field, a `json:"-"` field, an embedded type's
name, a field of a type with its own encoder, or an empty `omitempty` / zero
`omitzero` field. A reference through a `,string` field now yields its quoted
text. Migration: select the key the value has in its JSON (`blok generate`
accessors do, #240; they now also skip `,string` fields and types with any
of the four encoders, and emit one accessor per key, for the field
`encoding/json` writes); read promoted fields at the parent level; move data the
workflow must read out of `json:"-"`. No reference in this repository's
examples, fixtures or scaffolds needed migration.

Cost: selecting a three-byte field beside a 1 MiB sibling allocates 16 bytes
at one segment and 32 bytes at five (the
`TestReferenceWorkIsBoundedBySelectedMember` regression bound is 4 KiB),
against 64 and 272 bytes on `main` before #241; the quote example's
end-to-end run shows no measurable difference on the measuring machine.

Fixtures: `testdata/references/json-keys.json` holds valid and rejected cases.
`internal/engine/reference_test.go` resolves each against a typed node output
and against its `encoding/json` round trip through the production
interpreter, requires both to match the fixture, and asserts the Go types
handed on, including for `T` versus `*T` outputs and for members selected by
the shadowing, tag-dominance and duplicate-embedding rules.
`internal/engine/reference_property_test.go` compares every candidate key of
1,500 seeded random `reflect.StructOf` shapes, as `T` and `*T`, against
`json.Marshal` → `json.Unmarshal`. Limits: a
generated accessor for an `omitempty`/`omitzero` field is typed as always
present, but its reference fails when the value is empty; the generator still
emits no accessors for promoted fields; inspection capture (`observation.go`)
keeps its own bounded walker, which truncates values with embedded,
`,string` or `omitzero` fields instead of resolving them.

The initial portable contract is a bounded, JSON-compatible value subset with
explicit semantics for missing, null, optional fields, objects, arrays, string,
boolean, signed integer, exact decimal/money, timestamp, bytes/blob reference
and declared unions. Defaults are annotations until the normalization operation
explicitly applies them. Unknown fields, depth, payload size and collection
limits are policy-controlled. Unsupported schemas are rejected or require a
runtime validation diagnostic; they are never silently coerced.

E02 owns the executable schema and document semantics. This ADR records the
boundary and decision ownership, not an implementation claim.

### Separate product repositories

| Product | Separate implementation repository | This repository may contain |
| --- | --- | --- |
| Studio | Studio repository | Authenticated/versioned inspection APIs and consumer fixtures |
| Hosted registry | Registry repository | Package identity, trust, client and protocol contracts |
| BLOK Cloud | Cloud repository | Deployment, readiness, artifact and operations contracts |

Frontend assets, hosting control planes, billing, moderation and product
permissions do not belong in the framework repository.

### Evidence policy

The architecture lab is prototype evidence only. Equal-step source-shape
comparisons cannot satisfy current-Blok behavioral parity. Deterministic
three-goal authoring evaluation cannot satisfy live-model reliability. Lab
results also cannot establish production performance, application parity,
security, artifact retention, distributed failover or complete schema
enforcement. Those claims require the roadmap's executable gates and raw,
reproducible evidence.

In particular, the fleet target of millions of requests per second is not a
current capability claim. Any future capacity claim must disclose workload,
topology, guarantees, versions, warmup, repeated distributions, saturation,
failures and raw results.

## Change process

An author proposing a public or cross-boundary change first records the
affected contract, owner and compatibility class in a decision record. The
owning roadmap issue must be linked. The author then adds or updates valid and
invalid synthetic fixtures and runs the issue's required evidence. A reviewer
independent of the implementation owner checks the contract, compatibility,
security/durability/performance implications and fixture coverage for `R`
issues. Merge does not imply that a later milestone or product is complete.

The machine-readable fixtures in this directory are the inventory for ambiguous
ownership and evidence claims. They deliberately include rejected examples;
file presence or a green unrelated test is not evidence of support.
