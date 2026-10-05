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
  index built once per type; siblings are never encoded. The index reads json
  tags only in *plain form* (`internal/jsontag`): a valid UTF-8 name part
  without quotes, backslashes or backticks, which `encoding/json` uses
  verbatim as the key, and options that are each empty, a whole identifier,
  `case:<identifier>` or `format:<value>`. Outside plain form, Go 1.27's
  `encoding/json` decides the effect by its own parsing: it keeps the leading
  identifier of a malformed option and still applies it (`,omitempty ` and
  `,omitempty;` still omit, `,string ` still quotes), skips an option that
  does not start with a letter (`, omitempty`), and reads quoted parts its own
  way (`json:"'a,string'"` keeps the Go name and quotes). A struct with any
  tag outside plain form therefore resolves against its whole encoding, as
  does one using `embed` (an unnamed `embed` field is inlined) or `format:…`
  (encoding fails). Within plain form, `omitempty`, `omitzero` and `string`
  are modelled; every other identifier, including `case:…` (decoding only),
  unknown options such as `required` and look-alikes such as `omitEmpty`, is
  ignored by `encoding/json` and here. `omitempty` and `omitzero` are decided
  from the selected field alone, with `encoding/json`'s definitions (v1's
  emptiness, which the v2-backed implementation keeps for `encoding/json`,
  and a type's own `IsZero`). The only member ever encoded is a `,string`
  scalar. Map keys are named without encoding values. A container whose own
  type has one of the four encoders is encoded whole, because its keys exist
  nowhere else. The supported and tested implementation is Go 1.27's default
  (v2-backed) `encoding/json`. Its v1 implementation (`GOEXPERIMENT=nojsonv2`)
  names some keys differently (for example a `string`-kind map key with
  `MarshalText`) and is untested: this repository does not build in that
  mode (`internal/engine/observation.go` imports `encoding/json/v2`).
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
accessors do, #240; they now also skip `,string` fields, types with any of
the four encoders and types with a tag outside plain form, and emit at most
one accessor per key, for the field `encoding/json` writes, counting tagged
embedded fields in the contest); read promoted fields at the parent level; move data the
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

#### Lowered call inputs keep their references (#244)

`flow.Definition.Lower` lowers each call's recorded input to the same
structural reference the canonical document compiler produces for that edge:
the workflow input (`$input`) carries no reference, and an earlier call's
result or field (`$step.<id>[.<field>…]`, including generated accessors)
becomes one reference with that path. Lower previously dropped every call
input reference, so the engine handed every call the workflow input.

This is a behavioral correction linked to
[#244](https://github.com/well-prado/new-blok/issues/244), not a wire-shape or
document-version change. Inputs with no program form now fail `Lower` with an
error naming the call instead of silently running on the workflow input: a
`flow.Lit` value, a field of the workflow input, a reference whose step id
is not a strictly earlier call of the same program (forward, self, or a step
id only another definition has), and an empty field segment. The output
reference follows the same earlier-call rule. References are matched by step
id, not by the builder that made them: a reference leaked from another
definition whose step id happens to equal an earlier local call's lowers to
that local call, and a leaked workflow-input reference lowers as this
workflow's input. Neither is detected. Control constructs (`If`, `Choose`, `Each`,
`Parallel`, `TryFinally`, `Child`, `Compare`, `Default`, `Template`) are still
rejected as a whole; their arm calls never lower on their own. Migration: a
workflow that lowered a literal or workflow-input field as a call input
already ran that call on the whole workflow input, so it must route the value
through a call result or wait for a program literal form.
`flow/lower_conformance_test.go` compares each lowered program with
`internal/compile` and checks every call's delivered input through
`execution.Runner`. Lower does not yet check selected fields against node
output schemas; the engine still validates each call input against the node's
input schema before invoking it.

#### The lowered output instruction id is reserved in flow (#247)

`flow.Definition.Lower` appends one instruction of kind `output` with the id
`flow.OutputID` (`"output"`) to return the workflow's result. Every
id-taking builder (`Call`, `ArmCall`, `If`, `Choose`, `Each`, `Parallel`,
`TryFinally`, `Child`, `Compare`, `Default`, `Template`) shares one id
namespace and now panics with
`flow: instruction id "output" is reserved for the workflow output instruction Lower appends; rename the step`
when given that id, the same authoring-time failure a duplicate id already
produces. Previously `flow.Call(builder, "output", …)` lowered to a program
with two instructions named `output`; the engine reported two steps with that
id, and the same structure written as a document fails `duplicate_id`.

The id is reserved, rather than renamed to something no step can take, because
it keeps one id grammar across flow, the canonical compiler and documents:
document ids match `^[a-z][a-z0-9_-]{0,63}$`, flow ids were not grammar-checked
(they are since #251, below),
and the existing `output` id is what lowered programs, engine step results and
inspection events already carry. The rule lives in flow because only flow
synthesizes an instruction. The canonical compiler and the document validator
synthesize nothing: a document names its own output instruction (the
`valid.json` fixture calls it `respond`; migration picks `result` or `return`),
so a document call named `output` is valid, and a document that repeats any id
keeps failing `duplicate_id`. Documents therefore do not reserve the id. The
agent catalog's workflow lowering already refused a call named `output` and
now names the same `flow.OutputID` constant; its other divergences from
`flow.Lower` were #249 (below).

This is a behavioral validation tightening plus one additive exported constant
(`flow.OutputID`), linked to
[#247](https://github.com/well-prado/new-blok/issues/247). It is not a
wire-shape or document-version change, and the lowered output id is unchanged.
Only definitions using the id `output` for an authored step are affected.
Those made of plain calls already lowered to a program with duplicate ids;
a control construct named `output`, or a definition used only through
`Program()` or `agent.RegisterWorkflow`, used to fail in `Lower` or be
refused by the catalog, and now panics when the flow is defined instead.
Migration: rename the step.
`flow/reserved_output_test.go` proves rejection for every id-taking builder and
that resembling ids (`outputs`, `output-step`, `result`) still lower to the
canonical compiler's program; `internal/compile` and `contract` tests prove a
document call named `output` compiles and a repeated id is rejected.

#### Flow step ids follow the document id grammar (#251)

Every id-taking builder (`Call`, `ArmCall`, `If`, `Choose`, `Each`,
`Parallel`, `TryFinally`, `Child`, `Compare`, `Default`, `Template`) checks its
step id in one place, `Builder.reserveID`, against the document id grammar
`^[a-z][a-z0-9_-]{0,63}$`. A violation fails as
`flow: instruction id "<id>" does not match the id grammar ^[a-z][a-z0-9_-]{0,63}$; rename the step`,
the same authoring-time failure as the reserved `output` id and a duplicate
id. The grammar is not copied: `contract` exports it as `contract.IDPattern`
with `contract.ValidID`, and document validation now uses that function too.

Previously flow accepted any id except `output` (#247) and an empty `Call`
or `ArmCall` id. The
`.` is the defect that matters: a reference to step `a.b` is recorded as
`$step.a.b`, and lowering splits references on `.`, so with calls `a` and
`a.b`, returning `a.b` lowered to `{Step: a, Path: [b]}` — field `b` of step
`a` — with no error. Other out-of-grammar ids (uppercase, a leading digit,
`-` or `_`, `/`, `$`, `:`, spaces, non-ASCII letters, more than 64
characters, an empty id on a construct) had no document form, so the same
structure written as a document fails `invalid_id`. One grammar now holds in
flow, the canonical compiler and documents. The agent catalog's workflow
lowering reads only programs built by `flow.Define`, so its step ids now obey
the grammar as well; its other divergences from `flow.Lower` were #249
(below).

`flow.Define` now returns every builder violation as its error instead of
letting it escape as a panic: the id rules above, and the construct rules
(an `Each` concurrency outside 1–1024, a missing arm, an empty field path,
an empty `Child` workflow name or comparison operator, a literal that cannot
be encoded, a raw `js/` template). Tooling that loads definitions — the
scaffold's generated `New`, the parity harness — gets a diagnostic it can
report instead of a crash, which is what `Define` returning an error always
promised. Builders panic with an unexported error type and `Define` recovers
only that type, so a panic the application's own build callback raises (a
nil dereference, a deliberate `panic`) still propagates unchanged and is
never reported as a definition error. `MustDefine` does not recover: a
broken rule panics at the builder call that broke it, so the crash stack
still names the offending line. A builder called outside `Define` (a
`*Builder` kept after the callback returns) still panics.

Compatibility: a behavioral validation tightening in `flow`, a behavioral
change to `flow.Define`, and an additive `contract` export (`IDPattern`,
`ValidID`), linked to [#251](https://github.com/well-prado/new-blok/issues/251).
It is not a wire-shape or document-version change, and document validation
is unchanged. Affected definitions:

- a step id outside the grammar: `MustDefine` now panics and `Define` now
  returns an error where both used to succeed. Migration: rename the step to
  lowercase letters, digits, `_` or `-`, starting with a letter, at most 64
  characters (`lineItems` becomes `line-items` or `line_items`). No id in
  this repository's examples, scaffold, benchmarks, catalog or agent code
  needed renaming.
- `Define` given a callback that breaks any builder rule now returns
  `(Definition{}, err)` instead of panicking; callers that recovered that
  panic should check the error instead.
- the panic value raised by builders and by `MustDefine` is now an `error`
  whose message is unchanged; code asserting it is a `string` must use
  `fmt.Sprint` or `error` instead.
- the empty-id panic of `Call` and `ArmCall` changed from
  `flow: call id is required` to the grammar message for `""`.

`flow/id_grammar_test.go` proves rejection of fourteen grammar-invalid ids
for every id-taking builder through both `Define` (error) and `MustDefine`
(panic), that ids at the grammar's edges (`a`, a 64-character id) still lower
to the canonical compiler's program, that `Define` returns construct
violations, that a callback's own panic propagates, and that `MustDefine`'s
panic stack reaches the builder call. `contract/document_test.go` pins
`ValidID` to document validation's `invalid_id`. Limits: flow still does not
grammar-check `Choose` case keys, the `Child` workflow name or `Spec.Name`
(document workflow ids are a separate field that a flow `Spec.Name` such as
`shop/quote` does not map to). `migration/blokv2.go` kept its own copy of the
grammar until #256 (below).

#### Migration uses the shared id grammar (#256)

`migration/blokv2.go` carried a private `validID` regular expression, so a
change to `contract.IDPattern` (or to the copy) would have let migration
accept ids that documents reject, or the reverse, with no test failing. The
copy is deleted; `Convert` checks source step ids (`unsupported_step_id`) and
inventory node ids (`invalid_target_node_id`) with `contract.ValidID`.

Compatibility: none. The deleted expression was byte-for-byte
`^[a-z][a-z0-9_-]{0,63}$`, the same text as `contract.IDPattern`, and has not
changed since it was added (`ef67bcb`); the anchors, length bound and
character set are identical, so migration accepts and rejects exactly the
ids it did before, with the same diagnostics. Go's `$` (no `(?m)` flag)
matches only at the end of the text, so neither expression accepts `"a\n"`.
This is an internal refactor, not a wire-shape, diagnostic or document-version
change. `migration/blokv2_test.go` proves it with a shared table of edge ids
(1, 64 and 65 characters, dotted, uppercase, leading digit, leading `_` and
`-`, empty, non-ASCII, trailing newline) driven through `Convert`: migration
must agree with `contract.ValidID` on every row, for step ids and inventory
ids, and an accepted id must also pass `Document.Validate`. Mutating the old
copy to `{0,64}` or `{0,62}` turns the test red. An empty inventory id is not
an error: migration derives one from the `use` key's slug.

#### Agent catalog workflows lower through flow's lowering (#249)

`agent.RegisterWorkflow` used to lower a composed workflow with its own copy
of `flow.Lower`'s rules, and the copy had drifted. It accepted an empty field
segment (`$step.reserve.body..sku`), so the workflow registered and then
failed at invocation after its earlier calls had already run their effects;
it gave a whole-value reference an empty non-nil path where `flow.Lower`
gives `nil`; it left the output instruction at index 0; and every rejection
was a bare `ErrNotAgentSafe` with no reason.

There is now one lowering, `internal/lowering.Lower`. `flow.Definition.Lower`
calls it with zero `Options`; `RegisterWorkflow` calls it with the catalog's
two extensions, each an explicit option:

- `Children` lowers a `flow.Child` instruction as a call of the registered
  child workflow tool, under the same reference rules: `$child.<id>` names an
  earlier child and `$step.<id>` an earlier call.
- `Literals` lets a call take a `flow.Lit` input. The call lowers with no
  references, and the literal is returned beside the program, keyed by call
  id, for the catalog's dispatch to substitute, exactly as before.

The ids (grammar, the reserved `flow.OutputID`, duplicates) are checked
again by the lowering, so a program it returns never holds an id the
canonical compiler would reject; builders reject them first. The catalog
stores exactly the program `flow.Lower` produces for the same workflow, and
finds each step's tool by step id instead of by the program's node name.

**Decision: no program literal form, and `flow.Lower` keeps rejecting
literals.** A literal in `contract.InternalInstruction` would change the
versioned program format `internal/program` defines (it digests every
instruction and decodes with unknown fields disallowed), has no document form
for the canonical compiler to match, and would need the engine's input
resolution and inspection to carry it. None of that is needed to keep the
catalog's literal support, which only needs the value at dispatch. `flow.Lower` sets neither
option, so a developer-authored flow does not start lowering literals or
child calls; the #244 message for a literal is unchanged. A program literal
form, if a consumer needs one, is a separate wire decision.

Compatibility: a behavioral validation tightening and a diagnostic change in
`agent.RegisterWorkflow`, linked to
[#249](https://github.com/well-prado/new-blok/issues/249). No public API,
wire shape, document version or artifact digest changes (the catalog digest
still hashes `flow.Program`), and `flow.Lower`'s results and errors are
unchanged. Affected registrations:

- a call input or output with an empty field segment now fails registration
  instead of failing at invocation after earlier effects. Migration: fix the
  `flow.Select` path.
- a reference whose prefix names the other kind (`$step.<child id>`,
  `$child.<call id>`) now fails. Builders never record one; only a reference
  leaked from another definition can.
- a rejection still matches `errors.Is(err, agent.ErrNotAgentSafe)`, and its
  message now carries the reason, for example
  `agent: tool is not agent-safe: flow: call "commit": input "$step.reserve.body..sku" has an empty field`.

`agent/lower_conformance_test.go` runs the same workflow through the flow
builder and `flow.Lower` and through `RegisterWorkflow`, and requires
identical programs and identical node inputs and outputs, the same rejection
with the same reason for every input and control construct (`If`, `Choose`,
`Each`, `Parallel`, `TryFinally`, `Compare`, `Default`, `Template`) that
`flow.Lower` rejects, and the literal and child extensions to differ from
`flow.Lower` only by that extension. `internal/lowering/lowering_test.go`
covers the option-gated rules directly. Limits: `flow` and `agent` each copy
`flow.Program` into the lowering's instruction form (a few lines each; the
lowering cannot import `flow`, which imports it), guarded by the conformance
tests; the catalog still composes only calls and children, so a control
construct remains unsupported for agent tools; and a literal remains
invisible in the stored program, so the engine sees that call as taking the
workflow input until dispatch substitutes it; #260 (below) makes that
program unrunnable outside dispatch.

#### Catalog literals are checked against the tool's input schema (#261)

After #249 a literal call input was still only checked when dispatch reached
its call: `RegisterWorkflow` stored it without looking at it, so a literal
the tool's input schema refuses registered, and every invocation ran the
earlier calls' effects before failing on it. `RegisterWorkflow` now runs on
each literal, at registration, the same admission dispatch runs on it: the
1 MiB input bound, then the receiving tool's own `schema.Schema.Normalize`
(the binding's `in`, the one dispatch uses), then the bound again on the
normalized value. The two cannot diverge because they call the same
function on the same schema value.

**Decision: validate, do not rewrite.** `Normalize` rewrites values — it
applies defaults, puts `int64-string` integers in wire form, and re-encodes
keys in order. The catalog keeps the literal exactly as `flow` recorded it and
discards the normalized copy; dispatch normalizes it as before. The node and
the approval gate therefore receive byte-for-byte what they received before
#261, and the artifact digest (which hashes `flow.Program`) is unchanged.

Compatibility: a behavioral validation tightening and a diagnostic addition
in `agent.RegisterWorkflow`, linked to
[#261](https://github.com/well-prado/new-blok/issues/261). No public API,
wire shape, document version, artifact digest or dispatch input changes.
Affected registrations — each one could only ever fail at invocation, after
its earlier calls' effects:

- a literal the tool's input schema refuses (missing required field, wrong
  type, unknown field, null, range, format, union) now fails registration
  with `errors.Is(err, agent.ErrNotAgentSafe)`, naming the step and tool, for
  example `agent: tool is not agent-safe: call "commit": literal input does
  not satisfy the input schema of conf/commit@1.0.0: missing_required at
  $.quantity: required field is absent`. Migration: fix the `flow.Lit` value.
- a literal over 1 MiB, before or after normalization, now fails
  registration with `errors.Is(err, agent.ErrBudget)`, naming the step. It
  was already unadmittable: `tool.Budget` caps `MaxInputBytes` at 1 MiB, so
  dispatch refused it with `ErrBudget` on every invocation. The error class
  is kept; only the moment moves.
- a literal within 1 MiB but over one invocation's `MaxInputBytes` still
  registers and still trips `ErrBudget` at that invocation, as before: the
  budget is per invocation, so registration cannot know it.

A child workflow's literals are checked when that child registers, so a
parent can only compose a child whose literals passed; `flow.Child` records
no literal, so a child call cannot take one (the lowering refuses it). The
check covers any literal the lowering returns, whatever the instruction kind.
`agent/literal_validation_test.go` holds the cases; each rejection case is
red on the pre-#261 catalog, which registers the workflow and then, on
invocation, runs the reserve effect before failing.

#### A catalog program runs only through catalog dispatch (#260)

Because the program has no literal form, the stored catalog program is only
correct while dispatch substitutes the literals. The #257 review showed what
happens otherwise: it ran the stored program through `engine.RunObserved`
outside dispatch, the `StepProcessing` event for the literal call recorded
the workflow input (`{"quantity":2,"sku":"SECRET"}`), and the node ran with
it instead of the literal. Nothing observed or journaled catalog runs, so the
hazard was latent, but it was held off only by convention.

**Rule: never run, observe or journal a catalog program except through
catalog dispatch.** It is now enforced by the package structure, not by
review. The lowered program and its literals live in the unexported fields of
`agent/internal/catalogprogram.Program`, which package `agent` cannot read.
The type exposes `Literals` (copies of the values dispatch hands the calls),
`Equal` and `GoString` (for tests and diagnostics; `GoString` prints the
whole `catalogprogram.Program`, literals included), and one `Run`, which
builds the dispatch nodes, substitutes every literal and runs the engine.
`Run` takes no observer, journal or engine, so a caller cannot attach one.
Inside the package, a source test
(`agent/internal/catalogprogram/source_guard_test.go`) holds the same line.
It reads the package's syntax, not its types, and it is red on:

- any import outside the set `Run` needs, so `contract/inspection`,
  `internal/journal`, the event hub and `unsafe` are refused, as are dot
  and blank imports;
- each allowed package imported more than once, unaliased: a path
  imported twice in one file, a path taking two local names across the
  package, or `reflect` or the engine imported under any name but its
  default. The test still tracks every local name a path takes, so a
  second name cannot hide uses under the first;
- any selector naming an engine method other than `WithMaxSteps` and `Run`
  (`WithObserver`, `RunObserved`, `RunObservedPending`, `RunJournaled`,
  `EmitRunTerminal`, `RunControl`, or one added later), whether called or
  taken as a method value, and any string literal equal to such a name;
- any engine package selector except `New`, and any reference to
  `engine.New` except as the callee that starts the one chain, so
  `engine.New` cannot be passed to `reflect` or held in a variable;
- any `reflect` selector except `reflect.DeepEqual`, so no engine method
  can be reached by a computed name;
- anything other than exactly one `engine.New(...).WithMaxSteps(...).Run(...)`
  chain, in `(*Program).Run`.
`catalogprogram.Lower` is the only caller of the lowering with
`Options.Literals`. Package `agent` no longer imports `internal/engine` or
`internal/lowering`, so it cannot build an engine or lower a literal itself.
Nothing that leaves the package carries the program: `Listing` and
`Catalog.List` (what MCP lists) never did, and the artifact digest still
hashes the recorded `flow.Program`, whose literal is already public through
`flow.Definition.Program`, and not the lowered one.

**Decision: guard, do not observe.** The other option was substituting the
literal inside the engine, through a node wrapper or a synthetic step, so
that observed inputs would equal what the node received. That changes the
step count, the `WithMaxSteps` budget and the event stream. It is also the
program-literal-form question #249 deferred. If catalog runs need
inspection (#77) or a journal later, that is the design to take up then.
Until then, `Run` refuses both by having no way to accept them.

Compatibility: an internal refactor, linked to
[#260](https://github.com/well-prado/new-blok/issues/260). No public API,
wire shape, document version, artifact digest, gate admission, dispatch input
or result changes. `agent/dispatch_corpus_test.go` compares nine literal and
non-literal workflows with a golden file generated on origin/main before the
change. The comparison covers every admission (identity, digests, effects,
capabilities, budget, input bytes), every published output, every node
input, the result and error, and every listing digest.
`agent/program_guard_test.go` runs the review's escape as an overlay file.
On origin/main it compiles and shows the hazard. Now it fails to compile on
`catalogprogram.Program`. The same file walks every value package `agent`
can reach for a `contract.InternalProgram`, and checks the import boundary.
`agent/internal/catalogprogram` pins its surface and allows only `flow` and
itself to import the lowering. `source_guard_test.go` is red on the
review's second escape, which keeps the surface and makes `Run` switch to
`WithObserver(o).RunObserved` when the context carries an observer. Before
that test, a probe through `Catalog.Invoke` logged the commit step observed
with `{"quantity":2,"sku":"SECRET"}` while `go test ./agent/...` stayed
green. It is also red on importing the journal, on `RunJournaled`, and on
holding the engine in a variable.

The step bound `Run` passes, `MaxCalls+1`, can never cut a catalog run
short: registration and `Invoke` already refuse a workflow with more calls
than the budget, and a program has at most one instruction per call plus
the output. What the bound does is lift the engine's 10000-step default
for a workflow at the 10000-call ceiling, whose program has 10001
instructions. Without the bound, that workflow registers and then always
fails. Both directions are tested.

The second review found a further escape, inside `(*Program).Run` and in
front of the untouched chain. It passed `engine.New` to `reflect.ValueOf`
and called `MethodByName("With"+"Observer")` and
`MethodByName("Run"+"Observed")`, splitting the names so no forbidden
string appears. The leak reproduced. The test is now red on it, through
both the `engine.New` reference rule and the `reflect` rule.

The third review imported `reflect` and the engine a second time, as `rx`
and `eng`. It built the counted chain on `eng.New` and pointed `Equal` at
`rx.DeepEqual`, then ran the round-2 attack under the plain names. The test
had tracked only the last local name per path, so it passed while
`Catalog.Invoke` leaked. It is now red on that edit through the
import-once, one-name and unaliased rules. Because every name is tracked,
the round-2 `reflect` and `engine.New` findings fire as well. An alias
alone (`eng` for the engine) is red too.

Out of scope, because a syntax check cannot hold them:

- `reflect` or `unsafe` used from another package of the module on
  `Program`'s unexported fields;
- edits to the source test itself;
- code-generation tricks.

Each is a visible change to a file, not a silent one, but none is
impossible.

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
