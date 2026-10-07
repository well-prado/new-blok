# ADR 0028: Lowered control flow and its execution paths

- Status: in progress for E07-T10 (#333), delivered as stacked PRs. Slice 1a
  (this revision) lowers and runs compare, default, if, choose and
  try-finally in memory. Slice 1b adds each and parallel; slices 2+ make
  every construct durable on a single host.
- Date: 2026-10-07
- Roadmap: E07-T10 ([#333](https://github.com/well-prado/new-blok/issues/333)),
  closing gaps in E07-T05 (#47) and E07-T07 (#49)
- Owners: `internal/lowering` (lowering), `internal/engine` (`lowered.go`,
  the interpreter in `engine.go`), `contract` (`InternalProgram`), `flow`
  (recording)
- Builds on: ADR 0001 (public API changes), ADR 0027 (wait identity by
  step and iteration path), ADR 0003 (recovery records, #334)

## Context

`flow` exposes If, Choose, Compare, Each, Parallel, TryFinally, Child,
Select, Default and Template, but `flow.Definition.Lower` rejected every
instruction except a call, so no application could run a branch or a loop:
a workflow with `flow.Compare` and `flow.If` failed with
`flow: instruction "big" of kind "compare" cannot be lowered`
(origin/main 79ee0a7, reproduced in a freshly scaffolded app).
`execution.Runner` and `flowtest.Run` both run the lowered
`contract.InternalProgram`, and the engine's control path
(`internal/engine/control.go`: branch, each, parallel, try) had no caller
outside its tests.

The recording itself could not express nesting: an arm's calls were
appended to the workflow's flat instruction list, before the construct and
indistinguishable from top-level calls, and an arm had no builder to nest a
construct on.

The durable slices of #333 key scopes, joins, children and waits by a
step's invocation path and iteration path (the journal already keys effects
and waits that way: `OperationIdentity`, ADR 0027). The program form chosen
here fixes those paths.

## Decision

### Recording

Each arm records on its own `flow.Builder`; every builder of a definition
shares one id set, so ids stay one flat namespace across arms (the existing
duplicate rule). A construct's `flow.Instruction` carries its arms in
`Arms []flow.Arm{Name, Case, Instructions, Output}`, and its literal
operands encoded in `Literals` (by `Data` key; an if/choose condition
literal in `Literal`). `ArmBuilder.Builder()` returns the arm's builder,
so a construct nests: `flow.If(arm.Builder(), …)`. Recording a step on an
enclosing builder while one of its arms is being built is refused
(`flow: step "x" was recorded on an enclosing builder while one of its arms
was being built; …`): with arms recorded separately it would otherwise run
outside the arm, before the construct. An arm's builder is sealed when its
arm returns; a step recorded on it later (an `ArmBuilder` kept and used from
a sibling arm) would be in no arm the program runs, and is refused too
(`flow: step "x" was recorded on the builder of an arm that has already been
built; …`).

Arm names, which are also path segments: if `then`, `else`; choose
`case-<n>` (cases in sorted key order, the key in `Case`) then `default`;
try-finally `try`, `finally`; each `body`; parallel `<n>`.

### Program format

`contract.InternalProgram` gains `Format int` (`json:"format,omitempty"`).
Zero is the original call set (call, wait, output); `contract.ControlFormat`
(2) adds the control instructions. Lowering sets it only when a control
instruction is present, and every new field is `omitempty`, so a call-only
program encodes byte for byte as before and keeps its artifact digest
(`internal/program` hashes the encoded instructions). The engine refuses an
unknown format and a control instruction in a format-0 program.
`internal/program`'s version-1 artifact holds the call set only: `Build` and
`Decode` refuse a program of another format or any instruction with a
control body (`unsupported_program_format`), so a control program is never
written into v1 and read back without its format.

`contract.InternalInstruction` gains `Control *Control`:

```go
type Control struct {
	Operator string    // compare: eq, ne, gt, gte, lt, lte
	Operands []Operand // compare left,right; default value,fallback; if/choose condition
	Arms     []Arm
}
type Operand struct {
	Reference *Reference      // a value in scope
	Literal   json.RawMessage // or a JSON literal; exactly one is set
}
type Arm struct {
	Name         string                // path segment
	Match        json.RawMessage       // choose case value (JSON string)
	Instructions []InternalInstruction // indexed from 0 within the arm
	Output       *Operand              // the arm's result; nil only for finally
}
```

Example (`flow/control_nested_test.go`, `TestControlProgramEncoding`
pins it):

```json
{"index":1,"id":"big","kind":"compare","control":{"operator":"gt",
  "operands":[{"reference":{"step":"price","path":["total"]}},{"literal":100}]}}
{"index":2,"id":"route","kind":"if","control":{
  "operands":[{"reference":{"step":"big"}}],
  "arms":[{"name":"then","instructions":[…],"output":{"reference":{"step":"vip"}}},
          {"name":"else","instructions":[…],"output":{"reference":{"step":"kept"}}}]}}
```

The document form (`contract.Document`) is unchanged: control flow has no
document encoding, so the #244 rule "flow lowers exactly what the canonical
compiler produces" applies to calls only.

### Lowering rules (`internal/lowering`, `Options.Control`)

- `flow.Definition.Lower` sets `Control`; the agent catalog does not, so a
  composed workflow with control flow stays not agent-safe, rejected with
  the message it always had.
- Kinds lowered: compare, default, if, choose, try-finally. Each, parallel,
  child (in flow) and template are rejected by kind, wherever nested,
  before anything else is checked.
- References: `$step.<id>`, `$op.<id>` (compare, default) and `$join.<id>`
  (if, choose, try-finally), each with optional `.<field>` segments, lower
  to `contract.Reference`. `$input` fields and literal call inputs stay
  rejected as before; literals are accepted only as control operands.
- Scope: an arm sees what its construct saw plus its own earlier steps.
  Nothing inside an arm is visible outside it, including in a sibling arm;
  read the construct's result instead. Such a reference is refused naming
  where the step is: `input "$step.vip" names a step inside arm "then" of
  if "route", which is not visible here: read the construct's result
  instead`.
- Compare operators are checked; nesting is bounded at 64 levels
  (`lowering.MaxNesting`, the engine's bound too).

### Execution (`internal/engine`)

Before running anything the interpreter checks the whole program
(`checkProgram`): format, the shape of every control instruction (operand
count, exactly one form per operand, operator, arm names and order, choose
match values, only finally without an output, no output or wait inside an
arm), nesting depth, and the step budget counted through every arm. A
malformed program fails with `invalid_control` before its first step.

The instruction loop became `runBlock(frame, instructions)`; a call and the
output instruction run exactly as before. A *frame* holds one scope's
results: the root frame is `Result.State`; each arm runs in a new frame
whose parent is the frame of its construct. Reads walk up the frames; a
write goes to the arm's frame only, so arm results never reach
`Result.State`.

- compare: both operands are read as JSON values. eq/ne compare by value,
  numbers by magnitude whatever their encoding (`100` = `100.0`; parsed at
  512-bit precision, so no exponent makes parsing unbounded). gt, gte, lt,
  lte take two numbers or two strings (byte order); anything else is
  `invalid_comparison`.
- default: the value, unless its path reaches nothing (a missing field, a
  field of null) or its JSON form is null — nil, or a nil pointer, slice,
  map or interface of any type (a reference hands on Go values, so an unset
  optional field is a typed nil), or a value encoding itself as null; then
  the fallback. An empty slice or map is a value. Any other read error is
  `invalid_operand`.
- Values reach nodes as the node's type. A literal operand is a JSON value
  (numbers as `json.Number`, objects as maps), and a reference hands on the
  Go value it reaches (a `*T` field where the node takes a `T`). In a
  format-2 program every call's input goes through
  `node.Any.ConvertInput`: a value already of the node's input type passes
  as is, any other is converted through its JSON encoding, as a durable
  adapter restores a persisted output. A node never receives a value of
  another type. Values that reach no node (the workflow output, a compare)
  stay as they are: a literal fallback in the output is its JSON value.
  Call-only programs are unchanged (no conversion).
- if / choose: the condition is read as JSON (a named bool or string type
  selects like the plain one); a condition of another type is
  `invalid_condition`. The selected arm runs through the control path
  (`runControlStep`: `ControlBranch` for if, `ControlAction` for choose);
  choose runs the first case whose match equals the key, else default.
- try-finally runs through `ControlTry`: the finally arm runs after the try
  arm succeeds or fails, unless the run is canceled (cooperative
  cancellation, the control path's existing rule; the flow recording's
  "finally-not-guaranteed-after-suspension" note stands for durable
  suspension). A finally failure replaces the try outcome; otherwise a try
  failure is the run's failure, after finally ran.
- Each construct is itself a step in `Result.Steps`, after its arm's
  steps, carrying its result. It is never `Executed` (that reports a node
  invocation; the arm's steps report their own) and never an external
  call. Its trace span is a child of its enclosing span; its arm's steps
  are children of its span.
- A choose case value (`Arm.Match`) must be a JSON string; any other is
  `invalid_control`, never a case that silently cannot match.

A durable runner (`RunJournaled`) refuses a format-2 program with
`durable_control_unsupported` before touching its journal: replaying arm
steps by step id alone would be wrong once a step can run more than once
per run (#333 slices 2+).

### Paths

Every `engine.StepResult` and `execution.StepResult` carries:

- `InvocationPath`: the step id at the top level; inside an arm,
  `<construct invocation path>/<arm name>/<id>`. For example
  `payment/try/route/then/lane/case-0/coffee-vip`. Segments are ids or arm
  names, which match the id grammar or are `case-<n>`/`<n>`, so `/` never
  appears inside a segment. It is derived from the program tree at run
  time, not stored: the tree is in the artifact digest, so a path cannot
  disagree with the artifact.
- `IterationPath`: `root` outside every each, as the journal keys a
  top-level step's effects and waits today. Slice 1b defines it inside an
  each: `<each id>[<index>]`, joined with `/` for nested eaches
  (`orders[2]/lines[0]`). Ids are unique per workflow, so the each id
  suffices.

A step execution is identified by `(InvocationPath, IterationPath)`, the
pair `OperationIdentity` and ADR 0027's waits already use.

## Compatibility (ADR 0001)

| Change | Class | Migration |
| --- | --- | --- |
| `InternalProgram.Format`, `InternalInstruction.Control`, `Control`, `Operand`, `Arm`, `ControlFormat` in `contract` | API and wire, additive; omitted when zero, so call-only programs encode and digest as before | None |
| `flow.Definition.Lower` lowers compare, default, if, choose, try-finally | behavioral: programs it refused now lower | None |
| `flow.Program()`: an arm's instructions are in its construct's `Arms`, not in the workflow's list | behavioral, source-visible for code walking `Program().Instructions` | Walk `Arms` recursively |
| `flow.Instruction.Arms`, `.Literals`, `flow.Arm`, `ArmBuilder.Builder()` | API, additive | None |
| Recording a step on an enclosing builder inside an arm is a `Define` error | behavioral: such a step used to run before the construct | Record it on the arm |
| Recording a step on an arm's builder after its arm returned is a `Define` error | behavioral: such a step used to run outside the construct | Record it inside the arm's callback |
| `node.Any.ConvertInput` | API, additive | None |
| In a format-2 program a call's input is converted to the node's input type | behavioral (format 2 only) | None |
| `internal/program` `Build`/`Decode` refuse format ≠ 0 and control bodies | behavioral, fail closed | None |
| `StepResult.InvocationPath`, `.IterationPath` (engine, execution) | API, additive | None |
| Engine refuses unknown formats, misshapen control instructions, control in format 0, control under a durable runner | behavioral, fail closed | None |

## Evidence (slice 1a)

Review R round 1 found the typed cases: every control test had passed
`map[string]any` between nodes. `flow/control_typed_test.go` runs typed Go
nodes (typed nil `*string`, `*int`, `*struct`, nil `[]string`, int and
struct literals into typed nodes) and was RED before the fixes, as were the
sealed-arm, v1-artifact, choose-match and span tests.

RED on origin/main 79ee0a7 (each fails at `Lower` with "cannot be lowered",
the builder test with `Define err=<nil>`): `flow/control_run_test.go`,
`flowtest/control_test.go`, `execution/control_http_test.go` (the `blok
new` starter's composition with a branch, over HTTP), and the starter app
scaffolded by `blok new` with a branching workflow. Tests that use new API
(`control_nested_test.go`, `internal/engine/lowered_test.go`,
`internal/lowering` control tests) are RED under named mutations in the
#333 slice 1a PR. `TestCallOnlyLoweringKeepsItsEncodingAndDigest` passes on
origin/main by design and pins the encoding and digest recorded there.

## Limits

- Each, parallel, child and template still do not lower (slice 1b for
  each and parallel; child needs a child-run path; template has no defined
  substitution semantics).
- `$input` fields cannot be read by a call or a control operand; branch on
  a node's output.
- `internal/program`'s artifact format (version 1) refuses control
  programs; a version-2 artifact for them is decided with the durable
  slices.
- In memory only; nothing here is journaled or resumable.
- Per-iteration inspection events: with each (1b), one step id runs more
  than once per run, and inspection attempt ids are keyed by step id.
