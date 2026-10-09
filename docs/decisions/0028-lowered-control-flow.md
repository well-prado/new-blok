# ADR 0028: Lowered control flow and its execution paths

- Status: in progress for E07-T10 (#333), delivered as stacked PRs. Slice 1a
  lowers and runs compare, default, if, choose and try-finally in memory;
  slice 1b adds each and parallel, and lets control operands read the
  workflow input. Slice 2 (this revision) runs compare, default, if, choose
  and try-finally durably on a single host. Slice 3 makes each and
  parallel durable, slice 4 child runs, slice 5 resumes at startup.
- Date: 2026-10-07 (slice 2: 2026-10-08)
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
- Kinds lowered: compare, default, if, choose, try-finally, each, parallel.
  Child (in flow) and template are rejected by kind, wherever nested,
  before anything else is checked.
- References: `$step.<id>`, `$op.<id>` (compare, default), `$join.<id>`
  (if, choose, try-finally, each) and, inside an each's body,
  `$item.<each id>` (the current item; flow records an each's item under
  its id, so a nested body still reads the outer item), each with optional
  `.<field>` segments, lower to `contract.Reference`; the item lowers to a
  reference to the each's id, which the body's frame binds to the item.
- The workflow input: a control operand or an arm's result may read
  `$input[.<field>…]`, lowered to a reference to `contract.InputStep`
  (`"$input"`, outside the id grammar, so no step can take it). A call
  input still may not read a field of the input, and literal call inputs
  stay rejected: the call-only rules are the canonical compiler's (#244).
  Literals are accepted only as control operands.
- Scope: an arm sees what its construct saw plus its own earlier steps.
  Nothing inside an arm is visible outside it, including in a sibling arm;
  read the construct's result instead. Such a reference is refused naming
  where the step is: `input "$step.vip" names a step inside arm "then" of
  if "route", which is not visible here: read the construct's result
  instead`. An item read outside its each's body is refused the same way
  (`"$item.loop" reads the item of each "loop", which is readable only
  inside that each's body`).
- Parallel is the one exception, decided for slice 1b: a parallel has no
  result of its own (flow returns none), and once it completes every arm
  has completed, so its arms' steps (at the arm's top level) are readable
  after it, as if recorded before it. They are never readable in a sibling
  arm, which runs concurrently. A reference to `$join.<parallel id>` is
  refused.
- Each concurrency is from 1 to 1024 (`lowering.MaxConcurrency`, as
  `flow.Each`).
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
- Values a construct produced reach a typed node as its type, and nothing
  else changes. A literal operand is a JSON value (numbers as
  `json.Number`, objects as maps), an each's result is a `[]any`, and a
  construct hands on the Go value it read (a `*T` field where the node
  takes a `T`). So a call whose input reference names a control
  instruction (its result, or an each's item) is converted by
  `node.Any.ConvertInput` just before the node is invoked, and only to a
  value the source exactly holds: a value of the type as is, a non-nil
  `*T` dereferenced, null only into a type that can be nil, a JSON value
  decoded with unknown fields refused, a slice element by element. That
  decode is standard `encoding/json`: it matches field names
  case-insensitively and decodes a base64 string into a `[]byte`, and it
  leaves a field the value lacks at its zero value; the input schema is
  checked first, on the value as resolved rather than the converted one,
  so a required field the value lacks is refused before any decode
  (Review R round 3). Anything
  else is passed unchanged, so the node refuses it with its own
  `input_type_mismatch: expected <type>, got <type>`, as outside control
  flow. Values are never re-encoded through their own `MarshalJSON`. The
  node's input schema is checked on the value as resolved, before any
  conversion, exactly as in a call-only program.
  Every other call — reading the workflow input or a node's output — runs
  exactly as in a call-only program, even in a format-2 program (Review R
  round 2 found that converting every call made a program's calls behave
  differently once any construct was added: a nil `*T` became a zero `T`,
  an unknown field was dropped). Converting only construct values keeps
  the rule local to what control flow introduced.
  A literal reaching a node whose input type is an interface (`any`) keeps
  its JSON form: numbers arrive as `json.Number`.
  Values that reach no node are not converted: the workflow output and a
  construct's own result are what the construct produced, so a `Default`
  with a literal `[]string` fallback returns `[]any` as the run's output,
  not a `[]string` (`Ref[T]` types the authoring, not the run's output).
- if / choose: the condition is read as JSON (a named bool or string type
  selects like the plain one); a condition of another type is
  `invalid_condition`. The selected arm runs through the control path
  (`runControlStep`: `ControlBranch` for if, `ControlAction` for choose);
  choose runs the first case whose match equals the key, else default.
- try-finally runs through `ControlTry`: once the try arm has started, the
  finally arm runs after it succeeds or fails, including when an enclosing
  each or parallel canceled it because a sibling failed (fail-fast is the
  construct's own decision, decided by the orchestrator after Review R
  round 1 on the 1b PR). Only the caller's cancellation of the run itself
  skips finally (cooperative cancellation); the engine tells the two apart
  by the run's own context, which it carries in the context, and runs
  finally in a context canceled only by the run's. The flow recording's
  "finally-not-guaranteed-after-suspension" note stands for durable
  suspension. A finally failure replaces the try outcome, except in a
  sibling canceled by fail-fast, where the first error wins (the finally
  failure still appears in Steps); otherwise a try failure is the run's
  failure, after finally ran. A finally fail-fast started is still
  canceled when the caller then cancels the run (Review R round 2).
- Each construct is itself a step in `Result.Steps`, after its arm's
  steps, carrying its result. It is never `Executed` (that reports a node
  invocation; the arm's steps report their own) and never an external
  call. Its trace span is a child of its enclosing span; its arm's steps
  are children of its span.
- A choose case value (`Arm.Match`) must be a JSON string; any other is
  `invalid_control`, never a case that silently cannot match.

- each reads its items (a Go slice or array element by element, keeping the
  elements' types; anything else must read as a JSON array, else
  `invalid_items`), refuses more items than the step budget
  (`step_budget_exceeded`), and runs the body once per item through
  `ControlEach`: at most `Concurrency` items in flight (a worker pool of
  that size), each in its own frame binding the each's id to the item.
  The result is the bodies' results in item order, whatever order they
  finish in. The first item to fail cancels the items in flight and no
  further item starts (fail-fast); the run fails with that item's error.
  When several fail concurrently, "first" is whichever records its failure
  first, not the lowest index.
- parallel runs every arm at once through `ControlParallel`, each in its
  own frame; the first arm to fail (whichever records it first) cancels the
  others. When all arms succeed,
  their frames' results join the parallel's frame (the visibility rule
  above). It has no result.
- Concurrent arms share the run's bookkeeping, so it is serialised: the
  step list (`Result.Steps`, in completion order, which is not item order),
  observer events (one at a time, so an observer need not be
  goroutine-safe), and the last effectful step (`afterEffect`'s
  saturation rule). A step's span and external flag are per step.
- A step inside an each runs once per item, so its inspection attempt id
  names its iteration: `<run attempt>/<step id>@<iteration path>/<n>`
  (`attempt:…/line@orders[1]/lines[0]/1`); outside every each it stays
  `<run attempt>/<step id>/<n>`. The inspection recorder keeps one attempt
  per id, so each iteration is its own attempt of the step.

A durable runner (`RunJournaled`) runs a format-2 program only through a
journal that journals scopes (slice 2, below), and refuses each and
parallel with `durable_control_unsupported` before touching its journal
until slice 3 journals their joins.

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
  top-level step's effects and waits today; inside an each's body
  `<each id>[<index>]`, joined with `/` for nested eaches
  (`orders[2]/lines[0]`). Ids are unique per workflow, so the each id
  suffices. The each step itself runs in its enclosing iteration.
  Parallel arms do not add to it: an arm is told apart by its invocation
  path (`fan/0/…`).

A step execution is identified by `(InvocationPath, IterationPath)`, the
pair `OperationIdentity` and ADR 0027's waits already use. For a step at
the top level this is `(step id, "root")`, which is what E07-T09 slice C's
engine adapter writes for waits and effects until the engine supplies paths
(slice 2 adds both paths to `engine.StepIdentity` and `WaitIdentity`).
#382 is a prerequisite of that change: both identities are hashed into the
ids the journal and `internal/cluster` store (`WaitIDFor`), so adding fields
would change every existing wait and step id unless their encoding is made
stable first, and the cluster's wait id must include the iteration.

- Retry safety (#190) across concurrent arms: an arm's failure is
  classified when it fails, but a sibling may commit an effect after that
  and before the construct joins (an each or parallel waits for every arm
  it started). So a construct re-applies the rule to its failure after the
  join: once any step with effects has completed, a saturated failure no
  longer reads as saturation, and the caller does not retry the whole
  run.

### Joins and scopes (for the durable slices)

Recorded here so slices 2 and 3 key their journal records consistently:

- A scope (`journal.ScopeRecord`) per construct execution, path
  `<invocation path>@<iteration path>` (`@` appears in no segment), parent
  path the scope of the construct that encloses it (empty at the top),
  kind the instruction kind.
- A join (`journal.JoinRecord`, positional slots since #370) per each or
  parallel execution: `Expected` = number of items or arms, slot `i` =
  item `i` or arm `i`. #370 reads a JSON null slot as not yet filled, so a
  slot always holds the result wrapped, `{"output": <result>}`: an
  iteration whose body yields null, and every parallel arm (which yields
  nothing), still fills its slot.
- An each over no items has no join: `RecordJoin` requires `Expected ≥ 1`,
  so slice 3 records the empty each's scope as completed with output `[]`
  and writes no join row.

### Durable if, choose and try-finally on a single host (slice 2)

Like a hiker who marks the fork she took on the map before walking on: if
she has to start the walk again, she does not look at the signpost a
second time, she follows the mark.

- **Who journals.** `engine.ScopeJournal` (`EnterScope`, `ExitScope`)
  is implemented by `journal.RunJournal`, the SQLite journal under the
  run lease (ADR 0027). A durable runner whose journal does not implement
  it refuses every control program with `durable_control_unsupported`
  before touching the journal: `internal/cluster`'s step journal does not
  (`TestClusterRefusesControlPrograms` pins it; its run suspensions are
  already keyed so one step can suspend more than once, #396). Each
  and parallel are refused at any depth until slice 3.
- **Scopes.** If, choose and try-finally each enter a scope
  (`journal_scopes`) when they start: path
  `<invocation path>@<iteration path>` (`route@root`,
  `route/then/pay@root`), parent path the scope of the construct whose
  arm it runs in (empty at the top), kind the instruction kind. The scope's
  input is the decision: `{"arm":"then"}` for an if, the selected arm's
  name for a choose, none for a try-finally. A successful construct exits
  its scope with its result (bounded by `MaxStepResultBytes`). Compare and
  default have no arms and no scope: they are pure values recomputed on
  replay from journaled step results, the admitted input and literals.
- **The recorded decision wins.** The first entry records the decision; a
  replay re-enters the scope (a new scope attempt, fencing the earlier
  one, #351) and gets the recorded decision back, which the engine
  follows whatever its condition now reads. A committed step result is
  never recomputed and a recorded decision is never re-evaluated: an arm
  that committed an effect is the arm the run continues
  (`TestDurableBranchNeverReevaluatesItsDecision` changes the journaled
  value the condition reads and the recorded arm still runs). A scope
  recorded under another kind or parent is `ErrRequestConflict`, a
  decision naming no arm (or any decision on a try-finally)
  `journal_scope_conflict` wrapping `engine.ErrScopeConflict`, a canceled
  scope `ErrRecordFinal`; `journal.Permanent` reports all three, so a
  runner settles the run failed with that diagnostic instead of retrying
  it (`TestDurableUnfollowableDecisionSettlesFailed`, #404 Review R round
  1). A completed
  scope's arm is still replayed from the journal (its steps load, nothing
  runs again), so values keep the Go types a live run gives them; it is
  not exited again.
- **Steps in arms.** A step inside an arm is journaled under its real
  identity: `engine.StepIdentity.InvocationPath` is its invocation path
  (`route/then/vip`) and `IterationPath` the frame's iteration (`root`
  until slice 3). The journal keys its operation and its wait by that
  path. A top-level step leaves `InvocationPath` empty, so its stored
  identity, operation key and wait id are byte for byte what they were
  (ADR 0027 rules). `InvocationPath` is not an input to
  `engine.OperationKey` (no new key encoding): step ids are unique across
  a program's tree, so with the artifact digest the id determines the
  path; the SQLite journal's own key (`OperationIdentity`) already hashes
  the invocation path. Lowering records ids that way, and the engine's
  `checkProgram` refuses any program, in memory or durable, whose id
  appears twice anywhere in the tree (`duplicate_step_id`, configuration;
  #404 Review R round 1): two steps sharing an id would share an
  operation key, and a provider deduplicating on it would drop one
  effect.
- **Try-finally.** `finally` runs once per run across crashes: every step
  in it is journaled, so a replay loads what committed and runs only the
  rest. A wait suspending inside try is not the end of try: finally does
  not run at the suspension, only when the resumed try ends
  (`runTry`; this lifts the recording's
  `finally-not-guaranteed-after-suspension` caveat for single-host durable
  runs). Caller cancellation still skips finally and internal fail-fast
  still runs it (#383); an execution canceled by its caller inside try
  (the run itself is not canceled) runs no finally, and finally runs
  once on the next execution. A pure call that failed was
  never committed, so a replay runs it again (as at the top level) and
  the try may now succeed.
- **Waits in arms.** A wait may appear in an arm of an if, choose or
  try-finally (`checkProgram`), not inside an each or parallel body. It
  suspends the run inside the constructs; their scopes stay running; a
  signal and `PendingResumptions` resume it through the same lease path
  as a top-level wait.
- **Failure.** A construct that fails leaves its scope running (it may be
  retried: a transient fault, a lost lease, the caller's cancellation).
  Failing or canceling the run ends them: `Journal.FailRun` (and so
  `RunJournal.FailRun`) and `Journal.CancelRun` cancel the run's running
  scopes, with the failure code or the cancellation reason as their
  error, in the transaction that ends the run, so an operator can still
  cancel or fail a run that crashed inside an arm, as before control flow
  was durable (`TestOperatorCanEndARunKilledMidArm`, #404 Review R round
  1). A dispatched effect, a waiting wait or an uncertain operation still
  refuses both. An operator's cancel or fail of a run inside a try skips
  its `finally`, consistent with #383 (canceling the run skips it): the
  run has ended, and an execution still holding the lease can neither
  enter nor exit a scope (nor commit a step) afterwards, `ErrRunNotActive`
  (#404 Review R round 1b). `MarkRunUncertain` leaves the scopes running
  on purpose: it rewrites no facts, and the uncertain effect is inside
  them.
- **Transactions.** Entering and exiting a scope are their own
  transactions (each fenced by the lease and acknowledging the waits read
  since the last commit, as a step commit does), not the step's: the
  decision commits before any step of the arm runs, and a crash between
  the arm's last step and the exit re-enters and re-exits the scope on
  replay. That is two extra synchronous commits per construct.

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
| In a format-2 program a call reading a construct's value receives it as its input type when it reads exactly as one | behavioral (format 2, construct values only) | None |
| `internal/program` `Build`/`Decode` refuse format ≠ 0 and control bodies | behavioral, fail closed | None |
| `StepResult.InvocationPath`, `.IterationPath` (engine, execution) | API, additive | None |
| `flow.Definition.Lower` lowers each and parallel (1b) | behavioral: programs it refused now lower | None |
| `flow.Each`'s item source is `$item.<each id>` (was `$item`) in `Program()` | behavioral, source-visible for code reading recorded sources | Read the each id after `$item.` |
| `contract.Control.Concurrency`, `contract.InputStep` | API and wire, additive, omitted when zero | None |
| A control operand or arm result may read `$input[.<field>…]` | behavioral: refused before | None |
| Step attempt ids inside an each carry `@<iteration path>` | behavioral (new steps only); outside every each unchanged | None |
| Engine refuses unknown formats, misshapen control instructions, control in format 0, control under a durable runner | behavioral, fail closed | None |
| `engine.ScopeJournal`, `ScopeIdentity`, `ScopeEntry`, `StepIdentity.InvocationPath`, `.Invocation()`; `RunJournal.EnterScope`, `.ExitScope` (slice 2) | API, additive; `InvocationPath` omitted when empty, so top-level identities encode as before | None |
| A durable runner whose journal journals scopes runs if, choose, try-finally, compare and default; others still refuse control | behavioral: programs it refused now run | None |
| A wait may appear in an if, choose or try-finally arm | behavioral: refused before | None |
| A suspension inside try does not run finally | behavioral (only reachable durably) | None |
| `Journal.FailRun`, `RunJournal.FailRun` and `Journal.CancelRun` cancel the run's running scopes (they used to refuse a run with one); `journal.Permanent` reports `ErrRecordFinal` and `engine.ErrScopeConflict` | behavioral | None |
| The engine refuses a program whose step id appears twice anywhere in its tree (`duplicate_step_id`), any format | behavioral, fail closed; lowering and v1 artifacts already refused it | None |

## Evidence (slice 1a)

Review R round 1 found the typed cases: every control test had passed
`map[string]any` between nodes. `flow/control_typed_test.go` runs typed Go
nodes (typed nil `*string`, `*int`, `*struct`, nil `[]string`, int and
struct literals into typed nodes) and was RED before the fixes, as were the
sealed-arm, v1-artifact, choose-match and span tests. Round 2 added
`TestControlProgramsReadReferencesLikeCallOnlyPrograms` (the same call with
and without an unrelated compare must fail the same way: nil `*string` and
nil `*struct` with `null_not_allowed`, an unknown field with
`unknown_field`, a self-redacting `*secret` and a node's own `*int` with
`input_type_mismatch`), `TestConstructValuesAreNeverInventedForTypedNodes`
and `node`'s `TestConvertInputNeverInventsAValue`.

RED on origin/main 79ee0a7 (each fails at `Lower` with "cannot be lowered",
the builder test with `Define err=<nil>`): `flow/control_run_test.go`,
`flowtest/control_test.go`, `execution/control_http_test.go` (the `blok
new` starter's composition with a branch, over HTTP), and the starter app
scaffolded by `blok new` with a branching workflow. Tests that use new API
(`control_nested_test.go`, `internal/engine/lowered_test.go`,
`internal/lowering` control tests) are RED under named mutations in the
#333 slice 1a PR. `TestCallOnlyLoweringKeepsItsEncodingAndDigest` passes on
origin/main by design and pins the encoding and digest recorded there.

## Evidence (slice 1b)

RED on origin/main and on slice 1a's head (`6de9353`), each failing at
`Lower` with "cannot be lowered": `flow/each_parallel_test.go` (each order
and measured concurrency, fail-fast cancellation, parallel arms meeting at a
rendezvous neither can pass alone, then read after the join) and
`TestLoopWorkflowOverHTTP` (the starter's composition with a loop over the
posted lines). Tests that use new API (`each_parallel_nested_test.go`,
`internal/engine/each_parallel_test.go`, `TestEachAndParallelScopes`) are
RED under named mutations in the 1b PR, including the race detector for the
serialised step list and events.

## Evidence (slice 2)

`internal/journal/control_test.go` runs real engine programs through the
SQLite journal and SIGKILLs a child process at journal commit barriers
(after the decision commits, after an arm's effect commits, inside try,
inside finally, inside finally after try failed), then resumes in the
parent under a fresh lease; every node invocation is logged to a file, so
counts span both processes. All are RED on origin/main f3ffd4d (the
durable runner refused the program, or an arm could not hold a wait).
Mutations of the fix, each RED: following the recomputed decision,
running finally at a suspension, `FailRun` leaving scopes running, arm
steps keyed by id, M47a (blank `parent_path` in the scope insert; GREEN
on origin/main across `internal/journal` and `inspect`), no scope exit,
no each/parallel refusal, any journal running control (cluster), no
kind/parent check, a non-root iteration, the new decision replacing the
recorded one.

## Limits

- Child and template still do not lower (child needs a child-run path;
  template has no defined substitution semantics).
- A call cannot read a field of the workflow input (the canonical
  compiler's rule); a control operand can.
- `internal/program`'s artifact format (version 1) refuses control
  programs; a version-2 artifact for them is decided with the durable
  slices.
- Each and parallel run in memory only (slice 3 journals their joins);
  child runs are slice 4; resuming accepted runs at startup is slice 5.
  The cluster runs no control program durably: its step journal journals
  no scopes yet (#333).
- Slice 2's crash tests drive `engine.RunJournaled` over `RunJournal`
  with the leases `TakeRunLease` and `PendingResumptions` hand out, the
  calls the single-host resumer (#384) makes; they do not go through the
  resumer itself, which is not on main yet.
- `flow` cannot record a wait, so a wait in an arm comes only from a
  program built otherwise; flow keeps its suspension note.
- A construct's result over `journal.MaxStepResultBytes` (1 MiB) fails a
  durable run (`ErrStepResultLimit`, permanent) but not an in-memory one,
  exactly as a call's result already does: the bound is the journal's
  storage bound, and memory runs keep none (#404 Review R round 1 chose
  documenting over a new in-memory bound, for parity with calls).
- An each runs at most as many items as the step budget; there is no
  separate bound on the total steps its bodies run.
- Inspection still shows one step per id: an each body's step is one step
  with one attempt per iteration (at most 100, the recorder's bound), not
  one step per iteration, and the step's status is its last event's, so
  it reads completed or failed from whichever iteration finished last.
  Attempt ids bound the iteration part: past 48 characters it is replaced
  by `sha256:<32 hex>` of the path, so an id fits the recorder's 160, and
  the recorder matches completions on the same bounded form.
- `flowtest.Result.Step(id)` returns the first execution of id in
  completion order; inside an each, read `Steps()` and select by
  `IterationPath`.
