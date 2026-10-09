# ADR 0028: Lowered control flow and its execution paths

- Status: in progress for E07-T10 (#333), delivered as stacked PRs. Slice 1a
  lowers and runs compare, default, if, choose and try-finally in memory;
  slice 1b adds each and parallel, and lets control operands read the
  workflow input. Slice 2 runs compare, default, if, choose and
  try-finally durably on a single host; slice 3 each and parallel; slice 4
  (this revision) child runs. Slice 5 resumes at startup.
- Date: 2026-10-07 (slice 2: 2026-10-08; slice 3: 2026-10-08; slice 4:
  2026-10-09)
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
- Kinds lowered: compare, default, if, choose, try-finally, each, parallel
  and (slice 4) child, as contract kind `child` with the workflow it
  names in `Node` and its input reference in `References` (call input
  rules; `$child.<id>` reads its result later). The agent catalog's
  `Children` option keeps lowering a child as a call of the child
  workflow key instead. Template is rejected by kind, wherever nested,
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
  failure, after finally ran. A finally failure that replaces an
  uncertain try failure is itself uncertain (#412 Review R round 1): the
  try's effect still has an unknown outcome. A finally fail-fast started is still
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
journal that journals scopes (slice 2, below), and each and parallel only
through one that also journals their slots (slice 3, below); otherwise it
refuses with `durable_control_unsupported` before touching its journal.

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
- **Changed in slice 3: per-item scopes, not a join row.** This section
  first planned one `journal_joins` row (`RecordJoin`'s positional slots,
  #370) per each or parallel execution. A join row is rewritten whole on
  every slot it fills, so its cost grows with the square of the items
  (#386). Measured before slice 3 (real SQLite, synchronous FULL, one
  slot per transaction, origin/main d19cddc, load 9–20):

  | Storage | Items | Total | Per slot | Bytes rewritten |
  | --- | --- | --- | --- | --- |
  | One join row | 1,000 | 8.7 s | 8.7 ms | 14.9 MB |
  | One join row | 10,000 | 3 min 5 s | 18.5 ms, growing with n | 1.54 GB |
  | One row per slot (a table) | 10,000 | 4.8–21 s | 0.5–2.1 ms | 0.25 MB |
  | One completed scope per slot | 10,000 | 11.8–12.5 s | about 1.2 ms | 0.25 MB |

  So each item of an each, and each arm of a parallel, is a *slot*: a
  scope of its own, recorded completed with the item's result. It costs
  one row insert, flat per item, needs no schema change (`journal_scopes`,
  no journal v8), and inherits the scope rules: a completed scope is final
  (#334), compaction and inspection already handle scopes. `journal_joins`
  is not used for each or parallel. #370's positional-slot rules carry
  over: exactly N slots (the loop scope records N and a replay with
  another N is `journal_scope_conflict`), a filled slot never changes
  (`RecordSlot` with other bytes is `ErrRecordFinal`), and a slot holds
  the result wrapped, `{"output": <result>}`, so a null result still
  fills it.
- An each over no items records its scope completed with output `[]` and
  no slot.

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
  already keyed so one step can suspend more than once, #396). Each and
  parallel need a `LoopJournal` too (slice 3).
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

### Durable each and parallel on a single host (slice 3)

Like a stamp card: each item gets its stamp once it is done, and after a
restart nobody redoes a stamped item; the card is read instead.

- **Who journals.** `engine.LoopJournal` (a `ScopeJournal` plus `Slots`,
  `RecordSlot`, `FailScope`), implemented by `journal.RunJournal`. A
  durable runner whose journal journals scopes but not slots refuses each
  and parallel at any depth (`durable_control_unsupported`); the cluster
  journals neither.
- **Scopes.** The each or parallel enters its scope with the decision
  `{"items":N}` (N items or arms). Item `i` of an each is the slot
  `<each invocation>/body@<iteration>` (`loop/body@loop[3]`); arm `i` of a
  parallel is `<parallel invocation>/<i>@<iteration>` (`fan/0@root`); both
  have the loop's scope as parent, kind `item` or `arm`. A construct
  inside an item or arm has that slot as its parent
  (`loop/body/route@loop[0]` under `loop/body@loop[0]`), and its steps the
  item's iteration.
- **Slots.** Once an item's body (or an arm) succeeds, its result is
  recorded as its slot, `{"output": <result>}`, in one transaction
  (`RecordSlot`), after the body's last step committed: no hook joins it
  to that step's transaction. A crash in that window loses nothing: the
  item has no slot, so it replays, and its committed steps load instead of
  running. On entry an each reads its recorded slots in one range read of
  the scope key (`Slots`, by path prefix and parent) and **skips every
  item with a slot**: its body does not run again, its slot is its result.
  A parallel's arms always run again on a replay (their committed steps
  load), because steps after the parallel read their results; their slots
  record that they completed.
- **Results.** In a durable run an each's result is its slots' results as
  JSON values (numbers as `json.Number`, objects as maps), live and
  replayed alike, so a run that crashed and one that did not produce the
  same value; a typed node reading it converts it as any construct value
  (`node.Any.ConvertInput`). In memory it stays the Go values the bodies
  returned. The result is assembled from the slots in index order, every
  slot filled. The loop scope itself exits with a summary, `{"items":N}`,
  not the results again: a scope output is bounded like a step result
  (1 MiB), so storing every slot in it again bounded a whole loop by one
  result's limit, and a loop failed permanently at its exit after all its
  effects ran (#412 Review R round 1). Inspection reads the slots.
- **Slot bound.** A slot holds any result a step may commit plus its
  wrapping: `journal.MaxSlotBytes` = `MaxStepResultBytes` + 11 bytes of
  `{"output":}`, so an item whose result is one step's result fits. An
  item whose result is larger than any step result, such as an inner
  each's results, does not (see Limits).
- **Fail-fast is recorded.** When an item or arm fails and the loop fails
  fast, the loop's own failure (not a suspension, the caller's
  cancellation or a journal fault, which a later execution may get past)
  is recorded in its scope's decision,
  `{"items":N,"failed":{"code","class","step","uncertain"}}`
  (`FailScope`, fenced by the scope attempt). A later execution of the run
  reports that failure at once, starting no item, so a run that crashed
  after failing fast cannot run items it would never have run. The record
  is written when the loop joins its in-flight items; a crash before it
  lets a replay run the loop again. **Uncertainty is never lost** (#412
  Review R round 1, BLOCKER): the first failure is reported, but if any
  item or arm it canceled failed uncertain (an effect in flight, whose
  outcome is now unknown), the reported and recorded failure is marked
  uncertain, so the run ends uncertain instead of failing. Otherwise
  `FailRun` refuses the run (`ErrUncertain`) and every replay reports the
  same certain failure: the run would never settle. As a second line,
  the resumer ends a run uncertain when `FailRun` refuses it with
  `ErrUncertain`, whatever failure the execution reported.
- **Waits** stay refused inside an each or parallel (`checkProgram`), so
  no item suspends alone; the loop scope's entry is the run's first
  commit after a wait before it, so concurrent items never share a
  pending wakeup acknowledgement.
- **Cost.** One transaction per slot, plus the loop's entry and exit; a
  replay reads the slots once. A 1,000-item each leaves 1,001 scopes,
  which inspection pages over and compaction removes with the run
  (`TestThousandItemLoopInspectsAndCompacts`).

### Durable child runs on a single host (slice 4)

Like a manager who hands a task to a colleague with a numbered ticket and
goes home: the ticket's number is derived from her own desk and task, so
if she comes back after a crash she finds the same ticket, never opens a
second one, and picks up the colleague's answer once it is in.

- **Who journals.** `engine.ChildJournal` (`StartChild`, plus scopes and
  waits), implemented by `journal.RunJournal`. A durable runner whose
  journal does not implement it refuses a program with a child
  (`durable_control_unsupported`); in memory a child step fails
  (`child_requires_durable_runner`). A child may appear in an if, choose
  or try-finally arm, not inside an each or parallel (it suspends the run,
  like a wait).
- **Start, once.** The child step resolves its input like a call and calls
  `StartChild`, which in one transaction under the parent's lease: admits
  the child run, with an id and request key derived from the parent run
  and the step's scope path (`run:` + 32 hex of a SHA-256; request key
  `blok-child/<depth>/<parent run>/<digest>`), the parent's principal,
  the step input as its admitted input and, through the child workflow's
  input decoder, its engine-input identity fixed at admission (#380's
  rule); records it as the parent's child (`journal_children`); enters
  the step's scope (kind `child`, decision `{"child":"<run id>"}`); and
  schedules the parent's wait for it (named `blok.child:<run id>`, keyed
  by the step's identity). A replay finds the scope and returns the child
  it records: no second child is ever admitted. The runner resolves
  workflows for it (`RunJournal.WithChildren`; the resumer passes its
  registry).
- **Wait for the child.** The parent then waits at that wait and
  suspends, holding nothing. The transaction that ends the child (complete,
  fail, uncertain, cancel) settles the parent's record with the child's
  outcome (`engine.ChildOutcome`) and fires the parent's wait with it, so
  the parent is listed for resumption. A signal addressed to a child wait
  is refused (`ErrReservedSignal`): only the child's end fires it.
- **Outcome.** A completed child's output (as a JSON value) is the step's
  result, and the step's scope exits with `{"child":"<run id>"}`. A failed
  child fails the step (`child_failed`, class failure, the child's code and
  class in the message), an uncertain one makes it uncertain
  (`child_uncertain`), a canceled one fails it (`child_canceled`).
- **Starting the child.** The resumer starts a run's unstarted children
  as soon as the run suspends (`Journal.ChildrenToStart`), each once a
  worker is free; should that resumer die first, the interrupted-run scan
  takes them a lease after admission. It sweeps again when a child ends.
- **Depth.** A run's depth is in its request key (0 for a run not started
  as a child), so no walk is needed. `journal.Config.MaxChildDepth`
  (default 8) bounds it: a child step of a run at that depth is refused
  before anything is admitted (`child_depth_exceeded`, class admission,
  permanent).
- **No walk, no oracle (#397).** The child is always a run its own
  transaction admits, so it has no children and binding it cannot close a
  cycle: the subtree walk `RecordChild` does (181 ms at a 100,000-wide
  subtree, ADR 0003) never runs. A run already holding the derived id or
  request key that the step never recorded is refused with one
  diagnostic, `child_unavailable` ("the child run is not available"),
  whoever admitted it. Child refusals are `*engine.ChildError`;
  `journal.Permanent` reports them, so a runner settles the run failed.

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
| `engine.LoopJournal`; `RunJournal.Slots`, `.RecordSlot`, `.FailScope` (slice 3) | API, additive | None |
| A durable runner whose journal is a `LoopJournal` runs each and parallel; item and arm slots are `journal_scopes` rows of kind `item`/`arm` | behavioral: programs it refused now run; no schema change | None |
| A durable each's result is its slots' JSON values | behavioral (durable runs only, which refused each before) | None |
| `flow.Definition.Lower` lowers child as contract kind `child`; `engine.ChildJournal`, `ChildRequest`, `ChildStart`, `ChildOutcome`, `ChildError`, `ChildWaitPrefix`; `journal.ChildWorkflow`, `RunJournal.WithChildren`, `.StartChild`, `Journal.ChildrenToStart`, `IsChildRun`, `Config.MaxChildDepth`, `ErrReservedSignal` (slice 4) | API additive; behavioral: programs it refused now lower and run under a durable runner | None |
| Run end (complete, fail, uncertain, cancel) of a child settles its parent's record and fires its wait; signals named `blok.child:…` are refused | behavioral | None |
| `journal.MaxSlotBytes`; a loop scope's output is `{"items":N}`; a failure reported over an uncertain sibling or try is uncertain; the resumer ends uncertain a run `FailRun` refuses with `ErrUncertain` (#412 Review R round 1) | API additive; behavioral | None |

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

## Evidence (slice 3)

`internal/journal/loop_test.go` (real SQLite, real SIGKILL of a child
process at commit barriers, resumed in the parent; node invocations
logged to a file) and `internal/resumer/loop_crash_test.go` (a resumer in
a child process is killed after the third slot; a resumer in the parent
finds the run interrupted once the lease lapses and completes it). All
RED on origin/main d19cddc (the durable runner refused each and
parallel). Mutations, RED: slots never skipped, a null result read as an
empty slot, slots not wrapped, fail-fast not recorded, a recorded failure
ignored, the item count unchecked, a recorded slot overwritten, `Slots`
with neither its prefix range nor its parent bound (either bound alone
reads exactly the loop's slots, so removing only one stays GREEN), item
frames not their slot, no loop exit,
slots out of order, M47a and a blank slot parent, any scope journal
running loops. One mutation stays GREEN by construction: recording a
caller's cancellation as the loop's failure, because the record itself
is written under the canceled context and so never commits.

Review R round 1 (#412) added, each RED under a mutation of its fix:
fail-fast over a sibling's uncertain effect (journal and through the
resumer: settled uncertain on the first execution, where it was left
interrupted forever), the resumer's `ErrUncertain` fallback, the recorded
and replayed uncertainty, finally replacing an uncertain try, four
300 KiB items completing, an item of exactly `MaxStepResultBytes`
fitting its slot, a journal fault not recorded as the loop's failure,
`FailScope`'s attempt fence, and `RecordSlot`/`Slots` refused after the
run ended or under a stale lease. The parallel kill test now parks only
once its second arm has started (it counted that invocation; a delayed
arm failed it 3/3). Round 2 added the parallel half of the uncertainty
rule and kept a suspension from being marked uncertain, each RED under
a mutation of its fix.

## Evidence (slice 4)

`internal/journal/child_run_test.go` drives a parent and its child as
the resumer does (real SQLite) and SIGKILLs a child process just before
and just after the commit that starts the child, while the child runs
(after its effect committed), and after the child completed but before
the parent resumed; driving on in another process admits exactly one
child, runs every effect once and completes both.
`internal/resumer/child_crash_test.go` kills a resumer while the child it
started runs; a resumer in the parent process completes child and
parent. Also: failure and uncertainty reach the parent, the depth bound,
`child_unavailable` identical whoever holds the derived id, and child
waits refusing signals. RED on origin/main 4eebd62 where they compile
(the engine refused the kind, flow refused to lower it, the resumer test
never saw the child's effect); the journal tests are RED under named
mutations of the fix (no reuse of the recorded child, the binding error
leaking, no depth check, signalable child waits, a child's end waking
nobody, children not started by the resumer, refusals not permanent, the
parent's principal not inherited).

## Limits

- Template still does not lower (no defined substitution semantics).
- A child of another program format, or one run in memory, is not
  supported: child runs need a durable runner. A child step's input is
  the call input of one reference (or the workflow input), like a call.
- A call cannot read a field of the workflow input (the canonical
  compiler's rule); a control operand can.
- `internal/program`'s artifact format (version 1) refuses control
  programs; a version-2 artifact for them is decided with the durable
  slices.
- Resuming accepted runs at startup is slice 5.
  The cluster runs no control program durably: its step journal journals
  no scopes yet (#333).
- Slice 2's crash tests drive `engine.RunJournaled` over `RunJournal`
  with the leases the journal hands out, the calls the single-host
  resumer (#384) makes; slice 3 adds one crash test through the resumer
  itself.
- No bound on a run's total work across nested loops yet (#386 part 1).
- A slot is bounded like a step result, so nested loops can exceed it: an
  outer each item whose result is an inner each's results (four 300 KiB
  inner items, 1.2 MB) fails permanently (`journal_scope_slot`,
  `ErrStepResultLimit`) after the inner slots were written (#412 Review R
  round 2). Return a smaller result from the outer body, or read the inner
  results where they are used.
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
