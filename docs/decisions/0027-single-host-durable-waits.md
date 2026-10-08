# ADR 0027: Single-host durable waits

- Status: in progress for E07-T09 (#332), delivered in slices. Slice A
  (merged, #350) records wait identity and signal routing; slice B (#366,
  #378) makes wakeups crash-safe; slice C1 (this revision) makes
  `internal/journal` the engine's step and wait journal; slice C2 (the
  single-host resumer) completes it
- Date: 2026-10-07
- Roadmap: E07-T09 ([#332](https://github.com/well-prado/new-blok/issues/332)),
  closing gaps in E07-T04 (#46); prerequisite of E07-T10 (#333, nested
  control flow on a single host)
- Owner: `internal/journal` (`waits.go`, the `journal_waits` table)
- Builds on: ADR 0003 (embedded SQLite store, schema versions #291)

## Context

The SQLite journal stores a run's waits (`journal_waits`) and the signals
sent to it (`journal_signals`). Until #332 a wait was unique per run and
name: `UNIQUE (run_id, name)`. A workflow that waits for "approval" inside
a loop, or in two steps, needs one wait per iteration or step. The second
`ScheduleWait` failed with SQLite's raw `UNIQUE constraint failed` instead
of `ErrWaitExists`, and a signal meant for it was found the first, closed,
wait and was recorded late (the E07 audit's defect D2, reproduced on
origin/main aaf633c in the #332 slice A PR).

The engine already describes a wait by the step that waits
(`engine.WaitIdentity.Step`), and the journal already identifies an effect
by its step and iteration (`OperationIdentity.InvocationPath`,
`IterationPath`). Nested control flow (#333) adds iterations on a single
host, so a wait must be told apart by iteration as well as by step.

## Decision

**Identity.** A wait is identified by its run, the step that waits and the
iteration it waits in: `UNIQUE (run_id, invocation_path, iteration_path)`,
the same pair of paths `OperationIdentity` uses for an effect. Both paths
are required by `ScheduleWait`. The wait ID stays the primary key. The
name is no longer unique: it is what a signal addresses.

**Duplicates.** A `ScheduleWait` that reuses a wait ID, or a step and
iteration the run has already waited at (open or closed), returns
`ErrWaitExists` and writes nothing. The insert has no conflict target, so
both constraints resolve the same way; no raw SQLite error escapes.

**Signal routing** (decided by the user on 2026-10-07 after Review R
round 1: "FIFO + targeted"). A signal is addressed either by name or to
one wait.

- *By name* (`Signal`): a queue. The signal goes to the run's oldest open
  wait of its name. With none open it is pending, whatever waits of the
  name have closed before, and each later wait of the name takes the
  pending signal that arrived first. A loop that waits on "approval" in
  every iteration therefore receives every approval sent before, between
  or during its iterations, one per iteration, in the order they were
  sent. A backlog is not skipped: if two signals arrived before the first
  wait and a third between the first and second waits, the second wait
  takes the second signal and the third wait takes the third.
- *To one wait* (`SignalWait` with a `WaitTarget`: the wait ID, the step
  and iteration paths, or both, which must name the same wait): only that
  wait, never another. It is delivered when that wait is open and late
  when it has closed. When the run has no such wait of the signal's name
  (not scheduled yet, another name, or an ID and paths of different
  waits) it is refused with `ErrNotFound` and nothing is stored, as
  `internal/cluster`'s `DeliverSignal` refuses a signal for an unknown wait
  ID (`ErrWaitNotFound`): the sender retries once the wait exists, and the
  same signal ID is then delivered.
- *Late* otherwise means the run has ended: completed, failed, canceled or
  uncertain. `accepted` is the journal's only live run state, and no
  transition leads back to it. A signal to an ended run is recorded late,
  whether addressed by name or to a wait. A signal to a run the journal
  does not hold is `ErrNotFound` and stores nothing.
- A retry with a signal ID the run already has is a duplicate, reported
  with the first send's outcome, under any concurrency (one transaction
  per signal, writer-first, as before).

"Oldest" and "arrived first" are insertion order (SQLite's `rowid`), not
creation timestamps: the clock is injectable and may tie or step back, and
ties must not fall to ID text order ("loop[10]" before "loop[9]"). The
earlier rule of slice A's first revision, late when any wait of the name
had closed, contradicted the queue: a signal sent between two iterations
was lost while an older pending one was delivered (the reviewer's probe,
now a test). A sender that means one specific wait addresses it instead.

**Inspection.** Inspection still shows a run's waits as one step per name
(`wait:<name>`): its latest open wait, or its latest wait when none is
open, instead of an arbitrary one, so a suspended run does not show its
only wait step completed. Per-iteration inspection of waits belongs with
#333's iteration paths.

**Schema version 4.** The journal's schema version rises from 3 to 4
(ADR 0003's table). A journal found at 3 or older is migrated in its schema
transaction: `journal_waits` is rebuilt (SQLite cannot drop a table
constraint) with every row and column it had, and the old unique index is
gone. A wait written before #332 has no step identity: its paths are NULL,
which never collides with another wait (SQLite treats NULLs as distinct)
and which a new wait cannot use (both paths are required). A `CHECK` keeps
the two paths both NULL or both set. A new index on `(run_id, name, state)`
serves signal routing, which the old unique index served. A shape with
`journal_waits.iteration_path` and no stamp is classified 4. Binaries at
version 3 refuse a version-4 journal at open (#291); binaries from before
#291 cannot see the stamp, and can still insert a wait with NULL paths,
which the new table accepts as a legacy wait. Such a binary, for example an
old process still running during a rolling upgrade, also still routes a
signal by `(run_id, name)` alone and reads an arbitrary row once a name
repeats, with its own pre-#332 late rule: the same convention limit #286
accepted for pre-#291 binaries (ADR 0003, Limits).

## Wakeup durability (slice B)

**The defect (D1).** A timer claim (`ClaimDueWaits`) or a signal moved a
wait straight to `resumed`. If the process died before the run was
resumed, nothing could find that wakeup again: the wait was no longer due,
the signal was no longer pending, and the run stayed `accepted` forever
(the E07 audit's probe, reproduced on origin/main 83bd1bf in the slice B
PR: after claim and reopen, `reclaimable=0`, wait `resumed`, run
`accepted`). A coat check analogy: the attendant tore up the ticket when
the owner's name was called, before anyone had handed the coat over.

**States.** A wait is `waiting`, then `fired` (claimed by its timer, or
given a signal, including a pending one at schedule time), then
`acknowledged`; or `canceled`. Firing records `fired_at`. A fired wait is
a wakeup the run has not consumed yet, and it stays fired until
`AcknowledgeWait`, which the engine calls once it has committed the step
the wait resumed (slice C wires that). Acknowledging again is a no-op
under the current lease token, and `ErrLeaseLost` under any other; a wait
that has not fired is `ErrWaitNotFired`.

**Run leases** (Review R round 1 on #366, orchestrator decision: the
lease is on the run, so slice C and #333 have one executor per run; rounds
2 and 3: fenced by a journal-wide token per acquisition). Each `Journal` has a holder name
(`Config.Holder`, which must be unique per process; random by default) and
a lease length (`Config.WakeupLease`, 30 s by default). A run lease is
`lease_owner`, `lease_until`, `leased_at` and `lease_token` on
`journal_runs`. A run is held while `lease_until` is after the `now` a call
is given, whoever holds it, the caller included: a holder that wants to
keep a run renews it, never takes it again.

Every acquisition (a claim, a resumption listing, `TakeRunLease`) draws the
next value of one journal-wide sequence (the `lease_token` counter in
`journal_meta`, which compaction never resets) and stores it as
the run's `lease_token`, returned in `WaitRecord.LeaseToken` or by
`TakeRunLease`. Renewing, releasing and acknowledging (an acknowledged
retry included) require the run's current token, so an execution whose
lease was taken over, by another holder or by the same holder again (its
own sweep after a stall, or after another holder took the run and died),
gets `ErrLeaseLost` and must stop, and a token from one run never passes on
another. The token is a fencing counter, not a capability: it is not
secret, and it orders acquisitions; it does not authenticate the caller.

- `ClaimDueWaits(now, limit)` fires due waits of live (`accepted`) runs,
  and takes the lease of each of their runs that is not held. It returns
  the waits of the runs it leased: the caller executes those runs. A wait
  of a held run still fires, but is not returned, even to the holder that
  holds the run: it stays fired for that holder's execution, or for
  `PendingResumptions` once the lease lapses or is released. A due wait of
  an ended run does not fire.
- A signal fires its wait and does not touch the lease: the signaller does
  not execute runs.
- `PendingResumptions(now, limit)` takes, in one write-first transaction,
  the lease of up to `limit` live runs that have a fired wait and are not
  held, and returns their fired waits. Runs are served in the order they
  last got attention: the later of their last lease (`leased_at`, on the
  journal's clock; none counts as earliest) and their first pending wakeup
  (`fired_at`). A run whose resumption never finishes and a stream of newly
  woken runs therefore take turns, neither starving the other, and a run
  leased long ago that wakes again queues behind runs that woke before it.
  Run it on start and periodically.
- `TakeRunLease(runID, now)` leases a run its holder starts executing
  without a wakeup, a run just admitted for example, so the waits it
  schedules are not handed to another holder while it runs. Admission does
  not take it (slice C's runner does). A held run is `ErrLeaseLost`; an
  ended one `ErrRunNotActive`.
- `AcknowledgeWait(waitID, token)` consumes a wakeup under the run's
  current lease (`ErrLeaseLost` otherwise) and leaves the lease alone: the
  holder is still executing the run.
- `RenewRunLease(runID, token, now)` extends the lease to at least `now`
  plus the lease length; it never shortens it. `ReleaseRunLease(runID,
  token)` gives it up so the next holder may take the run at once; a
  holder releases it when the run suspends at a wait or ends.

The `now` given to these calls is the clock leases are measured on;
callers pass the same clock to all of them.

**Inspection.** A fired wait's step shows `running` (inspection/v1 has no
"woken" status) until it is acknowledged, or its run has ended, when it
shows `completed`.

**Migration (schema 6).** `fired_at` is added to `journal_waits` and
`lease_owner`, `lease_until`, `leased_at` and `lease_token` to
`journal_runs`, gated on the version found. Every `resumed` wait becomes
`fired` when its run is live, so origin/main's stranded wakeups are listed by `PendingResumptions`
after the upgrade, or `acknowledged` when its run has ended. That remap is
not gated on the version: like #286's tenant repair it runs on every open,
because a binary from before #291 cannot see the stamp and can still
write `resumed`; it only reads unless such a wait exists, so reopening a
migrated journal never waits for the write lock. There is no unstamped
shape for 6: every database with these columns was written after the
stamp existed. A schema-5 binary refuses a schema-6 journal (#291).
Version 5 is #334's scope attempt (PR #351), which merged first; this step
was renumbered from 5 to 6.

## Engine journal (slice C1)

`(*Journal).ForRun(runID, token)` returns a `RunJournal`, the engine's
`StepJournal` and `WaitJournal` for one run executed under that lease
token. A run's steps are identified as the journal identifies effects and
waits: the invocation path is the engine's step ID, and the iteration path
is `root` until #333 gives the engine iteration paths (ADR 0028 defines
both; a top-level step's invocation path is its ID).

- **Lease and input.** `VerifyRun` refuses a run that is not live, whose
  artifact differs, or whose lease is no longer the token's
  (`ErrLeaseLost`). A run's engine input (the engine's digest of the input
  its runner hands it, a typed decode of the admitted JSON re-encoded) is
  fixed once: at admission when the admitter knows it
  (`AdmissionRequest.EngineInput`, the decoded value itself, which `Admit`
  digests with the engine's own `engine.InputDigest`, so no caller
  encoding can differ from the engine's; a value `json.Marshal` refuses is
  refused at admission), otherwise by the run's first execution
  (`journal_runs.engine_input_digest`, schema 7). A later execution with
  another engine input is refused, and so is a repeat admission of the
  request key naming another engine input than the one fixed (Review R
  round 3 on #380: caller bytes with other whitespace, HTML escaping, key
  order or `\u` escapes than `json.Marshal`'s fixed an identity no
  execution could match). The admitted bytes are
  never compared with a re-encoding: a run admitted with valid input always
  runs, whether its typed decode reorders fields, keeps an integer beyond
  float64's exact range, drops an unknown field or zero-fills an optional
  one (Review R round 2 on #380 and round 1 on #384: each of those could
  never run).
- **Permanent conflicts.** `Permanent(err)` reports the errors no retry
  can fix: `ErrRequestConflict` (another engine input, step input or wait
  plan), `ErrWaitCanceled`, `ErrStepResultLimit`. A runner settles such a
  run as failed with a diagnostic; a lost lease and storage faults are not
  permanent. The engine still labels them `persistence`; the journal's
  sentinel is what classifies them (#333 is reworking the engine's run
  loop).
- **Calls.** A call with declared effects is recorded as dispatched before
  the node runs, in one transaction, and committed after; one found
  dispatched on replay is marked uncertain and the run fails as uncertain,
  never charging twice. A call with no effects journals nothing before it
  runs (running it again is safe) and its result is committed in one
  transaction. Every step's operation records the engine's digest of the
  step's input (`journal_operations.input_digest`, schema 7): a replay
  that resolves a different input at the same step (a later loop
  iteration, or an upgrade that resolves inputs differently) is
  `ErrRequestConflict`, never served the recorded result. Step results are
  bounded by `MaxStepResultBytes` (1 MiB, as a run's output). The engine
  already refuses a node result over 1 MiB as `invalid_output`, except for
  a node whose output schema is an open object; there the journal refuses
  it with `ErrStepResultLimit`, which the engine reports as
  `journal_step_complete` (class `persistence`); it is permanent (above).
- **Waits.** `Await` looks the step's wait up by run, invocation path and
  iteration path, and schedules it the first time. Its ID is a digest of
  an explicit encoding of the run, artifact, step, the engine's digest of
  the wait plan (name and timeout) and the iteration path, owned by the
  journal (`wait/v1`, pinned by a test vector; not of the engine's
  identity struct, whose encoding could move with a field rename), so
  every replay reads the same wait and a changed plan at the same step is
  `ErrRequestConflict`. The engine's digest of the wait plan is of
  `contract.WaitInstruction`'s JSON encoding: a new field without
  `omitempty` would change every suspended run's wait ID, which a test
  also pins. A wait without a timeout
  is due at the end of time: only a signal fires it. Waiting suspends the
  run; fired or acknowledged returns the stored outcome (the signal, or a
  timeout when no signal fired it); canceled is `ErrWaitCanceled`.
- **Acknowledgement.** A fired wait's outcome read by `Await` is
  acknowledged in the transaction of the next durable progress: the next
  step's result, the schedule of the next wait (a run waiting at b right
  after a is woken for b only, not again for a), the run found still
  waiting at a wait, or the run's end through `RunJournal.CompleteRun`,
  `FailRun` or `MarkRunUncertain`. Never before: a crash between reading
  the outcome and that progress leaves the wait fired, the run is listed
  again once its lease lapses, and the replay reads the same outcome.
  `Journal.CompleteRun` refuses a run with a fired, unacknowledged wait
  (`ErrRunActiveWork`); `Journal.FailRun` does not, because a failure ends
  the run whatever woke it.

The engine's `StepJournal` has no hook for a wait step's own commit, so a
wakeup is consumed with the step after it rather than with the wait step;
the effect is the same, since the wait step's output is the stored
outcome and replays identically.

## Identity encoding (#382)

A suspended run finds its step results and its waits again, after a
restart or an upgrade, by IDs derived from its identity. Like a locker
number, an ID must name one locker, the same one every time: the
iteration is part of it, and nothing but the identity's values moves it.

- **Iteration.** `engine.StepIdentity.IterationPath` is the iteration a
  step runs in (ADR 0028's iteration path); empty and `root`
  (`engine.RootIteration`) are the same root iteration. The SQLite
  journal's `RunJournal` keys a step's operation and its wait by it (it
  put every step at `root` before). `internal/cluster`'s
  `WaitIDFor(runID, stepID, iterationPath)` includes it, and so does the
  operation key that addresses the cluster's step records, so one wait or
  call step reached in two iterations of a loop is two records with
  independent outcomes. The cluster step journal refuses an identity
  whose operation key is not the one its fields derive.
- **Root is unchanged.** A root-iteration wait ID is the digest of
  `runID \x00 stepID`, as before #382; another iteration appends
  `\x00 iterationPath`. A root-iteration operation key is the digest of the
  bytes `json.Marshal` produced for the untagged `StepIdentity` before
  #382; another iteration adds `"IterationPath"`. Every wait, step record
  and operation already stored keeps its ID: no migration, and no journal
  schema change.
- **Explicit encodings.** What is hashed or stored never follows a Go
  field name. `engine.OperationKey` hashes its own versioned encoding
  (`operationKeyV1`), not `StepIdentity`; `StepIdentity` (stored in every
  cluster step record) and `journal.OperationIdentity` (hashed into every
  journal operation key) carry JSON tags spelling the names `json.Marshal`
  used before. Rules: never change a tag; a field added to a stored
  identity is `omitempty`, and its zero value means what every record
  written before it meant; a new input to a key or wait ID needs a new,
  explicitly versioned encoding, never an edit of an existing one. The
  journal's wait ID (`wait/v1`, above) already followed this.
- **Comparison.** Stored identities are written and compared with the
  root spelled empty (`StepIdentity.Canonical`, `SameExecution`), so a
  record written before #382 is the same execution as the engine's root
  step.
- **Grammar.** Both encodings are unique only over ids that follow the
  grammar (`contract.IDPattern`, `^[a-z][a-z0-9_-]{0,63}$`): the cluster
  wait ID joins with NUL bytes (step `b\x00a[1]` at the root would share
  an ID with step `b` in iteration `a[1]`), and `json.Marshal` maps
  invalid UTF-8 to U+FFFD (`x\xfe` and `x\xff` would share an operation
  key). Lowering enforces the grammar, and the engine's program check now
  refuses any other step id (`invalid_step_id`), at the top level or in an
  arm, before running anything, whoever built the program (#382 Review R
  round 1). Iteration paths are built by the engine from those ids
  (ADR 0028).

Pinned by `TestStepIdentityEncodingIsPinned` (`internal/engine`),
`TestOperationKeyFormatIsPinned` and the existing `TestWaitIDFormatIsPinned`
(`internal/journal`), and `TestWaitIDFormatIsPinned` (`internal/cluster`),
each with vectors taken from origin/main a3d90d2. The engine and journal
vectors failed on a3d90d2 when a Go field of the identity was renamed
(`ArtifactDigest`, `InvocationPath`) and pass with the same rename after
#382. The engine does not pass iteration paths yet (#333 slice 2 does);
ADR 0028 has a durable runner refuse control flow until then.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| `WaitRequest` requires `InvocationPath` and `IterationPath` | API, breaking for `internal/journal` callers (no caller outside tests) | Pass the waiting step's paths |
| `WaitRecord` carries `InvocationPath`, `IterationPath` (empty for a legacy wait) | API, additive | None |
| A run may wait on one name more than once; a signal by name goes to the oldest open wait of its name, else waits in a first-in, first-out queue for the next | behavioral | None |
| A signal by name is late only when the run has ended (before: whenever a wait of its name had closed); a signal to an unknown run is `ErrNotFound` (before: stored pending) | behavioral | None |
| `SignalWait` and `WaitTarget` address one wait | API, additive | None |
| A duplicate step identity or wait ID returns `ErrWaitExists`, not a raw constraint error | behavioral | None |
| Journal schema 3 → 4 | schema, one-way | On open, in the schema transaction, killed or not (see Evidence) |
| Wait states `resumed` → `fired` / `acknowledged`; run leases; `ClaimDueWaits` fires only waits of live runs and returns only those of runs it leased; `PendingResumptions`, `TakeRunLease`, `AcknowledgeWait`, `RenewRunLease`, `ReleaseRunLease` (the last three take the lease token), `ErrWaitNotFired`, `ErrLeaseLost`, `Config.Holder`, `Config.WakeupLease`, `WaitRecord.LeaseOwner`/`LeaseUntil`/`LeaseToken` (the run's lease) (slice B) | API and behavioral; no caller outside `internal/journal` | Call `PendingResumptions` on start; `TakeRunLease` when starting a run without a wakeup; `AcknowledgeWait` after the resumed step commits; renew while executing, release when the run suspends or ends |
| `Journal.ForRun`, `RunJournal` (engine `StepJournal` and `WaitJournal`), `Journal.WaitAt`, `ErrWaitCanceled` (slice C1) | API, additive | None |
| `Journal.CompleteRun` refuses a run with a fired, unacknowledged wait (slice C1) | behavioral | Acknowledge the wait (`AcknowledgeWait`, or a `RunJournal` commit) before completing |
| Journal schema 5 → 6 (slice B) | schema, one-way | On open; `resumed` waits remapped on every open |
| Journal schema 6 → 7: `journal_operations.input_digest`, `journal_runs.engine_input_digest` (slice C1) | schema, one-way | On open, `from < 7`; operations recorded before it have no digest and are not compared; a run's engine input is fixed by its next execution |
| `RunJournal.MarkRunUncertain`, `AdmissionRequest.EngineInput`, `Permanent`, `MaxStepResultBytes`, `ErrStepResultLimit` (slice C1) | API, additive | None |
| `engine.StepIdentity.IterationPath`, `RootIteration`, `NewStepIdentity`, `OperationKey`, `Iteration`, `Canonical`, `SameExecution`; JSON tags on `StepIdentity` and `journal.OperationIdentity` with the names already used (#382) | API, additive; stored bytes unchanged | None |
| `cluster.WaitIDFor` takes the iteration path; root-iteration IDs unchanged (#382) | API, breaking for its callers (tests only) | Pass `""` (or `engine.RootIteration`) for a step outside every loop |
| `RunJournal` keys steps and waits by the identity's iteration path; the cluster step journal refuses an identity whose operation key its fields do not derive (#382) | behavioral; the engine passes the root iteration until #333 | None |
| The engine refuses a program whose step id does not match `contract.IDPattern` (`invalid_step_id`, class `configuration`) (#382) | behavioral, fail closed; every lowered program already matches it | Rename the step to an id of the grammar |

## Evidence

In the #332 slice A PR, all RED on origin/main or under a named mutation
of the change:

- `TestRunWaitsOnOneNameInSuccessiveIterationsAndSteps`,
  `TestDuplicateWaitIsErrWaitExists` (`internal/journal`).
- `TestOriginMainWaitsMigrateToStepIdentity`: the migration of a database
  origin/main itself wrote at aaf633c
  (`testdata/restore/wait-identity-332`, generated by its `generate.go`
  from a checkout of that commit), with every row compared before and
  after, and every legacy wait state used after the migration.
- `TestWaitIdentityMigrationSurvivesAKill`: a process killed (SIGKILL) in
  the schema transaction just before and just after its commit.
- `TestUnstampedStepIdentityShapeIsClassified4`.
- Review R round 1: `TestSignalsByNameQueueFirstInFirstOut` (the
  reviewer's probe), `TestLoopOfThreeIterationsReceivesEverySignal`,
  `TestSignalAfterTheRunEndsIsLate`,
  `TestConcurrentDuplicateSignalsDeliverOnce`,
  `TestSignalOrderIsInsertionOrderNotIDOrder`,
  `TestInspectionShowsTheOpenWaitOfAName`,
  `TestTargetedSignalReachesOnlyItsWait`.

Slice B: `TestClaimedWakeupSurvivesReopen` (the audit's D1 probe),
`TestAcknowledgeWait`, `TestPendingResumptionsLeasesEachRunOnce`,
`TestLegacyResumedWaitsBecomeFiredOrAcknowledged` (including the aaf633c
database's stranded claim and signal); after Review R round 1,
`TestRunLeaseOutlivesAcknowledgement` (the reviewer's two probes, renewal,
release), `TestAnyLiveLeaseHoldsTheRun`, `TestClaimSkipsEndedRuns`,
`TestPendingResumptionsIsFair`, `TestReopenDoesNotWaitForAWriter`; after
round 2, `TestLeaseTokenFencesStaleExecutions` (the reviewer's three
probes), `TestTakeRunLeaseHoldsARunThatNeverSuspended`,
`TestNewWakeupsDoNotStarveALeasedRun`; after round 3,
`TestAcknowledgedRetryNeedsTheCurrentToken`, `TestLeaseTokensAreJournalWide`,
`TestRewokenRunRanksByItsNewWakeup`. Slice C1:
`TestEngineSuspendsAndResumesThroughTheJournal`,
`TestEngineWaitTimesOutThroughTheJournal`,
`TestCompleteRunRefusesAnUnconsumedWakeup`,
`TestEngineReplaysAfterACrashMidResumption`,
`TestEngineEffectInterruptedByACrashIsUncertain` (the last two kill a real
process); after Review R round 1, `TestWaitAfterWaitIsNotWokenAgain`,
`TestStaleExecutionWritesNothing`, `TestStepResultIsBoundToItsInput`,
`TestRunJournalMarksARunUncertain`, `TestStepResultIsBounded`; after
round 2, `TestEveryValidInputRuns`, `TestEngineInputIsFixedOnce`,
`TestPermanentErrors`, `TestWritesAfterTheRunEndedAreRefused`,
`TestWaitIDFormatIsPinned`, `TestOversizeStepResultIsRecognizable`; after
round 3, `TestEngineInputFixedAtAdmissionRuns`,
`TestEngineInputMustEncode`, `TestRepeatAdmissionComparesEngineInput`.

#382: `TestStepIdentityEncodingIsPinned` (`internal/engine`),
`TestOperationKeyFormatIsPinned`, `TestLoopIterationsWaitIndependently`
(`internal/journal`), `TestWaitIDFormatIsPinned`,
`TestLoopIterationsWaitIndependently` (against a real three-voter etcd
cluster), `TestStepKeyMustMatchItsIteration` (`internal/cluster`); after
Review R round 1, `TestStepIDsMustFollowTheGrammar` (`internal/engine`),
and `TestEngineSuspendsAndResumesThroughTheJournal` with its siblings now
pin the root iteration of the engine's waits and steps in the journal.

## Limits

- Nothing calls `PendingResumptions` or `AcknowledgeWait` yet: slice C's
  single-host resumer does, and decides whether acknowledgement commits in
  the same transaction as the resumed step.
- A lease is time-based on the caller's clock: a holder that stalls past
  its lease without renewing it can be overtaken, and the run executed
  twice. The engine's step journal makes replay of committed steps
  idempotent; fencing the stale holder's later writes is not part of this
  slice.
- `RenewRunLease` and `ReleaseRunLease` still succeed under the current
  token after the run has ended. That is harmless (resumption listings and
  claims only consider live runs); slice C's runner releases the lease when
  a run ends.
- The lease token fences journal writes, not nodes: a stale execution's
  next node still runs (a pure node journals nothing first) before its
  write is refused and it stops. Effects are recorded under the lease
  before they dispatch, so a stale execution never dispatches one.
- The journal's own `Journal` methods (`ScheduleWait`, `MarkUncertain`,
  `MarkRunUncertain`, …) take no lease; only `RunJournal`, the engine's
  path, is fenced.
- Mixed versions: a binary from before this slice, still running during an
  upgrade, may be executing a run whose wakeup it marked `resumed`; the
  first open by this binary remaps that wait to `fired` and a holder may
  then list and execute the same run: a double-resume window that lasts
  until the old process stops. Upgrade by stopping old processes first.
- No single-host runner resumes runs through `RunJournal` yet: slice C2
  lists resumptions on start, executes them, renews and releases leases.
- Every step is at the `root` iteration until #333 passes iteration paths
  to the engine (ADR 0028 refuses durable control flow until then). The
  journals key by the iteration path the engine passes (#382), but no
  engine execution has exercised a non-root path yet: the two-iteration
  tests drive the step and wait journals directly.
- A cluster wait inside a parallel arm or other construct is still told
  apart by step ID only; ADR 0028's invocation paths are unique per step,
  so this holds while step IDs are unique per workflow.
- `internal/cluster`'s run suspension (`Runtime.suspend`) commits an event
  identified by run, step and owner token: a second suspension at the same
  step under the same owner (a loop's next iteration) is refused as
  already written and the run stays `running` (a probe in #382's PR).
  Durable loops on the cluster must identify it by iteration too.
- Effect inputs over `MaxInspectionInputBytes` are refused at dispatch, as
  `BeginEffect` refuses them.
- Pending signals of a run that ends stay pending (never late
  retroactively) until the run is compacted with them.
- A signal's duplicate check compares the signal ID only, not its
  principal or payload (unlike `internal/cluster`, which refuses a
  different payload under the same ID as a conflict).
