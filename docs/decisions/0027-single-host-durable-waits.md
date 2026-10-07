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
is `root` until #333 gives the engine iteration paths (ADR 0031 defines
both; a top-level step's invocation path is its ID).

- **Lease.** `VerifyRun` refuses a run that is not live, whose artifact or
  input differs, or whose lease is no longer the token's
  (`ErrLeaseLost`). Every write a `RunJournal` makes runs in a transaction
  that first checks the lease (and, except for the run's end, that the run
  is live): recording a dispatch, marking one uncertain on replay or after
  a node failure, committing a step, scheduling or acknowledging a wait,
  and ending the run. A stale execution therefore changes nothing; only
  its next node runs before it learns, since a pure node journals nothing
  before it runs (Review R round 1 on #380).
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
  bounded by `MaxStepResultBytes` (1 MiB, as a run's output).
- **Waits.** `Await` looks the step's wait up by run, invocation path and
  iteration path, and schedules it the first time. Its ID is a digest of
  an explicit encoding of the run, artifact, step, the engine's digest of
  the wait plan (name and timeout) and the iteration path, owned by the
  journal (not of the engine's identity struct, whose encoding could move
  with a field rename), so every replay reads the same wait and a changed
  plan at the same step is `ErrRequestConflict`. A wait without a timeout
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

`internal/cluster`'s `WaitIDFor(runID, stepID)` addresses a wait by run
and step only. Once #333 runs a wait in a loop durably, every iteration
would map to one cluster wait, and the second iteration would read the
first one's outcome: it needs the iteration path (and the invocation path
inside an arm) before durable loops reach the cluster runtime. ADR 0031
(#333, in progress) has a durable runner refuse control flow until then.

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
| Journal schema 6 → 7: `journal_operations.input_digest` (slice C1) | schema, one-way | On open, `from < 7`; operations recorded before it have no digest and are not compared |
| `RunJournal.MarkRunUncertain`, `MaxStepResultBytes`, `ErrStepResultLimit` (slice C1) | API, additive | None |

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
`TestRunJournalMarksARunUncertain`, `TestStepResultIsBounded`.

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
  to the engine (ADR 0031, in progress, refuses durable control flow until
  then).
- Effect inputs over `MaxInspectionInputBytes` are refused at dispatch, as
  `BeginEffect` refuses them.
- Pending signals of a run that ends stay pending (never late
  retroactively) until the run is compacted with them.
- A signal's duplicate check compares the signal ID only, not its
  principal or payload (unlike `internal/cluster`, which refuses a
  different payload under the same ID as a conflict).
