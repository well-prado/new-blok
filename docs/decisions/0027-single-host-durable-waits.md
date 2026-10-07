# ADR 0027: Single-host durable waits

- Status: in progress for E07-T09 (#332), delivered as stacked PRs. Slice A
  (this revision) records wait identity. Slice B (crash-safe wakeups) and
  slice C (the engine's `WaitJournal` on `internal/journal`) extend it
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

## Evidence (slice A)

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

## Limits (slice A)

- A claimed or signalled wakeup is still not crash-safe (defect D1): a
  wait flips to resumed with no lease. Slice B.
- `internal/journal` does not yet implement `engine.WaitJournal`, and the
  single-host runner does not resume from it. Slice C, which also chooses
  how the engine's `StepIdentity` and #333's iteration path map onto the
  two paths recorded here.
- Pending signals of a run that ends stay pending (never late
  retroactively) until the run is compacted with them.
- A signal's duplicate check compares the signal ID only, not its
  principal or payload (unlike `internal/cluster`, which refuses a
  different payload under the same ID as a conflict).
