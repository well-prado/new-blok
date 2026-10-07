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

**Signal routing.** A signal is delivered to the run's oldest open wait of
its name (by creation time, then wait ID), so two open waits of one name,
for example in parallel branches, take signals in the order they started
waiting. With no open wait of its name, a signal is:

- pending, when the run has never waited on that name: the next wait of the
  name takes it when it is scheduled, as before #332;
- late, when a wait of that name has closed. A signal addressed only by
  name cannot tell a stale delivery for the closed wait (a retried approval
  under a new signal ID) from an early one for a wait not yet scheduled;
  handing it to the next wait could approve something the sender never
  saw, while a late result is visible to the sender, who can send again.

**Inspection.** Inspection still shows a run's waits as one step per name
(`wait:<name>`), now its latest wait, instead of an arbitrary one. Per-
iteration inspection of waits belongs with #333's iteration paths.

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
which the new table accepts as a legacy wait.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| `WaitRequest` requires `InvocationPath` and `IterationPath` | API, breaking for `internal/journal` callers (no caller outside tests) | Pass the waiting step's paths |
| `WaitRecord` carries `InvocationPath`, `IterationPath` (empty for a legacy wait) | API, additive | None |
| A run may wait on one name more than once; a signal goes to the oldest open wait of its name | behavioral | None |
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

## Limits (slice A)

- A claimed or signalled wakeup is still not crash-safe (defect D1): a
  wait flips to resumed with no lease. Slice B.
- `internal/journal` does not yet implement `engine.WaitJournal`, and the
  single-host runner does not resume from it. Slice C, which also chooses
  how the engine's `StepIdentity` and #333's iteration path map onto the
  two paths recorded here.
- A signal arriving between one wait of a name closing and the next
  opening is late (see Signal routing). A signal addressed to a specific
  wait, rather than a name, is not part of this record.
