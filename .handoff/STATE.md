# Orchestrator state — 2026-10-07 evening (laptop session → next session)

THIS BRANCH (`wip/orchestrator-handoff`) IS NOTES ONLY. NEVER MERGE IT.
Read in this order: STATE.md (this) → RULES.md → WORKER-BRIEF.md → ISSUES.md. `waiver-merge.sh` merges; `gate.sh` is the full local gate.
Ignore CLOUD-RULES.md and goenv.sh (cloud only).

## Goal
Finish epic #7 [E07] "Durable state, recovery and version retention".
**User priorities (2026-10-07):** T09 slice C (#332) and T10 (#333) first; then the rest of E07.
**User decisions recorded this session:**
- T10/T12 (single-host nested control flow) ARE in M3 scope — never document them as unsupported.
- Journal signals by name are FIFO-buffered and Late only when the run has ended; a sender can also target one wait (by wait ID or step+iteration). (ADR 0027)
- `v0.1.0-alpha` tagged and published as a GitHub pre-release on `79ee0a7` (verified by `go install …@v0.1.0-alpha` from the module proxy + `blok new` + test + serve). `blok version` still prints 0.0.0-dev → #375.

## Orchestrator decisions recorded this session (apply them; don't re-litigate)
- Journal schema versions: v4 #350 (wait identity), v5 #351 (scope attempt), v6 #366 (run lease + journal-wide lease token in journal_meta), **v7 = #380** (journal_operations.input_digest). Next free: v8.
- ADRs: 0027 = T09 (waits/signals/leases), **0028 = T10 control flow** (renumbered from 0031; 0029+ free).
- Run lease lives on `journal_runs` (owner/until/leased_at/token); every acquisition draws the next token from ONE journal-wide sequence; renew/release/acknowledge require the token; `TakeRunLease` for never-suspended runs; Admit unchanged.
- Join results (#370): positional slots — `Results` has exactly `Expected` slots, compact-JSON compared, a filled slot never changes/empties; a write that leaves a filled slot empty merges as a no-op; JSON-null branch results must be stored wrapped (ADR 0028 plans `{"output": …}`).
- A canceled scope stays canceled; scope completion fenced by attempt id (#351).
- #380/#384 round decisions: a run admitted with valid input must ALWAYS be runnable — run-input identity is defined on what was admitted (bytes or a canonical form computed once at admission, like internal/cluster), never on re-encoding a typed decode, exact for big integers; permanent conflicts (ErrWaitCanceled, ErrRequestConflict, journal_run_mismatch, ErrStepResultLimit) settle the run FAILED with a diagnostic, never retried; the resumer takes a worker slot BEFORE the lease and keys `running` by lease token; never-leased accepted runs are recovered after a grace period (admitter crash between Admit and Start).
- #383: `finally` runs on internal fail-fast cancellation; only a caller cancellation skips it. Retry-safety (#190) re-evaluated after the join.
- Stacked PRs: merge in stack order; a stacked child can be merged INTO its parent branch so one gate + one main merge covers both (done for #369→#368).

## Merged this session (main = f69df5f at the time of writing; re-check `git log origin/main`)
#327 #326 #322 #352 #350 #358 #349 #351 #363 #367 #370 #366 #369(via #368) #368 #376 #378.
Closed E07 tasks: T08 #331, T11 #334, T14 #337, T17 #340, T20 #343. Also #320 #321 #66 #359.
Evidence boxes ticked with links: #45 (14), #48 (AC4, V4), #49 (interrupted compaction, crash at snapshot/rename, corruption/disk-full). #47: evidence comment only (needs T10/T12).

## OPEN PRs at handoff (main = f69df5f) — exact next action for each
Verify heads with `gh pr view <n> --json headRefOid` before acting; nothing below has been merged.

| PR | Task | Head | Status | Next |
|---|---|---|---|---|
| #380 | T09 C1 engine adapter (journal **v7**) | `aac23383c19972ac2389ee3797661f9c52e5fc3c` | R1 changes + R2 approved; round-2 follow-ups now pushed (delta `003655d..aac2338`): engine input fixed ONCE (`AdmissionRequest.EngineInput` or first execution; `WithInput` removed), `journal.Permanent(err)` for ErrRequestConflict/ErrWaitCanceled/ErrStepResultLimit, fence run-state test, pinned wait-ID vector. Engine untouched. | **delta Review R round 3** of `003655d..aac2338` → gate on merged tree → merge |
| #384 | T09 C2a resumer `internal/resumer` | PR head `1fd524a` (stale, CONFLICTING) — fixes are on **`wip/332-resumer-r1` @ `1fd305e1ffc74b299b56b8996e1b8084aacf9f68`** | R1 REQUEST CHANGES; all findings implemented on the wip branch (B1 permanent conflicts fail with diagnostic, transient retries bounded by MaxRetries; B2 slot before lease, `running` keyed by token, Close honours deadline; S1 RenewEvery injectable; S2 lease from journal `WakeupLease()`; S3 never-leased runs recovered after one lease; S4 Sweep returns errors + `Config.OnError`) | (1) investigate ONE unexplained `-race -count=5` failure in the resumer timing tests (no output; not reproduced in 15 runs); (2) make the crowd-out test actually go RED without the live-lease filter (mutation in the wip commit message); (3) confirm J-M11 (decoder panic) RED; (4) fast-forward `codex/332-local-resumer` to the wip commit (normal push), update #384 body; (5) delta Review R `1fd524a..<new>` → gate → merge after #380 |
| #385 | T09 C2b benchmarks + wait fixtures, `Fixes #332` | `ce9550769d8e78b5564e683098420c7ff9ff92d1` | NOT reviewed; stacked on #384; its committed benchmark report was measured on the OLD resumer (`e536f2f`) | merge #384's final branch in; decide whether to re-measure (probably yes — resumer changed); full Review R round 1 → gate → merge; then tick #46 boxes (AC5, V1, V2, V4, V5 — map in #385 body; V3 → T13) |
| #379 | T10 1a: If/Choose/Default/Compare/TryFinally in memory | `70ad7641ac1565d3b23c47075dd90d9ceb53a7e6` | R1, R2 changes; round-2 fixes pushed (delta `6de9353..70ad764`): conversion only for construct-produced values, schema check before conversion, never invents values (B3); S5; N1–N3 | **delta Review R round 3** `6de9353..70ad764` → gate on merged tree → merge |
| #383 | T10 1b: Each/Parallel in memory (stacked on #379) | `e03e5d7836691a1745743d0ad3cf99e32acdeeca` | R1 changes; fixes pushed (merge `2a3a199` of fixed 1a + delta to `e03e5d7`): B-1 retry-safety re-applied after join, S-1 attempt-id hashing ≥48 chars + recorder match, S-2 finally on internal fail-fast, S-3 ADR names #382, nits | **delta Review R round 2** `ba9adfb..e03e5d7` (+ remerge of `2a3a199`) → when #379 merges, retarget to main (or merge #383 into #379's branch first) → gate → merge |
| #387 | #294: pruned audit record no longer resurrected | `06faed541eb3b8c4a156b0610fa2e22407be78ac` | R1 REQUEST CHANGES (no blockers): **S1** move the tombstone lookup BEFORE `r.Validate()` in `contract/audit/journal.go` (~158–160 vs ~181) — otherwise compacting a no-tenant row whose pruned record fails today's sensitivity rule now errors (main skipped it); **S2** first-time decision paths (`contract/approval/journal.go:158`, `internal/journal/reconcile.go:198`) must treat `inserted == false` as `ErrConflict` (a planted/stale tombstone silently suppresses a new record + its mirror); **S3** ADR 0021 Limits: repair is "run Prune" (resurrected records keep their original time; one Prune removes them — verified), and ideally ErrCorrupt names the record; nits: Verify ~4× slower (document/batch), test a record next to a tombstone of ANOTHER kind, soften "outlive retention indefinitely" | resume a fresh agent on branch `codex/294-pruned-audit-resurrection` (worktree `~/Projects/Personal/new-blok-294`) with these findings → delta review → gate → merge; file the two follow-ups listed in ISSUES.md "To file" |
| #388 | T16 #339 executable store/journal fixtures | `97aae21f5ace812e9308a273b0738e799b6cc5eb` | NOT reviewed (fixtures rewritten to `store/v2` / `journal/v2`; tests only) | full Review R round 1 → gate → merge → tick #43/#44 fixture boxes |

Stale remote branch `codex/332-wakeup-kill` (f178037) — superseded by #378; safe to delete.

## Next work after those (E07 queue)
1. T10 slice 2+ (durable control flow) — needs #382 first (cluster wait IDs need the iteration; stable identity encoding). Plan in ADR 0028 on the #379/#383 branches: slice 2 durable If/Choose/TryFinally (StepJournal paths, scopes `<invocation>@<iteration>`, M47a RED), slice 3 durable Each/Parallel (RecordJoin slots; see #386 for join-row cost), slice 4 Child (#372 first: child principal binding), slice 5 resume at startup.
2. T12 #335 nested crash matrix (after T10).
3. #284 audit start marker — NOW urgent: v0.1.0-alpha is released, so upgraded DBs with pre-audit decisions fail Verify.
4. T13 #336 fake-clock/time-jump tests (#46 V3), T15 #338 signal.Authorizer, T18 #341 provider result lookup, T19 #342 slices B–D (B #289 failed/canceled compaction; C #316 worker Compact re-check; D growth — **needs a user decision**: document+assert per-run tombstone cost vs tombstone retention; interacts with #364).
5. T21 #344 docs + ROADMAP M3 alignment (last), then close #7 only after: every task merged with Review R, #43–#49 boxes ticked with evidence links, and an independent specialist Review R of the whole epic recorded on #7.
6. Non-E07 follow-ups filed this session: see ISSUES.md.

## Known flaky tests (proven unrelated each time)
- #374 trigger/worker `TestCompactionWriteTransactionsAreTimeBounded`, `TestSlowHoldNeverBlocksWriters` (200ms budget under load). Re-run at lower load / on main to prove unrelated.
- #359 is FIXED (#376).

## Environment
- Go 1.27.1; Docker available (golang:1.27.1; `--tmpfs /mnt/small:size=4m -e BLOK_TEST_SMALL_FS=/mnt/small` gives real ENOSPC; `--cap-add SYS_PTRACE` for strace).
- CI workflow 372722919 stays `disabled_manually`; local gate is the CI.
- Port 8080 is taken by OrbStack on the laptop — scaffolded apps: `ADDR=127.0.0.1:18080`.
- Tetrix MCP is not relevant to this repo (and needs /mcp auth).

## Project board
Project 15, id PVT_kwHOAyzkKc4BlZqm. Status field PVTSSF_lAHOAyzkKc4BlZqmzhkGshw. Options: Backlog bff8901e, In progress 137e6238, In review 89403fb1, Done 2ccdddea, Blocked ef6a6955.
`gh project item-add 15 --owner well-prado --url <url> --format json --jq .id` then `gh project item-edit --project-id … --id … --field-id … --single-select-option-id …`.

## Worktrees on the laptop (all clean)
`~/Projects/Personal/new-blok-332b` (#380 branch), `-332c` (wip/332-resumer-r1), `-333` (#379), `-333b` (#383), `-294` (#387), `-339` (#388).
Agents from the previous session cannot be resumed — start fresh agents with WORKER-BRIEF.md and the PR/issue context.
