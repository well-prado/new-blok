# Orchestrator state — 2026-10-09 (session 4 → next session)

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
- Journal schema versions: v4 #350, v5 #351, v6 #366, **v7 = #380 (merged)**. Next free: **v8** (#394 did NOT use it; it bumped the AUDIT store schema 1→2 instead).
- ADRs: 0027 = T09 (waits/signals/leases), **0028 = T10 control flow** (renumbered from 0031; 0029+ free).
- Run lease lives on `journal_runs` (owner/until/leased_at/token); every acquisition draws the next token from ONE journal-wide sequence; renew/release/acknowledge require the token; `TakeRunLease` for never-suspended runs; Admit unchanged.
- Join results (#370): positional slots — `Results` has exactly `Expected` slots, compact-JSON compared, a filled slot never changes/empties; a write that leaves a filled slot empty merges as a no-op; JSON-null branch results must be stored wrapped (ADR 0028 plans `{"output": …}`).
- A canceled scope stays canceled; scope completion fenced by attempt id (#351).
- #380/#384 round decisions: a run admitted with valid input must ALWAYS be runnable — run-input identity is defined on what was admitted (bytes or a canonical form computed once at admission, like internal/cluster), never on re-encoding a typed decode, exact for big integers; permanent conflicts (ErrWaitCanceled, ErrRequestConflict, journal_run_mismatch, ErrStepResultLimit) settle the run FAILED with a diagnostic, never retried; the resumer takes a worker slot BEFORE the lease and keys `running` by lease token; never-leased accepted runs are recovered after a grace period (admitter crash between Admit and Start).
- #383: `finally` runs on internal fail-fast cancellation; only a caller cancellation skips it. Retry-safety (#190) re-evaluated after the join.
- Stacked PRs: merge in stack order; a stacked child can be merged INTO its parent branch so one gate + one main merge covers both (done for #369→#368).

## Merged so far (main = see `git log origin/main`; at handoff 2026-10-09 = merge of #415)
Sessions 1–3: see git history (#380 #379+#383 #387 #388 #393 #394 #401 #395 …).
Session 4 (10-09): #402 (#374) #403 (#399) #404 (T10 slice 2) #384 (T09 C2a resumer; closes #398) #409 (#406) #414 (#396) #412 (T10 slice 3, durable each/parallel) #415 (#405 partition renewal).
Every merge: Review R + full gate on the tree that lands, recorded as a PR comment; waiver-merge PROTECTION IDENTICAL.
T10 (#333): slices 1–3 merged; slice 4 = #423 (open); slice 5 = wip (below).

## OPEN PRs / WIP at handoff — exact next action

| PR / branch | Task | Head | Status | Next |
|---|---|---|---|---|
| #423 | T10 slice 4: durable Child (`Fixes #397`) | `91bb88e940d802adbf75ae551b2cd0ecc5d2947a` | NOT reviewed (reviewer stopped before starting). Design: StartChild one txn (admit child with id/request key derived from parent run+scope path, parent principal; RecordChild; scope decision {"child":id}; parent wait); child end settles parent + fires its wait; child waits refuse signals; MaxChildDepth 8 permanent; #397: fresh children only, `child_unavailable` identical diagnostics; resumer starts children. Touches internal/resumer/resumer.go (conflict expected with #421) | full Review R1 (6 focus areas in this session's prompt) → gate → merge |
| wip/333-durable-app | T10 slice 5: durable runtime in real apps (`Fixes #333`) | `c1bb06702b156400171666d66f4095fd1ad20250` (worktree -333f, stacked on #423) | Works: real-process SIGKILL+restart tests pass (mid-run, at wait, mid-loop, child, mixed build); scaffolded app run once. Decisions taken (USER/orchestrator should confirm): runtime in `execution` (execution.Durable / NewDurable / runtime.Dependency()), default journal `data/journal.db` relative to cwd, Run() returns output sync / suspended / accepted, new public `flow.Wait(builder,id,name,timeout) Ref[flow.Signal]`, control-program digest = sha256 of versioned encoding (decides ADR 0028's v2 artifact identity). Left: gofmt flow/wait_test.go; P9 test (Close drains resumer at deadline); ADR 0028 slice-5 + README; full validation; RED on #423 head | finish → PR after #423 merges → full Review R1 → gate → merge |
| #421 | #413 resumer backlog drain | `4f117f79369edcc12197a8efc23f42880f6a03c3` | Review R1 = REQUEST CHANGES (code sound): SHOULD-FIX the "every worker busy" path untested — M6 (no-free-worker sweep doesn't set `more`) and M8 (spin while all busy) survive; add a test: 1 worker held, 20 woken runs, release after 300ms → all settle promptly + Run uses ~no CPU while held. FOLLOW-UPs: woken runs starve due timers/scan (pre-existing ordering); transient retries can outpace scan pacing during a drain. Drain: 4 → 166 runs/s at defaults (2,000 woken) | relay to implementer (agent of #384/#385, worktree -413) → delta review → gate → merge (before #423 if possible; #423 then merges main) |
| #385 | T09 C2b evidence (`Fixes #332`) | `66638a551024548327fd27cc87c6326c3bf7f095` | All Review R1 findings fixed (footprint while sweeping, backlog arm, strict fixtures, p99 + #410 link, nits, resumer SIGKILL test). V4 NOT tick-ready until backlog arm re-measured after #421 merges (and #422) | delta Review R2 `2eb9da3..66638a5` → after #421 merges: merge main, re-measure backlog arm → gate → merge → tick #46 boxes |
| wip/411-cluster-suite-reliability | #407 + #411 cluster suite reliability | `16f2115165fb9450cafbf686074cb9dc5f879dcb` (worktree -411) | test-only; per-test root causes found (unrenewed hand-held leases → `holdOwner` helper; app/cluster near-limit: 503 retry only on owner change, progress-based wait, readiness retry). `TestSustainedLoadFailoverWithWorkerKill` = product behaviour on main (#405, now fixed by #415) — re-check on new main. Left: acceptance A/B, -count=10, quorum one-off, validation, PR | rebase onto main (has #415), validate, PR (`Fixes #407`, #411 Fixes/Refs) → review → merge |

## New issues filed 2026-10-09
#407 (cluster signal-CAS test lease), #408 (sqlite arrival-order test), #410 (run-to-first-wait 1.3–1.8× slower since ce95507 — bisect/profile), #411 (cluster suite load failures), #413 (→ #421), #422 (PendingResumptions scans every fired wait; drain slows with backlog). Dependabot PRs #416–#420 (dependency bumps) untouched.

## Orchestrator decisions this session
- Durable loops: one completed scope row per item/arm as its slot; no schema v8; join row not used (measured: join row 3 min / 1.54 GB at 10k items vs ~1.2 ms/item). Recorded on #386.
- Child: depth limit default 8 (`child_depth_exceeded`, permanent); child input = the child run's own admitted input.
- Journal schema v8 still FREE.

## Next work after those (E07 queue)
1. T10: slice 2 = #404 (open). Slice 3 durable Each/Parallel (plan: per-item scopes, RecordSlot join slots wrapped {"output":…}, skip filled slots on replay, fail-fast recorded in scope, measure #386 join-row cost at 1k/10k items; per-slot table would need journal v8 — ask first). Slice 4 Child (#372 done; see #397). Slice 5 resume at startup. #396 before durable CLUSTER loops (cluster wait IDs need the iteration; stable identity encoding). Plan in ADR 0028 on the #379/#383 branches: slice 2 durable If/Choose/TryFinally (StepJournal paths, scopes `<invocation>@<iteration>`, M47a RED), slice 3 durable Each/Parallel (RecordJoin slots; see #386 for join-row cost), slice 4 Child (#372 first: child principal binding), slice 5 resume at startup.
2. T12 #335 nested crash matrix (after T10).
3. Flaky-test class: #406 (devtool). The user wants every recurring failure root-caused and fixed, not re-proven each gate.
4. T13 #336 fake-clock/time-jump tests (#46 V3), T15 #338 signal.Authorizer, T18 #341 provider result lookup, T19 #342 slices B–D (B #289 failed/canceled compaction; C #316 worker Compact re-check; D growth — **needs a user decision**: document+assert per-run tombstone cost vs tombstone retention; interacts with #364).
5. T21 #344 docs + ROADMAP M3 alignment (last), then close #7 only after: every task merged with Review R, #43–#49 boxes ticked with evidence links, and an independent specialist Review R of the whole epic recorded on #7.
6. Non-E07 follow-ups filed this session: see ISSUES.md.

## Known flaky tests (proven unrelated each time)
- #374 → fixed by #402 (open). #400 fixed (#401). #399 → #403 (open). #406 devtool crash-loop fixture (open). Until fixed, prove unrelated (untouched pkg + rerun on main) and record it.
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
`-332c` (#384 branch content, wip/332-resumer-r1 = 6654aa5), `-332d` (#385), `-372` (#393), `-382` (#395), `-284` (#394). Merged-PR worktrees (-332b, -333, -333b, -294, -339) can be removed.
Agents from the previous session cannot be resumed — start fresh agents with WORKER-BRIEF.md and the PR/issue context.
