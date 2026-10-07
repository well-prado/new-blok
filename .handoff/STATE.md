# Orchestrator state — 2026-10-07 late (laptop session 2 → next session)

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

## Merged so far (main = a6984cb at handoff 2026-10-07 late; re-check `git log origin/main`)
Earlier session: #327 #326 #322 #352 #350 #358 #349 #351 #363 #367 #370 #366 #369(via #368) #368 #376 #378.
This session (2026-10-07 late), each with Review R + full gate recorded as a PR comment, then waiver-merge (PROTECTION IDENTICAL):
- #380 T09 C1 (journal v7) at 5dff4f8 (round-3 fix: `EngineInput any`, digested by the shared `engine.InputDigest`).
- #383 → folded into #379 (00e905f), then #379 merged = T10 slice 1 (#333 stays open for slices 2–5).
- #387 (closes #294) at 089cc51. #388 (T16, closes #339) at 97aae21; evidence comments on #43/#44.
Closed E07 tasks: T08 #331, T11 #334, T14 #337, T16 #339, T17 #340, T20 #343. Also #294 #320 #321 #66 #359.

## OPEN PRs at handoff — exact next action for each (verify heads with `gh pr view <n> --json headRefOid`)

| PR | Task | Head | Status | Next |
|---|---|---|---|---|
| #384 | T09 C2a resumer (`internal/resumer`), base **main** | `6654aa5f5f04c64885e45aa9f0d6ea6ef51b6825` | All R1 findings fixed; contains main a6984cb. The -race failures were 3 test races, now fixed (`56fc68d`, 280/280 at `-race -count=20`). Crowd-out test rewritten and RED without the live-lease filter; J-M11 RED. Targeted validation passes. Mutation matrix re-run is partial: S1/S2/S4/Settled-panic/M7 not re-run. **The PR BODY IS STALE** (still mentions WithInput, old Config, C2-M10 GREEN). | (1) rewrite the #384 body (What/Why/How + proofs) via gh api PATCH; (2) delta Review R `1fd524a..6654aa5` (+ remerge-diffs), including the missing mutations; (3) full gate → merge |
| #385 | T09 C2b benchmarks + wait fixtures, `Fixes #332`, base codex/332-local-resumer | `ce9550769d8e78b5564e683098420c7ff9ff92d1` (unchanged) | NOT reviewed; worktree `~/Projects/Personal/new-blok-332d` exists (clean) | merge 6654aa5 in (expect ADR 0027 conflicts; drop stale WithInput / TestTypedInputIsVerifiedAsTheSameJSONValue refs); re-measure the benchmarks at low load on the new resumer and update the README+JSON; full Review R1 → retarget to main after #384 merges → gate → merge → tick #46 boxes (AC5, V1, V2, V4, V5; V3 → T13) |
| #393 | #372 RecordChild principal binding + cycle refusal (any depth), base main | `8e78d7f93c9fe260b217f8ce88d2cba886c68ae4` | Review R1 INCOMPLETE (stopped at handoff): no blockers, leaning APPROVE; merges clean with a6984cb; tests and -race on importers pass on the merged tree. NOT done: mutation table, EXPLAIN/timing check. Nits: PR body wrongly claims ADR 0028 has a slice-4 note (reword or add one line to 0028 Limits pointing to ADR 0003 #372); ADR 0003 "not re-checked" vs a byte-identical repeat of an old invalid-JSON record now fails. Follow-up to file: ErrChildPrincipalMismatch vs ErrChildRunNotFound is an existence oracle once Child surfaces it — map to not-found at the API boundary in #333 slice 4. | finish Review R1 (mutations) → fix nits → gate → merge |
| #395 | #382 stable wait/step identity + iteration in cluster wait IDs, base main (a3d90d2) | `4e660dad36e9fe48b9f4a66357fa45a002840a82` | NOT reviewed. Existing IDs byte-identical (vectors pinned from a3d90d2); RED proofs on real 3-voter etcd; mutations M1–M6 RED; no schema change. Found #396. | full Review R1 (needs isolated etcd for internal/cluster, app/cluster) → merge main in → gate → merge. Unblocks T10 slice 2 |
| #394 | #284 audit start marker, base main a6984cb | `cc754b69914b50213742d972199ed1a5cef75077` | NOT reviewed. **Audit store schema 1→2 (journal v8 NOT used, still free).** Real fixtures from pre-#280 code and v0.1.0-alpha. 8/9 mutations RED. **USER-VISIBLE: v0.1.0-alpha binaries refuse an upgraded DB (NewerSchemaError); rollback = restore a backup.** Tell the user about this in the release notes. | full Review R1 → gate → merge |

## Next work after those (E07 queue)
1. T10 slice 2+ (durable control flow) — needs #382 (= PR #395) merged first, and #396 before durable CLUSTER loops (cluster wait IDs need the iteration; stable identity encoding). Plan in ADR 0028 on the #379/#383 branches: slice 2 durable If/Choose/TryFinally (StepJournal paths, scopes `<invocation>@<iteration>`, M47a RED), slice 3 durable Each/Parallel (RecordJoin slots; see #386 for join-row cost), slice 4 Child (#372 first: child principal binding), slice 5 resume at startup.
2. T12 #335 nested crash matrix (after T10).
3. #284 → PR #394 (open, see table).
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
`-332c` (#384 branch content, wip/332-resumer-r1 = 6654aa5), `-332d` (#385), `-372` (#393), `-382` (#395), `-284` (#394). Merged-PR worktrees (-332b, -333, -333b, -294, -339) can be removed.
Agents from the previous session cannot be resumed — start fresh agents with WORKER-BRIEF.md and the PR/issue context.
