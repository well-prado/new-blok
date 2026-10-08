# Orchestrator state — 2026-10-08 (session 3 → next session)

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

## Merged so far (main = f3ffd4d at handoff 2026-10-08; re-check `git log origin/main`)
Session 1: #327 #326 #322 #352 #350 #358 #349 #351 #363 #367 #370 #366 #369 #368 #376 #378.
Session 2 (10-07): #380 (T09 C1), #379+#383 (T10 slice 1), #387 (#294), #388 (T16 #339).
Session 3 (10-08): #393 (#372 child principal + cycles), #394 (#284 audit start marker; AUDIT schema 1->2, v0.1.0-alpha refuses upgraded DBs -> release note), #401 (#400 engine saturation test: test bug, forced ordering), #395 (#382 stable wait/step identity + iteration; engine refuses step ids outside the grammar).
Every merge: Review R + full gate on the tree that lands, recorded as a PR comment; waiver-merge PROTECTION IDENTICAL.
Closed E07 tasks: T08 #331, T11 #334, T14 #337, T16 #339, T17 #340, T20 #343. Prereqs closed: #372 #382 #284 #294.

## OPEN PRs / WIP at handoff — exact next action (verify heads with `gh pr view <n> --json headRefOid`)

| PR / branch | Task | Head | Status | Next |
|---|---|---|---|---|
| #384 | T09 C2a resumer | `e16b85254e1b4bb4bfd68e26bc9770edf184d50c` | Review R2 (delta 1fd524a..6654aa5) = REQUEST CHANGES; ALL findings 1–4f fixed + extra (saturated sweep no longer counts as a scan, closes #398's substance) — RED proofs in body; 4-pkg -race x10 at load 32–117 pass; contains main 69a1adf. Gate on 6654aa5 was green. | delta Review R3 `6654aa5..e16b852` → merge main → gate → merge; then close #398 if covered |
| #385 | T09 C2b benchmarks, `Fixes #332` | PR head `ce95507` (stale); progress on **`wip/332-evidence-r1` @ bab127c** (merges of #384 6654aa5 + e16b852, ADR conflicts resolved) | benchmarks NOT re-measured (load too high) | worktree `-332d`: fast-forward `codex/332-wait-evidence` to wip, merge latest #384/main; at load < ~6 re-measure old ce95507 vs new interleaved + 3-rep report; update README/JSON/ADR "Measured"; full Review R1 → retarget main after #384 → gate → merge → tick #46 boxes |
| #402 | #374 trigger/worker timing tests (test budget, not product) | `9af76ca56accb71ff0594caf01ec2b2f23f87f78` | Review R1 → 1 should-fix (row cap indistinguishable from time budget) FIXED (25ms+10ms cases; M12 RED); orchestrator approved delta. Reviewer's prod analysis: default busy timeout 5s, worst wait 0.57s under load → no action. Gate: all PASS except `go test` FAIL in **internal/tooling/devtool** (untouched; new flake → filed #406); race step not completed (gate stopped at shutdown) | re-run gate (or prove #406 unrelated on main + same tree) → merge |
| #403 | #399 cluster failover test (test bug: unrenewed hand-played owner lease) | `924ac8a4bd19f298e3bad0e1ef40d04c032085a8` | test-only; main failed 16/20 under load, PR 20/20; siblings hardened; NOT independently reviewed yet | Review R1 (orchestrator may review: test-only, it wrote none) → gate (+ cluster tests vs isolated etcd) → merge |
| #404 | T10 slice 2: durable If/Choose/Default/Compare/TryFinally (single host) | `bacad1c0bb6fdb1ab1a933fb4c3ecaf54c25e5ff` | Review R1 INCOMPLETE (stopped). Found so far: **SHOULD-FIX 1** engine.checkProgram doesn't enforce step-id uniqueness across arms (OperationKey omits InvocationPath → try/finally same id = same key); **SHOULD-FIX 2** `scopeConflict` (internal/engine/scope.go) not wrapped → `journal.Permanent` false, contradicts ADR 0028 (retries forever instead of FAILED); NIT ADR wording "execution canceled by caller (run not canceled)"; FOLLOW-UP open scopes after MarkRunUncertain block requireQuiescentRun (CancelRun/FailRun refuse runs mid-construct); NIT construct output >1MiB fails durably. Not run: mutations (PR's M1–M10, M47a, reviewer's), -race on crash tests | send findings to implementer (worktree `-333c`) → finish Review R1 (mutations) → gate → merge. Then slice 3 (plan in PR body / below) |
| wip/396-cluster-suspend-iteration | #396 | `9f5a5552a1b0196a300456522f59b1bc2312efc1` (worktree `-396`, local branch codex/396-cluster-suspend-iteration) | suspension keyed by run revision (`suspendTransitionID`); 2 acceptance tests RED on main / GREEN; other collision sites audited (safe) | add pinned vector test; upgrade-in-place probe on etcd; ADR 0027 (drop Limits entry); mutation (step back in id) RED; validate; open PR `Fixes #396` |
| #405 (issue) | cluster worker drops partition on first slow renewal (TTL/4, no retry) — PRODUCT bug | worktree `-405` (clean, at f3ffd4d) | design settled (validUntil = send time + TTL − TTL/4; AfterFunc cancel; retry renewals within validUntil; ADR 0019 + 0017 row) — see #399 agent's plan in this session; tests (a) slow keepalive keeps partition (RED on main), (b) lease really expires → give up before expiry, no effect after, (c) exact effect counts | implement |

## New issues filed 2026-10-08
#397 (child mismatch = existence oracle; + cycle-walk cost note), #398 (resumer saturated sweep; likely fixed in #384 e16b852), #399 (→ #403), #400 (→ #401, closed), #405 (partition renewal, product), #406 (devtool crash-loop fixture flake under load).

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
