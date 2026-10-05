# Orchestrator state — 2026-10-05 (moved to cloud)

THIS BRANCH (`wip/orchestrator-handoff`) IS NOTES ONLY. NEVER MERGE IT. Delete it when done.

Goal: (1) merge every open PR; (2) finish epic #7 [E07] "Durable state, recovery and version retention".
Hard rules: see RULES.md here, plus the original handoff in the conversation. The main points:
- CI stays off: workflow 372722919 must stay `disabled_manually`.
- Merge only via `waiver-merge.sh <pr> <full-sha>`, which must print PROTECTION IDENTICAL.
- One issue per PR.
- Every merge needs an independent adversarial Review R.
- Every new test must be shown RED first.
- At most 4 agents at a time.

The local paths in RULES.md and E07-AUDIT.md (`/Users/...`, `/private/tmp/...`) are from the laptop. In the cloud, use your own checkout or worktrees and a private GOCACHE.

## Merged this session
- PR #314 (#68), merge commit 018af7b. #68 is Done.

## Open PRs
| PR | Issue | Reviewed head | State |
|---|---|---|---|
| #322 | #66 blok dev | 7206cde | Review R: REQUEST CHANGES (9 findings, below). Partial fixes are on branch `wip/66-review-fixes` (231d35e), stopped mid-work, UNVALIDATED. It has merged main (f136530) plus WIP edits to dev.go, deployment.go and new tests dev_review_unix_test.go (cmd/blok and devtool). Continue from it, finish, prove RED/GREEN, then push to `codex/66-dev-watch-reload`. |
| #326 | #320 WAL first open | 67b8d63 | Review R was interrupted before a verdict. Restart it. |
| #327 | #321 journal audit stamp | f6e7bc8 | Review R: REQUEST CHANGES (2 should-fix + 1 nit, below). Branch `wip/321-review-fixes` (e6ff0ee) has merged main and added a tripwire test commit. Full validation was NOT run, and the ADR wording fix (item 2) is probably not done. Verify, finish, push to `codex/321-journal-audit-stamp`. |

Not ours: #223 (Windows track), #112, #113 (dependabot).

### #322 findings to fix
1. A signal during a rebuild's stop phase still starts the new app (dev.go build() → stopApp → launch, with no l.ctx check).
2. First-scan cap failure leaves a dead watcher that uses 61% of a CPU. Make it fatal, back off mid-session, adapt the poll interval to scan time, and record the cost in ADR 0026.
3. A symlinked package directory is neither watched nor refused. Raise dev_symlink_unwatched.
4. A same-size edit with the same mtime is missed. Add inode and ctime on unix (Windows keeps the old stamp, documented).
5. An app crash during a build is reported as app-stopped, which makes TestDevFixtures/dev-crash-loop-backs-off flaky.
6. `nohup blok dev &` dies on SIGHUP. Respect signal.Ignored.
7. DECIDED by the orchestrator: keep every executable built in the session (at least the last 5) until the session ends. On a deterministic journal/artifact-incompatibility refusal, do not crash-loop; emit a new `dev_durable_incompatible` with remediation (revert / exact command to resume with the kept executable / finish or discard runs). Detect it through a structured signal, not by parsing English. Document in ADR 0026 and test end to end.
8. Missing tests: the emit-level redactDiagnostic (dev.go:220, load-bearing), redact on changed paths, and the 20-path changed cap.
9. Nits: a session-lifetime guard so SIGKILL cleans the build dir; fix the README setsid overclaim; fix the PR body's "no executables behind" claim.

### #327 findings to fix
1. Prove "never reads audit rows" with a tripwire: a generated column on audit_records_v1.record that errors on read. Mutant M-c (check moved after the reads) must go RED.
2. ADR 0003/PR wording: pruned or unverifiable untenanted rows make every journal-only open with a newer audit stamp refuse, permanently. Correct the "a supporting binary repairs the row" claim, and add a test pinning it.
3. Nit: reword "mutation 1 proves ordering".

## Issues filed this session (all Backlog)
- #328: trigger/worker retention tests flake under -race at high load.
- #329: shop Teardown deletes a newer worker stamp.
- #330: inspect TestCrashRecoveryReconstructsRunFromJournal intermittent, possibly a real recovery race (E07-adjacent).

## E07 plan (epic #7)
Children #43–#49 are all CLOSED, but about 45 boxes are still unchecked: #45, #46, #47, #48 and #49 have open acceptance and validation boxes. The earlier audits kept the epic open because no independent specialist Review R was ever recorded. The epic says to close it only after a requirement-by-requirement audit.
1. Run the audit per E07-AUDIT.md: auditor 1 takes #43–#46, auditor 2 takes #47–#49. An audit started this session was stopped before reporting anything; restart both.
2. For each GAP/PARTIAL, file a new task E07-T08 and up (never renumber), implement it with real evidence (real process kills where a box says kill/crash; measured footprint for thousands of suspended runs), with one PR per task, each with Review R.
3. Run an independent specialist Review R of the whole epic's evidence (durability, security, compatibility, performance), and record it on #7.
4. Tick each child box with linked evidence (gh api PATCH on the issue bodies), then close #7.
Also consider #330 (recovery race?) and #301/#316/#294/#289/#284 if the audit maps them to E07 promises.

## Project board
Project 15, id PVT_kwHOAyzkKc4BlZqm. Status field PVTSSF_lAHOAyzkKc4BlZqmzhkGshw. Options: Backlog bff8901e, In progress 137e6238, In review 89403fb1, Done 2ccdddea, Blocked ef6a6955. #66, #320 and #321 are In review.
