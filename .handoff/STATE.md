# Orchestrator state — 2026-10-06 (cloud session → back to laptop)

THIS BRANCH (`wip/orchestrator-handoff`) IS NOTES ONLY. NEVER MERGE IT. Delete it when done.

Goal: (1) merge every open PR; (2) finish epic #7 [E07] "Durable state, recovery and version retention".
Hard rules: RULES.md here (laptop paths valid again on the laptop). CLOUD-RULES.md + goenv.sh are only for cloud sessions.

## Ready to merge (run on the laptop; the cloud gh token was invalid, so nothing was merged)
Merge in this order (independent; any order works, but re-check each head first):
```
bash .handoff/waiver-merge.sh 327 ef7a177f6cd72048e7e3c1dc3e99dd2ab408511e   # #321 journal audit stamp — Review R round 3: APPROVE
bash .handoff/waiver-merge.sh 326 9ce9a33c240d454fdb7f52a7391349e50799628a   # #320 WAL first open — Review R round 2: APPROVE
```
Each must print PROTECTION IDENTICAL. After merging: move #320/#321 to Done on Project 15, and merge origin/main into #322's branch if it is still open.
- #327 optional nits left as is (would need another review round): test name `…BeforeAnyAuditRowIsRead` slightly overclaims (discarded reads of tenant/digest columns aren't counted; ADR states the limit); helper comment says the rebuild "changes nothing a reader sees" (schema drops UNIQUE/CHECK/indexes).
- #326 non-blocking should-fixes filed as #346 (root re-exec on private TMPDIR fails instead of skipping; theoretical timer race in TestCancelledOpenStopsRetrying).

## #322 (#66 blok dev) — NOT ready
- Head b301012 (round-1 fixes, all 9 findings addressed). Review R round 2: REQUEST CHANGES:
  - BLOCKER: finding 3 incomplete — watch.go:142 ignores links named `_x`/`testdata`/`node_modules` but Go builds through them (reproduced end to end). Only leading-`.` names are safe.
  - should-fix: harmless out-of-project links (LICENSE, docs) now block every build while `blok check` passes; scan-cost delay `ScanDuty*scanTook` is unbounded (3 s stall → 30 s); `scanFailures = 0` reset untested.
  - nits: several untested claims (backoff cap, ScanDuty, EvalSymlinks root, stopApp exited branches, resume redaction/quoting), signal-ignore claim only true for SIGHUP/SIGINT, macOS only vetted, PR evidence on go1.27.0.
- A cloud agent was fixing these when this note was written; see the "Update" section at the bottom (if absent, the agent didn't finish: check `git log origin/codex/66-dev-watch-reload` for commits after b301012, and the PR body).
- After the fix: independent Review R round 3, then merge with waiver-merge.sh.
- Node worker reload test (TestDevNodeWorkerReload) can only run on the laptop (npm blocked in cloud).

## Working rule (see RULES.md, "Keep changes small and validate the delta")
Small stacked PRs, delta-only review rounds, targeted tests while iterating, and the full gate once before merge. Apply it to all E07-T08+ work, and split T10 (#333) per construct.

## E07 (epic #7)
- Two requirement-by-requirement audits done on origin/main 018af7b: `.handoff/E07-AUDIT1-REPORT.md` (#43–#46) and `.handoff/E07-AUDIT2-REPORT.md` (#47–#49 + epic gates + backlog mapping).
- Verdict: epic cannot close. 10 defects (D1–D10), integration gap (single-host engine never uses internal/journal; lowering rejects control flow), several ticked boxes weaker than they look.
- Filed as sub-issues of #7, milestone M3 (labels type:task, review:specialist, area:durability):
  | ID | Issue | Notes |
  |---|---|---|
  | E07-T08 | #331 | order crash matrix + write-first outbox (D3) |
  | E07-T09 | #332 | single-host waits via engine, no lost wakeups (D1, D2) |
  | E07-T10 | #333 | nested control flow durable on single host (XL) — needs T09, T11 |
  | E07-T11 | #334 | fence recovery records (D5–D7) |
  | E07-T12 | #335 | nested recovery crash matrix — needs T10, T11 |
  | E07-T13 | #336 | fake-clock / time-jump tests |
  | E07-T14 | #337 | pin SQLite durability pragmas + real ENOSPC |
  | E07-T15 | #338 | signal.Authorizer (D4) |
  | E07-T16 | #339 | executable store/journal fixtures |
  | E07-T17 | #340 | upgrade counts uncertain/waiting runs (D8) |
  | E07-T18 | #341 | provider result lookup reconciliation |
  | E07-T19 | #342 | compaction evidence + growth bound (lands #289, #316) |
  | E07-T20 | #343 | crash-safe backup/restore (D9) |
  | E07-T21 | #344 | docs + ROADMAP M3 alignment (last) |
  Also linked as sub-issues: #284, #294. Epic body lists all of these with the dependency order; audit summary comment posted on #7.
- Start-now candidates (no deps): T08, T09, T11, T13, T14, T15, T16, T17, T18, T19, T20, #284, #294.
- Close #7 only after: all of the above merged with Review R, child boxes ticked with evidence links, an independent specialist Review R of the whole epic recorded on #7.

## Other issues filed / updated this session
- #345: inspect TestStalledSubscribersCannotBlockRunOrJournalTransitions fails every time on Linux (TCP send buffer autotune swallows the stream). Makes every Linux `go test ./...` red; passes on macOS.
- #346: store/sqlite test robustness follow-ups from #326 review.
- #330: commented root cause — test race (marker written before run.started), not lost data; deterministic repro with a 300 ms sleep.

## Laptop to-dos
1. Run the two merges above.
2. Add #331–#346 to Project 15 (Backlog). The cloud GitHub connector has no project tools. #320/#321 → Done after merge.
3. Finish #322 (see above) — Review R round 3, then merge.
4. Restore stash 5ece76a in Deskree/blok if not yet done (`git stash apply 5ece76a`).
5. Start E07-T08+.

## Environment notes
- Use go1.27.1 (internal/package fixture runs go with GOTOOLCHAIN=local and needs 1.27.1).
- Cloud only: Go downloads need `proxy.golang.org` + `sum.golang.org` allowlisted AND `proxy.golang.org` removed from NO_PROXY (see goenv.sh). npm is blocked in cloud. Cloud runs as root.
- CI workflow 372722919 still disabled_manually (verified 2026-10-06).

## Project board
Project 15, id PVT_kwHOAyzkKc4BlZqm. Status field PVTSSF_lAHOAyzkKc4BlZqmzhkGshw. Options: Backlog bff8901e, In progress 137e6238, In review 89403fb1, Done 2ccdddea, Blocked ef6a6955.
