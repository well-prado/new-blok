# Orchestrator state — 2026-10-06 (cloud session → back to laptop)

THIS BRANCH (`wip/orchestrator-handoff`) IS NOTES ONLY. NEVER MERGE IT. Delete it when done.

Goal: (1) merge every open PR; (2) finish epic #7 [E07] "Durable state, recovery and version retention".
Hard rules: RULES.md here (laptop paths valid again on the laptop). CLOUD-RULES.md + goenv.sh are only for cloud sessions.

## Ready to merge (run on the laptop; the cloud gh token was invalid, so nothing was merged)
Merge (independent; any order works, but re-check each head first). #322 is below:
```
bash .handoff/waiver-merge.sh 327 ef7a177f6cd72048e7e3c1dc3e99dd2ab408511e   # #321 journal audit stamp — Review R round 3: APPROVE
bash .handoff/waiver-merge.sh 326 9ce9a33c240d454fdb7f52a7391349e50799628a   # #320 WAL first open — Review R round 2: APPROVE
```
Each must print PROTECTION IDENTICAL. After merging: move #320/#321 to Done on Project 15, and merge origin/main into #322's branch if it is still open.
- #327 optional nits left as is (would need another review round): test name `…BeforeAnyAuditRowIsRead` slightly overclaims (discarded reads of tenant/digest columns aren't counted; ADR states the limit); helper comment says the rebuild "changes nothing a reader sees" (schema drops UNIQUE/CHECK/indexes).
- #326 non-blocking should-fixes filed as #346 (root re-exec on private TMPDIR fails instead of skipping; theoretical timer race in TestCancelledOpenStopsRetrying).

## #322 (#66 blok dev) — READY (Review R round 4: APPROVE)
```
bash .handoff/waiver-merge.sh 322 ebc8cecfb5039f6f981181b1f353f674ed5eb623
```
- History: 7206cde → b301012 (round-1 fixes) → d851b6f (round-2) → ebc8cec (round-3). The round-4 review was delta-only and approved.
- Full gate run once on ebc8cec: everything passes except inspect #345, which fails on main too.
- Non-blocking follow-ups filed as #347: absolute link targets written through a symlinked parent are refused (macOS /var vs /private/var), and the project_unreadable branch of importsOf is untested.
- Only on the laptop: TestDevNodeWorkerReload (npm is blocked in the cloud) and a real macOS run of the round-2/3 fixes, which so far were only vetted for darwin, not run. Run `go test -count=1 ./internal/tooling/devtool ./cmd/blok` on the Mac before or after the merge.

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
2. Add #331–#347 to Project 15 (Backlog). The cloud GitHub connector has no project tools. #320/#321 → Done after merge.
3. Merge #322 (command above), after running the devtool and cmd/blok tests on the Mac.
4. Restore stash 5ece76a in Deskree/blok if not yet done (`git stash apply 5ece76a`).
5. Start E07-T08+.

## Environment notes
- Use go1.27.1 (internal/package fixture runs go with GOTOOLCHAIN=local and needs 1.27.1).
- Cloud only: Go downloads need `proxy.golang.org` + `sum.golang.org` allowlisted AND `proxy.golang.org` removed from NO_PROXY (see goenv.sh). npm is blocked in cloud. Cloud runs as root.
- CI workflow 372722919 still disabled_manually (verified 2026-10-06).

## Project board
Project 15, id PVT_kwHOAyzkKc4BlZqm. Status field PVTSSF_lAHOAyzkKc4BlZqmzhkGshw. Options: Backlog bff8901e, In progress 137e6238, In review 89403fb1, Done 2ccdddea, Blocked ef6a6955.
