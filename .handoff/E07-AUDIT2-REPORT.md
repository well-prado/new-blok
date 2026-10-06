# E07 audit 2 (#47–#49 + epic #7) — cloud session 2026-10-06, origin/main 018af7b
(Full report summarized; see E07-T08+ issues for the work items.)

## Headlines
- #47: nested-recovery is a journal storage API no engine calls. internal/lowering/lowering.go:80 rejects every non-call kind; internal/journal does not implement engine.StepJournal/WaitJournal (only internal/cluster/etcd does). No durable single-host run can contain Each/Choose/Try/Parallel nor resume through the engine.
- #48: mostly real tests. Gaps: provider lookup simulation (V3 GAP), upgrade-while-waiting (V4 PARTIAL), fixtures unread (V5 GAP). AC4 weakness: M48d (persisted replay_of blank) GREEN. AC5: M48g (CancelRun no-op) GREEN.
- #49: AC1 PARTIAL (M49a2 delete every run's waits/signals/checkpoints/scopes/children/joins → GREEN). AC3 PARTIAL (M49b/M49b2 skip source/restored integrity checks sqlite.go:397/:442 → GREEN). AC5 GAP (probe P8 real SIGKILL at compact barriers: behaviour correct). V1 GAP, V4 GAP (P11: ~365 B/run tombstone growth forever; journal_compacted/journal_artifacts never pruned), V5 GAP.
- Epic exit gates 1–5: all GAP. ROADMAP.md:56 M3 claim (nested branches/loops/joins/waits/signals/child runs recover) unmet single-host. PRs #150/#151/#152 have no reviews.

## Defects
- D5 HIGH CompleteScope (internal/journal/recovery.go) overwrites committed output; no completed guard/attempt fencing (P1).
- D6 HIGH RecordJoin/RecordChild upsert unconditionally: completed 2/2 join regresses to running 1/3; completed child replaced; child to nonexistent run accepted (P2, P3, P3b).
- D7 MED SaveCheckpoint accepts an artifact digest different from the run's admitted one and Recover accepts it (P4); StartScope/SaveCheckpoint succeed after CancelRun (P5).
- D8 MED PlanUpgrade/DecideUpgrade count only state='accepted'; uncertain runs on old artifact → AffectedRuns:0 (P6).
- D9 LOW-MED Restore opens source read-write (backup file modified, header 18:20 → WAL); no parent dir fsync after rename; failed post-rename integrity leaves bad destination blocking retry; kill mid VACUUM INTO leaves partial destination.
- D10 LOW inspect: reader connecting between admission and run.started gets spurious history_unavailable gap (#330 root cause).

## Backlog status
- #330: test race, not lost data. Deterministic repro: 300ms sleep before runner.Run in crash child → gap+snapshot frames. Child writes marker before run.started; subscriber gets ErrNotFound → AttachRecovered (inspect/events_http.go:180-196).
- #328: not reproduced (20/20 -race, load 8.8); timing-budget test issue.
- #316: real; mutations W1/W2/W3 to trigger/worker/retention.go:337-343 survive.
- #289: real; retention.go:141 selects only completed; P7.
- #294: real and pinned by a passing test contract/audit/reconcile_tenant_test.go:338-343 (must be inverted).
- #284: real by code reading (crossCheck contract/audit/journal.go:395-405; no audit start marker; Restore doesn't Verify).
- #301: distributed only. #329: real by code (examples/recipes/shop/recipe.go:223-240).
- inspect TestStalledSubscribersCannotBlockRunOrJournalTransitions fails 5/5 on Linux main: tcp_wmem autotune to 4 MB > ~261 KB stream, stalled readers never block (events_journal_test.go:201).
