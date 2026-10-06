# E07 audit 1 (#43–#46) — cloud session 2026-10-06, origin/main 018af7b

Method: detached worktree of origin/main, targeted packages run (all pass, incl. -race under heavy.lock); mutations applied and reverted per box.

## #43 (all boxes already checked)
- Raw results incl. durability settings: VERIFIED (data). Filesystem recorded only as "runtime temporary directory", no mount type.
- No ack of unflushed work: PARTIAL. store/sqlite/crash_test.go:13 TestProcessKillBeforeDuringAndAfterCommit (real SIGKILL child) passes. Mutation MN synchronous FULL→OFF (store/sqlite/sqlite.go:39): store/sqlite and internal/journal stay GREEN. No test asserts PRAGMA synchronous=2 per pooled connection.
- Native/cgo/port/backup: VERIFIED.
- Fixtures: testdata/store/fixtures.json is referenced only by the ADR; no test reads it.

## #44 (all boxes already checked)
Crash tests = real child-process Kill parked at Hooks.BeforeCommit/AfterCommit barriers (internal/journal/journal.go).
- Dedupe concurrency, crash barriers, replay/uncertain/retry key/lineage/stale writer, corruption: VERIFIED (journal_test.go:97, :243, :279, :149, :200, :427).
- Disk-full: PARTIAL — journal_test.go:457 uses a failingDatabase wrapper refusing before tx; no real ENOSPC/I/O injection.
- Synthetic fixtures: PARTIAL — testdata/store/journal-fixtures.json read by no test.

## #45 (all boxes unchecked) — main gap
- AC1 ack after durable transfer: VERIFIED at worker (trigger/worker/worker_test.go:171 real SIGKILL; mutation MA RED). Producer side (kill around Enqueue commit) missing.
- AC2 crash between acceptance and ack doesn't duplicate order: PARTIAL. MF (drop ON CONFLICT(request_key) DO NOTHING, examples/order/order.go:101) GREEN. No kill test drives order.Service.
- AC3 conflicting reuse fails: VERIFIED (worker_test.go:70; MB RED).
- AC4 bounded transient retries / no retry on validation: PARTIAL. MC RED; MD (retryable always true) conformance RED but TestInvalidOrderIsDeadLetteredWithoutBusinessRows GREEN (doesn't assert job state/attempt). Outbox DispatchOne retries unbounded; order handler never marks retryable → transient DB error dead-letters at attempt 1 (probe: state=dead attempt=1).
- AC5 order+outbox atomic: PARTIAL (behaviour correct by probe, untested). ME (swallow outbox insert error, order.go:109) GREEN.
- AC6 safe dead-letter diagnostics: VERIFIED (conformance internal-error; MG RED). HandlerError.Message app free text stored verbatim.
- V1 real persistence + dispatcher: VERIFIED (order_test.go:12, :82; MH RED). Only DispatchOne, no dispatcher loop.
- V2 kill producer/engine/dispatcher at handoffs: PARTIAL (only worker handler killed).
- V3 duplicates + provider timeout: PARTIAL ("DuplicateDelivery" never enqueues twice; timeout is errors.New).
- V4 exact rows after recovery: PARTIAL (order/outbox rows never counted after recovery).
- V5 fixtures: GAP (testdata/worker/order-fixtures.json read by no test; describes nonexistent test).
- V6–V8: PARTIAL.

## #46
- AC1 (checked) wait timestamps survive restart: PARTIAL. waits_test.go:14 reopen + DueAt readback. MM (ClaimDueWaits ignores due_at, waits.go:145) whole package GREEN. ML (no worker backoff) only census_test.go:227 red.
- AC2–4, AC6 (checked): VERIFIED at journal level (waits_test.go:14, :49, :77; MI/MJ/MK RED). Authorization is caller bool; contract/signal.Authorizer unused; only cancel-then-signal tested.
- AC5 (unchecked) thousands of suspended runs without goroutine each: GAP. internal/journal does NOT implement engine.WaitJournal/StepJournal; only internal/cluster (etcd) does. No single-host resume path. Probe: 5,000 waits, no goroutine growth, 5,000 wakeup burst drained in 137 ms (storage only).
- V1 kill around timer fire / signal consume: GAP (see D1).
- V2 signal before wait / concurrent twice: PARTIAL (probe: 16 concurrent dupes → 1 fresh + 15 dup; 50 signal-vs-timer races one winner; not on main).
- V3 fake clock DST/time jump: GAP. V4 footprint/wakeup bursts: GAP. V5 fixtures: GAP. V6–V8: PARTIAL.

## DEFECTS
- D1 HIGH: lost wakeup after crash. ClaimDueWaits (internal/journal/waits.go:139) and Signal flip wait to resumed with no lease / pending-resume record; nothing lists resumed-but-unfinished runs. Probe: claim, reopen → reclaimable=0, run still accepted → stranded.
- D2 HIGH: journal_waits UNIQUE(run_id, name) (internal/journal/journal.go:354) → a run cannot wait on the same name twice (loop / second approval); raw UNIQUE error instead of ErrWaitExists; matching signal classified Late.
- D3 MEDIUM: examples/order/order.go:130 DispatchOne read→external publish→write violates write-first rule (#176); concurrent writer → SQLITE_BUSY_SNAPSHOT (517) after successful publish (probe 3/3) → event re-published every pass, never marked sent.
- D4 LOW: contract/signal.Authorizer unused; journal.Signal takes caller authorized bool.

## Proposed work items (E07-T08+)
1. Order crash matrix + outbox hardening (M–L; fixes D3). Closes #45 AC2, AC4, AC5, V2, V3, V4, V5, V7. Child-process kill harness for examples/order (producer around Enqueue commit, handler mid-tx, dispatcher after publish before mark-sent); assert exact orders/order_outbox/worker_jobs rows + publish count. DispatchOne write-first (claim UPDATE…RETURNING, publish outside lock, mark sent) + outbox attempt/dead-letter budget; atomicity test (forced outbox PK conflict); real duplicate-enqueue test; TestInvalidOrder asserts dead at attempt 1; fixture-reading test. Files: examples/order/order.go, order_test.go, testdata/worker/order-fixtures.json.
2. Single-host durable waits (L; fixes D1, D2). Closes #46 AC5, V1, V2, V4, V5, AC1 restart half. Implement engine.WaitJournal (+StepJournal if in scope) on internal/journal; lease or fired→acknowledged state + pending-resume query; key waits by step/iteration identity (migration); kill tests at wait-claim/signal/wait-schedule barriers (reuse runTransitionChild); 10k suspended runs goroutine+RSS benchmark + wakeup burst benchmark. Files: internal/journal/waits.go, journal.go, waits_test.go, benchmarks/, ADR 0027.
3. Clock/time-jump tests for waits and retries (S). Closes #46 V3 + AC1 not-early; MM, ML RED.
4. Pin durability PRAGMAs per pooled connection (S); optional Linux tmpfs ENOSPC for #44 disk-full. MN RED.
5. Wire signal.Authorizer (S; D4) + signal-then-cancel ordering test.
6. Executable fixtures (S): tests reading journal-fixtures.json and store/fixtures.json (#43, #44, #45 fixture boxes).

## Backlog mapping
#330 → epic exit gate 2 + #44 crash-suite trust (20/20 -race pass here, not reproduced). #328 → #49 generic gates, epic gate 1. #316 → #49 compaction. #289 → #49 retention, epic gate 4. #294, #284 → #48/#49 audit/upgrade, epic gate 4. #301 → distributed only, not E07 single-host.
