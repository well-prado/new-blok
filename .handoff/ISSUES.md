# Follow-up issues filed during the 2026-10-07 session

All are labelled, on Project 15 (Backlog unless noted). None is an E07 sub-issue unless the epic body links it — decide per issue whether it gates E07 (most are E07-adjacent durability work).

| # | Title (short) | Source | Notes |
|---|---|---|---|
| #348 | ClassifyLink cleans `out/..` before resolving → escape | #322 R5 | blok dev / layout |
| #353 | Order outbox never compacts sent events | #349 R1 | |
| #354 | Order resubmitted after dedupe window with a different payload keeps old order | #349 R1 | |
| #355 | Old-binary mark-sent no-op on rows claimed by new binary | #349 R1 | relates #365 schema stamp |
| #356 | Reopen after full disk needs room for whole WAL (WAL-switch conn discarded) | #352 R1 | + synchronous constant + ADR line |
| #357 | Fence remaining recovery writers (CancelScope, CompleteScope after uncertain, joins/children after run end) | #351/#370 | |
| #360 | Uncertain runs can never be closed out → pin artifacts forever | #358 R1 | |
| #361 | Upgrade decisions not enforced at admission/drop; ActiveRuns fails open | #358 R1 | |
| #362 | Journal signal edge cases (pending signals of ended runs, dup conflicts, ErrNotFound ambiguity) | #350/#366 | |
| #364 | Journal Admit re-admits a request key after compaction | #363 R1 | real dedupe bug; interacts with T19-D tombstone decision |
| #365 | Order outbox due-row index + order schema stamp | #349 R2 | |
| #371 | Kill-test deploy example admission chain | #367 R1 | #45 V2 limit |
| #372 | RecordChild: bind child to parent's principal, refuse self-reference | #370 R1 | MUST land before T10's Child (slice 4) |
| #373 | Backup/Restore no-replace rename + temp sweep | #368 R1 | |
| #374 | trigger/worker timing tests flaky under load | gates | known flake; prove unrelated each time |
| #375 | `blok version` module version; ROADMAP §1; README install; requestId on schema errors | v0.1.0-alpha | small, user-visible |
| #377 | Backup/restore test gaps (pre-rename recheck, hot -journal, child output) | #368 R2 | |
| #381 | Control-flow usability ($input/literal arm results; cluster refuses control programs at registration) | #379 R1 | partly done in #383 ($input operands) |
| #382 | Cluster wait IDs need the iteration; stable identity encoding | #380 R1 | MUST land before T10 slice 2 (durable loops) |
| #386 | Run-level step/concurrency budget; O(n²) join rows | #383 R1 | before untrusted input drives loops / before T10 slice 3 |

## Filed 2026-10-07 late session
| #389 | Audit retry after Prune skips the content check | #387 R1 | |
| #390 | Audit Verify never reports a tombstone whose decision is missing | #387 R1 | |
| #391 | Pin the full wait-ID derivation chain end to end | #380 R3 | |
| #392 | Long caller-supplied attempt ids collapse iteration attempts in inspection | #383 R2 | |
| #396 | Cluster suspend commit ignores the loop iteration | #395 work | MUST land before durable cluster loops (#333) |

## To file (found at handoff 2, not yet filed)
- #393 R1: ErrChildPrincipalMismatch vs ErrChildRunNotFound is an existence oracle once #333 Child surfaces it.
- #384: Sweep marks the interrupted-run scan done even when no worker slot was free → up to 1/3 lease extra delay under saturation.
- ADR 0027 on main still says "ADR 0031" in ~3 places (T10 is ADR 0028).

## Previously "to file" (now filed as #389/#390)
- From #387 Review R round 1 (follow-ups, not blockers): (a) a retry after a prune skips the content check — a tombstone keeps only id digest + kind, so a different-tenant retry that was ErrConflict before the prune succeeds after it; (b) the audit cross-check never reports a tombstone whose decision is missing (e.g. after a mixed point-in-time restore).
