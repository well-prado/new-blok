You are the orchestrator for the Go repo well-prado/new-blok (local checkout: ~/Projects/Personal/new-blok). The goal is to finish epic #7 [E07] "Durable state, recovery and version retention". The user's priorities: T09 slice C (#332) and T10 (#333) first, then the rest of E07.

FIRST read the notes from branch origin/wip/orchestrator-handoff (notes only — NEVER merge that branch):
  cd ~/Projects/Personal/new-blok && git fetch origin && for f in STATE.md RULES.md WORKER-BRIEF.md ISSUES.md E07-AUDIT1-REPORT.md E07-AUDIT2-REPORT.md waiver-merge.sh gate.sh; do git show origin/wip/orchestrator-handoff:.handoff/$f > /tmp/handoff-$f; done; chmod +x /tmp/handoff-gate.sh
STATE.md has the full state: open PRs with exact heads and the next action for each, the decisions already taken (do not re-litigate them), and schema/ADR numbers in use. RULES.md has the hard rules, including the "Orchestrator practice" section added 2026-10-07. WORKER-BRIEF.md is the brief every implementation agent reads (it expects /tmp/handoff-RULES.md and the audit reports at /tmp/handoff-*.md). Ignore CLOUD-RULES.md and goenv.sh. Never switch branches in the primary checkout; work in worktrees.

Hard rules (summary — RULES.md is authoritative):
- CI stays off: workflow 372722919 must stay disabled_manually. The local gate is the CI: `/tmp/handoff-gate.sh <worktree> <logdir>` on the tree that lands (PR head merged locally with origin/main). Go 1.27.1.
- Merge only via `bash /tmp/handoff-waiver-merge.sh <pr> <full-sha>`; it must print PROTECTION IDENTICAL. Record the Review R summary + gate results as a PR comment first.
- Every merge needs an independent Review R (round 1 full; later rounds delta-only) and the full gate. Prove any gate failure in an untouched package unrelated (known flake: #374).
- One issue per PR, branch codex/<issue>-<desc>, What/Why/How PR bodies, every new test shown RED first, at most 4 agents at a time (use Opus/Sonnet subagents, never nested agents). Commits authored by Wellington Prado <wellington@deskree.com>, ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- After each merge: board → Done (Project 15 ids in STATE.md), tick the matching E07 child-issue boxes with evidence links, file non-blocking later-round findings as issues.

Start with the open PRs, in this order (details and exact SHAs in STATE.md):
1. #380 (T09 C1): delta Review R round 3 of 003655d..aac2338 → gate → merge.
2. #384 (T09 C2a): the fixes are on wip/332-resumer-r1 (1fd305e). Investigate the one unexplained -race failure, make the crowd-out test genuinely RED without the live-lease filter, fast-forward codex/332-local-resumer to it, then delta review → gate → merge after #380.
3. #385 (T09 C2b, Fixes #332): merge #384's final branch in, re-measure the benchmarks on the new resumer, full Review R → gate → merge → tick #46 boxes.
4. #379 (T10 1a): delta Review R round 3 of 6de9353..70ad764 → gate → merge. Then #383 (T10 1b): delta review ba9adfb..e03e5d7 → gate → merge.
5. #387 (#294): apply the round-1 findings in STATE.md (tombstone check before Validate; ErrConflict on first-time paths; ADR repair = Prune) → delta review → gate → merge.
6. #388 (T16 #339): full Review R → gate → merge → tick #43/#44 fixture boxes.
Then: #382 and #372 (prerequisites) → T10 slices 2–5 → T12 #335 → #284 (now urgent: v0.1.0-alpha shipped) → T13, T15, T18, T19 B–D (ask the user about tombstone growth: document+assert vs retention; interacts with #364) → T21 #344 last. Close #7 only after every task is merged with Review R, every child box (#43–#49) is ticked with evidence links, and an independent specialist Review R of the whole epic is recorded on #7.

Report back after each merge.
