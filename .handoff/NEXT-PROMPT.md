You are the orchestrator for the Go repo well-prado/new-blok (local checkout: ~/Projects/Personal/new-blok). Goal: finish epic #7 [E07] "Durable state, recovery and version retention". User priorities: T09 slice C (#332) and T10 (#333) first, then the rest of E07. The user wants visible forward progress: run up to 4 agents in parallel, gate while reviews run, merge as soon as review and gate are both green.

FIRST read the notes from origin/wip/orchestrator-handoff (notes only, NEVER merge that branch):
  cd ~/Projects/Personal/new-blok && git fetch origin && for f in STATE.md RULES.md WORKER-BRIEF.md ISSUES.md E07-AUDIT1-REPORT.md E07-AUDIT2-REPORT.md waiver-merge.sh gate.sh; do git show origin/wip/orchestrator-handoff:.handoff/$f > /tmp/handoff-$f; done; chmod +x /tmp/handoff-gate.sh
STATE.md is authoritative: the open-PR table has exact heads and the next action for each, plus the decisions already taken (don't re-litigate them). RULES.md holds the hard rules, WORKER-BRIEF.md is the brief for implementers, and ISSUES.md lists the follow-ups. Never switch branches in the primary checkout; work in worktrees.

Hard rules (summary; RULES.md is authoritative):
- CI stays off (workflow 372722919 disabled_manually). The local gate is the CI: /tmp/handoff-gate.sh <worktree> <logdir> on the tree that lands (PR head merged locally with origin/main). Go 1.27.1.
- Merge only via `bash /tmp/handoff-waiver-merge.sh <pr> <full-sha>`, which must print PROTECTION IDENTICAL. Post a PR comment with the Review R summary and gate results first.
- Every merge needs an independent Review R and the full gate. Round 1 reviews the whole PR; later rounds review only the delta. The orchestrator may review a small delta itself only if it wrote none of it. A gate failure in an untouched package must be proven unrelated (known flake #374: trigger/worker timing).
- One issue per PR. What/Why/How bodies. Every new test shown RED first. Max 4 agents (Opus subagents, never nested). Commits by Wellington Prado <wellington@deskree.com> with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- After each merge: move the board item to Done (Project 15 ids in STATE.md), tick E07 child boxes with evidence links, and file non-blocking findings as issues.

Start with the open PRs/WIP (details in STATE.md), running in parallel:
1. #421 (resumer backlog drain): relay the Review R1 SHOULD-FIX (an "every worker busy" test plus a CPU-spin check) → delta review → gate → merge.
2. #423 (T10 slice 4, durable Child): full Review R1 → gate → merge (after #421; expect a resumer.go merge).
3. wip/333-durable-app (T10 slice 5, durable runtime in real apps): finish (P9 test, docs, validation) → PR → full Review R1 → gate → merge. Closes #333. Confirm its design decisions (listed in STATE.md) with the user if any look questionable.
4. #385 (Fixes #332): delta Review R2 → after #421, re-measure the backlog arm → gate → merge → tick the #46 boxes.
5. wip/411 (cluster suite reliability): rebase onto main, validate, PR → review → merge.
Then: #422 (PendingResumptions cost), #410 (bisect the slowdown), #408 (sqlite arrival-order test), the T12 #335 crash matrix, T13, T15, T18, T19 B–D (ask the user about tombstone growth), and T21 last.
T12 #335, T13, T15, T18, T19 B–D (ask the user about tombstone growth: document+assert vs retention; interacts with #364), and T21 #344 last. Close #7 only after every task is merged with Review R, every child box (#43–#49) is ticked with evidence links, and an independent specialist Review R of the whole epic is recorded on #7.

Report back after each merge.
