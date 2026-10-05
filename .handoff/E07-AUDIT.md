# E07 evidence audit brief

Epic #7 [E07] "Durable state, recovery and version retention" in well-prado/new-blok. Its children #43–#49 are CLOSED, but their issue bodies still carry unchecked `- [ ]` acceptance-criteria and validation boxes. The epic closes only after a requirement-by-requirement audit. Your job is that audit for the issues assigned to you. It is READ + RUN only: you write no product code and you push nothing.

Setup: a detached worktree of current origin/main:
`git -C /Users/wellingtonprado/Projects/Personal/new-blok fetch origin && git -C /Users/wellingtonprado/Projects/Personal/new-blok worktree add --detach /private/tmp/<id>-audit origin/main`
Never switch the primary checkout's branch.

For EACH unchecked box (acceptance criteria, validation, AND the generic gates; dependency boxes just need the dependency's state) in each assigned issue:
1. Read the issue (`gh issue view N --comments`), its implementation PR(s) (`gh pr view`), and the relevant ADRs/docs.
2. Find the CURRENT evidence on main: the exact test(s) (file:line, test name), or a benchmark or measurement.
3. Classify the evidence:
   - **VERIFIED**: a real test exercises the real implementation (real SQLite store, real process kill where the box says crash/kill, real counts/rows asserted). You RAN it on current main and it passed. Give the command and the result. For crash or kill claims, confirm it's an actual process kill or a fault-injection crash barrier that aborts mid-transaction, not a mocked error; say which.
   - **PARTIAL**: evidence exists but misses part of the box (e.g. kills the engine but not the dispatcher; measures footprint for 100 runs, not thousands). Say exactly what's missing.
   - **GAP**: no evidence on main.
   - **N/A-WITH-REASON**: only if the box genuinely doesn't apply, with a citation.
   For anything you claim VERIFIED, prove the test can fail: apply a quick mutation to the implementation in your audit worktree, show the test goes red, then revert. At least one mutation per acceptance criterion.
4. Never tick boxes yourself, edit issues, or file issues. Report only.

Generic gates (`go vet`, `-race`, build, diff --check): run the RULES validation suite once on current main and report it.

Output, per issue: a table `box | verdict | evidence (test, file:line, command) | mutation red-proof | gap detail`. Then a consolidated GAP/PARTIAL list grouped into proposed work items. Each work item needs a title, the boxes it closes, a concrete implementation/test plan, an estimated size, and the files to touch. Group related gaps (e.g. "crash matrix via real process kill harness") so one PR closes several boxes. Flag anything where the current implementation is actually WRONG, not just untested, as a defect.

Rules file (obey): /private/tmp/claude-501/-Users-wellingtonprado-Projects-Deskree-blok/f3070409-be43-4f68-bb35-be523ef1df01/scratchpad/RULES.md. Private GOCACHE, private scratch dir; check `uptime` before timing tests. Cleanup at the end: `git worktree remove --force /private/tmp/<id>-audit`, GOCACHE, scratch. Do not spawn sub-agents.
