# E07 worker brief (shared by every implementation agent)

Repo: well-prado/new-blok (Go). Local primary checkout: ~/Projects/Personal/new-blok. NEVER switch branches
there and never edit files there; it is shared. Base for all work: current origin/main
(`git fetch origin` first).

## Read first
- `/tmp/handoff-RULES.md (extracted from origin/wip/orchestrator-handoff:.handoff/RULES.md)` — the hard rules. Obey all of it, especially "Keep changes small and validate the delta".
- `/tmp/handoff-E07-AUDIT1-REPORT.md` and `/tmp/handoff-E07-AUDIT2-REPORT.md` — the audits that found the defects
  (probes P1–P11, mutations MA–MN, M48*, M49*). Your issue body quotes them.
- Your issue: `gh issue view <N> --repo well-prado/new-blok`. Epic: #7.

## Setup
```
cd ~/Projects/Personal/new-blok && git fetch origin -q
git worktree add -b codex/<N>-<short-desc> ~/Projects/Personal/new-blok-<N> origin/main
cd ~/Projects/Personal/new-blok-<N>
export GOCACHE=/private/tmp/gocache-e07-<N>      # private cache; rm -rf it when you finish
mkdir -p /private/tmp/e07-<N>-scratch             # private scratch; rm -rf it when you finish
```
For a stacked slice, branch from the previous slice's branch instead and name it `codex/<N>-<desc>-<slice>`.
Go: go1.27.1 (installed). Docker is available (`golang:1.27.1` image, linux/arm64). For a real full disk on Linux:
`docker run --rm --tmpfs /mnt/small:size=1m ...` gives true ENOSPC (verified).

## How to work
- Small PRs: a few hundred lines. If your task is bigger, deliver ONLY the slice you were assigned, open its PR,
  and report; the orchestrator resumes you for the next slice after review.
- RED first: every new test must be shown failing on origin/main (or, when it cannot compile there, under a
  mutation that disables the fix). Record command + failing output. Reproduce the audit's mutations named in your
  issue's acceptance criteria and record each result (RED/GREEN).
- Real behaviour, not mocks: real SQLite files, real child-process SIGKILL (reuse the journal's
  `runTransitionChild` pattern / hooks in internal/journal), real ENOSPC. Never claim coverage from mocks,
  file presence or unrelated green tests.
- Targeted tests only while iterating: the packages you touch, plus `-race` on them. Do NOT run whole-repo
  `go test ./...` or `-race ./...` — the orchestrator runs the full gate once on the final reviewed head (other
  agents share this machine). Before handing over, run on your final head:
  `gofmt -l .` (empty), `git diff --check origin/main...HEAD`, `go vet ./...`,
  `GOOS=windows GOARCH=arm64 go vet ./...`, `GOOS=windows GOARCH=amd64 go vet ./...`, `go build ./...`,
  `CGO_ENABLED=0 go build ./...`, and `go test -count=1` + `go test -race -count=1` on every package you touched
  and every package that imports what you changed (find them with `go list -deps`/`grep`).
- Check `uptime` before timing-sensitive tests; if load > ~50 wait. Check `df -h /System/Volumes/Data`; stop and
  report if < 20 GB free.
- Never `go clean -cache`, never bare `git stash`, never enable CI / touch workflow 372722919, never merge, never
  edit branch protection, never force-push an open PR, never spawn sub-agents. No Windows support claims, no
  frontend work.
- ADRs in use go up to 0028 (0027 = T09 waits/leases, 0028 = T10 control flow). The next new ADR is 0029; ask the
  orchestrator before taking one. Prefer updating the existing ADR that covers the area (0003 SQLite store, 0021
  audit/retention, 0027 waits, 0028 control flow). Journal schema: v7 is taken by PR #380; the next is v8 — ask first.

## Commits and PR
- Commits authored by `Wellington Prado <wellington@deskree.com>` (set `GIT_AUTHOR_*`/`GIT_COMMITTER_*` env or
  `-c user.name/-c user.email`), message ending with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
- Push your branch, open the PR with `gh pr create --repo well-prado/new-blok --base <main or previous slice branch>`.
  Title like `fix(journal): ... (#N)` / `test(order): ... (#N)`.
- PR body: `## What` / `## Why` / `## How`, each opening in plain human language a reader outside this session can
  follow (an analogy is welcome). Under How: a "How we proved it" section with RED proofs (command + output),
  the mutation table, validation commands + results, and a "Limits" list of anything not proven. Final slice
  says `Fixes #N`; earlier slices say `Refs #N` and name the stack. End the body with:
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`
- If you must edit the body later: `gh api -X PATCH repos/well-prado/new-blok/pulls/<n> --input body.json`
  (not `gh pr edit`), then verify.

## Report back (your final message)
PR URL and number, branch, full head SHA, base, files changed + line count, what changed (short), every RED proof,
mutation results, validation commands with pass/fail, and anything unresolved, skipped or only partly proven.
If you were assigned only a slice, end with your concrete plan (files, tests, size) for the next slice(s).
Be honest; an unproven claim is worse than a reported gap. Clean up GOCACHE and scratch dirs (keep the worktree).
