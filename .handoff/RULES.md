# New Blok agent rules (read fully before doing anything)

Repo: well-prado/new-blok (Go). Today: 2026-10-05.

## Never
- Never switch branches in `/Users/wellingtonprado/Projects/Personal/new-blok` (shared primary checkout, another worker's branch). Work ONLY in your assigned worktree.
- Never enable GitHub Actions or touch workflow 372722919. Never merge a PR (the orchestrator merges). Never edit branch protection.
- Never run `go clean -cache`. Use your own `export GOCACHE=/private/tmp/gocache-<your-id>` and `rm -rf` it when done.
- Never use bare `git stash` / `git stash pop` (shared stack). Use WIP commits.
- Never put scratch files in a shared scratchpad. Use `/private/tmp/<your-id>-scratch/` and remove it when done.
- No `any`-style hand-waving: never claim something is tested by mocks, file presence or unrelated green tests.
- No Windows support claims (Windows is deferred, #297). No frontend work.

## Local validation (CI is off; this IS the CI). Run all on the final head:
```
gofmt -l .                      # must print nothing
git diff --check origin/main...HEAD
go mod verify
go vet ./...
GOOS=windows GOARCH=arm64 go vet ./...
GOOS=windows GOARCH=amd64 go vet ./...
go build ./... && CGO_ENABLED=0 go build ./...
go test -count=1 ./...
go test -race -count=1 ./...
```
Plus module suites when relevant (each has its own go.mod): `observe/otel`, `examples/recipes/external/shopapp`. The distributed suite needs an isolated etcd cluster (template: `/private/tmp/claude-501/-Users-wellingtonprado-Projects-Deskree-blok/f707afe8-c9f0-4fbf-a389-8f9985740d7a/scratchpad/c85/compose.yaml` + `env.sh`; copy it to your private dir, rename project+ports with python not sed, `docker compose down -v` afterwards). Only needed if you touch cluster/distributed code.
Load: check `uptime`; if load > ~50, wait before timing-sensitive tests. If a failure is a timing flake, prove it's unrelated (re-run on origin/main), don't wave it away.
Disk: `df -h /System/Volumes/Data` — stop and report if under 20 GB free.

## Branch / commit / PR conventions
- One issue per PR. Branch `codex/<issue>-<desc>`. The branch must contain current `origin/main` (merge origin/main in; don't force-push rewritten history on an open PR).
- Commits authored by Wellington Prado <wellington@deskree.com>, message ending with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
- PR body: `## What` / `## Why` / `## How`, each opening in plain human language (analogies welcome), then evidence (validation commands + results, RED proofs) under How. Include `Fixes #N`. End with:
  `🤖 Generated with [Claude Code](https://claude.com/claude-code)`
- Edit PR bodies with `gh api -X PATCH repos/well-prado/new-blok/pulls/<n> --input body.json` (not `gh pr edit`), then verify.
- Every new test must be shown RED first (on origin/main, or under a mutation of the fix) — record the command and the failing output in the PR body.
- ADR numbers in use go up to 0026 (0025 = #68 ownership, 0026 = #66 dev). Next new ADR is 0027.

## Reporting back
End with: final head SHA (full), what changed, validation results (each command: pass/fail), RED proofs, anything unresolved. Be honest about anything skipped.

## Keep changes small and validate the delta (added 2026-10-06)
Reason: #322 (~7,000 lines) cost one to three hours per review round because every round re-checked everything.
- **Small PRs.** Aim for a few hundred lines per PR. Split large tasks into stacked PRs up front, one reviewable piece each. For example, E07-T10 (#333) is one PR per construct: Each, Choose, TryFinally, Parallel, Child. Each stacked PR still links its issue (`Refs #N`; the last one says `Fixes #N`).
- **Delta-only review rounds.** The first Review R covers the whole PR. Later rounds review only `git diff <last-reviewed-head>..<new-head>` against the previous round's findings. A later round never re-reviews the whole PR.
- **Targeted tests while iterating.** Per fix, show the new test RED first and run only the packages the change touches.
- **Full gate once.** Run the whole-repo validation block above (gofmt, diff --check, mod verify, vet including both Windows arches, both builds, `go test ./...`, `go test -race ./...`, plus any module suites touched) once, on the final head, before merge.
- **No repeated flakiness batches by default.** Run `-count=N` repeats only for a test that is new, timing-sensitive, or has flaked before.
- **New non-blocking findings in a later round become issues,** not another round. Blockers and should-fix items on the changed lines are still fixed in the PR.
