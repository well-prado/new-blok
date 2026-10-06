# Cloud addendum to the New Blok agent rules (read with RULES.md)

Handoff notes: /tmp/claude-0/-home-user/8232c5f8-5376-5797-b41b-96ba08149fc1/scratchpad/handoff/{STATE.md,RULES.md,E07-AUDIT.md}.
RULES.md applies, except where this file replaces a laptop-specific detail.

## Environment (Linux cloud container, 4 CPUs, 15 GB RAM)
- Primary clone: /home/user/new-blok (shallow, depth ~300). NEVER switch its branch. Create your own worktree:
  `git -C /home/user/new-blok worktree add /home/user/wt/<id> <ref>` (or `--detach`), and remove it when done.
  If you need more history: `git -C /home/user/new-blok fetch --depth 1000 origin <branch>`.
- BEFORE ANY go COMMAND, in every shell: `. /tmp/claude-0/-home-user/8232c5f8-5376-5797-b41b-96ba08149fc1/scratchpad/goenv.sh`
  (routes proxy.golang.org through the egress proxy and selects the go1.27.0 toolchain). Check with `go version` → go1.27.0.
- GOCACHE: use the default shared cache (the Go cache is safe for concurrent use, and private caches would triple the
  CPU cost on this 4-core box). NEVER run `go clean -cache`.
- CPU is scarce. Wrap every whole-repo or heavy run (`go test ./...`, `go test -race ./...`, `go vet` across GOOS, module
  suites, big benchmarks) in the shared lock so they run one at a time:
  `flock /home/user/heavy.lock go test -race -count=1 ./...`
  Targeted package tests may run without the lock. Check `uptime` before timing-sensitive tests; with 4 CPUs, load > ~6 is high.
  If a failure looks like a timing flake, re-run it alone under the lock and on origin/main before calling it unrelated.
- Disk: `df -h /home/user`; stop and report if under 10 GB free.
- Scratch: your own dir /tmp/claude-0/-home-user/8232c5f8-5376-5797-b41b-96ba08149fc1/scratchpad/<id>/ ; remove at the end.
- The macOS-only `GOOS=windows` vet variants still apply (GOOS=windows GOARCH=arm64 and amd64 go vet ./...).

## GitHub
- The `gh` CLI does NOT work here (invalid token). Use the GitHub MCP tools (load them with ToolSearch, e.g.
  `select:mcp__github__pull_request_read,mcp__github__issue_read,mcp__github__update_pull_request,mcp__github__add_issue_comment,mcp__github__issue_write`).
  PR bodies: `mcp__github__update_pull_request` (body field) instead of `gh api PATCH`; re-read to verify.
- git push works: `git push origin HEAD:refs/heads/<branch>`. Never force-push an open PR's branch; merge origin/main in.
- Commits: author Wellington Prado <wellington@deskree.com> (already set in repo config). Message ends with:
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
- Never merge anything, never touch branch protection or workflow 372722919. Do not spawn sub-agents.
