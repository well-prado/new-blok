# ADR 0026: The blok dev build, watch and reload contract

- Status: implementation in review for E11-T03 (#66)
- Date: 2026-10-05
- Roadmap: E11-T03 ([#66](https://github.com/well-prado/new-blok/issues/66));
  builds on E11-T02 (#65, ADR 0024: the hardened go command, process group,
  pipe guard, signal handling, redaction and diagnostic registry), E12-T01
  (#67, ADR 0023: layout discovery and symbolic-link rules), E08 (#50–#53,
  ADR 0004: persistent worker generation and drain) and E07-T06 (#48:
  retained artifacts and readiness, ADR 0009)
- Consumers: developers, editors and agents through the `blok-dev/v1`
  event stream; E11-T04 (#102, doctor) and #157 (newcomer workflow)
- Windows: unverified. The native Windows checklist on #66 is a
  Windows-track follow-up (#297, #156, #157); nothing here claims Windows
  support

## Context

A developer editing an application wants the running application to follow
the code: save, and a moment later the endpoint answers with the new
behaviour. Go has no safe in-process reload, so "follow the code" means
rebuild the executable and replace the process. Done carelessly that loop
has well-known failures: a save storm starts a build storm; a crash loop
forks as fast as the machine allows; a broken edit takes the working
application down; Ctrl+C or a dead terminal leaves the application, or the
Node worker it started, running and holding its port; a rebuilt executable
written over the running one changes what an application that hashes its
own executable thinks it is running, so a durable run admitted under one
build could be resumed by different code under the same label.

## Decision

`blok dev` is one command over one API: `internal/tooling/devtool.Dev`
runs the loop and emits events; `cmd/blok` only parses arguments, routes
signals and encodes the events.

```text
blok dev [--json] [--package DIR] [directory] [-- application arguments]
```

### The loop

1. **Scan.** The watcher records every watched file (below) by size,
   modification time and mode, and on Linux and macOS by inode and
   status-change time too.
2. **Build.** Layout discovery (ADR 0023) runs first; any `layout_*`
   diagnostic fails the build. A watched source file that is a symbolic
   link fails it too (`dev_symlink_unwatched`, below). Stale or missing
   bindings for the manifest's `types` file are **regenerated** with the
   same `generate.Source` and atomic `generate.WriteFile` that `blok
   generate` uses (a hand-written bindings file is refused,
   `bindings_not_generated`); the write is recorded in the snapshot so it
   is not mistaken for an edit. Then `go build -o <new path> ./<main
   package>` runs through the ADR 0024 go command: `GOTOOLCHAIN=local`,
   `GOPROXY=off`, `GOFLAGS=-mod=readonly` (or `-mod=vendor`), `GOWORK=off`,
   its own process group, its pipe guard, interrupt then kill after 5 s.
   Its standard error becomes the same diagnostics `check` and `test`
   report (`go_compile_error` at `file:line:col`, `go_module_missing`, …).
3. **Replace.** Only a build that compiles replaces the running
   application: blok dev stops it (below), keeps its executable (below)
   and starts the new one. A build that fails emits `build-failed` with
   its diagnostics and `running: N`, and build N **keeps serving**. There
   is no in-process reload of Go code. An application that exited on its
   own while the build ran is reported as `app-exited` (with
   `dev_app_exited`), never as `app-stopped`: after a failed build it is
   restarted with backoff and `running` is 0; after a successful one the
   new build replaces it without a restart. A stop requested while the
   old application drains, or while a restart is due, starts nothing.
4. **Watch.** Every poll rescans. Changes accumulate until the project has
   been quiet for `Quiet` (300 ms), or until `MaxWait` (2 s) after the
   first change, then one build starts with all of them. Builds never
   overlap; changes made during a build are seen by the next poll and
   coalesce into at most one more build.

Defaults (all `DevOptions` fields): poll 250 ms, quiet 300 ms, max wait
2 s, stop grace 10 s, restart backoff 500 ms doubling to 30 s, reset after
a 10 s run or a new build.

### What is watched, and why polling

The watch set is derived from the project as layout discovery sees it:
`blok.json`, `go.mod`, `go.sum`, every non-test `.go` file of the module,
and every file under a node root (`layout.NodeRoot`: a foreign node's
`node.json`, `.mjs`, … restart its worker too). `_test.go` files are not
built into the application and never restart it. Directories the go
command and discovery skip are skipped: `.`/`_` prefixes, `vendor`,
`testdata`, `node_modules`, and nested modules.

Symbolic links follow ADR 0023: they are never followed. The walk reads
directory entries with `Lstat` semantics and records a link as the link
itself, so adding, removing or retargeting one is a change; nothing
outside the root is ever stat-ed, so editing a file a link points to
outside the project changes nothing. Because `go build` *would* follow a
link, a build is refused with `dev_symlink_unwatched` when the walk
meets a link it would follow to files the watcher does not see (layout
discovery's own `layout_symlink_*` codes cover node and workflow
directories first):

- a link that is itself a watched file (a linked `.go` file, …);
- any other link, whatever its name, that leaves the root (decided
  lexically by `layout.ClassifyLink`, which reads only inside the root):
  telling a linked package directory from a linked file would mean
  following it;
- a link inside the root into a skipped directory or a nested module.

A link inside the root to a watched directory or to a file nothing builds
(`README.md` → `docs/…`) is harmless and ignored, and so is a link inside a
skipped directory or named like one (`.env`). A dangling link or a cycle
gives the build nothing to read. The root itself is resolved once, as
discovery resolves it.

The watcher **polls** the tree. No dependency was added: fsnotify-style
kernel notification needs one watch per directory (inotify limits),
misses or reorders events across editors' write-and-rename saves, network
and container file systems, and differs per OS; a stat walk behaves the
same everywhere and its cost is bounded. One scan visits at most
`MaxWatchEntries` (100,000) directory entries and records at most
`MaxWatchedFiles` (20,000) files; past either it reports
`layout_limit_exceeded`.

- **At the first scan** a project past the bounds (or unreadable) is
  fatal: blok dev emits `watch-failed` and ends with exit 1, before any
  build, rather than run an application it could never rebuild.
- **Mid-session** it is reported once (`watch-failed`, `running` still
  serving) and the previous snapshot stays; the failing scans **back off**,
  the interval doubling from `Poll` up to `MaxScanBackoff` (10 s), until a
  scan succeeds again. Before this, a project over the bound was
  rescanned in full every 250 ms: about 61% of a CPU, measured in review.
- **The interval adapts to the scan's cost**: the next scan starts no
  sooner than `ScanDuty` (10) times the last scan's duration after it, so
  scanning takes at most about a tenth of one CPU however large the project
  is, at the price of noticing edits later in a large tree.

Cost, measured with `BenchmarkScanAtTheBounds` (`go test -run '^$' -bench
ScanAtTheBounds -count 5 ./internal/tooling/devtool`, linux/amd64, 4
vCPUs, warm page cache): one scan at the bounds (20,000 watched files among
100,000 entries) takes 162–181 ms (five samples), so a project that size is
rescanned about every 1.6–1.8 s; a scan of the starter is nine stats, and its
interval stays at `Poll` (250 ms).

The stamp is size, modification time and mode, plus, on Linux and macOS,
the inode and the status-change time (ctime). An edit that keeps the size
and restores the modification time (`touch -r`, `cp -p`, `rsync -a`,
`tar -x`) still changes the ctime, which no system call sets back, and a
replacement renamed over the file has a new inode, so both are seen.
Anything else that changes a watched file's ctime (`chmod`, a new hard
link, an extended attribute) also counts as a change and rebuilds. Other
systems, Windows included, keep the size, modification time and mode stamp
and do not see such an edit (unverified there, #297).

### The application process

- It runs with its working directory at the project root, standard input
  from the null device, and its own **process group**; a terminal's Ctrl+C
  reaches blok dev, never the application directly.
- It gets the ADR 0024 **pipe guard**: a `/bin/sh` in its own group reading
  a pipe only blok dev writes to. Whenever blok dev exits, however it exits
  (SIGKILL included), the guard sees end-of-file and kills the
  application's process group: the application and every process it
  started (its Node worker, any child), since a child stays in its
  parent's group unless it leaves it (`setsid`, a daemon: see Limits).
  It then removes blok dev's private build directory. The go command's
  guard does the same for a build, and blok dev keeps a third guard for
  the whole session, which names no group and only removes the build
  directory, so a killed blok dev leaves no executables behind even when
  neither a build nor the application is running.
- **Stopping** sends SIGTERM to the application alone, so it can drain:
  finish the requests it accepted and stop the workers it owns through
  their supervisors (ADR 0004's drain). After `StopGrace` (10 s), or at
  once on `Force` (a second Ctrl+C), the whole group is killed and the
  stop reports `dev_app_stop_timeout`. After the application exits its
  group is **swept** with SIGKILL, so a descendant it failed to stop dies
  with it, and the guard is retired.
- The environment is the caller's plus `BLOK_DEV=1` and
  `BLOK_DEV_GENERATION=<n>`, replacing any value the caller set. `n` grows
  with every start. An application uses it as its persistent workers'
  generation: a worker left over from an earlier start is refused at the
  handshake (`invalid_hello`; ADR 0004) instead of serving new code.
- Its standard output and error go to blok dev's (both to standard error
  with `--json`, so the event stream stays one JSON document per line).
  That live output is the application's own and is not altered, exactly as
  under `go run`. The last 20 lines of its standard error (1 KiB each) are
  kept for the `app-exited` event, redacted as a block (ADR 0024).
- **Restarts.** An application that exits on its own with status 0 is left
  stopped until the next change. So is one that exits with
  `deployment.ExitRetainedIncompatible` (below). Any other exit emits
  `app-exited` with `dev_app_exited`, its status and output tail, then
  `restart-scheduled`: 500 ms, 1 s, 2 s, … up to 30 s, reset after a 10 s
  run or a new build. At the cap that is two starts a minute, whatever the
  application does.

### Executables, artifact identity and durable runs

Each build writes a new executable, `build-<n>/<name>`, in a private
(0700) temporary directory created for the session; no path is ever
reused and no executable is written while anything runs it. blok dev never
names or claims an artifact identity: it sets no digest, version or label.
An application that binds its artifact to its own executable (as
`examples/deploy`'s durable deployment does, `NativeBinaryDigest` over
`os.Executable()`) therefore sees new code as a new identity, and its own
retained-artifact check (ADR 0009, #48) decides what to do with runs
admitted under the old one.

**Kept executables.** A replaced build's executable is not deleted: blok
dev keeps the executables of the `KeptBuilds` (5) most recent earlier
successful builds, plus the newest earlier one whose application did not
refuse its durable state, until the session ends; then the whole
directory is removed (on a panic too, and by the session guard if blok dev
is killed). Older ones are deleted once a newer build replaces them; none
of them is running, since only the current build ever runs. That bounds
the directory to seven executables.

**A deterministic refusal is not a crash.** An application whose durable
state retains runs it cannot adopt (another artifact identity, an unknown
checkpoint codec, a missing or changed retained manifest) will refuse them
on every start of the same executable, so restarting it is pointless. The
contract is structured, never parsed from output: the application exits
with `deployment.ExitRetainedIncompatible` (65, sysexits' `EX_DATAERR`),
which `deployment.ExitCode` returns for any error matching
`deployment.ErrRetainedIncompatible`. `app.RetainedArtifactProbe` wraps
every refusal decided by the retained journal's content as
`*app.RetainedIncompatibleError`, and leaves an error reading the journal
as it is; the application maps the first to
`deployment.ErrRetainedIncompatible` (`examples/deploy` does, and reports
the second as "retained journal unreadable", an ordinary exit 1 that is
restarted with backoff). `app` does not import `contract/deployment`,
which links `net` and would break the footprint of applications without a
listener. On exit
65 blok dev emits `app-exited` with `dev_durable_incompatible`, schedules
no restart and waits for the next change. The diagnostic's remediation
names three ways out: revert the change (Go builds are reproducible, so
the reverted build is byte-identical to the one that admitted the runs and
adopts them); finish the runs with the kept executable of the newest
earlier build that did not refuse, using the exact command in the event's
`resume` field (`cd <root> && <kept executable> <application
arguments>`, quoted for a POSIX shell and redacted), run in another
terminal while blok dev keeps running, since the executable is removed
when the session ends; or finish or discard the runs with the
application's own tools. An application exiting 65 for another reason is
reported the same way, which is why the code is reserved for this meaning
under `BLOK_DEV`.

### The event stream (`blok-dev/v1`)

```json
{"version": "blok-dev/v1", "event": "…", "build": 2, "generation": 3, "pid": 4242,
 "files": 9, "changed": [], "regenerated": [], "running": 1, "status": "exit status 1",
 "delayMs": 500, "output": [], "diagnostics": [], "resume": "cd … && …", "exitCode": 130}
```

| Event | Meaning |
| --- | --- |
| `watching` | the first scan; `files` watched |
| `watch-failed` | a scan failed (once per failure); the previous snapshot stays. At the first scan it is followed by `stopped` (exit 1) |
| `changed` | `files` paths changed; up to 20 listed in `changed` |
| `build-started` | build `build` began |
| `build-failed` | build `build` failed with `diagnostics`; `running` keeps serving (0: none) |
| `build-succeeded` | build `build` compiled; `regenerated` lists rewritten bindings |
| `app-started` | build `build` started as `pid` with `generation` |
| `app-stopped` | blok dev stopped it; `status`, and `dev_app_stop_timeout` if killed |
| `app-exited` | it exited on its own (during a build too); `status`, `output`, `dev_app_exited` unless status 0, or `dev_durable_incompatible` and `resume` for status 65 |
| `restart-scheduled` | it restarts after `delayMs` |
| `stopped` | blok dev ended with `exitCode` |

With `--json` each event is one JSON line on standard output. Without it
each is one `blok dev: …` line, followed by its diagnostics in ADR 0024's
human form (`<source>: <code>: <message>`, `expected:`, `actual:`, `fix:`)
and its output tail as `  | …` lines, and a `resume` field as an
`  resume: …` line. Every diagnostic passes
`observe/redact` as it is emitted, whatever produced it, and is ordered by
`diagnostic.Sort`; changed paths pass `redact.String`. Versioning follows
ADR 0024: a consumer rejects an unknown `version`; adding a field, an
event or a code keeps `v1`; renaming, removing or retyping a field, or
changing an event's or exit code's meaning, requires `v2`.

### Exit codes and signals

| Code | Meaning |
| --- | --- |
| 130 | stopped by SIGINT, SIGTERM, SIGHUP or SIGQUIT (the normal end; no diagnostic) |
| 1 | the project directory cannot be read (`project_unreadable`), or cannot be watched at the first scan (`layout_limit_exceeded`) |
| 2 | usage error: one stderr line, nothing started |
| 3 | the go command, the build directory or a process guard (the session's included) cannot start, or a panic (the group is killed first) |
| 4 | an event could not be written (a closed pipe included); the application is stopped first |

The first signal stops blok dev gracefully; any further one forces the
application down. A signal blok dev was started with ignored stays
ignored (it is not caught, which would un-ignore it): `nohup blok dev &`,
which starts it with SIGHUP ignored (and, from a non-interactive shell,
SIGINT too), keeps running when the terminal closes, as nohup promises,
and SIGTERM still stops it. The application inherits the ignored
disposition, as under nohup. SIGPIPE is caught, not ignored, so a closed output pipe
is exit 4 while the application inherits SIGPIPE's default disposition (an
ignored signal would stay ignored in it).

### Diagnostic codes

blok dev reuses ADR 0024's codes unchanged where the condition is the
same: layout's `layout_*` codes, `project_unreadable`,
`bindings_types_missing`, `bindings_generate_failed`, `bindings_missing`
(unreadable), `bindings_not_generated`, the `go_*` codes of a build, and
`process_guard_unavailable`. It adds:

| Code | Condition |
| --- | --- |
| `dev_main_package_missing` | the main package (`--package`, default `./cmd/<blok.json name>`) is not a directory inside the root |
| `dev_symlink_unwatched` | a watched source path is a symbolic link |
| `dev_build_dir_unavailable` | the private build directory cannot be created |
| `dev_bindings_write_failed` | regenerated bindings cannot be written |
| `dev_app_start_failed` | the built executable cannot be started |
| `dev_app_exited` | the application exited on its own with a failure status |
| `dev_app_stop_timeout` | the application did not exit within the grace and its group was killed |
| `dev_durable_incompatible` | the application exited with `deployment.ExitRetainedIncompatible`: its durable state retains runs this build cannot adopt; not restarted |

`devtool.Codes` registers every code with the commands that raise it;
`TestCodeRegistryMatchesSourceAndADR` keeps the registry, the code literals
in the source and the tables of ADRs 0024 and 0026 identical.

## Compatibility

- **Additive**: the `blok dev` command; `devtool.Dev`, `DevOptions`,
  `DevEvent` and the `blok-dev/v1` event stream, a new machine-readable
  contract; the `BLOK_DEV` and `BLOK_DEV_GENERATION` environment contract;
  eight `dev_*` diagnostic codes; `generate.WriteFile`;
  `deployment.ErrRetainedIncompatible`, `deployment.ExitRetainedIncompatible`,
  `deployment.ExitCode` and `app.RetainedIncompatibleError`.
- **Behavioural, examples only**: `examples/deploy`'s durable deployment
  returns `deployment.ErrRetainedIncompatible` (same message as before)
  for a refusal decided by the journal's content, and "retained journal
  unreadable" when the journal cannot be read; its command exits with
  `deployment.ExitCode` (65 for the refusal, 1 otherwise, as before).
- **Behavioural, none for existing commands**: `check`, `test`, `inspect`,
  `new`, `generate` and `version` keep their output, exit codes and
  diagnostics. `blok generate` now writes through `generate.WriteFile`,
  the same atomic temporary-file-and-rename it used before (moved, not
  changed). The bindings check was split into a plan shared with dev; its
  diagnostics are byte-identical (the `TestFixtures` goldens pass
  unchanged).
- **Unchanged**: `internal/tooling/layout`, `internal/runtime`,
  `runtime/worker`, the worker protocol and every wire, journal and
  artifact contract. `app.RetainedArtifactProbe` refuses exactly what it
  refused before, with the same messages; only the error's type is new. No
  dependency is added.

## Evidence

Recorded in the pull request for #66 with commands, versions and results.
The predeclared cases are `testdata/tooling/dev/fixtures.json` (9 cases, 15
runs across the two layouts) and the Go+Node and durable fixtures beside it.

## Limits

- Go code is rebuilt and the process replaced; nothing is reloaded in
  process. Requests that arrive while one process is stopping and the next
  is starting are refused (connection refused), never queued or repeated.
- blok dev keeps old executables only for the session, and runs only one
  build at a time: a durable application that refuses a retained run stays
  down until the code adopts it or the developer finishes the runs with the
  kept executable. It does not tell which earlier build admitted a run; it
  offers the newest earlier build that did not refuse. A multi-version
  manager is later work (architecture §7, M8).
- On systems other than Linux and macOS, polling cannot see an edit that
  keeps a file's size and restores its modification time; files a Go
  package embeds (`//go:embed`) are not watched unless a node root owns
  them. The watcher's reaction time grows with the project's scan cost
  (`ScanDuty`), and while scans fail with the backoff (up to 10 s).
- After the application exits, its group is swept by process-group id.
  The id is reserved while any member lives; a group with no members left
  could in principle be reused between the reap and the sweep (the sweep
  then finds no process or, in a pathological wrap-around, an unrelated
  group). The guard closes the same window for blok dev's own death.
- A descendant that leaves the application's process group (`setsid`, a
  daemon) is not the application's to stop: it is neither swept nor
  killed by the guard, on any stop, Ctrl+C or SIGKILL included.
- The guards are `/bin/sh` processes in their own process groups: if they
  are killed too (a `kill -9` of every process, an OOM kill), nothing
  removes the build directory (0700, executables only).
- Verified on macOS (darwin/arm64) and Linux (linux/arm64, the
  `golang:1.27.1` container image), Go 1.27.1; the Go+Node worker test ran
  on macOS only (Node 24.21.0), the container has no Node. amd64 hosts are
  not run here. Windows has no process group, guard or graceful stop here: the
  application is killed outright and its children may outlive it —
  unverified, and owned by the Windows track (#156, #157).
