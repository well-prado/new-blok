# ADR 0024: The check, test and inspect CLI contract

- Status: implementation in review for E11-T02 (#65)
- Date: 2026-10-05
- Roadmap: E11-T02 ([#65](https://github.com/well-prado/new-blok/issues/65));
  builds on E11-T01 (#64: `blok new`, `blok generate`, `blok.json`), E02-T04
  (#28: `internal/diagnostic`), E12-T01 (#67: `internal/tooling/layout`
  discovery, ADR 0023) and ADR 0021's redaction boundary
- Consumers: E11-T03 (#66, dev), E11-T04 (#102, doctor), the read-only
  development MCP (#69) and Studio, through the versioned report only
- Windows: unverified. The native Windows checklist on #65 is a Windows-track
  follow-up (#297, #156, #157); nothing here claims Windows support

## Context

A developer, an editor and an AI assistant all need the same answers about
an application: is it valid, do its tests pass, and what does it contain.
They need them as stable, machine-readable facts with source positions and
fixes, and with exit codes a script can branch on. They must not get them
by running the application: preparing a project for inspection must not
execute its package initialisation, its builders or its effects, and a test
report must come from the application's real tests, not an imitation.

## Decision

Three commands, one API. `internal/tooling/devtool` implements `Check`,
`Test` and `Inspect`; each returns one `Report`. `cmd/blok` parses
arguments, runs the API and encodes the report. The CLI never computes a
diagnostic of its own, so the CLI's JSON and the API's value are the same
document (asserted byte for byte).

```text
blok check   [--json] [directory]
blok test    [--json] [--run REGEXP] [--race] [directory]
blok inspect [--json] [--fields LIST] [directory]
```

### What runs, and what never runs

| Command | Reads | Runs |
| --- | --- | --- |
| `check` | layout discovery (`blok.json`, `go.mod`, node and workflow source), Go source (parsed, never loaded) | `go vet -json ./...`, which type-checks and compiles every package and runs none of them |
| `test` | layout discovery | `go test -json ./...`: the application's own tests, in their own process |

Neither `check` nor `test` downloads anything or edits the project (next
section); `inspect` starts no process at all.
| `inspect` | layout discovery, Go source (parsed) | nothing |

`check` runs, in order: `layout` (internal/tooling/layout discovery, ADR
0023: the strict manifest, the module, node and workflow identities, node
import independence, ownership and symlink rules), `bindings` (the manifest's types file
regenerated in memory by the same `generate.Source` `blok generate` uses,
compared with the committed `bindings_gen.go`), `workflow-steps` (the step-id
rules `flow.Define` enforces — id grammar, the reserved `output` id,
duplicates — applied to builder calls whose id is a literal) and `go-vet`.
The slow toolchain check runs last, so an interrupt during it still reports
every static result. Each check is listed with `passed`, `failed`,
`skipped` (with a reason) or `interrupted`; a check never disappears.

What a static read cannot decide is counted, never guessed: a step id that
is not a literal, a build function that is not a literal, a builder call in
a function literal that is not a builder argument. A builder call under Go
control flow (`if`, `for`, `switch`, `select`) may run zero or many times,
so it is checked for grammar but not compared for duplicates. Validation of
a lowered program by the canonical compiler needs the builders to run, so it
happens where the application runs them: in its tests, under `blok test`.
A duplicate step id is therefore reported by `check` statically and by
`test` when `flow.Define` rejects it at run time (both asserted).

### The go command's environment

`check` and `test` start the go command from the caller's environment with
four settings always replaced, because each could otherwise make blok fetch
or run code the caller did not choose, or edit the project:

| Setting | Value | Prevents |
| --- | --- | --- |
| `GOTOOLCHAIN` | `local` | downloading and running another toolchain (a `toolchain` line, `GOTOOLCHAIN=auto`) |
| `GOPROXY` | `off` | any module or checksum-database request; a module missing from the cache is a diagnostic |
| `GOFLAGS` | `-mod=readonly` (`-mod=vendor` with `vendor/modules.txt`) | rewriting `go.mod` or `go.sum`; it replaces the caller's `GOFLAGS`, so flags such as `-toolexec` or `-mod=mod` cannot be injected |
| `GOWORK` | `off` | building a workspace instead of the project's own module |

Environment variables take precedence over the go env file, so `go env -w`
settings are replaced too. Everything else (`GOMODCACHE`, `GOCACHE`,
`CGO_ENABLED`, `GOOS`…) is the caller's. A module that is not in the cache
is `go_module_not_in_cache` at the import that needs it, naming the module
("run go mod download"); a `go` directive newer than the installed toolchain
is `go_toolchain_too_old` at `go.mod`. The go command's progress lines
(`go: downloading …`) are informational, never diagnostics. A cold cache
therefore gives the same specific answer on every run, and a project that
needs nothing from the cache (the starter) passes with an empty one.

### Project discovery is layout's (ADR 0023)

E12-T01 (#67, ADR 0023) owns discovery, and all three commands consume it
through `ProjectSource` (`Load(ctx, root) (Workspace, []diagnostic.Diagnostic,
error)`), implemented by `LayoutSource`:

- `layout.Discover` supplies the manifest, the module, every node (its
  identity from its descriptor, never its directory) and every workflow, or
  its `*layout.Error`, whose diagnostics the report carries unchanged. Each
  condition keeps the single code ADR 0023 gives it: an unsupported layout
  is `layout_manifest_invalid` with field `layout`; a `blok.json` module
  that differs from `go.mod`'s is `layout_manifest_invalid` with field
  `module` (discovery compares them, so blok adds no second code); a node
  importing another node is `layout_node_imports_node`.
- `blok.json` is **strict, per ADR 0023**: an unknown field, trailing data
  or a path leaving the root is refused.
- When discovery fails, the manifest is still read through the strict
  `layout.LoadManifest` and go.mod's module directive is read, so the checks
  that need only those (`bindings`, `go-vet`) still run; `workflow-steps` is
  skipped and `inspect` describes nothing. No partial catalog is invented.
- The static reader no longer finds nodes or workflows. It reads only what
  layout does not carry — node options and workflow steps — at the Define
  call each layout position (`file:line`) names, plus HTTP routes and test
  and example functions from every package.
- For routes and test references, `goPackages` lists the module's Go files
  as the go command would (`vendor`, `testdata`, nested modules and `.`/`_`
  directories skipped), never following a symbolic link. Past 20,000 files,
  or past layout's per-file bound, it reports `layout_limit_exceeded`; a
  file it cannot parse is `layout_parse_failed`.

### The report (`blok-cli/v1`)

```json
{
  "version": "blok-cli/v1",
  "command": "check | test | inspect",
  "status": "passed | failed | error | interrupted",
  "exitCode": 0,
  "project": {"name", "module", "runtime", "layout", "triggers"},
  "checks": [{"name", "state", "reason?", "unresolved?"}],
  "tests": {"packages": [{"importPath", "dir", "status", "tests": [{"name", "status", "source?", "output?"}]}],
            "passed", "failed", "skipped", "incomplete"},
  "catalog": {"fields", "nodes", "workflows", "triggers", "unresolved", "redacted"},
  "diagnostics": [],
  "truncated": false
}
```

- `diagnostics` is always an array and every entry is the #28
  `diagnostic.Diagnostic` unchanged: `code`, `source`, `step`, `field`,
  `expected`, `actual`, `remediation`, `message`. Entries are ordered by
  `diagnostic.Sort` (code, source, step). `source` is `file:line[:col]`,
  slash-separated and relative to the project root — the form the compiler
  already uses — or a project-relative directory, or absent.
- Reports carry no timings, absolute paths or environment, so the same
  project produces the same bytes (asserted by running every golden case
  twice).
- A consumer rejects a `version` it does not know. Adding a field, a check,
  a status value for a new command, or a diagnostic code keeps `v1`.
  Renaming, removing or retyping a field, changing an exit code's meaning or
  reusing a diagnostic code for another condition requires `v2`.
- Bounds: 1,000 diagnostics, 10,000 tests, 64 output lines per test or
  package, 1 MiB per go output line. A report that drops anything says
  `"truncated": true`.
- Redaction: every diagnostic field passes `observe/redact` (ADR 0021) as
  the report is finished, whatever produced it — compiler text, test
  output, go's standard error. Test output and go's standard error are
  redacted as blocks: a PEM block, from its BEGIN line to its END line (or
  to the end of the kept output), becomes one marker line, every other line
  goes through `redact.Message`, and if the lines read together still look
  sensitive the whole block becomes one marker. Test names, package paths,
  directories and sources go through `redact.String`.

`--json` writes the report as one indented JSON document. Without it the
report is written for a person with stable line shapes:
`<source>: <code>: <message>`, then `  expected: …`, `  actual: …`,
`  fix: <remediation>`, and a final `blok <command>: <status>[, N problems]`.

### Exit codes

| Code | Meaning | Stdout | Stderr |
| --- | --- | --- | --- |
| 0 | ran; nothing to report | the report | empty |
| 1 | project invalid, a check or a test failed | the report | empty |
| 2 | usage error | empty | one line |
| 3 | the go command could not start (`go_toolchain_unavailable`) | the report | empty |
| 3 | an internal error (a panic); any go command it interrupted is killed | nothing | the panic and its stack |
| 4 | the report could not be written, including to a closed pipe (`--json \| head`): SIGPIPE is ignored for these commands | whatever was written | one line |
| 130 | SIGINT, SIGTERM, SIGHUP, SIGQUIT or a canceled context | the report, whole | empty |

Every invocation that gets past argument parsing writes exactly one report
whose `exitCode` is the process exit code. `blok new`, `blok generate` and
`blok version` keep their previous behaviour (exit 1 on any error).

### Diagnostic codes

| Code | Raised by | Condition |
| --- | --- | --- |
| `layout_manifest_missing`, `layout_manifest_invalid`, `layout_module_missing`, `layout_path_outside_root`, `layout_mixed`, `layout_invalid_runtime`, `layout_file_unowned`, `layout_file_unsupported`, `layout_parse_failed`, `layout_descriptor_missing`, `layout_descriptor_multiple`, `layout_descriptor_not_static`, `layout_descriptor_invalid`, `layout_descriptor_misplaced`, `layout_descriptor_constrained`, `layout_package_mismatch`, `layout_runtime_mismatch`, `layout_duplicate_identity`, `layout_duplicate_version`, `layout_path_collision`, `layout_ownership_overlap`, `layout_workflow_path_missing`, `layout_node_imports_node`, `layout_node_imports_workflow`, `layout_symlink_escape`, `layout_symlink_alias`, `layout_symlink_dangling`, `layout_symlink_loop`, `layout_limit_exceeded` | all | layout discovery's codes, passed through unchanged (ADR 0023); blok also raises `layout_parse_failed`, `layout_limit_exceeded` and `layout_file_unsupported` for the other Go files it lists |
| `project_unreadable` | all | the project directory cannot be opened |
| `bindings_types_missing`, `bindings_generate_failed`, `bindings_missing`, `bindings_not_generated`, `bindings_stale` | check | generated bindings absent, hand-written or out of date |
| `workflow_step_id_invalid`, `workflow_step_id_reserved`, `workflow_step_id_duplicate` | check | the step-id rules of `flow.Define` |
| `go_compile_error` | check, test | a positioned compiler error (identical from `go vet` and `go test`) |
| `go_vet_finding` | check | a `go vet` analyzer finding; `field` names the analyzer |
| `go_module_not_in_cache` | check, test | a required module is not in the module cache; `actual` names it ("run go mod download") |
| `go_module_missing` | check, test | no module in `go.mod` provides an imported package |
| `go_mod_needs_update` | check, test | `go.mod` needs changes the go command would have to write ("run go mod tidy") |
| `go_sum_missing` | check, test | `go.sum` lacks an entry the build needs |
| `go_toolchain_too_old` | check, test | the `go` directive needs a newer toolchain than the installed one |
| `go_toolchain_error` | check, test | any other unpositioned go command failure |
| `go_toolchain_unavailable` | check, test | the go command could not start (exit 3) |
| `test_failed` | test | a failing leaf test; `source` is the first `file:line` it reported |
| `test_package_failed` | test | a package failed outside any test (panic, `TestMain`, timeout) |
| `no_tests_ran` | test | go test passed but ran no test: nothing was verified |
| `interrupted` | all | the command was stopped; results are partial |

### Inspect: access and redaction policy

`inspect` is a static, read-only projection for the local user. It reads
only files inside the root (no symbolic links), never environment variables
or secret values, and never runs code. It describes `node.Define` /
`node.MustDefine` calls (identity, version, description, purity, effects,
capabilities, remote boundary), `flow.Define` / `flow.MustDefine` calls
(name, version, durability, steps) and `app.Route` literals (HTTP method,
path, workflow), each with its source position. Test and example
references are the `Test*` and `Example*` functions of the component's
package and of every package importing it directly: references, not
coverage.

- **Field policy.** `--fields` selects from `nodes, workflows, triggers,
  descriptions, sources, tests, examples, schemas`. An unselected field is
  absent, `catalog.fields` lists what was projected, and an unknown name is
  a usage error (exit 2), never ignored. `schemas` is never selected by
  default: a schema literal is application content, not catalog metadata.
- **Redaction.** Every projected string goes through `observe/redact`
  (ADR 0021): identifiers, versions, paths and methods through
  `redact.String`, descriptions through `redact.Message`, schema literals
  through `redact.JSON` (structured: a sensitive key hides its value; the
  result is canonical JSON with sorted keys). A schema that is not JSON is
  replaced by the marker rather than projected raw. `catalog.redacted`
  counts replacements.
- **Identity and unresolved detail.** Which nodes and workflows exist, and
  their identities, are layout's: an identity discovery cannot read
  statically is `layout_descriptor_not_static`, and inspect then describes nothing. A
  node option that is not a direct option call, or a step id that is not a
  literal, is listed under `catalog.unresolved` with its position and
  reason. Nothing is ever evaluated or guessed.

### Cancellation, and blok dying

`blok check|test|inspect` catch SIGINT, SIGTERM, SIGHUP and SIGQUIT (only
these commands, so Ctrl+C still ends an interactive `blok new` at once). The
first signal cancels the command's context. A go command blok starts runs in
its own process group; on cancellation that group receives SIGINT, and
SIGKILL after `InterruptGrace` (5 s) if it has not exited (a group that
ignores SIGINT is killed at the grace). Further signals are absorbed. The
report is then written whole: every check, package and test result gathered
before the stop, a test that had started but not finished as `incomplete`,
an `interrupted` diagnostic, `status: interrupted` and exit 130. A context
already canceled starts no go command.

A panic while go output is read kills the group at once, then reaches the
CLI, which reports it on stderr and exits 3.

blok can also die without running any code: SIGKILL, or a crash. For that,
each go command gets a **pipe guard**: a `/bin/sh` process, in its own process
group, whose standard input is a pipe only blok can write to. The kernel
closes blok's end whenever blok exits, however it exits; the guard reads
end-of-file and kills the go command's process group, test binaries
included. When the go command finishes normally blok kills the guard before
closing the pipe, so the guard never signals a group id the system may have
reused. One mechanism serves macOS and Linux. Linux's `Pdeathsig` was not
chosen: it would stop only the direct child, `go`, and never the test binary
that `go` starts. The guard needs `/bin/sh`; if it cannot start, the command
does not run (`go_toolchain_unavailable`, exit 3). The one window left open
is the moment between starting the go command and starting its guard.

## Compatibility

- **Additive**: the `check`, `test` and `inspect` commands; the
  `internal/tooling/devtool` package; the `blok-cli/v1` report, a new
  machine-readable contract; the help text lists the new commands.
- **Behavioural, none for existing commands**: `main` now routes through
  `execute`; `new`, `generate`, `version` and `help` keep their output and
  exit codes.
- **Unchanged**: `internal/diagnostic` (its #28 shape is reused as is),
  `internal/tooling/graphcheck` (no longer used by blok check, since layout
  owns the node-import rule), `internal/generate`, `internal/scaffold`
  and `internal/tooling/layout`, which this change consumes without
  modifying.
- **Behavioural, within this unreleased PR**: adopting layout discovery
  made `blok.json` strict (an unknown field is refused) and replaced this
  change's earlier `project_*`, `node_import_forbidden` and `source_*`
  codes with layout's, and the `project` and `node-imports` checks with one
  `layout` check. None of those shipped.
- The code registry is `devtool.Codes`; `TestCodeRegistryMatchesSourceAndADR`
  keeps it, the code literals in the source and the table above identical.

## Evidence

- `testdata/tooling/fixtures.json` predeclares 29 cases (35 runs across the
  two layouts) against real applications created by `scaffold.Create` and
  tidied: exit code, status, every diagnostic's code and position, output,
  error and effect counts (effects: project files created, changed or
  removed by the command — always 0). `internal/tooling/devtool`
  `TestFixtures` runs them.
- Golden JSON for 12 runs (`testdata/tooling/golden/*.json`), each run twice
  and required byte-identical; golden human stdout for four real processes
  (`cli-*.txt`), with stderr empty and exit codes asserted.
- CLI and API agreement: `cmd/blok` `TestToolStreamsAndExitCodes` requires
  each command's `--json` stdout to equal `WriteJSON` of the API's report,
  and the broken project's CLI output to equal the API golden.
  `TestCheckAndTestAgree` requires the same compile-error diagnostic from
  check and test, and a statically reported duplicate step to fail the
  application's test through `flow.Define`.
- Repair: `TestRepairWithOneDiagnostic` (both layouts) breaks the
  application three ways; each yields exactly one diagnostic, and doing only
  what it says (run `blok generate`; change the token on its line; stop
  importing the other node) makes check pass.
- Interrupts on macOS (darwin/arm64) and Linux (linux/arm64, the
  `golang:1.27.1` container image), Go 1.27.1: `TestToolInterruptFlushesAndExits130`
  sends a real SIGINT to blok, SIGINT to its process group (Ctrl+C) and
  SIGTERM while an application test sleeps; each exits 130 with a whole JSON
  report, the finished test `pass`, the running one `incomplete`, empty
  stderr, and the test binary gone. `TestInterruptFlushesPartialResults`
  does the same through the API.
- Writer failure: exit 4 with a stderr line for an in-process failing writer,
  for a real process whose standard output refuses writes, and for one
  writing to a closed pipe. A missing go command exits 3.
- No network, no writes: `TestNoNetworkNoWritesUnderAHostileEnvironment`
  gives check and test `GOTOOLCHAIN=auto`, a `toolchain go1.27.9` line,
  `GOFLAGS=-mod=mod`, an empty module cache, a dependency and a logging
  module proxy. The proxy receives zero requests, no file changes, both cold
  runs report the identical `go_module_not_in_cache` at the import, and the
  project passes once the module is in the cache. A newer `go` directive is
  `go_toolchain_too_old`, again with no request.
- Blok dying: `TestToolDeathKillsTheGoCommand` sends SIGKILL to blok while an
  application test runs; the guard kills the test binary and go, on macOS
  and in the Linux container. Its first Linux run failed, and found a real
  defect: dash rejects `kill -KILL -- -PGID`, so the guard did nothing there
  while bash (macOS) accepted it. The guard now writes `kill -KILL -PGID`.
  `TestPanicKillsTheGroup`, `TestGraceKillsAGroupThatIgnoresInterrupts` and
  the SIGHUP and SIGQUIT rows of the interrupt test cover the other ways
  blok stops.
- Secrets: the `test-secret-output` and `inspect-secret-route` fixtures and
  `TestStderrIsRedactedAndProgressIgnored` require a PEM key, an AWS key id
  in a test name and route, and a bearer token in go's stderr to be absent
  from the report.

## Limits

- Layout positions some conditions by file only (`layout_node_imports_node`
  names the importing file, not the import's line); blok reports layout's
  diagnostic unchanged rather than a second, differently positioned one.
- Workflow validation in `check` is static and literal-only; lowered-program
  compilation runs in the application's tests. Steps built in helper
  functions are not read (and are counted as unresolved when visible).
- Only the manifest's `types` file is checked for stale bindings; other
  generated files do not record their input.
- Inspect reads HTTP routes from `app.Route` literals only; the other eight
  trigger kinds and registrations built at run time are not described.
- Test and example references are direct-importer references, not coverage.
- `go_compile_error` and `go_vet_finding` text is the Go toolchain's; the
  goldens pin Go 1.27.1 and may change with the toolchain.
- Interrupt and process-group semantics are verified on darwin/arm64 and
  linux/arm64 (a container, not a bare host); amd64 hosts are not run here.
  Windows kills the go command without a process group, has no pipe guard,
  and a test binary may outlive it: unverified, and owned by the Windows
  track (#156, #157).
- Redaction is observe/redact's pattern matching (ADR 0021): it fails
  closed and can over-redact, as it does a path like `secret_test.go:7`
  (read as a `secret…: value` pair); it cannot find a credential it does not
  recognise.
- Pinning `GOFLAGS` drops any other flags the caller set there (for example
  `-tags`); blok check and test have no flag to pass them yet.
- The go command's own telemetry follows the user's `go telemetry` mode; it
  is not a module or toolchain fetch and blok does not change it.
