# ADR 0023: The check, test and inspect CLI contract

- Status: implementation in review for E11-T02 (#65)
- Date: 2026-10-05
- Roadmap: E11-T02 ([#65](https://github.com/well-prado/new-blok/issues/65));
  builds on E11-T01 (#64: `blok new`, `blok generate`, `blok.json`), E02-T04
  (#28: `internal/diagnostic`), the node-independence graph check
  (`internal/tooling/graphcheck`) and ADR 0021's redaction boundary
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
| `check` | `go.mod`, `blok.json`, Go source (parsed, never loaded) | `go vet -json ./...`, which type-checks and compiles every package and runs none of them |
| `test` | `go.mod`, `blok.json` | `go test -json ./...`: the application's own tests, in their own process |
| `inspect` | `go.mod`, `blok.json`, Go source (parsed) | nothing |

`check` runs, in order: `project` (manifest and module), `node-imports`
(graphcheck's node-independence rule), `bindings` (the manifest's types file
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

### Project discovery is a seam

`ProjectSource` is the narrow port both `check` and `inspect` consume:
`Load(ctx, root) (Workspace, []diagnostic.Diagnostic, error)`, where a
`Workspace` is the root, module, manifest and Go package file lists.
`DirectorySource` is the interim implementation: `blok.json` and `go.mod` at
the root plus every package directory the go command would see (`vendor`,
`testdata`, nested modules and `.`/`_` directories skipped). It never
follows a symbolic link, so nothing outside the root is read, and it is
bounded (20,000 Go files, 8 MiB per file, 1 MiB per manifest).
**E12-T01 (#67) replaces `DirectorySource`** with its manifest-based
discovery of both layouts; nothing else changes. This ADR does not decide
node identity from directories: inspect reports what descriptors declare.

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
| 4 | the report could not be written | whatever was written | one line |
| 130 | SIGINT, SIGTERM or a canceled context | the report, whole | empty |

Every invocation that gets past argument parsing writes exactly one report
whose `exitCode` is the process exit code. `blok new`, `blok generate` and
`blok version` keep their previous behaviour (exit 1 on any error).

### Diagnostic codes

| Code | Raised by | Condition |
| --- | --- | --- |
| `project_go_mod_missing`, `project_go_mod_invalid` | all | no readable `go.mod`, or no module directive |
| `project_manifest_missing`, `project_manifest_invalid` | all | no readable `blok.json`, invalid JSON, or a `types` path leaving the project |
| `project_module_mismatch` | all | `blok.json`'s module differs from `go.mod`'s |
| `project_layout_unsupported` | all | layout other than `classic` or `unified` |
| `project_too_large`, `project_unreadable` | all | discovery bounds exceeded, root unreadable |
| `node_import_forbidden` | check | a node package imports another node (graphcheck's code) |
| `bindings_types_missing`, `bindings_generate_failed`, `bindings_missing`, `bindings_not_generated`, `bindings_stale` | check | generated bindings absent, hand-written or out of date |
| `workflow_step_id_invalid`, `workflow_step_id_reserved`, `workflow_step_id_duplicate` | check | the step-id rules of `flow.Define` |
| `go_compile_error` | check, test | a positioned compiler error (identical from `go vet` and `go test`) |
| `go_vet_finding` | check | a `go vet` analyzer finding; `field` names the analyzer |
| `go_toolchain_error` | check, test | an unpositioned go command failure (for example a module that needs `go mod tidy`) |
| `go_toolchain_unavailable` | check, test | the go command could not start (exit 3) |
| `test_failed` | test | a failing leaf test; `source` is the first `file:line` it reported |
| `test_package_failed` | test | a package failed outside any test (panic, `TestMain`, timeout) |
| `no_tests_ran` | test | go test passed but ran no test: nothing was verified |
| `source_unreadable`, `source_parse_error` | inspect | a Go file could not be read or parsed (check leaves syntax to `go vet`) |
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
- **Unresolved.** A declaration whose identity is not a literal, or an
  option that is not a direct option call, is listed under
  `catalog.unresolved` with its position and reason. It is never evaluated
  and never guessed.

### Cancellation

`blok check|test|inspect` catch SIGINT and SIGTERM (only these commands, so
Ctrl+C still ends an interactive `blok new` at once). The first signal
cancels the command's context. A go command it started runs in its own
process group; on cancellation that group receives SIGINT, and SIGKILL after
`InterruptGrace` (5 s) if it has not exited, so neither go nor a test binary
outlives blok. Further signals are absorbed; blok exits within the grace. The
report is then written whole: every check, package and test result gathered
before the stop, a test that had started but not finished as `incomplete`,
an `interrupted` diagnostic, `status: interrupted` and exit 130. A context
already canceled starts no go command.

## Compatibility

- **Additive**: the `check`, `test` and `inspect` commands; the
  `internal/tooling/devtool` package; the `blok-cli/v1` report, a new
  machine-readable contract; the help text lists the new commands.
- **Behavioural, none for existing commands**: `main` now routes through
  `execute`; `new`, `generate`, `version` and `help` keep their output and
  exit codes.
- **Unchanged**: `internal/diagnostic` (its #28 shape is reused as is),
  `internal/tooling/graphcheck`, `internal/generate`, `internal/scaffold`
  and `blok.json` (read leniently: unknown fields are accepted, so #67 can
  add fields without breaking check).

## Evidence

- `testdata/tooling/fixtures.json` predeclares 23 cases (29 runs across the
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
- Writer failure: exit 4 with a stderr line for an in-process failing writer
  and for a real process whose standard output refuses writes. A missing go
  command exits 3.

## Limits

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
  Windows kills the go command without a process group and a test binary
  may outlive it: unverified, and owned by the Windows track (#156, #157).
