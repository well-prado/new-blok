# ADR 0023: Manifest-based layout discovery

- Status: implementation in review for E12-T01 (#67)
- Date: 2026-10-05
- Roadmap: E12-T01 ([#67](https://github.com/well-prado/new-blok/issues/67));
  builds on E11-T01 (#64, `blok new` / `blok generate`, PR #174), the
  JSON-key accessors (#240) and the modular-import and fixture governance of
  E01-T03 (#23)
- Owners: `internal/tooling/layout` (discovery, catalog, diagnostics). The
  scaffold (`internal/scaffold`), `blok generate` (`cmd/blok`) and the import
  graph check (`internal/tooling/graphcheck`) consume it
- Follow-up owners: E12-T02 (#68) enforces node independence across every
  language's import graph; this record covers Go imports only

## Context

Architecture §3 allows two layouts for the same application:

| Layout | Node directory |
| --- | --- |
| classic (default) | `runtimes/<runtime>/nodes/<node>/` |
| unified | `nodes/<runtime>/<node>/` |

and says that layout never changes node identity. Before this record nothing
read a project back. Each tool guessed for itself: `scaffold.NodeDir` mapped
a layout to a path, `blok generate` decoded `blok.json` with no validation
(an unknown field was ignored and a `types` path such as `../../x.go` was
followed outside the project), and `graphcheck` decided which node owned a
package by looking for any `nodes` element anywhere in its absolute path.
That last guess was wrong in both directions: a classic node's nested
package (`runtimes/go/nodes/alpha/rates`) was treated as a second node, so a
legal same-node import failed, and a checkout whose own path contained
`nodes/` collapsed every package into one "node", so a real cross-node import
passed.

## Decision

### Inputs: manifests and descriptors, never execution

Discovery reads exactly these, through an `os.Root` opened on the
symlink-resolved project root:

1. `blok.json` — decoded with unknown fields refused and trailing data
   refused. `layout` must be `classic` or `unified`. `types` and every
   `workflows` entry must be a clean, relative, forward-slash path inside the
   root; workflow paths must not overlap each other.
2. `go.mod` — its `module` directive, read as text. A non-empty manifest
   `module` must equal it.
3. Go source **syntax** (`go/parser`, `SkipObjectResolution`) of non-test
   files under node directories and workflow paths.
4. A foreign-runtime node's `node.json`: its `name`, `version` and optional
   `runtime`. The remaining `node.Descriptor` fields are the runtime
   catalog's to validate.

Discovery does not type-check, build, `go list`, `go run`, load plugins or
run package initialization. The package imports none of `os/exec`, `plugin`,
`go/build`, `go/importer`, `go/types`, `syscall` or `unsafe`, and a test
keeps it that way. Fixtures carry an `init` that panics; nothing compiles it.

### Identity comes from descriptors

A Go node directory's identity is the one `node.Define` / `node.MustDefine`
call in its non-test files. The call is recognised under an import alias, a
dot import and explicit type arguments; a blank import names nothing. The
name and version must be string literals, package-level string constants or
`+` concatenations of those. Anything computed at run time is
`layout_descriptor_not_static`: discovery reports it instead of guessing,
because a guess would make identity depend on something other than the
descriptor. Repeating the same identity inside one node is one node; two
different identities are `layout_descriptor_multiple`.

A workflow is each static `flow.Define` / `flow.MustDefine` call whose first
argument is a `flow.Spec` composite literal (keyed or positional) under a
workflow path. Names use the node identity grammar
`^[a-z][a-z0-9_/-]{0,127}$` (no `..`), versions `major.minor.patch`. This is
stricter than `flow.Define`, which only requires non-empty values; it is a
discovery requirement, not a change to `flow`.

Under the `go` runtime a `node.json` is a `layout_runtime_mismatch`, and
under any other runtime a Go `node.Define` is one; a `node.json` whose
`runtime` differs from its directory is one too. Runtime is a location
property recorded in the catalog; it is not part of identity.

### Explicit workflow paths

`blok.json` gains an optional `workflows` array of project-relative
directories or `.go` files. When it is absent, the single path `workflows`
is used, so every existing manifest keeps its meaning. Only declared paths
are read: a `flow.Define` elsewhere is not a workflow. An explicitly declared
path that does not exist is `layout_workflow_path_missing`; the implicit
default may be absent. A workflow path inside, or containing, a node
directory is `layout_ownership_overlap`.

### Ownership

Every regular file under a node directory, however deeply nested, belongs to
that node. Names starting with `.` or `_`, and `testdata`, `vendor` and
`node_modules` directories, are skipped as the go tool and package managers
own them. A Go file in a node may import the standard library, other
modules, any package of the same node, and shared module packages that are
neither a node nor under a workflow path. Importing another node is
`layout_node_imports_node`; importing a workflow package is
`layout_node_imports_workflow`. `layout.NodeRoot` is the single
root-anchored rule both discovery and `graphcheck` now use.

Two discovered paths equal ignoring letter case are `layout_path_collision`:
they are one file on case-insensitive file systems. Unicode normalization is
not folded (limit below).

### Symbolic links: classified, never followed

Directory entries are read with `Lstat` semantics. Every link met where
discovery reads (the manifest, `go.mod`, the `nodes`/`runtimes` bases,
runtime and node directories, node
trees, workflow paths, and every directory above a declared workflow path)
is reported and not followed:

| Code | Meaning |
| --- | --- |
| `layout_symlink_escape` | resolves, lexically at any hop or after resolution, outside the root |
| `layout_symlink_alias` | resolves inside the root: a second path to source that already has an owner |
| `layout_symlink_dangling` | points to nothing |
| `layout_symlink_loop` | part of a cycle, or exceeds 40 hops |

Classification checks containment before every hop, so it never inspects
anything reachable only by leaving the root, and it reads no contents.
Independently, every read goes through `os.Root`, which refuses to leave
the root even if the tree changes during discovery. Mutation M10 shows the
two layers are independent: removing the per-hop check alone is still
caught by the post-resolution check.

### Canonical catalog and digest

`Project.Catalog()` is the layout-independent view:

```json
{"version":1,
 "nodes":[{"name":"app/calculate-quote","version":"1.0.0","runtime":"go",
           "files":["bindings_gen.go","quote.go","types.go"]}],
 "workflows":[{"name":"quote","version":"1.0.0"}]}
```

It holds identities, runtimes and node-relative file names, never a project
path, a directory name or the layout. Nodes and workflows sort by
`name@version`; files sort. `Canonical()` is the compact `encoding/json`
encoding (struct field order fixes key order); `Digest()` is
`sha256:<hex>` of it. The same application in both layouts, with different
node directory names, encodes to the same bytes. This catalog is a tooling
document; it is not the worker protocol's `contract/runtime.CatalogDigest`,
which hashes full node descriptors.

### Bounds

| Bound | Value |
| --- | --- |
| node directories | 1024 (`contract/runtime.MaxCatalogNodes`) |
| workflows | 1024 |
| files read | 10000 |
| directory depth | 32 |
| file size | 1 MiB |
| diagnostics | 256, then one `layout_limit_exceeded` |

Each overrun is `layout_limit_exceeded`; discovery never reads without end.

### Diagnostics contract

A failed discovery returns `*layout.Error` holding
`internal/diagnostic.Diagnostic` records. Every record has a stable `Code`
from the table below, a `Source` that is a project-relative forward-slash
path (with `:line` for a Go call site), a `Message`, and a `Remediation`.
`Expected`/`Actual` are filled where meaningful. No record contains an
absolute or machine-specific path. Records sort by code, source, step,
field, expected, actual and message, so the same project yields the same
bytes on every machine. All problems are collected, not only the first.
Tools match on `Code` and `Source`; message wording may improve.

`layout_manifest_missing`, `layout_manifest_invalid`, `layout_module_missing`,
`layout_path_outside_root`, `layout_mixed`, `layout_invalid_runtime`,
`layout_file_unowned`, `layout_file_unsupported`, `layout_parse_failed`,
`layout_descriptor_missing`, `layout_descriptor_multiple`,
`layout_descriptor_not_static`, `layout_descriptor_invalid`,
`layout_descriptor_misplaced`, `layout_runtime_mismatch`,
`layout_duplicate_identity`, `layout_duplicate_version`,
`layout_path_collision`, `layout_ownership_overlap`,
`layout_workflow_path_missing`, `layout_node_imports_node`,
`layout_node_imports_workflow`, `layout_symlink_escape`,
`layout_symlink_alias`, `layout_symlink_dangling`, `layout_symlink_loop`,
`layout_limit_exceeded`.

`layout_duplicate_identity` is a repeated `name@version` among nodes (across
runtimes too) or among workflows. `layout_duplicate_version` is one name at
two versions in one source tree: older versions are retained artifacts or
packages (ADRs 0015, 0018), not parallel source directories.

### Wiring

- `scaffold.Manifest` is now an alias of `layout.Manifest` and
  `scaffold.NodeDir` delegates to `layout.NodeDir`. Output is unchanged:
  a golden captured from `origin/main` before the refactor pins every
  starter file's bytes in both layouts.
- `blok generate` without an input reads `blok.json` through
  `layout.LoadManifest`. With no `blok.json` it still falls back to
  `internal/app/types.go`.
- `graphcheck` decides node ownership with `layout.NodeRoot` relative to the
  analyzed root.

No new CLI command is added; `blok check` and migration between layouts
remain with their own issues.

## Compatibility classification

| Change | Class |
| --- | --- |
| `internal/tooling/layout` package, catalog document and diagnostic codes | additive (internal package; the codes are the tooling contract) |
| optional `blok.json` `workflows` field, omitted when empty | additive; existing manifests are byte-identical and keep their meaning |
| `scaffold.Manifest` becomes a type alias | source-compatible; same fields, tags and JSON |
| `blok generate` refuses an invalid, linked or root-escaping `blok.json` instead of silently falling back or following it | behavioral (fail closed); valid manifests behave exactly as before |
| `graphcheck` node ownership anchored at the analyzed root, nested node packages owned by their node | behavioral bug fix; the repository and existing fixtures produce the same diagnostics |
| starter output | unchanged (golden) |

No wire, journal, artifact or worker contract changes.

## Verification record

Focused suites: `go test -count=1 ./internal/tooling/... ./internal/scaffold
./cmd/blok`, then the repository gates listed in the PR.

- 26 synthetic `txtar` cases under
  `internal/tooling/layout/testdata/cases`, each with a predeclared exact
  catalog and digest or exact sorted `[code, source]` diagnostics, and
  output/error/effect counts in `testdata/provenance.json` (Apache-2.0,
  synthetic, no private data); 22 cases are negative.
- `classic-shop` and `unified-shop` are one application (nested Go node
  files, a shared domain import, a nodejs node beside the Go node) with
  different directory names; both produce digest
  `sha256:1f255e6ce8145ae2eb41650fa25d97942fb321da464285443063fd71f309ca3e`.
- The real starters of `blok new` in both layouts discover to digest
  `sha256:4bb9149a1605dc2d38950cec15a4d4fd195eaf0574d1c22c47aec4b5556af4af`,
  and `cmd/blok`'s fresh-application test builds each starter, lowers its
  workflow in the starter's own module, and asserts both layouts lower
  byte-identical programs, give the same `contract/runtime.CatalogDigest`,
  and that static discovery reports the identities the built code does.
- The case-collision test needs a case-sensitive file system; it skips on
  default APFS and passed on a case-sensitive APFS disk image.
- Mutations, each run against the focused suites and reverted:

| Mutation | Result |
| --- | --- |
| M1 identity from directory name | red |
| M2 symlinked directories followed | red |
| M3 duplicate identity/version check removed | red |
| M4 node directory in the catalog (layout-dependent output) | red |
| M5 same-node imports rejected | red |
| M6 case collision not folded | red |
| M7 explicit workflow paths ignored | red |
| M8 nested node packages become separate nodes | red |
| M9 unknown manifest fields accepted | red |
| M10 per-hop escape check removed | green: the post-resolution check still catches it (independent layer) |
| M10b both escape checks removed | red |
| M11 a link above a declared workflow path followed | red |

## Limits

- Windows is not certified. `go vet` passes for `windows/amd64` and
  `windows/arm64`; no test ran on Windows, and the symlink fixtures skip
  where links cannot be created.
- Case folding is `strings.ToLower`; Unicode normalization (NFC/NFD) is not
  folded.
- Only Go import graphs are checked; foreign-runtime imports are E12-T02.
- Discovery is a snapshot; a tree changing during discovery can produce a
  stale result, though `os.Root` still prevents reads outside the root.
- A descriptor identity built from anything other than literals and
  package-level string constants is refused, even if valid at run time.
