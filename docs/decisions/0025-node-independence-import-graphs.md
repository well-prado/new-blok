# ADR 0025: Node independence across language import graphs

- Status: implementation in review for E12-T02 (#68)
- Date: 2026-10-05
- Roadmap: E12-T02 ([#68](https://github.com/well-prado/new-blok/issues/68));
  builds on E12-T01 (#67, ADR 0023: discovery, node ownership, link and
  case rules) and E08-T03 (#52, the Node.js SDK and worker)
- Owners: `internal/tooling/ownership` (adapters, graph walk, diagnostics)
  and `testdata/imports` (fixtures). `blok check` (E11-T02, #65, ADR 0024)
  is the intended consumer; this record adds no CLI command
- Windows: not certified (see Limits)

## Context

Architecture §3 says nodes never import other nodes; workflows compose
them. ADR 0023 enforces part of that for Go only: a Go file inside a node
may not import another node's package or a workflow package *directly*.
Three gaps remained:

1. **Transitive evasion.** Node `a` imports a shared helper, the helper
   imports node `b`. Every direct edge is legal; the node still depends on
   another node.
2. **Aliases.** A second name for another node — a `go.mod` `replace`, a
   tsconfig `paths` alias or `baseUrl`, a package name with `exports`, a
   `package.json` `imports` entry, a `file:` dependency, an installed link —
   is not visible to a direct path check.
3. **Other languages.** Node.js is the first external runtime (AGENTS.md);
   its nodes had no source-level check. The Node SDK's
   `runtime/nodejs/scripts/ownership.mjs` checks the worker's executable
   graph at authoring time but needs Node and TypeScript installed and
   follows links.

A text search would answer none of these reliably: it cannot tell an
import from a string, a comment or a regular expression, and it cannot
resolve where a specifier lands.

## Decision

### The adapter contract

Each runtime supplies an `ownership.Adapter`:

```go
type Adapter interface {
	Runtime() string                 // the runtime directory it serves
	Roots(node layout.Node) []string // units the node's analysis starts from
	Analyze(unit string) Analysis    // parse one unit, resolve its imports
}
```

A *unit* is a project-relative path: a package directory for Go, a file
for Node.js. `Analyze` returns `Edge`s (an import's `file:line`, its
specifier and every unit it may resolve to) and `Findings` (diagnostics).
An adapter must parse source with a real grammar and resolve each import
with its toolchain's rules; where it cannot resolve, it reports, never
guesses. A runtime with no adapter (today: every runtime except `go` and
`nodejs`) is reported `ownership_runtime_unsupported` and its nodes are
never verified.

The engine is shared. For each node discovered by `layout.Discover` it
walks breadth-first from the node's roots, following edges into the node's
own units and into shared code, and stops at a unit owned by another node
(`layout.NodeRoot`, the root-anchored rule of ADR 0023) or under a declared
workflow path. Workflows and composition roots are never origins, so they
may import as many nodes as they like.

### What "verified" means

A node's `Status` is one of:

| Status | Meaning |
| --- | --- |
| `verified` | every import in the node's reachable graph was parsed and resolved by a checked adapter, and none reaches another node or a workflow, and the graph is acyclic |
| `violation` | the reachable graph crosses into another node or a workflow, or contains an import cycle |
| `unverified` | some part of the reachable graph could not be checked: a computed or evaluating form, an unresolved specifier, a link, a case mismatch, a parse failure, an unsupported file type, an invalid resolution config, a bound overrun, or a runtime with no adapter |

`Report.Verified()` is true only when every node is verified and there is
no diagnostic; `Report.Err()` returns an `*ownership.Error` otherwise.
`Class(code)` maps each code to `violation` or `unverified` so `blok check`
can report the distinction without a second table. A violation found next
to an unverified region still reports the violation. A computed import is
never treated as harmless: it is `unverified`, never `verified`.

Cycles fail. Go refuses import cycles at compile time; in JavaScript a
cycle makes a module's view of its imports depend on evaluation order
(a binding in its temporal dead zone, a partially filled CommonJS
`exports`). A node's dependency graph must be acyclic.

### Go adapter

Go source is parsed with `go/parser` (`SkipObjectResolution`), never
type-checked, built, `go list`-ed or run, as in `internal/tooling/graphcheck`
and ADR 0023. A unit is a package directory; roots are the directories of
the node's non-test `.go` files. Import paths resolve by module path: the
application module (`go.mod`'s `module`) maps to the project root, and a
`go.mod` `replace` whose target is a local `./` or `../` path maps that
module path to its directory — `replace example.com/billing =>
./nodes/go/b` makes `example.com/billing` node `b`. Other modules and the
standard library are external. A path inside the module with no non-test
Go files, or inside a nested module, is unresolved. Imports in
build-constrained files count like any other (ADR 0023). `import "plugin"`
and `//go:linkname` are `ownership_unsupported_form`: each reaches code
without a resolvable import. `_test.go` files are not part of a node, as
in ADR 0023.

Layout discovery keeps reporting a *direct* Go node or workflow import as
`layout_node_imports_node` / `layout_node_imports_workflow`; the ownership
check runs on a discovered project and adds transitive, aliased, linked and
mis-cased crossings.

### Node.js and TypeScript adapter

A unit is a file. Roots are the node's `.ts`, `.mts`, `.cts`, `.js`,
`.mjs` and `.cjs` files (declaration files included), except `*.test.*`
and `*.spec.*`, which — like `_test.go` — are not part of a node; a test
file that is imported is analyzed like any other.

**Parsing.** `internal/tooling/ownership/jslex.go` is a JavaScript and
TypeScript lexer written for this check, in pure Go with no dependency. It
tokenizes the whole file following the ECMAScript lexical grammar: line
and block comments, string literals with every escape decoded (`\x2e`,
`\u{2e}`, legacy octal `\56`, line continuations), template literals with
nested substitutions, identifier escapes (`require` is `require`),
numeric literals, and the regular-expression-versus-division goal decided
from the paren and brace context (a `/` after `if (…)` or a block `}`
starts a regular expression; after an operand, an object literal `}` or a
TypeScript non-null `!` it divides). An unterminated literal, comment or
expression, or an unbalanced bracket, fails the whole file
(`ownership_parse_failed`), so a mis-tokenization cannot drop an import: it
can only make the file unverified. A recognizer then reads the token
stream:

| Form | Treated as |
| --- | --- |
| `import … from "s"`, `import "s"`, `import type …`, `import defer/source …`, with attributes | edge |
| `export * from "s"`, `export * as n from "s"`, `export { … } from "s"`, `export type … from "s"` | edge |
| `import x = require("s")`, `export import x = require("s")` | edge |
| `require("s")`, `module.require("s")`, `require.resolve("s")` | edge |
| `import("s")`, `typeof import("s")`, `import.meta.resolve("s")` | edge |
| `/// <reference path="…">` / `<reference types="…">` | edge |
| `import(expr)`, `require(expr)`, a template with a substitution | `ownership_dynamic_import` |
| `eval(…)`, `Function(…)`/`new Function`, `createRequire`, `getBuiltinModule`, `mainModule`, `_load`, `dlopen`, `importScripts`, `ShadowRealm`, `new Worker`, `require` used as a value, `require.<member>` other than `resolve`/`main`, `module[…]`, the `vm` and `module` built-ins, `data:`/`http(s):` specifiers, an unrecognized `import`/`export` clause | `ownership_unsupported_form` |

Type-only imports are edges: a node depending on another node's types is
still coupled to it. This is stricter than the SDK's executable-graph
checker, which excludes type-only edges because they do not run. Strings,
comments, template text, regular expressions, property names
(`{ require: … }`, `x.import`) and methods named `import`/`require` are not
imports. `.jsx` and `.tsx` (JSX has no checked grammar here), `.node` and
`.wasm` are `ownership_source_unsupported`.

**Resolution.** Every candidate any of Node.js or TypeScript could select
is an edge (the union), so a declaration file and a runtime file cannot
disagree unnoticed:

1. Relative specifiers: the exact file; TypeScript's source for a `.js`,
   `.mjs`, `.cjs` or `.jsx` specifier (`.ts`, `.tsx`, `.d.ts`; `.mts`;
   `.cts`); every probe extension (`.ts .tsx .d.ts .mts .d.mts .cts
   .d.cts .js .jsx .mjs .cjs .json .node`); and, for a directory,
   `package.json` `main`/`module`/`types`/`typings` and `index.*`.
2. `#` specifiers: the nearest `package.json` `imports`, with Node's exact
   and `*` pattern matching (longest prefix); a target may be a `./` path
   or a package.
3. Bare specifiers, in order: built-ins (external); the nearest
   `tsconfig.json` or `jsconfig.json` `paths` (exact key, else the longest
   `*` prefix; substitutions relative to `baseUrl` when set, else to the
   config declaring `paths`; JSON with comments and trailing commas;
   relative `extends` chains, at most 8 deep, cycles refused; an
   uninstalled package `extends` is skipped) and `baseUrl`; a local package
   whose `package.json` `name` matches — any `package.json` inside a node
   directory, a root `workspaces` directory (`dir`, `dir/*`, `dir/**` one
   level) or the importing file's own package (self-reference) — resolved
   through its `exports` (conditions are a union; patterns and subpaths per
   Node; an unexported subpath is unresolved) or main fields and files; a
   dependency declared in a `package.json` at or above the file — `file:`,
   `link:` and `portal:` resolve to that directory, `workspace:` with no
   matching workspace is unresolved, any other version is external;
   finally an installed `node_modules/<name>` directory is external, and an
   installed link is classified (below). Anything else is
   `ownership_import_unresolved`.

Absolute paths and `file:` URLs are `ownership_import_outside_root`.
Nothing is installed, built or run; `node_modules` is read only to
recognise an installed third-party package or a config it provides.

### Links and letter case (ADR 0023)

All reads go through one `projectFiles` view over an `os.Root` opened on
the discovered root. Each path element is matched against its directory's
listing **by exact name**, so resolution gives the same answer on
case-sensitive and case-insensitive file systems. An element that matches
only ignoring case is `ownership_import_case_mismatch` (unverified); its
on-disk target is still checked for ownership, so a mis-cased path into
another node is also a violation. A link anywhere in a resolved path is
never followed: it is classified with ADR 0023's codes
(`layout_symlink_escape`, `_alias`, `_dangling`, `_loop`) by
`layout.ClassifyLink`, and an alias's lexical target is still checked for
ownership, so a link into another node is a violation. An installed
`node_modules` link that resolves outside the project (a package manager's
store) is external; one that resolves into a node or workflow is reported
and fails. Files are opened with `layout.OpenRegular` (non-blocking on
Unix, type re-checked on the handle, hard links refused).

### Diagnostics

Records are `internal/diagnostic.Diagnostic`, sorted by code, source, step,
field, expected, actual and message, deduplicated, at most 256 then one
`ownership_limit_exceeded`. `Source` is a project-relative `file:line` (the
node file a violating chain starts from, or the import's location), a unit,
or a link path; no record holds an absolute path. Violations name the
target unit in `Actual` and the whole chain in `Message`. Tools match on
`Code` and `Source`; wording may improve.

| Code | Class |
| --- | --- |
| `ownership_node_imports_node` (direct) | violation |
| `ownership_transitive_node_import` (through shared code) | violation |
| `ownership_node_imports_workflow` | violation |
| `ownership_import_cycle` (`Source` = first member, `Actual` = members) | violation |
| `ownership_import_unresolved` | unverified |
| `ownership_import_outside_root` | unverified |
| `ownership_import_case_mismatch` | unverified |
| `ownership_dynamic_import` | unverified |
| `ownership_unsupported_form` | unverified |
| `ownership_source_unsupported` | unverified |
| `ownership_parse_failed` | unverified |
| `ownership_config_invalid` | unverified |
| `ownership_runtime_unsupported` | unverified |
| `ownership_limit_exceeded` | unverified |
| `layout_symlink_escape`, `layout_symlink_alias`, `layout_symlink_dangling`, `layout_symlink_loop` | unverified |

### Bounds

| Bound | Value |
| --- | --- |
| units analyzed per check | 10000 (`layout.MaxFiles`) |
| file size | 1 MiB (`layout.MaxFileBytes`) |
| source read per check | 32 MiB (`layout.MaxTotalBytes`) |
| entries listed from one directory | 4096 (`layout.MaxDirEntries`) |
| imports per file | 4096 |
| tokens per file | 1,048,576 |
| specifier resolution nesting (`imports` → package → …) | 16 |
| tsconfig `extends` depth | 8 |
| diagnostics | 256, then `ownership_limit_exceeded` |

An overrun is `ownership_limit_exceeded` and leaves the node unverified.

### Report for `blok check`

`ownership.Check(project *layout.Project) (*Report, error)` and
`CheckDir(root)` return a `Report{Nodes, Diagnostics}`: one `NodeResult`
per discovered node (`name`, `version`, `runtime`, `dir`, `status`,
`units` read) sorted by directory, and the diagnostics above. The error is
only for a root that cannot be opened (or, for `CheckDir`, discovery's
`*layout.Error`). The JSON encoding is deterministic.

### Dependencies

None. The Go adapter uses the standard library's `go/parser`; the Node.js
adapter is pure Go. `go.mod` and `go.sum` are unchanged. The pinned
TypeScript 5.9.3 of `runtime/nodejs` was used only as a differential oracle
during review (offline from the local npm cache), not at run time.

### Layout API additions

Two exported functions in `internal/tooling/layout`, factored out of
discovery without changing its behaviour: `OpenRegular(root, rel)` (the
file-open rules, with `ErrNotRegular` and `*HardLinkError`) and
`ClassifyLink(root, absRoot, rel) (code, target)` (link classification,
now also returning an alias's symlink-free target).

## Compatibility classification

| Change | Class |
| --- | --- |
| `internal/tooling/ownership` package, report document and `ownership_*` codes | additive (internal package; the codes are the tooling contract) |
| `layout.OpenRegular`, `layout.ErrNotRegular`, `layout.HardLinkError`, `layout.ClassifyLink` | additive; discovery output and diagnostics unchanged (existing layout suite green) |
| projects that pass discovery | unchanged; the ownership check is a separate step a tool opts into |
| starter output | unchanged |

No wire, journal, artifact, worker or manifest contract changes.

## Verification record

- `internal/tooling/ownership` tests: 21 synthetic `txtar` fixtures under
  `testdata/imports` (Apache-2.0, synthetic), each with predeclared node
  statuses and exact sorted `[code, source]` diagnostics, unit counts for
  the positive cases, and output/error/effect counts in
  `testdata/imports/provenance.json`. Go: positive, direct (discovery),
  transitive, generated, `replace` alias, workflow, cycle, unverified forms,
  link, case. Node.js: positive (classic and unified paths, relative,
  tsconfig alias, `#imports`, workspace package, JSON, self-node dynamic
  import, reference, composition root), relative (classic layout), alias
  (`paths` via `extends`, `baseUrl`, re-export), package (`exports`
  conditions, `file:` dependency with pattern subpath, `#imports` to a
  package), generated (direct and via a shared generated registry),
  transitive (ESM → re-export → CommonJS), dynamic/evaluating forms,
  unresolved and cycle, links (shared and `node_modules`), case, workflow,
  JSX and an unsupported runtime.
- The real `blok new` starter in both layouts, and a Node.js example built
  from it with the repository's actual Node.js SDK source and worker
  fixture node file (its `../../../sdk/nodejs/index.js` import unchanged in
  the unified layout; the workspace package `@blok/nodejs-sdk` for a second
  node), verify with zero diagnostics and non-empty graphs.
- `TestImportForms` pins 45 lexical cases; `TestRepositoryNodeSourcesLex`
  lexes every Node.js source in the repository. Differentially, the pinned
  TypeScript 5.9.3 parser and this lexer found the identical 111 imports in
  the repository's 31 Node.js sources and the identical 51 in the fixtures.
- `TestCheckNeverExecutesSource` is structural: the package imports nothing
  that builds, loads or runs code and reads only through the `os.Root`;
  fixtures carry a Go `init` panic and a top-level JavaScript `throw`.
- Mutations are listed in the pull request; each turns a test red.

## Limits

- Windows is not certified. `go vet` passes for `windows/amd64` and
  `windows/arm64`; no test ran on Windows, the link fixtures skip there,
  and off Unix files are not opened non-blocking or link-counted (ADR
  0023).
- The lexer follows the lexical grammar; it is not a full parser. Its
  regular-expression decision mirrors the grammar for every form in the
  fixtures and the repository, and any lexical failure fails closed, but
  contrived code where context-free brace classification differs from the
  grammar (for example a block statement after a `case x:` label followed
  by a regular expression) can make a file unverified.
- The unverified list covers the named loader and evaluating forms. Code
  that reaches Node's loader indirectly through an object graph not in that
  list (for example the CommonJS wrapper's `arguments`) is not detected; it
  is a documented gap, as is spawning another process.
- Union resolution is conservative: a candidate only one tool would pick
  still counts, and type-only imports count.
- Resolution ignores tsconfig `include`/`files` (the nearest config
  applies), `rootDirs`, project references, `typesVersions`, the
  `browser` field, `exports` legacy folder mappings (`"./x/"`) and package
  `extends` that are not installed. Go workspaces (`go.work`) and vendored
  copies are not resolved; their paths are external.
- Ownership is checked only from nodes: shared code and workflows may
  import nodes, and a node reached only from a workflow is not affected.
- Imports in a node's test files are not checked, as in ADR 0023.
- The check is a snapshot of the tree; `os.Root` still prevents reads
  outside the root if the tree changes during the check.
