# ADR 0025: Node independence across language import graphs

- Status: implementation in review for E12-T02 (#68); revised after two
  specialist reviews of PR #314, which found 15 and then 6 routes by which
  a node loaded another node while reported verified; the second revision
  replaces the JavaScript denylist with an allowlist
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

**A violation is an error; unverified is a warning.** `Report.Err()`
returns an `*ownership.Error` carrying the violation-class diagnostics
only, and nil when there is none; `Report.Violations()` and
`Report.Warnings()` split the diagnostics by class, and
`Report.Unverified()` lists the unverified nodes. An unverified node was
not shown to break independence — with the JavaScript allowlist most real
code is unverified (below) — so a tool such as `blok check` fails on
violations and reports unverified nodes as warnings by default.
`Report.Verified()` is the strict "every node verified, nothing found".
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
build-constrained files count like any other (ADR 0023). A `//go:embed`
pattern is matched as Go matches it — each element a `path.Match` glob
relative to the package directory, a matched directory embedding its tree,
names starting with `.` or `_` below it left out unless the pattern has
the `all:` prefix — against every discovered file of every other node;
each matched file is ownership-checked like an import, so `nodes/*/b/*.go`
or `all:nodes` in a root-package file is a node import. A pattern that
reaches into another node's directory but matches none of its discovered
files (which leave out hidden and `_` names) is unverified. Embedded data
is not traversed as code. `_test.go` files are not part of a
node, as in ADR 0023.

Everything else go build compiles or consults that the adapter does not
resolve makes the reaching node unverified, never verified:

| Unverified | Why |
| --- | --- |
| a `go.work` at the project root | a workspace can redirect any module path to any directory (`ownership_unsupported_form`, `Source` `go.work`); it applies to every Go node |
| a `vendor` directory at the project root | vendored module source is not resolved (`Source` `vendor`); every Go node |
| `import "C"` | cgo compiles a C preamble whose `#include` can name any file |
| `.c .cc .cpp .cxx .m .s .S .sx .f .F .for .f90 .syso .swig .swigcxx` beside a package | compiled with the package; their includes and symbols are not checked |
| a `.go` file that is a symbolic link | go build compiles it; the link is never followed (`layout_symlink_*`) |
| a `.go` entry that is not a regular file | `ownership_source_unsupported` |
| `import "plugin"`, `//go:linkname` | each reaches code without a resolvable import |
| a package directory listing more than 4096 entries | `ownership_limit_exceeded` |

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
tokenizes the whole file following the ECMAScript lexical grammar: a byte
order mark and a hashbang line, line and block comments, string literals
with every escape decoded (`\x2e`, `\u{2e}`, legacy octal `\56`, line
continuations), template literals with nested substitutions, identifier
escapes (`\u0072equire` is `require`) and numeric literals. An
unterminated literal, comment or expression, or an unbalanced bracket,
fails the whole file (`ownership_parse_failed`).

A lexer cannot always decide whether `/` starts a regular expression or
divides; that needs a parser. A wrong guess can hide an import inside a
mis-read literal: `function () {} / 1; require("../b"); 1 / 1` read as a
regular expression from the first `/` to the last swallows the `require`.
So the lexer decides only where the previous token settles it — after an
operand, a `]`, `++`/`--`, a property name (`o.if(4) / 2`, `x.return /
2`), a non-control `)` or a TypeScript non-null `!` it divides; after an
operator, `(`, `,` or a reserved word such as `return` it starts a regular
expression — and every other position is `ownership_ambiguous_syntax`,
which makes the file unverified:

- after `}` (a block, class body, function or object literal: `class {} /
  1`, `L: {} /re/`);
- after the `)` of an `if`, `while`, `for` or `with` head;
- after `>` (`a > /re/`, or TypeScript's instantiation `f<T> / 2`);
- after a contextual keyword that may be an identifier (`of`, `yield`,
  `await`, `let`, `async`: `var of = 4; of / 2`).

The lexer still makes a guess there so the rest of the file is read and a
violation elsewhere is still reported, but the file is never verified. In
1,366 TypeScript and JavaScript files of the Blok TypeScript repository
(`~/Projects/Deskree/blok`, excluding `node_modules`, `dist` and
declaration files) no position was ambiguous.

**HTML-like comments.** Annex B lets a CommonJS script use `<!--`
anywhere, and `-->` at the start of a line, as a line comment; a module
refuses both. Read as a less-than and a template, `<!-- `` ` `` would
swallow the next line's `require`. The lexer reads both as line comments,
as CommonJS does, and every occurrence makes the file unverified, because
the two goals disagree about the file.

**The allowlist.** JavaScript reaches its module loader, evaluation and
the `Function` constructor through the object graph in more ways than any
list of forbidden forms can name: the first review found 15 such routes
and the second six more (`const m = module; m["require"](…)`,
`(() => 0)["constructor"](…)`, `const { constructor: F } = () => 0`,
`process["main" + "Module"]`, a sloppy function's `this` handed to
`Reflect.get`, and an HTML-like comment). The recognizer is therefore an
allowlist. **"Verified" means exactly this: every file in the node's
reachable graph lexed with no ambiguous `/` and no HTML-like comment, and
the only places it mentions a sensitive word, or computes a property key,
are the enumerated safe forms below; every import those forms make
resolved; and nothing crosses a node or workflow.** It does not mean the
code was understood; it means it stayed inside forms whose loading
behavior is known.

The sensitive words are `require`, `module`, `process`, `this`,
`globalThis`, `global`, `self`, `window`, `eval`, `Function`, `Reflect`,
`Proxy`, `import`, `arguments`, `constructor`, `__proto__`, `prototype`,
the reflection APIs `getOwnPropertyDescriptor`, `getOwnPropertyDescriptors`,
`getPrototypeOf`, `setPrototypeOf`, `getOwnPropertyNames`,
`defineProperty` and `defineProperties`, and the loader names `createRequire`, `getBuiltinModule`, `mainModule`,
`_load`, `dlopen`, `importScripts`, `ShadowRealm`, `_linkedBinding`,
`Worker` and `binding` (the last only as a property, `process.binding`, or
called bare). The safe forms, the complete list, are:

| Safe form | Effect |
| --- | --- |
| `import … from "s"`, `import "s"`, `import type …`, `import defer/source …` (with attributes) | edge; the declaration's own binding names are part of it |
| `export * from "s"`, `export * as n from "s"`, `export { … } from "s"`, `export type … from "s"` | edge |
| `import x = require("s")`, `export import x = require("s")` | edge |
| `require("s")` (one string argument), `require.resolve("s")` | edge |
| `import("s")` (also `typeof import("s")`), `import.meta.resolve("s")` | edge |
| `import.meta.url`, `import.meta.dirname`, `import.meta.filename` | data |
| `module.exports` | the module's exports |
| `process.env argv cwd platform arch version versions pid exit exitCode nextTick hrtime stdout stderr stdin uptime memoryUsage emitWarning on once` (one of these, after a dot) | process data and control that do not reach the loader |
| `this.<name>`, `this?.<name>`, `this.#name` | a property read; a sensitive `<name>` is still flagged as a property. `this[…]` and `this?.[…]` are not this form |
| `constructor(…) {` / `constructor(…);` | a method definition, not an access |
| `/// <reference path="…">` / `<reference types="…">` | edge |

Everything else is `ownership_unsupported_form` (or
`ownership_dynamic_import` for a call of `import`, `require`,
`require.resolve` or `import.meta.resolve` whose argument is not one
string literal):

- any other occurrence of a sensitive word: as an identifier (`const m =
  module`, `typeof require`, `return this`, the TypeScript type
  `Function`), as a property (`x.require`, `o.constructor`,
  `x.prototype`), as an object or destructuring key (`{ constructor: F }`),
  and as a string or template-without-substitutions literal whose value is
  the word (`"process"`, `` `mainModule` ``, `o["constructor"]`) — import
  specifiers excepted;
- reflection with a name built at run time:
  `Object.getOwnPropertyDescriptor(Object.getPrototypeOf(f), "constr" +
  "uctor").value` is the `Function` constructor without any sensitive
  string or computed key appearing. It is covered by making the reflection
  APIs themselves sensitive words (above), not by following the string;
- every computed property key — `[…]` directly inside braces in a
  property-name position, in a destructuring pattern (`const { [k]: F } =
  f`), an object literal or a class body alike, since they are not told
  apart — whose key is not a number or a non-sensitive string literal
  (TypeScript index signatures `[k: T]` and mapped types `[K in T]` are
  types, not keys). A `[k in …]` key is let through only when it holds no
  `?`, `||`, `&&`, `??` or comma at its own depth: at run time `[k in o]`
  is a boolean key, but `{ [k in o ? a : a]: F } = f` selects `a`, so it is
  a computed key like any other. A mapped type whose `as` clause holds a
  conditional type is not told apart from it and is unverified;
- every computed member access (`x[…]` after an operand, including `?.[`)
  whose key is not empty (a TypeScript array type), a number, a string
  literal of a non-sensitive word, or an arithmetic expression built only
  from names, numbers, property reads, grouping and `- * / % ** ++ --`
  (never `+`, a call, a string, a comparison, a conditional or a comma),
  whose value is a number and so cannot name a property such as
  `constructor`: `text[at - 1]` is allowed, `text[at]` is not;
- AMD `define(…)`;
- the built-ins `vm`, `module`, `worker_threads`, `child_process`,
  `cluster` (with or without `node:`), and `data:`, `http:`, `https:`
  specifiers;
- an unrecognized `import`/`export` clause (`import A = B.C` included);
- a `/` whose goal needs a parser (`ownership_ambiguous_syntax`) and an
  HTML-like comment;
- `.jsx`, `.tsx`, `.node`, `.wasm`, and a reached file with no script
  extension that does not lex as JavaScript (`ownership_source_unsupported`).

Declaration files (`.d.ts`, `.d.mts`, `.d.cts`) hold no run-time code:
their imports are still edges and their lexical checks (ambiguous `/`,
HTML-like comments) still apply, but the allowlist does not.

Type-only imports are edges: a node depending on another node's types is
still coupled to it. This is stricter than the SDK's executable-graph
checker, which excludes type-only edges because they do not run.

Node's CommonJS loader runs a required file with any extension other than
`.js`, `.json` and `.node` — or none — as JavaScript, so any reached file
that is not JSON is analyzed as JavaScript; one that does not lex is
unverified, never treated as an inert asset.

**What it costs.** Failing closed makes most real JavaScript unverified.
On the 1,381 script files of the Blok TypeScript repository
(`~/Projects/Deskree/blok`, `.ts .mts .cts .js .mjs .cjs` including
declaration files, excluding `node_modules`, `dist`, `.git` and `.blok`),
540 (39.1%) are unverified (the 13 declaration files are exempt; none
failed to lex; 517, 37.4%, before the reflection APIs and computed
property keys were added). The files containing each form: a computed
member access 311, the string `"module"` 115, a computed property key
58, `this` outside `this.<name>` 46, `.prototype` 39, `process` beyond its
data members 33, `import(expression)` 28, the string `"process"` 28,
`Function` (mostly the TypeScript type; no file would be rescued by a
type-position exemption) 26, `arguments` 21, `module` beyond `exports` 21,
`window` 20, `globalThis` 19. The repository's own Node.js SDK (`sdk/nodejs`) is
unverified for the same reasons (`text[at]`, `object[key]`,
`Object.prototype`), so a Node.js node that imports it is unverified, never
a violation; a node written in the safe forms verifies. A reviewer who
wants a node verified can rewrite the offending lines in safe forms; the
check will not guess on their behalf. Go nodes are unaffected.

**Resolution.** Every candidate any of Node.js or TypeScript could select
is an edge (the union), so a declaration file and a runtime file cannot
disagree unnoticed:

1. Relative specifiers: the exact file (whatever its extension);
   TypeScript's source for a `.js`, `.mjs`, `.cjs` or `.jsx` specifier
   (`.ts`, `.tsx`, `.d.ts`; `.mts`; `.cts`); every probe extension (`.ts
   .tsx .d.ts .mts .d.mts .cts .d.cts .js .jsx .mjs .cjs .json .node`);
   and, for a directory, `package.json` `main`/`module`/`types`/`typings`
   and `index.*`.
2. `#` specifiers: the nearest `package.json` `imports`, with Node's exact
   and `*` pattern matching (longest prefix); a target may be a `./` path
   or a package.
3. Bare specifiers, in order:
   1. built-ins (external, except the loader modules above);
   2. the nearest `tsconfig.json` or `jsconfig.json` `paths` (exact key,
      else the longest `*` prefix; substitutions relative to `baseUrl` when
      set, else to the config declaring `paths`; JSON with comments and
      trailing commas; relative `extends` chains, at most 8 deep, cycles
      refused, each config loaded once; an uninstalled package `extends` is
      skipped) and `baseUrl`;
   3. a local package whose `package.json` `name` matches — any
      `package.json` inside a node directory, a root `workspaces`
      directory (`dir`, `dir/*`, `dir/**` one level) or the importing
      file's own package (self-reference) — resolved through its `exports`
      (conditions are a union; patterns and subpaths per Node; an
      unexported subpath is unresolved) or main fields and files;
   4. a `file:`, `link:` or `portal:` dependency declared in a
      `package.json` at or above the file: that directory, as a local
      package;
   5. the installed `node_modules/<name>` nearest the file, checked
      **before** any other declaration is trusted, because it decides what
      Node loads. A link into a node or workflow is reported and is a
      violation, even for a declared registry dependency; a link into a
      package manager's store (a path with a `node_modules` element inside
      the project, or outside the project) counts as installed; any other
      link — to shared project code — is reported and never followed
      (unverified);
   6. then: a declared `workspace:` dependency with no matching workspace
      is unresolved; any other declared dependency (registry, git,
      tarball) is external third-party code, trusted like another Go
      module; a package that is installed but declared nowhere is
      unverified (`ownership_import_unresolved`: undeclared installed code
      is neither trusted nor analyzed); anything else is unresolved.

Absolute paths and `file:` URLs are `ownership_import_outside_root`.
Nothing is installed, built or run.

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
ownership, so a link into another node is a violation. Installed
`node_modules` links follow step 3.5 above. Files are opened with `layout.OpenRegular` (non-blocking on
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
| `ownership_ambiguous_syntax` | unverified |
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
| resolution work per check | 4,194,304 path elements looked up (`maxWork`) |
| diagnostics | 256, then `ownership_limit_exceeded` |

An overrun is `ownership_limit_exceeded` and leaves the node unverified.
Time is bounded by counted work, not by a clock: every path element looked
up (each a map access over cached directory listings) is counted, and past
the budget every lookup is a bound overrun. Resolution is cached per
directory and specifier, `exports`/`imports` matches per package and key
(repeated targets resolved once), and each tsconfig is loaded once. A
package whose 0.9 MiB `exports` array repeats one target 100,000 times,
required 4,000 times and from 50 directories, previously took 2 m 55 s and
allocated 119 GB; it now takes about 0.3 s. A diamond of tsconfig
`extends` (9 levels of 8 configs each extending all 8 of the next) took
8.3 s and 2.6 GB and now takes milliseconds and fails closed on the depth
bound.

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

- `internal/tooling/ownership` tests: 31 synthetic `txtar` fixtures under
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
  package, a shared package whose non-first `require` condition reaches a
  node), generated (direct and via a shared generated registry),
  transitive (ESM → re-export → CommonJS), dynamic/evaluating forms,
  unresolved and cycle, links (shared and `node_modules`), case, workflow,
  JSX and an unsupported runtime.
- Every probe of the three reviews is a node in one of ten `probe-*`
  fixtures (`probe-lexer`, `probe-loaders`, `probe-files-packages`,
  `probe-go-sources`, `probe-go-replace`, `probe-go-work`,
  `probe-go-vendor`, `probe-allowlist`, `probe-go-embed`,
  `probe-reflection`) with its predeclared verdict; none of the 24 routes
  that were verified is verified any more, and the controls that held
  still hold.
- `TestUnverifiedIsAWarning` pins the report contract: an unverified-only
  report has no error, and a mixed report's error carries the violations
  only.
- The real `blok new` starter in both layouts verifies with zero
  diagnostics and a non-empty graph. A Node.js example built from it with
  the repository's actual Node.js SDK source and worker fixture node file
  (its `../../../sdk/nodejs/index.js` import unchanged in the unified
  layout; the workspace package `@blok/nodejs-sdk` for a second node) has
  no violation: the two nodes that reach the SDK are unverified, with
  every finding in the SDK's files, and a third node written in the safe
  forms verifies. `TestNodeStarterWithDeclaredSDK` is the real shape of
  a Node.js node: the starter plus a node importing `@blok/nodejs-sdk` as
  a declared npm dependency, written in the safe forms; it verifies in
  both layouts.
- `TestImportForms` pins the lexical cases, including the ambiguous `/`
  positions, a hashbang holding a quote and a byte order mark;
  `TestRepositoryNodeSourcesLex` lexes every Node.js source in the
  repository. Differentially, the pinned TypeScript 5.9.3 parser and this
  lexer found the identical 111 imports in the repository's 31 Node.js
  sources and the identical 51 in the fixtures (first revision).
- `TestResolutionTimeIsBounded` and `TestWorkBudgetFailsClosed` bound the
  resolution work; `TestBoundsLeaveNodesUnverified` the entry and size
  bounds.
- `TestCheckNeverExecutesSource` is structural: the package imports nothing
  that builds, loads or runs code and reads only through the `os.Root`;
  fixtures carry a Go `init` panic and a top-level JavaScript `throw`.
- Mutations, each applied to a committed tree and reverted, turn a test
  red; they are listed in the pull request.

## Limits

- Windows is not certified. `go vet` passes for `windows/amd64` and
  `windows/arm64`; no test ran on Windows, the link fixtures skip there,
  and off Unix files are not opened non-blocking or link-counted (ADR
  0023).
- **What "verified" can and cannot promise for Node.js.** Verified is an
  allowlist result: every sensitive word and computed key in the reachable
  graph sits in one of the enumerated safe forms. Within those forms the
  loading behavior is known; nothing else is assumed. The guarantee rests
  on the safe forms being safe — for example that `this.<name>` and the
  listed `process` members cannot reach a loader without a sensitive name
  or a computed key appearing — and on the lexer tokenizing as the
  engine does; both are argued above, not proven. Native nodes are
  trusted application code (AGENTS.md); this check catches shortcuts, it
  is not a sandbox.
- The lexer is not a parser. It never decides an ambiguous `/` silently
  (above). The allowlist does not depend on brace kinds: a sensitive word
  is flagged wherever it is not one of the safe forms.
- Spawning another process is not an import; `child_process`,
  `worker_threads` and `cluster` are unverified instead.
- Installed third-party packages that a `package.json` declares (registry,
  git or tarball) are trusted and not analyzed, like other Go modules; an
  undeclared installed package is unverified.
- Union resolution is conservative: a candidate only one tool would pick
  still counts, and type-only imports count.
- Resolution ignores tsconfig `include`/`files` (the nearest config
  applies), `rootDirs`, project references, `typesVersions`, the `browser`
  field, `exports` legacy folder mappings (`"./x/"`) and package `extends`
  that are not installed. A `go.work` or `vendor` directory at the project
  root is not resolved: it makes every Go node unverified. A `go.work` in
  a directory above the project root, `GOWORK`, `GOFLAGS=-modfile` and
  `-mod=vendor` from the environment are outside the project and not
  observed (ADR 0023 never reads outside the root).
- Ownership is checked only from nodes: shared code and workflows may
  import nodes, and a node reached only from a workflow is not affected.
- Imports in a node's test files are not checked, as in ADR 0023.
- The check is a snapshot of the tree; `os.Root` still prevents reads
  outside the root if the tree changes during the check.
