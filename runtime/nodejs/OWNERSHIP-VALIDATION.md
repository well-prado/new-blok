# E08-T03 (#52): ownership executable-resolution correction

Review R finding 6 identified that TypeScript could resolve a value import to a
package's harmless declaration file while its executable imported another node.
The checker now uses the pinned TypeScript 5.9.3 implementation-only resolver,
with separate import/require conditions and existing JavaScript precedence.
Explicit type-only edges are excluded; unresolved executable targets and
computed dependencies still fail closed. This corrects authoring validation;
it provides no native-code sandbox or independent specialist approval.

Six filesystem ownership tests cover separated declaration/runtime exports,
transitive packages, import/require branches, JS/TS shadowing, type-only and
mixed imports, missing executable targets, and computed dependencies. Temporary
packages contain synthetic constants only, with no provider effects. The
negative package test fails against the original checker with `Missing expected
exception`, and passes with the correction. The fixture also imports/requires
the actual executable modules to confirm Node selects the reviewed branches.

## Native matrix, 2026-10-02

Node 22.18.0 / npm 10.9.3 on Docker linux/arm64 and Node 24.21.0 /
npm 11.19.0 on darwin/arm64 use the checked-in npm lock and TypeScript 5.9.3.
From `runtime/nodejs`, both ran:

```sh
npm ci --ignore-scripts
node --test test/ownership.test.mjs
npm test
npm run lint
npm run build
npm run check:generated
```

Ownership tests passed 6/6 and the complete native suite passed 17/17 on both
majors. Lint (including no-emit typecheck and ownership), build, and generated
binding drift checks passed. The clean npm installs reported zero vulnerabilities.
The Node 22 container used a temporary snapshot of the worktree, excluding
`.git`, `node_modules`, and `dist`; it installed its own locked dependencies.

## Go boundary

No Go implementation, protocol, SDK, worker execution, or Go test source changes
are included in this correction. Existing Go gates remain applicable; the
ownership regression is exercised by the native tests, not by Go-only tests.
Go 1.27.1 linux/arm64 uses a read-only worktree mount with these commands:

```sh
go test -race ./contract/runtime ./internal/runtime ./runtime/worker
go vet ./...
go test -race ./...
go build ./...
git diff --check
```

Focused race tests, full vet/race/build gates, and the host diff check all passed.
The ordinary Go-only run does not opt into `BLOK_NODE_INTEGRATION_ROOT`; it is
not a fresh Go-to-Node integration or load measurement. No Actions or GitHub
state updates are part of this correction. Independent Review R remains for
another reviewer after this implementation.
