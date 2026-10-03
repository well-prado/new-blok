# #75 / E14-T02 validation record

Local branch: `codex/75-durable-approval-gates`. Updated: 2026-10-03.
Implementation and evidence are synthetic pre-alpha work. No push or issue
closure is implied. This record distinguishes local behavior from unresolved
dependency/review evidence.

## Environment and commands

Linux/arm64 Docker, `golang:1.27.1`, `--cpus=2`, `GOMAXPROCS=2`, Go package
parallelism `-p 2`; existing caches `new-blok-m4-gomod` and
`new-blok-m4-gocache`. Go reports `go1.27.1 linux/arm64`. There is no host Go
installation. The actual Node test runs as a compiled Go test executable under
Node 22.18.0 Linux/arm64 and Node 24.21.0 Darwin/arm64. Darwin binaries are
cross-compiled with `GOOS=darwin GOARCH=arm64 CGO_ENABLED=0`; Darwin integration
is explicitly **without race**, while Linux integration uses the race build.

Run these inside the Go container mounted at `/work`:

```sh
gofmt -w agent/policy contract/approval
go test -race -p 2 ./agent/policy ./contract/approval -count=1
go vet -p 2 ./...
go test -race -p 2 ./... -count=1
go build -p 2 ./...
go mod verify
go test -p 2 ./contract -run=^$ -fuzz=FuzzParseBounded -fuzztime=5s -parallel=1
go test -race -p 2 ./agent/policy -run '^TestProcessKillApprovalDispatchPublication$' -count=10 -v
```

The first full vet/race/build run passed after merging #53's deadline fix
`25202e0`, latest #50/#51 conformance improvements and #62 provider ports
`5e9d4b1` through #74. Module verification passed. Five-second bounded parser
fuzz smoke passed (three executions, no new interesting input; this is only
smoke, not broad fuzz coverage). Actual SQLite process-kill tests passed all
six windows ten times each (60 SIGKILL/reopen scenarios).

Actual Node policy invocation:

```sh
# Compile in Go container; run inside node:22.18.0 with root mounted at /work.
go test -race -c ./agent/policy -o /evidence/policy-linux.test
BLOK_NODE_INTEGRATION_ROOT=/work /evidence/policy-linux.test \
  -test.run=TestActualNativeNodeCatalogDurablePolicy -test.v -test.count=3

# Cross-compile in Go container; run on Darwin with its actual source root.
GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 go test -c ./agent/policy \
  -o /evidence/policy-darwin.test
BLOK_NODE_INTEGRATION_ROOT=<absolute-source-root> /evidence/policy-darwin.test \
  -test.run=TestActualNativeNodeCatalogDurablePolicy -test.v -test.count=3
```

Both integrations passed three repetitions. Each exercises native read → actual
persistent Node gRPC quote → child/root deterministic verification → SQLite
result commit. Missing review and changed input dispatch no additional native
effect. The ordinary Go suite explicitly skips this test without the runtime
environment; that skip is not the integration evidence.

The final rerun on #74 head `c6e6ae9` (including review correction `dbd8303`)
passed focused race tests, full `go vet`, full `go test -race -count=1`, full
`go build`, and repository-wide formatting. `TestCatalogResourceChangeRequiresFreshReview`
changed only the actual registered native child's reservation while retaining
the same name/version/program/deployment artifact. Its parent tool digest
changed and the existing review was refused with zero reads, effects, durable
attempts or trusted results. The adapter also compares admission resources
against its immutable bound listing. The actual Node 22/Linux race and Node
24/Darwin non-race policy integrations were rebuilt and each passed three
repetitions on this final baseline.

Node 24.21.0 SDK `npm run lint`, `npm run check:generated`, `npm test` passed
again on that baseline with 17 tests and zero skipped/failed. SDK dependency installation and build are
local verification; no SDK edits belong to this issue.

## Crash and publication observations

Each of ten repetitions produced these observed state/effect counts on reopen:

| SIGKILL window | Durable review | External effect | Attempts | Trusted results |
| --- | --- | --- | --- | --- |
| approval-before | false | false | 0 | 0 |
| approval-after | true | false | 0 | 0 |
| dispatch-before | true | false | 1 | 0 |
| dispatch-after | true | true | 1 | 0 |
| publication-before | true | true | 1 | 0 |
| publication-after | true | true | 1 | 1 |

Every reopen checks SQLite integrity and preserved reviewer/time audit. Every
already dispatched/committed operation refuses blind redispatch. A committed
approval without a dispatched attempt permits the reviewed operation; an
uncommitted approval does not. This proves process-restart/retained-volume
barriers, not disk-loss survival or recovery with a substituted executable.

`adversarial.json` and `catalog.json` declare expected errors/effects/attempts/
publication counts before execution. Actual catalog/provider tests verify root
and child policy integration, exact scope, normalized equivalent input, failed
child evidence preventing payment, and failed root evidence publishing nothing
after the external payment. Verifier/executor errors containing synthetic
credentials are absent from returned error strings/unwrap chains and persisted
uncertainty detail; canonical cancellation/deadline causes remain inspectable.

## Latest local audit (2026-10-03)

On the current branch, focused `go test -race -p 2 ./agent/policy
./contract/approval -count=1`, full `go vet -p 2 ./...`, full `go test -race
-p 2 ./... -count=1`, and `go build -p 2 ./...` passed with Docker Go 1.27.1
Linux/arm64. `git diff --check` passed. Separately, all six process-kill windows
passed ten repetitions each (60 cases), and the opt-in actual Node policy
integration passed three repetitions on Node 22.18.0 Linux/arm64 with race and
Node 24.21.0 Darwin/arm64 without race. Node 24 SDK lint, generated drift and
all 22 native tests passed. None of this proves Windows support. Independent
Review R is ongoing; this audit is not an issue-completion claim.

## Completion limits

The subsequent integrated audit merges current `origin/main` (`0a3f5ca`),
the explicit worker shutdown deadline correction (`c56f594`), raw operation-key
UTF-8 validation (`eab409b`), latest #74/#62 changes, and canonical catalog/null
fixes. Full Go vet/race/build passed again. Node 22.18.0/Linux and
24.21.0/Darwin lint/generated checks and all 26 SDK tests passed; the actual
policy integration passed three repetitions on each (Linux race, Darwin
non-race). Independent review cleared the scope-reentry and cleanup-redaction
defects after three race repetitions. The permanent actual catalog-child
regression proves a separately approved write cannot widen a read child's
scope: zero write effects, one root attempt, zero trusted commits.

These are integrated local checks, not merged-main, native-Windows, or
deployment readiness certification. The PR remains pending dependency review
and merge; no GitHub Actions result is claimed.

- #74's reviewed resource-inclusive digest and worker-boundary corrections are
  merged locally and the resource-only approval regression passed. Its own
  dependency/review/merge state remains under #74's owner; local integration
  does not close that issue or endorse unsupported remote token execution.
- #48 merged through PR #151 (`b7a2979b141a80fbd49bbee04edfe31ec56e8ca7`)
  and #49 through PR #152 (`0dcf425ad82bda188184f75a791c0293b08b2d98`).
  Their current integrated compatibility with this branch still requires
  verification; historical merge state is not policy integration evidence.
  Unknown effects remain blocked pending authorized reconciliation.
- #49 store operational lifecycle is outside this implementation scope; approval audit saturates at
  the configured capacity and is never silently evicted.
- Independent security/durability Review R, dependency closure and merged
  integration remain outstanding. Local checks do not mark #75 complete on
  GitHub. No Actions, workflow or protection changes are made.
