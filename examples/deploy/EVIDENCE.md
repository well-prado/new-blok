# Local #81 evidence — 2026-10-02

Checkout: `/Users/wellingtonprado/Projects/Personal/new-blok-m4-81`, branch
`codex/81-deployment-runtime`. All fixtures are synthetic; no image, commit or
branch is pushed. Committed #53 dependency history is merged through `005f437`,
including `25202e0` cancellation-cause propagation and subsequent absolute
deadline/call-bound fixes. No #81 edits to the worker transport implementation.

Toolchain: Docker Engine 29.4.0 (OrbStack), Go 1.27.1 linux/arm64 in Docker,
Node 22.18.0 linux/arm64 in Docker, host Node 24.21.0 darwin/arm64,
TypeScript 5.9.3 from the worker lock. Host Go is absent.

## Commands and observed behavior

Focused checks use Docker Go with the named volumes `new-blok-m4-gomod` and
`new-blok-m4-gocache`:

```sh
docker build -f examples/deploy/Dockerfile.node --target validation \
  -t new-blok-81-validation .
docker run --rm \
  -v /Users/wellingtonprado/Projects/Personal/new-blok-m4-81:/src -w /src \
  -v new-blok-m4-gomod:/go/pkg/mod \
  -v new-blok-m4-gocache:/root/.cache/go-build \
  -e GOMAXPROCS=3 new-blok-81-validation \
  go test -race -count=1 ./examples/deploy ./app ./runtime/worker \
  ./internal/runtime ./contract/deployment
```

`BLOK_NODE_INTEGRATION_ROOT=/src` is set by the validation image, so the actual
Node worker/deployment tests execute. Focused tests cover missing secrets/files,
invalid worker binds, store corruption and format mismatch, exact typed quote
output 3000, post-start worker-artifact mutation withdrawing readiness, actual
gRPC worker calls, and synthetic artifact/checkpoint inventory failures.

The same Docker volume invocation runs the full gate:

```sh
go vet ./...
go test -race -p 3 ./...
go build ./...
git diff --check
```

Node checks on both declared versions passed all 17 tests with no skips:

```sh
cd runtime/nodejs
npm ci
npm run lint
npm run test
npm run check:generated
```

The application module additionally passes `node --check examples/deploy/nodes.mjs`;
both container scripts pass `bash -n`. Node image builds install locked
dependencies and compile TypeScript in the actual Node 22.18.0 build stage.
The Dockerfile-specific ignore file excludes host dependencies, build output
and Git metadata from that context. Go image builds use a BuildKit compilation
cache; validation runs use the explicitly named Docker volumes above.

```sh
bash examples/deploy/container-test.sh
bash examples/deploy/node-container-test.sh
```

Native/durable suite: loopback host publication, explicit external-bind rejection,
health/readiness/metrics, native quote output 3000, successful SIGTERM exit,
missing secret rejection, unauthorized order rejection, trailing JSON rejection
without an extra queued order, committed admission before stop, processing
after volume restart, retained output after another
restart, and no repeat processing (`processed:false`). The durable fixture is
`fixture-81`, coffee quantity two, one retained order totaling 3000 cents.

Go/Node suite: exact quote output 3000; invalid quantity returns 400; missing
secret/executable and incompatible catalog fail before HTTP admission; configurable
private worker port; two simultaneous 1500ms requests occupy both slots; probes
remain available and the third request returns 503 with one rejection counted;
SIGTERM lets both accepted requests return 3000 and exits zero; killing only the
child worker leaves health 200 but readiness/admission 503 and `blok_ready 0`;
restart serves a fresh quote; a 50ms drain budget with a 2000ms request cancels
the HTTP request and exits one. All quote/probe nodes are pure, with zero external
business effects. This is behavioral integration evidence, not a capacity test.

The first actual integration attempt rejected an unsupported schema `enum`;
the catalog now uses the supported schema subset and the node checks SKU.
The first container attempt exposed a missing generated `proto.sha256` file;
the image now includes it and the artifact digest covers it. Corrected runs
pass. The unit test using host filesystem assets did not detect that packaging
omission; the actual container gate did.

## Current retained-journal correction (2026-10-03)

Commit `eecc5a6` connects readiness to actual SQLite journal artifact and
checkpoint inventory, with missing/incompatible retained-run regression tests.
The optional deployment server is isolated in `app/deploy`; native applications
retain their ordinary `app` dependency. Docker Go 1.27.1 Linux/arm64 focused
race tests (`./app/... ./examples/deploy ./internal/journal`), full vet,
full race tests and build passed. `git diff --check` passed.

Current `bash examples/deploy/container-test.sh` and
`bash examples/deploy/node-container-test.sh` both passed on `0f76baa`.
The native/durable suite verified committed journal admission/checkpoints,
volume restart, no repeated processing, live readiness withdrawal with zero
new admissions, and cold missing-artifact/incompatible-codec/manifest failures.
The Node suite verified real worker quotes, startup failures, overload/probes,
accepted-request SIGTERM drain, worker loss, restart and deadline expiry.
Synthetic evidence is retained at `/tmp/new-blok-81-volume.3YFxTj` and
`/tmp/new-blok-81-worker.CiP4cl`. No CI or Windows completion claim is made.

## Pending acceptance and review

#49/PR #152 is merged and its journal/store implementation is integrated here.
Actual retained inventory now gates startup and readiness; synthetic
`app.ArtifactProbe` tests are no longer the only evidence. This single-version
example refuses incompatible retained executable identities rather than
claiming transparent upgrades or multi-version routing.

#51's explicit shutdown deadline fix `c56f594` is integrated. Later #53/#74
changes remain their owners' work. Passing
local cooperative SIGTERM tests does not establish bounded shutdown for every
broken/noncooperative transport. Independent R review is still required.

Worker readiness uses an actual bounded pure RPC because `Supervisor.Ready()`
is cached lifecycle/negotiation state. It consumes replay identities and fails
closed after the bounded generation budget. The example requires planned
recycling; it does not provide automatic worker reconnect, indefinite uptime,
remote TLS topology, signed provenance, a sandbox or a durable Node workflow.

No #81 acceptance checkboxes, dependency issues, shared main, other worktrees,
GitHub workflow files, branch protection or hosted product state were changed.
