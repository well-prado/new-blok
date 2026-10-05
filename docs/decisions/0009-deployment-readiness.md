# ADR 0009: Deployment readiness composition

- Issue: E17-T01 (#81)
- Status: retained SQLite journal integration implemented; independent review and native Windows evidence pending
- Compatibility: additive public `app.RetainedArtifactProbe`; optional endpoints move to `app/deploy` before #81 release

`app.ArtifactProbe(expected, inventory)` adds an explicit readiness adapter
for the existing artifact manifest/checkpoint contract. The caller supplies
the real inventory. Missing inventory, invalid manifests, identity mismatches,
and incompatible checkpoint formats fail closed. Existing deployments are
unchanged; adopting applications pass the returned function as their artifact
check. The contract test supplies synthetic inventory; it does not prove
embedded-store checkpoint enumeration or restored-run compatibility.

The Go/Node example owns its executable, workflow, selected child worker and
dependency shutdown. It negotiates the compiled catalog and a digest of selected
worker files over authenticated loopback. One shared JSON catalog supplies both
Go's embedded descriptor and the application's Node module. Configuration
cannot select an arbitrary node module. The public HTTP listener permits an
external bind only with explicit opt-in. The private worker listener is not
published outside the container.
HTTP deployment endpoints live in `app/deploy`, explicitly selected by the
application. Moving `Deployment`, `DeploymentChecks` and `NewDeployment` there
keeps core `app` free of network imports, preserving the no-trigger dependency
gate. Callers of the unreleased #81 API import `app/deploy` for these three
names; configuration remains in `contract/deployment`. Artifact probes remain
in core `app` and depend only on the provider-neutral artifact contract.
The worker address is configurable only as `127.0.0.1` and a nonzero port,
matching the selected Node adapter; remote/IPv6 transport is not offered here.

`Supervisor.Ready()` describes lifecycle and cached negotiation. It does not
establish current transport reachability after process/stream loss. Deployment
readiness therefore executes a pure, zero-delay quote RPC with a 250ms context
and checks the result. This belongs to application composition: the application
selects a safe probe and its expected result. No transport/supervisor/process
implementation is changed by #81; #51 and #53 retain that ownership.

Admission has no waiting queue. HTTP work uses at most 32 slots (two by default),
while the worker negotiates 64 concurrent calls, leaving headroom for probes.
Excess work returns 503 and a rejection counter. Operational probes bypass
HTTP admission. Worker replay identities remain bounded by the current worker
implementation (Node defaults to 8192 replay entries, two per RPC); probes
consume those identities too. Deployments must recycle
before that budget is exhausted. This example is not an unlimited uptime or
capacity claim. Probe requests are expected to be rate limited by the operator.

The worker-file inventory is a local change detector, not signed image
provenance, full dependency-byte integrity, or a durable execution manifest.
Unmodified dependencies come from the image's npm lock installation. Trusted
application files and native/Node code remain trusted code, not a sandbox.

The durable order example connects `app.RetainedArtifactProbe` to the journal's
streamed `RetainedArtifacts` inventory in one SQLite transaction. Left joins
preserve missing artifact records. Admissions before their first checkpoint,
terminal retained runs, and checkpoints left without a run are included; audit
records alone are not resumable executable state. The projection exposes only
identities and manifest metadata, never run inputs, outputs or checkpoint state.
Visitor errors and canceled database reads fail closed. Memory is bounded by
the current row; the existing one-second readiness context bounds the scan.

The application derives its manifest from the actual executable bytes, embedded
composition source, input/checkpoint schemas, Go compiler version and embedded
build/dependency information. The input schema also validates admission. This
is local compatibility identity, not signed provenance or source lockfile
attestation. The journal checkpoint digest identifies the supported versioned
codec/schema, not mutable state bytes. A retained manifest must hash to the run's
identity and the selected executable's identity; checkpoint artifact and codec
must agree. A manifest record does not prove an old executable is available.
This single-version application refuses such upgrades, retaining the volume
for restart with the original executable. No multi-version manager is supplied.

Startup verifies retained state before registering the current manifest, so it
cannot silently repair missing retained artifacts. After registration, readiness
also requires the current artifact record. Store/worker probes wait for the
application's initialized state, preventing reads of partially opened resources.
Failure before listener binding rejects startup; faults after binding keep
health available and make readiness and business admission return 503. A
shutdown request that arrives while startup is still running is a shutdown,
not a readiness failure: the startup probe judges the dependencies on a
context the request cannot cancel, and the drain then handles it (#156).
On Windows, Ctrl+C and Ctrl+Break on the console are the shutdown request.

Orders commit journal admission, the existing queue handoff and a checkpoint
before returning 202. These are separate commits: a pre-acknowledgment interruption
can leave an admission without a checkpoint or a queue item without its final
checkpoint. Retrying the same request key deduplicates both journal and queue;
acknowledged work has its queue item and checkpoint. The queue still owns
processing and transactional business/outbox state. Journal handoff records
remain retained after queue completion; this example does not compact them or
claim engine checkpoint execution, automatic recovery of unacknowledged work,
or universal exactly-once effects. Older queue-only volumes remain readable;
their existing jobs cannot retroactively prove journal artifact identity.

Actual SQLite admission/checkpoint, cold restart, verified SQLite restore and
failure tests complement container volume faults. Independent R review remains
required. Native Windows deployment/permissions/lifecycle evidence remains
pending #156; Linux containers and SIGTERM tests do not establish it.
