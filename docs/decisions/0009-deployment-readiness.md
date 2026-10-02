# ADR 0009: Deployment readiness composition

- Issue: E17-T01 (#81)
- Status: implemented slice; retained journal checkpoint integration pending #49
- Compatibility: additive public `app.ArtifactProbe`; deployment examples only

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

The durable order example checks SQLite integrity and its own order-format
marker, and demonstrates committed admission and retained business output over
volume restart. It does not enumerate real retained journal checkpoints. That
compatibility criterion remains pending #49/PR #152 and #48's artifact retention
contract. Independent review and broader #51 shutdown fixes remain separate
requirements; an observed local signal test does not close them.
