# E08-T04 (#53): authenticated worker and equivalent order evidence

The workloads use the real native engine and the same compiled structural order:
price → actual HTTP payment provider → receipt → response. Input/output schemas,
normalization, immutable copies, provider request/body bounds, 2-second provider
deadline, operation key and idempotency semantics are the same. The provider
counts requests and committed synthetic charges; a lost response is uncertain.
Memory mode is intentional: these are not durable throughput or fleet benchmarks.

`TestActualNodeEquivalentOrderFailureAndIdempotency` executes success, duplicate
key, unknown SKU, business rejection, explicit transient rejection, uncertain
success-with-response-loss and cancellation against the real selected Node
process. `TestActualNodeKillBeforeAndAfterProviderEffect` waits for dispatch or
committed charge, kills that process, checks no receipt/no hidden retry, and runs
a native order to prove the engine survives. Run both with:

```sh
BLOK_NODE_INTEGRATION_ROOT=<built-repo> go test ./benchmarks/worker -run TestActualNode -v
```

Controlled load: 20 warmup orders, five batches of 50 orders per implementation,
concurrency exactly one, distinct business keys. Both implementations perform
270 HTTP requests and 270 effects, including warmup. Every measured output is
checked, not merely timed. Latency excludes startup; raw per-order nanoseconds
and cumulative provider counts are recorded. Worker startup measures selected
process/handshake readiness; RSS is a single post-start worker snapshot from
`ps`, not peak memory, allocation profiling or native-memory comparison.

```sh
BLOK_NODE_INTEGRATION_ROOT=<built-repo> BLOK_WORKLOAD_REPORT=<report.json> \
  go test ./benchmarks/worker -run TestControlledEquivalentOrderSamples -v
node benchmarks/worker/summary.mjs <report.json>
```

Committed evidence uses Go 1.27.1 arm64, Node 22.18.0 in a Linux Docker container
limited to two CPUs, and Node 24.21.0 on the Darwin host. OS/topology differ
between matrices; compare native versus Node **within each report**, never
between OS reports. These local serial samples are not isolated hardware,
production capacity, scalable load, live provider or superiority claims.

Transport proof uses actual token/principal rejection, generation/catalog
mismatch, truncated/oversized serialized messages, and a paused socket consumer
with 800000-byte synthetic outputs. Saturation closes the stream; pending effects
remain uncertain. Authenticator sessions copy/narrow scopes, bind credential and
principal and recheck revocation; reconnect cannot replace credentials or restore
dropped capabilities. The Node local listener uses a generation-bound token;
rotation requires explicit drain/restart with a new generation, not live widening.
Authenticated transport and native trust are not an OS sandbox.

`worker.BlobHandler` is an explicitly selected local HTTP read adapter to the
bounded credential-owned blob store; applications own listener/timeouts/connection
limits. `createBlobReader` is an injected Node dependency with fixed loopback
endpoint, token and principal, bounded streaming reads, size/digest verification
and AbortSignal. Blob contents bypass the 1 MiB call frame. The actual Node
socket test transfers 1350000 synthetic bytes and rejects spoofing, wrong token,
cross-owner access, wrong size, oversize and revoked credentials. References are
not inline payloads, and no user input can choose the endpoint or credential.
The store enforces aggregate byte and 1024-entry bounds before allocation;
`ResolveAuthorized` accounts repeated references against the caller's byte bound.

Native checks: `npm ci --ignore-scripts`, `npm test` (17), lint, build, generated
drift and `npm audit` (zero findings), on both supported Node majors. Go checks:
focused race, full vet/race/build and diff check. Go-only runs explicitly skip
foreign-runtime gates; the opt-in gates are required as separate evidence.
