# Persistent Node.js worker (E08-T03 / #52)

Pre-alpha additive SDK, not a published npm package. Applications explicitly
select this runtime; Go-only applications do not install or launch it.
Node 22 and 24 are the supported major-version matrix. Dependencies and
TypeScript are pinned in package-lock.json; install with `npm ci --ignore-scripts`.
Native addons, when selected by an application, remain that application's
platform/ABI packaging responsibility; this worker has no addon dependency.

From this directory run `npm test`, `npm run lint`, `npm run check:generated`.
`npm run generate` deterministically regenerates bindings from the one canonical
`contract/runtime/runtime.proto`; startup refuses a mismatched proto digest.
Build with `npm run build`. The CLI takes one compiled ESM module exporting
`nodes`; append `--discover` for canonical descriptors and their digest:

```sh
node dist/runtime/nodejs/main.js dist/testdata/worker/nodejs/nodes.js --discover
```

Declare each node's directory in `ownership.json` and its entry module in
`entries`. `npm run ownership -- path/to/application-ownership.json` checks
resolved imports/re-exports (including transitive utility and literal dynamic
imports); unresolved and computed dependencies fail closed. Runtime node-to-node
invocation also fails. This is an authoring check, not a sandbox for native code.
Discovery imports explicit composition code; it does not invoke nodes, but
application module initialization must itself be effect-free.

Server configuration: `BLOK_WORKER_TOKEN` (32–4096 bytes),
`BLOK_WORKER_ARTIFACT` (sha256 digest), `BLOK_WORKER_GENERATION` (positive uint64
decimal), `BLOK_WORKER_ADDRESS` (fixed `127.0.0.1:port`),
`BLOK_WORKER_PRINCIPAL`, and `BLOK_WORKER_CAPABILITIES` (JSON string array).
`BLOK_WORKER_PROTO` can point to an identical canonical proto. Token and verified
principal are bound to transport metadata, not user input. The local listener
does not implement remote TLS; remote exposure is refused. No credentials are
included in descriptors, errors, or discovery. SIGTERM/SIGINT cancel the session
and active cooperative work; a selected process supervisor reaps the process.

One persistent Connect stream negotiates protocol 1.0, exact catalog/artifact/
generation and intersected bounds. Production does not implement unary Invoke.
Default frame 1 MiB, blob declarations 8 MiB aggregate, concurrent execution 64,
call duration 60 seconds, outbound queue 64 messages/2 MiB, stalled writes 5 seconds.
Inputs are normalized before execution, outputs before publication; exact int64
uses decimal strings on wire and BigInt for unsafe native JSON integers.
Cancellation reaches AbortSignal and fences output. An uncooperative node keeps
its execution slot until its actual completion; cancellation does not undo effects.

Replay identities are retained for the entire generation: default 8192 entries
(two per call). At capacity/replay/write saturation the stream fails with
RESOURCE_EXHAUSTED; pending effect outcomes are uncertain, not silently retried.
An explicit drained restart requires a new generation. Blob declarations are
validated but this SDK does not dereference blob content; the owning application
must inject its authenticated blob provider. Required node capabilities must be
present in the call's narrowed grant. Provider errors are redacted and keep their
class and logical operation key; only explicit transient errors are retryable.

The Go integration gate explicitly launches this built worker and real engine:
`BLOK_NODE_INTEGRATION_ROOT=<repo> go test ./runtime/worker -run TestActualNodeWorkerThroughTypedWorkflow -v`.
The ordinary Go-only gate skips that opt-in test; it is not Node conformance proof.
