# ADR 0004: Persistent worker runtime protocol

- Status: implemented; pre-alpha compatibility boundary
- Date: 2026-10-02
- Roadmap: E08-T01 (#50)

## Decision

Foreign-language nodes use one long-lived, bidirectional stream per selected
worker generation. The native engine is the client and the worker is the
server. Each request invokes one node (bounded requests may run concurrently); it has no
workflow, trigger, store, retry, or child-run API.

The `contract/runtime/runtime.proto` file is the wire source of truth. The Go
package beside it defines validation and negotiation semantics and explicit
wire converters. The engine does not import that transport. Protocol major versions are incompatible. Minor versions
are backward-compatible only when the worker advertises at least the client's
minor version.

Protocol 1.1 adds the optional `Frame.Log` message for structured, per-call
worker logs. It is tagged with the active call, attempt, and generation; it
cannot change execution results. A 1.0 client can connect to a 1.1 worker, which
suppresses log frames for that negotiated session. A 1.1 client rejects a 1.0
worker until the worker is rebuilt. Log message/attribute limits and redaction
are defined by E15-T01 (#76) and the inspection projection decision.

`Call.traceparent` (12) and `Call.tracestate` (13) optionally carry the
dispatching step's W3C trace context (E16-T01, #79, ADR 0020). They are
additive inside 1.1 rather than a minor bump: they are correlation only, a
worker that ignores them is conformant, and proto3 peers ignore unknown
fields, so requiring 1.2 would reject every 1.1 worker for data that cannot
change execution. Go validates them before sending (canonical version-00
traceparent, at most 256 printable bytes of tracestate, no tracestate alone)
and omits them when they alone would exceed the negotiated frame ceiling.
The Node worker exposes a canonical context as `ctx.trace` and drops a
malformed one without failing the call.

## Identity and retry rules

Every connection binds an artifact digest, canonical catalog digest, authenticated
principal, negotiated capability set, and non-zero worker
generation. A mismatch rejects the connection before traffic. Every call has a
unique call ID and attempt ID. Attempts may share a logical idempotency key;
transport reconnect never silently retries a call whose effect outcome is
uncertain. Only errors explicitly classified transient and not uncertain are
safe to retry.

## Bounds and value semantics

The maximum frame is 1 MiB, aggregate blob references per call are 8 MiB,
and concurrent calls are 64. Calls and negotiation permit at most 128
capabilities; calls permit at most 128 blob references. A call deadline is at
most five minutes from validation, shared by Go and Node (not an unadvertised
worker-only duration setting). Input reserves 1024 bytes for bounded call metadata.
Logical idempotency keys are opaque valid UTF-8 strings of at most 128 bytes,
not protocol identifiers: business keys may contain spaces, slashes and Unicode.
Go validates their encoding before protobuf dispatch; both peers enforce the
byte bound and preserve the key in uncertain/error results.
Node scans each raw call key before protobuf decoding, rejecting malformed UTF-8
without rejecting a legitimately encoded U+FFFD replacement character.
The complete encoded call envelope is also checked against the negotiated
frame ceiling before queueing: the 1024-byte input reserve alone does not bound
128 capabilities or blob-reference metadata. An unset principal reserves the
maximum authenticated adapter identity. Catalog hashing applies the native
`node.ValidateDescriptor` declaration rules, not merely nonempty metadata.
Both peers negotiate the minimum of their limits, and values are rejected
before dispatch when they exceed it. Payload schemas use the existing bounded
schema contract: null/presence is explicit and portable integers use the
`int64-string` wire form. A digest or capability is metadata, never a secret.

Capabilities are an intersection: the worker cannot gain permissions during
negotiation or reconnect. Capability names containing orchestration authority
are invalid at this boundary. Authentication (mTLS or an equivalent local
credential) remains an adapter concern and must establish the principal before
this contract is accepted.

## Evidence boundary

The semantic package has executable declared fixtures for identity, generation,
limits, capability, deadline, missing/null and exact int64 rules. Go protobuf
and gRPC bindings are generated with Buf v1.57.2, protoc-gen-go v1.36.10, and
protoc-gen-go-grpc v1.5.1 via `sh scripts/generate-runtime.sh`; regenerate and
check `git diff --exit-code -- contract/runtime/wire` for drift. gRPC-Go
v1.76.0 supplies the transport only; it is excluded from the engine.

`TestUnaryStreamingControlledSpike` records five batches of 100 sequential
calls after 100 warmup calls per transport on one loopback channel. Raw
samples in `testdata/runtime/transport-spike-go1.27.1-linux-arm64.json` compare
unary and streaming echo overhead with identical payloads. This is a transport
comparison and supplies no application throughput or Node parity claim.
Unary `Invoke` exists for this comparison; application calls use `Connect`.

This newly introduced contract has no supported predecessor; migration of
earlier pre-alpha hand-written wire shapes requires regeneration and a new
catalog/artifact identity. Unsupported protocol majors fail before dispatch.
Workers expose no orchestration RPC. #51 owns process/client lifecycle, #52
owns Node schema/server conformance, and #53 owns authenticated workload and
blob authorization evidence.

A process supervisor owns the worker's whole process tree. The worker's
standard streams go to the null device, never pipes, so a descendant that
outlives the worker cannot keep it looking alive. On POSIX, after the drain,
the supervisor sends SIGTERM and kills at its cleanup bound. Windows has no
such request (#156): the worker starts with its own hidden console, so the
host's Ctrl+C or Ctrl+Break does not reach it, inside a job object set to
kill every member when it closes. The worker is created suspended, joins the
job, and only then is resumed (#224), so it cannot create a descendant outside
the job: a process joins a job at creation only if its parent is already in
it. Stopping terminates the job; closing it after the worker exits also ends
descendants that outlived it, and breakaway is not allowed. A released job
handle is never used again. A worker that fails to join its job is terminated
while still suspended, before it has run anything.

#53 review hardening tightens pre-alpha call validation to the shared five-minute
deadline bound. Earlier clients requesting longer deadlines must split the work
or use durable workflow waiting, then rebuild their application. Node rechecks
absolute expiry before publishing either output or an error: a blocked event
loop is not allowed to turn an expired call into successful output. Explicit
cancellation retains the logical operation key. Neither deadline nor cancel
claims to undo an external effect already dispatched by trusted code.
