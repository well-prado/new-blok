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
and concurrent calls are 64. Input reserves 1024 bytes for bounded call metadata.
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
