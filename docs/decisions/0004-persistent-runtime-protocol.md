# ADR 0004: Persistent worker runtime protocol

- Status: accepted for implementation
- Date: 2026-10-02
- Roadmap: E08-T01 (#50)

## Decision

Foreign-language nodes use one long-lived, bidirectional stream per selected
worker generation. The native engine is the client and the worker is the
server. The worker receives only a single node call at a time; it has no
workflow, trigger, store, retry, or child-run API.

The `contract/runtime/runtime.proto` file is the wire source of truth. The Go
package beside it defines validation and negotiation semantics without a
transport dependency. Protocol major versions are incompatible. Minor versions
are backward-compatible only when the worker advertises at least the client's
minor version.

## Identity and retry rules

Every connection binds an artifact digest, catalog digest, and non-zero worker
generation. A mismatch rejects the connection before traffic. Every call has a
unique call ID and attempt ID. Attempts may share a logical idempotency key;
transport reconnect never silently retries a call whose effect outcome is
uncertain. Only errors explicitly classified transient and not uncertain are
safe to retry.

## Bounds and value semantics

The default maximum frame is 1 MiB, blob is 8 MiB, and concurrent calls is 64.
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

The semantic package has negative fixtures for identity, generation, limits,
capability, deadline, and uncertainty rules. A generated binding drift check,
real gRPC lifecycle, authenticated transport, and measured unary/streaming
comparison belong to the subsequent worker issues and are not claimed by this
ADR alone.
