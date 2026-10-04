# ADR 0015: Package identity, compatibility, and trust protocol

- Status: accepted for implementation in E13-T01 (#70)
- Date: 2026-10-03
- Owners: framework package contract (`contract/package`); hosted registry
  implementation belongs to the separate registry product

## Context

Node and workflow packages need portable immutable identities and consumer
checks that work without a hosted service. This record defines the framework
contract and the registry boundary. It does not claim the future CLI resolver,
filesystem cache, hosted registry, namespace ownership service, or install
workflow has shipped. Those remain with E13-T02–T04 and the separate registry
product.

## Package document

A package is a JSON bundle containing a versioned manifest, optional publisher
signature, and artifact bytes (base64 in JSON). The manifest binds:

- `identity.name` as exactly `namespace/name` and stable `major.minor.patch`
  version;
- `kind` (`node` or `workflow`) and the SHA-256 digest of the exact artifact
  bytes;
- a required typed metadata block: node packages carry the existing
  `node.Descriptor`; workflow packages carry the existing
  `contract.Document`; an existing `tool.Manifest` may be explicitly declared
  as agent policy;
- zero or more unique namespaced dependencies, each with a version range and
  optional required manifest digest;
- engine and schema version ranges, plus optional named runtime ranges;
- a required SPDX license identifier (or an explicitly named `LicenseRef`),
  bounded to 256 bytes; and
- source URL, hexadecimal source revision, and bounded builder identity.

The node descriptor and workflow document are validated by their owning
contracts. Their input/output schemas (including workflow node and binding
schemas) must also parse as the existing bounded `contract/schema.Schema`
subset. Node effect, required-capability, and determinism declarations must
become the ordinary node package's capability/effect declaration. If a node
also explicitly declares `tool.Manifest`, its effects, capabilities, and
determinism must match the node descriptor. A workflow may carry a
`tool.Manifest` only when explicitly claiming the existing agent-compatible
policy; the workflow document itself currently has no aggregate capability
field. Missing policy metadata never implies agent eligibility: the registered
agent catalog must still fail closed. This package does not invent
`trusted-legacy`, `denied-to-agents`, or another capability vocabulary.
Metadata validation and canonicalization do not execute package code. When
present, `capabilityManifest` uses the existing `tool.Manifest` JSON member
names and validation (`Version`, `Compatibility`, `Effects`, `Capabilities`,
`SecretRefs`, `Deterministic`); it is a signed declaration, not agent admission
or proof of safety. Package signature verification establishes package
integrity under the caller's key policy only. It does not register a node or
workflow with `agent.Catalog`, and it does not make an ordinary descriptor
agent-eligible. That boundary still requires explicit existing tool policy
and trusted application registration; missing or invalid manifests fail
closed there. This contract does not reinterpret `trusted-legacy` or
`denied-to-agents` as valid `tool.Manifest` values.

Before document/schema validation or metadata cloning, the native package API
applies the 64 KiB canonical-manifest budget and bounded collection limits:
256 workflow node descriptors, 256 bindings, 1,024 instructions, 4,096 total
references, 64 path segments per reference, and existing node/tool capability
list limits. Raw schemas individually cannot exceed the manifest ceiling. The
final encoded manifest is checked against the exact 64 KiB ceiling as well.

The canonical manifest is compact UTF-8 JSON with struct fields in the order
shown above, omitted `omitempty` fields absent, dependencies sorted by package
name, runtime-map keys sorted lexicographically, descriptor capability lists
sorted, any declared tool-manifest lists sorted, schema object keys
canonicalized, and workflow node/binding descriptors sorted by ID (instruction
order remains significant). Strings use Go
`encoding/json` escaping, including lowercase `\u003c`, `\u003e`, and
`\u0026` for `<`, `>`, and `&`; no insignificant whitespace or trailing
newline is included. The golden digest in `local-node-v1.json` pins this byte
profile for non-Go consumers. The manifest digest is SHA-256 of those bytes.
The signature is Ed25519 over the same canonical bytes; it therefore binds
identity, compatibility, license, provenance, dependencies, artifact digest,
schemas, and capability metadata. The artifact digest separately checks the
exact artifact bytes. Artifact bytes remain opaque to this contract; the
synthetic payload in `local-node-v1.json` is only an artifact-digest fixture,
not typed package evidence. Typed evidence is the separately parsed existing
descriptor/document, schema, and capability contracts. A signature is an
attestation, not proof that a build was reproducible or that source is safe.
The signed manifest co-binds typed metadata with an artifact digest but this
slice does not extract metadata from an archive or prove that descriptor and
workflow declarations correspond to executable artifact contents.

Compatibility and dependency constraints use this portable subset of semantic
version expressions: whitespace-separated exact or comparator clauses, for
example `>=1.2.0 <2.0.0`. OR expressions, wildcards, and prerelease versions are
not supported, whitespace-only ranges are invalid, and numeric components must
fit unsigned 64-bit values. Consumers check engine, schema, every declared
runtime, and package dependencies during resolution. This contract verifies
compatibility inputs; graph resolution, lock generation, cycle diagnostics,
cache management, and atomic project edits belong to E13-T02/#71 and later
issues.

## Immutability and local use

The `(namespace/name, version)` identity maps to one canonical manifest and
artifact. Re-publishing identical content is idempotent; any changed manifest,
artifact, or signature for that version conflicts. No update or delete operation
exists in the protocol. A new version creates a new identity.

Package bytes can be verified and stored in-process by `Bundle.Verify` and
`package.Store` without a registry connection. This is the framework's local
offline contract; it does not provide durable filesystem caching or the CLI
install/remove/update commands. Consumers must supply the target engine, schema,
and runtime versions when checking a bundle.

The in-memory store defaults to 256 packages and 128 MiB of aggregate artifact
bytes. Hard ceilings are 4096 packages and 1 GiB; callers may select lower
limits with `NewStoreWithLimits`. At capacity, an identical existing publish
still succeeds. New publishes fail with `store_capacity_exceeded`, checking
package count before aggregate bytes for deterministic saturation behavior.

## Trust

There are two results, deliberately distinct:

| Result | Required evidence | Meaning |
| --- | --- | --- |
| `unsigned-local` | No signature and an explicit caller policy allowing bytes from an already-established local source | Local trust only; never implies publisher identity |
| `trusted` | Valid Ed25519 signature over canonical manifest and key ID present in the caller's trusted-key set | Manifest integrity and possession of the configured signing key |

Missing signatures fail by default. Unknown keys and bad signatures always fail.
Signer key IDs are limited to 128 allowed ASCII characters, and the encoded
Ed25519 signature must be exactly 88 bytes before decoding.
The caller is responsible for trusted-key distribution, rotation, revocation,
and deciding which local sources qualify for unsigned use. Registry TLS and
service authentication are separate from package publisher signatures and from
runtime sandboxing. Native package code remains application-trusted code.

## Publisher ownership and authorization

Every hosted publishing request must authenticate a publisher principal using
the service's account or workload credentials, independently of the package
signature. The service authorizes that principal against an explicit namespace
grant and an active signing-key binding for that namespace. A valid Ed25519
signature proves possession of its key and integrity of the manifest; it does
not establish namespace ownership, grant publish rights, or replace service
authentication. A service must not infer ownership from the first signed
package it receives.

Namespace creation/claim must establish an owner through the registry's
authenticated account or organization verification process before accepting
publishes. Only the owner or a principal the owner explicitly grants publish
authority may publish there. An ownership transfer requires an authenticated,
audited transfer approved by the current owner and accepted by the new owner;
it changes future authorization only and never rewrites historical package
ownership metadata or immutable package bytes. Key rotation is an explicit
owner-authorized key-binding change: the owner registers the new key before
use and may overlap old and new active keys during a transition. A retired key
ceases to authorize new publishes when retirement takes effect; revocation
immediately blocks future publishes signed by that key. Retired or revoked
keys and their historical signatures remain recorded; rotation or revocation
does not re-sign, replace, or delete an existing `(identity, version)`. A
different signature for an existing version is an immutable-version conflict;
publish rotated signatures as a new package version. Consumers relying on
current revocation state must obtain fresh authenticated registry trust data;
offline signature verification alone cannot establish that a key remains
unrevoked.

Publishing returns `401` when service credentials are missing or invalid and
`403` when the authenticated principal lacks the namespace grant or its key
binding is inactive/revoked. Both are terminal failures: consumers and
automation must not retry as unsigned, fall back to a different namespace, or
interpret either response as a successful publish. Private mirrors that accept
publishes inherit these authentication, grant, key-binding, and immutability
rules; mirroring content does not confer publish entitlement. The local
`unsigned-local` policy is only a consumer decision for an established local
source and never grants or implies hosted-publish authorization.

License and provenance fields are mandatory and syntax-checked. The current
contract does not include the full SPDX license list or verify claims about
source repositories, revisions, builders, or build attestations. Review policy
and provenance verification beyond the declared fields belong to the registry
and release pipeline.

## Consumer and publishing protocol

Base path: `/v1/packages/{namespace}/{name}/{version}`.

| Method | Success | Other defined response | Request/response |
| --- | --- | --- | --- |
| `GET` | `200` | `404` not found | Returns the JSON bundle |
| `PUT` | `201` created; `200` identical existing content | `401` unauthenticated; `403` not authorized; `409` immutable-version conflict | Sends and returns the JSON bundle |

Requests and responses are bounded to the base64 encoding of an 8 MiB artifact
plus the independently bounded 64 KiB canonical manifest and 728 bytes for the
signature/envelope/newline. The fixed envelope reserve covers the 128-byte key
ID, 88-byte encoded signature and 512 bytes for their JSON fields and wrappers.
An actual signed maximum-artifact/maximum-manifest round trip is checked by
`protocol-boundary.json`; neither ceiling consumes the other's allowance.
Registry base URLs are limited
to 2048 bytes. A successful `PUT` response must verify to the exact submitted
manifest digest, artifact digest, signature, and identity; a registry cannot
acknowledge a substituted but otherwise valid package. The client rejects
unsupported URL schemes, URL credentials, unknown response fields, trailing
JSON, oversized documents, incompatible packages, and invalid integrity or
trust evidence. The status and ownership rules above are framework
service-contract requirements; their implementation, transport
policy, moderation, search, billing, quotas, and audit storage are owned by the
separate hosted registry product. The registry must preserve immutable
identity semantics and must not treat a successful HTTP response as signature
verification by itself.

The checked-in `testdata/packages/protocol-fixtures.json` is consumed by tests
running the actual package client against an in-process mock registry backed by
the same immutable store contract. It specifies successful fetch, explicit
unsigned trust, incompatibility, not-found, idempotent publish, version
conflict, substituted publish response, authentication and namespace
authorization denials, tampered artifacts and creation of a new immutable
version. Its predeclared effect counts measure actual registry requests and
new retained package identities, not an uninstrumented execution counter.
Package operations have no artifact execution API; these fixtures do not
experimentally certify execution safety or a sandbox. The mock service is a
protocol fixture, not an implementation of hosted authentication or ownership
verification.

## Compatibility and evidence

| Change | Classification | Consumer action |
| --- | --- | --- |
| Add a manifest field with deterministic zero-value semantics | Additive only if old readers can safely reject/ignore it as versioned | Bump manifest format when old readers cannot preserve its meaning |
| Change identity syntax, canonical bytes, digest inputs, range grammar, or signature bytes | Wire-breaking | Bump manifest/protocol version and publish migration fixtures |
| Tighten trust or compatibility acceptance | Behavioral | Add accepted/rejected fixtures and migration guidance |
| Replace content under an existing package version | Prohibited | Publish a new version |

Golden and negative cases are in `testdata/packages`. Go tests validate actual
manifest verification, typed metadata validation and signature binding,
local immutable storage, and the consumer protocol against the mock service.
Tests prove missing/invalid metadata rejection, metadata-tampering signature
failure, canonical schema ordering, and that store reads cannot mutate retained
typed metadata. The checked-in artifact payload remains opaque test input and
is not counted as typed or executable package evidence.
This E13-T01 slice does not establish package resolution, lock generation,
durable offline cache behavior, atomic project edits, CLI add/remove/update,
hosted-registry availability, namespace ownership service, build
reproducibility, sandboxing, or package execution.

## Verification record

On 2026-10-03, Go 1.27.1 on Darwin arm64 passed
`go test -race ./contract/package -count=10`. A Docker Linux run using Go
1.27.1, `GOMAXPROCS=3`, and `-p=3` passed `go vet -p=3 ./...` and
`go build -p=3 ./...`. The full `go test -race -p=3 ./...` run passed the
package contract and all reported packages except the unrelated
`trigger/sse.TestHeartbeatKeepsSilentStreamsAlive`, which observed an extra
heartbeat at event 4. `trigger/cron` passed in that Linux run. The package
signature test uses the fixed synthetic Ed25519 seed `0x42` repeated to the
standard seed length. After integrating `origin/main` at
`a910ef39e151a316a01dd0fcc9a5f27952c0424c`, the complete bounded race command
passed, including the contention and SSE suites; this makes the first SSE
failure a reproduced flake rather than a persistent failure. No GitHub Actions
workflow was enabled or dispatched.

After the independent package review regressions were added, Go 1.27.1 on
Linux passed bounded `go vet -p=3 ./...`, `go build -p=3 ./...`, and
`go test -race -p=3 ./...` with `GOMAXPROCS=3`. The subsequent change that
checks the artifact-size limit before copying caller-provided bytes passed
`GOMAXPROCS=3 go test -p=3 -race ./contract/package -count=10`,
`GOMAXPROCS=3 go vet -p=3 ./contract/package`, `go test ./contract/package`,
and `git diff --check`. The parent’s serialized combined platform gates remain
pending; these local runs do not substitute for their final result.

After adding the publisher-authorization policy and `401`/`403` denial
fixtures, Go 1.27.1 on Darwin arm64 passed
`GOMAXPROCS=3 go test -p=3 -race ./contract/package -count=3`,
`GOMAXPROCS=3 go vet -p=3 ./contract/package`, and `git diff --check`.

After binding existing node descriptors, workflow documents, schemas, and
optional explicit agent manifests, Go 1.27.1 on Darwin arm64 passed focused
package tests (`GOMAXPROCS=3 go test -p=3 ./contract/package`) including
metadata omission, schema rejection, canonicalization, signature tampering,
ordinary-package trust without implicit agent admission, and workflow-document
cases. Preflight-bound checks are part of this issue branch; resolver/installer
evidence remains with E13-T02–T04.
