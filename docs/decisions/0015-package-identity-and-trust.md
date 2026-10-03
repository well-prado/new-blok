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
- zero or more unique namespaced dependencies, each with a version range and
  optional required manifest digest;
- engine and schema version ranges, plus optional named runtime ranges;
- a required SPDX license identifier (or an explicitly named `LicenseRef`); and
- source URL, hexadecimal source revision, and bounded builder identity.

The canonical manifest is compact UTF-8 JSON with struct fields in the order
shown above, omitted `omitempty` fields absent, dependencies sorted by package
name, and runtime-map keys sorted lexicographically. Strings use Go
`encoding/json` escaping, including lowercase `\u003c`, `\u003e`, and
`\u0026` for `<`, `>`, and `&`; no insignificant whitespace or trailing
newline is included. The golden digest in `local-node-v1.json` pins this byte
profile for non-Go consumers. The manifest digest is SHA-256 of those bytes.
The signature is Ed25519 over the same canonical bytes; it therefore binds
identity, compatibility, license, provenance, dependencies, and artifact digest. The
artifact digest separately checks the exact artifact bytes. A signature is an
attestation, not proof that a build was reproducible or that source is safe.

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
| `PUT` | `201` created; `200` identical existing content | `409` immutable-version conflict | Sends and returns the JSON bundle |

Requests and responses are bounded to the base64 encoding of an 8 MiB artifact
plus 64 KiB of manifest/signature JSON overhead. A successful `PUT` response
must verify to the exact submitted manifest digest, artifact digest, signature,
and identity; a registry cannot acknowledge a substituted but otherwise valid
package. The client rejects unsupported URL schemes, URL credentials, unknown
response fields, trailing JSON, oversized documents, incompatible packages,
and invalid integrity or trust evidence. Registry authentication, transport
policy, namespace authorization, ownership verification, moderation, search,
billing, quotas, and audit implementation are owned by the separate hosted
registry product. The registry must preserve immutable identity semantics and
must not treat a successful HTTP response as signature verification by itself.

The checked-in `testdata/packages/protocol-fixtures.json` is consumed by tests
running the actual package client against an in-process mock registry backed by
the same immutable store contract. It specifies successful fetch, explicit
unsigned trust, incompatibility, not-found, idempotent publish, version
conflict, substituted publish response, and tampered artifact outcomes. Expected
package execution count is zero: installation and inspection never execute
package contents.

## Compatibility and evidence

| Change | Classification | Consumer action |
| --- | --- | --- |
| Add a manifest field with deterministic zero-value semantics | Additive only if old readers can safely reject/ignore it as versioned | Bump manifest format when old readers cannot preserve its meaning |
| Change identity syntax, canonical bytes, digest inputs, range grammar, or signature bytes | Wire-breaking | Bump manifest/protocol version and publish migration fixtures |
| Tighten trust or compatibility acceptance | Behavioral | Add accepted/rejected fixtures and migration guidance |
| Replace content under an existing package version | Prohibited | Publish a new version |

Golden and negative cases are in `testdata/packages`. Go tests validate actual
manifest verification, signature checks, local immutable storage, and the
consumer protocol against the mock service. These checks establish the
framework contract only; they do not establish hosted-registry availability,
durable offline cache behavior, namespace ownership, build reproducibility,
sandboxing, or CLI integration.

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
