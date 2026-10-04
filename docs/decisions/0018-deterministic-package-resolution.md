# ADR 0018: Deterministic package resolution, locks, and verified cache

- Status: implementation in review for E13-T02 (#71)
- Date: 2026-10-04
- Owners: framework package contract (`contract/package`) and package manager
  integration (`internal/package`)
- Supersedes: none; refines the implementation boundary in ADR 0015

## Decision

Resolve Blok package manifests as one deterministic graph, persist a canonical
exact lock, and replay that lock only after verifying every artifact and edge.
The existing language package managers remain authoritative for their own
dependencies. This layer records their resolved lock inputs and execution
context; it does not install foreign dependencies or execute package contents.

The public contract lives in `contract/package`. `Resolve` sorts candidates
using semantic-version precedence and selects the highest stable version
satisfying every incoming constraint, including optional manifest-digest
pins. Resolution uses bounded backtracking so a compatible transitive graph
can be found without making selection depend on source enumeration order.
Cycles, incompatible constraints, unsupported range grammar, invalid source
catalogs, and resource-limit exhaustion have distinct diagnostics. The
accepted portable range grammar remains the exact/comparator subset from ADR
0015; ORs, wildcards, and prereleases are rejected.

A format-versioned lock contains sorted roots, each selected package identity,
canonical manifest digest, artifact digest, trust class, exact resolved edges,
and captured native-manager lock records. `Lock.Validate` proves that roots
and every edge point to the recorded identities/digests, rejects cycles and
duplicate or multi-version identities, and checks native lock paths and
collection bounds. `Canonical` fixes byte ordering and `Digest` hashes those
bytes. `VerifyLock` fetches only the exact identities in the lock and checks
current signature/trust policy, compatibility, artifact digest, manifest
digest, and dependency edges. It never re-resolves to a newer version.

The filesystem cache is bounded to 256 identity indexes and 144 MiB of
content-addressed bundle data, with each encoded bundle limited by the package
contract's bound. A cross-process lock serializes capacity reservations,
immutable-identity checks, index publication, and lock replacement. Blobs are
published before their index using same-directory temporary files, sync, and
atomic rename. An identity index points to exactly one immutable blob; reads
re-hash the blob, require canonical encoding, and run full package
verification under the caller's policy on every access. There is no
hash-sorted fallback. A `(name, version)` already present can only be inserted
again with identical manifest, artifact, and signature. Corruption fails
closed. Offline `CacheSource.FetchLocked` verifies an existing lock's exact
graph, while offline candidate listing is scoped to the requested package and
trust policy. A missing exact identity returns the offline-missing diagnostic.

Lock writes use canonical bytes and atomic same-directory replacement. Cache
and lock coordination is cancellation-aware: lock acquisition uses bounded
nonblocking retries and observes the request context on Unix and Windows.
Windows support is implemented but native Windows execution evidence belongs
to the separate #157 milestone; no Windows validation claim is made here.

## Native managers and unsupported inputs

Go capture invokes the Go tool to verify modules and list the exact module
graph with `-mod=readonly`; it binds `go.mod`, optional `go.sum`, an in-project
`go.work`/`go.work.sum`, Go/toolchain version, target, cgo, flags, and workspace
context. Versioned replacements record module checksums. A local replacement
or additional workspace module within the project is snapshotted by sorted
relative file path, file mode, size, and content. Symlinks, special files,
oversized source trees, external workspace/module paths, and alternate
modfile/overlay inputs fail with actionable unsupported-input diagnostics.

npm capture asks npm to validate/project the existing lock graph without
installing packages or running scripts, and binds `package.json`,
`package-lock.json`, npm/Node versions, and every exact package version and
integrity. Only lockfile versions 2 and 3 are accepted. Workspace/link and
local-file dependencies are explicitly unsupported until their source trees
can be pinned; missing integrity fails closed. Registry URLs, npm configuration,
credentials, and command stderr are not copied into lock records or
diagnostics. No universal installer or lifecycle-hook execution is added.

## Consequences and scope

The lock describes execution-relevant Blok artifacts plus the native manager
graph/context that owns foreign dependencies. Native lock file digests bind
the source lock inputs; local Go source is additionally content-pinned. npm
workspace links remain unsupported rather than being represented as if a
lockfile digest alone pinned mutable source. Offline exact replay requires all
locked Blok artifacts to be present and verified. The cache is not a registry,
publisher, package executor, CLI add/remove/update transaction, sandbox, or
reproducible-build attestor. Atomic project mutations and the user-facing CLI
remain E13-T03 (#72); hosted publishing remains separate.

## Verification record

Focused fixtures in `testdata/packages/resolution-fixtures.json` predeclare
expected selected-package, source-read, package-execution, and external-effect
counts for deterministic selection and conflict/cycle/unsupported-range
failures. Cache tests cover exact replay, offline missing, corruption,
immutable identity conflict, trust-policy scoping, concurrent capacity and
lock writes, malformed lock bounds, and context cancellation. Native
integration tests invoke the actual Go and npm tools and verify lock files
remain unchanged; a Go local-replacement test proves source-content changes
alter the recorded digest and external replacements fail.

On final implementation commit `37c40bd753eabf238144d0270cafcc59fd8ac894`,
after refreshing `origin/main` to `8a3a0c470cd3c23ee6744956b430fa4fb736a379`,
Go 1.27.1 on Darwin arm64 passed `go mod verify`, `gofmt` cleanliness,
`git diff --check`, `GOMAXPROCS=3 go vet -p=3 ./...`,
`GOMAXPROCS=3 go test -race -p=3 ./...`, and
`GOMAXPROCS=3 go build -p=3 ./...`. The changed package tests also passed
`GOMAXPROCS=3 go test -p=3 ./contract/package ./internal/package` and a
three-count focused race run before the final context/strict-lock hardening;
the full race run above includes that final hardening. The internal package
test binary cross-compiled for `GOOS=windows GOARCH=amd64`; this is not native
Windows execution evidence. No GitHub Actions workflow was dispatched.
