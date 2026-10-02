# New Blok

Read `ROADMAP.md`, `docs/architecture.md`, and the assigned issue before editing. This is the framework repository. Studio, the hosted registry, and BLOK Cloud are separate products; framework integration contracts belong here.

## Architecture invariants

- Applications own a Go module and executable. Go-only applications require no foreign runtime.
- Nodes accept typed input and return typed output/error. Nodes never import or invoke other nodes; workflows own composition.
- Builders record structural programs; they never execute business effects. No raw source expression evaluation.
- The engine cannot import trigger implementations, stores, UI assets, broker clients, ORM packages, or AI provider SDKs.
- Modules register explicitly through composition; no global registration or network connections in `init`.
- Values are logically immutable; maps, slices, pointers and parallel branches require ownership isolation.
- Native nodes are trusted application code. Manifests and gRPC alone do not sandbox code.
- Node.js is the first external runtime. Every later worker passes the same negotiated persistent gRPC contract. Never spawn a process per step.
- Durable acknowledgment follows committed admission. Unknown effects remain uncertain until idempotency or reconciliation establishes their outcome.
- Approval and evidence gates run before dispatch or result publication as applicable. Model output cannot grant authority.
- All queues, payloads, retries, recursion and concurrency are bounded. Every bound has a documented saturation policy.
- Performance claims require representative workloads, equivalent guarantees, repeated samples and raw evidence.

## Delivery

- One issue, branch `codex/<issue>-<description>`, and PR per implementation change. Keep GitHub Project status current.
- Stable `E##-T##` IDs are never renumbered. Parent epics aggregate evidence; they do not replace implementation issues.
- Read issue dependencies. A green narrow test cannot close a broader requirement.
- Add a package/file only for a real responsibility. Avoid copied SDK source, empty scaffolds, speculative abstractions and second sources of truth.
- Use normal Go tools. Prefer standard library and explicit dependency injection. Document any new dependency's purpose.
- Focused tests first, then `go vet ./...`, `go test -race ./...`, and `go build ./...`. Run issue-specific conformance, crash, fuzz or load gates when required.
- Fixtures are synthetic. Never commit credentials, private source data, local journals, user payloads or public performance claims unsupported by evidence.
- Generated artifacts must be deterministic, marked, and checked for drift. Update examples and machine-readable contracts with authoring changes.
- Report implemented behavior, evidence and limits. Do not mark tasks done because a mock or source fixture resembles the intended behavior.

## Current state

The repository contains a tested CLI bootstrap and a delivery roadmap. Application APIs in `docs/architecture.md` are proposals until their implementation issues pass. Do not present planned commands as available.
