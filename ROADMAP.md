# New Blok — delivery roadmap

Revision 1 · 2026-10-01 · framework implementation

## 1. Goal and current state

Build an open-source Go application framework with typed nodes, structural workflows, independent triggers, honest durability and persistent language workers. Developers and AI assistants should compose entire applications from reusable generic nodes and small custom nodes, test them with normal tools and deploy selected capabilities.

The current repository contains a tested Go CLI bootstrap (`help`, `version`), CI and governance. Application engine/tooling capabilities below are planned until their issues pass. This roadmap preserves the Go application design's scope while requiring stronger evidence than the architecture lab provided. Millions of requests per second is a fleet capacity target; it is a release claim only after controlled measurements with stated workload/guarantees.

## 2. Product boundaries

This repo owns the Go framework, CLI, node/workflow package client, generic node contracts/reference adapters, runtime SDKs/workers, and conformance suites. Studio UI, hosted registry service and BLOK Cloud are separate products. Their authenticated/versioned framework contracts and consumer fixtures are included; their frontend, hosting control service, billing and registry moderation implementation are not.

Go is native. Node.js is the first external runtime, followed by the old framework's Python3, Rust, Java, Kotlin, C#, PHP, Ruby, Swift, Dart and Elixir workers plus optional remote Go and separately selected Bun/Deno. All nine trigger kinds are required: HTTP, webhook, worker, cron, pubsub, gRPC, SSE, WebSocket and MCP. Nodes are typed tools; workflows are composed tools. All share capability, schema, identity and admission rules.

## 3. Milestones and release evidence

Milestones are capability gates without artificial dates. M0–M1 establish a native application; M2–M3 establish recoverable work; M4 establishes Node.js; M5–M7 expand adapters/tooling/AI/inspection; M8 proves distributed coverage; M9 gates production release. Task milestones may precede an epic's final milestone when later work consumes a foundation.

| Milestone | Exit evidence |
| --- | --- |
| M0 — Contracts and sustainable foundation | A tested Go bootstrap, reviewed architecture decisions, schema/identity contracts, synthetic fixture inventory and enforceable dependency rules. No unfinished API is advertised as released. |
| M1 — Complete native Go application | A newcomer builds, tests and serves a typed quote application without hand-editing IR or installing a foreign runtime. Invalid types have actionable diagnostics; bounded execution and real HTTP pass race and integration tests. |
| M2 — Durable orders and jobs | One selected embedded backend atomically admits and resumes accepted orders/jobs. Process-kill tests prove acknowledgment barriers, deduplication, uncertainty and outbox behavior. |
| M3 — Recovery across control flow and versions | Nested branches, loops, parallel joins, waits, signals and child runs recover after every specified crash boundary. Artifact mismatch, compaction, backup and reconciliation fail safely. |
| M4 — Node.js and persistent worker conformance | Node.js is a real persistent gRPC worker with schema/error/cancellation/capacity/generation parity, authenticated transport and fault-tested lifecycle; tiny-call overhead is measured honestly. |
| M5 — All nine modular triggers | HTTP, webhook, worker, cron, pubsub, gRPC, SSE, WebSocket and MCP have real adapters, shared admission tests, protocol integration tests and removal/footprint evidence. |
| M6 — Developer tooling and package ecosystem | CLI scaffold/generate/check/dev/test/inspect and node/workflow installation work in both layouts with deterministic locks, offline cache, atomic changes and trusted artifacts. Generic nodes and recipes use the same contracts. |
| M7 — AI tools, Studio APIs and observability | Agents discover and compose nodes/workflows under enforced policy, including custom nodes. Versioned authorized notebook inspection and optional production telemetry are consumed by contract fixtures without placing Studio/Cloud UI here. |
| M8 — Distributed deployment and runtime coverage | Self-hosted deployment contracts, multi-version routing, fenced ownership and resharding pass failures. Every target worker runs actual conformance on a published matrix. Capacity evidence includes distributed topology and overload. |
| M9 — Production release and ecosystem handoff | Executable current-Blok parity, live-model DX evaluation, controlled native/worker/durable/fleet benchmarks, security review and signed reproducible releases pass. Publish supported limits and separate Studio/registry/Cloud handoffs. |

## 4. Execution model and project fields

[GitHub Project](https://github.com/users/well-prado/projects/15) is linked to this repository. The English roadmap follows the Conciliei approach: parent epics with real sub-issues, stable E##-T## IDs, milestones with evidence gates and issue-level tests, criteria and dependencies. Detailed engineering scope stays here and in the matching issue; `.github/roadmap.json` maps stable IDs to GitHub identities for audit/synchronization.

- **Status:** Backlog, Ready, In progress, In review, Done, Blocked. Ready requires all dependencies and required contract decisions; Blocked needs a concrete unmet prerequisite. Never claim Done from a source-shaped mock.
- **Roadmap ID / Epic / Area:** stable IDs and owning implementation responsibility.
- **Priority:** P0 protects critical contracts/release gates, P1 delivers required capability, P2 improves optional experience. Priority does not waive dependencies.
- **Size:** S is a bounded small change, M is a coherent implementation slice. Split a larger task before implementing without renumbering existing IDs.
- **Review:** A = automated gates plus normal maintainer review. R = independent specialist review of the relevant security/contract/durability/capacity evidence.

Use native GitHub parent/sub-issue and blocked-by relationships in addition to dependency checklists. Every implementation issue gets its own `codex/<issue>-<description>` branch and PR. Keep project status current. No future issue is marked complete by this planning delivery; only the verified bootstrap is Done.

## 5. Cross-cutting contract requirements

Read [architecture.md](docs/architecture.md) and [AGENTS.md](AGENTS.md). Each task inherits these invariants:

1. Nodes never import or invoke nodes. Workflows own composition. Builders record programs and never execute business effects.
2. Native Go uses ordinary typed functions, constructors, `context.Context` and application-owned binaries. No foreign runtime for a Go-only app.
3. Engine dependencies stay narrow. Optional adapters, stores, providers, telemetry and product assets are independently selectable; no global registration or effectful discovery.
4. Schema semantics, structural refs, immutable value ownership, artifact identity and source diagnostics agree across native/wire/journal paths.
5. Admission is authorized and bounded. Every queue/payload/retry/depth/tenant resource policy has saturation behavior. Principal trust cannot come from caller JSON.
6. Durable acceptance follows committed state. Retries require truthful effect contracts; unknown effects stay uncertain. Recovery retains exact artifact/checkpoint identity.
7. Agent catalog/approval/evidence rules fail closed. Child workflows cannot widen authority. Native code is trusted; manifests and gRPC do not imply sandboxing.
8. Debug telemetry can sample/drop with visible policy. Required audit/state uses reliable paths. Secret references stay opaque, data redaction is explicit, all fixtures are synthetic.
9. Performance claims disclose workload/topology/guarantees, versions, raw repeated samples, warmup, distributions, saturation and failures. Equal step counts or no-op timing cannot establish application parity.
10. CLI installs packages atomically with verified identities/locks/artifacts; native language managers still own language dependencies. Hosted ecosystem products remain separate.

## 6. Definition of Done

An issue is complete only after all acceptance criteria and its exact validation scenarios pass. The PR includes focused tests and applicable race/fuzz/protocol/crash/load/consumer evidence, commands/versions/seeds/raw results, compatibility impact and limits. Run `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`; native SDK checks are additional. Documentation/examples/generated artifacts remain truthful and reproducible. Required review concludes with evidence. Project/issue state is updated after merge and verified behavior; bootstrap delivery records the initial commit directly.

A milestone closes after integrated exit evidence and a requirement audit, not after counts alone. Unsupported platform/runtime guarantees remain explicit. No release blocker is moved to a follow-up merely to close a milestone. Proposed APIs, lab status and green unrelated checks are not implementation evidence.

## 7. Epic catalog

| Epic | Completion milestone | Objective |
| --- | --- | --- |
| E01 — Open-source foundation and delivery governance | M0 | Establish a small, tested repository with truthful public documentation and evidence-based delivery controls. |
| E02 — Canonical contracts, schemas and identities | M0 | Freeze one inspectable structural contract with precise cross-language value and identity semantics. |
| E03 — Typed Go nodes and workflow authoring | M1 | Make ordinary Go functions composable with compile-time wiring and inspectable structural output. |
| E04 — Compiler and portable execution program | M1 | Validate programs once and lower structural references without hidden runtime evaluation. |
| E05 — Bounded Go execution engine and test harness | M1 | Execute compiled workflows correctly with bounded resources and application-friendly testing. |
| E06 — Application lifecycle and HTTP vertical slice | M1 | Deliver an application-owned Go executable with real HTTP and explicit module lifecycle. |
| E07 — Durable state, recovery and version retention | M3 | Establish precise single-host recovery before adding distributed ownership. |
| E08 — Persistent gRPC runtime protocol and Node.js first | M4 | Connect foreign nodes without giving workers a second workflow engine or an unbounded process lifecycle. |
| E09 — Complete modular trigger catalog | M5 | Implement every promised protocol against one admission and lifecycle contract. |
| E10 — Generic nodes and complete application recipes | M6 | Make reusable generic nodes and custom business nodes obey the same contracts without turning the core into an ORM. |
| E11 — Go CLI and everyday developer experience | M6 | Provide fast feedback and ordinary Go ownership without hidden registration or mandatory hosted services. |
| E12 — Unified and classic layouts with ownership enforcement | M6 | Offer layout choice without changing identities, contracts or node independence. |
| E13 — Node and workflow package client and registry contracts | M6 | Install reusable packages safely and reproducibly while keeping the hosted registry a separate product. |
| E14 — Policy-enforced AI tools and composed workflow tools | M7 | Let agents assemble complete applications while execution enforces authorization, evidence and budgets. |
| E15 — Notebook inspection APIs and Studio handoff | M7 | Expose every dev execution step through a secure versioned API consumed by a separate simple Studio. |
| E16 — Optional production observability and reliable audit | M7 | Make production behavior measurable without unbounded overhead or debug event guarantees masquerading as audit. |
| E17 — Self-hosting, artifacts and Cloud integration contracts | M8 | Make applications easy to deploy through reproducible binaries and operational contracts without embedding a hosting product. |
| E18 — Distributed ownership, fencing and capacity | M8 | Scale accepted work across a fleet with tested ownership and failure semantics. |
| E19 — Full runtime and SDK coverage | M8 | Give every promised language a real persistent worker and the same semantic/failure conformance. |
| E20 — Production evidence, migration and open-source release | M9 | Release only what application, security, usability and capacity evidence actually proves. |

## 8. Implementation task briefs

All paths below are ownership targets. Create only files/packages justified by the task; they are not empty scaffold requirements. Proposed package naming may change through its owning decision issue with compatibility review.

### E01 — Open-source foundation and delivery governance

Establish a small, tested repository with truthful public documentation and evidence-based delivery controls.

#### E01-T01 — Bootstrap the public Go repository and linked roadmap

Issue: E01-T01 · Initial status: Done

Roadmap ID: E01-T01
Epic: E01 — Open-source foundation and delivery governance
Milestone: M0 — Contracts and sustainable foundation
Priority: P0 | Size: M | Review: R

## Problem and intended result

Create the application-owned module baseline, CLI help/version command, Apache-2.0 license, README, contribution/security policies, CI, linked public GitHub Project and issue hierarchy. No placeholder engine or empty extension packages.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§1, 2, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `cmd/blok, .github, README.md, LICENSE, AGENTS.md, ROADMAP.md`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Bootstrap CLI builds and rejects unsupported commands without success output.
- [ ] README distinguishes available code from proposed API.
- [ ] CI pins actions and passes formatting/vet/race/build.
- [ ] Every task has milestone, parent epic, dependencies, review and project metadata.
- [ ] Private vulnerability reporting is enabled and no private lab source/payload is published.

## Validation and required evidence

- [ ] Run CLI help/version and unsupported-command exit checks.
- [ ] Exercise output writer failure.
- [ ] Audit repository links, license and GitHub hierarchy against ROADMAP.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

None beyond the repository bootstrap. Confirm current architecture and issue context before starting.

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E01-T02 — Record public API boundaries and architecture decision policy

Issue: E01-T02 · Initial status: Ready

Roadmap ID: E01-T02
Epic: E01 — Open-source foundation and delivery governance
Milestone: M0 — Contracts and sustainable foundation
Priority: P0 | Size: S | Review: R

## Problem and intended result

Resolve public versus internal package ownership, versioning policy, supported schema subset and ADR change process. Carry forward the design proposal while identifying lab claims that remain unproven.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§1, 2, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `docs/architecture.md, docs/decisions, AGENTS.md`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Public APIs have a named responsibility and owner.
- [ ] Contract changes require compatibility/migration evidence.
- [ ] Studio, hosted registry and Cloud have explicit separate-repository boundaries.
- [ ] Lab equal-step and deterministic-model evidence cannot satisfy parity/live-AI gates.

## Validation and required evidence

- [ ] Review API inventory against docs/architecture.md.
- [ ] Add decision fixtures mapping every ambiguous contract to an owning issue.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E01-T01 — Bootstrap the public Go repository and linked roadmap

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E01-T03 — Enforce modular imports and synthetic fixture governance

Issue: E01-T03 · Initial status: Backlog

Roadmap ID: E01-T03
Epic: E01 — Open-source foundation and delivery governance
Milestone: M0 — Contracts and sustainable foundation
Priority: P0 | Size: M | Review: R

## Problem and intended result

Add Go package graph checks that prevent engine imports of adapters/stores/providers/UI and prevent node-to-node imports. Specify allowed domain/utility imports and generated-code ownership.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§1, 2, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/tooling, .github/workflows, testdata`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Engine negative dependency fixtures fail with stable diagnostics.
- [ ] Unified/classic node ownership identifies aliases and transitive forbidden node references.
- [ ] Synthetic fixtures include source/license provenance.
- [ ] No runtime registration or connections happen during discovery.

## Validation and required evidence

- [ ] Test good and bad package graphs, build tags and import aliases.
- [ ] Run checks against a minimal Go-only binary and a selected-adapter binary.
- [ ] Verify private/local data patterns are excluded.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E01-T02 — Record public API boundaries and architecture decision policy

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E01-T04 — Create release quality gates and supported toolchain matrix

Issue: E01-T04 · Initial status: Backlog

Roadmap ID: E01-T04
Epic: E01 — Open-source foundation and delivery governance
Milestone: M0 — Contracts and sustainable foundation
Priority: P1 | Size: M | Review: R

## Problem and intended result

Extend CI with generated drift, race, fuzz smoke, dependency/license scanning and conformance gates as packages arrive. Define controlled performance runner policy and version bump rules.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§1, 2, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `.github/workflows, docs/decisions`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Current supported Go/OS/architecture matrix is explicit.
- [ ] Required jobs run on relevant PRs and no skipped gate masquerades as success.
- [ ] Fuzz/crash/load suites have reproducible seeds and bounded budgets.
- [ ] Benchmark regressions use noise-aware controlled runners.
- [ ] Branch protection is verified for the supported hosting plan.

## Validation and required evidence

- [ ] Induce format, drift, race and dependency failures in synthetic branches/fixtures.
- [ ] Verify CI rejects each.
- [ ] Document restricted fork permissions and release-job credentials.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E01-T03 — Enforce modular imports and synthetic fixture governance

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E02 — Canonical contracts, schemas and identities

Freeze one inspectable structural contract with precise cross-language value and identity semantics.

#### E02-T01 — Specify workflow, binding and descriptor documents

Issue: E02-T01 · Initial status: Ready

Roadmap ID: E02-T01
Epic: E02 — Canonical contracts, schemas and identities
Milestone: M0 — Contracts and sustainable foundation
Priority: P0 | Size: M | Review: R

## Problem and intended result

Reconcile lab workflow/v1 with application-first workflow/binding separation. Specify versioned call/control/output records, source spans and node/workflow descriptors; publish valid/invalid golden fixtures.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 6, 8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract, testdata/contracts, docs/decisions`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] A workflow has domain input/output independent of trigger records.
- [ ] IDs, version/digest syntax and uniqueness are validated.
- [ ] Missing/null, discarded output and optional fields are distinct.
- [ ] Unknown instruction/version fails with actionable code.
- [ ] Document-to-internal conversion is explicit and deterministic.

## Validation and required evidence

- [ ] Round-trip valid nested documents.
- [ ] Reject duplicate IDs, future versions, malformed references and invalid bindings.
- [ ] Fuzz parsing with bounded depth/payload.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E01-T02 — Record public API boundaries and architecture decision policy

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E02-T02 — Implement supported schema validation and normalization

Issue: E02-T02 · Initial status: Backlog

Roadmap ID: E02-T02
Epic: E02 — Canonical contracts, schemas and identities
Milestone: M0 — Contracts and sustainable foundation
Priority: P0 | Size: M | Review: R

## Problem and intended result

Define and enforce scalar ranges, optional/null, objects, collections, unions, unknown fields, explicit defaults, timestamps and binary/blob references for native and wire values. Document static subset limitations.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 6, 8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/schema, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Input and output validation uses one normalized schema contract.
- [ ] Int64 values survive JavaScript boundaries without precision loss.
- [ ] Defaults are applied only by specified normalization.
- [ ] Validation preserves source path and distinguishes absent/null.
- [ ] Unprovable compatibility cannot silently pass.

## Validation and required evidence

- [ ] Golden corpus for integer bounds, decimal policy, nested defaults, null arrays and unknown fields.
- [ ] Native and JSON/protobuf corpus outputs match.
- [ ] Fuzz adversarial nesting and large payloads.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E02-T01 — Specify workflow, binding and descriptor documents

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E02-T03 — Implement immutable artifact identity and deployment manifests

Issue: E02-T03 · Initial status: Backlog

Roadmap ID: E02-T03
Epic: E02 — Canonical contracts, schemas and identities
Milestone: M0 — Contracts and sustainable foundation
Priority: P0 | Size: M | Review: R

## Problem and intended result

Bind human versions to content digests for workflow, native binary, worker catalog, schemas, locks, compiler and checkpoint format. Prevent same-version content replacement.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 6, 8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/artifact, testdata/artifacts`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Canonical digest includes execution-relevant content.
- [ ] Digest changes for binary/schema/lock changes and is stable under irrelevant formatting.
- [ ] Immutable version conflicts are rejected.
- [ ] Manifests contain no secret values or machine-local paths.
- [ ] Matching artifacts can be located without trusting version strings.

## Validation and required evidence

- [ ] Golden digest fixtures.
- [ ] Conflicting version/artifact registration tests.
- [ ] Verify cross-platform manifest determinism and incompatible checkpoint diagnostics.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E02-T01 — Specify workflow, binding and descriptor documents

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E02-T04 — Publish diagnostics and conformance harness contracts

Issue: E02-T04 · Initial status: Backlog

Roadmap ID: E02-T04
Epic: E02 — Canonical contracts, schemas and identities
Milestone: M0 — Contracts and sustainable foundation
Priority: P1 | Size: M | Review: R

## Problem and intended result

Define stable machine-readable diagnostics and reusable store/trigger/worker/SDK semantic conformance runners. Separate missing test coverage from passing implementations.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 6, 8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/conformance, testdata, internal/diagnostic`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Diagnostics include code, source, step, field, expected/actual and remediation.
- [ ] Each harness exercises real implementations through narrow ports.
- [ ] Contract corpus is versioned and deterministic.
- [ ] Unsupported tests/matrices are reported explicitly.
- [ ] Third-party adapter runs require no internal-engine imports.

## Validation and required evidence

- [ ] Golden error snapshots.
- [ ] Deliberately broken adapter fails each applicable invariant.
- [ ] Corpus regeneration has no diff.
- [ ] Run schema corpus against native bootstrap fixtures.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E02-T02 — Implement supported schema validation and normalization
- [ ] E02-T03 — Implement immutable artifact identity and deployment manifests

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E03 — Typed Go nodes and workflow authoring

Make ordinary Go functions composable with compile-time wiring and inspectable structural output.

#### E03-T01 — Implement typed node definitions and explicit registration

Issue: E03-T01 · Initial status: Backlog

Roadmap ID: E03-T01
Epic: E03 — Typed Go nodes and workflow authoring
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: A

## Problem and intended result

Implement typed input/output function descriptors, constructor-injected dependencies, effect metadata and explicit application registries. Keep node identity independent from paths.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 5](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `node, examples/quote`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Nodes accept context.Context and typed input and return typed output/error.
- [ ] Duplicate identity/version and missing metadata diagnostics are stable.
- [ ] Registration performs no business calls or global init side effects.
- [ ] Cancellation and structured domain errors retain classification.
- [ ] Direct node invocation needs no engine.

## Validation and required evidence

- [ ] Unit-test pure quote and injected repository fake.
- [ ] Reject duplicates and incompatible schema descriptors.
- [ ] Test canceled context and panicking node boundary.
- [ ] Run race tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E02-T04 — Publish diagnostics and conformance harness contracts

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E03-T02 — Implement typed whole-value workflow references and outputs

Issue: E03-T02 · Initial status: Backlog

Roadmap ID: E03-T02
Epic: E03 — Typed Go nodes and workflow authoring
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: A

## Problem and intended result

Implement Define/MustDefine, Builder, Ref, Lit and Call for whole-value wiring. Record definitions at build/preparation time and return explicit workflow output.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 5](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `flow, testdata/authoring`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Go compiler rejects wrong node input/output types.
- [ ] Builder never executes node functions.
- [ ] References resolve to structural records with source identity.
- [ ] Define returns diagnostics and MustDefine is an explicit startup convenience.
- [ ] Repeated definition generates equivalent programs.

## Validation and required evidence

- [ ] Compile positive/negative authoring fixtures.
- [ ] Assert fake effect counters remain zero while building.
- [ ] Whole-value quote workflow round-trip matches fixture.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E03-T01 — Implement typed node definitions and explicit registration

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E03-T03 — Generate typed field accessors and node input bindings

Issue: E03-T03 · Initial status: Backlog

Roadmap ID: E03-T03
Epic: E03 — Typed Go nodes and workflow authoring
Milestone: M1 — Complete native Go application
Priority: P1 | Size: M | Review: R

## Problem and intended result

Use Go package/type analysis to generate deterministic field refs and argument structs for literals/references. Do not execute package init or copy SDK sources.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 5](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/generate, examples/order, testdata/generate`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Generated bindings handle nested/optional/collection fields supported by schema.
- [ ] Wrong field types fail Go compilation.
- [ ] Regeneration is byte-stable and files marked generated.
- [ ] Imported packages with side-effect init are analyzed without executing.
- [ ] Unsupported types produce source diagnostics.

## Validation and required evidence

- [ ] Compile quote/order composed-field examples.
- [ ] Init-bomb fixture remains unexecuted.
- [ ] Drift/alias/build-tag/nested-pointer fixtures.
- [ ] Verify no generated runtime source copies.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E03-T02 — Implement typed whole-value workflow references and outputs
- [ ] E02-T02 — Implement supported schema validation and normalization

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E03-T04 — Implement scoped control-flow authoring primitives

Issue: E03-T04 · Initial status: Backlog

Roadmap ID: E03-T04
Epic: E03 — Typed Go nodes and workflow authoring
Milestone: M1 — Complete native Go application
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add typed If/Choose, Each, Parallel, Try/Finally, child workflow and portable comparison/template/default operations. Record only structure; define joins and unique IDs.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 4, 5](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `flow, testdata/control`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Arm/iteration refs cannot escape except through declared joins.
- [ ] Choose arms have compatible output schema.
- [ ] Each requires bounded concurrency and collects input order.
- [ ] Duplicate IDs across arms are rejected.
- [ ] No raw expression strings or arbitrary source execution exists.
- [ ] Cancellation/suspension/finally semantics are distinct.

## Validation and required evidence

- [ ] Negative scope and duplicate-ID fixtures.
- [ ] Golden nested control-flow programs.
- [ ] Compile compatible/incompatible joins.
- [ ] Verify builder effect counters stay zero.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E03-T03 — Generate typed field accessors and node input bindings

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E04 — Compiler and portable execution program

Validate programs once and lower structural references without hidden runtime evaluation.

#### E04-T01 — Compile schema-compatible references and trigger mappings

Issue: E04-T01 · Initial status: Backlog

Roadmap ID: E04-T01
Epic: E04 — Compiler and portable execution program
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: R

## Problem and intended result

Validate registered calls, output fields, reference order/scope, selected modules and binding mapping against normalized schemas. Emit indexed references and source-mapped diagnostics.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§4, 5](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/compile, testdata/compiler`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] All unresolved nodes/fields and future/sibling refs fail before serving.
- [ ] Static compatibility uses documented subset.
- [ ] Dynamic edges retain required runtime validation.
- [ ] Whole and field refs use one resolver.
- [ ] Source positions survive generation and lowering.

## Validation and required evidence

- [ ] Golden valid/invalid order graphs.
- [ ] Nested loop/arm reference corpus.
- [ ] Wrong trigger mapping and missing-module cases.
- [ ] Fuzz malformed graph inputs.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E03-T04 — Implement scoped control-flow authoring primitives

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E04-T02 — Lower versioned instructions with deterministic artifact checks

Issue: E04-T02 · Initial status: Backlog

Roadmap ID: E04-T02
Epic: E04 — Compiler and portable execution program
Milestone: M1 — Complete native Go application
Priority: P1 | Size: M | Review: R

## Problem and intended result

Produce immutable program instructions and indexes for calls, control flow, output and child targets. Validate resource budgets, recursion and cycle rules.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§4, 5](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/compile, internal/program`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Program digests are reproducible.
- [ ] Interpreter rejects unsupported program/checkpoint version.
- [ ] Cycle/recursion/maximum-depth policy is explicit and bounded.
- [ ] No whole-context copying or per-step compilation is introduced.
- [ ] Document/source provenance is preserved.

## Validation and required evidence

- [ ] Program snapshots and deterministic rebuilds.
- [ ] Self-recursive/cyclic/oversized fixtures reject at compile.
- [ ] Unknown opcode tests.
- [ ] Benchmark compile cost separately from execution.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E04-T01 — Compile schema-compatible references and trigger mappings
- [ ] E02-T03 — Implement immutable artifact identity and deployment manifests

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E04-T03 — Enforce value ownership across native and wire paths

Issue: E04-T03 · Initial status: Backlog

Roadmap ID: E04-T03
Epic: E04 — Compiler and portable execution program
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: R

## Problem and intended result

Define representation and copy/borrow boundaries for pointers, slices, maps and nested values. Optimize only after preserving logical immutability.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§4, 5](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/value, testdata/ownership`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] A node cannot mutate prior committed output via an alias.
- [ ] Parallel siblings receive isolated writable data.
- [ ] Encoded and native paths preserve scalar/null/default semantics.
- [ ] Blob references have size/access/lifetime rules.
- [ ] Copying/validation cost is included in benchmarks.

## Validation and required evidence

- [ ] Adversarial node mutates nested maps/slices and sibling outputs remain unchanged.
- [ ] Race tests across fan-out.
- [ ] Native/wire golden corpus parity.
- [ ] Allocation profile includes isolation work.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E04-T02 — Lower versioned instructions with deterministic artifact checks
- [ ] E02-T02 — Implement supported schema validation and normalization

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E05 — Bounded Go execution engine and test harness

Execute compiled workflows correctly with bounded resources and application-friendly testing.

#### E05-T01 — Execute native calls and explicit workflow outputs

Issue: E05-T01 · Initial status: Backlog

Roadmap ID: E05-T01
Epic: E05 — Bounded Go execution engine and test harness
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: A

## Problem and intended result

Implement memory-mode interpreter for compiled calls with input/output validation, immutable committed outputs, structured errors and panic containment.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§5, 6, 7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/engine, testdata/execution`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Successful calls commit one output.
- [ ] Failed calls publish none.
- [ ] Workflow result follows declared output.
- [ ] Invalid node outputs fail before publication.
- [ ] Context cancellation and panic become distinct structured outcomes.
- [ ] Interpreter imports no concrete trigger/store/provider.

## Validation and required evidence

- [ ] Real quote/order functions through interpreter.
- [ ] Inject invalid output, error, panic and canceled context.
- [ ] Compare compiled and authored behavior.
- [ ] Run race suite.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E04-T03 — Enforce value ownership across native and wire paths

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E05-T02 — Execute branches, loops, parallel groups and error control

Issue: E05-T02 · Initial status: Backlog

Roadmap ID: E05-T02
Epic: E05 — Bounded Go execution engine and test harness
Milestone: M1 — Complete native Go application
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement control instructions with scoped invocation/iteration identity, typed joins, ordered collections and fail-fast cooperative sibling cancellation.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§5, 6, 7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/engine, testdata/control`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Branches execute exactly selected arm.
- [ ] Each/parallel enforce declared bounds and isolate outputs.
- [ ] Join result order is deterministic despite completion order.
- [ ] Try catches business errors without swallowing cancellation/suspension.
- [ ] Finally follows specified terminal semantics.
- [ ] Child depth is bounded.

## Validation and required evidence

- [ ] Nested branches/loops/parallel and partial failures.
- [ ] Controlled out-of-order completion.
- [ ] Finally on success/error/cancel/suspend.
- [ ] Race and goroutine leak checks.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E05-T01 — Execute native calls and explicit workflow outputs

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E05-T03 — Implement admission budgets, fairness and cancellation

Issue: E05-T03 · Initial status: Backlog

Roadmap ID: E05-T03
Epic: E05 — Bounded Go execution engine and test harness
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: R

## Problem and intended result

Add bounded active/runnable queues, per-binding/tenant budgets, retry/child limits and cooperative deadlines. Specify saturation and shutdown policies.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§5, 6, 7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/engine, contract/admission`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Overload rejects or durably defers according to configured boundary.
- [ ] A noisy tenant cannot starve other tenants.
- [ ] Cancellation stops new dispatch and rejects obsolete attempt results.
- [ ] Queue memory is bounded under slow nodes.
- [ ] No goroutine/process is spawned unboundedly per request.

## Validation and required evidence

- [ ] Saturation and fairness tests with blocking fakes.
- [ ] Deadline/cancel/late-result races.
- [ ] Repeated shutdown leak checks.
- [ ] Sustained overload memory profile.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E05-T02 — Execute branches, loops, parallel groups and error control

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E05-T04 — Provide typed in-process node and workflow testing

Issue: E05-T04 · Initial status: Backlog

Roadmap ID: E05-T04
Epic: E05 — Bounded Go execution engine and test harness
Milestone: M1 — Complete native Go application
Priority: P1 | Size: M | Review: A

## Problem and intended result

Add public testing helpers using production compiler/interpreter, identity/version mocks, validated mock outputs and deterministic clocks for later durable suites.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§5, 6, 7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `flowtest, nodetest, examples`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Pure node tests require no app/listener/store.
- [ ] Workflow tests use real engine semantics.
- [ ] Invalid mocks fail output validation.
- [ ] Tests inspect resolved inputs, attempts, outputs and execution flags.
- [ ] Harness has no divergent interpreter.

## Validation and required evidence

- [ ] Quote/order harness examples.
- [ ] Mock input/output failure fixtures.
- [ ] Fake clock exercises retry scheduling without sleeps.
- [ ] Validate harness results against real application run.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E05-T03 — Implement admission budgets, fairness and cancellation

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E06 — Application lifecycle and HTTP vertical slice

Deliver an application-owned Go executable with real HTTP and explicit module lifecycle.

#### E06-T01 — Implement application composition and graceful lifecycle

Issue: E06-T01 · Initial status: Backlog

Roadmap ID: E06-T01
Epic: E06 — Application lifecycle and HTTP vertical slice
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: A

## Problem and intended result

Add app composition for nodes/workflows/adapters/dependencies. Validate duplicates and missing requirements before admission; own startup, readiness and signal-driven bounded drain.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§1, 2, 6, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `app, contract/lifecycle`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Selected modules register explicitly and config cannot load code.
- [ ] Failed startup accepts no traffic and closes initialized dependencies.
- [ ] Shutdown stops admission, drains/cancels according to policy, flushes required state and closes workers.
- [ ] Duplicate routes/workflows fail diagnostically.
- [ ] Go-only app needs no foreign process.

## Validation and required evidence

- [ ] Partial-startup and shutdown order fixtures.
- [ ] Signal integration test on supported OS.
- [ ] Missing dependency/duplicate registration negative cases.
- [ ] Goroutine/resource leak checks.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E05-T04 — Provide typed in-process node and workflow testing

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E06-T02 — Serve typed HTTP bindings through shared admission

Issue: E06-T02 · Initial status: Backlog

Roadmap ID: E06-T02
Epic: E06 — Application lifecycle and HTTP vertical slice
Milestone: M1 — Complete native Go application
Priority: P0 | Size: M | Review: R

## Problem and intended result

Implement method/path/parameter/body mapping, payload limits, trusted principal injection, status/error mapping, cancellation and shared listener composition.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§1, 2, 6, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/http, contract/admission`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Domain input is schema-validated before dispatch.
- [ ] Client auth fields cannot impersonate principal.
- [ ] Memory result maps to HTTP JSON and internal failure hides private detail.
- [ ] Duplicate/ambiguous routes fail preflight.
- [ ] Slow/oversized requests are bounded.
- [ ] Disconnect policy is explicit.

## Validation and required evidence

- [ ] httptest and actual listener quote requests.
- [ ] Wrong types, malformed JSON, auth spoofing, routing conflicts and slow clients.
- [ ] Cancel/deadline/backpressure integration.
- [ ] Verify engine imports stay clean.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E06-T01 — Implement application composition and graceful lifecycle

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E06-T03 — Ship the complete native quote application and DX fixture

Issue: E06-T03 · Initial status: Backlog

Roadmap ID: E06-T03
Epic: E06 — Application lifecycle and HTTP vertical slice
Milestone: M1 — Complete native Go application
Priority: P1 | Size: M | Review: A

## Problem and intended result

Create one conventional Go module/example with pure pricing node, typed workflow, explicit HTTP composition and tests. Document only commands verified to work.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§1, 2, 6, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `examples/quote, README.md`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Fresh checkout builds/runs quote endpoint without Node/Python/containers.
- [ ] Coffee quantity 2 returns exactly 3000 minor units.
- [ ] Invalid SKU/quantity has useful error and run correlation.
- [ ] No handwritten IR required for authoring.
- [ ] Example is tested in CI and dependency footprint is recorded.

## Validation and required evidence

- [ ] Build example binary.
- [ ] Send successful/invalid HTTP requests.
- [ ] Run direct node and real workflow tests.
- [ ] Fresh-user task captures setup time and diagnostic observations.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E06-T02 — Serve typed HTTP bindings through shared admission

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E07 — Durable state, recovery and version retention

Establish precise single-host recovery before adding distributed ownership.

#### E07-T01 — Select one embedded durable backend through an executable spike

Issue: E07-T01 · Initial status: Backlog

Roadmap ID: E07-T01
Epic: E07 — Durable state, recovery and version retention
Milestone: M2 — Durable orders and jobs
Priority: P0 | Size: M | Review: R

## Problem and intended result

Compare SQLite and the justified alternative on atomic transitions, group commit, crash recovery, backup, inspection, toolchain/linking and volume packaging. Record one winner; do not implement two production backends initially.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `store, docs/decisions, testdata/store`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Raw benchmark and crash results include filesystem/durability settings.
- [ ] Transactions cannot acknowledge unflushed accepted work.
- [ ] Decision explains native/cgo/platform constraints.
- [ ] Port supports replaceability without importing implementation into engine.
- [ ] Chosen implementation has supported backup/restore strategy.

## Validation and required evidence

- [ ] Process-kill before/during/after commit.
- [ ] Reopen corrupted/truncated database.
- [ ] Batch and single-commit latency under concurrency.
- [ ] Reproducible packaging on target matrix.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E02-T04 — Publish diagnostics and conformance harness contracts
- [ ] E05-T03 — Implement admission budgets, fairness and cancellation

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E07-T02 — Implement atomic admission, checkpoints and uncertainty state

Issue: E07-T02 · Initial status: Backlog

Roadmap ID: E07-T02
Epic: E07 — Durable state, recovery and version retention
Milestone: M2 — Durable orders and jobs
Priority: P0 | Size: M | Review: R

## Problem and intended result

Implement selected store transitions for accepted runs, effect intent, attempts, committed results and uncertain outcomes. Scope stable operation identity by workflow/invocation/iteration/attempt as defined.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/journal, store/embedded`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Admission deduplication is atomic under concurrent duplicates.
- [ ] Acceptance/completion acknowledgments follow durable barriers.
- [ ] Committed results replay without effect redispatch.
- [ ] External success without commit becomes uncertain.
- [ ] Stale writers/results cannot overwrite current attempt.
- [ ] Integrity errors fail closed.

## Validation and required evidence

- [ ] Kill process at admission/intent/dispatch/result/ack barriers.
- [ ] Concurrent duplicate input and conflicting key tests.
- [ ] Disk-full/write-failure/corruption fixtures.
- [ ] Verify observable effect counts.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E07-T01 — Select one embedded durable backend through an executable spike
- [ ] E05-T01 — Execute native calls and explicit workflow outputs

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E07-T03 — Deliver durable jobs and an outbox-backed order application

Issue: E07-T03 · Initial status: Backlog

Roadmap ID: E07-T03
Epic: E07 — Durable state, recovery and version retention
Milestone: M2 — Durable orders and jobs
Priority: P0 | Size: M | Review: R

## Problem and intended result

Add the initial local durable worker trigger and order recipe with business unique request keys and transactional outbox. Specify acknowledgment ownership and retries.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/worker, examples/order`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Job delivery acknowledges only after defined durable transfer.
- [ ] Crash between acceptance and ack does not duplicate order.
- [ ] Conflicting request reuse fails.
- [ ] Transient retries are bounded and validation errors do not retry.
- [ ] Business order and outbox event commit atomically.
- [ ] Dead letters carry safe diagnostics.

## Validation and required evidence

- [ ] Actual order persistence and outbox dispatcher integration.
- [ ] Kill producer/engine/dispatcher at handoff boundaries.
- [ ] Duplicate deliveries and provider timeout.
- [ ] Verify exact rows and effect counts after recovery.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E07-T02 — Implement atomic admission, checkpoints and uncertainty state
- [ ] E06-T03 — Ship the complete native quote application and DX fixture

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E07-T04 — Persist waits, retry timers and authorized signals

Issue: E07-T04 · Initial status: Backlog

Roadmap ID: E07-T04
Epic: E07 — Durable state, recovery and version retention
Milestone: M3 — Recovery across control flow and versions
Priority: P0 | Size: M | Review: R

## Problem and intended result

Add durable suspension with deterministic test clock, signal identities, pre-wait arrival handling, authorization and wakeup races. Suspended runs release active workers.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/journal, internal/engine, contract/signal`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Retry/wait timestamps survive restart.
- [ ] Signal consumption and resume decision are atomic.
- [ ] Duplicate/late signals have stable outcomes.
- [ ] Unauthorized signals cannot affect run state.
- [ ] Thousands of suspended runs do not hold one goroutine each.
- [ ] Cancel-versus-signal ordering is explicit.

## Validation and required evidence

- [ ] Kill before/after timer firing and signal consumption.
- [ ] Signals arriving before wait and twice concurrently.
- [ ] Fake-clock daylight/time-jump tests.
- [ ] Measure idle footprint and wakeup bursts.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E07-T03 — Deliver durable jobs and an outbox-backed order application

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E07-T05 — Recover nested control flow, child runs and parallel joins

Issue: E07-T05 · Initial status: Backlog

Roadmap ID: E07-T05
Epic: E07 — Durable state, recovery and version retention
Milestone: M3 — Recovery across control flow and versions
Priority: P0 | Size: M | Review: R

## Problem and intended result

Persist branch decisions, loop invocation paths, child lineage, partial results and joins. Define cancellation propagation and prevent duplicate dispatch after resume.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/journal, testdata/recovery`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Nested scopes reconstruct the exact execution path.
- [ ] Completed child/iteration results reuse committed values.
- [ ] Parent/child/join transitions remain consistent after crashes.
- [ ] Cancellation does not claim an effect was undone.
- [ ] Changed artifact/checkpoint cannot silently resume.

## Validation and required evidence

- [ ] Crash matrix across nested Each/Choose/Try/Parallel/child joins.
- [ ] Partial success and lost worker fixtures.
- [ ] Corrupted lineage and obsolete attempt tests.
- [ ] Compare recovered and uninterrupted outputs/effect counts.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E07-T04 — Persist waits, retry timers and authorized signals
- [ ] E05-T02 — Execute branches, loops, parallel groups and error control

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E07-T06 — Implement reconciliation, artifact retention and safe upgrades

Issue: E07-T06 · Initial status: Backlog

Roadmap ID: E07-T06
Epic: E07 — Durable state, recovery and version retention
Milestone: M3 — Recovery across control flow and versions
Priority: P1 | Size: M | Review: R

## Problem and intended result

Expose authorized inspection/reconciliation for uncertain effects and matching-artifact startup checks. First upgrade strategy drains or retains the exact executable; explicit migration requires compatibility evidence.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/journal, app, cmd/blok`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Old native code is never substituted by matching version string.
- [ ] Missing artifact blocks affected recovery with a diagnostic.
- [ ] Reconciliation records actor/evidence and remains idempotent.
- [ ] New replay run carries lineage and fresh approvals.
- [ ] Upgrade/cancel policies cannot silently discard accepted work.

## Validation and required evidence

- [ ] Restart with same/version-conflicting/missing binary.
- [ ] Concurrent reconciliation attempts.
- [ ] Simulated provider result lookup.
- [ ] Upgrade while waiting/running and cancellation acceptance tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E07-T05 — Recover nested control flow, child runs and parallel joins
- [ ] E02-T03 — Implement immutable artifact identity and deployment manifests

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E07-T07 — Implement verified retention, compaction, backup and restore

Issue: E07-T07 · Initial status: Backlog

Roadmap ID: E07-T07
Epic: E07 — Durable state, recovery and version retention
Milestone: M3 — Recovery across control flow and versions
Priority: P1 | Size: M | Review: R

## Problem and intended result

Bound history growth with verified checkpoints, retention policy, audit separation and backup/restore without losing resumable artifacts or active run state.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `store/embedded, internal/journal, testdata/restore`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Compaction retains all state required for active runs.
- [ ] Backup has documented consistent cut and durability limits.
- [ ] Restore validates integrity and artifact availability.
- [ ] Required audit retention is independent of debug telemetry.
- [ ] Interrupted compaction leaves a recoverable store.

## Validation and required evidence

- [ ] Crash at snapshot/rename/delete boundaries.
- [ ] Restore old and fresh backups then resume nested runs.
- [ ] Corruption/disk-full fixtures.
- [ ] Long-run storage growth and retention tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E07-T06 — Implement reconciliation, artifact retention and safe upgrades

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E08 — Persistent gRPC runtime protocol and Node.js first

Connect foreign nodes without giving workers a second workflow engine or an unbounded process lifecycle.

#### E08-T01 — Specify and generate the negotiated worker protocol

Issue: E08-T01 · Initial status: Backlog

Roadmap ID: E08-T01
Epic: E08 — Persistent gRPC runtime protocol and Node.js first
Milestone: M4 — Node.js and persistent worker conformance
Priority: P0 | Size: M | Review: R

## Problem and intended result

Define runtime protocol versions, frame direction, local/remote connection topology, catalog/artifact digests, call/attempt/generation IDs, capabilities, errors, cancellation, blob limits and reusable channel strategy.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/runtime, internal/runtime, docs/decisions`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Negotiation rejects unsupported/incompatible contracts.
- [ ] Schema presence/null/int64 rules match E02.
- [ ] Limits and saturation are explicit.
- [ ] Code generation is pinned and drift checked.
- [ ] Connection loss uncertainty and safe retries are specified.
- [ ] Worker cannot invoke unrestricted orchestration.

## Validation and required evidence

- [ ] Proto/SDK drift checks.
- [ ] Native/wire golden conformance.
- [ ] Wrong version/catalog/digest/identity fixtures.
- [ ] Unary-versus-streaming spike with raw latency/load evidence.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E02-T04 — Publish diagnostics and conformance harness contracts
- [ ] E07-T02 — Implement atomic admission, checkpoints and uncertainty state

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E08-T02 — Implement supervised Go worker client and lifecycle

Issue: E08-T02 · Initial status: Backlog

Roadmap ID: E08-T02
Epic: E08 — Persistent gRPC runtime protocol and Node.js first
Milestone: M4 — Node.js and persistent worker conformance
Priority: P0 | Size: M | Review: R

## Problem and intended result

Start persistent local workers only when selected, reuse authenticated channels, verify catalogs and manage drain/restart/generations under bounded capacity.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/runtime, app`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] No per-step process spawn.
- [ ] Startup fails before traffic on missing runtime/catalog.
- [ ] Concurrent calls carry unique stable attempt identity.
- [ ] Deadline/cancel propagates and late results are discarded.
- [ ] Reconnect cannot silently retry uncertain effects.
- [ ] Worker crashes do not crash native engine.

## Validation and required evidence

- [ ] Actual subprocess gRPC integration.
- [ ] Kill/disconnect during read/effect and after remote success.
- [ ] Catalog-generation mismatch, capacity exhaustion and startup timeout.
- [ ] Leak/process-count/race tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E08-T01 — Specify and generate the negotiated worker protocol
- [ ] E06-T01 — Implement application composition and graceful lifecycle
- [ ] E07-T06 — Implement reconciliation, artifact retention and safe upgrades

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E08-T03 — Implement Node.js SDK and persistent worker

Issue: E08-T03 · Initial status: Backlog

Roadmap ID: E08-T03
Epic: E08 — Persistent gRPC runtime protocol and Node.js first
Milestone: M4 — Node.js and persistent worker conformance
Priority: P0 | Size: M | Review: R

## Problem and intended result

Implement typed JavaScript/TypeScript node descriptors and actual persistent gRPC server with dependency injection, provider adapters, AbortSignal, controlled async concurrency and native dependency packaging.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/nodejs, runtime/nodejs, testdata/worker`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Node.js discovery matches canonical descriptor/digest.
- [ ] Input/output normalized schema parity passes.
- [ ] AbortSignal receives deadline/cancellation.
- [ ] Worker validates limits and rejects duplicate/stale call identity.
- [ ] Provider failures retain error classification and idempotency key.
- [ ] Node imports another node are prohibited by ownership checker.

## Validation and required evidence

- [ ] Real Node.js server against Go client.
- [ ] Typed positive/negative node fixtures.
- [ ] Slow/panicking/rejected provider cases and cancellation.
- [ ] Restart/generation/schema integer boundary conformance.
- [ ] Verify npm lock and supported Node matrix.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E08-T02 — Implement supervised Go worker client and lifecycle
- [ ] E03-T01 — Implement typed node definitions and explicit registration

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E08-T04 — Harden worker transport and publish equivalent workload evidence

Issue: E08-T04 · Initial status: Backlog

Roadmap ID: E08-T04
Epic: E08 — Persistent gRPC runtime protocol and Node.js first
Milestone: M4 — Node.js and persistent worker conformance
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add mTLS or authenticated local transport, principal/capability scope, message/blob limits and equivalent Go/Node order workloads. Evaluate trusted code and sandbox controls separately.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/runtime, internal/runtime, benchmarks/worker`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Unauthorized or mismatched worker cannot execute calls.
- [ ] Credentials/capability scope never widen on reconnect.
- [ ] Large payloads use authorized bounded blob references.
- [ ] Native and worker outputs/errors/cancel/uncertainty agree.
- [ ] Startup/RSS/latency/throughput/raw samples are published separately.

## Validation and required evidence

- [ ] Certificate/token invalidation and spoofed principal tests.
- [ ] Oversized/truncated messages and slow consumers.
- [ ] Equivalent full workflow with real provider mock server.
- [ ] Repeated native/worker load and crash measurements.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E08-T03 — Implement Node.js SDK and persistent worker

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E09 — Complete modular trigger catalog

Implement every promised protocol against one admission and lifecycle contract.

#### E09-T01 — Publish shared trigger conformance and modularity checks

Issue: E09-T01 · Initial status: Backlog

Roadmap ID: E09-T01
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Extend the initial HTTP/worker contracts into reusable admission, mapping, principal, deduplication, capacity, cancellation, ack and shutdown tests for all adapters.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/conformance, trigger`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Every trigger declares memory/durable completion and disconnect behavior.
- [ ] No adapter owns an interpreter or retry engine.
- [ ] Missing/unselected adapter has no listener/goroutines/assets.
- [ ] Same domain workflow can have multiple compatible bindings.
- [ ] Extension author can run conformance without engine internals.

## Validation and required evidence

- [ ] Broken adapter negative suite.
- [ ] Compile/run binary with no trigger and selected trigger.
- [ ] Multi-binding schema/auth fixture.
- [ ] Actual HTTP/worker replay through harness.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E06-T02 — Serve typed HTTP bindings through shared admission
- [ ] E07-T03 — Deliver durable jobs and an outbox-backed order application
- [ ] E02-T04 — Publish diagnostics and conformance harness contracts

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E09-T02 — Implement signed webhook admission

Issue: E09-T02 · Initial status: Backlog

Roadmap ID: E09-T02
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Verify original request bytes before mapping, provider signature/key rotation, replay windows and stable event deduplication; connect to shared durable submission.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/webhook`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Modified body/signature fails.
- [ ] Old/replayed events obey configured policy.
- [ ] Secret key remains opaque and redacted.
- [ ] Concurrent duplicate events create one accepted run.
- [ ] Ack timing follows durable ownership.
- [ ] Provider-specific verification is pluggable.

## Validation and required evidence

- [ ] Actual signed requests with altered whitespace/body/key/time.
- [ ] Key-rotation overlap and replay fixtures.
- [ ] Crash between accepted run and HTTP acknowledgment.
- [ ] Malformed/oversized body tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E09-T01 — Publish shared trigger conformance and modularity checks

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E09-T03 — Implement cron schedules with durable occurrence identity

Issue: E09-T03 · Initial status: Backlog

Roadmap ID: E09-T03
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add timezone-aware cron scheduling, daylight-saving rules, overlap and missed-tick policy, persisted occurrence identity and graceful scheduler ownership.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/cron`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Spring/fall DST behavior is documented and deterministic.
- [ ] Duplicate ticks do not duplicate accepted work.
- [ ] Overlap skips/queues according to policy.
- [ ] Missed occurrences have bounded catch-up.
- [ ] Restart reconstructs next occurrence without sleeping worker per schedule.

## Validation and required evidence

- [ ] Fake-clock DST/timezone/leap and wall-clock shift corpus.
- [ ] Crash around occurrence commit.
- [ ] Overlap and long downtime catch-up.
- [ ] Many schedules memory/CPU profile.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E09-T01 — Publish shared trigger conformance and modularity checks
- [ ] E07-T04 — Persist waits, retry timers and authorized signals

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E09-T04 — Implement pubsub adapter with delivery transfer semantics

Issue: E09-T04 · Initial status: Backlog

Roadmap ID: E09-T04
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Ship one selected broker integration using scoped subscription/cursor, explicit ack transfer, provider flow control and shared execution retry ownership; document extension port.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/pubsub`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Cursor/ack durability behavior is explicit.
- [ ] Redelivery deduplicates stable IDs.
- [ ] Slow consumers remain bounded.
- [ ] Poison messages follow tested dead-letter policy.
- [ ] Principal/module configuration validates before consuming.
- [ ] Disconnect/rebalance does not lose accepted work.

## Validation and required evidence

- [ ] Actual broker integration container.
- [ ] Duplicate/poison/oversized messages.
- [ ] Crash at admission/ack/cursor boundary.
- [ ] Rebalance/connection loss and reconnect.
- [ ] Verify engine has no broker import.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E09-T01 — Publish shared trigger conformance and modularity checks
- [ ] E07-T03 — Deliver durable jobs and an outbox-backed order application

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E09-T05 — Implement application gRPC trigger bindings

Issue: E09-T05 · Initial status: Backlog

Roadmap ID: E09-T05
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Generate service/method mapping to domain inputs and outputs with deadlines, metadata-authenticated principal, bounded messages and status/error policy. Keep separate from worker transport.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/grpc`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Declared proto/schema mapping validates at startup.
- [ ] Client cancellation follows binding policy.
- [ ] Domain errors and internal errors map safely.
- [ ] Method authorization is server-side.
- [ ] Large messages and concurrency are bounded.
- [ ] Shared/independent TLS listener options are tested.

## Validation and required evidence

- [ ] Real client/server generated proto calls.
- [ ] Missing/wrong fields, integer bounds and status mapping.
- [ ] Deadline/auth/capacity/shutdown tests.
- [ ] Verify worker and application services do not collide.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E09-T01 — Publish shared trigger conformance and modularity checks
- [ ] E08-T01 — Specify and generate the negotiated worker protocol

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E09-T06 — Implement SSE streams with replay cursors and slow-client policy

Issue: E09-T06 · Initial status: Backlog

Roadmap ID: E09-T06
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add authorized subscriptions, event framing, heartbeat, bounded buffers, disconnect policy and cursor retention/reconnect contract. Progress is not a durable signal.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/sse`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] SSE uses HTTP event framing correctly.
- [ ] Cursor replay has explicit retention/gap outcome.
- [ ] Slow clients cannot grow memory unboundedly.
- [ ] Unauthorized subscriber sees no payload.
- [ ] Disconnect does not silently cancel durable work.
- [ ] Event drop and audit policies stay distinct.

## Validation and required evidence

- [ ] Actual HTTP streaming with reconnect/Last-Event-ID.
- [ ] Slow reader, cursor gap, heartbeat and disconnect.
- [ ] Authorization/redaction tests.
- [ ] Long stream leak and buffer-bound tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E09-T01 — Publish shared trigger conformance and modularity checks
- [ ] E07-T04 — Persist waits, retry timers and authorized signals

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E09-T07 — Implement WebSocket connection and message workflow bindings

Issue: E09-T07 · Initial status: Backlog

Roadmap ID: E09-T07
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add connect/message/disconnect bindings with principal, connection identity, origin checks, frame limits and bounded outbound/inbound flow; expose helper ports instead of business transport context.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/websocket`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Connection scopes cannot be spoofed by message fields.
- [ ] Oversized/malformed frames fail safely.
- [ ] Slow clients use declared disconnect/backpressure policy.
- [ ] Disconnect events are deduplicated.
- [ ] Message ordering and concurrent workflow policy are explicit.
- [ ] Shutdown closes and drains without leaks.

## Validation and required evidence

- [ ] Real socket handshake/origin/auth and ping/pong.
- [ ] Multi-client isolation and spoofed connection IDs.
- [ ] Slow-reader flood and disconnect races.
- [ ] Graceful shutdown and goroutine/RSS bounds.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E09-T01 — Publish shared trigger conformance and modularity checks

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E09-T08 — Implement MCP node and workflow exposure

Issue: E09-T08 · Initial status: Backlog

Roadmap ID: E09-T08
Epic: E09 — Complete modular trigger catalog
Milestone: M5 — All nine modular triggers
Priority: P1 | Size: M | Review: R

## Problem and intended result

Expose selected nodes as typed tools and workflows as composed tools/resources using supported MCP transports. Apply trusted principal, schema, capability/approval and cancellation rules through admission.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§6](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `trigger/mcp`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Only explicitly exposed compatible descriptors are visible.
- [ ] Workflow tools validate domain inputs/outputs.
- [ ] Tool/resource errors preserve protocol semantics without leaking secrets.
- [ ] Auth/capability checks run server-side.
- [ ] Unapproved effects cannot dispatch.
- [ ] Concurrent calls and cancellation are bounded.

## Validation and required evidence

- [ ] Actual MCP client/server negotiation and tool/resource calls.
- [ ] Unknown schema/version and unauthorized discovery/invocation.
- [ ] Cancel/deadline/overload.
- [ ] Effect requiring approval remains blocked until scoped decision.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E09-T01 — Publish shared trigger conformance and modularity checks
- [ ] E14-T02 — Enforce durable approvals, evidence and publication gates

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E10 — Generic nodes and complete application recipes

Make reusable generic nodes and custom business nodes obey the same contracts without turning the core into an ORM.

#### E10-T01 — Implement pure validation, mapping and formatting catalog nodes

Issue: E10-T01 · Initial status: Backlog

Roadmap ID: E10-T01
Epic: E10 — Generic nodes and complete application recipes
Milestone: M4 — Node.js and persistent worker conformance
Priority: P1 | Size: M | Review: A

## Problem and intended result

Ship small schema-described generic validation, selection/mapping, template/default and exact arithmetic helpers. Prefer typed native authoring; no arbitrary evaluation engine.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `catalog, testdata/catalog`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Generic and custom descriptors use same registry.
- [ ] Effects/determinism are truthful.
- [ ] Bounds cover collection size and template output.
- [ ] Validation failures preserve field diagnostics.
- [ ] Money/integer operations detect overflow.
- [ ] No node invokes another node.

## Validation and required evidence

- [ ] Golden schema/format/null/overflow fixtures.
- [ ] Fuzz bounded inputs.
- [ ] Generic workflow composition test.
- [ ] Ownership and manifest conformance.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E03-T04 — Implement scoped control-flow authoring primitives
- [ ] E05-T04 — Provide typed in-process node and workflow testing

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E10-T02 — Implement effect nodes through injected provider ports

Issue: E10-T02 · Initial status: Backlog

Roadmap ID: E10-T02
Epic: E10 — Generic nodes and complete application recipes
Milestone: M4 — Node.js and persistent worker conformance
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add HTTP request, email, database operation, payment, publish, audit and structured-generation contracts with one synthetic/reference adapter each. Keep provider SDKs outside engine.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `catalog, provider, testdata/providers`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Each effect names narrow capabilities/secret refs.
- [ ] Idempotency/deadline/cancel reaches providers.
- [ ] Transient/business/uncertain errors are distinguished.
- [ ] Provider implementation bounds requests and redacts credentials.
- [ ] Missing provider fails startup.
- [ ] Effect nodes never fake external success.

## Validation and required evidence

- [ ] Actual mock HTTP/email/payment endpoints.
- [ ] Timeout-before/after-success and repeated key cases.
- [ ] Database rollback/outbox fixtures.
- [ ] Invalid generated output/schema tests.
- [ ] No credential appears in trace/error.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E10-T01 — Implement pure validation, mapping and formatting catalog nodes
- [ ] E07-T03 — Deliver durable jobs and an outbox-backed order application
- [ ] E08-T03 — Implement Node.js SDK and persistent worker

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E10-T03 — Deliver authenticated CRUD, webhook, job and streaming recipes

Issue: E10-T03 · Initial status: Backlog

Roadmap ID: E10-T03
Epic: E10 — Generic nodes and complete application recipes
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: R

## Problem and intended result

Create runnable application recipes using ordinary Go dependencies and selected adapters, migrations and synthetic data. Include module/source/test references for AI discovery.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `examples/recipes`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Recipes declare required storage/modules/secrets and migrations.
- [ ] Authorization is verified server-side with two principals.
- [ ] Fresh setup commands work without hidden defaults.
- [ ] Durable jobs/outbox and signed webhooks are real.
- [ ] Streaming uses bounded client policies.
- [ ] No engine-specific ORM/auth provider requirement.

## Validation and required evidence

- [ ] End-to-end synthetic CRUD and access-denied cases.
- [ ] Restart job and duplicate webhook delivery.
- [ ] Streaming slow client.
- [ ] Clean install/upgrade/teardown and migration replay.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E10-T02 — Implement effect nodes through injected provider ports
- [ ] E09-T02 — Implement signed webhook admission
- [ ] E09-T06 — Implement SSE streams with replay cursors and slow-client policy

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E11 — Go CLI and everyday developer experience

Provide fast feedback and ordinary Go ownership without hidden registration or mandatory hosted services.

#### E11-T01 — Implement new and generate for conventional Go applications

Issue: E11-T01 · Initial status: Backlog

Roadmap ID: E11-T01
Epic: E11 — Go CLI and everyday developer experience
Milestone: M1 — Complete native Go application
Priority: P1 | Size: M | Review: A

## Problem and intended result

Create initial Go starter and selected trigger configuration with deterministic type-binding generation. Support noninteractive flags and clear interactive layout/runtime choices.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `cmd/blok, internal/tooling, examples`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] New app owns its module and executable.
- [ ] Native starter requires only Go.
- [ ] No account/registry/container is required by default.
- [ ] Generated bindings and config are deterministic.
- [ ] Existing paths are never overwritten silently.
- [ ] Selection imports only requested modules.

## Validation and required evidence

- [ ] Fresh temp-directory scaffold/build/test/HTTP quote smoke.
- [ ] Interactive cancel and noninteractive equivalents.
- [ ] Init-side-effect/type-error generator fixtures.
- [ ] Regenerate twice and diff.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E06-T03 — Ship the complete native quote application and DX fixture
- [ ] E03-T03 — Generate typed field accessors and node input bindings

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E11-T02 — Implement check, test and machine-readable inspection

Issue: E11-T02 · Initial status: Backlog

Roadmap ID: E11-T02
Epic: E11 — Go CLI and everyday developer experience
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: A

## Problem and intended result

Wrap real compiler, test harness and catalog inspection with stable JSON diagnostics, source locations, module/trigger/workflow descriptions and meaningful exit codes.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `cmd/blok, internal/tooling`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] CLI and API diagnostics agree.
- [ ] Inspect applies access/redaction policy.
- [ ] Invalid graph/type/module exits nonzero.
- [ ] Human fixes and machine fields are stable.
- [ ] No fake execution replaces go test.
- [ ] Cancellation flushes diagnostics and returns correct exit.

## Validation and required evidence

- [ ] Golden stdout/stderr/exit cases.
- [ ] Broken application repair using one diagnostic.
- [ ] Catalog metadata contains source/examples/test refs.
- [ ] Writer failure and interrupt tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E11-T01 — Implement new and generate for conventional Go applications
- [ ] E02-T04 — Publish diagnostics and conformance harness contracts

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E11-T03 — Implement dev build/watch and supervised worker reload

Issue: E11-T03 · Initial status: Backlog

Roadmap ID: E11-T03
Epic: E11 — Go CLI and everyday developer experience
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: R

## Problem and intended result

Watch source, regenerate/build and gracefully restart application. Drain persistent worker generations and retain compatible artifacts for active durable runs.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/tooling, cmd/blok`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Native Go edits rebuild/restart rather than promised in-process reload.
- [ ] Failed builds show diagnostics while last healthy app follows documented policy.
- [ ] No watch/restart fork storm.
- [ ] Durable artifact mismatch cannot relabel code.
- [ ] Worker startup/shutdown is bounded.
- [ ] Ctrl-C cleans child processes.

## Validation and required evidence

- [ ] Edit good/bad source and inspect actual endpoint.
- [ ] Rapid changes and deleted files.
- [ ] Crash/interrupt process tree cleanup.
- [ ] Durable wait while code changes.
- [ ] Generation mismatch/slow shutdown.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E11-T02 — Implement check, test and machine-readable inspection
- [ ] E08-T04 — Harden worker transport and publish equivalent workload evidence
- [ ] E07-T06 — Implement reconciliation, artifact retention and safe upgrades

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E11-T04 — Implement doctor and operationally honest CLI output

Issue: E11-T04 · Initial status: Backlog

Roadmap ID: E11-T04
Epic: E11 — Go CLI and everyday developer experience
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: A

## Problem and intended result

Validate configuration, dependency versions, bindings, stores, worker artifacts, listener availability and deployment readiness without executing business effects.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `cmd/blok, internal/tooling`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Missing runtime/provider/artifact has actionable diagnostic.
- [ ] Doctor distinguishes warning/unsupported/failure.
- [ ] Secret values are never printed.
- [ ] Checks do not mutate business data.
- [ ] JSON schema/exit codes are versioned.
- [ ] Offline mode clearly explains unavailable checks.

## Validation and required evidence

- [ ] Missing tools/bad config/occupied ports and inaccessible store.
- [ ] Supported/unsupported runtime matrix.
- [ ] Credentials in env cannot leak.
- [ ] Verify no effect calls during doctor.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E11-T03 — Implement dev build/watch and supervised worker reload
- [ ] E17-T01 — Implement deployment validation and production runtime endpoints

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review A means automated checks plus normal maintainer review; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E12 — Unified and classic layouts with ownership enforcement

Offer layout choice without changing identities, contracts or node independence.

#### E12-T01 — Implement manifest-based discovery for both layouts

Issue: E12-T01 · Initial status: Backlog

Roadmap ID: E12-T01
Epic: E12 — Unified and classic layouts with ownership enforcement
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: R

## Problem and intended result

Support nodes/<runtime>/<node> and runtimes/<runtime>/nodes/<node>, explicit workflow paths and node-local multi-file ownership. Identity comes from descriptors rather than directories.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/tooling/layout`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Both layouts emit same canonical catalog/program.
- [ ] Duplicate identities/versions and path collisions fail.
- [ ] Discovery stays inside project root and handles symlinks safely.
- [ ] No source execution during discovery.
- [ ] Same-node file imports and shared domain utilities are allowed.

## Validation and required evidence

- [ ] Equivalent classic/unified fixtures.
- [ ] Duplicate identity, aliases, nested files, symlink escape and mixed-runtime packages.
- [ ] Generated catalog digests match.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E11-T01 — Implement new and generate for conventional Go applications
- [ ] E01-T03 — Enforce modular imports and synthetic fixture governance

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E12-T02 — Enforce node independence across all language import graphs

Issue: E12-T02 · Initial status: Backlog

Roadmap ID: E12-T02
Epic: E12 — Unified and classic layouts with ownership enforcement
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: R

## Problem and intended result

Extend ownership analysis for supported language import forms and package managers. Require each runtime adapter to supply checked graph resolution, not regex-only guarantees.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/tooling/ownership, testdata/imports`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Relative/aliased/package/generated imports of another node fail.
- [ ] Same-node helper/domain imports remain legal.
- [ ] Cycles and unresolved graph regions are reported.
- [ ] Unsupported dynamic imports never receive verified status.
- [ ] Workflows may compose multiple nodes normally.

## Validation and required evidence

- [ ] Negative/positive import fixtures per current runtime.
- [ ] Transitive and alias evasions.
- [ ] Symlink and case-sensitive paths.
- [ ] Check native and Node starters.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E12-T01 — Implement manifest-based discovery for both layouts
- [ ] E08-T03 — Implement Node.js SDK and persistent worker

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E12-T03 — Implement transactional layout migration

Issue: E12-T03 · Initial status: Backlog

Roadmap ID: E12-T03
Epic: E12 — Unified and classic layouts with ownership enforcement
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: R

## Problem and intended result

Plan dry-run migrations, stage moves/import rewrites, validate catalog/graph equality and atomically commit or rollback. Include generated bindings and native lockfiles.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§3](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/tooling/layout, cmd/blok`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Identity/schema/workflow output unchanged.
- [ ] Existing user changes/files are preserved.
- [ ] Collisions/unsupported imports block before mutation.
- [ ] Interrupt/error restores original tree.
- [ ] Running migrations twice is idempotent.
- [ ] Dry-run prints complete reviewable plan.

## Validation and required evidence

- [ ] Classic→unified→classic golden tree.
- [ ] Fail during every write/move boundary.
- [ ] Collision, permissions, symlinks, spaces and case-only rename.
- [ ] Build/test and catalog equality after migration.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E12-T02 — Enforce node independence across all language import graphs
- [ ] E11-T02 — Implement check, test and machine-readable inspection

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E13 — Node and workflow package client and registry contracts

Install reusable packages safely and reproducibly while keeping the hosted registry a separate product.

#### E13-T01 — Specify package identity, compatibility and trust protocol

Issue: E13-T01 · Initial status: Backlog

Roadmap ID: E13-T01
Epic: E13 — Node and workflow package client and registry contracts
Milestone: M6 — Developer tooling and package ecosystem
Priority: P0 | Size: M | Review: R

## Problem and intended result

Define namespaced immutable node/workflow packages, manifests, artifacts, dependency constraints, provenance, signatures, licensing, registry protocol and private mirror interfaces.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/package, docs/decisions`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Package versions cannot be overwritten with different content.
- [ ] Dependencies include runtime/schema/engine compatibility.
- [ ] Publishing service API and ownership policy are documented for separate repo.
- [ ] No hosted service is required for local package use.
- [ ] Trust policy distinguishes unsigned/local/trusted packages.

## Validation and required evidence

- [ ] Valid/invalid package golden fixtures.
- [ ] Version conflict, missing license/provenance and bad signature.
- [ ] Protocol fixture consumed by mock registry service.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E02-T03 — Implement immutable artifact identity and deployment manifests
- [ ] E08-T01 — Specify and generate the negotiated worker protocol

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E13-T02 — Implement deterministic resolution, locks and offline cache

Issue: E13-T02 · Initial status: Backlog

Roadmap ID: E13-T02
Epic: E13 — Node and workflow package client and registry contracts
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: R

## Problem and intended result

Resolve compatible node/workflow package graphs with exact artifact hashes and native dependency integration. Keep Go/npm/other managers authoritative for their dependencies.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/package, contract/package`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Lock captures all execution-relevant resolved artifacts.
- [ ] Cycles/conflicts/unsupported runtime ranges fail clearly.
- [ ] Offline install works from verified cache.
- [ ] Cache entries verify digest on read.
- [ ] Concurrent resolution does not corrupt lock/cache.
- [ ] No universal runtime installer is introduced.

## Validation and required evidence

- [ ] Conflicting/transitive/cyclic dependency fixtures.
- [ ] Corrupted cache and offline missing entry.
- [ ] Concurrent cache writes.
- [ ] Repeated resolutions yield identical locks.
- [ ] Go/npm native lock integration.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E13-T01 — Specify package identity, compatibility and trust protocol

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E13-T03 — Implement atomic add, remove, update and verify commands

Issue: E13-T03 · Initial status: Backlog

Roadmap ID: E13-T03
Epic: E13 — Node and workflow package client and registry contracts
Milestone: M6 — Developer tooling and package ecosystem
Priority: P0 | Size: M | Review: R

## Problem and intended result

Stage package changes, safely extract verified artifacts, update registration/locks and rebuild/check before applying. Support reviewable dry-run and rollback.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `cmd/blok, internal/package`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Traversal/symlink/zip-bomb extraction is blocked.
- [ ] Digest/signature checked before installation.
- [ ] Unapproved install hooks never execute.
- [ ] Failed update leaves previous working app.
- [ ] Remove detects dependent workflows and does not erase user code.
- [ ] Both node layouts work.

## Validation and required evidence

- [ ] Actual mock registry download and local packages.
- [ ] Interrupted writes, malicious archives, conflicting identity and bad hash.
- [ ] Add/update/remove idempotency.
- [ ] Fresh app build/tests before/after rollback.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E13-T02 — Implement deterministic resolution, locks and offline cache
- [ ] E12-T03 — Implement transactional layout migration

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E13-T04 — Publish offline examples and hosted registry handoff

Issue: E13-T04 · Initial status: Backlog

Roadmap ID: E13-T04
Epic: E13 — Node and workflow package client and registry contracts
Milestone: M6 — Developer tooling and package ecosystem
Priority: P1 | Size: M | Review: R

## Problem and intended result

Ship example reusable node and workflow packages, inspect/license/trust outputs and registry contract suite. Define publishing/search/moderation/account operations for separate registry repo.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `examples/packages, contract/conformance, docs`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Example installation is end-to-end without public service.
- [ ] Packaged workflow dependencies resolve reproducibly.
- [ ] Owner/namespace/signature policy is documented.
- [ ] Hosted registry failures have clear cached/offline behavior.
- [ ] No billing/search service implementation enters framework repo.

## Validation and required evidence

- [ ] Local/mirror/mock-hosted install conformance.
- [ ] Workflow package round-trip execution.
- [ ] Cache fallback and revoked artifact fixture.
- [ ] Consumer contract test proves registry implementation can integrate.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E13-T03 — Implement atomic add, remove, update and verify commands
- [ ] E10-T03 — Deliver authenticated CRUD, webhook, job and streaming recipes

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E14 — Policy-enforced AI tools and composed workflow tools

Let agents assemble complete applications while execution enforces authorization, evidence and budgets.

#### E14-T01 — Implement filtered node/workflow tool catalogs and invocation

Issue: E14-T01 · Initial status: Backlog

Roadmap ID: E14-T01
Epic: E14 — Policy-enforced AI tools and composed workflow tools
Milestone: M4 — Node.js and persistent worker conformance
Priority: P0 | Size: M | Review: R

## Problem and intended result

Expose schema-described nodes and composed workflows through versioned tool interfaces with source/example/test metadata, identity/version and transitive capability scope.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `agent, contract/tool`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Missing/invalid/legacy manifest is not agent-safe.
- [ ] Workflow tools aggregate child effects and cannot widen authority.
- [ ] Catalog reflects authorized caller scope and omits secrets.
- [ ] Native/remote tools use same admission path.
- [ ] Depth/time/token/resource budgets are explicit.

## Validation and required evidence

- [ ] Catalog read/write role fixtures.
- [ ] Hidden/invalid metadata denial.
- [ ] Workflow tool chaining and child privilege widening attempts.
- [ ] Actual native/Node tool invocation and normalized result schema.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E10-T02 — Implement effect nodes through injected provider ports
- [ ] E08-T04 — Harden worker transport and publish equivalent workload evidence
- [ ] E02-T04 — Publish diagnostics and conformance harness contracts

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E14-T02 — Enforce durable approvals, evidence and publication gates

Issue: E14-T02 · Initial status: Backlog

Roadmap ID: E14-T02
Epic: E14 — Policy-enforced AI tools and composed workflow tools
Milestone: M4 — Node.js and persistent worker conformance
Priority: P0 | Size: M | Review: R

## Problem and intended result

Bind authorized approval to action/input/workflow/artifact digest, effects, reviewer, expiry and execution scope. Persist decisions and validate assertions/evidence/trusted provenance before publishing.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `agent/policy, internal/journal, contract/approval`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Stale/missing/rejected/expired approval fails closed before effect dispatch.
- [ ] Changed input/artifact cannot reuse approval.
- [ ] Child calls inherit narrowed scope.
- [ ] Model content cannot establish authority/trusted provenance.
- [ ] Gate failure publishes no trusted result.
- [ ] Decisions survive restart with audit.

## Validation and required evidence

- [ ] Adversarial approval digest/scope/reviewer/expiry fixtures.
- [ ] Crash before/after approval commit and dispatch.
- [ ] Changed proposal retry.
- [ ] Forged evidence/model instruction and child widening.
- [ ] Inspect traces for denied effect counts.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E14-T01 — Implement filtered node/workflow tool catalogs and invocation
- [ ] E07-T06 — Implement reconciliation, artifact retention and safe upgrades

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E14-T03 — Expose read-only development MCP and bounded authoring tools

Issue: E14-T03 · Initial status: Backlog

Roadmap ID: E14-T03
Epic: E14 — Policy-enforced AI tools and composed workflow tools
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P1 | Size: M | Review: R

## Problem and intended result

Provide inspect/catalog/check/test projections for agents with project-root confinement and dev authorization. Separate read-only inspection from edits/invocation/migrations/deployments.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `agent/dev, contract/tool`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Read-only server cannot mutate files or execute arbitrary commands.
- [ ] Paths and symlinks are confined.
- [ ] Diagnostics are stable and contain repair surface.
- [ ] Optional write tools require host capability/approval.
- [ ] Every repair proposal runs real compiler/tests within budget.
- [ ] Dev API is disabled/protected in production.

## Validation and required evidence

- [ ] Actual tool client integration.
- [ ] Traversal/symlink and command injection attempts.
- [ ] Invalid-first proposal repaired within budget.
- [ ] Infinite repair and missing metadata denial.
- [ ] Production-mode access rejection.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E14-T02 — Enforce durable approvals, evidence and publication gates
- [ ] E11-T02 — Implement check, test and machine-readable inspection
- [ ] E15-T01 — Implement versioned run and step inspection projections

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E14-T04 — Generate and evaluate complete generic/custom application proposals

Issue: E14-T04 · Initial status: Backlog

Roadmap ID: E14-T04
Epic: E14 — Policy-enforced AI tools and composed workflow tools
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P1 | Size: M | Review: R

## Problem and intended result

Create bounded application drafts from authorized catalog and recipes, generating custom nodes only for gaps. Validate type/schema/build/tests and measure live-provider outcomes against deterministic replay baseline.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `agent/authoring, benchmarks/ai, examples`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Generic/custom nodes share manifest and review pipeline.
- [ ] Draft includes composition, dependencies, selected triggers, tests and migration requirements.
- [ ] Unsupported goals fail clearly rather than fake completion.
- [ ] Model repair attempts/cost/time are recorded.
- [ ] All side effects remain gated.
- [ ] Live and deterministic results are reported separately.

## Validation and required evidence

- [ ] Synthetic CRUD/job/webhook/MCP app task corpus.
- [ ] Real Go build/test/HTTP behavior.
- [ ] Custom-node gap, malformed output, prompt injection and repair budget exhaustion.
- [ ] Two configurable model providers with pinned settings and raw redacted records.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E14-T03 — Expose read-only development MCP and bounded authoring tools
- [ ] E10-T03 — Deliver authenticated CRUD, webhook, job and streaming recipes
- [ ] E13-T04 — Publish offline examples and hosted registry handoff

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E15 — Notebook inspection APIs and Studio handoff

Expose every dev execution step through a secure versioned API consumed by a separate simple Studio.

#### E15-T01 — Implement versioned run and step inspection projections

Issue: E15-T01 · Initial status: Backlog

Roadmap ID: E15-T01
Epic: E15 — Notebook inspection APIs and Studio handoff
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P1 | Size: M | Review: R

## Problem and intended result

Expose run/step inputs, processing events, attempts, output/error, logs and timing with lineage, pagination, authorized redaction and payload limits. Keep runtime state private.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§2, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `inspect, contract/inspection`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Input→processing→output states are real engine events.
- [ ] Failed/canceled/suspended/uncertain outcomes are distinct.
- [ ] Inspection cannot mutate committed values.
- [ ] Authorized field projection and blob access apply.
- [ ] Large histories paginate.
- [ ] API rejects unsupported versions.

## Validation and required evidence

- [ ] Actual Go and Node run inspection.
- [ ] Invalid/failed/waiting/canceled/uncertain cases.
- [ ] Two-principal data isolation.
- [ ] Pagination/truncation/redaction golden responses.
- [ ] No secret values in projection.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E05-T04 — Provide typed in-process node and workflow testing
- [ ] E07-T06 — Implement reconciliation, artifact retention and safe upgrades
- [ ] E08-T04 — Harden worker transport and publish equivalent workload evidence

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E15-T02 — Implement bounded live dev event streaming

Issue: E15-T02 · Initial status: Backlog

Roadmap ID: E15-T02
Epic: E15 — Notebook inspection APIs and Studio handoff
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add resumable authorized stream of step transitions/logs with cursor/gap policy, opt-in input/output capture and slow-client bounds. Separate telemetry from reliable state/audit.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§2, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `inspect, observe/event`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Slow Studio cannot block required journal transitions.
- [ ] Optional event loss is visible through gap markers.
- [ ] Input/output capture is configurable and access checked.
- [ ] Subscriber lifetime is bounded.
- [ ] Production defaults disable sensitive dev event capture.
- [ ] No UI dependency enters engine.

## Validation and required evidence

- [ ] Live actual HTTP/worker runs.
- [ ] Disconnect/reconnect/cursor gap.
- [ ] Slow/unauthorized subscribers and buffer saturation.
- [ ] Crash recovery event reconstruction.
- [ ] Leak and latency effect measurements.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E15-T01 — Implement versioned run and step inspection projections
- [ ] E09-T06 — Implement SSE streams with replay cursors and slow-client policy

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E15-T03 — Publish Studio consumer contracts and notebook UX specification

Issue: E15-T03 · Initial status: Backlog

Roadmap ID: E15-T03
Epic: E15 — Notebook inspection APIs and Studio handoff
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P1 | Size: M | Review: R

## Problem and intended result

Specify separate Studio integration: restrained Vercel/shadcn-like UI, notebook step detail, graph navigation, keyboard accessibility, real states and API auth. Ship consumer fixtures without a Studio frontend here.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§2, 9](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `docs/studio-contract.md, contract/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Notebook shows input/process/output with attempts/logs/error/timing.
- [ ] Dev and production permissions differ.
- [ ] Consumer handles large runs, gaps, redaction and unknown versions.
- [ ] Read-only first and partial replay/source editing are explicitly gated.
- [ ] Frontend-design skill and browser accessibility/performance validation are specified for Studio repo.

## Validation and required evidence

- [ ] Consumer contract fixture parses actual API/events.
- [ ] Golden notebook state scenarios for failure/wait/cancel/uncertain.
- [ ] Verify no frontend bundle/dependency appears in framework.
- [ ] Separate-repo handoff lists endpoint/auth/version requirements.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E15-T02 — Implement bounded live dev event streaming
- [ ] E14-T02 — Enforce durable approvals, evidence and publication gates

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E16 — Optional production observability and reliable audit

Make production behavior measurable without unbounded overhead or debug event guarantees masquerading as audit.

#### E16-T01 — Implement optional metrics, traces and structured logs

Issue: E16-T01 · Initial status: Backlog

Roadmap ID: E16-T01
Epic: E16 — Optional production observability and reliable audit
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P1 | Size: M | Review: R

## Problem and intended result

Add narrow engine observation ports and independently selected OpenTelemetry-compatible exporters. Propagate run/step/attempt/tenant correlation with bounded cardinality and explicit sampling.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§9, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `observe, contract/observe`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Unselected exporters add no network/listener/provider dependency.
- [ ] Trace context crosses Go/worker/child boundaries.
- [ ] Labels exclude unbounded user IDs/payloads.
- [ ] Sampling/drop/backpressure policy is explicit.
- [ ] Instrumentation cannot replace validation or approval gates.
- [ ] Exporter failure follows configured optional policy.

## Validation and required evidence

- [ ] No-exporter binary footprint.
- [ ] Real exporter mock collector integration.
- [ ] Cardinality/queue saturation and collector outage.
- [ ] Trace lineage across Node and child workflow.
- [ ] Latency/RSS measurements with each mode.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E15-T01 — Implement versioned run and step inspection projections
- [ ] E08-T04 — Harden worker transport and publish equivalent workload evidence

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E16-T02 — Enforce sensitive-data access, redaction and reliable audit

Issue: E16-T02 · Initial status: Backlog

Roadmap ID: E16-T02
Epic: E16 — Optional production observability and reliable audit
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P0 | Size: M | Review: R

## Problem and intended result

Define input/output/error/log redaction and audit retention/access with separate reliability policy. Record approval/reconciliation/deployment decisions without model-visible secrets.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§9, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `observe/redact, contract/audit`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Secrets are opaque by default in catalogs/telemetry.
- [ ] Authorized inspection applies field policy.
- [ ] Audit failure blocks operations that require durable audit.
- [ ] Optional log drop cannot remove mandatory audit.
- [ ] Retention deletion respects active runs/legal application policy.
- [ ] Document native code leakage limits honestly.

## Validation and required evidence

- [ ] Secret-like fixtures in inputs/outputs/errors/logs.
- [ ] Encoded content risk and explicit redaction boundaries.
- [ ] Two-principal/tenant inspection denial.
- [ ] Audit store unavailable before approval/reconcile.
- [ ] Restore audit with durable state.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E16-T01 — Implement optional metrics, traces and structured logs
- [ ] E14-T02 — Enforce durable approvals, evidence and publication gates
- [ ] E07-T07 — Implement verified retention, compaction, backup and restore

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E16-T03 — Publish operational SLO metrics and monitoring integrations

Issue: E16-T03 · Initial status: Backlog

Roadmap ID: E16-T03
Epic: E16 — Optional production observability and reliable audit
Milestone: M7 — AI tools, Studio APIs and observability
Priority: P1 | Size: M | Review: R

## Problem and intended result

Define readiness/health, admission saturation, queue depth, latency, error, uncertainty, timer lag, worker availability, storage growth and exporter loss metrics; provide vendor-neutral monitoring examples.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§9, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `observe, examples/monitoring, docs`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Metrics distinguish admission/completion/steps/external calls.
- [ ] Alerts distinguish stalled from suspended/uncertain.
- [ ] Monitoring stack is optional and separate.
- [ ] Tenant cardinality and retention budgets are documented.
- [ ] Cloud consumer contract contains no proprietary runtime dependency.

## Validation and required evidence

- [ ] Synthetic slow-store/worker-death/timer-lag/overload scenarios produce expected signals.
- [ ] Collector outage/drop metrics.
- [ ] Validate monitoring queries/rules on recorded fixtures.
- [ ] Measure overhead with modes off/on.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E16-T02 — Enforce sensitive-data access, redaction and reliable audit
- [ ] E17-T01 — Implement deployment validation and production runtime endpoints

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E17 — Self-hosting, artifacts and Cloud integration contracts

Make applications easy to deploy through reproducible binaries and operational contracts without embedding a hosting product.

#### E17-T01 — Implement deployment validation and production runtime endpoints

Issue: E17-T01 · Initial status: Backlog

Roadmap ID: E17-T01
Epic: E17 — Self-hosting, artifacts and Cloud integration contracts
Milestone: M4 — Node.js and persistent worker conformance
Priority: P1 | Size: M | Review: R

## Problem and intended result

Define app manifest/config env handling, readiness/health/metrics, durable volume ownership, worker networking, resource bounds and graceful signal drain.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `app, contract/deployment, examples/deploy`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Listener addresses are configurable and external bindings explicit.
- [ ] Readiness requires compatible artifacts/store/workers.
- [ ] Required secret refs validate without printing values.
- [ ] Admission/drain budgets enforce overload/shutdown behavior.
- [ ] Go-only deployment is one executable.
- [ ] Unselected service has no operational requirement.

## Validation and required evidence

- [ ] Actual native and Go/Node container deployment.
- [ ] Missing/incompatible store/worker and secrets.
- [ ] SIGTERM drain/restart.
- [ ] Readiness/health/metrics under overload.
- [ ] Network bind and volume restart tests.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E06-T01 — Implement application composition and graceful lifecycle
- [ ] E07-T07 — Implement verified retention, compaction, backup and restore
- [ ] E08-T04 — Harden worker transport and publish equivalent workload evidence

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E17-T02 — Build reproducible binaries, SDK packages and signed artifacts

Issue: E17-T02 · Initial status: Backlog

Roadmap ID: E17-T02
Epic: E17 — Self-hosting, artifacts and Cloud integration contracts
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Pin build/generation tools and dependency locks; produce target-matrix artifacts, checksums, SBOM/licenses/provenance and verification policy. Minimize app images and CLI bundles.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `.github/workflows, cmd/blok, sdk, docs/release`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Release build repeats with identical execution artifacts under declared reproducibility scope.
- [ ] SDK packages include required protocol assets.
- [ ] Signing credentials remain in scoped release job.
- [ ] Install verifies digest/provenance.
- [ ] Native application has no foreign runtime baggage.
- [ ] Unsupported target fails explicitly.

## Validation and required evidence

- [ ] Build twice and compare manifests/digests.
- [ ] Actual archive/package import/CLI smoke.
- [ ] Tampered/missing protocol assets and invalid signature tests.
- [ ] Cross-platform build and native smoke on supported runners.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E17-T01 — Implement deployment validation and production runtime endpoints
- [ ] E13-T03 — Implement atomic add, remove, update and verify commands
- [ ] E01-T04 — Create release quality gates and supported toolchain matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E17-T03 — Implement multi-version deployment and artifact retention routing

Issue: E17-T03 · Initial status: Backlog

Roadmap ID: E17-T03
Epic: E17 — Self-hosting, artifacts and Cloud integration contracts
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Route new runs to new artifacts while retaining old binary/worker generations for accepted work. Define drain, rollback, compatible checkpoint migration and artifact garbage collection.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `app/deploy, contract/deployment`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Old runs never execute silently substituted native code.
- [ ] Upgrade admission and rollback are atomic with manifest routing.
- [ ] Retention respects waits/child runs and approval digests.
- [ ] Incompatible upgrade blocks with actionable plan.
- [ ] Resource bounds cover retained generations.
- [ ] Garbage collection cannot delete referenced artifact.

## Validation and required evidence

- [ ] Deploy versions A/B with active waits and effects.
- [ ] Kill deployment controller/worker during routing.
- [ ] Rollback and missing old artifact.
- [ ] GC race and long-lived child.
- [ ] Verify exact outputs/artifact lineage.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E17-T02 — Build reproducible binaries, SDK packages and signed artifacts
- [ ] E07-T06 — Implement reconciliation, artifact retention and safe upgrades

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E17-T04 — Publish Cloud contract and self-hosted deployment recipes

Issue: E17-T04 · Initial status: Backlog

Roadmap ID: E17-T04
Epic: E17 — Self-hosting, artifacts and Cloud integration contracts
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Provide deployment/artifact/health/observability/control APIs and examples for standalone binaries, containers and orchestrated fleet. Specify BLOK Cloud as an independent service product.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `docs/cloud-contract.md, examples/deploy, contract/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] All framework features remain usable without Cloud account.
- [ ] Cloud interface is versioned/authenticated and scoped.
- [ ] Example setup/teardown/restore commands work.
- [ ] Hosting provider does not get engine-specific private APIs.
- [ ] Separate Cloud backlog handoff covers infrastructure/UI/billing without creating them here.

## Validation and required evidence

- [ ] Deploy quote/order and Go/Node examples using documented recipe.
- [ ] Run readiness/drain/backup/restore.
- [ ] Consumer contract test for Cloud provisioning/control adapter.
- [ ] Verify no hosted credentials or billing dependencies in core.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E17-T03 — Implement multi-version deployment and artifact retention routing
- [ ] E16-T03 — Publish operational SLO metrics and monitoring integrations
- [ ] E18-T03 — Implement online resharding, drain and rolling recovery

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E18 — Distributed ownership, fencing and capacity

Scale accepted work across a fleet with tested ownership and failure semantics.

#### E18-T01 — Specify distributed persistence and partition ownership

Issue: E18-T01 · Initial status: Backlog

Roadmap ID: E18-T01
Epic: E18 — Distributed ownership, fencing and capacity
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P0 | Size: M | Review: R

## Problem and intended result

Choose persistence/coordination model through failure/load spike. Define partitions, leases, fencing tokens, timer/signal ownership, blob availability and retention consistency; publish guarantee matrix.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `store/distributed, docs/decisions, benchmarks/distributed`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Partition ownership has one authoritative fencing rule.
- [ ] Lost/paused owner cannot commit after takeover.
- [ ] Storage replication/ack failure guarantees are explicit.
- [ ] Timer/signal/blob ownership follows partition migration.
- [ ] Selected dependency cost/topology is measured.
- [ ] No claim that a log alone supplies failover.

## Validation and required evidence

- [ ] Network partition/process pause/lease expiry simulation.
- [ ] Competing writers and storage quorum failure.
- [ ] Replica lag and blob unavailability.
- [ ] Raw commit/load/restore measurements with topology.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E07-T07 — Implement verified retention, compaction, backup and restore
- [ ] E17-T01 — Implement deployment validation and production runtime endpoints

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E18-T02 — Implement fenced failover and distributed work admission

Issue: E18-T02 · Initial status: Backlog

Roadmap ID: E18-T02
Epic: E18 — Distributed ownership, fencing and capacity
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P0 | Size: M | Review: R

## Problem and intended result

Add partitioned routing and transactional fenced commits for runs/timers/signals, bounded admission and worker scheduling; preserve effect uncertainty on ownership change.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/cluster, store/distributed`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Old owner/result rejected after fencing advance.
- [ ] Accepted work survives stated failures.
- [ ] Deduplication identity is cluster-consistent.
- [ ] No tenant can monopolize admission.
- [ ] Storage outages have declared reject/defer behavior.
- [ ] External effects never gain universal exactly-once promise.

## Validation and required evidence

- [ ] Kill/pause/partition owners around every effect transition.
- [ ] Concurrent duplicates from different ingress nodes.
- [ ] Signal/timer ownership races.
- [ ] Failover under sustained load with exact effect counts.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E18-T01 — Specify distributed persistence and partition ownership

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E18-T03 — Implement online resharding, drain and rolling recovery

Issue: E18-T03 · Initial status: Backlog

Roadmap ID: E18-T03
Epic: E18 — Distributed ownership, fencing and capacity
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Move ownership and referenced blobs/timers safely while versions run, preserving lineage and backpressure. Define limits for partitions, tenants, queued and suspended runs.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `internal/cluster, benchmarks/distributed`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Resharding retains accepted work and stable idempotency identities.
- [ ] Fence prevents old owner from publishing.
- [ ] Rolling drain does not starve new or old artifacts.
- [ ] Resource usage is bounded during migration.
- [ ] Recovery/rollback restores authoritative routing.

## Validation and required evidence

- [ ] Reshard with nested runs/waits/child outputs and large blobs.
- [ ] Kill at transfer boundaries.
- [ ] Mixed artifact rollback and repeated migrations.
- [ ] Fairness/RSS/latency under load during movement.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E18-T02 — Implement fenced failover and distributed work admission
- [ ] E17-T03 — Implement multi-version deployment and artifact retention routing

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E18-T04 — Publish fleet-scale load and operational capacity envelopes

Issue: E18-T04 · Initial status: Backlog

Roadmap ID: E18-T04
Epic: E18 — Distributed ownership, fencing and capacity
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Exercise millions-of-requests-per-second ambition using controlled distributed load; report achievable envelope instead of fabricated capacity. Define request/workflow/step/external-call counts and storage economics.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§7, 10](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `benchmarks/fleet, docs/capacity`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Hardware, topology, payload, guarantees, TLS, observability and generators are disclosed.
- [ ] Warmup and repeated p50/p95/p99/throughput/CPU/RSS/errors/queue-depth results are raw.
- [ ] Generator bottleneck is identified.
- [ ] Overload/failover remains within stated bounds.
- [ ] Million-RPS claim appears only if measured.
- [ ] Cost/retention model includes retries, audit and blobs.

## Validation and required evidence

- [ ] Native HTTP, durable jobs, Node worker and mixed-tenant load.
- [ ] Slow/large/invalid inputs and saturated providers.
- [ ] Soak and ownership failures while load runs.
- [ ] Repeated controlled samples and regression/noise analysis.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E18-T03 — Implement online resharding, drain and rolling recovery
- [ ] E16-T03 — Publish operational SLO metrics and monitoring integrations
- [ ] E19-T14 — Certify cross-runtime application and release conformance

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E19 — Full runtime and SDK coverage

Give every promised language a real persistent worker and the same semantic/failure conformance.

#### E19-T01 — Publish SDK extension kit and supported runtime matrix

Issue: E19-T01 · Initial status: Backlog

Roadmap ID: E19-T01
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Extract worker/SDK implementation guidance, generated bindings, language dependency ownership, OS sandbox hooks, import ownership plugin and test matrix from Node.js evidence.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/conformance, docs/runtime-extension.md`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] SDK requires no workflow interpreter.
- [ ] Runtime advertises catalog/artifact/protocol compatibility.
- [ ] Matrix includes exact versions/platforms and unavailable status.
- [ ] Installer uses native dependency manager.
- [ ] Conformance exercises actual process/server.
- [ ] Template forbids per-step spawn.

## Validation and required evidence

- [ ] Build second-worker skeleton against Go conformance.
- [ ] Negative runtime/version/catalog fixtures.
- [ ] Validate packaging/ownership plugin checklist.
- [ ] Worker template passes process count and cancellation.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E08-T04 — Harden worker transport and publish equivalent workload evidence
- [ ] E12-T02 — Enforce node independence across all language import graphs
- [ ] E17-T02 — Build reproducible binaries, SDK packages and signed artifacts

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T02 — Implement Python3 persistent worker and SDK

Issue: E19-T02 · Initial status: Backlog

Roadmap ID: E19-T02
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Python3 SDK/runtime adapter against the extension kit. Cover Python async/thread cancellation semantics, decimal/int64 presence, pip/uv lock ownership and import aliases. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/python3, runtime/python3, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Python3 worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T03 — Implement Rust persistent worker and SDK

Issue: E19-T03 · Initial status: Backlog

Roadmap ID: E19-T03
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Rust SDK/runtime adapter against the extension kit. Cover Tokio cancellation/drop behavior, serde presence/numeric mapping, Cargo locks and module graph. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/rust, runtime/rust, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Rust worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T04 — Implement Java persistent worker and SDK

Issue: E19-T04 · Initial status: Backlog

Roadmap ID: E19-T04
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Java SDK/runtime adapter against the extension kit. Cover JVM process/thread budgets, interrupt/cooperative cancellation, Maven/Gradle artifacts and long integer schema mapping. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/java, runtime/java, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Java worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T05 — Implement Kotlin persistent worker and SDK

Issue: E19-T05 · Initial status: Backlog

Roadmap ID: E19-T05
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Kotlin SDK/runtime adapter against the extension kit. Cover Coroutine structured cancellation, nullability/defaults, JVM compatibility and Gradle package artifacts. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/kotlin, runtime/kotlin, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Kotlin worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T06 — Implement C# persistent worker and SDK

Issue: E19-T06 · Initial status: Backlog

Roadmap ID: E19-T06
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement C# SDK/runtime adapter against the extension kit. Cover .NET CancellationToken, nullable/default semantics, NuGet restore locks and Assembly packaging. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/csharp, runtime/csharp, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual C# worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T07 — Implement PHP persistent worker and SDK

Issue: E19-T07 · Initial status: Backlog

Roadmap ID: E19-T07
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement PHP SDK/runtime adapter against the extension kit. Cover Persistent gRPC hosting model, cooperative cancellation, PHP numeric/string semantics and Composer locks. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/php, runtime/php, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual PHP worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T08 — Implement Ruby persistent worker and SDK

Issue: E19-T08 · Initial status: Backlog

Roadmap ID: E19-T08
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Ruby SDK/runtime adapter against the extension kit. Cover Persistent gRPC thread/fiber hosting, cooperative interruption, integer semantics and Bundler locks. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/ruby, runtime/ruby, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Ruby worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T09 — Implement Swift persistent worker and SDK

Issue: E19-T09 · Initial status: Backlog

Roadmap ID: E19-T09
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Swift SDK/runtime adapter against the extension kit. Cover Structured concurrency cancellation, Codable optional/default semantics, SwiftPM and supported platform artifacts. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/swift, runtime/swift, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Swift worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T10 — Implement Dart persistent worker and SDK

Issue: E19-T10 · Initial status: Backlog

Roadmap ID: E19-T10
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Dart SDK/runtime adapter against the extension kit. Cover Async/isolate cancellation boundaries, integer encoding/nullability and pub lock ownership. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/dart, runtime/dart, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Dart worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T11 — Implement Elixir persistent worker and SDK

Issue: E19-T11 · Initial status: Backlog

Roadmap ID: E19-T11
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Elixir SDK/runtime adapter against the extension kit. Cover OTP supervision, BEAM mailbox bounds, process cancellation, maps/numeric mapping and Mix locks. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/elixir, runtime/elixir, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Elixir worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T12 — Implement Go remote persistent worker and SDK

Issue: E19-T12 · Initial status: Backlog

Roadmap ID: E19-T12
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Go remote SDK/runtime adapter against the extension kit. Cover Optional remote Go topology, context cancellation, matching native schema semantics and Go module/artifact locking. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/go, runtime/go, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Go remote worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T13 — Implement Bun and Deno persistent worker and SDK

Issue: E19-T13 · Initial status: Backlog

Roadmap ID: E19-T13
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Implement Bun and Deno SDK/runtime adapter against the extension kit. Cover Separate explicit worker identities, Node compatibility limits, permissions, lockfiles and persistent gRPC support. Reuse workflow semantics from Go engine only; supply narrow provider and node ownership integration.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `sdk/javascript, runtime/javascript, testdata/conformance`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Actual persistent gRPC process negotiates canonical catalog and artifact identity.
- [ ] Input/output/null/int64/default/error corpus matches native contract.
- [ ] Cancellation/deadline/capacity/restart/uncertain effects and generations pass.
- [ ] Dependency/SDK packaging works on declared supported matrix.
- [ ] Cross-node imports are rejected and same-node helpers allowed.
- [ ] Unsupported platforms/runtimes fail explicitly.
- [ ] No process is spawned per node call.

## Validation and required evidence

- [ ] Launch actual Bun and Deno worker from Go client.
- [ ] Run shared schema/error/auth/cancel/overload and crash suite.
- [ ] Kill after provider success before result publication.
- [ ] Fresh dependency install/package import and aliased import fixtures.
- [ ] Record startup/RSS/call overhead with versions and raw samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T01 — Publish SDK extension kit and supported runtime matrix

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E19-T14 — Certify cross-runtime application and release conformance

Issue: E19-T14 · Initial status: Backlog

Roadmap ID: E19-T14
Epic: E19 — Full runtime and SDK coverage
Milestone: M8 — Distributed deployment and runtime coverage
Priority: P1 | Size: M | Review: R

## Problem and intended result

Run equivalent business workflows with every worker, explicit supported matrices and dependency-boundary removal tests. Publish certified/experimental status per runtime and version.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§8](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `contract/conformance, .github/workflows, benchmarks/runtime`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] All advertised supported runtime combinations pass actual conformance.
- [ ] Unsupported/experimental status is visible in CLI/catalog/docs.
- [ ] Native versus worker business outputs/errors/effects agree.
- [ ] Removing runtime does not change unrelated application.
- [ ] Release assets contain protocol/schema metadata.
- [ ] Benchmarks distinguish per-runtime costs.

## Validation and required evidence

- [ ] Cross-language order workflow and generic/custom nodes.
- [ ] Mixed-worker failure/cancel/restart.
- [ ] Multi-version generation tests.
- [ ] Fresh package installs on actual platform runners.
- [ ] Per-runtime repeated raw benchmark samples.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E19-T02 — Implement Python3 persistent worker and SDK
- [ ] E19-T03 — Implement Rust persistent worker and SDK
- [ ] E19-T04 — Implement Java persistent worker and SDK
- [ ] E19-T05 — Implement Kotlin persistent worker and SDK
- [ ] E19-T06 — Implement C# persistent worker and SDK
- [ ] E19-T07 — Implement PHP persistent worker and SDK
- [ ] E19-T08 — Implement Ruby persistent worker and SDK
- [ ] E19-T09 — Implement Swift persistent worker and SDK
- [ ] E19-T10 — Implement Dart persistent worker and SDK
- [ ] E19-T11 — Implement Elixir persistent worker and SDK
- [ ] E19-T12 — Implement Go remote persistent worker and SDK
- [ ] E19-T13 — Implement Bun and Deno persistent worker and SDK

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

### E20 — Production evidence, migration and open-source release

Release only what application, security, usability and capacity evidence actually proves.

#### E20-T01 — Execute behaviorally equivalent current-Blok application comparisons

Issue: E20-T01 · Initial status: Backlog

Roadmap ID: E20-T01
Epic: E20 — Production evidence, migration and open-source release
Milestone: M9 — Production release and ecosystem handoff
Priority: P0 | Size: M | Review: R

## Problem and intended result

Run actual old and new engines on equivalent quote/order/job/webhook/streaming contracts with matching schemas/provider mocks, retries/cancel/durability. Translate supported workflows and report unsupported expressions honestly.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `migration, benchmarks/parity, testdata/parity`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Parity is based on observable business outputs/effects/errors rather than step counts.
- [ ] Both engine versions/configuration and commands are pinned.
- [ ] Success/failure/cancel/retry/recovery workloads have expected results.
- [ ] Unsupported migrations are actionable and never silently rewritten.
- [ ] Inertia/SPA mapping requires separate explicit contract.
- [ ] Raw results include startup/idle/load/recovery.

## Validation and required evidence

- [ ] Execute both engines against same mock provider services.
- [ ] Compare outputs/effect ledgers/status semantics.
- [ ] Process kill/duplicate delivery/error and cancellation.
- [ ] Migration round-trip plus deliberate unsupported mapper expressions.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E06-T03 — Ship the complete native quote application and DX fixture
- [ ] E07-T07 — Implement verified retention, compaction, backup and restore
- [ ] E09-T08 — Implement MCP node and workflow exposure
- [ ] E08-T04 — Harden worker transport and publish equivalent workload evidence

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E20-T02 — Measure newcomer DX and live AI application success

Issue: E20-T02 · Initial status: Backlog

Roadmap ID: E20-T02
Epic: E20 — Production evidence, migration and open-source release
Milestone: M9 — Production release and ecosystem handoff
Priority: P1 | Size: M | Review: R

## Problem and intended result

Evaluate setup, typed node/workflow composition, diagnosis/repair, package install, testing, inspection and deployment with unfamiliar developers and configurable live model providers.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `benchmarks/dx, benchmarks/ai, docs/evidence`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Task corpus includes generic and custom nodes plus all trigger selection.
- [ ] Success means actual application tests pass, not generated text.
- [ ] Time/repair/cost/diagnostic usefulness and failure rates are measured.
- [ ] Deterministic replay is separate from live model evidence.
- [ ] Participant/model conditions and uncertainty are disclosed.
- [ ] Regressions create concrete follow-up issues.

## Validation and required evidence

- [ ] Run predeclared application corpus on clean environments.
- [ ] Pin model settings and bounded repair.
- [ ] Include wrong refs, missing provider, stale approval, runtime failure and custom node gaps.
- [ ] Review redacted raw records and statistical limits.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E14-T04 — Generate and evaluate complete generic/custom application proposals
- [ ] E15-T03 — Publish Studio consumer contracts and notebook UX specification
- [ ] E13-T04 — Publish offline examples and hosted registry handoff
- [ ] E17-T04 — Publish Cloud contract and self-hosted deployment recipes

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E20-T03 — Complete security, fuzz, chaos and performance release audits

Issue: E20-T03 · Initial status: Backlog

Roadmap ID: E20-T03
Epic: E20 — Production evidence, migration and open-source release
Milestone: M9 — Production release and ecosystem handoff
Priority: P0 | Size: M | Review: R

## Problem and intended result

Audit capability/tenant isolation, parser/resource bounds, approvals/secrets, supply chain, distributed fencing and failure recovery. Profile optimization only after equivalence and correctness gates.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `testdata/security, benchmarks, docs/release`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] No release-blocking security/conformance failures remain hidden.
- [ ] Fuzz corpus covers schema/IR/protocol/archive inputs with bounded budgets.
- [ ] Chaos includes store/worker/network/artifact failures.
- [ ] Performance results retain validation/isolation/durability guarantees.
- [ ] Controlled runner publishes raw distributions and capacity limits.
- [ ] Required review conclusions link fixes/tests.

## Validation and required evidence

- [ ] Adversarial matrix across all trust boundaries.
- [ ] Extended race/fuzz/crash/soak suites.
- [ ] Restore and failover drills.
- [ ] Native/worker/HTTP/durable/fleet repeated profiles.
- [ ] Compare controlled baseline using documented noise policy.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E20-T01 — Execute behaviorally equivalent current-Blok application comparisons
- [ ] E20-T02 — Measure newcomer DX and live AI application success
- [ ] E18-T04 — Publish fleet-scale load and operational capacity envelopes
- [ ] E19-T14 — Certify cross-runtime application and release conformance

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.

#### E20-T04 — Publish production release, supported guarantees and ecosystem handoffs

Issue: E20-T04 · Initial status: Backlog

Roadmap ID: E20-T04
Epic: E20 — Production evidence, migration and open-source release
Milestone: M9 — Production release and ecosystem handoff
Priority: P0 | Size: M | Review: R

## Problem and intended result

Finalize stable API/brand/version compatibility after evidence review, signed artifacts, tested getting-started/docs, migration policy and Studio/registry/Cloud consumer handoffs. Enable release/support automation.

## Required context and ownership

- Read [AGENTS.md](https://github.com/well-prado/new-blok/blob/main/AGENTS.md), [ROADMAP.md §4–6](https://github.com/well-prado/new-blok/blob/main/ROADMAP.md), and [architecture §§10, 11, 12](https://github.com/well-prado/new-blok/blob/main/docs/architecture.md).
- Owning implementation areas (create only as required): `README.md, docs, .github/workflows, release artifacts`.
- Inspect actual current code, relevant dependency issues and conformance fixtures. Do not treat proposed APIs or lab closure as implemented guarantees.
- Boundary: deliver this issue's behavior. Adjacent tasks remain under their own stable IDs. Studio UI, hosted registry and Cloud services are separate products; only owned framework contracts/fixtures belong here.

## Acceptance criteria

- [ ] Clean install executes documented quote/order examples and CLI/package flow.
- [ ] Supported runtime/trigger/platform matrix and durability limits are explicit.
- [ ] Signed checksummed release and SBOM/provenance verify.
- [ ] README claims match measured evidence.
- [ ] Separate product contracts have consumer tests and owner/backlog handoff.
- [ ] Every milestone gate has evidence and unresolved non-blockers are declared.
- [ ] No unresolved release blocker is relabeled done.

## Validation and required evidence

- [ ] Fresh download/verify/install/build/test/deploy per supported OS.
- [ ] Documentation command smoke and broken-link audit.
- [ ] Consumer conformance for Studio/Cloud/registry.
- [ ] Backup/upgrade/rollback release drill.
- [ ] Maintainer approval of release evidence.
- [ ] Add synthetic fixtures with predeclared expected output/error/effect counts. Include negative/failure cases, not just happy-path compilation.
- [ ] Run focused package/language tests first, then `go vet ./...`, `go test -race ./...`, `go build ./...` and `git diff --check`. New SDK code also runs its native lint/type/build/test commands with exact versions recorded in the PR.
- [ ] Run the applicable actual protocol/store/worker/crash/load suite described above. A fake, file-presence test or green unrelated suite does not satisfy integration evidence. If no Go source changes, document why each Go gate is unchanged and still run the current repository gate.
- [ ] Record commands, versions, fixtures/seeds, results and material limitations. Update examples, machine-readable contract/diagnostic fixtures and documentation if behavior changes.

## Dependencies

- [ ] E20-T03 — Complete security, fuzz, chaos and performance release audits

## Cross-cutting invariants

Nodes never import/invoke nodes; workflows compose. Builders do not execute effects. Native values preserve logical immutability. Trust/authentication, input validation and authorization are distinct. All execution queues/payloads/retries/depth are bounded. Durable acknowledgments follow committed state. Unknown effects are reconciled or protected by tested idempotency. Secret values/customer data never enter Git, issues or model-visible catalogs. Scale claims require equivalent representative measured workloads.

## Delivery and review

One branch `codex/<issue-number>-<description>` and one PR, linked to this issue and project. Keep Status current. Review R means independent specialist review of the relevant contract/security/durability/performance evidence; record review conclusions. Completion requires every acceptance item and ROADMAP Definition of Done. Do not close a dependency/epic solely because this issue passed.
