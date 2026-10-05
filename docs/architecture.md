# Building complete applications with New Blok

Design baseline · 2026-10-01 · proposed APIs

This is the implementation baseline derived from the Go application design proposal and architecture experiments. The roadmap owns delivery and exit evidence. Examples below describe the intended API; they become supported only when the corresponding issues pass. The earlier working name Lattice is replaced here by New Blok, whose final brand remains open.

## 1. Application model

A node is a typed function with explicit dependencies. A workflow records sequencing and control flow. A trigger binding translates authenticated protocol input into a workflow's domain input. An application registers these components and owns its executable.

Builders run during preparation and record structural documents. They do not execute nodes or external effects. Complex calculations belong in ordinary Go nodes; the engine never evaluates raw Go or JavaScript strings. Nodes never import or invoke another node. Child workflows are explicit workflow instructions.

```text
Go functions + generated bindings → validated structural program
                                           ↓
Selected triggers → admission → Go engine → native nodes / gRPC workers
                                  ↓                    ↓
                             durable store       injected dependencies
                                  ↓
                      inspection / telemetry / audit
```

The same workflow may have multiple trigger bindings with separately validated mappings and policy. Domain types do not carry HTTP headers, broker connections or transport-specific state. A verified principal travels through a separate trusted execution channel; caller data cannot establish it.

## 2. Package and product boundaries

Begin with one versioned Go module and small public packages only where responsibilities exist: `node`, `flow`, `app`, `contract`, trigger adapters, the embedded store, observation adapters, worker integration, and testing helpers. Compiler, interpreter and journal implementation remain internal. Introduce packages when their implementation issues require them; do not create empty directories now.

The engine imports neither HTTP servers, broker clients, storage implementations, frontend assets, ORM packages nor AI provider SDKs. Narrow ports and explicit registration make adapters selectable. `app.Use` is composition, not discovery by global `init`. Installing an extension does not activate it. Applications import/register selected packages and rebuild; native dynamic Go plugins are not the universal plugin mechanism.

Studio UI, the hosted registry service and BLOK Cloud live in separate repositories. This repository owns their versioned inspection, package, deployment and operations contracts. Dev inspection starts read-only. Studio should be simple, accessible and visually restrained, with a notebook showing input → processing → output, attempts, logs, errors and timings. Its future UI work uses the frontend-design skill and can use shadcn components. Production monitoring consumes optional telemetry exporters and authorized APIs, never inherited development permissions.

## 3. Native node and workflow authoring

Intended node surface:

```go
type QuoteInput struct {
    SKU string `json:"sku"`
    Quantity int `json:"quantity"`
}
type Quote struct { TotalCents int64 `json:"totalCents"` }

func CalculateQuote(ctx context.Context, in QuoteInput) (Quote, error) {
    if err := ctx.Err(); err != nil { return Quote{}, err }
    if in.SKU != "coffee" || in.Quantity < 1 || in.Quantity > 100 {
        return Quote{}, fmt.Errorf("invalid quote input")
    }
    return Quote{TotalCents: int64(in.Quantity) * 1500}, nil
}
var CalculateQuoteNode = node.Define(
    "shop/calculate-quote", "1.0.0", CalculateQuote, node.Pure(),
)
```

Descriptors identify nodes independently of paths and declare input/output schemas, effects, capability requirements, opaque secret references, runtime, idempotency behavior, limits and agent policy. Pure is a declaration, not sandboxing proof. Validation must enforce supported constraints beyond struct decoding.

Intended whole-value workflow surface:

```go
var QuoteWorkflow = flow.MustDefine[QuoteInput, Quote](
    flow.Spec{Name: "shop/quote", Version: "1.0.0", Durability: flow.Memory},
    func(w *flow.Builder, input flow.Ref[QuoteInput]) flow.Ref[Quote] {
        return flow.Call(w, "calculate", CalculateQuoteNode, input)
    },
)
```

Returning a reference declares the output. `Define` returns a broken builder rule as an error for tooling; `MustDefine` panics at the offending builder call (#251). Whole-value wiring works without generation. Field-level composition uses generated typed accessors. A call takes one input reference, so a call cannot yet read two earlier steps at once; the generated argument structs that would combine references, and literal call inputs, have no consumer or program form yet. Today `Lower` carries the workflow input, whole call results and accessor-selected fields of earlier calls into the program, and rejects literal or workflow-input-field call inputs, which have no program form yet ([ADR 0001](decisions/0001-public-api-boundaries.md), #244). The agent catalog lowers composed workflows through the same rules (`internal/lowering`); its only extensions are literal call inputs, which its dispatch substitutes, and child workflow calls (#249). `Lower` appends the workflow output instruction under the id `output` (`flow.OutputID`), so no builder accepts that id for a step (#247). Step ids follow the document id grammar `^[a-z][a-z0-9_-]{0,63}$` (`contract.IDPattern`), so an id never contains the `.` that separates a reference's fields (#251). Ordinary Go fields cannot hold both `string` and `Ref[string]`. Generation uses Go package/type analysis, never executes arbitrary package initialization, marks generated files and is deterministic.

Unified layout: `nodes/<runtime>/<node>/`; classic layout: `runtimes/<runtime>/nodes/<node>/`. Workflow source remains under a dedicated application workflow directory. Layout does not change node identity. Files inside one node may import each other and approved utility/domain packages, but cannot import another node. CLI migration is transactional and validates ownership/collisions. Layout discovery (`internal/tooling/layout`, [ADR 0023](decisions/0023-layout-discovery.md)) reads `blok.json`, `go.mod`, Go syntax and foreign `node.json` descriptors without building or running anything, takes identity from the `node.Define`/`flow.Define` descriptor rather than the directory, reads workflows only from the manifest's explicit `workflows` paths (default `workflows`), never follows a symbolic link, and emits the same canonical catalog digest for both layouts; duplicate identities or versions, case collisions, cross-node imports and link escapes fail as stable `layout_*` diagnostics. Layout migration is not implemented yet.

The application composition root registers nodes, workflows and adapters; configuration supplies operational settings and cannot silently add code. A Go-only application uses normal `go mod`, `go test`, `go build`, `context.Context` and constructors, and requires no Node.js, broker, container or registry account unless selected modules need them.

## 4. Structural program and schema semantics

The human-readable document is independent from protobuf transport encoding. Adopt one versioned structural IR after reconciling the lab `workflow/v1` and proposed application shape in E02. Bindings are separate from workflows. Compile references to indexed instructions once per artifact and validate before admission; never compile schemas per step.

Define missing versus null, signed integer ranges, exact money, timestamps, defaults, unknown fields, optional objects, collections, unions and binary/blob references. JSON Schema defaults are annotations; normalization is a specified operation. Static compatibility supports a declared subset; unprovable edges yield a diagnostic or runtime validation requirement.

The implemented bounded subset requires exactly one union branch to match,
including null. A null schema rejects non-null values. The pure catalog's
generic numeric envelope preserves exact JSON integers; its supplied inner
schema owns integer range/wire rules. Template expansion preflights the byte
budget before allocating output. See [ADR 0012](decisions/0012-catalog-null-and-template-bounds.md)
for the correction, supported array limits and pending Node mirror integration.

Field references select `encoding/json` object keys, so a path resolves identically on a typed node output and on its JSON-decoded form, with work bounded by the selected member; a selected field keeps its Go type. See [ADR 0001](decisions/0001-public-api-boundaries.md#field-references-select-encodingjson-keys-241).

Logical values are immutable. Maps, slices and pointers require isolation between nodes and branches. Typed native fast paths must preserve this rule, and benchmarks include required validation/copies. Portable encodings serve journals and worker boundaries. Stable diagnostics name code, file/line, workflow, step, field, expected/actual and remediation.

## 5. Control flow

Call, condition, iteration, try, wait, parallel and child workflow are explicit instructions. Step IDs are globally unique in a definition; attempt identities also contain iteration and invocation paths. Arm handles cannot escape scope. A typed `Choose` joins compatible outputs. Bounded `Each` preserves input order; parallel joins declare outputs and cancellation behavior. No unlimited concurrency or recursive invocation.

Runtime decisions use typed comparisons and versioned pure operations. Optional defaults and string templates are explicit. First parallel policy is fail-fast with cooperative sibling cancellation. Catch handles business failures; suspension and cancellation remain distinct. Finally does not run because work merely suspends and cannot be promised after process death.

## 6. Admission and all nine triggers

One admission path authenticates/authorizes, maps and validates input, deduplicates, checks capacity and selects execution durability in a documented order. Admission returns accepted work separately from completion. Disconnect behavior is binding-specific: cancel memory work or stop waiting for durable work.

| Trigger | Adapter responsibility |
| --- | --- |
| HTTP | routes, body/parameter mapping, limits, status/headers/cookies |
| Webhook | original-body signature verification, replay window and event deduplication |
| Worker | lease, acknowledgment transfer, retry ownership and dead letter |
| Cron | timezone, daylight saving, overlap, missed occurrences and stable tick identity |
| Pub/sub | subscription, cursor, acknowledgment and provider flow control |
| gRPC | protobuf mapping, deadlines, status and cancellation |
| SSE | HTTP event framing, cursors, reconnect and slow-client bounds |
| WebSocket | connection/message/disconnect, identity, framing and backpressure |
| MCP | tools/resources, schemas, caller authorization and output mapping |

HTTP and one durable job path ship first. Other adapters pass shared conformance plus actual protocol integration tests. They implement no second interpreter, mapper or retry engine. The shared `trigger` contract, the per-kind completion/disconnect table and the `contract/conformance.RunTrigger` harness are recorded in [ADR 0005](decisions/0005-trigger-adapter-contract.md). Multiple bindings can share a listener where protocols permit; independent TLS/listeners remain selectable. Queue acknowledgment follows an explicit transfer model and stable delivery deduplication.

Worker handlers write durable business state through the handler's `worker.Tx`
so effects and acknowledgment commit together. The handler context carries the
claim's write domain when the store exposes it; a nested queue submission to
that same domain returns an actionable error before waiting on its own writer
lock. A nested submission that bypasses that check (a replaced context, or a
wrapper that hides the domain) waits out one busy timeout; its saturation
names the write domain it waited on, and when that is the claim's own the job
fails as a nested submission instead of being deferred. Saturation from
another store remains backpressure and defers the job without spending an
attempt. Stores and wrappers opt into detection by exposing and forwarding
`store.WriteDomainProvider` and by naming the domain on their busy errors
(`store.WithWriteDomain`).

## 7. Durability, effects and artifacts

Memory mode permits in-flight loss. Journal mode resumes accepted work after process restart with its volume intact; it does not imply disk-loss survival or multi-host failover. Choose one embedded backend through a measured spike (SQLite candidate, Pebble alternative), implement the winner and keep the port replaceable.

Intent precedes effect dispatch where required. Progress and outputs are committed before acknowledgment. Group commit amortizes persistence without weakening barriers. Runs retain exact program, artifact, schema and checkpoint identities; branch choices, iteration paths, attempts and joins survive restart. Timers/retry delays suspend without occupying an active worker. Signals are authorized, durable, deduplicated and support arrival-before-wait races.

An external success followed by a crash before result commit creates an uncertain outcome. Provider idempotency, business unique keys, transactional outboxes or explicit reconciliation may establish the result. Never promise universal exactly-once external effects or treat cancellation as reversal. Stale attempts cannot publish outputs.

Deployment manifests bind workflow document, native binary, worker artifacts, schemas, module locks, runtime versions, compiler and checkpoint formats. Immutable versions cannot be overwritten. Initial upgrades drain/retain compatible executables or refuse startup with actionable diagnostics. A later multi-version manager retains old workers for old runs. Replay creates a new run with lineage; partial reruns require separate tested effect semantics.

Retention, compaction, backup, corruption checks and restore must preserve verified checkpoints and audit obligations. The current SQLite backup contract uses `VACUUM INTO` to create a new, transaction-consistent snapshot; it never overwrites an existing destination. Restore first runs `PRAGMA integrity_check`, copies into a temporary file, syncs it, and renames it into a new destination before checking the restored database again. These guarantees cover one local filesystem only: they do not provide disk-loss survival, replication, or multi-host failover. Compaction erases a run's content once it is past retention, keeping only digests and timestamps; SQLite deletes securely and the write-ahead log is purged after compaction, but a backup taken before an erasure still holds the content (ADR 0021 §7). The worker queue erases finished jobs the same way, to digest-only tombstones that keep its dedupe contract (ADR 0006, #290). Business tables remain application-owned; the journal is not an ORM or a substitute for domain persistence.

Distributed ownership remains an evaluation spike, not an available backend. [ADR 0017](decisions/0017-distributed-persistence-ownership.md) records an isolated etcd v3.6.5 fencing prototype, the incarnation-plus-revision fence needed across snapshot restore, the S3 blob acknowledgment boundary, and the limits of its one-host failure experiments. No app or engine code imports the prototype, and the spike does not select a production backend.

## 8. Workers and runtime coverage

Go runs natively. Node.js is the first external worker over persistent authenticated gRPC, with reusable channels and explicit topology. Protocol negotiation defines catalog digests, call/attempt/generation identity, deadlines, cancellation, capacity, frame direction, reconnect, errors, payload/blob bounds and idempotency. Lost transport can leave an effect uncertain. No fresh process per step.

Target workers include Node.js/TypeScript, Python3, Rust, Java, Kotlin, C#, PHP, Ruby, Swift, Dart and Elixir; Go also has an optional remote worker conformance path. Bun and Deno are separate JavaScript worker selections with tested compatibility. Language SDKs describe node contracts and never reinterpret workflows. Install dependencies with existing language managers; the Blok package client coordinates manifests/locks, not replacement package managers.

Each runtime issue requires full schema, cancellation, overload, restart, generation, security and error conformance on a declared supported matrix. Version strings alone never prove compatibility. Unsupported platforms fail diagnostically rather than appearing available.

## 9. Security, AI tools and inspection

Native code shares application trust. Remote processes become security boundaries only with credentials, filesystem/network/OS isolation and scoped policy. Separate application authorization from input validation. Secret providers resolve opaque references only for authorized executions; inputs, outputs, errors, logs and inspection have redaction/access policies.

Agent catalogs fail closed for missing/invalid policy. A node is a typed tool; a workflow is a composed tool with typed input/output, declared effects and inherited limits. Child calls cannot widen authority. Approvals bind action/input/workflow/artifact digests, approver, scope and lifetime and persist reliably. Assertions, evidence and trusted provenance are checked before result publication; model output is not authority.

The implemented pre-alpha durable tool policy is described in [ADR 0008](decisions/0008-durable-tool-policy.md). Its trusted catalog adapter also binds the actual registered tool/program digest, including transitive child identities and resource reservations, independently of the admitted deployment artifact. SQLite decisions and audit survive process restart; deterministic child/root verification precedes trusted commit. Ambiguous dispatched operations block redispatch pending #48's reconciliation and exact executable retention; this slice does not establish safe-upgrade or distributed recovery guarantees.

CLI and read-only development MCP expose versioned inspection/validation projections with source/test references and stable diagnostics. The CLI half is `blok check`, `blok test` and `blok inspect`: one `blok-cli/v1` report per run carrying #28 diagnostics, stable exit codes, a static field-allowlisted and redacted catalog, and no execution of application code outside `go test` ([ADR 0024](decisions/0024-check-test-inspect-cli-contract.md)). Bounded repair validates every proposal against the real compiler. Generic nodes and custom nodes use the same descriptor and review path. Complete recipes cover authenticated CRUD, jobs, signed webhooks, schedules, streaming and MCP with explicit storage/migration/module dependencies.

Dev inspection streams bounded per-step input, started/processing events, output, attempts, timing, logs and errors. Sensitive content is authorized/redacted and opt-in. Production telemetry supports OpenTelemetry-compatible export without importing providers into the engine: the engine allocates W3C trace context for observed runs under an explicit `TracePolicy` and hands it to nodes, workers and child runs through the stdlib-only `contract/observe` port, and the optional `observe/otel` module (a separate Go module, so unselected applications carry none of its dependencies) exports traces, metrics and logs with bounded labels and a drop-and-count policy that never blocks a run ([ADR 0020](decisions/0020-optional-observability-export.md)). Optional events may drop/sample under pressure; required audit and state transitions use reliable paths with explicit failure policy.

Mandatory audit is a separate contract, not telemetry ([ADR 0021](decisions/0021-sensitive-data-and-reliable-audit.md)). `contract/audit` records approval, reconciliation and deployment decisions (actor, outcome, digests and opaque scope names; never inputs, evidence text or secret values) in the same store transaction as the decision, so an audit failure refuses the decision with nothing applied; an optional mirror receives copies only after commit and its drops cannot remove a record. Audit reads are tenant-authorized and digest-verified, retention never deletes a record for an active run, under legal hold or younger than the legal minimum, and audit is backed up and restored with durable state. `observe/redact` is the single redaction boundary for inspection, worker logs, telemetry attributes, catalog listings and audit records, including bounded decoding of JSON-in-string, percent-encoded and base64 content; it covers framework-owned channels only, and a native node can still leak what it is given.

## 10. Registry, deployment and scale

Node/workflow packages have namespaced immutable identities, schemas, capability manifests, artifacts, dependency bounds and integrity digests. E13-T02 implements deterministic dependency resolution, exact execution-relevant locks, and a bounded verified offline cache; see [ADR 0018](decisions/0018-deterministic-package-resolution.md). E13-T03 owns the later CLI add/remove/update/verify operations and atomic project-file changes. Go and npm remain authoritative for their own dependency graphs; Blok captures their exact native locks and execution context without acting as a universal installer. Hosted publishing, ownership/moderation/search infrastructure and billing belong to the separate registry product.

App-owned binaries support health/readiness/metrics, bounded admission, signal-driven drain, graceful worker shutdown and durable-volume configuration. Operational SLO metrics (readiness, admission saturation, queue depth, latency, errors, uncertainty, timer lag, worker availability, storage growth and exporter loss) have one catalogue in the stdlib-only `observe/slo` port, rendered on `/metrics` and optionally exported by `observe/otel`; alerts distinguish stalled work (its owner is gone) from waiting and uncertain work, and `examples/monitoring` holds vendor-neutral collector, rule and dashboard examples ([ADR 0022](decisions/0022-operational-slo-metrics.md)). Cloud consumes reproducible deployment manifests, artifacts, readiness and operational APIs; framework self-hosting remains first-class. No proprietary service is required for core operation.

Millions of requests per second is a fleet-scale design target, not a bootstrap claim. Partition ownership, fencing, replicated persistence, timers, blobs, load balancing, fairness and resharding require independent failure and capacity tests. Distinguish accepted requests, completed workflows, steps and external calls. Model retention and audit/telemetry cost per event. ADR 0017 covers only one-host Docker failure and load experiments; those measurements are not fleet RPS or multi-region capacity. A replicated journal must fence each state commit in the same authoritative storage transaction; an ownership log alone is not failover.

Benchmarks include useful native quote HTTP, journaled orders, bounded parallel work, worker equivalents, suspended runs, mixed tenants, slow clients, invalid/large payloads and overload. Publish hardware/topology/configuration, tool versions, warmup, repeated distributions, throughput, p50/p95/p99, CPU, RSS, allocations, queue depth, errors and crash recovery. Use controlled runners/noise policy for regressions. Optimization requires profiles.

## 11. Evidence carried forward

The architecture lab demonstrated slices of Go execution, journal replay, Node.js gRPC, trigger normalization, CLI layouts, inspection, approvals and container operations. It did not establish application-level speedup over current Blok. Equal four-step source fixtures do not establish behavioral parity. Its deterministic three-goal authoring evaluation yielded two valid plans and one rejected plan; it does not measure live model reliability. Native listener coverage, complete schema enforcement, artifact retention, distributed failover, signing and production capacity still require implementation evidence.

The roadmap therefore includes executable current-Blok parity workloads, live-provider AI evaluation, crash injection, per-runtime conformance and controlled fleet load gates. Lab status labels do not waive these requirements.

## 12. Decisions and technical references

Open decisions have explicit owners: canonical schema/IR (E02), Go API ergonomics (E03), embedded backend (E07), worker topology/codec (E08), registry trust/distribution (E13), distributed persistence/ownership (E18), and release capacity envelope (E20). Resolve each through a decision record and executable evidence before dependent behavior is frozen. The public-surface inventory, compatibility process and synthetic evidence policy are recorded in [ADR 0001](decisions/0001-public-api-boundaries.md), with machine-readable ownership and evidence fixtures alongside it. These fixtures are planning contracts until their owning implementation issues add executable validation.

- [Go module/package layout](https://go.dev/doc/modules/layout)
- [Go release history](https://go.dev/doc/devel/release)
- [ProtoJSON presence and numeric mapping](https://protobuf.dev/programming-guides/json/)
- [JSON Schema annotations/defaults](https://json-schema.org/understanding-json-schema/reference/annotations)
- [gRPC cancellation](https://grpc.io/docs/guides/cancellation/)
- [gRPC performance and channel reuse](https://grpc.io/docs/guides/performance/)

Read `ROADMAP.md` for issue-specific implementation boundaries, tests and release evidence.
