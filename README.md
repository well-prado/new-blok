<h1 align="center">New Blok</h1>
<p align="center">Compose applications. Understand every step.</p>
<p align="center">
  <a href="https://github.com/well-prado/new-blok/actions/workflows/ci.yml"><img src="https://github.com/well-prado/new-blok/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE">Apache-2.0</a> · <a href="ROADMAP.md">Roadmap</a> · <a href="docs/architecture.md">Architecture</a> · <a href="CONTRIBUTING.md">Contributing</a>
</p>

## Build with clarity

New Blok is an open-source Go application framework built around typed nodes, inspectable workflows, and independently selectable triggers. Our aim is to make building a complete application feel natural: write ordinary Go, compose useful operations, test behavior locally, and deploy the capabilities your application needs.

A node does one piece of work. A workflow connects nodes into an application operation. A trigger exposes that operation through a protocol. Your application owns the dependencies and executable.

**Development status: pre-alpha.** This repository starts the real framework implementation. The application APIs in the architecture guide are proposals unless their owning issues pass; no production release or throughput claim is available yet. `new-blok` is the working name.

## A framework that grows with your application

The delivery plan covers:

- **Native Go applications:** typed functions, generated field references, structural workflows, normal `go test`, and an application-owned binary.
- **Durable work:** explicit admission, retries, waits, signals, child workflows, recovery, and honest handling of uncertain effects.
- **Every entry point:** HTTP, webhook, cron, worker, pub/sub, gRPC, SSE, WebSocket, and MCP through one admission contract.
- **Your preferred language:** Go in process; Node.js first over persistent gRPC, then Python, Rust, Java, Kotlin, C#, PHP, Ruby, Swift, Dart and Elixir. Bun and Deno receive their own tested worker adapters.
- **Developer tooling:** a Go CLI, unified or classic node layouts, clear diagnostics, and reproducible node/workflow package installation.
- **Native Windows:** first-class Windows execution and easy PowerShell installation alongside macOS/Linux, without requiring WSL, Bash or containers. This remains planned but is **unverified**: native Windows validation is deferred to a separate Windows track ([#297](https://github.com/well-prado/new-blok/issues/297)) in [#156](https://github.com/well-prado/new-blok/issues/156) and [#157](https://github.com/well-prado/new-blok/issues/157).
- **AI composition:** schema-described nodes as tools and workflows as composed tools, with discoverable capabilities, bounded execution, and enforced authorization.
- **Production insight:** optional telemetry exporters, bounded observability, redaction, and reliable audit paths.

These are roadmap commitments, each with acceptance tests and release gates. We measure modularity, developer experience, recovery and capacity instead of treating them as slogans.

## Get involved

To run the bootstrap today:

```sh
git clone https://github.com/well-prado/new-blok.git
cd new-blok
go run ./cmd/blok version
go test -race ./...
go build -o bin/blok ./cmd/blok
```

Create a conventional Go application without a registry account or foreign runtime:

```sh
go run ./cmd/blok new ../my-app --module example.com/my-app --non-interactive --framework .
cd ../my-app
go test ./...
go run ./cmd/my_app
```

The starter is a New Blok application: a typed quote node, a workflow that
composes it through generated typed accessors, and the HTTP trigger, wired in
`internal/app`. Post `{"sku":"coffee","quantity":2}` to
`http://localhost:8080/quotes`. `--framework .` points the new module at this
checkout through a `replace` directive; a `blok` installed from a published
version depends on that version instead. `--layout unified` places the node
under `nodes/go/` instead of `runtimes/go/nodes/`. `blok new` runs
`go mod tidy`, so the application builds as created.

The framework generator analyzes Go source without executing package
initializers and writes only marked generated files. In the application,
`blok generate` regenerates the accessors from the types file `blok.json`
names; repeat it with `--check` to verify byte-stable output. `blok.json` is validated when read: an unknown field or a
path outside the project is refused. It may list explicit `workflows` paths
(default `workflows`); layout discovery reads nodes and workflows from their
descriptors without running code ([ADR 0023](docs/decisions/0023-layout-discovery.md)).

In the application, `blok check` validates it without running any of its
code (`blok.json` and `go.mod`, node import independence, stale bindings,
workflow step ids and `go vet`), `blok test` runs its tests through
`go test`, and `blok inspect` describes its nodes, workflows and HTTP routes
with source, test and example references. Each takes `--json` for the
versioned `blok-cli/v1` report and exits 0 when clean, 1 on problems and 130
when interrupted ([ADR 0024](docs/decisions/0024-check-test-inspect-cli-contract.md)).
Their Windows behaviour is unverified.

The initial toolchain is Go 1.27.1. The first application milestone delivers a quote service that can be authored, tested and served entirely in Go. Follow the [roadmap](ROADMAP.md) for implementation order, issue dependencies, and exit evidence.

### Run the native quote application

The quote example is a Go-only executable; it does not require Node.js, Python, a container, or a foreign runtime:

```sh
go run ./examples/quote/cmd/quote
curl -i -X POST http://localhost:8080/quotes \
  -H 'content-type: application/json' \
  -d '{"sku":"coffee","quantity":2}'
# {"totalCents":3000}
```

Invalid SKU or quantity requests return a bounded client error with a `requestId` for correlation. The executable owns composition, lifecycle, and the HTTP listener; no handwritten workflow IR is required.

The pre-alpha [durable tool policy](docs/decisions/0008-durable-tool-policy.md) connects the registered agent catalog to SQLite approval/audit and deterministic publication gates. Its tests exercise real HTTP providers, persistent Node workers and process-kill recovery. Exact executable retention and authorized reconciliation remain dependencies; these tests do not establish production or live-model reliability.

## An ecosystem with clear boundaries

Application-owned [deployment examples](examples/deploy/README.md) exercise
native Go and persistent Go/Node containers, readiness, bounded admission and
signal drain. The SQLite order example checks actual retained journal artifact
and checkpoint compatibility, refusing incompatible cold starts. Local evidence
and remaining limits are recorded alongside the examples; native Windows
deployment evidence remains pending #156.

This repository owns the framework, CLI, package client, adapter contracts, and conformance suites. Studio, the hosted package registry, and BLOK Cloud will be separate products.

Studio will offer a simple notebook-like development experience: input → processing → output, with attempts, logs, errors and timing visible for each step. Its UI will consume a versioned inspection API. Cloud will consume deployment and operational contracts; applications will remain self-hostable.

The scale goal includes deployments serving millions of requests per second across a measured fleet. Capacity depends on workload, hardware, durability and topology. Controlled benchmarks, overload tests, failure injection and published limits gate every scale claim.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) and the assigned issue before starting. Every implementation task has scope, dependencies, tests, acceptance criteria and required review. Vulnerabilities belong in [private reports](SECURITY.md).

## License

New Blok is open-source software licensed under the [Apache License 2.0](LICENSE).
