<h1 align="center">New Blok</h1>
<p align="center">Compose applications. Understand every step.</p>
<p align="center">
  <a href="https://github.com/well-prado/new-blok/actions/workflows/ci.yml"><img src="https://github.com/well-prado/new-blok/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE">Apache-2.0</a> · <a href="ROADMAP.md">Roadmap</a> · <a href="docs/architecture.md">Architecture</a> · <a href="CONTRIBUTING.md">Contributing</a>
</p>

## Build with clarity

New Blok is an open-source Go application framework built around typed nodes, inspectable workflows, and independently selectable triggers. Our aim is to make building a complete application feel natural: write ordinary Go, compose useful operations, test behavior locally, and deploy the capabilities your application needs.

A node does one piece of work. A workflow connects nodes into an application operation. A trigger exposes that operation through a protocol. Your application owns the dependencies and executable.

**Development status: pre-alpha.** This repository starts the real framework implementation. It currently contains a tested CLI bootstrap and an executable delivery plan. The application APIs in the architecture guide are proposals; no production release or throughput claim is available yet. `new-blok` is the working name.

## A framework that grows with your application

The delivery plan covers:

- **Native Go applications:** typed functions, generated field references, structural workflows, normal `go test`, and an application-owned binary.
- **Durable work:** explicit admission, retries, waits, signals, child workflows, recovery, and honest handling of uncertain effects.
- **Every entry point:** HTTP, webhook, cron, worker, pub/sub, gRPC, SSE, WebSocket, and MCP through one admission contract.
- **Your preferred language:** Go in process; Node.js first over persistent gRPC, then Python, Rust, Java, Kotlin, C#, PHP, Ruby, Swift, Dart and Elixir. Bun and Deno receive their own tested worker adapters.
- **Developer tooling:** a Go CLI, unified or classic node layouts, clear diagnostics, and reproducible node/workflow package installation.
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

The initial toolchain is Go 1.27.1. The first application milestone delivers a quote service that can be authored, tested and served entirely in Go. Follow the [roadmap](ROADMAP.md) for implementation order, issue dependencies, and exit evidence.

## An ecosystem with clear boundaries

This repository owns the framework, CLI, package client, adapter contracts, and conformance suites. Studio, the hosted package registry, and BLOK Cloud will be separate products.

Studio will offer a simple notebook-like development experience: input → processing → output, with attempts, logs, errors and timing visible for each step. Its UI will consume a versioned inspection API. Cloud will consume deployment and operational contracts; applications will remain self-hostable.

The scale goal includes deployments serving millions of requests per second across a measured fleet. Capacity depends on workload, hardware, durability and topology. Controlled benchmarks, overload tests, failure injection and published limits gate every scale claim.

## Contributing and security

Read [CONTRIBUTING.md](CONTRIBUTING.md) and the assigned issue before starting. Every implementation task has scope, dependencies, tests, acceptance criteria and required review. Vulnerabilities belong in [private reports](SECURITY.md).

## License

New Blok is open-source software licensed under the [Apache License 2.0](LICENSE).
