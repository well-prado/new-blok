# Contributing

New Blok is being built in public. Start with the [roadmap](ROADMAP.md), the [architecture](docs/architecture.md), and an issue whose dependencies are satisfied. Propose changes to unresolved contracts in their owning issue before implementing a conflicting API.

Use Go 1.27.1 for the initial toolchain. Run:

```sh
go vet ./...
go test -race ./...
go build ./...
```

The distributed tests run only when `BLOK_DISTRIBUTED_ENDPOINTS` (with `BLOK_DISTRIBUTED_ETCD_VOTERS`, `BLOK_DISTRIBUTED_ETCD_NETWORK` and `BLOK_DISTRIBUTED_S3_*`) names a real etcd cluster; plain `go test -race ./...` is safe with it set, because `internal/clustertest` gives every cluster test a shared lock and every test that pauses or partitions voters an exclusive one, across packages, keyed by the endpoints (no `-p=1` needed).

Formatting follows `gofmt`. Focus tests on observable behavior, failure handling and contract boundaries. Durable execution changes require process-crash evidence; worker changes require cross-language conformance; performance changes require profiles and repeated measurements.

Create a branch named `codex/<issue>-<description>` and one pull request per implementation issue. Describe the problem, resulting behavior, tests, compatibility impact, and limitations. Include the issue's required evidence. Keep its project status current. Never publish secrets or customer data.

Constructive, respectful collaboration is expected. Address the work and evidence; do not tolerate harassment or discrimination. Report conduct concerns privately to the repository maintainer through GitHub. Report vulnerabilities using [the security policy](SECURITY.md).
