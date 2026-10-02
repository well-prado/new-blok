# ADR 0002: Release quality gates and validation matrix

- Status: accepted for implementation planning
- Date: 2026-10-02
- Roadmap: E01-T04 (#24)

## Validation matrix

Go 1.27.1 is the initial toolchain. The current matrix is intentionally
explicit:

| OS / architecture | Status | Evidence |
| --- | --- | --- |
| Ubuntu `linux/amd64` | Required CI gate | GitHub Actions `go` job |
| Linux `linux/arm64` | Local validation | Go 1.27.1 Docker image on the development host |
| macOS `darwin/arm64` | Development host only; no release claim | Host tooling availability is checked per task |
| Other OS/architectures | Unsupported until a matrix issue adds evidence | No CI or release guarantee |

The repository's supported baseline is Go 1.27.1 on the required Linux CI
target. A green test on one architecture does not establish portability.

## Required pull-request gates

The `go` job runs on every pull request and every push to `main`. It is not
conditional on changed paths. It runs formatting, module verification, vet,
the full race suite, the bounded contract fuzz smoke, graph conformance tests
and build. A failed step fails the job; there is no allowed-success path for a
skipped required step. GitHub branch protection was verified on 2026-10-02:
`main` requires the strict `go` check, enforces administrators and requires a
pull request; the supported plan currently requires zero approvals.

Generated drift is dormant until generated artifacts exist. When a generator
is introduced, its owning issue must add a deterministic `generate` followed by
a clean-tree check. Dependency/license scanning is currently limited to
`go mod verify` because the module has no third-party dependencies; adding the
first dependency requires a pinned scanner and license policy before merge.

## Fuzz, crash and load policy

The current CI fuzz smoke uses a fixed command and bounded five-second budget:

```sh
go test ./contract -run=^$ -fuzz=FuzzParseBounded -fuzztime=5s -parallel=1
```

Fuzz seeds and fixtures are synthetic. A crash suite and load suite do not yet
exist; their owning implementation issue must add a reproducible seed/workload,
bounded duration and resource budget before the related capability can be
called release-tested. No performance claim is made from the current smoke.

## Benchmark regression policy

Benchmark comparisons must use a pinned Go/toolchain and runner image, fixed
fixtures and seeds, warmup, at least five measured samples, and a documented
noise policy. Compare distributions (including p50/p95/p99 where applicable),
not one fastest sample. A regression gate must record CPU, memory, topology,
configuration and saturation/failure behavior; it may not gate on an
uncontrolled wall-clock threshold. Until a representative benchmark exists,
there is no benchmark pass or throughput claim.

## Release and credential policy

Pull requests run with read-only repository contents and no release
credentials. Fork pull requests must not receive secrets or write tokens.
Release signing, publishing and deployment credentials are restricted to
explicit release workflows and are not needed by CI validation. The quality
fixtures at `testdata/quality/fixtures.json` remain synthetic and declare
expected output/error/effect counts, including negative cases.

