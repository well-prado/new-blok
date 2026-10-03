# ADR 0012: Catalog null normalization and template expansion bounds

- Status: implemented in the #61 correction branch; integration pending
- Date: 2026-10-02
- Roadmap: E10-T01 ([#61](https://github.com/well-prado/new-blok/issues/61))

## Correction

The merged pure catalog's direct handler tests did not exercise engine schema
validation. Its generic value union rejected valid objects/scalars and explicit
null. Template rendering checked the output limit after allocating the expanded
string. These are historical #61 defects, not new runtime/retention work.

## Canonical schema behavior

`type: "null"` accepts only null. An explicit `nullable: true` continues to
accept null. Otherwise unions evaluate null through their branches, just as
other values, and still require exactly one match. Zero matches and multiple
matches fail with `compatibility_unproven`; duplicate null branches and
overlapping integer/number branches are not relaxed.

The catalog generic value envelope uses one number branch, which carries exact
JSON integers as `json.Number` without floating-point conversion. The nested
schema supplied to `catalog/validate` still enforces int64 range and explicit
`int64-string` wire encoding. Supported top-level arrays remain string arrays;
this correction does not advertise arbitrary array schemas.

## Template budget

Input templates and output are at most 65,536 bytes, and the values collection
has at most 1,024 entries. A preflight pass visits borrowed literal slices and
replacement values and checks the remaining byte budget before accounting for
each part. It stops on overflow or a missing value without allocating expanded
output. Only a successful preflight allocates a builder of the exact output
size. Rendering is nonrecursive: placeholder text in a replacement stays text.
The byte limit includes UTF-8 bytes, not characters.

## Compatibility and migration

This is a corrective pre-alpha contract change. A null schema no longer
silently converts non-null values to null; callers relying on that behavior
must supply null or declare the actual type. Valid null/object/scalar catalog
calls now execute through normal engine validation. Ambiguous unions remain
invalid. The catalog envelope descriptor changes, so consumers must recompute
schema/catalog/artifact identities; a matching version string is insufficient.

The Node SDK is absent from this main-based branch. The parent must port the
same normalizer order and null type rejection to `sdk/nodejs/schema.ts`, remove
the comment preserving the old Go null bug, and run the shared schema corpus
plus actual worker conformance. Do not publish Go/Node parity until that port
passes. No #52 merge or Node SDK implementation is claimed here.

## Executable evidence

- `testdata/conformance/schema-corpus.json`: accepted null/string/object unions,
  rejected non-null/null schemas, no-match and ambiguous unions. The canonical
  golden runner executes every case; Node consumers use the same corpus.
- `testdata/catalog/normalization-fixtures.json`: expected output/error/effect
  counts for real engine validation, including defaults, exact int64 extrema,
  wire strings, null, string arrays, field errors and unsupported arrays.
- `catalog/workflow_test.go`: real workflow input/output validation and state
  publication, map output with exact integer/null, template byte boundaries,
  repeated-placeholder overflow, missing keys and nonrecursive replacement.
- The amplification allocation regression uses inputs that would expand to
  8 MiB and requires rejection allocations below 256 KiB, including regexp
  scanning scratch under race instrumentation. It measures rejection,
  not application performance. The larger repeated-placeholder engine test
  would expand to approximately 859 MB if rendered first.
- `FuzzNullUnionNormalization`: only null satisfies the null schema, the
  string/null union is stable, and ambiguous null unions always fail.

## Local validation

Docker image `golang:1.27.1`, reported `go1.27.1 linux/arm64`, `--cpus 2`,
`GOMAXPROCS=2`, `GOTOOLCHAIN=local`, `GOFLAGS=-mod=readonly`, network disabled.
Source and `new-blok-m4-gomod` were mounted read-only; the reusable
`new-blok-m4-gocache` build/fuzz cache was writable. No GitHub Actions ran.

Passed on 2026-10-02:

```sh
go test -count=1 ./catalog ./contract/schema ./flowtest
go vet ./...
go test -race -count=1 ./...
go build ./...
go test ./contract/schema -run=^$ -fuzz=^FuzzNullUnionNormalization$ -fuzztime=10s -parallel=2
go test ./contract/schema -run=^$ -fuzz=^FuzzNormalizeBounded$ -fuzztime=10s -parallel=2
go test ./catalog -run=^$ -fuzz=^FuzzTemplateBounded$ -fuzztime=10s -parallel=2
git diff --check
```

Fuzz runs completed 217,773, 232,482 and 83,861 executions respectively,
without failures. The first race run exposed an overly narrow 64 KiB
allocation-test threshold: regexp scratch used 157,798 bytes. The corrected
256 KiB scanning budget passed the full race run; the renderer still allocates
no expanded output until preflight succeeds. No Node or mixed-runtime gate is
claimed on this branch. Independent review and parent-owned Node port remain
required before integrated completion.
