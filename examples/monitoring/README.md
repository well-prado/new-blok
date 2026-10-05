# Monitoring examples

Vendor-neutral examples for the operational SLO catalogue of
[ADR 0022](../../docs/decisions/0022-operational-slo-metrics.md). Everything
here is optional: an application that uses none of it pays nothing.

| File | What it is |
| --- | --- |
| `catalogue.json` | Generated from `observe/slo.Catalogue()`: every metric's OTLP and Prometheus name, unit, labels and series bound. Do not edit; `go test ./observe/slo -run TestCatalogueFileHasNoDrift -update`. |
| `otel-collector.yaml` | OpenTelemetry Collector (core distribution) config: OTLP/HTTP in from `observe/otel`, Prometheus exposition out on `:8889`. |
| `prometheus/recording.rules.yaml` | `blok:*` recording rules: admission, completion, steps and external calls kept apart. |
| `prometheus/alerting.rules.yaml` | Alerts. Stalled work pages; waiting work never alerts; uncertain outcomes are tickets. |
| `dashboard.json` | A Grafana-compatible dashboard over the recording rules. |
| `testdata/scenarios.json` | Synthetic scenarios with predeclared metric deltas and alert outcomes. |
| `testdata/recorded/*.prom` | Expositions recorded before and after each scenario's fault by the test that produced it. |

Two ways to collect, usable together or alone:

- scrape the application's `/metrics` (`app/deploy`, standard library) for
  readiness, admission and every `slo.Source` the application composes;
- export OTLP with `observe/otel` (set `Config.Operational` to the same
  sources) and scrape the collector's `:8889`, which adds completion, step,
  external-call, latency and telemetry-loss metrics. Scrape the collector with
  `honor_labels: true` so `job` stays the exporting service.

`go test ./examples/monitoring` parses both rule files, checks every series
against the catalogue and evaluates the rules on each recorded scenario.
Re-record with `BLOK_RECORD_FIXTURES=1` on the producing tests named in
`scenarios.json`; the event-path recordings need
`BLOK_OTEL_COLLECTOR_IMAGE=otel/opentelemetry-collector:0.162.0`, the cluster
ones `BLOK_DISTRIBUTED_ENDPOINTS`.
