# ADR 0022: Operational SLO metrics, alert semantics and monitoring integrations

- Status: implementation in review for E16-T03 (#105)
- Date: 2026-10-05
- Roadmap: E16-T03 ([#105](https://github.com/well-prado/new-blok/issues/105));
  builds on [ADR 0020](0020-optional-observability-export.md) (optional
  export, `observe/otel`), [ADR 0021](0021-sensitive-data-and-reliable-audit.md)
  (redaction and audit), [ADR 0016](0016-versioned-inspection.md)
  (inspection events), [ADR 0019](0019-durable-distributed-execution.md)
  (admission slots, partitions, fencing), [ADR 0006](0006-durable-submission-and-webhooks.md)
  (worker queue) and [ADR 0009](0009-deployment-readiness.md) (readiness and
  `/metrics`, #81)
- Owners: operational port and catalogue (`observe/slo`), exporter
  (`observe/otel`), state hooks (`app/deploy`, `trigger/worker`,
  `internal/cluster`, `internal/runtime`, `internal/engine`), examples
  (`examples/monitoring`)

## Context

ADR 0020 exports what the engine observes: runs and steps. An operator who is
paged needs more than that, and needs it to mean one thing: is the deployment
ready, is admission saturated, how deep and how old is the queued work, is a
timer late, is a worker reachable, is storage growing towards its limit, and
is the telemetry itself being lost. Most of these are states that only the
owning component knows (the limiter, the queue table, the partition leases),
not events. And the most expensive mistake in this domain is a page for a run
that is merely waiting for a human to approve it, or a silent queue whose
owner died. This decision fixes the vocabulary, the names and the alert
semantics, and keeps all of it optional.

## Decision

### Two production paths, one catalogue

| Path | What | Where it comes from | Who renders it |
| --- | --- | --- | --- |
| Snapshot | state: readiness, admission, work census, timers, workers, partitions, storage | `slo.Source` functions owned by the component that knows the state | `app/deploy` `/metrics` (standard library), and `observe/otel` over OTLP when `Config.Operational` is set |
| Events | completion, steps, external calls, latency, telemetry loss | engine observation events (ADR 0016) | `observe/otel` only |

`observe/slo` (root module, standard library plus `contract/observe`) holds
the port (`Snapshot`, `Source`, `Sampler`), the liveness classification, the
catalogue (`Catalogue()`, the single source of truth for names, units, labels
and bounds) and `WriteText`, a Prometheus text 0.0.4 renderer. The catalogue is
generated into `examples/monitoring/catalogue.json` and checked for drift.

A `Sampler` runs every source on its own goroutine bounded by a timeout
(default 1 s, at most 30 s); a source still running from the previous sample is
skipped rather than started again, so a stuck source costs one goroutine. A
failing, timed-out, panicking, invalid or conflicting source contributes
nothing and is counted in `blok.operational.sample.failures`. Sampling never
fails a scrape, a collection or a run. In `observe/otel` the sampler runs on
the metric reader's goroutine once per collection, never on a run's.

### Admission, completion, steps and external calls are four counts

One accepted request completes one run of many steps, some of which call out
of the process. Collapsing them hides overload behind fast steps or slow
providers behind healthy admission, so each has its own instrument:

| Counts | Instrument | Meaning |
| --- | --- | --- |
| admission | `blok.admission.requests{blok.result, blok.reason}` | requests offered at the edge; `accepted`/`none` or `rejected` by `capacity`, `draining`, `not_ready`, `unavailable`, `invalid`, `conflict` |
| completion | `blok.runs{blok.workflow, blok.outcome, blok.tenant}`, `blok.run.duration` | runs reaching an outcome |
| steps | `blok.steps{…, blok.error.class}`, `blok.step.duration` | step attempts |
| external calls | `blok.external.calls{…}`, `blok.external.call.duration` | attempts of steps whose node declares effects (`node.Effects`) |

The engine marks the step events of an effectful node with the in-process
field `inspection.Event.External` (`json:"-"`, like `Tenant` and `Trace`).
Outcome vocabularies keep `uncertain` apart from `failed` everywhere.

### Liveness is the alert contract

Every census reports unfinished work through `slo.Classify`, in this order:

| Liveness | Rule | Alert |
| --- | --- | --- |
| `uncertain` | a dispatched effect has no committed result | ticket: reconcile, never retry blindly; never counted as stalled or as an error |
| `waiting` | suspended on a signal or timer, or delayed by a retry backoff | never alerts; an overdue timer shows up as timer lag instead |
| `stalled` | not waiting, not uncertain, and the owner it needs is gone (claim lease expired, partition without a live owner) | page after 2 minutes |
| `active` | claimed under a live owner | none |
| `pending` | admitted, waiting for an owner that exists | backlog age warns after 5 minutes over 300 s |

Uncertain wins over waiting and a missing owner because an operator who reads
"stalled" restarts things, and an uncertain effect must not be re-dispatched.
Waiting never needs an owner, so a dead owner cannot make it stalled; the
unowned partition itself pages once (`BlokPartitionUnowned`), whatever it
holds. A normal takeover leaves a partition unowned for less than the owner
TTL plus the acquire interval; the alerts' `for: 2m` absorbs it.

Severities: `page` (stalled work, unowned partition, unavailable worker,
not-ready deployment that is not draining, timer lag over 60 s), `warn`
(run error ratio over 5 %, admission saturation over 90 %, shedding, backlog,
latency, storage forecast), `ticket` (uncertain outcomes, telemetry loss,
failing sources, truncated census). Thresholds marked "example" in the rule
files are application choices; the semantics are not.

### State hooks (the smallest observation each owner can give)

| Component | Hook | Reports |
| --- | --- | --- |
| `app/deploy` | `Deployment.Operational()`, `DeploymentChecks.Operational` | readiness, each readiness dependency (secrets aggregated, never named), admission slots in use and configured, accepted and rejected requests by reason; `/metrics` renders it plus any extra sources |
| `trigger/worker` | `Queue.Census`, `Queue.CensusSource` | one read-only aggregate over `worker_jobs`: ready pending (`pending`, oldest age), backoff pending (`waiting`), processing under a live lease (`active`), processing under an expired lease (`stalled`), dead letters |
| `internal/cluster` | `Runtime.Census`, `Runtime.CensusSource` | per partition: the live owner, the overdue part of the due-timer index (count and lag), and up to `readLimit` (at most 4096) run records from the admission-slot index; `Truncated` when the bound is hit |
| `internal/runtime` (`runtime/worker.Supervisor`) | `Supervisor.Availability`, `AvailabilitySource` | lifecycle readiness, calls in flight, concurrent-call capacity |
| any store | `slo.FileStorage` | bytes of the named files (for example a SQLite database and its WAL) and the declared budget |

All of them only read. None claims, renews, writes or takes a claim turn.

### Exporter loss is counted, never retried into a run

`blok.telemetry.dropped{reason}` (ADR 0020) gains `metric_exports_failed`.
A failed metric export cannot report itself, but metrics are cumulative: the
next successful export carries the count, so a collector outage is visible
once it ends (`BlokTelemetryLoss`). Source failures are
`blok.operational.sample.failures`. The collector's own `otelcol_*`
self-telemetry (enabled in the example config) is the collector side of the
same loss. Telemetry loss is a ticket, never a page: runs are unaffected by
construction (ADR 0020).

### Metric catalogue

Units are UCUM as OpenTelemetry records them; the Prometheus name follows the
example collector config's `translation_strategy: UnderscoreEscapingWithSuffixes`
(dots to underscores, a unit in braces dropped, `s` adds `_seconds`, `By`
adds `_bytes`, counters add `_total`). Label keys are `blok.x` in OTLP and
`blok_x` in Prometheus, except the pre-existing `reason`. Bound is the most
attribute sets per instrument; "app" means bounded by application-declared
names and capped by `observe/otel`'s `MaxSeries` (default 1000, hard 10000,
plus one overflow set).

| OTel name | Prometheus | Kind | Unit | Labels | Bound |
| --- | --- | --- | --- | --- | --- |
| `blok.ready` | `blok_ready` | gauge | | | 1 |
| `blok.draining` | `blok_draining` | gauge | | | 1 |
| `blok.dependency.ready` | `blok_dependency_ready` | gauge | | dependency (≤16) | 16 |
| `blok.admission.active` | `blok_admission_active` | gauge | `{request}` | | 1 |
| `blok.admission.capacity` | `blok_admission_capacity` | gauge | `{request}` | | 1 |
| `blok.admission.requests` | `blok_admission_requests_total` | counter | `{request}` | result (2), reason (7) | 14 |
| `blok.work.items` | `blok_work_items` | gauge | `{item}` | source (≤16), liveness (5) | 80 |
| `blok.work.oldest_pending.age` | `blok_work_oldest_pending_age_seconds` | gauge | `s` | source | 16 |
| `blok.work.dead_letters` | `blok_work_dead_letters` | gauge | `{item}` | source | 16 |
| `blok.census.truncated` | `blok_census_truncated` | gauge | | source | 16 |
| `blok.timers.overdue` | `blok_timers_overdue` | gauge | `{timer}` | source | 16 |
| `blok.timer.lag` | `blok_timer_lag_seconds` | gauge | `s` | source | 16 |
| `blok.worker.ready` | `blok_worker_ready` | gauge | | worker (≤16) | 16 |
| `blok.worker.in_flight` | `blok_worker_in_flight` | gauge | `{call}` | worker | 16 |
| `blok.worker.capacity` | `blok_worker_capacity` | gauge | `{call}` | worker | 16 |
| `blok.partitions` | `blok_partitions` | gauge | `{partition}` | owned (2) | 2 |
| `blok.storage.used` | `blok_storage_used_bytes` | gauge | `By` | store (≤16) | 16 |
| `blok.storage.budget` | `blok_storage_budget_bytes` | gauge | `By` | store | 16 |
| `blok.operational.sample.failures` | `blok_operational_sample_failures_total` | counter | `{sample}` | | 1 |
| `blok.runs` | `blok_runs_total` | counter | `{run}` | workflow, outcome (5), tenant (65) | app |
| `blok.run.duration` | `blok_run_duration_seconds` | histogram | `s` | workflow, outcome, tenant | app |
| `blok.steps` | `blok_steps_total` | counter | `{step}` | workflow, step, outcome (4), error.class | app |
| `blok.step.duration` | `blok_step_duration_seconds` | histogram | `s` | workflow, step, outcome | app |
| `blok.external.calls` | `blok_external_calls_total` | counter | `{call}` | workflow, step, outcome (4), error.class | app |
| `blok.external.call.duration` | `blok_external_call_duration_seconds` | histogram | `s` | workflow, step, outcome | app |
| `blok.telemetry.dropped` | `blok_telemetry_dropped_total` | counter | `{event}` | reason (8) | 8 |

Duration histograms use the explicit second boundaries
0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
60 and 300 (`slo.DurationBuckets`): 17 buckets, so one attribute set is 19
Prometheus series. The previous SDK default boundaries were tuned for
milliseconds and put every run under five seconds in one bucket.

### Tenant cardinality and retention budgets

The tenant is a label only on `blok.runs` and `blok.run.duration`, and only as
an allowlisted value (at most 64) or `other`; never on steps, external calls
or snapshot metrics. Per instance, the series are bounded by:

- snapshot: at most about 300 series (the bounds above, all names capped at 16);
- events: `runs ≤ W × 5 × (T+1)`, `run.duration ≤ 19 × W × 5 × (T+1)`,
  `steps ≤ S × 4 × C`, `step.duration ≤ 19 × S × 4`, `external ≤ E × 4 × C`,
  `external.duration ≤ 19 × E × 4`, each attribute-set count capped by
  `MaxSeries`; `W` workflows, `S` steps, `E` effectful steps, `T` allowlisted
  tenants, `C` distinct error classes plus one.

Worked example: 10 workflows, 50 steps of which 20 are effectful, 3 error
classes, 8 allowlisted tenants gives about 450 + 8,550 + 800 + 3,800 + 320 +
1,520 + 8 ≈ 15,400 event series plus 300 snapshot series per instance.

Retention cost in a Prometheus-compatible TSDB is about
`series × (86400 / scrape_seconds) × days × bytes_per_sample`, with 1 to
2 bytes per compressed sample. At a 15 s scrape that example is about 90 to
180 MB per instance per day, 1.4 to 2.7 GB for 15 days. Budget guidance:

- keep `TenantLabels` to the tenants someone alerts on individually (eight or
  fewer is a good default); the run-duration histogram multiplies tenants by 19;
- drop `blok.tenant` from histograms at the collector (the core
  `attributes` processor can delete it) when per-tenant latency is not needed;
- keep raw data 15 days and the recording rules (`blok:*`, one series per
  group) for long-term retention: they are small and they are what the
  alerts read;
- `MaxSeries` protects the exporter, not the TSDB: size it so that
  `instruments × MaxSeries × 19` fits the budget.

### Monitoring stack: optional, separate, vendor-neutral

Nothing in this decision adds a requirement to the root `go.mod`.
`observe/slo` links only the standard library; `app/deploy`,
`trigger/worker`, `internal/cluster` and `runtime/worker` link no telemetry
SDK (`TestNoOptInFootprint`, `go list -deps -test`). OTLP export stays in the
separate `observe/otel` module (ADR 0020). `examples/monitoring` ships an
OpenTelemetry Collector config (core distribution components only:
`otlp`, `memory_limiter`, `batch`, `prometheus`, `debug`, `health_check`),
Prometheus recording and alerting rules, and a Grafana-compatible dashboard.
The application may run Prometheus against `/metrics`, a collector against
OTLP, both, or neither.

### Cloud consumer contract

BLOK Cloud is a separate product (ADR 0001). Its framework contract is the
same one any operator uses, and it requires nothing proprietary at runtime:

- formats: Prometheus text exposition 0.0.4 on `/metrics`, OTLP/HTTP protobuf
  from `observe/otel`, and the `/readyz` JSON status (ADR 0009);
- names, units, labels and bounds: `examples/monitoring/catalogue.json`
  (machine-readable, generated, drift-checked);
- semantics: the liveness vocabulary and severities above, and the rule
  files as a reference implementation;
- the framework sends nothing anywhere by itself: no agent, SDK, endpoint,
  credential or account is required, and a consumer may not require a label
  or endpoint outside the catalogue.

### Compatibility classification

- Additive API: package `observe/slo`; `inspection.Event.External`
  (in-process only, no inspection/v1 wire change); `deploy.DeploymentChecks.Operational`,
  `Deployment.Operational`; `worker.Queue.Census`, `CensusSource`;
  `Supervisor.Availability`, `AvailabilitySource`; `cluster.Runtime.Census`,
  `CensusSource` (internal); `otel.Config.Operational`, `SampleTimeout`,
  `Stats.SampleFailures`, `otel.MetricExternalCalls`, `MetricExternalDuration`.
- Behavioral, `/metrics`: the content type gains `charset=utf-8`; the body gains
  HELP/TYPE comments and the catalogued snapshot families. `blok_ready` and
  `blok_draining` keep their names and values; `blok_active` and
  `blok_admission_rejected_total` are kept unchanged as deprecated aliases of
  `blok_admission_active` and the rejected `blok_admission_requests_total`.
- Behavioral, `observe/otel`: duration histograms use second boundaries
  instead of the SDK default; `blok.telemetry.dropped` gains
  `metric_exports_failed`; three new instruments. Dashboards built on the old
  `le` values must be updated.
- Catalogue evolution: adding a metric or a closed label value is additive and
  needs a cardinality review here; renaming or removing a metric, label or
  value, or changing a unit, is breaking for consumers and requires a new
  decision.
- No protocol, journal, store schema or persisted byte changes.

## Evidence

- Liveness, bounds, sampler isolation and the catalogue: `observe/slo` tests
  (classification table, refused unbounded labels, stuck/panicking/failing/
  invalid/conflicting sources, exposition equals catalogue, drift check,
  standard-library-only footprint, no-opt-in footprint).
- Synthetic scenarios with predeclared deltas in
  `examples/monitoring/testdata/scenarios.json`, each produced by real
  components: overload (`app/deploy`, real limiter), worker death (SQLite
  queue, a worker process killed with SIGKILL holding a lease), retry backoff
  (waiting, the negative), cluster owner death, a waiting run whose owner died
  and timer lag (real etcd 3.6.5, owners dying by lease expiry), slow store,
  uncertain-versus-failed, errors, and collector outage (`observe/otel`,
  in-process OTLP receiver and an actual collector running the example config).
- Rules and queries: `examples/monitoring` parses both rule files, checks
  every series against the catalogue, replays each recorded fixture and
  asserts the declared alerts fire and the declared silent ones never do,
  including that no undeclared page fires. `internal/tooling/promrule` is the
  evaluator; its own tests pin the PromQL semantics it implements.
- Collector: the example config passes `otelcol validate` (0.162.0) and a
  broken copy fails; the collector's Prometheus exporter exposes snapshot
  metrics with exactly the series `slo.WriteText` renders.
- Overhead: `TestMeasureMode` modes `off`, `metrics` and `slo`; figures in the PR.

## Limits

- No `promtool` or Prometheus runs here. The rules are validated by
  `internal/tooling/promrule`, a documented PromQL subset (selectors,
  `rate`/`increase`/`delta` with Prometheus extrapolation, `deriv`,
  `predict_linear`, `histogram_quantile`, `*_over_time` min/max, aggregations,
  one-to-one matching, `and`/`or`/`unless`, `for`). It refuses what it does not
  implement; it is not Prometheus and can disagree with it at edge cases.
- Recorded fixtures are real expositions taken before and after each fault;
  rule evaluation replays the measured interval as a steady state over a
  synthetic 30-minute timeline, and the timer-lag census is read with a clock
  90 s after the timer was due (the store state is real).
- Journaled and cluster runs emit no observation events (ADR 0016, 0019), so
  their outcomes, including uncertain ones, are not in `blok.runs`; the
  cluster census reports only unfinished runs. Their uncertain outcomes are
  visible through inspection (#263). The SQLite journal (`internal/journal`)
  and the cron trigger have no census in this slice.
- Stall detection is ownership-based. A step that hangs under a live owner is
  visible as latency, not as a stall.
- Worker availability is lifecycle readiness (ADR 0009): it does not probe the
  transport.
- The cluster census costs three linearizable reads per partition plus one per
  active run up to its read limit, per sample; choose the scrape interval and
  limit accordingly.
- The dashboard is checked for query syntax and catalogued names only; it was
  not rendered.
- Collector translation was verified with otel/opentelemetry-collector 0.162.0
  only. Windows is vetted (`GOOS=windows go vet`) but not executed; no Windows
  claim is made. Overhead figures are one developer machine's.
