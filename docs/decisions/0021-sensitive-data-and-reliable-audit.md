# ADR 0021: Sensitive-data redaction boundaries and reliable audit

- Status: implementation in review for E16-T02 (#80)
- Date: 2026-10-05
- Roadmap: E16-T02 ([#80](https://github.com/well-prado/new-blok/issues/80));
  builds on ADR 0008 (durable approvals, #75), ADR 0016 (inspection, #76/#77),
  ADR 0020 (optional observability, #79) and the #49 journal retention,
  compaction, backup and restore
- Amends: [ADR 0008](0008-durable-tool-policy.md) (approval decisions now
  require audit), [ADR 0016](0016-versioned-inspection.md) (projection
  redaction and error labels), [ADR 0020](0020-optional-observability-export.md)
  (allowlisted log attributes stay opaque)
- Owners: audit contract (`contract/audit`), redaction boundary
  (`observe/redact`); the enforcement points named below stay with their
  packages

## Context

Two different things were both being called "audit". Telemetry and the
development event stream are allowed to drop and sample under pressure
(ADRs 0016, 0020); that is the right policy for them and the wrong one for
the record of who approved a payment, who decided an uncertain effect's
outcome, or who allowed a deployment to discard in-flight work. A record
like that has to exist exactly when the decision exists.

Separately, redaction was duplicated: inspection had a sensitive-key list,
the worker had another key list and a substring list for messages, and
telemetry used the `contract/observe` credential pattern. None looked inside
encoded content, so a credential wrapped in base64, percent-encoding or a
JSON string sailed through all three.

## Decision

### 1. The audit contract

`contract/audit.Record` is one immutable fact: `ID`, `Kind`
(`approval.decision`, `reconciliation.decision`, `deployment.decision`),
optional `Tenant`, the authenticated `Actor`, the `Subject` decided, optional
`RunID` and `Action`, the `Outcome` (`approved`, `rejected`, `applied`,
`accepted`, `refused`), an optional `Reason` label, up to 16 named sha256
`Digests`, up to 64 opaque `Refs` (scope and capability names), and `At`.

A record never holds content. Evidence text, provider results, inputs and
outputs are bound by digest only. Every string field is refused
(`ErrSensitive`) if `observe/redact` finds it credential-shaped, plainly or
encoded, so a record is safe to show to an authorized reader and to a model.
Records are at most 16 KiB.

The owning operation chooses a deterministic ID (`approval:<decision>`,
`reconcile:<operation>`, a fresh id per deployment decision). The same
record appended again is a no-op; different content under the same ID is
`ErrConflict`.

| Decision | Record | Actor | Digests / refs |
| --- | --- | --- | --- |
| `approval.JournalStore.Record` | `approval.decision`, approved or rejected | reviewer from `ReviewAuthorizer` | proposal, input, artifact, tool; granted scope names |
| `journal.Reconcile` | `reconciliation.decision`, applied | the reconciling operator | evidence and result digests |
| `journal.DecideUpgrade` (new) | `deployment.decision`, accepted or refused (`would_discard_accepted_work`) | the deciding operator | from/to artifact digests when canonical |

`journal.PlanUpgrade` stays an unaudited, read-only preview; only
`DecideUpgrade` is a decision.

### 2. Reliability policy: mandatory audit is not telemetry

| Path | Policy |
| --- | --- |
| Mandatory audit (`audit.Journal.Append`) | Written **inside the decision's own store transaction**. Decision and record commit together or not at all. Any store failure is `ErrUnavailable` (fixed message; the cause is reachable only through `errors.Unwrap`), and the owner returns it, so the transaction rolls back: no decision row, no reconciliation, the effect stays uncertain, no plan. A full store (`MaxRecords`, hard 10,000,000) is `ErrCapacity`, which is also `ErrUnavailable`: audit is never silently discarded. |
| Composition | `approval.NewJournalStore` and `journal.Reconcile`/`DecideUpgrade` require an audit journal that writes to **the same `store.Database`** (`Shares`); without it they return `ErrRequired`. |
| Optional copy (`Config.Mirror`) | `Notify` offers records to the mirror only after the commit returned. `Offer` must not block; a refusal or a panic counts a drop (`Stats().MirrorDropped`). The mirror cannot delay, fail or remove a record. |
| Telemetry and the event stream | Unchanged (ADRs 0016, 0020): drop-and-count, never authoritative, never consulted by any audit or approval gate. |

A refused decision can be retried once the store recovers; the retry writes
the decision and its record exactly once.

### 3. Access

`audit.Journal.List(ctx, tenant, cursor, limit)` authorizes through the
application's `ReadAuthorizer` **before** reading; an unauthorized reader
gets `ErrDenied` and no records. Tenant is stamped from the trusted context
(`audit.WithTenant`) by the boundary that authenticated the operator. Every
returned record is checked against its stored sha256 digest and its indexed
columns. Pages are at most 200 records.

Inspection authorization is unchanged: exact principal match (ADR 0016).
Tenant is not a second key there, so applications must use tenant-qualified
principals (`tenant-a/alice`); the evidence shows two tenants' same-named
users denied each other.

### 4. Retention

`audit.Journal.Prune(ctx, cutoff, activity)` deletes records committed
before `cutoff`, in bounded batches (256 per transaction), and never:

- a record younger than `Config.MinRetention`, the application's legal
  minimum, whatever cutoff it is given;
- a record whose `RunID` the `RunActivity` reports active. The journal
  implements it (`ActiveRuns`): accepted and uncertain runs are active. The
  check runs in the prune transaction, so a run cannot change state between
  check and delete. Without an activity port, Prune deletes nothing
  (`ErrRequired`);
- a record the application's `Config.Hold` keeps (legal hold). A panicking
  hold keeps the record.

Journal run compaction (#49, `journal.Compact`) already deletes only
completed runs; it now also honours `journal.Config.Hold` (a legal hold on
run data, reported as `HeldRuns`). Compaction never touches audit records.

### 5. Backup and restore

Audit tables (`audit_records_v1`, `audit_meta_v1`) live in the journal's
database, so `journal.Backup` and `sqlite.Backend.Restore` carry audit and
durable state together, from the same point in time. After a restore,
`audit.Journal.Verify` re-checks every record's digest, columns and the
record count and fails with `ErrCorrupt` otherwise.

### 6. One redaction boundary

`observe/redact` is the single implementation; it builds on the
`contract/observe` credential pattern (`observe.SensitiveText`, which this
decision widens) instead of duplicating it.

- **Keys**: a key containing `password`, `passwd`, `secret`, `token`,
  `authorization`, `credential`, `apikey`, `privatekey`, `cookie` or
  `sessionid` (case and `_` `-` `.` ignored) hides its whole value.
- **Text**: `key=value` / `key: value` for those markers with any prefix or
  suffix (`client_secret=`, `access_token:`) and quoted as in JSON text;
  `Bearer …`; `Basic <base64 user:password>`; AWS access key ids; JWT
  shapes; URL userinfo (`scheme://user:password@`); PEM private key headers.
- **Encoded content**: a string is also inspected after decoding JSON inside
  it (keys included, so an escaped key such as `password` is found),
  percent-encoding, and standard/URL base64 with or without padding (the
  whole string or any delimited token, including a `key=value` value), up to
  three nested decodings. Base64 is decoded only for tokens of at least 12
  bytes whose decoding is valid, mostly printable UTF-8.
- **Bounds**: decoding is attempted only on strings up to 16 KiB; longer
  strings are matched as written. Structural depth is bounded at 64.

Enforcement points (each covered by a mutation-tested check):

| Channel | Point |
| --- | --- |
| Inspection payloads (recorder, journal source, live stream capture) | `inspect` `bound`/`sanitizeRaw` → `redact.Value` |
| Inspection error labels | `projectLabel`: unsafe → `untrusted_label`, credential-shaped (an encoded label included) → `redacted_label`; also for labels a durable source supplies |
| Inspection log messages | at observation and again at projection → `redact.Message` |
| Worker log frames | `runtime/worker` key, value and message checks → `observe/redact` |
| Telemetry log bodies and allowlisted attributes | `observe/otel`: allowlisting selects a key; a sensitive key or credential-shaped value is still exported as `[redacted]` |
| Model-visible catalog listings | `agent` refuses registration (`ErrSensitiveListing`) when a description, schema literal (for example a `default`) or reviewed reference is credential-shaped |
| Audit records | `Record.Validate` refuses (`ErrSensitive`) |

Secrets reach nodes as opaque reference names (ADRs 0010, 0011); listings do
not expose secret reference names at all.

### What the framework does not guarantee (native-code leakage)

Native nodes are trusted application code and share the application's
process (ADR 0001, AGENTS.md). Redaction covers only the channels the
framework owns, listed above. It cannot stop code from leaking what it is
given:

- A node that receives a resolved credential or customer data can send it
  anywhere: its own network calls, files, process stdout/stderr (never
  attributed to a run, never redacted), a third-party SDK's logging, or a
  returned value encoded in a way the patterns do not recognise.
- Worker processes are not sandboxed by manifests or by gRPC (ADR 0004);
  they can do the same.
- Pattern matching is not secret detection. Not found: hex, compressed,
  encrypted or custom-encoded content; four or more nested encodings;
  encoded content in strings over 16 KiB; a secret split across fields or
  messages; a secret in ordinary prose with no recognisable marker; a
  secret used as a map key name; numbers. The boundary cases in
  `observe/redact/testdata/cases.json` pin several of these as not found.
- It over-redacts by design: a key such as `maxTokens` or prose such as
  `tokens: 5` is redacted. Business payloads are unaffected; only the
  projected copy changes.
- Digest verification detects partial restores and accidental corruption.
  It is not tamper evidence against someone who can write the database and
  recompute digests.

Applications keep secrets out of free-form text and out of node outputs,
use opaque secret references, and treat any custom export they add as
outside this guarantee.

## Compatibility

Pre-alpha. Classified per surface:

- **Breaking (pre-alpha)**: `approval.NewJournalStore` requires
  `Config.Audit` on the same database and returns `audit.ErrRequired`
  otherwise. `journal.Reconcile` requires `journal.Config.Audit`. Every
  caller in the repository is updated. Existing decision rows are not
  rewritten and receive no backfilled record; a decision recorded before
  this change still authorizes, and an idempotent retry of it writes its
  missing record.
- **Behaviour tightening**: more content is redacted in inspection, worker
  logs and telemetry (the widened shared pattern and the encoded layer);
  projected error labels may read `redacted_label`; catalog registration
  refuses credential-shaped listings.
- **Additive**: `contract/audit`, `observe/redact`,
  `observe.SensitiveText`, `journal.DecideUpgrade`, `journal.ActiveRuns`,
  `journal.Config.Hold`, `journal.CompactionReport.HeldRuns`,
  `agent.ErrSensitiveListing`. New tables are created on open; no existing
  table changes. The `inspection/v1` wire shape is unchanged.

## Evidence

- `contract/audit/testdata/cases.json`: 15 predeclared cases (11 refusals)
  run against fresh SQLite stores, each declaring the error, decision,
  audit, reconciliation, committed/uncertain operation, authorization, plan
  and mirror counts, including audit unavailable (a SQLite trigger aborting
  every audit insert) before approval, reconciliation and deployment
  decisions, a full audit store, credential-shaped actors (plain and
  base64) and missing composition. No audit row may contain the synthetic
  secret marker.
- `contract/audit/integration_test.go`: recovery after an outage, optional
  mirror drop and mirror panic with every durable record intact, retention
  around active runs, legal hold and legal minimum, run compaction under
  legal hold, backup → more decisions → restore with audit and durable state
  agreeing and later decisions absent from both, tampered restored audit
  detected, two-tenant audit reads, pagination.
- `observe/redact/testdata/cases.json`: 24 cases including JSON-in-string,
  escaped keys, percent-encoding, base64 (std, URL, double, inside a query
  value), Basic auth, URL userinfo, PEM, plus negative and boundary cases.
- `testdata/inspection/redaction/cases.json` through the real engine and the
  SQLite journal source: secret-shaped inputs, outputs, error codes and
  logs, field-policy subsets, and two-principal / two-tenant denial.
- Telemetry (`observe/otel/redaction_test.go`, real OTLP/HTTP exporter),
  worker log frames, and catalog listings.
- 22 deliberate mutations, each shown red; commands and results are in the
  PR.

## Limits

- Journaled and cluster runs on `store/distributed` (ADR 0019) have no audit
  integration and no `RunActivity`; Prune's activity port answers only for
  runs the journal holds. A record whose run lives elsewhere is treated as
  inactive unless the application composes an activity port for it.
- The compaction tombstone (`journal_audit`, #49) still stores a compacted
  run's output. It is a retention record of durable state, not this audit
  contract, and no read API exposes it.
- No hash chain or external anchoring of audit records.
- The live event stream (`observe/event`, `inspect/events*.go`) was not
  reshaped: it gains the new redaction through the shared `inspect`
  projection only.
- Windows is vetted (`GOOS=windows go vet`) but not executed.
