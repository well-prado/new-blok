# ADR 0021: Sensitive-data redaction boundaries and reliable audit

- Status: implementation in review for E16-T02 (#80)
- Date: 2026-10-05
- Roadmap: E16-T02 ([#80](https://github.com/well-prado/new-blok/issues/80));
  builds on ADR 0008 (durable approvals, #75), ADR 0016 (inspection, #76/#77),
  ADR 0020 (optional observability, #79) and the #49 journal retention,
  compaction, backup and restore
- Amended by: #281 (§7, erasure of run data), #286 (§8, the tenant of a
  reconciliation)
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
`reconcile:<operation>`, a fresh id per deployment decision) and a
deterministic time (the decision's own), so a retried decision produces the
identical record: appending it again is a no-op and reports that nothing was
inserted, and different content under the same ID is `ErrConflict`.

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
| Mandatory audit (`audit.Journal.Append`) | Written **inside the decision's own store transaction**. Decision and record commit together or not at all. Any store failure is `ErrUnavailable` (fixed message `audit: durable audit unavailable`; the cause is reachable only through `errors.Unwrap`), and the owner returns it, so the transaction rolls back: no decision row, no reconciliation, the effect stays uncertain, no plan. A full store (`MaxRecords`, hard 10,000,000) is `ErrCapacity`, which is also `ErrUnavailable` but reads `…: capacity exhausted`, because it needs provisioning or pruning rather than an outage response: audit is never silently discarded. `Stats().Refused` counts every refusal and `RefusedCapacity` the full-store subset. |
| Composition | `approval.NewJournalStore` and `journal.Reconcile`/`DecideUpgrade` require an audit journal that writes to **the same `store.Database`** (`Shares`); without it they return `ErrRequired`. |
| Optional copy (`Config.Mirror`) | Owners call `Notify` only after the commit returned and only for a record that was inserted, so an idempotent retry is not mirrored twice. `Offer` must not block; a refusal or a panic counts a drop (`Stats().MirrorDropped`). The mirror cannot delay, fail or remove a record. |
| Telemetry and the event stream | Unchanged (ADRs 0016, 0020): drop-and-count, never authoritative, never consulted by any audit or approval gate. |

A refused decision can be retried once the store recovers; the retry writes
the decision and its record exactly once. A re-delivered approval or
reconciliation whose record is missing (it predates audit) writes that
record. A re-delivered reconciliation whose record exists is a duplicate
and never rewrites it. A backfilled record, whether a re-delivery or
compaction (§7) writes it, takes the decision's own tenant, or the system
tenant `""` when that is unknown, never the tenant of whoever re-delivers
or compacts (§8).

### 3. Access

`audit.Journal.List(ctx, tenant, cursor, limit)` authorizes through the
application's `ReadAuthorizer` **before** reading; an unauthorized reader
gets `ErrDenied` and no records. Tenant is stamped from the trusted context
(`audit.WithTenant`) by the boundary that authenticated the operator. Every
returned record is checked against its stored sha256 digest and its indexed
columns. Pages are at most 200 records. The cursor is the tenant's own
sequence number, so it reveals nothing about other tenants' write volume.

Reads verify **integrity only** (digest, decodable shape, indexed columns,
canonical encoding). They never re-run `Validate`: the sensitivity rule and
bounds can widen between releases, and a record accepted under an older
rule must stay readable, listable and prunable rather than lock every
reader and Prune out and let the store fill.

Inspection authorization is unchanged: exact principal match (ADR 0016).
Tenant is not a second key there, so applications must use tenant-qualified
principals (`tenant-a/alice`); the evidence shows two tenants' same-named
users denied each other.

### 4. Retention

`audit.Journal.Prune(ctx, cutoff, activity)` deletes records committed
strictly before `cutoff`, in bounded batches (256 per transaction), leaves
a tombstone (`audit_pruned_v1`) holding the **sha256 of the record id** and
its kind (ids are application-chosen and may carry personal data), and
never:

- a record younger than `Config.MinRetention`, the application's legal
  minimum, whatever cutoff it is given;
- a record whose `RunID` the `RunActivity` reports active. The journal
  implements it (`ActiveRuns`) and fails closed: accepted and uncertain runs
  are active, and so is a run the journal does not know at all unless a
  compaction tombstone proves it ended, so an approval for a run held
  elsewhere (a cluster run) is never pruned on a guess. The check runs in
  the prune transaction, so a run cannot change state between check and
  delete. Without an activity port, Prune deletes nothing (`ErrRequired`);
- a record the application's `Config.Hold` keeps (legal hold). A panicking
  hold keeps the record.

Journal run compaction (#49, `journal.Compact`) already deletes only
completed runs; it now also honours `journal.Config.Hold` (a legal hold on
run data, reported as `HeldRuns`) and, since #281,
`journal.Config.MinRetention`. Compaction never deletes audit records; what
it erases is described in §7.

### 5. Backup and restore

Audit tables (`audit_records_v1`, `audit_meta_v1`, `audit_pruned_v1`) live
in the journal's database, so `journal.Backup` and `sqlite.Backend.Restore`
carry audit and durable state together, from the same point in time.

After a restore, `audit.Journal.Verify(ctx, owners...)` proves two things:

1. **Integrity** (`ErrCorrupt`): every record matches its digest, columns
   and canonical encoding, and the record count matches the counter.
2. **Agreement** (`ErrMismatch`), for each `audit.Owner` passed (the
   approval store and the journal implement it): every durable decision
   has its record or a prune tombstone of the same id digest and kind, and
   every record of the owner's
   kind names a decision the owner still has. This catches an audit table
   that was dropped and recreated empty, a deleted row whose counter was
   adjusted, and a restore that mixed audit and state from different
   points.

It does not prove agreement for deployment decisions, which have no owner
table, and it is not tamper evidence (see below). It holds one read
transaction and, per owner, the set of its decision ids in memory.

### 6. One redaction boundary

`observe/redact` is the single implementation; it builds on the
`contract/observe` credential scanner instead of duplicating it. The
scanner has two predicates, because redacting a projected copy and refusing
content are different decisions:

- **Broad** (`observe.SensitiveText`, `redact.Sensitive`), for redacting
  projected copies: any value after a sensitive key counts, except a plain
  number after an exact token-count name (`max_tokens: 256`). A number after
  any other token key (`access_token=48291736`) is redacted.
- **Strict** (`observe.CredentialText`, `redact.Credential`), for refusing
  content: an unconditional credential shape (listed below), or a value
  after a sensitive key or bearer scheme that has **at least 8 characters
  and both letters and digits**. That rule is deliberately narrow so tool
  descriptions such as `password: the new password` or `max_tokens: 256`
  are admitted, and it therefore misses letters-only, digits-only, uniform
  and short values, and `Bearer <letters>`. Refusal is best effort; the
  broad predicate still redacts all of these in every projected copy.

What is recognised:

- **Keys**: a key containing `password`, `passwd`, `pwd`, `passphrase`,
  `secret`, `token`, `authorization`, `credential`, `apikey`, `privatekey`,
  `cookie` or `sessionid` (case and `_` `-` `.` ignored) hides its whole
  value. Only the exact names of model-token counts and limits are exempt
  from `token` (`observe.TokenCountKey`: `max_tokens`, `total_tokens`,
  `prompt_tokens`, `completion_tokens`, `input_tokens`, `output_tokens`,
  `token_count`, `tokenLimit` and close equivalents, also as the last
  segment of a dotted path). Plural keys that hold tokens (`refreshTokens`,
  `accessTokens`, `csrfTokens`) stay sensitive.
- **Text**: `key=value` / `key: value` for those markers with any prefix or
  suffix (`client_secret=`, `access_token:`), quoted or JSON-escaped as in
  `body={\"password\":…}`; XML elements (`<password>…`); `Cookie:`,
  `Set-Cookie:`, `sessionid=`; `Bearer …`; `Basic <base64 user:password>`;
  AWS access key ids; JWT shapes; URL userinfo (`scheme://user:password@`);
  PEM private key headers; `ghp_`/`gho_`/`ghu_`/`ghs_`/`ghr_`,
  `github_pat_`, `sk-` and `xoxa-`/`xoxb-`/`xoxp-`/`xoxr-`/`xoxs-` tokens.
- **Encoded content**: a string is also inspected after decoding JSON inside
  it (keys included, so an escaped key such as `pass\u0077ord` is found when
  the string is JSON), percent-encoding (`+` as a space), and standard/URL
  base64 with or without padding (the whole string, any delimited token, and
  the value of a `key=value` token, padding included), up to three nested
  decodings. Base64 is decoded only for tokens of at least 12 bytes whose
  decoding is valid, mostly printable UTF-8.
- **Bounds**: decoding is attempted only on strings up to 16 KiB; longer
  strings are matched as written. Structural depth is bounded at 64. A
  projected payload larger than `min(2 × its slot, 128 KiB)` is truncated
  before it is decoded or redacted. Crafted payloads just under the cap
  were measured at 189–218 ms each in review (~1.8 ms per KiB); ordinary
  text costs ~16 µs per KiB. Work per page is bounded by about twice the
  response limit, so a crafted full page can take seconds.

Enforcement points (each covered by a mutation-tested check):

| Channel | Point |
| --- | --- |
| Inspection payloads (recorder, journal source, live stream capture) | `inspect` `bound`/`sanitizeRaw` → `redact.Value` |
| Inspection error labels | `projectLabel`: unsafe → `untrusted_label`, credential-shaped (an encoded label included) → `redacted_label`; also for labels a durable source supplies |
| Inspection log messages | at observation and again at projection → `redact.Message` |
| Worker log frames | `runtime/worker` key, value and message checks → `observe/redact` |
| Telemetry log bodies and allowlisted attributes | `observe/otel`: allowlisting selects a key; a sensitive key or credential-shaped value is still exported as `[redacted]` |
| Model-visible catalog listings | `agent` refuses registration (`ErrSensitiveListing`) when a description, schema literal (for example a `default`) or reviewed reference holds an actual credential value (strict predicate); prose that mentions a password or a token limit is admitted |
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
- Pattern matching is not secret detection. Not found: hex, encrypted or
  custom-encoded content; compressed content, including gzip inside base64
  (out of scope); four or more nested encodings; encoded content in strings
  over 16 KiB; unicode-escaped JSON that does not start the string
  (`body={"pass\u0077ord":…}`); a secret split across fields or messages; a
  secret in ordinary prose with no recognisable marker; a secret used as a
  map key name; provider token formats not listed above; numbers. The
  boundary cases in `observe/redact/testdata/cases.json` pin several of
  these as not found.
- The broad predicate over-redacts by design: prose such as
  `authorization: granted` or `token: optional`, or a key such as
  `tokenizer`, is redacted in projected copies. Business payloads, replay
  and digests are unaffected; only the projected copy changes. Refusal (the
  catalog, audit actors) uses the strict predicate, except that audit
  record fields refuse on the broad one, since a record has no prose.
- Digest verification detects partial restores and accidental corruption.
  It is not tamper evidence against someone who can write the database and
  recompute digests.

Applications keep secrets out of free-form text and out of node outputs,
use opaque secret references, and treat any custom export they add as
outside this guarantee.

### 7. Erasure of run data (#281)

Compaction is the journal's erasure. A run's content is everything an
application or provider handed the framework for it: request key,
principal, input, effect inputs and results, uncertainty and failure text,
scope inputs and outputs, checkpoint state, signal payloads, child and join
results, the output, and a reconciliation's evidence, provider result and
actor. It is kept exactly as long as the run is retained, and erased with
it.

`journal.Compact(ctx, before)` erases each completed run that finished
strictly before the cutoff, unless `Config.Hold` keeps it (a panicking hold
keeps it) or it finished less than `Config.MinRetention` ago (the cutoff is
clamped to `now − MinRetention`, as `audit.Prune` does). Failed, canceled,
uncertain and active runs are never compacted. For each erased run, in one
transaction:

1. Each reconciliation of the run whose audit record is missing (it
   predates audit) gets that record first, since the actor it needs is
   about to be erased. It takes the tenant stored with the reconciliation,
   or the system tenant `""` for a row from before #286 (§8). Without an audit
   journal composed nothing is written, and `Verify` reports the decision as
   it reports any pre-audit decision (#284).
2. Each reconciliation keeps its operation key, run id, tenant, state,
   creation time, `evidence_digest` and `result_digest` (the digests its audit
   record carries) and gains `erased_at`; actor, evidence and result are
   erased. The row stays, so `audit.Verify`'s cross-check still knows the
   decision existed, and the record's digests stay comparable to it.
   Its tenant (§8) is kept too, like its run id, so the deciding tenant can
   still be answered and nobody else can. A tenant is a label, but an
   application that uses per-person tenants keeps that person's tenant on
   every reconciliation it erases, forever; that is personal data outside
   erasure, within the scope of
   [#289](https://github.com/well-prado/new-blok/issues/289).
3. Attempts, then operations, then waits, signals, checkpoints, scopes,
   children and joins are deleted, then the run. The children-first order
   is what origin/main lacked: a reconciliation, a checkpoint or a scope
   made every compaction that reached such a run fail on a foreign key, so
   it could never be compacted. `journal_reconciliations` no longer has a
   foreign key to its operation, because the decision outlives it.
4. A tombstone (`journal_compacted`) is left: run id, `request_digest`,
   artifact, `input_digest`, `output_digest`, state, `completed_at`,
   `compacted_at`. It proves the run existed and ended (`ActiveRuns` reads
   it, so the run's audit records become prunable) and holds no content. The
   request key is digested because it is application-chosen and may carry
   personal data, as audit prune tombstones digest record ids.

A re-delivered reconciliation whose run was compacted is a duplicate that
returns `Duplicate` and `Erased` with the operation key and state only:
erased content is never returned again, to any caller or tenant. Before
erasure a duplicate returns the stored evidence and result, to the deciding
tenant only (§8).

**Bytes, not just rows.** Deleting a row only hides it. The SQLite backend
therefore opens every pooled connection with `secure_delete=ON` (ADR 0003),
which overwrites deleted content with zeros in the page, in freed cell
space and on freed and overflow pages. The write-ahead log still holds
older copies of every page written since it was last reset, so `Compact`
then calls the store's optional `store.Purger.PurgeLog`
(`PRAGMA wal_checkpoint(TRUNCATE)`). A reader's open snapshot keeps the log
in use, and a truncating checkpoint holds every writer while it waits for
readers, so the purge waits at most 100 ms (or the busy timeout, if
shorter) and then fails with `store.ErrBusy`, truncating nothing. The debt
is not forgotten: the compaction that erases content increments
`erasure_generation` in `journal_meta` in the same transaction, a
successful purge records the generation it covered as `purged_generation`,
and every later `Compact`, even one that removes nothing and even in a
restarted process, retries the purge while the first exceeds the second.
`CompactionReport.LogPurged` reports a purge in this pass and
`PurgePending` that erased content may still be in the log. Measured with a
reader held across a compaction: concurrent writers waited at most
103–108 ms (three runs), where they had waited the full 5 s busy timeout
and failed busy; the next compaction after the reader closed, in a freshly
opened journal, removed the content from the log
(`TestBlockedLogPurgeIsRetriedWithoutStallingWriters`). A reader that never
closes keeps the content in the log for as long as it stays open.

**Upgrading.** Opening a journal written before #281 migrates it inside the
schema transaction, so a crash leaves the old journal intact and the next
open migrates (shown with a killed process). Legacy tombstones
(`journal_audit`, which kept the output and request key) are rewritten as
digest-only tombstones (their unknown `input_digest` stays empty and
`compacted_at` 0) and the table is dropped. Legacy reconciliations are
rebuilt with their run id and digests, keeping their content while their run
is retained; one whose operation is gone is erased. Such an orphan cannot
exist in a journal origin/main wrote, because its foreign key forbade it,
so that branch is defensive and no test reaches it. Reopening a migrated
journal changes nothing. Content that origin/main deleted before secure
deletion existed stays in the file's free space, beyond the reach of any
row; one `store.Purger.PurgeFree` (`VACUUM`, then the log purge) removes it.
It rewrites the whole database, so it is the operator's explicit step after
upgrading, not part of opening.

**Backup and restore.** A backup is a copy, and erasure cannot reach a copy
it does not hold. What is guaranteed:

- A backup taken after a compaction holds none of the erased content
  (`Backup` writes a fresh file with `VACUUM INTO`: no free space, no log),
  and restoring it verifies.
- A backup taken before a compaction still holds the content. **Restoring
  it re-imports content that was erased after it was taken.** Nothing in
  the backup can know about a later erasure. What is guaranteed is that
  erasure is a function of the policy, not a remembered list: the next
  `Compact` with the same cutoff, hold and minimum erases again everything
  it erased before, and `audit.Verify` passes before and after.
- Applications with an erasure obligation therefore run `Compact` (and
  check `LogPurged`) on every restored database before serving it, and keep
  backups no longer than their erasure deadline allows. Erasing content
  inside an existing backup file is outside the framework.

### 8. The tenant of a reconciliation (#286)

A reconciliation belongs to the tenant that decided it: the tenant of the
deciding context (`audit.WithTenant`), which is now stored with it
(`journal_reconciliations.tenant`) as well as in its record. That tenant,
and only that tenant, may repeat the decision.

| Re-delivery under | Retained run | Compacted run | Writes |
| --- | --- | --- | --- |
| The deciding tenant | `Duplicate`, the original actor, evidence and result (never the re-delivery's own) | `Duplicate` + `Erased`, key and state only | a missing record, under the deciding tenant |
| Any other tenant, the system tenant `""` included | `ErrNotReconciliable`, an empty `Reconciliation` | not found, an empty `Reconciliation` | nothing |

**Why a refusal, not a bare duplicate.** ADR 0006's idempotency rule is
that a duplicate is the same key with the same identity; a key reused with
a different identity is never silently deduplicated. Another tenant's
re-delivery is not a retry of its own decision; it is a fresh attempt to
decide an operation that is already decided, and a successful
`Duplicate` would tell that tenant its evidence was accepted when it was
discarded. So the journal answers it exactly as it answers a fresh
reconciliation of an operation that settled without one: the operation is
looked up as if the reconciliation did not exist. The response is the same
error and the same empty value, field for field, as for such an operation
(`ErrNotReconciliable` while it is retained, and the same not-found error
once it is compacted), so it carries no evidence, no result, no actor, and
not even the fact that a reconciliation exists. What it still reveals is
what any authorized caller already learned before #286: that the operation
key exists and is no longer uncertain. Operations themselves are not
tenant-scoped, which is outside this decision. A refused re-delivery writes
nothing, as every refused decision writes nothing (§2); the deciding
tenant's next re-delivery, or compaction, writes a missing record.

Tenant comparison is exact. The system tenant `""` is not a wildcard here:
a system-context re-delivery of tenant A's reconciliation is refused like
any other. Application-wide reading goes through `audit.Journal.List` and
its `ReadAuthorizer` (§3), not through re-delivery.

**Rows from before #286, fixed once.** Such a row has no stored tenant
(`NULL`). Opening the journal gives it one, in the schema transaction:
the tenant of its audit record, which the original decision wrote under its
own tenant, read through `audit.StoredTenant`, which verifies the record
first; or the system tenant `""` when it has no record and no prune
tombstone (it predates audit, or audit was never composed on the
database). The tenant is written once and
never derived again, so ownership does not depend on audit retention: an
audit record that `Prune` later removes, while its run is still kept, does
not change who owns the decision, and a restart after the prune does not
either. Re-delivery reads only the stored column. Re-delivering a
reconciliation whose record was pruned writes that record again, under the
stored tenant, as any re-delivery backfills a missing record.

A row from before #286 with no audit record (it predates audit too) thus
belongs to the system tenant `""`, the tenant compaction files its
backfilled record under, so the journal and the audit agree on who owns it.
Its deciding tenant cannot be known. Only a system-context re-delivery
returns it; the deciding tenant's own re-delivery gets `ErrNotReconciliable`,
terminally, and so cannot recover through re-delivery. This is accepted
before alpha: such a row was written before audit was mandatory, and an
operator can still re-deliver it from the system context.

**A row still without a tenant is owned by nobody.** Three rows can have
none after an open:

- one whose audit record failed verification at that open: the row is left
  `NULL` rather than adopt an altered tenant, and `Verify` reports the
  record as `ErrCorrupt`;
- one whose audit record was pruned before that open, by an older binary
  before the upgrade or after it (#291): `audit.Pruned` finds its tombstone
  (the sha256 of `reconcile:<operation>`, kind `reconciliation.decision`),
  which proves the decision had a tenant that is now unknown, so it is not
  given the system tenant's; compaction does not re-create its record under
  `""` either, and `Verify` accepts the tombstone;
- one an older binary inserted afterwards, since there is no journal schema
  version to refuse it
  ([#291](https://github.com/well-prado/new-blok/issues/291)).

Every re-delivery of such a row, the deciding and the system tenant's
included, is answered as for a never-reconciled operation and writes
nothing. The next open fixes the last kind from its record, which any binary since #80
writes in the same transaction under the deciding tenant (one with no
record and no tombstone takes `""`); the first stays unowned until its
record verifies again, and the second stays unowned for good.

What an unowned row costs, and what an operator can do: the decision
itself stands. The reconciliation committed its operation, with the
provider result, in the same transaction, so the run continues and replays
from the committed operation; only a re-delivery, which is an idempotent
retry of the decision, can no longer be answered with the original. Its
content is kept, unread, until compaction erases it with its run (§7). No
API reassigns a tenant. An operator who knows the deciding tenant from its
own records can set it directly, with the journal closed
(`UPDATE journal_reconciliations SET tenant = ? WHERE operation_key = ? AND
tenant IS NULL`); that is outside the framework, which does not verify the
choice. To avoid the state, open the journal with this release before
running `audit.Journal.Prune` on an upgraded database, and do not prune
with an older binary afterwards.

**Migration.** Opening a journal adds the nullable `tenant` column inside
the schema transaction, after the #281 migration, and then fixes every row
without a tenant as above, with the same pattern: idempotent (the column is
added only if absent, only rows without a tenant are touched, and reopening
changes nothing), retried while concurrent openers race (#235), and rolled
back whole by a crash. A journal rebuilt by the #281 migration gets the
column from the rebuilt table. Every open scans the reconciliations for rows
without a tenant; reconciliations are operator decisions, so the table is
small.

## Compatibility

Pre-alpha. Classified per surface:

- **Breaking (pre-alpha)**: `approval.NewJournalStore` requires
  `Config.Audit` on the same database and returns `audit.ErrRequired`
  otherwise. `journal.Reconcile` requires `journal.Config.Audit`. Every
  caller in the repository is updated. Existing decision rows are not
  rewritten and receive no backfilled record; a decision recorded before
  this change still authorizes; an idempotent retry of it, or a
  re-delivery of a pre-audit reconciliation, writes its missing record.
  Until then `Verify` with owners reports `ErrMismatch` for such a store.
  `audit.Journal.Append` returns whether it inserted.
- **Behaviour tightening**: more content is redacted in inspection, worker
  logs and telemetry (the widened shared pattern and the encoded layer);
  projected error labels may read `redacted_label`; oversized projected
  payloads are truncated earlier; catalog registration refuses listings
  holding a credential value.
- **Additive**: `contract/audit`, `observe/redact`,
  `observe.SensitiveText`, `observe.CredentialText`, `observe.LooksSecret`, `journal.DecideUpgrade`, `journal.ActiveRuns`,
  `journal.Config.Hold`, `journal.CompactionReport.HeldRuns`,
  `agent.ErrSensitiveListing`. New tables are created on open; no existing
  table changes. The `inspection/v1` wire shape is unchanged.

Erasure (#281), classified separately:

- **Behaviour change, with data migration**: opening a journal rewrites
  `journal_audit` into `journal_compacted` and drops it, and rebuilds
  `journal_reconciliations` without its foreign key, adding `run_id`,
  `evidence_digest`, `result_digest` and `erased_at` (`evidence` and
  `result_json` become nullable). Both happen in the schema transaction,
  once, and are not reversible: an older binary does not understand the
  new tables (it recreates an empty `journal_audit` and would write content
  into it again), so downgrade by restoring a pre-upgrade backup.
- **Behaviour change**: `Compact` now also deletes a run's waits, signals,
  checkpoints, scopes, children and joins, erases its reconciliations'
  content, and compacts runs it used to fail on; it purges the store's log
  afterwards. A duplicate reconciliation of a compacted run returns empty
  actor, evidence and result with `Erased`. The SQLite backend deletes
  securely on every connection (ADR 0003).
- **Breaking (internal package)**: `journal.CompactionReport.RetainedAudit`
  is `Tombstones` and `Journal.AuditCount` is `TombstoneCount`; neither is
  public API.
- **Additive**: `journal.Config.MinRetention`,
  `CompactionReport.ErasedReconciliations`, `LogPurged` and `PurgePending`,
  the `journal_meta` table (created on open),
  `Reconciliation.Erased`, and the optional `store.Purger` capability
  (`PurgeLog`, `PurgeFree`, `store.PurgerOf`).

The tenant of a reconciliation (#286), classified separately:

- **Behaviour change (breaking, pre-alpha)**: a re-delivered reconciliation
  under a tenant other than the deciding one, the system tenant included,
  is refused (`ErrNotReconciliable`, or not found once compacted) with an
  empty `Reconciliation` and nothing written, where it returned `Duplicate`
  with the original evidence and result (and, after compaction,
  `Duplicate` + `Erased`) and backfilled a missing record under its own
  tenant. A reconciliation from before #286 with no audit record is
  answered only to the system tenant. The deciding tenant's re-delivery is
  unchanged.
- **Behaviour change**: compaction backfills a missing record under the
  stored tenant; only rows from before #286 still take `""`.
- **Schema change, with data migration**: `journal_reconciliations` gains a
  nullable `tenant` column on open, and every row without one is given its
  verified record's tenant, or `""`, in the same transaction; not
  reversible, but an older binary ignores the column (#291) and a row it
  inserts is fixed on the next open.
- **Additive**: `audit.StoredTenant`, `audit.Pruned`.

## Evidence

- `contract/audit/testdata/cases.json`: 15 predeclared cases (11 refusals)
  run against fresh SQLite stores, each declaring the error, decision,
  audit, reconciliation, committed/uncertain operation, authorization, plan
  and mirror counts, including audit unavailable (a SQLite trigger aborting
  every audit insert) before approval, reconciliation and deployment
  decisions, a full audit store, credential-shaped actors (plain and
  base64) and missing composition. No audit row may contain the synthetic
  secret marker.
- `contract/audit/review_test.go`: a record accepted under an older rule
  stays verifiable, listable and prunable; Verify's cross-check refuses
  dropped-and-recreated tables, a deleted row with an adjusted counter, a
  missing reconciliation record and a record without its decision, and
  accepts prune tombstones; strict cutoff, uncertain, unknown and compacted
  runs, panicking holds in audit and compaction; reconciliation backfill;
  no duplicate mirroring; capacity versus outage; per-tenant cursors.
- `contract/audit/integration_test.go`: recovery after an outage, optional
  mirror drop and mirror panic with every durable record intact, retention
  around active runs, legal hold and legal minimum, run compaction under
  legal hold, backup → more decisions → restore with audit and durable state
  agreeing and later decisions absent from both, tampered restored audit
  detected, two-tenant audit reads, pagination.
- `observe/redact/testdata/cases.json`: 41 cases including JSON-in-string,
  escaped keys, percent-encoding (`+` as space), base64 (std, URL alphabet,
  double, triple, padded inside `key=value` and inside a percent-encoded
  query value), Basic auth, URL userinfo, PEM, cookie text, `pwd` and
  passphrase keys, provider token prefixes, JSON-escaped prose, XML, token
  counts kept, plural token keys and digits after token keys redacted, plus negative and boundary cases (hex, unmarked prose, four
  encodings, unicode-escaped JSON after a prefix, gzip+base64).
- `testdata/inspection/redaction/cases.json` through the real engine and the
  SQLite journal source: secret-shaped inputs, outputs, error codes and
  logs, field-policy subsets, and two-principal / two-tenant denial.
- Telemetry (`observe/otel/redaction_test.go`, real OTLP/HTTP exporter),
  worker log frames, catalog listings (refused and admitted), and the
  projection size gate (`inspect/bound_internal_test.go`).
- 52 deliberate mutations, each shown red; commands and results are in the
  PR.
- Erasure (#281), against real SQLite files: `contract/audit/erasure_test.go`
  (a compacted run, a reconciled run, legal hold, and restore of backups
  taken before and after erasure, each byte-searching the database file,
  its log and its shared-memory index for distinct synthetic markers, with
  `Verify` passing), `erasure_migration_test.go` (a journal written by
  origin/main at `ea3eec6`,
  `testdata/restore/erasure-281/legacy-main-ea3eec6.db.gz`, migrated,
  reopened unchanged, and killed inside the migration transaction),
  `erasure_limits_test.go` (legal minimum, the legacy free-space residue
  and `PurgeFree`, a reader blocking the log purge, the pre-audit record
  backfill), and `store/sqlite/erasure_test.go` (secure deletion on every
  pooled connection, a control without it that keeps the content, purge
  refusing beside a reader). The tests are red on origin/main; mutations
  are listed in the PR.
- Tenant of a reconciliation (#286), against real SQLite files:
  `contract/audit/reconcile_tenant_test.go` (another tenant's re-delivery of
  a pre-audit and of a recorded reconciliation compared field for field and
  error for error with a never-reconciled operation, before and after
  compaction, with no record written; the deciding tenant's duplicate
  unchanged under `""` and under a named tenant; compaction run in another
  tenant's context backfilling under the deciding tenant; a row with no
  known tenant answered only to `""`; a journal written by origin/main at
  `99a9228`, `testdata/restore/reconcile-tenant-286/legacy-main-99a9228.db.gz`,
  migrated with each row given its record's tenant, answered by it before
  and after its records are pruned, and reopened unchanged; the same
  fixture with a record's tenant column altered, whose row stays unowned; a
  reconciliation whose record is pruned while its run is kept, before and
  after a restart; a row an older binary writes after the migration, owned
  by nobody until the next open; a record pruned before the upgrade, whose
  row stays unowned, also after compaction). The tests are red on
  origin/main, and
  the pruning case on the first revision of this change; mutations are
  listed in the PR.

## Limits

- Journaled and cluster runs on `store/distributed` (ADR 0019) have no audit
  integration and no `RunActivity` of their own. Prune fails closed for
  them: the journal's activity port reports a run it does not hold as
  active, so such a record is kept until the application composes an
  activity port that can prove the run ended.
- An upgraded database's decisions recorded before audit existed are
  reported by `Verify` as `ErrMismatch`, not as legacy; an audit start
  marker is
  [#284](https://github.com/well-prado/new-blok/issues/284).
- Erasure (§7) is retention-driven only: there is no API to erase one
  run or one subject's runs on request before their cutoff. Failed,
  canceled and uncertain runs are never compacted, and a parent's copy of a
  child's result is kept with the parent, so that content is kept
  ([#289](https://github.com/well-prado/new-blok/issues/289)).
- Worker queue payloads (`worker_jobs`) are kept forever; they are outside
  the journal's compaction
  ([#290](https://github.com/well-prado/new-blok/issues/290)).
- The journal has no schema version, so an older binary opening a migrated
  journal is not refused
  ([#291](https://github.com/well-prado/new-blok/issues/291)).
- Erasure stops at SQLite's files. The filesystem, an SSD's remapped
  blocks, VACUUM's temporary file, OS caches, copies an application made
  and backups taken before an erasure are outside it (§7). Compaction runs
  in one transaction however many runs it erases.
- An erased reconciliation keeps its operation key, run id, state, time
  and digests forever, and its audit record keeps the actor until the
  record is pruned. A digest of low-entropy content (a short result such
  as `{"ok":true}`) can be confirmed by guessing; digests are
  pseudonymous, not anonymous.
- Secure deletion applies to every delete and update in the shared
  database (journal, audit, worker queue, cron, approvals). Measured in
  review over 500 operations, three interleaved samples each, without and
  with it: worker queue 295–342 vs 301–397 ms, journal 89–112 vs
  87–121 ms, `Compact` 27–29 vs 31–33 ms (about +12%). Worker and journal
  costs are within noise; compaction pays for the zeroing.
- Prune tombstones (`audit_pruned_v1`) keep a pruned record's id digest
  and kind forever, so Verify can tell pruned from missing.
- No hash chain or external anchoring of audit records.
- The live event stream (`observe/event`, `inspect/events*.go`) was not
  reshaped: it gains the new redaction through the shared `inspect`
  projection only.
- A reconciliation written before #286 whose record was missing and was
  backfilled by an earlier re-delivery took that re-delivery's tenant
  (the bug #286 fixes); nothing can tell such a record from the original,
  so it keeps answering to that tenant.
- A tenant-less row whose audit record fails verification stays unowned,
  answered to nobody, until the record is repaired; nothing repairs it. One
  whose record was pruned before the upgrade stays unowned for good, unless
  an operator sets its tenant by hand (§8).
- Journal operations are not tenant-scoped: any authorized caller can learn
  whether an operation key exists and is uncertain, and can reconcile an
  uncertain operation of any tenant. #286 scopes the reconciliation, not
  the operation.
- Windows is vetted (`GOOS=windows go vet`) but not executed.
