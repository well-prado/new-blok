# ADR 0021: Sensitive-data redaction boundaries and reliable audit

- Status: implementation in review for E16-T02 (#80)
- Date: 2026-10-05
- Roadmap: E16-T02 ([#80](https://github.com/well-prado/new-blok/issues/80));
  builds on ADR 0008 (durable approvals, #75), ADR 0016 (inspection, #76/#77),
  ADR 0020 (optional observability, #79) and the #49 journal retention,
  compaction, backup and restore
- Amended by: #281 (§7, erasure of run data), #286 (§8, the tenant of a
  reconciliation), #291 (§9, schema versions), #284 (§10, the audit start
  marker)
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
inserted, and different content under the same ID is `ErrConflict`. An ID
`Prune` retired (§4) is never written again: appending a record of the same
kind under it is the same no-op, whether or not the record passes today's
validation, and one of another kind is `ErrConflict` (#294). A decision made
for the first time must write its record, so an owner that finds nothing
inserted there (its record id already exists or has a tombstone, which only
a partial restore or a hand edit leaves) refuses the decision with
`ErrConflict` and applies nothing.

| Decision | Record | Actor | Digests / refs |
| --- | --- | --- | --- |
| `approval.JournalStore.Record` | `approval.decision`, approved or rejected | reviewer from `ReviewAuthorizer` | proposal, input, artifact, tool; granted scope names |
| `journal.Reconcile` | `reconciliation.decision`, applied | the reconciling operator | evidence and result digests |
| `journal.DecideUpgrade` (new) | `deployment.decision`, accepted or refused (`would_discard_accepted_work`) | the deciding operator | from/to artifact digests when canonical |

`journal.PlanUpgrade` stays an unaudited, read-only preview; only
`DecideUpgrade` is a decision.

Both count the same runs (#340): every run on the old artifact that is not
`completed`, `failed` or `canceled`. That is an `accepted` run, including one
suspended on a wait (a wait does not change the run's state); an `uncertain`
run, whose effect outcome awaits reconciliation; and a run in any state this
binary does not know, so the count fails closed. While that count is above
zero, an upgrade that discards runs is refused (`ErrUpgradeWouldDiscard`,
recorded as `would_discard_accepted_work`, a label kept for compatibility)
and one that retains them reports them as `AffectedRuns`. `CancelRun` does not
make room: it refuses a run with an uncertain effect (`ErrUncertain`) or an
active wait (`ErrRunActiveWork`). No journal call moves a run out of
`uncertain` (reconciliation decides the effect, not the run), so an uncertain
run keeps its artifact retained.

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
and never rewrites it. A record `Prune` removed (§4) is not missing in this
sense: its tombstone says it existed and retention ended it, so a
re-delivered reconciliation, a retried approval or a compaction backfill
answers as before and writes nothing (#294), even when the record was
accepted under an older sensitivity rule that today's `Validate` refuses.
A backfilled record, whether a re-delivery or compaction (§7) writes it, takes the decision's own tenant, or the system
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
its kind (ids are application-chosen and may carry personal data), which
also retires the id: `Append` never writes it again (§1, #294). It never
deletes:

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

Audit tables (`audit_records_v1`, `audit_meta_v1`, `audit_pruned_v1`, and
since #284 `audit_start_v1` and `audit_legacy_v1`) live in the journal's
database, so `journal.Backup` and `sqlite.Backend.Restore`
carry audit and durable state together, from the same point in time.

After a restore, `audit.Journal.Verify(ctx, owners...)` proves two things:

1. **Integrity** (`ErrCorrupt`): every record matches its digest, columns
   and canonical encoding, the record count matches the counter, and no
   record has a prune tombstone of its own id digest. `Append` never
   writes a pruned id again, so a record next to its tombstone was written
   around retention: by a binary before #294, which re-created a pruned
   record on re-delivery, or by hand. The error names that record's id,
   and a tombstone of any kind counts, as `Append` refuses another kind
   under a pruned id too. The ids are checked in batches of 256, one
   indexed lookup per batch (Limits gives the cost). Every audit start
   marker matches its legacy list (§10).
2. **Agreement** (`ErrMismatch`), for each `audit.Owner` passed (the
   approval store and the journal implement it): every durable decision
   has its record, a prune tombstone of the same id digest and kind, or a
   place on its kind's legacy list (§10: recorded before audit existed on
   the database, reported as legacy, not as a mismatch), and
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
   journal composed nothing is written, and `Verify` accepts the decision
   only if the start marker listed it as legacy (§10).
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

**The worker queue (#290).** A worker job's payload, principal, trace
context, error text and request key are erased the same way, by
`worker.Queue.Compact`: digest-only tombstones that keep the queue's dedupe
contract, a legal hold that fails closed, a legal minimum, bounded batches,
and the same pending log purge. Its design and dedupe window are in ADR 0006
("Retention of finished jobs").

### 8. The tenant of a reconciliation (#286)

A reconciliation belongs to the tenant that decided it: the tenant of the
deciding context (`audit.WithTenant`), which is now stored with it
(`journal_reconciliations.tenant`) as well as in its record. That tenant,
and only that tenant, may repeat the decision.

| Re-delivery under | Retained run | Compacted run | Writes |
| --- | --- | --- | --- |
| The deciding tenant | `Duplicate`, the original actor, evidence and result (never the re-delivery's own) | `Duplicate` + `Erased`, key and state only | a missing record, under the deciding tenant; never one `Prune` removed (#294) |
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
reconciliation whose record was pruned returns the same duplicate as before
the prune and writes nothing: the prune tombstone retired the record's id
(§4, #294). Until #294 such a re-delivery wrote the record again, with its
original time, so the record existed again after retention had removed it,
until the next `Prune` removed it again.

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
  before the upgrade or by a pre-#291 binary after it: `audit.Pruned` finds its tombstone
  (the sha256 of `reconcile:<operation>`, kind `reconciliation.decision`),
  which proves the decision had a tenant that is now unknown, so it is not
  given the system tenant's; compaction does not re-create its record under
  `""` either, and `Verify` accepts the tombstone;
- one a binary built before #291 inserted afterwards: such a binary has no
  schema version check. Since #291 a binary that predates #286 is refused
  at open (§9), so only binaries built before the stamp existed can still
  do this.

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
A row left unowned also costs a binary whose audit support is older than
the database's audit stamp: every journal-only open it makes is refused
for as long as the row exists, because the repair would have to read
audit to retry it (#321, ADR 0003 "Cross-component reads").

**Migration.** Opening a journal adds the nullable `tenant` column inside
the schema transaction, after the #281 migration, and then fixes every row
without a tenant as above, with the same pattern: idempotent (the column is
added only if absent, only rows without a tenant are touched, and reopening
changes nothing), retried while concurrent openers race (#235), and rolled
back whole by a crash. A journal rebuilt by the #281 migration gets the
column from the rebuilt table. Every open scans the reconciliations for rows
without a tenant; reconciliations are operator decisions, so the table is
small.

### 9. Schema versions (#291)

The journal, audit, approval and worker queue tables are stamped with
their schema version in `blok_schema_versions`, one row per component, in
the same transaction as their migration; the contract, the versions and
their history are in ADR 0003 ("Schema versions"). For this decision it
means:

- A journal migrated by #281 or #286 (stamp `journal` 3), an audit store
  (`audit` 1; 2 since #284, §10), approvals (`approval` 1) and a queue with tombstones
  (`worker` 2) are refused at open, with `store.NewerSchemaError` naming
  the component and both versions, by any binary from #291 on that
  supports an older version. Such a binary can therefore no longer insert
  an untenanted reconciliation (§8) or recreate the legacy tombstone table
  (§7), and an older worker can no longer accept a duplicate of a compacted
  job (ADR 0006). A later release that changes one of these shapes raises
  its version, and this release refuses it in turn.
- A database written before #291 has no stamp. Its journal and queue are
  classified by shape (journal 1 before #281, 2 after #281, 3 after #286;
  queue 1 before #290, 2 after it), migrated as before, and stamped as
  upgraded from that version. Shown with databases written by origin/main
  at `ea3eec6`, `99a9228`, `8633027` and `ef330a3`.
- The #281 and #286 migrations keep their shape guards and still run on
  every open, so a binary built before #291, which cannot see the stamp,
  is still repaired after: the legacy tombstone table it recreates is
  rewritten, an untenanted row it inserts is given its record's tenant.

### 10. The audit start marker (#284)

Audit became mandatory with #80, and `v0.1.0-alpha` shipped it. A database
that already held approval decisions or reconciliations then has decisions
that never had a record, and before #284 `Verify` reported every one of
them as `ErrMismatch`, forever: the only repair was to retry each old
decision, which needs its original proposal and grant. An operator taught
to ignore `ErrMismatch` would also ignore it when it means tampering. The
start marker draws the line once: decisions recorded before audit existed
on the database are **legacy**, counted and reported separately; every
later decision must have its record.

**The line is a list, not a time or a position.** Each owner kind
(`approval.decision`, `reconciliation.decision`) has one marker row in
`audit_start_v1`. Opening the audit store with a binary from #284 on, when
it is created (audit schema 0) or upgraded from audit schema 1, opens a
marker for each kind, in the schema transaction that stamps audit 2
(ADR 0003). The owner's first open composed with audit
(`approval.NewJournalStore`, `journal.New` with `Config.Audit`) calls
`audit.Journal.Start`, which, in one write transaction, lists each of the
owner's decisions that has neither a record nor a prune tombstone in
`audit_legacy_v1` (the sha256 of its record id, as a prune tombstone keeps
it, since ids may carry personal data) and closes the marker with the
list's size and digest (sha256 of the kind and the sorted id digests, one
per line). No decision can commit between the listing and the marker. A
legacy list needs no ordering of decisions, so it cannot be confused by
clock skew, by a clock that went backwards, or by owner tables whose row
order changed (the #281 rebuild of `journal_reconciliations`).

- A fresh database: the markers close when the owners first open, with
  nothing listed. Every decision needs its record.
- A database written before #280: the markers list every decision; `Verify`
  passes and reports them as legacy.
- A database that went through `v0.1.0-alpha` (audit 1, no marker): the
  markers list the decisions without a record, which are the ones recorded
  before audit; the alpha binary's own decisions have their records and
  are matched.
- A legacy decision whose record is written later (an idempotent retry, a
  re-delivery, or compaction's backfill, §7) is matched by its record and
  no longer counted as legacy; it stays on the list.

`audit.Journal.VerifyReport` returns the record count, the legacy count
per owner kind passed, and every closed marker (`Marker{Legacy, Digest}`);
`Verify` keeps its signature and returns the record count.

**Written once.** `Start` never rewrites a closed marker. A store already
at audit 2 never opens a marker again, so removing the marker rows, or
dropping and recreating the marker tables, does not let the next open list
whatever decisions lack records by then: that kind has no marker, nothing
is legacy, and every unrecorded decision is `ErrMismatch`.

**Tamper evidence, and its limits.** `Verify` recomputes each closed
marker's list size and digest: a decision added to the list (moving the
line forward to hide a decision whose record was removed), a list or count
edited on its own, or a list left behind without its marker is
`ErrCorrupt`; removing a marker and its list turns its legacy decisions
into `ErrMismatch`. This is the same evidence as the record digests (§6):
it detects partial restores, accidental damage and a rewrite that does not
know the scheme. Someone who can write the database and recompute a
sha256 can rewrite a marker and its list consistently, or reopen a marker
by hand (`started = 0`) so that the next open lists again; inside the
database neither is detectable. The closed markers in the report never
change once written, so an operator who keeps them outside the database
detects both (`TestMovingTheStartMarkerForwardIsDetected` pins the hand-
reopened marker: it verifies, with one more legacy decision and a
different digest). A legacy decision is accepted on the strength of the
marker alone: on a database that ran `v0.1.0-alpha`, a decision whose
record was removed before this release first opened it is indistinguishable
from a pre-audit decision and is listed as legacy.

## Compatibility

Pre-alpha. Classified per surface:

- **Breaking (pre-alpha)**: `approval.NewJournalStore` requires
  `Config.Audit` on the same database and returns `audit.ErrRequired`
  otherwise. `journal.Reconcile` requires `journal.Config.Audit`. Every
  caller in the repository is updated. Existing decision rows are not
  rewritten and receive no backfilled record; a decision recorded before
  this change still authorizes; an idempotent retry of it, or a
  re-delivery of a pre-audit reconciliation, writes its missing record.
  Until #284 `Verify` with owners reported `ErrMismatch` for such a
  store; since #284 it reports them as legacy (§10).
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
  reversible. A binary built before #291 ignores the column and a row it
  inserts is fixed on the next open; from #291 on, a binary that predates
  the column is refused at open (§9).
- **Additive**: `audit.StoredTenant`, `audit.Pruned`.

Schema versions (#291), classified separately (ADR 0003 has the full
table):

- **Behaviour change (breaking for downgrades)**: a binary from #291 on
  refuses a database stamped newer than it supports
  (`store.ErrNewerSchema`), where an older binary used to open it and fail
  later or misbehave. Downgrade by restoring a pre-upgrade backup, as §7
  already required.
- **Schema change, additive**: `blok_schema_versions`, created and filled
  on the first open by #291, in each component's schema transaction; an
  unstamped database is classified by shape. No existing table changes.
- **Additive**: `store.ErrNewerSchema`, `store.NewerSchemaError`.

Pruned records stay pruned (#294), classified separately:

- **Behaviour change**: `Append` of a record whose id has a prune
  tombstone writes nothing (a no-op, `inserted` false, for the same kind;
  `ErrConflict` for another kind). A re-delivered reconciliation, a
  retried approval and a compaction backfill of a pruned record therefore
  answer as before and no longer write, mirror or count it again. The
  tombstone is checked before `Validate`, so a pruned record accepted under
  an older sensitivity rule does not turn that answer into
  `ErrSensitive`.
- **Behaviour change (stricter)**: a first approval or reconciliation
  whose `Append` inserts nothing (its id already has a record or a
  tombstone) is `ErrConflict`, with nothing applied, written or mirrored.
- **Behaviour change (stricter)**: `Verify` returns `ErrCorrupt` for a
  record that has a prune tombstone of its own id. A store in which a
  binary before #294, `v0.1.0-alpha` included, re-created a pruned record
  fails `Verify`, naming the record's id, until the next `Prune` removes
  it (Limits).
- No schema change.

The audit start marker (#284), classified separately:

- **Behaviour change**: `Verify` accepts a decision without a record when
  its kind's start marker lists it, and reports it as legacy
  (`VerifyReport`), where it returned `ErrMismatch`. A decision recorded
  after the marker without its record is still `ErrMismatch`. A marker
  that disagrees with its list, or a list without a marker, is
  `ErrCorrupt`.
- **Schema change, additive, with a version raise**: `audit_start_v1` and
  `audit_legacy_v1` are created on open; audit schema 2. A binary from #291
  on that supports audit 1 (`v0.1.0-alpha` included) refuses a database
  this release opened (`store.NewerSchemaError`): it would ignore the
  marker and report every legacy decision as `ErrMismatch` again, and a
  marker it never saw could not stay write-once. Downgrade by restoring a
  pre-upgrade backup, as for every raise.
- **Behaviour change**: `approval.NewJournalStore` and `journal.New` with
  `Config.Audit` close their kind's marker on their first open, in one
  extra write transaction; later opens read one row.
- **Additive**: `audit.Journal.Start`, `audit.Journal.VerifyReport`,
  `audit.Report`, `audit.Marker`.

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

- Schema versions (#291), against real SQLite files:
  `internal/migration/components_test.go` (every component stamps a fresh
  database, refuses one stamped newer with the component and both
  versions named and nothing written, migrates an older stamp forward;
  databases written by origin/main at `ef330a3`, `ea3eec6`, `99a9228` and
  `8633027` classified by shape, stamped and reopened unchanged; concurrent
  first opens of an unstamped database all start),
  `internal/journal/version_test.go` and `trigger/worker/version_test.go`
  (a head-migrated journal and queue refused by a binary built with a
  lower version, and opened by the same and a newer one), and
  `contract/audit/erasure_migration_test.go` (a process killed inside the
  migration leaves no stamp). Red on origin/main; mutations and the real
  pre-#286 and pre-#290 binaries are in the PR.
- Pruned records stay pruned (#294), against real SQLite files:
  `contract/audit/pruned_record_test.go` (the deciding tenant re-delivers
  a reconciliation whose record was pruned, before and after a restart,
  and gets the same duplicate with no record written, mirrored or counted;
  compaction after the prune re-creates nothing; an approval retried after
  its record was pruned writes nothing; another kind under a pruned id is
  `ErrConflict`; a pruned record put back verbatim with its counter
  adjusted is `ErrCorrupt`, with and without owners, and passes once its
  tombstone is removed instead; one `Prune` removes a record re-created
  the way a binary before #294 did it, and `Verify` passes; a record next
  to a tombstone of another kind is `ErrCorrupt`; the record is found and
  named in any batch of 256; a pruned reconciliation accepted under an
  older sensitivity rule compacts, unowned, and re-delivers, owned, without
  an error; a first approval or reconciliation under a planted tombstone
  is `ErrConflict` and applies nothing, and commits once the tombstone is
  gone). Two tests in `reconcile_tenant_test.go` that pinned the
  re-created record were inverted. Red on origin/main or on the first
  #294 head; mutations and a store written by the origin/main code are in
  the PR.

- The audit start marker (#284), against real SQLite files:
  `contract/audit/start_test.go`, with a database written by origin/main at
  `71b8351`, before #280, by the pre-audit code path
  (`testdata/restore/audit-start-284/legacy-main-71b8351.db.gz`: three
  approvals, two reconciliations, no audit tables) and the same database
  upgraded and used by `v0.1.0-alpha` (`alpha-79ee0a7.db.gz`: audit 1, one
  more approval and reconciliation with records; the alpha binary's own
  `Verify` of it is `ErrMismatch`, which its generator checks). Both verify
  with three legacy approvals and two legacy reconciliations, reopen
  unchanged, and match a legacy reconciliation once compaction backfills
  its record; a decision recorded after the marker whose record is removed
  is `ErrMismatch`, for both kinds; a decision added to the legacy list,
  with or without the count raised, and a list without its marker are
  `ErrCorrupt`; a marker removed with its list, or its tables dropped, is
  not written again on reopen and the legacy decisions mismatch; a fresh
  database closes both markers with nothing listed. Red on origin/main;
  mutations are in the PR.

## Limits

- Journaled and cluster runs on `store/distributed` (ADR 0019) have no audit
  integration and no `RunActivity` of their own. Prune fails closed for
  them: the journal's activity port reports a run it does not hold as
  active, so such a record is kept until the application composes an
  activity port that can prove the run ended.
- The audit start marker (§10) is evidence against a rewrite that does not
  know the scheme, not against someone who can write the database and
  recompute a sha256; keep the reported markers outside the database to
  detect that. On a database that ran `v0.1.0-alpha`, a record removed
  before this release first opened it makes its decision legacy. A marker
  closes on the owner's first open composed with audit: a journal opened
  only without audit leaves the reconciliation marker open, and a
  reconciliation an old binary writes before it closes is listed as
  legacy.
- Erasure (§7) is retention-driven only: there is no API to erase one
  run or one subject's runs on request before their cutoff. Failed,
  canceled and uncertain runs are never compacted, and a parent's copy of a
  child's result is kept with the parent, so that content is kept
  ([#289](https://github.com/well-prado/new-blok/issues/289)).
- Worker queue payloads (`worker_jobs`) are kept forever; they are outside
  the journal's compaction
  ([#290](https://github.com/well-prado/new-blok/issues/290)).
- Schema versions (§9) are checked only by binaries from #291 on. A binary
  built before #291 still opens a migrated journal, audit store or queue
  without refusal, with the effects §7, §8 and ADR 0006 describe; the next
  open by a current binary repairs what it can.
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
  and kind forever, so Verify can tell pruned from missing, and the id is
  retired for good (#294).
- A record a binary before #294 re-created after it was pruned (a
  re-delivered reconciliation, a retried approval, or a compaction
  backfill of a pruned record) sits next to its own tombstone, and
  `Verify` now reports `ErrCorrupt` for it, naming its id. Nothing
  repairs it automatically, and the repair is to run `Prune`: the older
  binary re-created the record with its original time, so a cutoff that
  pruned it once covers it again, and one `Prune` removes it, refreshes
  its tombstone and decrements the counter. A `Hold` or a run still
  active keeps it, as it keeps any record. Shown on a store written by
  the origin/main code (re-delivery after a prune) and opened by the
  #294 code: `Verify` named the record, one `Prune` removed it, and
  `Verify` passed with the tombstone alone.
- `Verify` checks every record's id against the tombstones, in batches of
  256. Measured in review on 20,000 records, best of 7, under load
  (macOS, 1-minute load average about 26): without tombstones 119 ms
  without the check (origin/main), 427 ms with one lookup per record,
  164 ms batched; with 20,000 other tombstones 147, 459 and 284 ms. The
  cost is index page reads in the pure-Go SQLite driver, which batching
  does not remove. It was not measured at `MaxRecords` scale.
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
