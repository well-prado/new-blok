# ADR 0016: Versioned run and step inspection projections

- Status: accepted for the application-composed Go and journal inspection slices and the live development event stream
- Date: 2026-10-03
- Roadmap: E15-T01 (#76); live event stream E15-T02 (#77)

## Context

Notebook consumers need stable read-only run and step data without access to
mutable engine state. Authentication belongs to the application/adapter; the
engine must report trusted ownership supplied by that boundary. Payload fields
must be redacted, bounded, and authorized independently of execution input.

## Decision

The additive wire contract is `inspection/v1` in `contract/inspection`. Public
application composition selects an optional observer through `app.Config` and
the public `execution.NewRunner`; application-owned modules do not import the
internal interpreter. Ordinary `Engine.Run` does not JSON-serialize observation payloads.
Durable terminal projection is separately opt-in through the application-owned
`RunOutcomePort`: `execution.Runner` records completed, failed, canceled, or
explicitly uncertain outcomes after the real engine returns. When this port is
configured, run-terminal observation is deferred until the durable outcome
attempt resolves; a failed terminal write is projected uncertain, never briefly
completed. The journal
provides a principal-checking adapter for that port without being imported by
the engine. The trusted trigger/application boundary supplies the run ID and
principal together; the adapter verifies their ownership match before any
terminal mutation. If the application does not configure the port, the runner
does not claim durable terminal state.
Observed calls require a trusted run and principal, preserve supplied event
times, and have a unique execution-attempt identity; each step attempt has its
own identity and immutable input/output snapshots. The engine also captures
structured Go node logs only when inspection is selected. Diagnostic labels
are bounded and allowlisted; provider error messages are not projected.
Observer payload capture is also bounded before serialization: each input or
output is limited to 32 KiB, 8,192 visited values, and 32 nested levels. Larger
or unsupported values become `{"$truncated":true}`. Capture structurally copies
JSON-compatible fields and does not invoke caller-defined `MarshalJSON` methods.
Custom JSON/text marshalers, named-byte slices, anonymous embedded fields, and `string`/`omitzero` JSON tags
are explicitly truncated rather than approximated. Ordinary nil slices retain
JSON null and `omitempty` retains standard empty-collection semantics. These
capture limits affect diagnostic snapshots only, never business values.
Go 1.27 custom `MarshalJSONTo` and `AppendText` representations and unsupported
quoted/invalid JSON field tags likewise truncate without invoking application
methods. Map keys with custom representations also truncate rather than
exposing their underlying value.
Duplicate projected struct field names truncate before `omitempty` selection;
capture never exposes a value that standard JSON would discard as ambiguous.
Invalid standard-library time representations also truncate.
Raw JSON also uses a bounded token scan for depth and visited items before
retention; its serialized form does not bypass the structural capture limits.

`inspect.Recorder` is a process-local development projection. It authorizes by
exact principal, returns the same not-found result for unknown and unauthorized
IDs, supports versioned cursor pagination and field allowlists, redacts
sensitive JSON keys, and bounds payloads and responses. Hard defaults cap it at
1,024 runs, 100,000 accepted events, 64 MiB accounted retained event data,
4,096 steps per run, 100 attempts and 100 logs per step, and 64 KiB per event.
Saturation drops later observations, increments `Stats().DroppedEvents`, and
sets a visible `truncated` projection flag for retained runs. The recorder does
not evict history or promise restart retention. Page reads select the requested
page before projection. Each response projects only the five most recent
attempts and four most recent logs per step, with per-step truncation flags.
Payload fields have a 64-byte minimum projection envelope; an explicit smaller
`MaxPayloadBytes` is rejected, never raised. The effective per-payload ceiling
is also reduced as needed to fit the aggregate payload slots under the response
limit (256 KiB default, 1 MiB hard maximum). Oversized response envelopes fail
closed rather than violating the response ceiling.
Encoded cursors are limited to 1,024 bytes before base64 decoding.

`inspection.Source` is a narrow bounded read port. The journal implements it
without exposing the store implementation: principal authorization and SQL
pagination precede materialization, payload reads are capped to the computed
per-payload page budget, and attempts are SQL-limited before materialization.
Reads derive waiting/resumed status, uncertain operation status,
attempt IDs/history, and same-principal child lineage from actual journal rows.
Existing journal databases are migrated with an owner field; old ownerless
runs remain inaccessible through inspection. `InspectSource` applies the same
redaction and response projection. Blob retrieval is a separate opt-in path:
the caller must allow it, provide a principal-bound authenticated reader, and
set a hard maximum no greater than 1 MiB; digest, size, session, and principal
are checked before bytes are returned.

New durable step-scope and effect-intent inputs are capped at 64 KiB each,
checked before JSON validation and before any SQL write. Attempt input is copied
from its already-bounded immutable effect intent, not accepted independently.
The additive migration leaves historical scope/operation/attempt input columns
NULL; recovery never fills those unknown values from a later invocation. Run
admission input remains its pre-existing journal contract and is not silently
reclassified by this inspection-input limit. A terminal failed-run fact is an
explicit `FailRun` transition from the execution owner, not an inference from
`FailAttempt`; it is rejected while operations, waits, or scopes remain active,
and uncertain effects remain uncertain rather than being converted to failure.
New successful-run output writes are capped at 1 MiB before JSON validation or
SQL persistence.
The runner binds execution to its application lease, so shutdown abort reaches
the node before dependencies close. Terminal writes use a bounded app-abort
context detached from request cancellation so the outcome can commit before
the lease is released. If terminal persistence fails after execution, the
runner returns an explicit uncertain result with the run ID for reconciliation;
it never presents the completed effect as a safely retryable generic failure.

## Live development event stream (E15-T02, #77)

A page answers "what happened"; a live stream answers "what is happening",
as a browser would follow it. It is telemetry: best effort, bounded, and
unable to affect the run or the journal.

**Composition.** `inspect.NewEventStream` is an ordinary
`inspection.Observer` the application selects (alone or with the recorder
through `inspect.CombineObservers`). `inspect.NewEventHandler` serves it as
Server-Sent Events at `GET .../runs/{run}/events`. The engine, the inspection
contract and `execution` import neither; `observe/event`, the hub, imports
only the standard library. An application that selects no stream has no
stream.

**Never blocks the run.** The engine calls observers synchronously on the
run's goroutine, so `Observe` does bounded in-memory work under one short
lock and no I/O. Each reader has a queue of `QueueDepth` (32) frames; a
publication that finds it full disconnects that reader (`slow_subscriber`)
instead of waiting, and the reader's blocked socket write is interrupted at
once rather than at its write deadline. The run, its journaled effects and
its terminal outcome (`RunOutcomePort`) are therefore never held by a
reader; the outcome is written before the terminal frame, as before.

**Capture is opt-in, at the source.** `inspect.Capture{Inputs, Outputs,
Logs}` defaults to false: the zero configuration keeps transitions, timing,
attempts and classified error labels only, and discards payloads and logs
before anything is retained. A reader then sees a field only if the stream
captured it **and** the application's `Policy(principal)` grants it; error
labels are policy-gated but not capture-gated. Captured content gets the
recorder's key redaction and credential-shaped log redaction. The owner
principal never appears on the wire.

**Authorization.** The application's `Authenticate` yields the reader
(401 otherwise). `Authorize(reader, owner)` defaults to the owner only; a
refused reader gets the same 404 as an unknown run. Run IDs and principals
are at most 256 bytes. `Authorize` and `Policy(principal)` are evaluated once
per connection, not per frame: a change of grant reaches an open
subscription when it reconnects, which `MaxDuration` bounds. A run's owner
never changes during a subscription. Every connection is admitted against
the reader limits (`MaxSubscribers`, `SubscribersPerPrincipal`) right after
authentication and before it replays, follows or reads durable state; a
replay-only connection holds its admission until it has been written.

**Frames and cursors.** Each frame is `id`, `event` (the inspection kind,
`gap`, `snapshot` or `end`) and one line of compact JSON. Ids are
`<epoch>.<incarnation>.<run hash>:<seq>`: a process restart is a new epoch;
a run the hub forgot and later re-created is a new incarnation; the hash
binds the id to its run.

| Last-Event-ID | Outcome |
| --- | --- |
| none | everything retained; a `gap` first if early frames were evicted |
| this incarnation, retained | exactly the frames after it |
| this incarnation, evicted | `gap {reason: retention, missed: n}`, then what is retained |
| an earlier incarnation | `gap {reason: retention}` (count unknown) |
| the recovered attachment a publisher reclaimed | `gap {reason: reowned}` |
| another epoch | `gap {reason: restart}` |
| ahead, a later incarnation, another run's, malformed, over 128 bytes | 400 `invalid_cursor`, no stream bytes |
| at the end of a closed run | 204, which stops EventSource |

A gap's id is the position just before the first replayed frame, so
reconnecting from it adds no second gap. **Loss is never silent:** an
observation that cannot be represented (invalid, over `MaxEventBytes`) is
replaced in sequence by `gap {reason: dropped}`; a run re-created after
eviction opens with `gap {reason: evicted}`; a late log after the late window
appends `gap {reason: late_dropped}`. Every drop, eviction, rejection and
slow reader is counted in `Hub.Stats`.

**Late worker logs.** A worker log can reach inspection after its step and
its run completed (above, #226). A stream does not stop at the terminal
frame: for a reader allowed to see logs it keeps following for `LateWindow`
(2 s, at most 30 s) after it, delivers any step log published meanwhile, then
sends `end` (`{"lateWindowMs": …}`) with the id of the last frame it passed.
A log after the window is dropped and marked (`late_dropped`); a reader
without log access ends at the terminal frame. `RunSuspended` is not
terminal. A worker log the runtime receives after its call's result is still
dropped by the runtime, as before; the window cannot recover it.

**Recovery.** The hub is memory. With `Source` configured (the journal's
`inspection.Source`), a run this process has no history of is read from
durable state *as the reader*, so the source authorizes again, and the reader
gets a `gap` and a `snapshot` frame `{source: journal, reconstructed: true,
page, unavailable: [...]}` built by `InspectSource` with the same field
policy and a 64 KiB bound.

Following such a run live needs its **durable owner**, because the engine
publishes under the run's trusted principal and readers are authorized
against it. A source that also implements `inspect.RunOwnerSource`
(`RunOwner(ctx, reader, runID)`; the journal does, and being owner-only it
names the reader for the reader's own runs) has the run attached under that
owner; the reader is then checked with `Authorize(reader, owner)`, the gap is
`restart` for an old cursor or `history_unavailable` without one, and a
non-terminal run is followed live, so a resumed execution in this process
streams normally. A source that cannot name the owner yields the gap
(`history_unavailable`), the snapshot and `end`, without ids and without
following; the reader reconnects for a fresh reconstruction or for the live
run once the engine publishes it. The reader never becomes the owner. As a
second line, a publication whose trusted owner differs from a *recovered*
run's attached owner wins: the run is replaced by a new incarnation, its
followers are detached (`reowned`) to re-authorize, and `Stats.Reowned`
counts it. A reader resuming from the old attachment's cursor gets
`gap {reason: reowned}`; a reclaim by a mid-run publication also opens the new
incarnation with that marker (one by the run's first observation loses
nothing and has none). A live run's owner is never reclaimed (a conflicting
publisher is rejected and counted).

A recovered run whose trusted owner publishes is live from that moment: it
leaves the attaching reader's recovered budget, can no longer be displaced as
a recovered run, and a reader without a cursor no longer gets a
`history_unavailable` gap or a durable read for it.

Recovered runs have their own budget: one reader principal may hold
`RecoveredPerPrincipal` (16) attached runs, and at the budget its own least
recently used unfollowed recovered run is recycled. A recovered run may
displace only another recovered run or a closed one, never a live run; when
every retained run is live, the read is refused as saturated.

The snapshot holds journal facts only: transient transitions, logs, and
steps that had not yet written a journal fact are not reconstructed.

**Following a run nobody publishes (#263).** A recovered run that no
execution in this process publishes for (a crashed run, a run on another
replica, every cluster run) has nothing that would end it. Its follower
therefore re-reads it from `Source` every `RecoveredPoll` (2 s; at least
100 ms, at most 1 min), as the reader and with the connection's field
selection, each read bounded by `SourceTimeout`. A reconstruction that
changed is sent as a new `snapshot` frame, at the reader's current position;
an unchanged one is not resent. When the source reports the run terminal
(`completed`, `failed`, `canceled`, `uncertain`; `suspended` is not), the
follower receives that final snapshot and `end`, is released, and the hub
closes the attachment, so a later reader gets the reconstruction and `end` at
once. One connection makes at most `MaxDuration / RecoveredPoll` durable
reads, and `Authorize` and `Policy` are still evaluated once per connection.
Polling stops as soon as a trusted publisher claims the run, which then ends
with its own terminal frame. A failed poll (a timeout, an outage) is skipped,
not retried at once. A source that classifies its errors
(`inspect.RefusingSource`) can report a poll as refused, meaning the reader
may no longer see the run; that follower is closed at once, without `end`, so
a revoked grant does not stay open until `MaxDuration` (its reconnect gets
the not-found answer). A source that also implements
`inspect.UnavailableSource` names what a reconstruction could not include;
those notes (at most 8, each at most 128 bytes) join the snapshot's
`unavailable` list.

**Idle unfinished runs (#263).** A run that started here and never finished
(suspended, or whose execution stopped without a terminal transition) used to
be protected from recovered reads forever, so a hub at `MaxRuns` holding only
such runs refused every durable read. An unfinished run that has had no
publication and no follower for `IdleTimeout` (10 min, at most 2 h) is now as
recyclable by a recovered read as a closed run (`Stats.IdleRecycled`). A
followed run never is, and a follower leaving restarts the bound. If its
publisher later resumes it, the run is re-created behind an `evicted` gap, as
for any eviction.

**Durable and cluster runs (#263).** The cluster runtime's
`InspectionSource` (`internal/cluster`) is an `inspection.Source` and
`RunOwnerSource` over `store/distributed`. A run is owned by its tenant, as
its durable run record names it; the reader never supplies the owner. By
default a reader may look up only its own tenant's runs; an application may
map a reader to at most 16 tenants, and `Authorize` must agree. A run is found
only in the partition of one of those tenants and only when its record names
that tenant, so another tenant's run is not found even in the same partition.
Steps are the step journal's committed facts in program order: the step
records are keyed by an operation key that digests each step's resolved
input, so the source replays the run through the engine's journaled
interpreter with a read-only journal that restores committed outputs, as a
takeover does, and stops at the first step without one. That journal refuses
every dispatch, so inspection never invokes a node and never writes. Only the
end of the committed journal ends a replay normally: a store error, a
malformed record or an expired context fails the read, which is then never
returned as a shorter page (and a poll is skipped rather than sending it as
the final reconstruction). A replay reads at most one record per instruction
and stops after one step beyond the requested page. Depth is bounded per
read: a page must start within the first 256 steps (`MaxInspectionSteps`), so
one read costs at most 256 + limit + 1 step reads (457 for the largest page);
the page that reaches the bound is marked truncated and offers no next
cursor, and a page starting beyond it is refused before any read. The stream's
snapshot is always the first page. The distributed store records no
timestamps, so run and step times are zero, and every cluster snapshot lists
`timestamps` as unavailable; step input is not kept (only its digest); a step
record holds only its current attempt. A run whose registered artifact no
longer matches, or whose input is no longer retained (#265), is shown without
steps and says which.

Cluster runs do not emit live inspection events (ADR 0019). A cluster run
executes on whichever replica owns its partition, while the hub is
process-local, so a live frame would reach only readers connected to that
replica; the durable store is the one view every replica shares. A durable
run also spans several engine calls (suspension, replay after takeover)
whose per-attempt events would misreport a suspension as a failed step and a
restored output as a fresh completion. Snapshots polled at `RecoveredPoll`
report each committed step transition from any replica, cannot affect
execution (they only read), and end the stream at the terminal state.

**Bounds** (zero takes the default; above the hard limit `New` refuses):

| Bound | Default | Hard limit | Saturation |
| --- | --- | --- | --- |
| retained runs `MaxRuns` | 256 | 16,384 | evict least recently used unfollowed run; all followed: drop and count |
| frames / bytes per run | 1,024 / 256 KiB | 16,384 / 16 MiB | evict oldest; reader sees `retention` gap |
| `MaxRuns × RunBytes` | 64 MiB | 1 GiB | refused at `New` |
| frame `MaxEventBytes` | 32 KiB | 1 MiB | payloads → `{"$truncated":true}`, else `dropped` gap |
| admitted readers total / per principal; live followers per run | 64 / 16; 8 | 4,096 each | empty stream with `retry:` and `: saturated` |
| recovered runs per reader principal | 16 | 16,384 | recycle own LRU unfollowed recovered run; none free: saturated |
| reader queue `QueueDepth` | 32 | 1,024 | disconnect `slow_subscriber`; resume from cursor |
| `MaxSubscribers × (QueueDepth × MaxEventBytes + RunBytes)`: a live queue plus one run's replay per reader | 80 MiB | 256 MiB | refused at `New` |
| subscription `MaxDuration` | 30 min | 2 h | ends; client resumes from cursor |
| heartbeat / write timeout | 15 s / 5 s | 1 min / 1 min | write timeout ends the subscription |
| durable snapshot read / size (one per admitted reader) | 5 s / 64 KiB | 1 min / 1 MiB | no snapshot; the gap is still sent |
| recovered follower poll `RecoveredPoll` | 2 s | 100 ms – 1 min | one durable read per tick; a failed read is skipped |
| unfinished run idle bound `IdleTimeout` | 10 min | 2 h | past it, an unfollowed unfinished run is recyclable by a recovered read |
| cluster reconstruction depth `MaxInspectionSteps` | 256 steps | fixed | a page starting beyond it is refused; the page reaching it is truncated |

`Hub.Close` ends every subscription (`shutdown`) and refuses new ones while
publication continues, so stopping the stream never affects a run.

**Limits.** One hub per process: a reader connected to another replica sees
nothing from this one, and a cluster run is visible only through its durable
reconstruction, at most `RecoveredPoll` late, never as live frames (#263,
above). The engine serializes observation payloads whenever
any observer is selected, including when the stream discards them; that costs
CPU, not retention. (ADR 0020 adds `observe.PayloadObserver`, which lets a
payload-free observer such as the telemetry exporter avoid that cost; the
stream does not declare it yet.) Measured overhead and latency in the PR are one
developer machine's figures, not performance claims.

## Compatibility and limits

The contract is additive. Unsupported versions and malformed/cross-query
cursors fail closed. Returned byte slices are independent snapshots. The
journal source is an inspection view of the existing journal facts, not a new
general-purpose event store or promise that every workflow transition is
durable. Node.js SDK logging uses an explicit call-scoped logger and optional
protocol 1.1 log frames tagged with call, attempt, and generation; process-wide
stdout/stderr are never attributed to a run. Protocol 1.0 clients can use a
1.1 worker without logs. A 1.1 client rejects a 1.0 worker and requires its
upgrade. Logs have per-record and per-call bounds, use the same field selection
and attribute-key redaction as Go logs, and redact common credential-shaped
free-form text. Pattern matching cannot identify arbitrary secrets embedded in
ordinary prose, so applications must keep free-form log messages free of
credentials/customer payloads and put sensitive values only in structured
attributes. There is no concrete-trigger integration or universal API server
in this decision.

A worker log is delivered off the call's result path (`Call.OnLog`), so it
may reach inspection after its step, and its run, have completed. That can
happen even without queue pressure: the result can simply win the race. A
log the runtime received before its call's result is still attributed to its
step and appears in later projections, with the time it was delivered rather
than the time it was emitted. A log frame received after the result, or one
that arrives while the log queue is full, is dropped as before. A reader
that needs a step's logs therefore reads after its completion rather than at
it (#226). A consumer that stops at a run's terminal event can miss such
logs unless it keeps reading for a bound after that event; the live stream
(#77) does, for its late window.

## Evidence

Executable scenarios run the real Go engine and real Node worker; compare an
actual engine projection with a checked-in golden response; exercise
two-principal isolation, redaction, immutable projections, pagination,
unsupported versions, recorder saturation, and bounded authenticated blob
retrieval; read real SQLite journal wait/resume, uncertain-attempt, and
child-lineage facts; and assert an actual Node worker log appears in an
authorized projection with its synthetic token attribute redacted. Node
integration requires `BLOK_NODE_INTEGRATION_ROOT` and the built runtime worker.
This evidence does not imply durable retention for recorder data.

Live stream evidence (#77): actual `trigger/http` runs streamed over real
HTTP connections, followed mid-run, resumed after disconnect without loss or
duplicates, and refused for unauthenticated, foreign and malformed-cursor
readers; synthetic fixture cases in `testdata/inspection/events/cases.json`
with predeclared frames, error codes, effect and payload counts; readers that
never read, attached to a run moving a journaled effect through intent,
attempt and commit and persisting its terminal outcome through the journal
port, with the run and journal unaffected and the readers' blocked handlers
interrupted; a real process killed mid-effect and its run reconstructed from
the SQLite journal under the old cursor; late logs after `run.completed`, both
from the engine's logger and from the actual Node worker; and measured run
overhead, delivery latency, goroutine and retained-heap figures. Each guarded
behavior was shown to fail under a deliberate mutation before it was trusted.

Durable run evidence (#263): a run executed by the cluster runtime against a
real three-voter etcd cluster, followed over real HTTP by its tenant while its
first step was in flight, suspended at a wait and resumed by a signal, with
each step-journal transition reconstructed and the stream ended after the
durable run completed; a tenant in the same partition and an unknown
principal answered 404, and a mapped reader attached the run under its
tenant. A journal run completed with no publisher ended its follower within a
poll interval, with bounded reads, no resent snapshot, one policy evaluation
and no payload under default capture; a hub at `MaxRuns` with idle unfinished
runs admitted the recovered read only past `IdleTimeout`. A completed run read
under 1–60 ms deadlines returned only full pages or errors, never a short
page; paging a 300-step run by cursor stopped at a truncated page at the bound
whose read cost 262 store reads, and a deeper page was refused with no read; a
follower stopped polling once a publisher claimed its run, an outage skipped
polls, and a refusal closed the follower. Each guarded behavior was shown to
fail under a deliberate mutation.
