# ADR 0008: Durable tool approval and publication policy (#75 / E14-T02)

Status: implemented pre-alpha slice; dependency completion and independent
security/durability review remain required before issue closure.

## Binding and trusted composition

`contract/approval.Proposal` binds action (including version), normalized input
digest, admitted workflow and deployment artifact, run, invocation/iteration
paths, effects and exact accepted capabilities. Catalog proposals also bind
`ToolDigest`: the actual registered catalog identity, independently of the
deployment artifact. That identity covers schemas, policy, resource reservations
and, for composed tools, structural program and transitive child digests.
Changing either digest, input, action, effects, scope or execution identity
requires a new immutable decision ID and authorized review. Replay runs cannot
reuse a source run's decision.

`policy.BindCatalog(policy, registry)` constructs the real #74 catalog with a
`CatalogGate`. Applications register nodes/workflows using `Catalog()`, then
`Bind(name, version, exactCapabilities, deterministicVerifier)` for every
reachable tool. Bind reads the registered listing and rejects capabilities
whose canonical digest differs; it never derives tool authority from a broad
principal grant. Bindings cannot be overwritten. Only trusted application
composition supplies verifiers and authenticated principals. Model JSON can
contain neither an executable callback nor an authenticated approval grant.

`CatalogGate.Prepare` derives workflow/artifact from committed admission,
normalizes against the actual registered schema, and constructs the proposal
for review. `Invoke` repeats that derivation and calls the real catalog through
`Policy.Invoke`; callers cannot dispatch directly through `Catalog().Invoke`.
The private session checks each root/child admission's actual identity, digest,
effects and narrowed capabilities against the bound tool and reviewed root.
The catalog continues to own workflow execution and resource budgets; this
adapter contains no second interpreter. Worker grants remain an independently
verified worker boundary, never inferred from catalog scope. Token-bearing
remote tools remain unsupported until #74 supplies a verified bounded adapter.

The review channel authenticates reviewers through `ReviewAuthorizer` and checks
their authority for the exact proposal and grant. A model-provided reviewer
name is never accepted. `ScopeAuthorizer` separately supplies authenticated
execution capabilities. Child calls cannot widen inherited capabilities even
with a separately approved proposal. Catalog reentry cannot reset authority or
budgets. Assertions supplied by callers are untrusted claims; their source and
determinism flags confer no provenance.

## Durable decisions, dispatch and publication

`approval.JournalStore` uses the existing transactional database on the journal
volume. Its owned `approval_decisions_v1` table atomically stores decision,
proposal, binding and reviewer/time audit. It does not modify journal tables.
Duplicate identical decisions are idempotent; rejection, expiry, reviewer,
proposal or grant conflicts cannot overwrite an ID. Reads verify the persisted
binding/audit and fail closed on inconsistency. No unrestricted decision Put
method exists. New dependency: none; persistence uses the existing SQLite port.

Limits: at most 1,000,000 configured decisions (saturation rejects a new ID),
16 KiB per proposal/decision, 64 scope/effect entries, 512 bytes per identity,
maximum 24-hour decision lifetime. Policy input/output bounds are configured
and capped at 16 MiB; assertions at 64 and 1024 bytes per assertion. Catalog
budgets retain #74's stricter 1 MiB input/output bound and explicit call, token,
depth and deadline limits. Retention/administrative deletion is not provided;
capacity must be provisioned explicitly rather than silently discard audit.

Dispatch order: committed admission → exact valid review → durable intent →
durable attempt → repeat review/deadline check → actual tool/provider. Child
evidence is deterministically verified before the engine can use child output;
root verification and repeat review/deadline checks precede durable result
commit. A verifier must check every required assertion/evidence/provenance rule
against this exact proposal/output using authoritative application sources.
Its code is trusted application code; the framework cannot prove determinism or
sandbox a verifier. Verifiers receive detached values and cannot mutate the
published bytes or widen a child's scope.

Executor/verifier failures expose sanitized codes, with canonical context
cancellation/deadline causes only. Raw external error strings and unwrap chains
never reach returned errors or journal evidence. A failure after the dispatch
barrier records a fixed uncertainty message, with an independent five-second
cleanup deadline, and publishes no trusted result. If that cleanup cannot commit,
the dispatched attempt still blocks redispatch after restart. Cancellation never
reverses an external effect.

A composed catalog invocation is one durable operation. Child results are gated
for internal engine use, but do not become independently recoverable trusted
checkpoints. Any ambiguous crash/error blocks redispatch of the whole operation;
no implicit retry or universal exactly-once guarantee exists. #48 owns authorized
reconciliation, exact executable retention and safe upgrade/recovery. This slice
checks the admitted artifact identity but does not replace #48's retained binary
startup proof. #49 owns journal retention, backup/restore and store operations;
this slice does not add an audit deletion/compaction lifecycle. No distributed
or disk-loss guarantee is claimed.

## Compatibility and evidence

Pre-alpha contract replacement: the previous overwriteable JSON file decision
store and model-claimed deterministic evidence API are removed. Applications
must inject a durable reader, authenticated review/scope ports and deterministic
verifiers. `ToolDigest` is optional only for non-catalog targets; omitted JSON
preserves their binding representation. The catalog adapter always supplies a
valid digest, so a legacy/empty digest decision cannot authorize catalog dispatch.
Existing immutable decisions are never upgraded to grant new authority.

Executable synthetic fixtures live in `agent/policy/testdata/adversarial.json`
and `catalog.json`, with expected error/effect/attempt/publication counts.
`TestCatalogPolicyProviderFixtures` uses #74's actual registry/catalog/engine,
#62's injected payment node and real HTTP endpoint, and SQLite. It proves
normalized equivalent inputs, exact scopes, digest changes, denial before
effects, child evidence blocking downstream payment, and root evidence blocking
trusted commit after an effect. `TestActualNativeNodeCatalogDurablePolicy` uses
a real persistent Node gRPC worker with native → Node composition, three trusted
verifications and SQLite commit; it explicitly skips without its runtime setup.
This is deterministic synthetic integration, not live-model reliability evidence.

`TestProcessKillApprovalDispatchPublication` kills an actual subprocess with
SIGKILL in six windows: before/after approval commit, before/after external
dispatch, and before/after result commit. SQLite integrity, decision audit,
effect marker, durable attempts/results and no blind redispatch are checked on
reopen. `TestChangedRetryAndReplayRequireFreshReview`, child-widening/reentry,
mutation isolation, concurrent immutable review and external error redaction
tests cover the other acceptance cases. Fixture provenance and the documentation
inventory are checked by `TestPolicyEvidenceInventory`.

Required evidence commands and observed tool versions/results are recorded in
[the #75 validation record](../validation/75-durable-policy.md). #74 and #48
dependency/review status must remain honest; these local gates do not close them
or waive independent Review R.
