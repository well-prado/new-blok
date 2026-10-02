# ADR 0006: Reviewed catalog and tool admission (#74 / E14-T01)

Status: implemented pre-alpha. Version 1 manifests explicitly declare
`agent-compatible`, exact effects/capabilities and determinism. Missing, invalid,
legacy and unsupported declarations fail closed; manifests are not sandboxing.
Reviewed source/example/test references are required repository-relative paths,
not source text, credentials or model-provided callback code.

Applications inject a `node.Registry` into `agent.NewCatalog`. Registration
captures the actual registered handler and descriptor; a caller's definition
cannot replace code or reclassify effects. Capabilities and manifests are
composition-owned, not accepted from invocation JSON. Schema-described native
and worker-backed nodes use the same admission/normalization/publication path.
Worker descriptors bind their actual required dispatch capabilities into the
canonical catalog digest. Registration rejects a manifest omitting any configured
worker grant; dispatch also checks the trusted narrowed scope before sending.
Changing worker requirements therefore requires a new catalog negotiation.
Catalog listings copy metadata/schema collections and filter against a verified
caller principal. Opaque secret references never appear in model-visible listings.

`RegisterWorkflow` snapshots supported structural call/child programs and every
registered child's policy/artifact identity. It aggregates effects, capabilities,
secret references, worst-case token reservations, call count and depth. Children
receive only their own narrowed capabilities. The existing engine runs programs;
this is not a second interpreter or source-expression evaluator. Unsupported
control-flow kinds fail closed at registration rather than conceal authority.
Only call/child chains are currently agent-tool-compatible; ordinary authoring
support outside agent registration is unchanged.

Budget bounds: depth 1–64; input/output up to 1 MiB; explicit positive token/call
budget and deadline; calls at most 10000; at most 64 active top-level invocations,
without an unbounded waiting queue. A node's worst-case provider token limit is
reserved before dispatch and passed through trusted context; the injected
provider must enforce it. Recursive invocations cannot reset ledger, principal,
Positive token reservations on remote nodes fail closed at registration and at
the worker adapter. No verified remote token-budget adapter exists in this
slice, so token-using remote agent tools are explicitly unsupported; reserving
Go context values does not establish enforcement in Node. Zero-token native and
remote tools remain supported. Resources enter node/transitive workflow digests
and admission records, including resource-only policy changes.
deadline or depth. Approval/evidence adapters run before each dispatch and
publication when injected. Durable review and provenance are owned by #75.

Compatibility: this replaces the previous pre-alpha caller-supplied callback
catalog. Applications must register real nodes, provide verified principal IDs
and reviewed metadata, and re-register workflow policy snapshots. No released
API is being migrated; artifact identity changes with program/policy changes.

Evidence: catalog tests assert read/write filtering, immutable registry/policy,
hidden children, malformed/missing public metadata, default normalization,
forged model grants, token/call/depth/deadline denial before effects, denied
publication and concurrent access under race. The explicit
`TestActualNativeNodeToolWorkflowAdmission` starts a real Node process, composes
native read → Node quote through the real engine, verifies all three admission
records and normalized int64 output, then denies a caller lacking read scope
without a second effect. Go-only tests skip this foreign-runtime gate explicitly.
`TestActualNodeEffectScopeCannotWidenOrBypassTokenBudget` starts an effectful Node
and real HTTP provider; weaker manifests/callers and unsupported reservations
dispatch zero effects, while the reviewed grant executes exactly one charge.
No live AI performance, native-code sandbox or production release claim.
