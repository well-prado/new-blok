# ADR 0010: Injected effect nodes (#62 / E10-T02)

Status: implemented pre-alpha additive surface. No commercial provider integration
or universal exactly-once claim. Existing HTTP response classifications are
tightened: ambiguous 5xx/408, lost responses and post-dispatch cancellation are
uncertain, not automatically retryable. Reference 429 means explicit rate-limit
rejection; only that documented provider outcome is transient.

Applications construct typed HTTP request, email, payment, database/outbox,
message publish, audit and structured-generation nodes through `catalog` with
injected `provider.Port`. Nodes require one exact capability and opaque secret
reference names, bounded request/output size and deadlines. Resolved credentials
belong exclusively to composition-owned endpoint headers. Missing/typed-nil
providers and invalid declarations fail during construction. Registration and
workflow builders do not execute providers.

The synthetic JSON endpoint adapter makes real bounded HTTP requests to a fixed
composition-owned URL, never a user-selected URL. Redirects are refused; userinfo,
query and fragment are rejected by endpoint construction. Idempotency keys reach
the provider, not an attempt counter. Context and bounded deadline reach network
dispatch. Response schemas and credential echoes are checked before returning.
The adapter redacts arbitrary remote/SDK causes and preserves known error class
and operation key. A manifest describes authority; it does not sandbox native code.

Raw response presence fix (`providerrawpresencefix1`, #62): catalog constructors
bind their exact output schema to an owned endpoint copy through
`WithOutputSchema`. The adapter validates and normalizes the original response
bytes before typed decoding. Missing required fields and forbidden nulls remain
uncertain even when Go would decode them as zero values; an explicitly present
null succeeds only when the value schema permits it. Unknown fields follow the
schema's `additionalProperties` policy, including arbitrary nested generation
values. Invalid responses publish no output and do not erase an observed effect.
This is a pre-alpha behavioral tightening: standalone JSON endpoint callers must
call `WithOutputSchema` and execute the returned port; unbound `Execute` refuses
dispatch. Existing catalog composition binds automatically. Raw-body
`RequestEndpoint` uses the HTTP status/body contract and requires no JSON schema.

`provider.Records` owns a narrow records operation on an injected transactional
database. Business record and outbox row commit together. Repeated matching
operation keys return the established identity; changed inputs conflict. It is
not an arbitrary SQL node, ORM, broker or second journal implementation.

Executable evidence: `catalog/effects_test.go` drives the predeclared fixtures in
`testdata/providers/effects.json` against actual HTTP endpoints. It observes
requests/effects for every success, rejection, rate limit, timeout, lost response,
repeat key, invalid generated output, extra fields and credential echo. SQLite
tests force outbox rollback, reopen/deduplicate and conflict. The order outbox
test loses the email success response, then recovers with the same provider key
and one effect. Journal tests reopen an unknown payment and refuse blind retry.
These use synthetic providers; no live account or secret is required.

The presence regression fixtures cover absent/null HTTP bodies, receipt IDs,
database event IDs and generated values, nested required fields, permitted null
versus missing nullable values, and permitted/rejected unknown fields. They
declare exact request/effect/publication counts. Before the fix, six targeted
fixtures published output despite declaring zero publications. Author validation
is not independent Review R; another reviewer must recheck this fix.

Local Go 1.27.1 linux/arm64 gates: focused race tests for provider/catalog,
then `go vet ./...`, `go test -race ./...`, `go build ./...`, `git diff --check`.
The engine remains provider-free. Independent review/dependency merge evidence
is required before closing the issue.

Author rerun for `providerrawpresencefix1` uses Docker `golang:1.27.1`,
`go1.27.1 linux/arm64`, and the fixed synthetic JSON corpus (no random seed):

```sh
go test -race ./provider ./catalog -count=1
go vet ./...
go test -race ./... -count=1
go build ./...
git diff --check
```

No SDK code changes or live-provider evidence are included in this fix.
