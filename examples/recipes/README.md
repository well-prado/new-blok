# Runnable application recipes

## Shop: authenticated CRUD, durable jobs, signed webhooks, and SSE

This recipe composes the actual Blok `app`, `trigger/http`, `trigger/worker`,
`trigger/webhook`, `trigger/sse`, and `store.Database` APIs. It selects SQLite
through `store/sqlite`; the engine and the business code depend on the store
interface, not an ORM. All identities, records, tokens, event bodies, and keys
below are synthetic.

### Requirements and ownership

| Concern | Selected module / owner | Configuration |
|---|---|---|
| Durable database | `store/sqlite` (example choice) | `SHOP_DB_PATH`, persistent writable file |
| Caller authentication | Recipe bearer-token authenticator | `SHOP_TOKEN_ALICE`, `SHOP_TOKEN_BOB` (each at least 16 bytes) |
| Provider verification | `trigger/webhook` Standard Webhooks verifier | `SHOP_WEBHOOK_SECRET` (at least 16 bytes; production should resolve it from an external secret manager) |
| Durable admission and worker | `trigger/worker` on the same `store.Database` | Queue schema is installed by `worker.New` |
| Outbox delivery | Exported `shop.OutboxPublisher` port; executable selects `shop.SyntheticSink` | Separate durable `SHOP_SINK_DB_PATH`; one event per atomic claim; 30-second default lease |
| Streaming | `trigger/sse` with the durable worker as submitter/tracker | queue depth 1; 32 endpoint subscribers; 4 per stream; 8 KiB event ceiling |
| Business schema | This recipe's numbered migrations | `shop_schema_migrations`, `shop_records`, `shop_outbox` |
| Listener | Standard library `net/http` | `SHOP_LISTEN_ADDR` |

The two configured caller tokens establish the `alice` and `bob` principals.
Record reads, updates, and deletes include the authenticated owner in their
database predicate; another principal receives the same stable `not_found`
response as an unknown record. The current `trigger/http` adapter maps its
available classified validation errors to HTTP 400 and has no not-found status class,
so the recipe preserves concealment and reports that status limitation rather
than changing the trigger package. The webhook principal is established only
after signature verification. A caller cannot set any principal through JSON.

### Fresh setup and run

Run from the repository root with Go 1.27.1:

```sh
export SHOP_DB_PATH="$PWD/.local/shop.db"
export SHOP_TOKEN_ALICE='synthetic-alice-token-0001'
export SHOP_TOKEN_BOB='synthetic-bob-token-00002'
export SHOP_WEBHOOK_SECRET='synthetic-webhook-key-00000001'
export SHOP_SINK_DB_PATH="$PWD/.local/synthetic-receiver.db"
export SHOP_LISTEN_ADDR='127.0.0.1:8080'

go run ./examples/recipes/cmd/shop migrate-up
go run ./examples/recipes/cmd/shop migrate-status
go run ./examples/recipes/cmd/shop serve
```

`migrate-up` creates the parent directory and database, applies ordered
migrations in transactions, and is safe to replay. `serve` requires every
token, the webhook key, listener address, and a separate sink database
explicitly; it does not choose example credentials or a hidden database path.
Its bounded poll loop consumes one durable job and then claims/publishes at
most one outbox event. Business state, outbox event, and worker acknowledgment
share the worker transaction. Publishing happens after an atomic lease claim
has committed, outside the app database write transaction.

The independent consumer module at
[`external/shopapp`](external/shopapp) imports the exported recipe and SQLite
packages from outside the root Go module. From that directory, run
`go test ./...` to exercise the exported composition, then `go run .` with the
same environment. Its local `replace` points to this checkout; use a released
module version when consuming a published release.

CRUD example:

```sh
curl -i -X POST http://127.0.0.1:8080/records \
  -H 'Authorization: Bearer synthetic-alice-token-0001' \
  -H 'Content-Type: application/json' \
  -d '{"id":"record-1","value":"synthetic"}'
curl -i http://127.0.0.1:8080/records/record-1 \
  -H 'Authorization: Bearer synthetic-bob-token-00002'
# 400 with `{"error":"not_found"}`: the record exists but belongs to alice.
curl -i -X PUT http://127.0.0.1:8080/records/record-1 \
  -H 'Authorization: Bearer synthetic-alice-token-0001' \
  -H 'Content-Type: application/json' \
  -d '{"requestKey":"update-record-1","value":"revised"}'
```

Queue a job with `POST /jobs` and a bearer token. The signed webhook route is
`POST /webhooks/orders`; it accepts the Standard Webhooks headers
`webhook-id`, `webhook-timestamp`, and `webhook-signature`. It verifies the
original body bytes and returns 202 only after durable submission commits; a
redelivery of the same provider event returns 200 without accepting another
job. A changed body with the original signature is rejected.

To follow a durable job, `POST /jobs/stream` with bearer auth, an
`Idempotency-Key`, and a `JobSchema` JSON body. The response contains a stream
ID; `GET /jobs/stream/{streamID}` with the same bearer token follows it. SSE
uses replay cursors, bounded per-subscriber queues, finite stream duration,
write deadlines, retention/event byte caps, and disconnects slow clients. A
disconnect stops waiting; it does not cancel an accepted job.

### Migrations, replay, and teardown

The app-owned v1 migration creates `shop_records`; v2 adds `updated_at` and
creates `shop_outbox`; v3 adds lease and claim-token columns. The worker
package independently installs and upgrades `worker_jobs`. The test suite
starts from both an empty database and a v1
database, replays migrations, verifies the resulting schema, and tears down
then installs again. To remove this recipe's tables, stop every process using
the file and ensure the SQLite file is dedicated to this recipe (the worker
queue table is shared by worker kinds in that database), then run:

```sh
go run ./examples/recipes/cmd/shop teardown
```

Teardown drops only `shop_*` and the selected worker queue table. It leaves the
SQLite file and any unrelated application tables in place.

### Executable evidence and source map

The fixture [shop/fixtures.json](shop/fixtures.json) predeclares outcomes for
owner CRUD and idempotent update, cross-principal denial, signed duplicate
delivery, atomic competing outbox claims, accepted-then-error reconciliation
after process restart, external-module composition, and slow subscriber
bounds. Tests run real HTTP handlers, SQLite transactions, the durable queue,
webhook verification, and an SSE HTTP connection. The two-node typed workflow
validates the record command and builds a stable `shop.record.changed` event
consumed by transactional app handlers; it does not claim every persistence
operation is an engine node. The synthetic receiver durably deduplicates event
IDs and rejects changed payloads under a reused ID.

- Composition, migrations, CRUD, job handler, and bounded outbox claims: `shop/recipe.go`
- Durable deduplicating local receiver: `shop/sink.go`
- Typed validation and stable-event preparation workflow invoked by create, update, and worker handlers: `shop/recipe.go` (`node.Define`, `flow.Define`, `engine.Run`)
- Real HTTP, migration replay/upgrade/teardown, process restart, and streaming tests: `shop/recipe_test.go`
- Runnable commands and explicit environment validation: `cmd/shop/main.go`
- Independent consumer module and executable: `external/shopapp/`
- Expected synthetic outcomes: `shop/fixtures.json`

The worker adapter transaction-ownership issue tracked by #180 is not changed
here and remains an upstream dependency. In particular, this recipe does not
replace the handler's cancelable context for transaction SQL or claim that
SQLite busy timeouts bound statement execution. Worker cancellation/transaction
ownership acceptance therefore remains pending the independently reviewed
upstream fix and its integration evidence.

The current recipe handler accepts the existing `*sql.Tx` callback ABI; the
proposed #180 `worker.Tx` ABI will require an explicit recipe migration once
the authoritative fix is reviewed and merged. No concurrent handler-SQL
serialization guarantee is inferred from the proposed wrapper or from this
recipe's tests.
Parent-reported review evidence for the unmerged #180 proposal includes an
independent concurrent-statement regression reproducer failing 3/3 runs:
`SQLITE_FULL` rolls back the claim while a concurrently queued small insert
commits a business row, and `ProcessOnce` returns nil. A sequential handler
cancellation regression passed natively, but does not cover this concurrency
failure. Do not use or merge the proposed fix until this is independently
resolved and reviewed.

The typed workflow here validates the command and prepares the stable event;
the application handlers own the record and outbox SQL transaction. This is a
deliberate example boundary, not a claim that persistence is an engine node.
Architecture sections 3 and 9 describe nodes as typed functions and workflows
as composed typed tools, with the application composition root registering
nodes, workflows, and adapters. The recipe currently exercises that boundary
for validation/event preparation. Issue #62 delivers effect nodes through
injected provider ports and tests real synthetic provider endpoints; this
recipe's `OutboxPublisher` is instead called by the app-owned outbox loop,
outside the workflow. Its durable synthetic receiver and restart tests
demonstrate the external-delivery boundary and uncertain-outcome reconciliation,
not an effect node in the two-step workflow. A workflow that composes business
persistence and provider effects as nodes would need to demonstrate those nodes
and their transactional/durable acknowledgment boundary explicitly. That is
not inferred from this recipe's current composition, and worker transaction
ownership also remains gated on #180.

Run root-module evidence with `go test ./examples/recipes/...`, and the
external-consumer evidence separately from `examples/recipes/external/shopapp`
with `go test -race -p 3 ./...` plus `go vet -p 3 ./...`. Root `./...` does not
include this nested Go module.

Delivery remains at least once across lease expiry, so a receiver
must durably deduplicate stable event IDs. The synthetic receiver implements
and tests that contract, including accept-then-error followed by app restart.
SQLite is a local single-host store, and the in-memory SSE replay hub starts a
new epoch after process restart.
