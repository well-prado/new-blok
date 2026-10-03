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
| Streaming | `trigger/sse` with the durable worker as submitter/tracker | queue depth 1; 32 endpoint subscribers; 4 per stream; 8 KiB event ceiling |
| Business schema | This recipe's numbered migrations | `shop_schema_migrations`, `shop_records`, `shop_outbox` |
| Listener | Standard library `net/http` | `SHOP_LISTEN_ADDR` |

The two configured caller tokens establish the `alice` and `bob` principals.
Record reads, updates, and deletes include the authenticated owner in their
database predicate; another principal receives the same stable `not_found`
response as an unknown record. The current `trigger/http` adapter maps its available
classified validation errors to HTTP 400 and has no not-found status class,
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
export SHOP_LISTEN_ADDR='127.0.0.1:8080'

go run ./examples/recipes/cmd/shop migrate-up
go run ./examples/recipes/cmd/shop migrate-status
go run ./examples/recipes/cmd/shop serve
```

`migrate-up` creates the parent directory and database, applies v1 then v2 in
transactions, and is safe to replay. `serve` requires every token, the
webhook key, and listener address explicitly; it does not choose example
credentials or a hidden database path. The worker polls the durable queue and
commits the business record, outbox event, and delivery acknowledgment in one
worker transaction.

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
creates `shop_outbox`. The worker package independently installs and upgrades
`worker_jobs`. The test suite starts from both an empty database and a v1
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
delivery, outbox retry, process restart, and slow subscriber bounds. Tests run
real HTTP handlers, SQLite transactions, the durable queue, webhook
verification, and an SSE HTTP connection. The job restart test reopens the
same database in a separate test process.

- Composition, migrations, CRUD, job handler, and outbox: `shop/recipe.go`
- Typed record validator and workflow execution: `shop/recipe.go` (`node.Define`, `flow.Define`, `engine.Run`)
- Real HTTP, migration replay/upgrade/teardown, process restart, and streaming tests: `shop/recipe_test.go`
- Runnable commands and explicit environment validation: `cmd/shop/main.go`
- Expected synthetic outcomes: `shop/fixtures.json`

Run the focused evidence with `go test ./examples/recipes/...`. This recipe
does not claim exactly-once external publication: outbox consumers must
deduplicate by stable event ID if a publish succeeds but its state update does
not commit. SQLite is a local single-host store, and the in-memory SSE replay
hub starts a new epoch after process restart.
