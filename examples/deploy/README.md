# Application-owned deployment examples (#81)

Run from the repository root. The native image owns one Go executable and
selects no worker/store. The durable image adds SQLite orders; the Node image
owns a Go executable plus one persistent authenticated child worker.

```sh
bash examples/deploy/container-test.sh
bash examples/deploy/node-container-test.sh
```

Both suites build local images and use synthetic input only. They publish HTTP
on a random **host loopback** port; nothing is pushed. The Node image fixes its
private gRPC listener to loopback (`127.0.0.1:9001` by default), which is not exposed by Docker.
The worker catalog is embedded into Go and shared with the Node module; changing
the catalog produces a failed startup negotiation. Worker death withdraws
readiness while `/healthz` remains available. Restarting the container creates a
fresh worker. Memory workflows do not resume after process death.
The native/durable test driver uses Python's SQLite library already present in
the pinned Go build image to inject faults into its synthetic mounted volume.
It runs beside the application in Linux for coherent SQLite WAL access; the
deployed Go-only scratch image selects no foreign runtime.

Build/run manually:

```sh
docker build -f examples/deploy/Dockerfile.node -t new-blok-81-node .
docker run --rm -p 127.0.0.1:8080:8080 \
  -e BLOK_WORKER_TOKEN=synthetic-deployment-token-000000001 new-blok-81-node
curl -fsS http://127.0.0.1:8080/readyz
curl -fsS http://127.0.0.1:8080/metrics
curl -fsS -X POST http://127.0.0.1:8080/quotes \
  -H 'Content-Type: application/json' \
  -d '{"sku":"coffee","quantity":2,"delayMs":0}'
# {"totalCents":3000}
```

For real deployments inject a protected worker credential (32–4096 bytes),
not the synthetic fixture value. Worker credentials never appear in HTTP probe
errors or startup diagnostics. HTTP quote authentication is an application
policy not supplied by this synthetic example; restrict its listener to the
intended network. The durable endpoints require `BLOK_DEPLOY_TOKEN`.

Operational settings: `BLOK_LISTEN` (literal IP and port), `BLOK_EXTERNAL`
(explicit external-bind opt-in), `BLOK_MAX_ADMISSION` (Node default 2, maximum
32), and `BLOK_DRAIN_TIMEOUT` (default 5s). Saturation rejects immediately with
503. On SIGTERM the executable drains HTTP before closing dependencies; only
admitted requests hold the drain, so connections that have not sent a request
are closed at once. If the
HTTP drain expires, it cancels active handlers with `application_drain_timeout`
and allows the application's configured `AbortGrace` (1s by default) for
cooperative cleanup. The total handler-drain bound is the HTTP drain timeout
plus that grace. Trusted handler code that ignores cancellation can still be
running when dependencies close after the grace, and the process reports a
nonzero exit. Probes bypass admission and show readiness, active work, draining
state and rejection count.
Probe RPCs have a 250ms context; repeated probes consume the worker's bounded
identity budget (8192 Node replay entries, two per RPC), so schedule container
recycling and avoid excessive probing.
`BLOK_WORKER_ADDRESS` may select another nonzero port on `127.0.0.1`;
other addresses (including IPv6), hostnames and port-zero binds are rejected
to match the current selected Node adapter's supported transport.
Use Docker CPU/memory/PID bounds appropriate to the application's measured
workload; these tests establish behavior, not a production capacity envelope.

For the durable image, mount an operator-owned writable directory at `/data`
and set `BLOK_VOLUME=/data/orders.db`. The scratch image runs as uid/gid 65532;
set volume ownership accordingly. One process owns the example volume. Retaining
the directory survives process/container restart, not disk loss or multi-host
failover. The test retains its synthetic directory and prints its path.

The Go-only scratch image requires no foreign process, certificate bundle,
broker or hosted service. The Node image pins Node 22.18.0 and its dependency
lock; SDK checks also run on host Node 24.21.0. Reproducible signed releases,
SBOMs, and platform certification belong to their own roadmap issues.

## Retained journal readiness and limits

The durable composition now uses the real SQLite journal inventory from runs,
checkpoints and registered artifact manifests. Every new 202 order response
follows journal admission, queue admission and checkpoint commits. Readiness
checks retained identities against the executable's actual bytes, composition,
schemas, embedded Go build/dependency information and supported checkpoint
codec. It checks admitted runs before their first checkpoint too. Missing
artifacts or incompatible identities/codecs reject cold startup before binding;
live faults return readiness/admission 503 with no additional journal, queue
or business records. Startup does not regenerate missing retained artifacts.

The application supports one executable identity. A rebuilt/different binary
is refused while incompatible journal records remain; retain the original
binary and volume for restart. Manifest registration alone cannot provide an
older executable. Queue processing and outbox atomicity remain unchanged.
Unacknowledged partial admission can be retried using the same request key;
the three commits do not constitute one atomic journal/queue transaction.
Handoff checkpoints remain retained after processing. Automatic engine replay,
multi-version execution, compaction policy and production capacity are outside
this example. Old queue-only volumes have no journal compatibility evidence
for their pre-existing jobs. Build identities are not signed provenance.

The focused suite covers actual SQLite cold restart and backup/restore plus
manifest, executable and checkpoint faults. The container suite also corrupts
the mounted journal and proves live zero admission and cold startup rejection.
Independent review remains required. Native Windows evidence is pending #156;
these Linux container/SIGTERM runs make no Windows support claim. See
[ADR 0009](../../docs/decisions/0009-deployment-readiness.md) and
[local evidence](EVIDENCE.md).
