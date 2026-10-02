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
503. On SIGTERM the executable drains HTTP before closing dependencies; drain
expiry cancels active handlers and reports a nonzero exit. Probes bypass
admission and show readiness, active work, draining state and rejection count.
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

## Evidence limits

The SQLite format marker and integrity check do **not** prove compatibility
with retained journal checkpoints. `app.ArtifactProbe` has synthetic contract
tests; integrating it with actual restored/retained inventories is pending
#49/PR #152 (unmerged), with #48's retention behavior. #81 is not complete from
these tests. Broader #51 shutdown and #53 publication fixes and independent
review also remain with their owners. See
[ADR 0009](../../docs/decisions/0009-deployment-readiness.md).
