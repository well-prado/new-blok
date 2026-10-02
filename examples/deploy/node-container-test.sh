#!/usr/bin/env bash
# Actual selected Node deployment. All data/credentials here are synthetic.
set -euo pipefail
cd "$(dirname "$0")/../.."
docker build -f examples/deploy/Dockerfile.node -t new-blok-81-node .
name="blok81-node-$$"
token=synthetic-deployment-token-000000001
fault_dir=$(mktemp -d /tmp/new-blok-81-worker.XXXXXX)
trap 'docker rm -f "$name" >/dev/null 2>&1 || true' EXIT
url_for() { printf 'http://%s' "$(docker port "$name" 8080/tcp)"; }
code() { curl --max-time 5 -s -o /dev/null -w '%{http_code}' "$1"; }
start() {
 docker run -d --name "$name" -p 127.0.0.1::8080 -e BLOK_WORKER_TOKEN="$token" "$@" new-blok-81-node >/dev/null
 url=$(url_for)
 for attempt in {1..100}; do if [[ $(code "$url/readyz" || true) == 200 ]]; then return; fi; sleep 0.05; done
 docker logs "$name"; return 1
}
quote() { curl --max-time 5 -fsS -X POST "$url/quotes" -H 'Content-Type: application/json' -d "{\"sku\":\"coffee\",\"quantity\":2,\"delayMs\":$1}"; }
stop() { docker kill --signal=TERM "$name" >/dev/null; [[ $(docker wait "$name") == 0 ]]; docker rm "$name" >/dev/null; }
if docker run --rm new-blok-81-node; then echo 'missing worker secret accepted'; exit 1; fi
if docker run --rm -e BLOK_WORKER_TOKEN="$token" -e PATH=/missing new-blok-81-node; then echo 'missing worker executable accepted'; exit 1; fi
# Compiled Go catalog must disagree with this deliberately changed worker catalog.
node -e 'const fs=require("fs");const d=JSON.parse(fs.readFileSync("examples/deploy/node-catalog.json"));d[0].description="synthetic incompatible catalog";fs.writeFileSync(process.argv[1],JSON.stringify(d))' "$fault_dir/catalog.json"
if docker run --rm -e BLOK_WORKER_TOKEN="$token" -v "$fault_dir/catalog.json:/src/examples/deploy/node-catalog.json:ro" new-blok-81-node; then echo 'incompatible catalog accepted'; exit 1; fi
start -e BLOK_WORKER_ADDRESS=127.0.0.1:9002
[[ $(quote 0) == '{"totalCents":3000}' ]]
[[ $(curl -s -o /dev/null -w '%{http_code}' -X POST "$url/quotes" -H 'Content-Type: application/json' -d '{"sku":"coffee","quantity":0,"delayMs":0}') == 400 ]]
# Hold both slots; probes stay accessible and excess admission is rejected.
quote 1500 >"$fault_dir/one" & first=$!
quote 1500 >"$fault_dir/two" & second=$!
for attempt in {1..100}; do metrics=$(curl -fsS "$url/metrics"); if [[ "$metrics" == *'blok_active 2'* ]]; then break; fi; sleep 0.01; done
[[ "$metrics" == *'blok_active 2'* ]]
[[ $(code "$url/healthz") == 200 && $(code "$url/readyz") == 200 ]]
[[ $(curl -s -o /dev/null -w '%{http_code}' -X POST "$url/quotes" -H 'Content-Type: application/json' -d '{"sku":"coffee","quantity":2,"delayMs":0}') == 503 ]]
[[ $(curl -fsS "$url/metrics") == *'blok_admission_rejected_total 1'* ]]
# SIGTERM must let the two accepted requests finish and then close the worker.
docker kill --signal=TERM "$name" >/dev/null
wait "$first"; wait "$second"
[[ $(docker wait "$name") == 0 ]]
[[ $(<"$fault_dir/one") == '{"totalCents":3000}' && $(<"$fault_dir/two") == '{"totalCents":3000}' ]]
docker rm "$name" >/dev/null
start
# Kill only the owned child Node worker while the Go executable remains alive.
docker exec "$name" node -e 'const fs=require("fs");for(const p of fs.readdirSync("/proc")){if(/^\d+$/.test(p)){try{const cmd=fs.readFileSync(`/proc/${p}/cmdline`,"utf8").split("\0");if(cmd[1]==="runtime/nodejs/dist/runtime/nodejs/main.js")process.kill(Number(p),"SIGKILL")}catch{}}}'
[[ $(code "$url/healthz") == 200 && $(code "$url/readyz") == 503 ]]
[[ $(curl -fsS "$url/metrics") == *'blok_ready 0'* ]]
[[ $(curl -s -o /dev/null -w '%{http_code}' -X POST "$url/quotes" -H 'Content-Type: application/json' -d '{"sku":"coffee","quantity":2,"delayMs":0}') == 503 ]]
stop
start
[[ $(quote 0) == '{"totalCents":3000}' ]]
stop
# Force the drain deadline while a cooperative worker call is active.
start -e BLOK_DRAIN_TIMEOUT=50ms
quote 2000 >"$fault_dir/timeout" 2>/dev/null & pending=$!
for attempt in {1..100}; do if [[ $(curl -fsS "$url/metrics") == *'blok_active 1'* ]]; then break; fi; sleep 0.01; done
docker kill --signal=TERM "$name" >/dev/null
if wait "$pending"; then echo 'deadline did not cancel active request'; exit 1; fi
[[ $(docker wait "$name") == 1 ]]
echo 'PASS actual Go/Node quote, missing secret/executable, incompatible catalog, overload/probes, SIGTERM drain/deadline, worker kill/readiness, restart'
echo "Synthetic fault evidence retained at $fault_dir"
