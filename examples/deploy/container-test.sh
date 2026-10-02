#!/usr/bin/env bash
# Synthetic local integration only. No registry push or hosted CI.
set -euo pipefail
cd "$(dirname "$0")/../.."
docker build -f examples/deploy/Dockerfile -t new-blok-81-native .
docker build -f examples/deploy/Dockerfile --build-arg COMMAND=durable -t new-blok-81-durable .
test_dir=$(mktemp -d /tmp/new-blok-81-volume.XXXXXX)
chmod 777 "$test_dir"
native="blok81-native-$$"
durable="blok81-durable-$$"
trap 'docker rm -f "$native" "$durable" >/dev/null 2>&1 || true' EXIT
url_for() { printf 'http://%s' "$(docker port "$1" 8080/tcp)"; }
ready() {
  for attempt in {1..100}; do
    if curl -fsS "$1/readyz" >/dev/null 2>&1; then return 0; fi
    sleep 0.05
  done
  return 1
}
docker run -d --name "$native" -p 127.0.0.1::8080 new-blok-81-native >/dev/null
native_url=$(url_for "$native")
ready "$native_url"
curl -fsS "$native_url/healthz"
curl -fsS "$native_url/metrics"
quote=$(curl -fsS -X POST "$native_url/quotes" -H 'Content-Type: application/json' -d '{"sku":"coffee","quantity":2}')
[[ "$quote" == '{"totalCents":3000}' ]]
docker kill --signal=TERM "$native" >/dev/null
[[ $(docker wait "$native") == 0 ]]
docker rm "$native" >/dev/null
if docker run --rm -e BLOK_EXTERNAL=false new-blok-81-native; then echo 'unsafe bind accepted'; exit 1; fi
if docker run --rm -v "$test_dir:/data" -e BLOK_VOLUME=/data/orders.db new-blok-81-durable; then echo 'missing secret accepted'; exit 1; fi
start_durable() {
  docker run -d --name "$durable" -p 127.0.0.1::8080 -v "$test_dir:/data" -e BLOK_VOLUME=/data/orders.db -e BLOK_DEPLOY_TOKEN=synthetic-fixture-token new-blok-81-durable >/dev/null
  durable_url=$(url_for "$durable")
  ready "$durable_url"
}
start_durable
[[ $(curl -s -o /dev/null -w '%{http_code}' -X POST "$durable_url/orders" -H 'Content-Type: application/json' -d '{"requestKey":"fixture-81","sku":"coffee","quantity":2}') == 401 ]]
[[ $(curl -s -o /dev/null -w '%{http_code}' -X POST "$durable_url/orders" -H 'Authorization: Bearer synthetic-fixture-token' -H 'Content-Type: application/json' -d '{"requestKey":"fixture-81-invalid","sku":"coffee","quantity":2} {}') == 400 ]]
curl -fsS -X POST "$durable_url/orders" -H 'Authorization: Bearer synthetic-fixture-token' -H 'Content-Type: application/json' -d '{"requestKey":"fixture-81","sku":"coffee","quantity":2}'
# Stop after committed admission and before processing.
docker kill --signal=TERM "$durable" >/dev/null
[[ $(docker wait "$durable") == 0 ]]
docker rm "$durable" >/dev/null
start_durable
[[ $(curl -fsS -X POST "$durable_url/process" -H 'Authorization: Bearer synthetic-fixture-token') == '{"processed":true}' ]]
record=$(curl -fsS "$durable_url/orders/fixture-81" -H 'Authorization: Bearer synthetic-fixture-token')
[[ "$record" == *'"TotalCents":3000'* ]]
docker kill --signal=TERM "$durable" >/dev/null
[[ $(docker wait "$durable") == 0 ]]
docker rm "$durable" >/dev/null
start_durable
[[ $(curl -fsS "$durable_url/orders/fixture-81" -H 'Authorization: Bearer synthetic-fixture-token') == "$record" ]]
[[ $(curl -fsS -X POST "$durable_url/process" -H 'Authorization: Bearer synthetic-fixture-token') == '{"processed":false}' ]]
echo "PASS native bind/endpoints/quote/SIGTERM; durable auth/committed-admission/restart/retained-order/no-repeat"
echo "Synthetic volume retained at $test_dir"
