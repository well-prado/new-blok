#!/bin/bash
# Interleaved A/B of the waits workload (benchmarks/waits/README.md):
# PAIRS pairs of one repetition per arm, each arm its own process, run back
# to back with the order alternated, the host's 1-minute load average
# recorded before and after each arm. Reports land in OUT as
# p<k>-old.json / p<k>-new.json, with progress.txt listing the loads.
#
#   benchmarks/waits/ab/ab.sh <old-checkout> <new-checkout> <out-dir> [pairs]
#
# The diagnostic triples of the committed A/B file used a third arm built
# from the new checkout with diagnostic-lease-first.patch applied.
set -euo pipefail
old=$(cd "$1" && pwd); new=$(cd "$2" && pwd); out=$3; pairs=${4:-8}
mkdir -p "$out"; out=$(cd "$out" && pwd)
topology=${BLOK_BENCH_TOPOLOGY:?describe the host, CPU, disk and load}
for arm in old new; do
  dir=$([ $arm = old ] && echo "$old" || echo "$new")
  (cd "$dir" && go test -c -o "$out/waits.$arm.test" ./benchmarks/waits)
done
run() {
  arm=$1 tag=$2
  dir=$([ $arm = old ] && echo "$old" || echo "$new")
  before=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || cut -d' ' -f1 /proc/loadavg)
  (cd "$dir/benchmarks/waits" && BLOK_WAITS_REPETITIONS=1 BLOK_WAITS_MODES=${BLOK_WAITS_MODES:-burst} \
    BLOK_BENCH_SOURCE_REVISION=$(git -C "$dir" rev-parse HEAD) BLOK_BENCH_TOPOLOGY="$topology" \
    BLOK_WAITS_REPORT="$out/$tag.json" "$out/waits.$arm.test" -test.run TestWaitFootprintAndBurstSamples -test.timeout 60m > "$out/$tag.log" 2>&1)
  after=$(sysctl -n vm.loadavg 2>/dev/null | awk '{print $2}' || cut -d' ' -f1 /proc/loadavg)
  echo "$tag $arm load1m_before=$before load1m_after=$after" | tee -a "$out/progress.txt"
}
for k in $(seq 1 "$pairs"); do
  if [ $((k % 2)) = 1 ]; then run old "p$k-old"; run new "p$k-new"; else run new "p$k-new"; run old "p$k-old"; fi
done
