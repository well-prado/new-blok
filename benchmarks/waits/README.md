# E07-T09 (#332): suspended-run footprint and wakeup bursts

The workload uses the real single-host path: a SQLite file journal (WAL,
`synchronous=FULL`), the engine and `internal/resumer`, in one process. Its
program waits on `approval` and outputs the wait's outcome; no node runs, so
the samples measure suspension and resumption, not node work.

Each repetition uses a fresh journal and:

1. **Admits** N runs (`AdmitNanoseconds`).
2. **Measures the footprint before any run executes** (goroutines, resident
   memory, heap in use).
3. **Starts** every run until all N are suspended at their wait
   (`SuspendNanoseconds`; resumer: 8 workers).
4. **Measures the footprint again** with all N suspended.
5. **Signals** every run back to back from one goroutine while the resumer
   sweeps (`Interval` 20 ms, `Batch` 500, `Wake` after each signal), until
   all N complete.

Latency is per run, from its signal's commit to its completion settling; all
N raw values are recorded in microseconds. Throughput is N divided by the
time from the first signal to the last completion.

Footprint is of the whole test process:
- goroutines from `runtime.NumGoroutine`;
- resident KiB from `ps -o rss= -p PID`, an instantaneous snapshot, not a peak;
- heap in use from `runtime.MemStats.HeapInuse`.

All three are taken after `runtime.GC` and a 100 ms pause, so finished
executions have exited.

```sh
BLOK_BENCH_SOURCE_REVISION=<full-measured-source-commit> \
  BLOK_BENCH_TOPOLOGY='<host, CPU and disk, non-race build>' \
  BLOK_WAITS_REPORT=<report.json> \
  go test ./benchmarks/waits -run TestWaitFootprintAndBurstSamples -v -timeout 60m
```

`BLOK_WAITS_RUNS` (default 10000), `BLOK_WAITS_REPETITIONS` (3),
`BLOK_WAITS_WORKERS` (8) and `BLOK_WAITS_BATCH` (500) change the workload.

`TestSuspendedRunsCostStorageNotGoroutines` runs without the gate, at 1,000
runs. It fails if suspending them, or waking all of them, leaves more than 10
goroutines above the count before.

## Committed evidence

### `waits-go1.27.1-darwin-arm64.json`: the measured report

- **Setup.** 3 repetitions of 10,000 runs; Go 1.27.1 non-race test binary;
  Darwin arm64, Apple M4, 10 CPUs (GOMAXPROCS 10), 24 GiB; a shared developer
  host with other agents' work running (1-minute load average 4.9 before and
  5.6 after; 5-minute 8.5 → 7.8); journal on the host's APFS temporary
  directory.
- **Source revision.** `7d08ca9` (recorded in the report): #384's resumer
  after Review R rounds 1–3, merged with main at `d19cddc`.

| Repetition | Goroutines before → suspended → after burst | RSS KiB before → suspended | Heap in use before → suspended | Suspend 10,000 | Burst (runs/s) | p50 / p90 / p99 / max latency |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 3 → 3 → 3 | 29,136 → 36,672 | 6.2 → 7.0 MiB | 6.89 s | 9.64 s (1,037) | 2.81 / 4.58 / 30.4 / 78.6 ms |
| 2 | 3 → 3 → 3 | 44,080 → 44,160 | 6.8 → 7.1 MiB | 12.08 s | 13.19 s (758) | 3.71 / 6.16 / 59.4 / 119.0 ms |
| 3 | 3 → 3 → 3 | 37,072 → 35,600 | 7.0 → 7.2 MiB | 15.27 s | 17.78 s (562) | 2.85 / 7.70 / 72.0 / 694.4 ms |

What these show, and only this:
- **Goroutines.** 10,000 suspended runs held no goroutine: the count is the
  same before, with all suspended, and after the burst.
- **Resident memory.** It rose about 7 MiB in the first repetition, which
  includes the SQLite page cache of a 10,000-run journal. Later repetitions
  start from the memory the process already holds, and do not grow.
- **Heap.** It grew under 1 MiB per 10,000 suspended runs.
- **Burst.** It was bound by the signalling loop: one writer committing
  10,000 signals back to back took as long as the burst
  (`SignalNanoseconds` = `BurstNanoseconds` to the millisecond in every
  repetition). The resumer kept pace with the signals rather than draining a
  backlog.

Times on this host vary with its load far more than with the code: the same
old resumer ran the workload at 2,000–2,083 runs/s (suspend 3.0 s) on
2026-10-07 (load not recorded; measured at `e536f2f`, to which `ce95507` adds
only a test, fixtures and docs), and at 400–680 runs/s (suspend 8.5–21.7 s) on
2026-10-08 at load 4–17. Compare within one A/B pair, never across reports.

### `ab-ce95507-darwin-arm64.json`: old against new, interleaved

The resumer changed after the first report (#384 Review R rounds 1–3), so the
old measured revision `ce95507` (the resumer of `1fd524a`) was run against the
new one in pairs: one repetition of 10,000 runs per arm, each arm its own
process, run back to back, the order alternated from pair to pair, with the
host's 1-minute load average recorded before and after each arm. Ratios are
new ÷ old within a pair.

| Batch (new revision) | Pairs | Load (1 min) | Burst runs/s, new ÷ old: median (range) | Suspend time, new ÷ old: median (range) | p50, new ÷ old: median (range) |
| --- | --- | --- | --- | --- | --- |
| `76f5ea8` (#384 after round 3) | 8 | 29–140 | 0.95 (0.81–1.38) | 1.26 (0.93–2.24), slower in 7 of 8 | 1.17 (0.83–2.00) |
| `7d08ca9` (that, merged with main `d19cddc`) | 8 | 8–38 | 0.99 (0.31–1.21) | 1.81 (1.14–3.41), slower in 8 of 8 | 1.05 (0.80–10.2) |

What these show, and only this:
- **Footprint is unchanged.** Goroutines 3 → 3 → 3 in every arm but two
  samples that caught the database's transient goroutines (15 before
  suspension in one new arm, 11 while suspended in one old arm; 3 after the
  burst in both); resident memory and heap in use within the same ranges.
- **Burst throughput: no measured change.** The medians are 0.95 and 0.99,
  and the spread within pairs (0.31–1.38) is the host's load moving.
- **Suspension got slower: 15 of 16 pairs.** Starting 10,000 runs to their
  wait took a median 1.26× and 1.81× as long. **The cause is not isolated.**
  The two arms differ in the resumer (#384 rounds 1–3) and in the engine and
  journal changes merged since (#380 rounds 2–3, #382/#395, #404). One
  suspect was tested and not confirmed: `Start` taking a worker before its
  lease (#384 round 1). A variant of `7d08ca9` that leases first and queues on
  a worker, as `ce95507` did, was run in 4 interleaved triples at load 4–17
  (`Diagnostic` in the file); it was not consistently faster than `7d08ca9`
  (suspend 16.9/20.1/16.2/11.6 s against 14.9/13.9/16.1/23.8 s; `ce95507`
  8.5/21.7/15.3/14.1 s).

These are local samples on a shared host, not a capacity, fleet, sustained
load or durability-under-load claim.
