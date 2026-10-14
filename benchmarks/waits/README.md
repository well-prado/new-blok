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
5. **Starts `Run`** (`Interval` 20 ms, `Batch` 500) and, five intervals
   later, **measures the footprint a third time**, with all N suspended
   while `Run` sweeps over them.
6. **Signals** every run back to back from one goroutine, in one of two
   modes, until all N complete:
   - **burst**: `Run` keeps sweeping and `Wake` follows each signal;
   - **backlog**: `Run` is stopped first and no `Wake` is called, as with
     signals from another process or timers due together; once every
     signal is committed, `Run` starts and drains them.

Latency is per run, from its signal's commit to its completion settling; all
N raw values are recorded in microseconds. Throughput is N divided by the
wakeup time: from the first signal to the last completion (burst), or from
`Run`'s start to the last completion (backlog). A backlog run's latency also
includes the time its signal waited while the rest were committed.

Footprint is of the whole test process:
- goroutines from `runtime.NumGoroutine`;
- resident KiB from `ps -o rss= -p PID`, an instantaneous snapshot, not a peak;
- heap in use from `runtime.MemStats.HeapInuse`.

All three are taken after `runtime.GC` and a 100 ms pause, so finished
executions have exited.

```sh
BLOK_BENCH_SOURCE_REVISION=<full-measured-source-commit> \
  BLOK_BENCH_TOPOLOGY='<host, CPU, disk and load, non-race build>' \
  BLOK_WAITS_REPORT=<report.json> \
  go test ./benchmarks/waits -run TestWaitFootprintAndBurstSamples -v -timeout 60m
```

The gated test refuses a missing or abbreviated `BLOK_BENCH_SOURCE_REVISION`
(it must be the full 40-character commit) and a missing topology.
`BLOK_WAITS_MODES` (default `burst,backlog`), `BLOK_WAITS_RUNS` (10000),
`BLOK_WAITS_REPETITIONS` (3), `BLOK_WAITS_WORKERS` (8) and `BLOK_WAITS_BATCH`
(500) change the workload. A run that settles any way but suspended or
completed fails the measurement at once.

`TestSuspendedRunsCostStorageNotGoroutines` runs without the gate, at 1,000
runs, in both modes. It fails if suspending them, sweeping over them, or
waking all of them leaves more than 10 goroutines above the count before.

## Committed evidence

### `waits-go1.27.1-darwin-arm64.json`: the measured report

- **Setup.** 3 repetitions of 10,000 runs in each mode; Go 1.27.1 non-race
  test binary; Darwin arm64, Apple M4, 10 CPUs (GOMAXPROCS 10), 24 GiB; a
  shared developer host with other agents' work running; journal on the
  host's APFS temporary directory. The host's 1-minute load average was 47.3
  when the run started and 14.2 when it ended (sampled at those two moments
  only).
- **Source revision.** `f2ccd51` (recorded in the report): #384's resumer
  after Review R rounds 1–3, with main at `d803f3d`. **It does not include
  #413's backlog fix (PR #421).**

| Mode (rep) | Goroutines before → suspended → sweeping → after | RSS KiB before → suspended → sweeping | Heap in use | Suspend 10,000 | Signals | Wakeup (runs/s) | p50 / p90 / p99 / max latency |
| --- | --- | --- | --- | --- | --- | --- | --- |
| burst (1) | 3 → 3 → 4 → 3 | 30,256 → 35,968 → 36,368 | 6.3 → 6.9 → 6.8 MiB | 10.25 s | 12.33 s | 12.33 s (811) | 3.8 / 5.5 / 10.2 / 36.9 ms |
| burst (2) | 3 → 3 → 4 → 3 | 53,888 → 47,344 → 47,344 | 7.1 → 7.3 → 7.1 MiB | 18.01 s | 13.38 s | 13.38 s (747) | 4.1 / 9.2 / 64.8 / 116.3 ms |
| burst (3) | 3 → 3 → 4 → 3 | 64,752 → 64,752 → 64,784 | 7.5 → 7.7 → 7.5 MiB | 4.40 s | 6.66 s | 6.66 s (1,502) | 2.0 / 2.7 / 5.1 / 19.2 ms |
| backlog (1) | 3 → 3 → 4 → 3 | 40,496 → 40,640 → 42,704 | 6.9 → 7.2 → 7.1 MiB | 10.33 s | 3.01 s | 76.50 s (131) | 53.5 / 74.1 / 76.3 / 76.5 s |
| backlog (2) | 3 → 3 → 4 → 3 | 47,200 → 50,256 → 50,256 | 7.2 → 7.3 → 7.3 MiB | 13.17 s | 3.68 s | 73.18 s (137) | 56.7 / 71.0 / 73.0 / 73.2 s |
| backlog (3) | 3 → 3 → 4 → 3 | 64,816 → 64,816 → 64,816 | 7.6 → 7.8 → 7.6 MiB | 5.52 s | 1.39 s | 39.82 s (251) | 27.4 / 37.5 / 39.6 / 39.8 s |

What these show, and only this:
- **Goroutines.** 10,000 suspended runs held no goroutine: 3 before, 3
  suspended, 3 after. While `Run` swept over them the count was 4; the one
  more is `Run`'s own goroutine. (This report took one sweeping sample; the
  test now keeps the fewest of five, so a sweep in flight with its database
  goroutines does not count.)
- **Resident memory.** It rose about 6 MiB in the first repetition, which
  includes the SQLite page cache of a 10,000-run journal. Later repetitions
  start from the memory the process already holds.
- **Heap.** It grew under 1 MiB per 10,000 suspended runs, sweeping or not.
- **Burst.** It was bound by the signalling loop: the last completion came
  1.4–3.8 ms after the last signal's commit. The resumer kept pace with the
  signals rather than draining a backlog.
- **Backlog.** 10,000 woken runs drained at **131–251 runs/s**, 4–6× slower
  than the same runs woken one at a time. Two causes are known, neither fixed
  here:
  - `Run` swept only on its interval tick and took only the workers free at
    that moment: a cap of `Workers / Interval` (here 400 runs/s; 4 runs/s at
    the defaults). #413 (PR #421) removes it.
  - Each `PendingResumptions` call reads every fired wait before applying its
    limit, so a listing costs O(backlog) (#422). In a preview, #421's resumer
    drained the same backlog at 188–226 runs/s against 147–149 runs/s
    without it (one repetition each, interleaved, load 7–21): the listing,
    not the tick, is what remains.
  This arm is re-measured once #413 merges. Until then #46 V4 ("measure idle
  footprint and wakeup bursts") is **not tick-ready**: bursts and footprint
  are measured, a backlog drains at the rate above.

Load is the most likely reason times on this host vary between days: the
same old resumer ran the burst at 2,000–2,083 runs/s on 2026-10-07 (load not
recorded; measured at `e536f2f`, to which `ce95507` adds only a test,
fixtures and docs) and at 400–680 runs/s on 2026-10-08 at load 4–17. That is
a hypothesis, not a measurement: nothing else was held fixed between the
days. Compare within one A/B pair, never across reports.

### `ab/ab-ce95507-darwin-arm64.json`: old against new, interleaved

The resumer changed after the first report (#384 Review R rounds 1–3), so the
old measured revision `ce95507` (the resumer of `1fd524a`) was run against the
new one in pairs, burst mode only: one repetition of 10,000 runs per arm,
each arm its own process, run back to back, the order alternated from pair to
pair, with the host's 1-minute load average sampled just before and just
after each arm (not during it). Ratios are new ÷ old within a pair; above 1
means higher (faster for runs/s, slower for the times and latencies).

| Batch (new revision) | Pairs | Load (1 min) | Burst runs/s, new ÷ old: geomean, median (range) | Suspend time: geomean, median (range) | p50: geomean, median (range) | p99: geomean, median (range) |
| --- | --- | --- | --- | --- | --- | --- |
| `76f5ea8` (#384 after round 3) | 8 | 29–140 | 0.97, 0.95 (0.81–1.38) | 1.38, 1.26 (0.93–2.24), above 1 in 7 of 8 | 1.21, 1.17 (0.83–2.00) | 1.86, 2.05 (0.71–4.39), above 1 in 6 of 8 |
| `7d08ca9` (that, merged with main `d19cddc`) | 8 | 8–38 | 0.79, 0.99 (0.31–1.21) | 1.91, 1.81 (1.14–3.41), above 1 in 8 of 8 | 1.48, 1.05 (0.80–10.2) | 2.38, 1.45 (0.13–33.3), above 1 in 6 of 8 |

What these show, and only this:
- **Footprint is unchanged.** Goroutines 3 → 3 → 3 in every arm but two
  samples that caught the database's transient goroutines (15 before
  suspension in one new arm, 11 while suspended in one old arm; 3 after the
  burst in both); resident memory and heap in use within the same ranges.
- **Burst throughput: no measured change by median** (0.95, 0.99). The
  geomean of the second batch (0.79) is pulled down by two pairs whose load
  rose during the new arm (pairs 1 and 5).
- **Suspension got slower: 15 of 16 pairs** (median 1.26× and 1.81×), and
  **p99 latency rose in 12 of 16** (median 2.05× and 1.45×). **The causes
  are not isolated** and are tracked in #410. The two arms differ in the
  resumer (#384 rounds 1–3) and in the engine and journal changes merged
  since (#380 rounds 2–3, #382/#395, #404). One suspect was tested and not
  confirmed: `Start` taking a worker before its lease (#384 round 1). A
  variant of `7d08ca9` that leases first and queues on a worker, as `ce95507`
  did (`ab/diagnostic-lease-first.patch`), was run in 4 interleaved triples at
  load 4–17 (`Diagnostic` in the file); it was not consistently faster than
  `7d08ca9` (suspend 16.9/20.1/16.2/11.6 s against 14.9/13.9/16.1/23.8 s;
  `ce95507` 8.5/21.7/15.3/14.1 s).

Reproduce with `benchmarks/waits/ab/ab.sh <old-checkout> <new-checkout>
<out-dir> [pairs]` (`BLOK_BENCH_TOPOLOGY` required; burst mode by default,
`BLOK_WAITS_MODES` to change it). The triples' third arm is the new checkout
with `git apply benchmarks/waits/ab/diagnostic-lease-first.patch`.

These are local samples on a shared host, not a capacity, fleet, sustained
load or durability-under-load claim.
