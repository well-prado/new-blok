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

`waits-go1.27.1-darwin-arm64.json`:

- **Setup.** 3 repetitions of 10,000 runs; Go 1.27.1 non-race test binary;
  Darwin arm64, Apple M4, 10 CPUs (GOMAXPROCS 10), 24 GiB; a shared developer
  host with other work running; journal on the host's APFS temporary directory.
- **Source revision.** The report records it. Measured after the interrupted-run
  scan moved off every sweep (#384): with it on every sweep, the same burst
  drained at about 464 runs/s, p50 8.9 ms.

| Repetition | Goroutines before → suspended → after burst | RSS KiB before → suspended | Heap in use before → suspended | Suspend 10,000 | Burst (runs/s) | p50 / p90 / p99 / max latency |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 3 → 3 → 3 | 28,832 → 36,144 | 6.2 → 6.9 MiB | 3.01 s | 4.80 s (2,083) | 1.50 / 2.00 / 4.90 / 39.4 ms |
| 2 | 3 → 3 → 3 | 44,352 → 44,384 | 6.8 → 7.1 MiB | 3.02 s | 4.86 s (2,057) | 1.51 / 1.99 / 5.41 / 48.1 ms |
| 3 | 3 → 3 → 3 | 44,640 → 44,672 | 6.9 → 7.2 MiB | 3.05 s | 5.00 s (2,001) | 1.55 / 1.98 / 5.85 / 40.4 ms |

What these show, and only this:
- **Goroutines.** 10,000 suspended runs held no goroutine: the count is the
  same before, with all suspended, and after the burst.
- **Resident memory.** It rose about 7 MiB in the first repetition, which
  includes the SQLite page cache of a 10,000-run journal. Later repetitions
  start from the memory the process already holds, and do not grow.
- **Heap.** It grew well under 1 MiB per 10,000 suspended runs.
- **Burst.** Throughput is bounded by the signalling loop: one writer
  committing 10,000 signals back to back took as long as the burst
  (`SignalNanoseconds` ≈ `BurstNanoseconds`). The resumer kept pace with the
  signals rather than draining a backlog.

These are local samples on a shared host, not a capacity, fleet, sustained
load or durability-under-load claim. Compare within a report, not across
machines.
