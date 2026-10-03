# ADR 0009: Cron schedules with durable occurrence identity

- Status: accepted
- Date: 2026-10-03
- Roadmap: E09-T03 ([#56](https://github.com/well-prado/new-blok/issues/56))
- Builds on: [ADR 0005](0005-trigger-adapter-contract.md), [ADR 0006](0006-durable-submission-and-webhooks.md)

## Context

Clocks are not monotonic. They repeat an hour in the autumn, skip one in the
spring, are corrected by NTP, and stop entirely while a process is down. A
scheduler that "runs at 02:30" must say what that means on those days. It
must never run the same occurrence twice, and must not keep one sleeping
goroutine per schedule.

## Decision

`trigger/cron` declares durable / redeliver / trusted-producer.

### Expressions and time zones

- **Expressions:** five fields (minute hour day-of-month month day-of-week),
  with `*`, numbers, ranges, lists and steps. Day-of-week 0 and 7 are
  Sunday. When both day fields are restricted, a day matches if either does
  (classic cron). A day field is unrestricted only when it is exactly `*`.
  Vixie cron instead treats any day field starting with `*` (such as `*/2`)
  as unrestricted and then requires both fields; here `*/2` is a
  restriction like `1-31/2`, so `0 0 */10 * 1` fires on days 1, 11, 21 and
  31 *or* on Mondays. Macros: `@yearly`/`@annually`, `@monthly`, `@weekly`,
  `@daily`/`@midnight`, `@hourly`.
- **Refused at parse:** out-of-range values, zero steps, reversed ranges,
  empty list items, and expressions that can never fire (`0 0 31 2 *`).
- **Time zone:** every schedule names an IANA zone. `Local` and an empty
  zone are refused, so a deployment's host settings cannot change when work
  runs.

### Daylight saving, deterministic

| Situation | Policy | Behavior |
| --- | --- | --- |
| Spring-forward gap (wall time does not exist) | `skip` (default) | No firing that day. |
| | `shift` | One firing at the transition instant; several gap times collapse into it. |
| Fall-back fold (wall time occurs twice) | `once` (default) | The earlier instant. |
| | `twice` | Both instants, in instant order. |

Firings are ordered by instant, including folds across midnight: Goose Bay
fell back from 00:01 to 23:01 until 2011, so midnight's first instant comes
before the repeated 23:10. The fixture corpus covers New York, London, Lord
Howe (30-minute DST), Kolkata (+05:30, no DST), Tokyo, Goose Bay, leap days
and the 2100 non-leap year. Go's time package has no leap seconds, so none
are scheduled.

**How the next firing is found.** Between two zone transitions the offset
is constant, so each wall-clock time is one instant and wall-clock order is
instant order. `Next` scans the hour and minute bitsets forward from the
current wall-clock time instead of enumerating whole days. A firing within
48 hours of a transition is confirmed against the exact mapping, which probes
the offsets 36 hours either side of a wall-clock time and applies the gap and
fold policies. A spring-forward gap adds its shifted firing at the
transition. Go's `ZoneBounds` reports transitions, except on the last UTC
day of a leap year in a zone extended by a TZ rule (America/Fort_Wayne on
2008-12-31, America/New_York on 2040-12-31). There it answers with a period
that ended before its input, so it is checked: the next offset change is
then found by hourly probes and bisection, and every firing is confirmed.

`Next` is checked against an exact brute-force reference. The reference maps
every scheduled wall-clock time of consecutive dates through the exact
mapping and stops only when no later date can hold an earlier instant. The
check covers every transition 1970–2040 in every zone of Go's time zone
database, six expressions and all four policy combinations. The reference
caught the Goose Bay ordering, which the previous two-date merge got wrong.

### Occurrence identity and the cursor

- **Identity:** an occurrence is its UTC instant. It is submitted through
  `trigger.Submitter` under `cron:<schedule>:<RFC 3339 instant>`, with the
  schedule's payload (validated and normalized against its schema when the
  schedule is added) and its configured principal.
- **Cursor:** each schedule has a persisted cursor, the last handled
  occurrence, which never moves backwards. A clock that jumps backwards
  therefore cannot re-fire anything, and a duplicate tick finds nothing new.
- **Write order:** cursor writes happen after the tick's submissions, in one
  transaction per tick. A crash between submission and cursor only repeats
  submissions, which deduplicate. Process-kill tests cover before submit,
  after submit and after the cursor write.
- **Redefinition:** a schedule's definition digest is stored with its
  cursor. It covers the parsed expression, zone, kind, normalized payload,
  parsed input schema, principal and policies, so whitespace, key order and
  macros do not change a definition. Re-adding the same definition resumes
  it; a different definition under the same name, a changed schema included,
  is refused (`ErrConflict`), so a renamed or changed schedule never inherits
  a stale cursor. A new schedule starts from now and does not backfill.

### Downtime, overlap and the scheduler loop

- **On time and late:** an occurrence handled within `LateAfter` (one
  minute, inclusive) of its instant is on time and always handled. A later
  one is late: it was missed, typically during downtime. An on-time
  occurrence whose tick fails stays on time while it is retried, so a store
  outage longer than a minute delays it instead of dropping it. Up to 100
  such occurrences are kept per schedule, in memory: after a restart they are
  late.
- **Catch-up:** of the late occurrences, the `MaxCatchUp` most recent are
  submitted. That is 1 when the field is left zero, none with `NoCatchUp`,
  and up to 100. The rest are counted in `Result.Missed`, exactly up to
  10 000 and clamped there, and are skipped. Once the count reaches the
  clamp, the scan jumps close to now rather than enumerating every missed
  firing: a year of minutely firings takes a few milliseconds.
- **Overlap** applies when an occurrence is due while the previous one's
  work has not settled. Settled means finished or dead-lettered, as reported
  by a `Tracker`, which `worker.Queue` implements as `Settled`:
  - `allow` (default): submits regardless;
  - `skip`: drops the occurrence and records it;
  - `coalesce`: holds only the newest occurrence, persisted with the cursor,
    and submits it once the previous work settles.
- **One goroutine:** `Run` ticks, then waits until the earliest next firing,
  a held occurrence's 1 s recheck, a retry, or a newly added schedule. It
  never keeps one goroutine per schedule. Restart rebuilds every next firing
  from the stored cursors. `AddAll` registers many schedules in one
  transaction, without blocking ticks. It writes before it reads, so it waits
  for a concurrent cursor write under SQLite's busy timeout instead of
  failing with `SQLITE_BUSY`.
- **Failures:** a schedule whose submission or tracker fails is retried
  with backoff (1 s, doubling to 5 minutes) while the others keep running,
  and `Run` keeps looping. A failed cursor write is kept and retried by the
  next tick; until then a crash would only repeat submissions, which
  deduplicate. Each tick's results and errors go to the optional `report`
  callback of `Run`.
- **Concurrency:** ticks are serialized, so a host may call `Tick` while
  `Run` is running without duplicating work. A host `Tick` wakes `Run`, since
  it may have changed what `Run` waits for.

## Compatibility

| Change | Class | Migration |
| --- | --- | --- |
| New package `trigger/cron`, table `cron_cursors` | additive | created by `cron.New` |
| `worker.Queue.Settled` | additive | none |

## Limits

- **Measured profile.** On the development host (linux/arm64 container,
  `golang:1.27.1`, in-memory submitter, no race detector):
  - `Next` takes 0.2–0.3 µs, whether the schedule is minutely, `*/5` or
    hourly (under 1 µs near a transition).
  - 10 000 hourly schedules across five zones add in about 0.2 s (20 µs
    each) and use about 3.6 KB of heap each. A tick in which all 10 000
    fire takes about 70 ms.
  - 1000 dense schedules in America/New_York (half minutely, half `*/5`):
    a steady-state tick takes about 10 ms, and the first tick after three
    days down takes about 0.55 s.

  With the SQLite queue as the submitter, every submission is also its own
  transaction.
- **A corrected clock leaves schedules silent.** If the clock is stepped
  forward and then corrected, the occurrences due at the wrong time have
  fired, and the cursor, which never moves backwards, sits ahead of the
  clock. The schedule is silent until the clock passes the cursor again,
  and every tick reports how far behind the clock is (`Result.Behind`) so the
  host can alert. Bounding the cursor by monotonic time was rejected: a
  suspended host and a legitimate NTP step look the same as a wrong clock,
  and would stop legitimate catch-up.
- **No seconds field, no names.** No seconds field, no month or day names,
  and none of `L`/`W`/`#`.
- **Shift collapsing.** `shift` collapses several gap firings into one
  occurrence by design.
- **One scheduler per store.** A cursor table is owned by one running
  scheduler; running two over the same store needs the distributed
  ownership in E18.
