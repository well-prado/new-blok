package cluster

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/observe/slo"
	"github.com/well-prado/new-blok/store/distributed"
)

// CensusName names the cluster runtime in operational metrics.
const CensusName = "cluster"

// MaxCensusReads bounds the run records one census reads.
const MaxCensusReads = 4096

// Census reports the cluster's unfinished runs, overdue timers and partition
// ownership (ADR 0022). For every partition it reads the live owner, the
// bounded admission-slot index and the due-timer index through now, then up
// to readLimit run records in partition order. Runs are classified by
// slo.Classify:
//
//   - waiting (suspended on a signal or timer): Waiting, whether or not its
//     partition has an owner; a timer it cannot fire shows up as timer lag;
//   - accepted in an owned partition: Pending; running in an owned
//     partition: Active (a successor owner re-claims a dead owner's run);
//   - accepted or running in a partition without a live owner: Stalled.
//     Ownership lapses briefly during a normal takeover, so alert rules
//     require it to persist.
//
// Uncertain runs are terminal here (their admission slots are released), so
// they appear in the run outcome counters, not in the census. Reaching
// readLimit marks the work census Truncated and its counts are lower bounds;
// a full timer index page marks the timer census Truncated. The census only
// reads; it never claims, renews or writes.
func (r *Runtime) Census(ctx context.Context, now time.Time, readLimit int) (slo.Snapshot, error) {
	if now.IsZero() || readLimit < 1 || readLimit > MaxCensusReads {
		return slo.Snapshot{}, ErrInvalid
	}
	work := slo.Work{Source: CensusName}
	timers := slo.Timers{Source: CensusName}
	partitions := slo.Partitions{Total: r.limits.Partitions}
	reads := 0
	for index := 0; index < r.limits.Partitions; index++ {
		partition := fmt.Sprintf("p-%04d", index)
		owned := true
		if _, err := r.store.CurrentOwner(ctx, partition); errors.Is(err, distributed.ErrOwnershipLost) {
			owned = false
		} else if err != nil {
			return slo.Snapshot{}, err
		}
		if owned {
			partitions.Owned++
		}
		due, err := r.store.ListDueTimers(ctx, partition, now, readLimit)
		if err != nil {
			return slo.Snapshot{}, err
		}
		timers.Overdue += len(due)
		timers.Truncated = timers.Truncated || len(due) == readLimit
		for _, timer := range due {
			if lag := now.Sub(timer.DueAt); lag > timers.Lag {
				timers.Lag = lag
			}
		}
		ids, err := r.store.ListActiveRunIDs(ctx, partition, r.limits.PartitionAdmissions)
		if err != nil {
			return slo.Snapshot{}, err
		}
		for _, id := range ids {
			if reads == readLimit {
				work.Truncated = true
				break
			}
			reads++
			record, _, err := r.readRun(ctx, partition, id)
			if err != nil {
				return slo.Snapshot{}, err
			}
			switch record.State {
			case "waiting":
				work.Add(slo.Classify(slo.Observation{Waiting: true, OwnerLive: owned}))
			case "accepted":
				work.Add(slo.Classify(slo.Observation{OwnerLive: owned}))
			case "running":
				work.Add(slo.Classify(slo.Observation{Claimed: true, OwnerLive: owned}))
			}
		}
	}
	return slo.Snapshot{Work: []slo.Work{work}, Timers: []slo.Timers{timers}, Partitions: &partitions}, nil
}

// CensusSource is Census at the sample time as an operational source.
func (r *Runtime) CensusSource(readLimit int) slo.Source {
	return func(ctx context.Context) (slo.Snapshot, error) {
		return r.Census(ctx, time.Now().UTC(), readLimit)
	}
}
