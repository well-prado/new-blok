package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/well-prado/new-blok/observe/slo"
	"github.com/well-prado/new-blok/store/distributed"
)

// CensusName names the cluster runtime in operational metrics.
const CensusName = "cluster"

// MaxCensusReads bounds the run records one census reads.
const MaxCensusReads = 4096

// CensusParallelism bounds the census's concurrent etcd transactions.
const CensusParallelism = 16

// CensusTimeout is the cluster census source's own sampling budget.
const CensusTimeout = 5 * time.Second

// Census reports the cluster's unfinished runs, overdue timers and partition
// ownership (ADR 0022). Each partition costs one transaction (its live
// owner, its due-timer index through now and its admission-slot index), and
// up to readLimit run records are then read in batches of
// distributed.MaxStateBatch; at most CensusParallelism transactions are in
// flight. 256 partitions and 4096 runs therefore cost 256 + 64 transactions
// in about 20 sequential round trips. Runs are classified by slo.Classify:
//
//   - waiting (suspended on a signal or timer): Waiting, whether or not its
//     partition has an owner; a timer it cannot fire shows up as timer lag;
//   - accepted in an owned partition: Pending; running in an owned
//     partition: Active (a successor owner re-claims a dead owner's run);
//   - accepted or running in a partition without a live owner: Stalled.
//     Ownership lapses for up to Takeover during a normal takeover (the
//     owner TTL plus the acquire interval), so alert rules require it to
//     persist at least that long.
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
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	censuses := make([]distributed.PartitionCensus, r.limits.Partitions)
	if err := parallel(ctx, r.limits.Partitions, func(index int) error {
		census, err := r.store.CensusPartition(ctx, fmt.Sprintf("p-%04d", index), now, readLimit)
		censuses[index] = census
		return err
	}); err != nil {
		return slo.Snapshot{}, err
	}
	work := slo.Work{Source: CensusName, Takeover: r.limits.OwnerTTL + acquireRetryInterval}
	timers := slo.Timers{Source: CensusName}
	partitions := slo.Partitions{Total: r.limits.Partitions}
	// Choose which run records to read, in partition order, within the
	// bound; then read them in parallel batches.
	type batch struct {
		partition int
		ids       []string
	}
	var batches []batch
	budget := readLimit
	for index, census := range censuses {
		if census.Owned {
			partitions.Owned++
		}
		timers.Overdue += len(census.Due)
		timers.Truncated = timers.Truncated || census.Truncated && len(census.Due) == readLimit
		for _, timer := range census.Due {
			if lag := now.Sub(timer.DueAt); lag > timers.Lag {
				timers.Lag = lag
			}
		}
		ids := census.Runs
		if len(ids) > budget || census.Truncated && len(census.Runs) == readLimit {
			work.Truncated = true
		}
		ids = ids[:min(len(ids), budget)]
		budget -= len(ids)
		for start := 0; start < len(ids); start += distributed.MaxStateBatch {
			batches = append(batches, batch{partition: index, ids: ids[start:min(start+distributed.MaxStateBatch, len(ids))]})
		}
	}
	states := make([][]RunRecord, len(batches))
	if err := parallel(ctx, len(batches), func(i int) error {
		raw, err := r.store.ReadStates(ctx, fmt.Sprintf("p-%04d", batches[i].partition), batches[i].ids)
		if err != nil {
			return err
		}
		for _, data := range raw {
			if data == nil {
				continue
			}
			var record RunRecord
			if err := json.Unmarshal(data, &record); err != nil {
				return fmt.Errorf("cluster: decode run state: %w", err)
			}
			states[i] = append(states[i], record)
		}
		return nil
	}); err != nil {
		return slo.Snapshot{}, err
	}
	for i, records := range states {
		owned := censuses[batches[i].partition].Owned
		for _, record := range records {
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

// parallel runs task(0..n-1) on at most CensusParallelism goroutines and
// returns the first error; the remaining tasks see a canceled context.
func parallel(ctx context.Context, n int, task func(int) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	next := make(chan int)
	var wg sync.WaitGroup
	var once sync.Once
	var first error
	for w := 0; w < min(CensusParallelism, n); w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range next {
				if err := task(i); err != nil {
					once.Do(func() { first = err; cancel() })
				}
			}
		}()
	}
	for i := 0; i < n; i++ {
		select {
		case next <- i:
		case <-ctx.Done():
			i = n
		}
	}
	close(next)
	wg.Wait()
	if first == nil && ctx.Err() != nil {
		return ctx.Err()
	}
	return first
}

// CensusSource is Census at the sample time as an operational source with
// its own budget (CensusTimeout).
func (r *Runtime) CensusSource(readLimit int) slo.Source {
	return slo.Source{Name: CensusName, Timeout: CensusTimeout, Read: func(ctx context.Context) (slo.Snapshot, error) {
		return r.Census(ctx, time.Now().UTC(), readLimit)
	}}
}
