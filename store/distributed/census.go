package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// MaxStateBatch bounds ReadStates; it stays under etcd's default
// --max-txn-ops (128).
const MaxStateBatch = 64

// PartitionCensus is one partition's operational state, read in one
// transaction (ADR 0022).
type PartitionCensus struct {
	// Owned reports a live owner lease.
	Owned bool
	// Due are the timers due through the census time, at most the limit.
	Due []DueTimer
	// Runs are the run IDs holding an admission slot, at most the limit.
	Runs []string
	// Truncated reports that Due or Runs was cut at the limit.
	Truncated bool
}

// CensusPartition reads a partition's live owner, its due timers through now
// and its admission-slot index in one linearizable transaction guarded by the
// current incarnation: one round trip instead of three. It only reads.
func (s *Store) CensusPartition(ctx context.Context, partition string, now time.Time, limit int) (PartitionCensus, error) {
	if !validName(partition) || now.IsZero() || limit < 1 || limit > 4096 {
		return PartitionCensus{}, errors.New("distributed store: census requires a valid partition, time and bounded limit")
	}
	timers := timerIndexPrefix(partition)
	timersEnd := timers + fmt.Sprintf("%020d/\xff", now.UTC().UnixNano())
	response, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(
			clientv3.OpGet(ownerKey(s.incarnation, partition)),
			clientv3.OpGet(timers, clientv3.WithRange(timersEnd), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(int64(limit+1))),
			clientv3.OpGet(globalSlotsPrefix(partition), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(int64(limit+1))),
		).Commit()
	if err != nil {
		return PartitionCensus{}, fmt.Errorf("distributed store: partition census: %w", err)
	}
	if !response.Succeeded {
		return PartitionCensus{}, ErrIncarnation
	}
	var census PartitionCensus
	if owner := response.Responses[0].GetResponseRange().Kvs; len(owner) > 0 {
		candidate := Owner{Partition: partition, ID: string(owner[0].Value), Token: owner[0].CreateRevision, Incarnation: s.incarnation, LeaseID: clientv3.LeaseID(owner[0].Lease)}
		census.Owned = candidate.valid()
	}
	for _, entry := range response.Responses[1].GetResponseRange().Kvs {
		if len(census.Due) == limit {
			census.Truncated = true
			break
		}
		var due int64
		if _, err := fmt.Sscanf(strings.TrimPrefix(string(entry.Key), timers), "%d/", &due); err != nil || !validName(string(entry.Value)) {
			return PartitionCensus{}, errors.New("distributed store: malformed timer index entry")
		}
		census.Due = append(census.Due, DueTimer{StateID: string(entry.Value), DueAt: time.Unix(0, due).UTC()})
	}
	for _, entry := range response.Responses[2].GetResponseRange().Kvs {
		if len(census.Runs) == limit {
			census.Truncated = true
			break
		}
		if !validName(string(entry.Value)) {
			return PartitionCensus{}, errors.New("distributed store: active-run slot contains an invalid run ID")
		}
		census.Runs = append(census.Runs, string(entry.Value))
	}
	return census, nil
}

// ReadStates reads up to MaxStateBatch projections of a partition in one
// linearizable transaction guarded by the current incarnation. A missing
// projection is nil.
func (s *Store) ReadStates(ctx context.Context, partition string, stateIDs []string) ([][]byte, error) {
	if !validName(partition) || len(stateIDs) > MaxStateBatch {
		return nil, errors.New("distributed store: batched read requires a valid partition and at most MaxStateBatch states")
	}
	if len(stateIDs) == 0 {
		return nil, nil
	}
	ops := make([]clientv3.Op, len(stateIDs))
	for i, id := range stateIDs {
		if !validName(id) {
			return nil, errors.New("distributed store: batched read requires valid state IDs")
		}
		ops[i] = clientv3.OpGet(projectionKey(partition, id))
	}
	response, err := s.client.Txn(ctx).If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).Then(ops...).Commit()
	if err != nil {
		return nil, fmt.Errorf("distributed store: batched state read: %w", err)
	}
	if !response.Succeeded {
		return nil, ErrIncarnation
	}
	out := make([][]byte, len(stateIDs))
	for i := range stateIDs {
		if kvs := response.Responses[i].GetResponseRange().Kvs; len(kvs) > 0 {
			out[i] = append([]byte(nil), kvs[0].Value...)
		}
	}
	return out, nil
}
