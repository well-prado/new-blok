package clustertest

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// WaitHealthy waits until every endpoint is a healthy member of one cluster:
// each answers a status request naming the same, non-zero leader, reports no
// errors, and serves a linearizable read through itself (so it reaches the
// leader and has applied up to the current commit). The condition must hold
// on two consecutive probes. It returns the last reason it was unhealthy when
// ctx ends first.
func WaitHealthy(ctx context.Context, endpoints []string) error {
	endpoints = normalize(endpoints)
	if len(endpoints) == 0 {
		return errors.New("no endpoints to probe")
	}
	clients := make([]*clientv3.Client, len(endpoints))
	defer func() {
		for _, client := range clients {
			if client != nil {
				_ = client.Close()
			}
		}
	}()
	for index, endpoint := range endpoints {
		client, err := clientv3.New(clientv3.Config{Endpoints: []string{endpoint}, DialTimeout: time.Second})
		if err != nil {
			return fmt.Errorf("client for %s: %w", endpoint, err)
		}
		clients[index] = client
	}
	var lastErr error
	var stableLeader uint64
	for {
		leader, err := probe(ctx, endpoints, clients)
		switch {
		case err != nil:
			lastErr, stableLeader = err, 0
		case leader == stableLeader:
			return nil
		default:
			stableLeader = leader
		}
		select {
		case <-ctx.Done():
			if lastErr == nil {
				lastErr = ctx.Err()
			}
			return lastErr
		case <-time.After(250 * time.Millisecond):
		}
	}
}

// probe checks every endpoint once and returns the leader they agree on.
func probe(ctx context.Context, endpoints []string, clients []*clientv3.Client) (uint64, error) {
	var leader uint64
	for index, endpoint := range endpoints {
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		status, err := clients[index].Status(probeCtx, endpoint)
		if err == nil {
			_, err = clients[index].Get(probeCtx, "/blok-clustertest/health")
		}
		cancel()
		switch {
		case err != nil:
			return 0, fmt.Errorf("%s: %w", endpoint, err)
		case status.Leader == 0:
			return 0, fmt.Errorf("%s: no leader", endpoint)
		case len(status.Errors) > 0:
			return 0, fmt.Errorf("%s: member errors %v", endpoint, status.Errors)
		case leader != 0 && status.Leader != leader:
			return 0, fmt.Errorf("%s: leader %x, other members report %x", endpoint, status.Leader, leader)
		}
		leader = status.Leader
	}
	return leader, nil
}
