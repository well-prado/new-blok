package clustertest

import (
	"context"
	"errors"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	// healthInterval is the pause between probes.
	healthInterval = 250 * time.Millisecond
	// healthSpan is how long the same leader and term must hold before the
	// cluster counts as healthy. It spans more than one election timeout
	// (etcd's default is 1s), so a member that is about to call an election
	// (a re-election of the same leader included, which bumps the term) is
	// seen doing it.
	healthSpan = 1500 * time.Millisecond
)

// memberStatus is what one endpoint reported in one probe.
type memberStatus struct {
	endpoint string
	leader   uint64
	term     uint64
	errors   []string
	err      error
}

// clusterState is what every member agreed on in one probe.
type clusterState struct {
	leader uint64
	term   uint64
}

// clock lets the stability rule be tested without waiting in real time.
type clock interface {
	Now() time.Time
	Sleep(ctx context.Context, d time.Duration) error
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// WaitHealthy waits until every endpoint is a healthy member of one cluster:
// each answers a status request naming the same, non-zero leader in the same
// Raft term, reports no errors, and serves a linearizable read through itself
// (so it reaches the leader and has applied up to the current commit). That
// leader and term must hold on every probe for at least healthSpan, longer
// than one election timeout. It returns the last reason it was unhealthy
// when ctx ends first.
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
	probe := func(ctx context.Context) (clusterState, error) {
		return agree(gather(ctx, endpoints, clients))
	}
	return waitStable(ctx, probe, healthInterval, healthSpan, realClock{})
}

// waitStable probes until the same agreed state has been observed on every
// probe for at least span. A failed probe or a changed leader or term starts
// the span again.
func waitStable(ctx context.Context, probe func(context.Context) (clusterState, error), interval, span time.Duration, clock clock) error {
	var lastErr error
	var stable clusterState
	var since time.Time
	observed := false
	for {
		state, err := probe(ctx)
		now := clock.Now()
		switch {
		case err != nil:
			lastErr, observed = err, false
		case !observed || state != stable:
			if observed {
				lastErr = fmt.Errorf("leader %x term %d changed to leader %x term %d", stable.leader, stable.term, state.leader, state.term)
			}
			stable, since, observed = state, now, true
		case now.Sub(since) >= span:
			return nil
		}
		if sleepErr := clock.Sleep(ctx, interval); sleepErr != nil {
			if lastErr == nil {
				lastErr = fmt.Errorf("leader %x term %d not yet stable for %s: %w", stable.leader, stable.term, span, sleepErr)
			}
			return lastErr
		}
	}
}

// gather asks every endpoint, through its own client, for its status and a
// linearizable read.
func gather(ctx context.Context, endpoints []string, clients []*clientv3.Client) []memberStatus {
	statuses := make([]memberStatus, len(endpoints))
	for index, endpoint := range endpoints {
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		status, err := clients[index].Status(probeCtx, endpoint)
		if err == nil {
			_, err = clients[index].Get(probeCtx, "/blok-clustertest/health")
		}
		cancel()
		statuses[index] = memberStatus{endpoint: endpoint, err: err}
		if err == nil {
			statuses[index].leader, statuses[index].term, statuses[index].errors = status.Leader, status.RaftTerm, status.Errors
		}
	}
	return statuses
}

// agree returns the leader and term every member reports, or why they do not
// form one healthy cluster.
func agree(statuses []memberStatus) (clusterState, error) {
	if len(statuses) == 0 {
		return clusterState{}, errors.New("no members probed")
	}
	var state clusterState
	for index, status := range statuses {
		switch {
		case status.err != nil:
			return clusterState{}, fmt.Errorf("%s: %w", status.endpoint, status.err)
		case status.leader == 0:
			return clusterState{}, fmt.Errorf("%s: no leader", status.endpoint)
		case len(status.errors) > 0:
			return clusterState{}, fmt.Errorf("%s: member errors %v", status.endpoint, status.errors)
		case index > 0 && status.leader != state.leader:
			return clusterState{}, fmt.Errorf("%s: leader %x, %s reports %x", status.endpoint, status.leader, statuses[0].endpoint, state.leader)
		case index > 0 && status.term != state.term:
			return clusterState{}, fmt.Errorf("%s: raft term %d, %s reports %d", status.endpoint, status.term, statuses[0].endpoint, state.term)
		}
		state = clusterState{leader: status.leader, term: status.term}
	}
	return state, nil
}
