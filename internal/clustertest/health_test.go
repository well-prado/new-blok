package clustertest

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeClock advances only when slept on, and reports the deadline after
// limit sleeps, so the stability rule is measured in simulated time.
type fakeClock struct {
	now    time.Time
	sleeps int
	limit  int
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Sleep(_ context.Context, d time.Duration) error {
	if c.sleeps >= c.limit {
		return context.DeadlineExceeded
	}
	c.sleeps++
	c.now = c.now.Add(d)
	return nil
}

type probeResult struct {
	state clusterState
	err   error
}

// sequence returns a probe that yields results in order and then repeats the
// last one forever, recording the simulated time of each probe.
func sequence(clock *fakeClock, results ...probeResult) (func(context.Context) (clusterState, error), *[]time.Duration) {
	start := clock.now
	var at []time.Duration
	return func(context.Context) (clusterState, error) {
		at = append(at, clock.now.Sub(start))
		result := results[min(len(at)-1, len(results)-1)]
		return result.state, result.err
	}, &at
}

var unhealthy = probeResult{err: errors.New("http://127.0.0.1:3: context deadline exceeded")}

func healthy(leader, term uint64) probeResult {
	return probeResult{state: clusterState{leader: leader, term: term}}
}

const (
	testInterval = 250 * time.Millisecond
	testSpan     = 1500 * time.Millisecond
)

func TestHealthSpanIsLongerThanAnElectionTimeout(t *testing.T) {
	// etcd's default --election-timeout is 1000ms.
	if healthSpan <= time.Second || healthInterval >= healthSpan/2 {
		t.Fatalf("healthSpan=%s healthInterval=%s: the span must exceed one election timeout and cover several probes", healthSpan, healthInterval)
	}
}

func TestHealthyClusterIsReportedAfterTheSpan(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0), limit: 100}
	probe, at := sequence(clock, healthy(1, 2))
	if err := waitStable(context.Background(), probe, testInterval, testSpan, clock); err != nil {
		t.Fatal(err)
	}
	if returned := (*at)[len(*at)-1]; returned < testSpan {
		t.Fatalf("reported healthy after %s, before the %s span", returned, testSpan)
	}
}

// TestOneHealthyProbeIsNotHealth: a cluster that answers once and then
// stops (a voter still unpausing, a leader about to step down) is unhealthy,
// as is one that answers twice in a row but for less than the span.
func TestOneHealthyProbeIsNotHealth(t *testing.T) {
	for name, results := range map[string][]probeResult{
		"one probe":            {healthy(1, 2), unhealthy},
		"two probes in a row":  {healthy(1, 2), healthy(1, 2), unhealthy},
		"healthy then flapped": {healthy(1, 2), healthy(1, 2), healthy(1, 2), unhealthy, healthy(1, 2), unhealthy},
	} {
		clock := &fakeClock{now: time.Unix(0, 0), limit: 40}
		probe, _ := sequence(clock, results...)
		err := waitStable(context.Background(), probe, testInterval, testSpan, clock)
		if err == nil || !strings.Contains(err.Error(), "127.0.0.1:3") {
			t.Fatalf("%s: waitStable = %v, want the unhealthy probe's error", name, err)
		}
	}
}

// TestReElectionRestartsTheSpan: the same member re-elected in a new term is
// an election the previous probes did not see settle, as is a new leader.
func TestReElectionRestartsTheSpan(t *testing.T) {
	for name, after := range map[string]probeResult{
		"same leader, new term": healthy(1, 3),
		"new leader":            healthy(2, 3),
	} {
		clock := &fakeClock{now: time.Unix(0, 0), limit: 100}
		// Term 2 for three probes (0, 250, 500ms), then the change at 750ms.
		probe, at := sequence(clock, healthy(1, 2), healthy(1, 2), healthy(1, 2), after)
		if err := waitStable(context.Background(), probe, testInterval, testSpan, clock); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		changed := 3 * testInterval
		if returned := (*at)[len(*at)-1]; returned-changed < testSpan {
			t.Fatalf("%s: reported healthy %s after the election at %s, want at least the %s span", name, returned-changed, changed, testSpan)
		}
	}
}

func TestUnstableClusterReportsTheElection(t *testing.T) {
	clock := &fakeClock{now: time.Unix(0, 0), limit: 40}
	// A new term on every probe never settles.
	var term uint64 = 1
	probe := func(context.Context) (clusterState, error) {
		term++
		return clusterState{leader: 1, term: term}, nil
	}
	err := waitStable(context.Background(), probe, testInterval, testSpan, clock)
	if err == nil || !strings.Contains(err.Error(), "changed to leader 1 term") {
		t.Fatalf("waitStable over constant re-elections = %v, want the term change reported", err)
	}
}

func TestMembersMustAgreeOnLeaderAndTerm(t *testing.T) {
	member := func(endpoint string, leader, term uint64) memberStatus {
		return memberStatus{endpoint: endpoint, leader: leader, term: term}
	}
	state, err := agree([]memberStatus{member("a", 7, 4), member("b", 7, 4), member("c", 7, 4)})
	if err != nil || state != (clusterState{leader: 7, term: 4}) {
		t.Fatalf("agreeing members: state=%+v err=%v", state, err)
	}
	for name, test := range map[string]struct {
		members []memberStatus
		want    string
	}{
		"unreachable member": {[]memberStatus{member("a", 7, 4), {endpoint: "b", err: errors.New("context deadline exceeded")}, member("c", 7, 4)}, "b: context deadline exceeded"},
		"no leader":          {[]memberStatus{member("a", 7, 4), member("b", 0, 4), member("c", 7, 4)}, "b: no leader"},
		"member errors":      {[]memberStatus{member("a", 7, 4), {endpoint: "b", leader: 7, term: 4, errors: []string{"NOSPACE"}}, member("c", 7, 4)}, "member errors"},
		"two leaders":        {[]memberStatus{member("a", 7, 4), member("b", 8, 4), member("c", 7, 4)}, "b: leader 8"},
		"lagging term":       {[]memberStatus{member("a", 7, 4), member("b", 7, 4), member("c", 7, 3)}, "c: raft term 3"},
		"nobody probed":      {nil, "no members probed"},
	} {
		if _, err := agree(test.members); err == nil || !strings.Contains(err.Error(), test.want) {
			t.Errorf("%s: agree = %v, want an error containing %q", name, err, test.want)
		}
	}
}
