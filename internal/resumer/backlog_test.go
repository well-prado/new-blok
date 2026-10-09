package resumer

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// drain starts Run and waits for n settlements. It gives up when none
// comes for idle (Run is waiting for a tick: an hour in these tests), or
// after within in all; it reports how many settled and how long they took.
func (r *rig) drain(n int, idle, within time.Duration) (int, time.Duration) {
	r.t.Helper()
	ctx, stop := context.WithCancel(r.ctx)
	r.t.Cleanup(stop)
	begin := time.Now()
	go r.resumer.Run(ctx)
	deadline := time.After(within)
	for settled := 0; settled < n; settled++ {
		select {
		case <-r.settled:
		case <-time.After(idle):
			return settled, time.Since(begin)
		case <-deadline:
			return settled, time.Since(begin)
		}
	}
	return n, time.Since(begin)
}

// suspend admits and starts n runs, and waits until all are suspended.
func (r *rig) suspend(n int) []string {
	r.t.Helper()
	runs := make([]string, n)
	for i := range runs {
		runs[i] = r.admit(fmt.Sprintf("backlog-%d", i))
		if err := r.resumer.Start(r.ctx, runs[i]); err != nil {
			r.t.Fatal(err)
		}
	}
	r.await(n)
	return runs
}

// TestSignalBacklogDrainsWithoutWaitingForTicks is #413: runs woken
// without Wake (signals from another process cannot call it) are all
// resumed back to back, not Workers per Interval. The Interval here is an
// hour: before #413 Run took the first Workers runs and then waited for
// the next tick.
func TestSignalBacklogDrainsWithoutWaitingForTicks(t *testing.T) {
	const n = 200
	r := newRigWith(t, program(0, "test/notify"), "a", "", rigOptions{config: func(c *Config) { c.Workers = 4 }})
	runs := r.suspend(n)
	for _, run := range runs {
		r.signal(run, "s-"+run)
	}
	settled, took := r.drain(n, 10*time.Second, 3*time.Minute)
	if settled != n {
		t.Fatalf("%d of %d woken runs resumed in %v with a 1 h interval and 4 workers (none for 10 s at the end); want all, back to back", settled, n, took)
	}
	if got := r.row(`SELECT COUNT(*) FROM journal_runs WHERE state = 'completed'`); got != fmt.Sprint(n) {
		t.Fatalf("%s runs completed; want %d", got, n)
	}
	t.Logf("%d woken runs drained in %v (%.0f runs/s)", n, took, float64(n)/took.Seconds())
}

// TestTimerBurstDrainsWithoutWaitingForTicks is #413: timers that come
// due together are all claimed and resumed back to back.
func TestTimerBurstDrainsWithoutWaitingForTicks(t *testing.T) {
	const n = 200
	r := newRigWith(t, program(60_000, "test/notify"), "a", "", rigOptions{config: func(c *Config) { c.Workers = 4 }})
	r.suspend(n)
	r.clock.Advance(time.Minute)
	settled, took := r.drain(n, 10*time.Second, 3*time.Minute)
	if settled != n {
		t.Fatalf("%d of %d due timers resumed in %v with a 1 h interval and 4 workers (none for 10 s at the end); want all, back to back", settled, n, took)
	}
	if got := r.row(`SELECT COUNT(*) FROM journal_runs WHERE state = 'completed' AND CAST(output_json AS TEXT) = '{"timedOut":true}'`); got != fmt.Sprint(n) {
		t.Fatalf("%s runs completed timed out; want %d", got, n)
	}
	t.Logf("%d due timers drained in %v (%.0f runs/s)", n, took, float64(n)/took.Seconds())
}

// TestInterruptedBacklogDrainsWithoutWaitingForTicks is #413: after a
// crash, more interrupted runs than workers are all recovered back to
// back, not Workers per third of a lease.
func TestInterruptedBacklogDrainsWithoutWaitingForTicks(t *testing.T) {
	const n = 60
	r := newRigWith(t, straight("test/notify"), "a", "", rigOptions{config: func(c *Config) { c.Workers = 4 }})
	for i := range n {
		run := r.admit(fmt.Sprintf("crashed-%d", i))
		// leased by a holder that died
		if _, err := r.journal.TakeRunLease(r.ctx, run, r.clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	r.clock.Advance(31 * time.Second)
	settled, took := r.drain(n, 10*time.Second, 3*time.Minute)
	if settled != n {
		t.Fatalf("%d of %d interrupted runs recovered in %v (none for 10 s at the end); want all, back to back", settled, n, took)
	}
}

// TestUnsettledRunsDoNotSpinTheResumer: the back-to-back sweeps follow
// progress. Woken runs this resumer cannot execute (their workflow is not
// registered at their artifact) are released at once, and must not be
// taken again and again between ticks.
func TestUnsettledRunsDoNotSpinTheResumer(t *testing.T) {
	path := t.TempDir() + "/journal.db"
	a := newRigWith(t, program(0, "test/notify"), "a", path, rigOptions{})
	runs := a.suspend(8)
	for _, run := range runs {
		a.signal(run, "s-"+run)
	}
	other := program(0, "test/notify")
	other.Digest = "sha256:" + fmt.Sprintf("%064d", 7)
	b := newRigWith(t, other, "b", path, rigOptions{config: func(c *Config) { c.Workers = 4 }})
	ctx, stop := context.WithCancel(b.ctx)
	go b.resumer.Run(ctx)
	time.Sleep(500 * time.Millisecond)
	stop()
	b.resumer.Close(context.Background())
	executions := 0
	for range len(b.settled) {
		<-b.settled
		executions++
	}
	// One sweep takes 4; a spinning resumer takes them thousands of times.
	if executions > 8 {
		t.Fatalf("%d executions in 500 ms of runs it cannot execute, with a 1 h interval; want one sweep's worth", executions)
	}
	t.Logf("%d executions", executions)
}
