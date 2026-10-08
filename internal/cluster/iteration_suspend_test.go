package cluster

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/store/distributed"
)

// iterationSuspendFixture is one admitted run whose single wait step,
// "approval", is reached in several loop iterations, driven at the step
// journal (durable loops do not lower yet) and suspended as processOne
// suspends it.
type iterationSuspendFixture struct {
	t        *testing.T
	ctx      context.Context
	store    *distributed.Store
	runtime  *Runtime
	owner    distributed.Owner
	tenant   string
	runID    string
	artifact string
	timeout  int64
}

func newIterationSuspendFixture(t *testing.T, ctx context.Context, name string, timeoutMillis int64) *iterationSuspendFixture {
	t.Helper()
	store := integrationDistributedStore(t)
	runtime := newWaitIntegrationRuntime(t, store, name, []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval", TimeoutMillis: timeoutMillis}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}, nil)
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, name+"-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: name, Workflow: name, Input: json.RawMessage(`{"value":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.Acquire(ctx, partition, name+"-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, owner)
	return &iterationSuspendFixture{t: t, ctx: ctx, store: store, runtime: runtime, owner: owner, tenant: tenant, runID: admission.RunID, artifact: runtime.workflows[name].Program.Digest, timeout: timeoutMillis}
}

func (f *iterationSuspendFixture) wait(iteration string) engine.WaitIdentity {
	return engine.WaitIdentity{Step: engine.NewStepIdentity(f.runID, f.artifact, "approval", iteration, []byte(`{"name":"approval"}`)), Name: "approval", TimeoutMillis: f.timeout}
}

func (f *iterationSuspendFixture) runState() string {
	f.t.Helper()
	run, err := f.runtime.GetRun(f.ctx, f.tenant, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	return run.State
}

// suspendAt opens iteration's wait and suspends the run there, as
// processOne does when the engine reports the suspension.
func (f *iterationSuspendFixture) suspendAt(j *runStepJournal, iteration string) {
	f.t.Helper()
	if result, ready, err := j.Await(f.ctx, f.wait(iteration)); err != nil || ready {
		f.t.Fatalf("%s: result=%+v ready=%v err=%v; want a wait of its own", iteration, result, ready, err)
	}
	if err := f.runtime.suspend(f.ctx, f.owner, j.record, "approval"); err != nil {
		f.t.Fatalf("%s: suspend: %v; want the run suspended", iteration, err)
	}
	if state := f.runState(); state != "waiting" {
		f.t.Fatalf("%s: run state %q after suspension; want waiting", iteration, state)
	}
}

func (f *iterationSuspendFixture) signal(iteration string) {
	f.t.Helper()
	id := "s-" + iteration
	if result, err := f.runtime.DeliverSignal(f.ctx, f.tenant, WaitIDFor(f.runID, "approval", iteration), id, "operator", json.RawMessage(`"`+id+`"`), true); err != nil || !result.Accepted || result.Duplicate || result.Late {
		f.t.Fatalf("signal %s: result=%+v err=%v", id, result, err)
	}
	if state := f.runState(); state != "accepted" {
		f.t.Fatalf("signal %s: run state %q; want accepted (resumable)", id, state)
	}
}

func (f *iterationSuspendFixture) signaled(j *runStepJournal, iteration string) {
	f.t.Helper()
	want := "s-" + iteration
	result, ready, err := j.Await(f.ctx, f.wait(iteration))
	if err != nil || !ready || result.SignalID != want || string(result.Payload) != `"`+want+`"` || result.TimedOut {
		f.t.Fatalf("%s: result=%+v ready=%v err=%v; want signal %s", iteration, result, ready, err, want)
	}
}

// TestLoopIterationsSuspendIndependently is #396's acceptance test against a
// real three-voter etcd cluster: one wait step reached in two iterations of
// a loop suspends the run twice under one owner. Both suspensions commit,
// and each iteration's signal resumes the run with its own outcome. On
// origin/main f3ffd4d the suspension was identified by run, step and owner
// token only, so the second one was refused as already written and the run
// stayed running.
func TestLoopIterationsSuspendIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newIterationSuspendFixture(t, ctx, "loop-suspend", 0)

	j := claimForSteps(t, ctx, f.runtime, f.owner, f.runID)
	f.suspendAt(j, "loop[0]")
	f.signal("loop[0]")
	j = claimForSteps(t, ctx, f.runtime, f.owner, f.runID)
	f.signaled(j, "loop[0]")
	f.suspendAt(j, "loop[1]")
	f.signal("loop[1]")
	j = claimForSteps(t, ctx, f.runtime, f.owner, f.runID)
	f.signaled(j, "loop[0]")
	f.signaled(j, "loop[1]")
	if waiting := eventCount(t, ctx, f.store, f.owner.Partition, "run.waiting"); waiting != 2 {
		t.Fatalf("run.waiting events=%d; want one per suspension (2)", waiting)
	}
}

// TestLoopIterationTimersSuspendIndependently: the timer path keys by
// iteration too. Iteration 0's timer fires and resumes the run; iteration
// 1 then suspends under the same owner and is signaled before its own
// timer, which then finds nothing to fire.
func TestLoopIterationTimersSuspendIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	f := newIterationSuspendFixture(t, ctx, "loop-timers", 60_000)
	late := time.Now().UTC().Add(time.Hour)

	j := claimForSteps(t, ctx, f.runtime, f.owner, f.runID)
	f.suspendAt(j, "loop[0]")
	fired, err := f.runtime.FireDueWaits(ctx, f.owner, late, 64)
	if err != nil || len(fired) != 1 || fired[0].WaitID != WaitIDFor(f.runID, "approval", "loop[0]") {
		t.Fatalf("first timer: fired=%+v err=%v; want loop[0]'s wait", fired, err)
	}
	if state := f.runState(); state != "accepted" {
		t.Fatalf("run state %q after loop[0]'s timer; want accepted", state)
	}
	j = claimForSteps(t, ctx, f.runtime, f.owner, f.runID)
	if result, ready, err := j.Await(ctx, f.wait("loop[0]")); err != nil || !ready || !result.TimedOut {
		t.Fatalf("loop[0]: result=%+v ready=%v err=%v; want timed out", result, ready, err)
	}
	f.suspendAt(j, "loop[1]")
	f.signal("loop[1]")
	if fired, err := f.runtime.FireDueWaits(ctx, f.owner, late, 64); err != nil || len(fired) != 0 {
		t.Fatalf("second timer: fired=%+v err=%v; want nothing (loop[1] was signaled)", fired, err)
	}
	j = claimForSteps(t, ctx, f.runtime, f.owner, f.runID)
	if result, ready, err := j.Await(ctx, f.wait("loop[0]")); err != nil || !ready || !result.TimedOut {
		t.Fatalf("loop[0] replay: result=%+v ready=%v err=%v; want timed out", result, ready, err)
	}
	f.signaled(j, "loop[1]")
	for kind, want := range map[string]int{"run.waiting": 2, "wait.timed_out": 1, "wait.signaled": 1} {
		if got := eventCount(t, ctx, f.store, f.owner.Partition, kind); got != want {
			t.Fatalf("%s events=%d; want %d", kind, got, want)
		}
	}
}

// TestSuspendTransitionIDIsPinned: a suspension's event ID is the run, the
// run revision it starts from and the owner's fence (#396). The vectors pin
// that encoding; the last one is the pre-#396 ID (run and step) that
// origin/main f3ffd4d committed for the same suspension, which the new
// encoding never produces.
func TestSuspendTransitionIDIsPinned(t *testing.T) {
	const run = "run-00000000000000000000000000000396"
	for _, vector := range []struct {
		revision int64
		want     string
	}{
		{41, "suspend-eff5c415e456b5a2aa2e8c47b926f340"},
		{42, "suspend-17c187e00646ca3ef1a48e509ba42b99"},
	} {
		if got := suspendTransitionID(run, vector.revision, 7); got != vector.want {
			t.Fatalf("revision %d: suspendTransitionID=%s; want %s", vector.revision, got, vector.want)
		}
	}
	const preIssue = "suspend-337a07de2fcfd209116195c8859d19aa"
	if got := transitionID("suspend", run+"\x00approval", 7); got != preIssue {
		t.Fatalf("pre-#396 suspension ID=%s; want %s", got, preIssue)
	}
	if suspendTransitionID(run, 41, 7) == preIssue {
		t.Fatal("the revision-keyed suspension ID equals the pre-#396 one")
	}
}
