package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/store/distributed"
)

// TestWaitIDFormatIsPinned: a suspended run's signals are addressed to this
// ID, so a root-iteration wait keeps the ID origin/main a3d90d2 gave it
// (before iterations existed, #382), however the root is spelled; another
// iteration has its own.
func TestWaitIDFormatIsPinned(t *testing.T) {
	const run, pinned = "run:00000000000000000000000000000382", "wait-a3346057a5525be7890c857903b8d5f01680ba904e3807abbbdc04868e912709"
	for _, root := range []string{"", engine.RootIteration} {
		if got := WaitIDFor(run, "approval", root); got != pinned {
			t.Fatalf("iteration %q: WaitIDFor=%s; want %s", root, got, pinned)
		}
	}
	first, second := WaitIDFor(run, "approval", "loop[0]"), WaitIDFor(run, "approval", "loop[1]")
	if first == pinned || second == pinned || first == second {
		t.Fatalf("iteration waits %s, %s share an ID (root %s)", first, second, pinned)
	}
}

// claimForSteps claims run under owner as processOne does and returns the
// step journal an execution of it uses.
func claimForSteps(t *testing.T, ctx context.Context, runtime *Runtime, owner distributed.Owner, runID string) *runStepJournal {
	t.Helper()
	data, revision, err := runtime.store.ReadState(ctx, owner.Partition, runID)
	if err != nil || revision == 0 {
		t.Fatalf("read run %s: revision=%d err=%v", runID, revision, err)
	}
	var record RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	record.State, record.OwnerID, record.Fence = "running", owner.ID, owner.Token
	state, _ := json.Marshal(record)
	claim := transitionID("claim", runID+"\x00revision="+strconv.FormatInt(revision, 10), owner.Token)
	if _, err := runtime.store.CommitFencedState(ctx, owner, runID, revision, claim, "run.claimed", state, state); err != nil {
		t.Fatal(err)
	}
	return &runStepJournal{runtime: runtime, owner: owner, record: record}
}

// TestLoopIterationsWaitIndependently is #382's acceptance test on the
// cluster backend, at the step journal (durable loops do not lower yet):
// one wait step reached in two iterations of a loop is two waits, each
// signaled on its own and replayed with its own outcome. On a3d90d2 the
// wait ID ignored the iteration, so the second iteration read the first
// one's signal.
func TestLoopIterationsWaitIndependently(t *testing.T) {
	store := integrationDistributedStore(t)
	runtime := newWaitIntegrationRuntime(t, store, "loop-waits", []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "loop-waits-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "loop-waits", Workflow: "loop-waits", Input: json.RawMessage(`{"value":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.Acquire(ctx, partition, "loop-waits-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, owner)
	holdOwner(t, store, owner, 5*time.Second)
	artifact := runtime.workflows["loop-waits"].Program.Digest
	at := func(iteration string) engine.WaitIdentity {
		return engine.WaitIdentity{Step: engine.NewStepIdentity(admission.RunID, artifact, "approval", iteration, []byte(`{"name":"approval"}`)), Name: "approval"}
	}
	signal := func(iteration string) {
		t.Helper()
		id := "s-" + iteration
		if result, err := runtime.DeliverSignal(ctx, tenant, WaitIDFor(admission.RunID, "approval", iteration), id, "operator", json.RawMessage(`"`+id+`"`), true); err != nil || !result.Accepted || result.Duplicate || result.Late {
			t.Fatalf("signal %s: result=%+v err=%v", id, result, err)
		}
	}
	outcome := func(j *runStepJournal, iteration string) {
		t.Helper()
		want := "s-" + iteration
		result, ready, err := j.Await(ctx, at(iteration))
		if err != nil || !ready || result.SignalID != want || string(result.Payload) != `"`+want+`"` || result.TimedOut {
			t.Fatalf("%s: result=%+v ready=%v err=%v; want signal %s", iteration, result, ready, err, want)
		}
	}

	j := claimForSteps(t, ctx, runtime, owner, admission.RunID)
	if _, ready, err := j.Await(ctx, at("loop[0]")); err != nil || ready {
		t.Fatalf("first iteration: ready=%v err=%v; want waiting", ready, err)
	}
	signal("loop[0]")
	j = claimForSteps(t, ctx, runtime, owner, admission.RunID)
	outcome(j, "loop[0]")
	if result, ready, err := j.Await(ctx, at("loop[1]")); err != nil || ready {
		t.Fatalf("second iteration: result=%+v ready=%v err=%v; want a wait of its own", result, ready, err)
	}
	signal("loop[1]")
	j = claimForSteps(t, ctx, runtime, owner, admission.RunID)
	outcome(j, "loop[0]")
	outcome(j, "loop[1]")
	for _, iteration := range []string{"loop[0]", "loop[1]"} {
		wait, err := runtime.GetWait(ctx, tenant, WaitIDFor(admission.RunID, "approval", iteration))
		if err != nil || wait.State != "signaled" || wait.SignalID != "s-"+iteration || wait.OperationKey != at(iteration).Step.OperationKey {
			t.Fatalf("%s: wait=%+v err=%v", iteration, wait, err)
		}
	}

	// A call step's records are keyed by iteration too: loop[0]'s committed
	// result is not loop[1]'s. A root step's record stores its identity in
	// the form written before #382.
	input := json.RawMessage(`{"value":1}`)
	call := func(iteration string) engine.StepIdentity {
		return engine.NewStepIdentity(admission.RunID, artifact, "notify", iteration, input)
	}
	for _, iteration := range []string{"loop[0]", engine.RootIteration} {
		attempt, err := j.Begin(ctx, call(iteration), input, nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := j.Complete(ctx, attempt, json.RawMessage(`"`+iteration+`"`)); err != nil {
			t.Fatal(err)
		}
	}
	if output, found, err := j.Load(ctx, call("loop[0]")); err != nil || !found || string(output) != `"loop[0]"` {
		t.Fatalf("loop[0] result=%s found=%v err=%v", output, found, err)
	}
	if output, found, err := j.Load(ctx, call("loop[1]")); err != nil || found {
		t.Fatalf("loop[1] read result=%s found=%v err=%v; want none of its own yet", output, found, err)
	}
	data, _, err := store.ReadState(ctx, partition, stepStateID(call("").OperationKey))
	if err != nil {
		t.Fatal(err)
	}
	var stored struct {
		Identity json.RawMessage `json:"identity"`
	}
	if err := json.Unmarshal(data, &stored); err != nil || bytes.Contains(stored.Identity, []byte("IterationPath")) {
		t.Fatalf("root step record identity %s err=%v; want the pre-#382 form", stored.Identity, err)
	}
}

// TestStepKeyMustMatchItsIteration: the operation key addresses a step's
// records, so an identity whose key is another iteration's is refused
// before it reaches them.
func TestStepKeyMustMatchItsIteration(t *testing.T) {
	run := RunRecord{RunID: "run:00000000000000000000000000000382", ArtifactDigest: "sha256:0000000000000000000000000000000000000000000000000000000000000382"}
	first := engine.NewStepIdentity(run.RunID, run.ArtifactDigest, "approval", "loop[0]", []byte(`{}`))
	second := engine.NewStepIdentity(run.RunID, run.ArtifactDigest, "approval", "loop[1]", []byte(`{}`))
	if !sameStepIdentity(first, run, "approval") || !sameStepIdentity(second, run, "approval") {
		t.Fatal("an engine identity was refused")
	}
	second.OperationKey = first.OperationKey
	if sameStepIdentity(second, run, "approval") {
		t.Fatal("loop[1] with loop[0]'s operation key was accepted")
	}
}
