package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
)

// TestSignalRaceReconcilesAnHTMLDuplicate covers the third duplicate check of
// #259, the one an HTTP test can only reach by luck: two deliveries of the
// same signal race, and the loser reconciles against the winner's committed
// wait record. A real etcd transaction barrier commits the winner just before
// the loser's transaction, so the loser always takes that path. With a payload
// the store HTML-escapes, the loser must still see a duplicate of its own
// signal, not a conflict; a reordered payload is a different signal.
func TestSignalRaceReconcilesAnHTMLDuplicate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	testID := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	directStore := integrationDistributedStore(t)
	barrier := &signalCASBarrierClient{Client: integrationClient(t)}
	wrappedStore := integrationStoreFor(t, barrier)
	runtime := newWaitIntegrationRuntime(t, wrappedStore, "signal-html-race", []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}, map[string]node.Any{})
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, partition := tenantForEmptyPartition(t, ctx, wrappedStore, runtime, "signal-html-race")
	owner, err := wrappedStore.Acquire(ctx, partition, "signal-html-race-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, wrappedStore, owner)

	const winner = `{"note":"<a> & <b>","n":1}`
	for index, race := range []struct {
		name      string
		loser     string
		duplicate bool
	}{
		{name: "identical bytes", loser: winner, duplicate: true},
		{name: "insignificant whitespace", loser: "{ \"note\": \"<a> & <b>\", \"n\": 1 }", duplicate: true},
		{name: "reordered keys", loser: `{"n":1,"note":"<a> & <b>"}`, duplicate: false},
	} {
		admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: fmt.Sprintf("signal-html-race-%s-%d", testID, index), Workflow: "signal-html-race", Input: json.RawMessage(`{"value":1}`)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
			t.Fatalf("%s: initial process error=%v, want suspended work yield", race.name, err)
		}
		waitID := WaitIDFor(admission.RunID, "approval")
		barrier.eventID = waitTransition("signal", tenant+"\x00"+waitID+"\x00html-signal")
		// The winner: the same signal ID and principal, committed through
		// an unwrapped client exactly as DeliverSignal commits it.
		barrier.before = func() error {
			waitData, waitRevision, readErr := directStore.ReadState(ctx, partition, waitStateID(tenant, waitID))
			if readErr != nil {
				return readErr
			}
			var wait WaitRecord
			if err := json.Unmarshal(waitData, &wait); err != nil || wait.State != "waiting" {
				return fmt.Errorf("winner found wait %+v: %w", wait, err)
			}
			runData, runRevision, readErr := directStore.ReadState(ctx, partition, admission.RunID)
			if readErr != nil {
				return readErr
			}
			var run RunRecord
			if err := json.Unmarshal(runData, &run); err != nil || run.State != "waiting" {
				return fmt.Errorf("winner found run %+v: %w", run, err)
			}
			wait.State, wait.SignalID, wait.Principal, wait.Payload = "signaled", "html-signal", "synthetic-principal", json.RawMessage(winner)
			encodedWait, _ := json.Marshal(wait)
			run.State, run.OwnerID, run.Fence = "accepted", owner.ID, owner.Token
			encodedRun, _ := json.Marshal(run)
			_, commitErr := directStore.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{
				{StateID: waitStateID(tenant, waitID), ExpectedRevision: waitRevision, State: encodedWait},
				{StateID: admission.RunID, ExpectedRevision: runRevision, State: encodedRun},
			}, nil, barrier.eventID, "wait.signaled", encodedWait)
			return commitErr
		}
		triggers := barrier.triggerCount.Load()
		barrier.arm(1)
		result, deliverErr := runtime.DeliverSignal(ctx, tenant, waitID, "html-signal", "synthetic-principal", json.RawMessage(race.loser), true)
		barrier.arm(0)
		if got := barrier.triggerCount.Load() - triggers; got != 1 {
			t.Fatalf("%s: barrier committed the winner %d times, want 1", race.name, got)
		}
		if race.duplicate {
			if deliverErr != nil || result != (SignalResult{Accepted: true, Duplicate: true}) {
				t.Errorf("%s: losing delivery=%+v err=%v, want an accepted duplicate", race.name, result, deliverErr)
			}
		} else if !errors.Is(deliverErr, ErrRequestConflict) || result != (SignalResult{}) {
			t.Errorf("%s: losing delivery=%+v err=%v, want ErrRequestConflict", race.name, result, deliverErr)
		}
		wait, err := runtime.GetWait(ctx, tenant, waitID)
		if err != nil || wait.State != "signaled" || string(wait.Payload) != `{"note":"\u003ca\u003e \u0026 \u003cb\u003e","n":1}` {
			t.Fatalf("%s: wait=%+v payload=%s err=%v, want the winner's signal stored with its unchanged encoding", race.name, wait, wait.Payload, err)
		}
		if completed, err := runtime.processOne(ctx, owner); err != nil || completed.State != "completed" {
			t.Fatalf("%s: run after the race=%+v err=%v, want completed", race.name, completed, err)
		}
	}
}
