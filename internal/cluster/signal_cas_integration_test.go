package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestSignalRetriesAWaitRunCASConflict uses real etcd transactions. The
// intercepted signal transaction first advances the still-waiting run's
// revision through a second real store client, making only the run comparison
// fail while leaving the wait projection open.
func TestSignalRetriesAWaitRunCASConflict(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	testID := fmt.Sprintf("%d-%d", os.Getpid(), time.Now().UnixNano())
	directStore := integrationDistributedStore(t)
	barrier := &signalCASBarrierClient{Client: integrationClient(t)}
	wrappedStore := integrationStoreFor(t, barrier)
	var prefixEffects, suffixEffects atomic.Int64
	prefix := node.MustDefine("fixture/signal-cas-prefix", "1.0.0", func(_ context.Context, input waitInput) (integrationOutput, error) {
		prefixEffects.Add(1)
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("counted pre-signal effect"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema)), node.Effects("fixture:signal-cas-prefix")).Any()
	suffix := node.MustDefine("fixture/signal-cas-suffix", "1.0.0", func(_ context.Context, input engine.WaitResult) (integrationOutput, error) {
		suffixEffects.Add(1)
		var signalInput waitInput
		if err := json.Unmarshal(input.Payload, &signalInput); err != nil {
			return integrationOutput{}, err
		}
		if input.SignalID != "cas-signal" || input.TimedOut {
			return integrationOutput{}, fmt.Errorf("unexpected resumed signal input: %+v", input)
		}
		return integrationOutput{Value: signalInput.Value + 1}, nil
	}, node.Description("counts resumed signal effect"), node.Schemas([]byte(waitConsumerSchema), []byte(outputSchema)), node.Effects("fixture:signal-cas-suffix")).Any()
	runtime := newWaitIntegrationRuntime(t, wrappedStore, "signal-cas-retry", []contract.InternalInstruction{
		{Index: 0, ID: "prefix", Kind: "call", Node: "fixture/signal-cas-prefix"},
		{Index: 1, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
		{Index: 2, ID: "suffix", Kind: "call", Node: "fixture/signal-cas-suffix", References: []contract.Reference{{Step: "approval"}}},
		{Index: 3, ID: "output", Kind: "output", References: []contract.Reference{{Step: "suffix"}}},
	}, map[string]node.Any{"fixture/signal-cas-prefix": prefix, "fixture/signal-cas-suffix": suffix})
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, partition := tenantForEmptyPartition(t, ctx, wrappedStore, runtime, "signal-cas-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "signal-cas-request-" + testID, Workflow: "signal-cas-retry", Input: json.RawMessage(`{"value":7}`)})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := wrappedStore.Acquire(ctx, partition, "signal-cas-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, wrappedStore, owner)
	if _, err := runtime.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
		t.Fatalf("initial process error=%v, want suspended work yield", err)
	}
	waitID := WaitIDFor(admission.RunID, "approval")
	wait, err := runtime.GetWait(ctx, tenant, waitID)
	if err != nil || wait.State != "waiting" {
		t.Fatalf("persisted wait=%+v err=%v", wait, err)
	}
	run, err := runtime.GetRun(ctx, tenant, admission.RunID)
	if err != nil || run.State != "waiting" {
		t.Fatalf("suspended run=%+v err=%v", run, err)
	}
	var revisionBumps atomic.Int32
	barrier.before = func() error {
		data, revision, readErr := directStore.ReadState(ctx, partition, admission.RunID)
		if readErr != nil {
			return readErr
		}
		if revision == 0 {
			return errors.New("run projection disappeared before injected conflict")
		}
		bumpID := revisionBumps.Add(1)
		_, commitErr := directStore.CommitFencedState(ctx, owner, admission.RunID, revision,
			fmt.Sprintf("test-revision-bump-%s-%d", waitID, bumpID), "test.signal_revision_bump", data, data)
		return commitErr
	}
	payload := json.RawMessage(`{"value":77}`)
	barrier.eventID = waitTransition("signal", tenant+"\x00"+waitID+"\x00cas-signal")
	barrier.arm(4)
	result, err := runtime.DeliverSignal(ctx, tenant, waitID, "cas-signal", "synthetic-principal", payload, true)
	if !errors.Is(err, ErrUnavailable) || result.Accepted || result.Late || result.Duplicate {
		t.Fatalf("signal after four run-only CAS conflicts=%+v err=%v; want retryable ErrUnavailable without acknowledgement", result, err)
	}
	if got := barrier.triggerCount.Load(); got != 4 {
		t.Fatalf("real etcd transaction barrier injected %d run-revision conflicts, want 4", got)
	}
	wait, err = runtime.GetWait(ctx, tenant, waitID)
	if err != nil || wait.State != "waiting" {
		t.Fatalf("retryable signal conflict acknowledged or closed live wait: wait=%+v err=%v", wait, err)
	}
	run, err = runtime.GetRun(ctx, tenant, admission.RunID)
	if err != nil || run.State != "waiting" {
		t.Fatalf("retryable signal conflict changed run state: run=%+v err=%v", run, err)
	}
	barrier.arm(0)
	result, err = runtime.DeliverSignal(ctx, tenant, waitID, "cas-signal", "synthetic-principal", payload, true)
	if err != nil || !result.Accepted || result.Late || result.Duplicate {
		t.Fatalf("signal after run-only CAS retries=%+v err=%v; want newly accepted signal", result, err)
	}
	if got := barrier.eventCommitCount.Load(); got != 1 {
		t.Fatalf("successful retry committed signal transition events=%d, want exactly 1", got)
	}
	completed, err := runtime.processOne(ctx, owner)
	if err != nil || completed.State != "completed" || string(completed.Output) != `{"value":78}` {
		persistedWait, waitErr := runtime.GetWait(ctx, tenant, waitID)
		persistedRun, runErr := runtime.GetRun(ctx, tenant, admission.RunID)
		active, activeErr := wrappedStore.ListActiveRunIDs(ctx, partition, runtime.limits.PartitionAdmissions)
		t.Fatalf("resumed run=%+v err=%v; persisted wait=%+v waitErr=%v persisted run=%+v runErr=%v active=%v activeErr=%v", completed, err, persistedWait, waitErr, persistedRun, runErr, active, activeErr)
	}
	duplicate, err := runtime.DeliverSignal(ctx, tenant, waitID, "cas-signal", "synthetic-principal", payload, true)
	if err != nil || !duplicate.Duplicate || duplicate.Late {
		t.Fatalf("retry after committed signal=%+v err=%v; want duplicate", duplicate, err)
	}
	if prefixEffects.Load() != 1 || suffixEffects.Load() != 1 {
		t.Fatalf("external effect counts prefix=%d suffix=%d; want exactly 1/1", prefixEffects.Load(), suffixEffects.Load())
	}

	for _, conflictCount := range []int32{1, 3} {
		prefixBase, suffixBase := prefixEffects.Load(), suffixEffects.Load()
		admission, err = runtime.Admit(ctx, Submission{
			Tenant: tenant, RequestKey: fmt.Sprintf("signal-cas-retry-%s-%d", testID, conflictCount),
			Workflow: "signal-cas-retry", Input: json.RawMessage(`{"value":80}`),
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
			t.Fatalf("%d-conflict retry run process error=%v, want suspended work yield", conflictCount, err)
		}
		retryWaitID := WaitIDFor(admission.RunID, "approval")
		barrier.eventID = waitTransition("signal", tenant+"\x00"+retryWaitID+"\x00cas-signal")
		barrier.before = func() error {
			data, revision, readErr := directStore.ReadState(ctx, partition, admission.RunID)
			if readErr != nil {
				return readErr
			}
			if revision == 0 {
				return errors.New("run projection disappeared before injected conflict")
			}
			bumpID := revisionBumps.Add(1)
			_, commitErr := directStore.CommitFencedState(ctx, owner, admission.RunID, revision,
				fmt.Sprintf("test-revision-bump-%s-%d", retryWaitID, bumpID), "test.signal_revision_bump", data, data)
			return commitErr
		}
		retryPayload := json.RawMessage(fmt.Sprintf(`{"value":%d}`, 80+conflictCount))
		triggerBase := barrier.triggerCount.Load()
		eventCommitBase := barrier.eventCommitCount.Load()
		barrier.arm(conflictCount)
		retried, retryErr := runtime.DeliverSignal(ctx, tenant, retryWaitID, "cas-signal", "synthetic-principal", retryPayload, true)
		if retryErr != nil || !retried.Accepted || retried.Duplicate || retried.Late {
			t.Fatalf("signal after %d run-only CAS conflicts=%+v err=%v; want newly accepted signal", conflictCount, retried, retryErr)
		}
		if got := barrier.triggerCount.Load() - triggerBase; got != conflictCount {
			t.Fatalf("%d-conflict retry injected %d real run-revision conflicts", conflictCount, got)
		}
		if got := barrier.eventCommitCount.Load() - eventCommitBase; got != 1 {
			t.Fatalf("%d-conflict retry committed signal transition events=%d, want exactly 1", conflictCount, got)
		}
		retryOutput := fmt.Sprintf(`{"value":%d}`, 81+conflictCount)
		completed, err = runtime.processOne(ctx, owner)
		if err != nil || completed.State != "completed" || string(completed.Output) != retryOutput {
			t.Fatalf("run after %d-conflict retry=%+v err=%v, want output %s", conflictCount, completed, err, retryOutput)
		}
		if prefixEffects.Load()-prefixBase != 1 || suffixEffects.Load()-suffixBase != 1 {
			t.Fatalf("%d-conflict retry external effect deltas prefix=%d suffix=%d; want exactly 1/1", conflictCount, prefixEffects.Load()-prefixBase, suffixEffects.Load()-suffixBase)
		}
	}

	for index, collision := range []struct {
		name      string
		principal string
		payload   json.RawMessage
	}{
		{name: "principal", principal: "other-principal", payload: payload},
		{name: "payload", principal: "synthetic-principal", payload: json.RawMessage(`{"value":78}`)},
	} {
		admission, err = runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "signal-cas-conflict-" + testID + "-" + collision.name, Workflow: "signal-cas-retry", Input: json.RawMessage(`{"value":7}`)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
			t.Fatalf("%s collision run process error=%v, want suspended work yield", collision.name, err)
		}
		collisionWaitID := WaitIDFor(admission.RunID, "approval")
		barrier.eventID = waitTransition("signal", tenant+"\x00"+collisionWaitID+"\x00cas-signal")
		barrier.before = func() error {
			waitData, waitRevision, readErr := directStore.ReadState(ctx, partition, waitStateID(tenant, collisionWaitID))
			if readErr != nil {
				return readErr
			}
			var competingWait WaitRecord
			if err := json.Unmarshal(waitData, &competingWait); err != nil || competingWait.State != "waiting" {
				return fmt.Errorf("competing signal found wait %+v: %w", competingWait, err)
			}
			runData, runRevision, readErr := directStore.ReadState(ctx, partition, admission.RunID)
			if readErr != nil {
				return readErr
			}
			var competingRun RunRecord
			if err := json.Unmarshal(runData, &competingRun); err != nil || competingRun.State != "waiting" {
				return fmt.Errorf("competing signal found run %+v: %w", competingRun, err)
			}
			competingWait.State, competingWait.SignalID, competingWait.Principal = "signaled", "cas-signal", collision.principal
			competingWait.Payload = append(json.RawMessage(nil), collision.payload...)
			encodedWait, _ := json.Marshal(competingWait)
			competingRun.State, competingRun.OwnerID, competingRun.Fence = "accepted", owner.ID, owner.Token
			encodedRun, _ := json.Marshal(competingRun)
			_, commitErr := directStore.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{
				{StateID: waitStateID(tenant, collisionWaitID), ExpectedRevision: waitRevision, State: encodedWait},
				{StateID: admission.RunID, ExpectedRevision: runRevision, State: encodedRun},
			}, nil, barrier.eventID, "wait.signaled", encodedWait)
			return commitErr
		}
		barrier.arm(1)
		conflicting, conflictErr := runtime.DeliverSignal(ctx, tenant, collisionWaitID, "cas-signal", "synthetic-principal", payload, true)
		if !errors.Is(conflictErr, ErrRequestConflict) || conflicting.Accepted || conflicting.Late || conflicting.Duplicate {
			t.Fatalf("same-ID %s race result=%+v err=%v; want request conflict without acknowledgement", collision.name, conflicting, conflictErr)
		}
		if got := barrier.triggerCount.Load(); got != int32(9+index) {
			t.Fatalf("barrier injections=%d after %s race, want %d", got, collision.name, 9+index)
		}
		wait, err = runtime.GetWait(ctx, tenant, collisionWaitID)
		if err != nil || wait.State != "signaled" || wait.Principal != collision.principal || string(wait.Payload) != string(collision.payload) {
			t.Fatalf("concurrent %s signal was not preserved: wait=%+v err=%v", collision.name, wait, err)
		}
		completed, err = runtime.processOne(ctx, owner)
		if err != nil || completed.State != "completed" {
			t.Fatalf("run after concurrent %s signal=%+v err=%v", collision.name, completed, err)
		}
		if duplicate, duplicateErr := runtime.DeliverSignal(ctx, tenant, collisionWaitID, "cas-signal", collision.principal, collision.payload, true); duplicateErr != nil || !duplicate.Duplicate {
			t.Fatalf("exact %s competitor retry=%+v err=%v; want duplicate", collision.name, duplicate, duplicateErr)
		}
	}
	if prefixEffects.Load() != 5 || suffixEffects.Load() != 5 {
		t.Fatalf("aggregate real-backend effect counts prefix=%d suffix=%d; want exactly 5/5", prefixEffects.Load(), suffixEffects.Load())
	}
}

type signalCASBarrierClient struct {
	*clientv3.Client
	remaining        atomic.Int32
	triggerCount     atomic.Int32
	eventCommitCount atomic.Int32
	eventID          string
	before           func() error
}

func (c *signalCASBarrierClient) arm(count int32) { c.remaining.Store(count) }

func (c *signalCASBarrierClient) Txn(ctx context.Context) clientv3.Txn {
	return &signalCASTxn{Txn: c.Client.Txn(ctx), owner: c}
}

type signalCASTxn struct {
	clientv3.Txn
	owner *signalCASBarrierClient
	ops   []clientv3.Op
}

func (t *signalCASTxn) If(compares ...clientv3.Cmp) clientv3.Txn {
	t.Txn = t.Txn.If(compares...)
	return t
}

func (t *signalCASTxn) Then(ops ...clientv3.Op) clientv3.Txn {
	t.ops = append(t.ops, ops...)
	t.Txn = t.Txn.Then(ops...)
	return t
}

func (t *signalCASTxn) Else(ops ...clientv3.Op) clientv3.Txn {
	t.Txn = t.Txn.Else(ops...)
	return t
}

func (t *signalCASTxn) Commit() (*clientv3.TxnResponse, error) {
	putCount := 0
	for _, operation := range t.ops {
		if operation.IsPut() {
			putCount++
		}
	}
	if putCount >= 3 && consumeSignalConflict(&t.owner.remaining) {
		if t.owner.before == nil {
			return nil, errors.New("signal CAS barrier has no revision-bump callback")
		}
		if err := t.owner.before(); err != nil {
			return nil, fmt.Errorf("inject run-revision conflict: %w", err)
		}
		t.owner.triggerCount.Add(1)
	}
	response, err := t.Txn.Commit()
	if err == nil && response.Succeeded && t.owner.eventID != "" {
		for _, operation := range t.ops {
			if operation.IsPut() && strings.Contains(string(operation.KeyBytes()), t.owner.eventID) {
				t.owner.eventCommitCount.Add(1)
				break
			}
		}
	}
	return response, err
}

func consumeSignalConflict(remaining *atomic.Int32) bool {
	for {
		count := remaining.Load()
		if count <= 0 {
			return false
		}
		if remaining.CompareAndSwap(count, count-1) {
			return true
		}
	}
}
