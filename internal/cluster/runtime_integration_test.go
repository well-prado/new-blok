package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type integrationInput struct {
	Value int `json:"value"`
}

type integrationOutput struct {
	Value int `json:"value"`
}

type loadInput struct {
	Tenant   string `json:"tenant"`
	Sequence int    `json:"sequence"`
}

type loadOutput struct {
	Tenant   string `json:"tenant"`
	Sequence int    `json:"sequence"`
}

func TestTwoIngressNodesPersistAndExecuteOneIdempotentRun(t *testing.T) {
	fixtureBytes, err := os.ReadFile("../../testdata/distributed/runtime-execution-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		IngressRequests            int    `json:"ingressRequests"`
		ExpectedAccepted           int    `json:"expectedAccepted"`
		ExpectedDuplicates         int    `json:"expectedDuplicates"`
		ExpectedConflicts          int    `json:"expectedConflicts"`
		ExpectedEffects            int    `json:"expectedEffects"`
		ExpectedFinalState         string `json:"expectedFinalState"`
		SameKeyDifferentInputError string `json:"sameKeyDifferentInputError"`
	}
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}

	storeA := integrationDistributedStore(t)
	storeB := integrationDistributedStore(t)
	var effects atomic.Int64
	definition := node.MustDefine("fixture/effect", "1.0.0", func(_ context.Context, input integrationInput) (integrationOutput, error) {
		effects.Add(1)
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("synthetic external effect counter"), node.Schemas(
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
	), node.Effects("fixture:counter")).Any()
	program := contract.InternalProgram{WorkflowID: "distributed-fixture", Digest: "sha256:" + strings.Repeat("a", 64), Instructions: []contract.InternalInstruction{{Index: 0, ID: "effect", Kind: "call", Node: "fixture/effect"}}}
	workflows := map[string]Workflow{"distributed-fixture": {
		Program: program,
		DecodeInput: func(raw json.RawMessage) (any, error) {
			var input integrationInput
			decoder := json.NewDecoder(strings.NewReader(string(raw)))
			decoder.DisallowUnknownFields()
			if err := decoder.Decode(&input); err != nil {
				return nil, err
			}
			return input, nil
		},
	}}
	limits := Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 5 * time.Second}
	first, err := New(storeA, engine.New(map[string]node.Any{"fixture/effect": definition}), workflows, limits)
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(storeB, engine.New(map[string]node.Any{"fixture/effect": definition}), workflows, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := first.Check(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, partition := tenantForEmptyPartition(t, ctx, storeA, first, "tenant")
	request := Submission{Tenant: tenant, RequestKey: "request-1", Workflow: "distributed-fixture", Input: json.RawMessage(`{"value":41}`)}
	start := make(chan struct{})
	results := make(chan struct {
		admission Admission
		err       error
	}, fixture.IngressRequests)
	var ready sync.WaitGroup
	for index := 0; index < fixture.IngressRequests; index++ {
		ready.Add(1)
		go func(index int) {
			defer ready.Done()
			<-start
			runtime := first
			if index%2 == 1 {
				runtime = second
			}
			admission, err := runtime.Admit(ctx, request)
			results <- struct {
				admission Admission
				err       error
			}{admission, err}
		}(index)
	}
	close(start)
	ready.Wait()
	close(results)
	var accepted, duplicates, conflicts int
	var runID string
	for result := range results {
		if errors.Is(result.err, ErrRequestConflict) {
			conflicts++
			continue
		}
		if result.err != nil {
			t.Fatalf("ingress error: %v", result.err)
		}
		runID = result.admission.RunID
		if result.admission.Accepted {
			accepted++
		} else {
			duplicates++
		}
	}
	if accepted != fixture.ExpectedAccepted || duplicates != fixture.ExpectedDuplicates || conflicts != fixture.ExpectedConflicts {
		t.Fatalf("ingress counts accepted=%d duplicates=%d conflicts=%d, want %d/%d/%d", accepted, duplicates, conflicts, fixture.ExpectedAccepted, fixture.ExpectedDuplicates, fixture.ExpectedConflicts)
	}
	owner, err := storeA.Acquire(ctx, partition, "fixture-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.processOne(ctx, owner); err != nil {
		t.Fatalf("execute persisted run: %v", err)
	}
	if _, err := first.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
		t.Fatalf("duplicate execution result=%v, want ErrNoWork", err)
	}
	run, err := first.GetRun(ctx, tenant, runID)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != fixture.ExpectedFinalState || effects.Load() != int64(fixture.ExpectedEffects) {
		t.Fatalf("final state=%q effect count=%d, want %q/%d", run.State, effects.Load(), fixture.ExpectedFinalState, fixture.ExpectedEffects)
	}
	if _, err := first.Admit(ctx, Submission{Tenant: tenant, RequestKey: request.RequestKey, Workflow: request.Workflow, Input: json.RawMessage(`{"value":42}`)}); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("same-key changed-input error=%v, expected fixture error %s", err, fixture.SameKeyDifferentInputError)
	}
}

func TestTakeoverPersistsUncertaintyAndRejectsPausedOwnersResult(t *testing.T) {
	store := integrationDistributedStore(t)
	entered := make(chan struct{})
	releaseEffect := make(chan struct{})
	var effects atomic.Int64
	definition := node.MustDefine("fixture/paused-effect", "1.0.0", func(ctx context.Context, input integrationInput) (integrationOutput, error) {
		effects.Add(1)
		close(entered)
		<-releaseEffect
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("effect pauses after its synthetic external write"), node.Schemas(
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
	), node.Effects("fixture:counter")).Any()
	program := contract.InternalProgram{WorkflowID: "paused-effect-fixture", Digest: "sha256:" + strings.Repeat("b", 64), Instructions: []contract.InternalInstruction{{Index: 0, ID: "effect", Kind: "call", Node: "fixture/paused-effect"}}}
	workflow := Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input integrationInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	runtime, err := New(store, engine.New(map[string]node.Any{"fixture/paused-effect": definition}), map[string]Workflow{"paused-effect-fixture": workflow}, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "paused-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "paused-request", Workflow: "paused-effect-fixture", Input: json.RawMessage(`{"value":7}`)})
	if err != nil || !admission.Accepted {
		t.Fatalf("admission=%+v err=%v", admission, err)
	}
	oldOwner, err := store.Acquire(ctx, partition, "paused-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldDone := make(chan struct {
		record RunRecord
		err    error
	}, 1)
	go func() {
		record, err := runtime.processOne(ctx, oldOwner)
		oldDone <- struct {
			record RunRecord
			err    error
		}{record, err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("effect handler did not reach the pause barrier")
	}
	var newOwner distributed.Owner
	for newOwner.ID == "" {
		newOwner, err = store.Acquire(ctx, partition, "takeover-owner", time.Second)
		if errors.Is(err, distributed.ErrOwnershipLost) {
			if err := waitContext(ctx, 100*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if newOwner.Token <= oldOwner.Token {
		t.Fatalf("fence did not advance: old=%d new=%d", oldOwner.Token, newOwner.Token)
	}
	takenOver, err := runtime.processOne(ctx, newOwner)
	if err != nil {
		t.Fatalf("new owner recovery: %v", err)
	}
	if takenOver.State != "uncertain" || effects.Load() != 1 {
		t.Fatalf("takeover state=%q effect count=%d, want uncertain/1", takenOver.State, effects.Load())
	}
	close(releaseEffect)
	old := <-oldDone
	if !errors.Is(old.err, distributed.ErrOwnershipLost) {
		t.Fatalf("stale owner result error=%v, want ownership lost", old.err)
	}
	final, err := runtime.GetRun(ctx, tenant, admission.RunID)
	if err != nil || final.State != "uncertain" || len(final.Output) != 0 || effects.Load() != 1 {
		t.Fatalf("final=%+v err=%v effects=%d; want uncertain/no result/exactly one effect", final, err, effects.Load())
	}
}

func TestFailoverUnderSustainedLoadPreservesFairnessAndEffectCounts(t *testing.T) {
	fixtureBytes, err := os.ReadFile("../../testdata/distributed/runtime-failover-load-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Tenants                   int    `json:"tenants"`
		RunsPerTenant             int    `json:"runsPerTenant"`
		ExpectedInitialAccepted   int    `json:"expectedInitialAccepted"`
		ExpectedAccepted          int    `json:"expectedAccepted"`
		ExpectedUncertain         int    `json:"expectedUncertain"`
		ExpectedCompleted         int    `json:"expectedCompleted"`
		ExpectedExternalEffects   int64  `json:"expectedExternalEffects"`
		ExpectedFirstRoundTenants int    `json:"expectedFirstRoundDistinctTenants"`
		ExpectedStaleResult       string `json:"expectedStaleResult"`
	}
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	store := integrationDistributedStore(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var effects atomic.Int64
	var effectOrderMu sync.Mutex
	effectOrder := make([]string, 0, fixture.Tenants*fixture.RunsPerTenant)
	definition := node.MustDefine("fixture/load-effect", "1.0.0", func(_ context.Context, input loadInput) (loadOutput, error) {
		invocation := effects.Add(1)
		effectOrderMu.Lock()
		effectOrder = append(effectOrder, input.Tenant)
		effectOrderMu.Unlock()
		if invocation == 1 {
			close(entered)
			<-release
		}
		return loadOutput{Tenant: input.Tenant, Sequence: input.Sequence}, nil
	}, node.Description("synthetic effect blocks after effect for failover"), node.Schemas(
		[]byte(`{"type":"object","properties":{"tenant":{"type":"string"},"sequence":{"type":"integer"}},"required":["tenant","sequence"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"tenant":{"type":"string"},"sequence":{"type":"integer"}},"required":["tenant","sequence"],"additionalProperties":false}`),
	), node.Effects("fixture:load-counter")).Any()
	program := contract.InternalProgram{WorkflowID: "load-fixture", Digest: "sha256:" + strings.Repeat("d", 64), Instructions: []contract.InternalInstruction{{Index: 0, ID: "effect", Kind: "call", Node: "fixture/load-effect"}}}
	workflow := Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input loadInput
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	limits := Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: time.Second}
	oldRuntime, err := New(store, engine.New(map[string]node.Any{"fixture/load-effect": definition}), map[string]Workflow{"load-fixture": workflow}, limits)
	if err != nil {
		t.Fatal(err)
	}
	newRuntime, err := New(store, engine.New(map[string]node.Any{"fixture/load-effect": definition}), map[string]Workflow{"load-fixture": workflow}, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := oldRuntime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	base := fmt.Sprintf("load-%d", time.Now().UnixNano())
	partition, err := partitionWithoutPendingRuns(ctx, store, limits.Partitions)
	if err != nil {
		t.Skipf("no isolated partition available for sustained-load fixture: %v", err)
	}
	tenants := make([]string, 0, fixture.Tenants)
	for candidate := 0; len(tenants) < fixture.Tenants; candidate++ {
		tenant := fmt.Sprintf("%s-%d", base, candidate)
		if oldRuntime.Partition(tenant) == partition {
			tenants = append(tenants, tenant)
		}
	}
	sort.Strings(tenants)
	accepted := make([]struct{ tenant, runID string }, 0, fixture.Tenants*fixture.RunsPerTenant)
	for _, tenant := range tenants {
		for sequence := 0; sequence < fixture.RunsPerTenant; sequence++ {
			input, _ := json.Marshal(loadInput{Tenant: tenant, Sequence: sequence})
			admission, err := oldRuntime.Admit(ctx, Submission{Tenant: tenant, RequestKey: fmt.Sprintf("request-%d", sequence), Workflow: "load-fixture", Input: input})
			if err != nil || !admission.Accepted {
				t.Fatalf("admit tenant=%q sequence=%d: admission=%+v err=%v", tenant, sequence, admission, err)
			}
			accepted = append(accepted, struct{ tenant, runID string }{tenant, admission.RunID})
		}
	}
	if len(accepted) != fixture.ExpectedInitialAccepted {
		t.Fatalf("initial accepted count=%d, want %d", len(accepted), fixture.ExpectedInitialAccepted)
	}
	oldOwner, err := store.Acquire(ctx, partition, "load-owner-before", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	oldDone := make(chan error, 1)
	go func() { _, processErr := oldRuntime.processOne(ctx, oldOwner); oldDone <- processErr }()
	select {
	case <-entered:
	case err := <-oldDone:
		t.Fatalf("first persisted run returned before reaching the effect barrier: %v", err)
	case <-ctx.Done():
		t.Fatal("first external effect did not reach pause barrier")
	}
	// Continue admitting work while the old owner is paused after its effect.
	for sequence := fixture.RunsPerTenant; sequence < fixture.RunsPerTenant+2; sequence++ {
		tenant := tenants[sequence%len(tenants)]
		input, _ := json.Marshal(loadInput{Tenant: tenant, Sequence: sequence})
		admission, err := oldRuntime.Admit(ctx, Submission{Tenant: tenant, RequestKey: fmt.Sprintf("during-pause-%d", sequence), Workflow: "load-fixture", Input: input})
		if err != nil || !admission.Accepted {
			t.Fatalf("admit during paused owner tenant=%q: %v", tenant, err)
		}
		accepted = append(accepted, struct{ tenant, runID string }{tenant, admission.RunID})
	}
	// Two more accepted runs keep admission live during failover.
	for sequence := 0; sequence < 2; sequence++ {
		tenant := tenants[sequence%len(tenants)]
		input, _ := json.Marshal(loadInput{Tenant: tenant, Sequence: fixture.RunsPerTenant + sequence})
		admission, err := oldRuntime.Admit(ctx, Submission{Tenant: tenant, RequestKey: fmt.Sprintf("during-pause-%d", sequence), Workflow: "load-fixture", Input: input})
		if err != nil || !admission.Accepted {
			t.Fatalf("reconcile during-pause admission: %+v %v", admission, err)
		}
		accepted = append(accepted, struct{ tenant, runID string }{tenant, admission.RunID})
	}
	if len(accepted) != fixture.ExpectedAccepted {
		t.Fatalf("accepted under load=%d, want %d", len(accepted), fixture.ExpectedAccepted)
	}
	var newOwner distributed.Owner
	for newOwner.ID == "" {
		newOwner, err = store.Acquire(ctx, partition, "load-owner-after", time.Second)
		if errors.Is(err, distributed.ErrOwnershipLost) {
			if err := waitContext(ctx, 100*time.Millisecond); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	firstRecovered, err := newRuntime.processOne(ctx, newOwner)
	if err != nil {
		t.Fatalf("reconcile paused effect: %v", err)
	}
	completed, uncertain := 0, 0
	processed := map[string]bool{firstRecovered.RunID: true}
	if firstRecovered.State == "completed" {
		completed++
	} else if firstRecovered.State == "uncertain" {
		uncertain++
	} else {
		t.Fatalf("unexpected first recovered run state=%q", firstRecovered.State)
	}
	for {
		run, processErr := newRuntime.processOne(ctx, newOwner)
		if errors.Is(processErr, ErrNoWork) {
			break
		}
		if processErr != nil {
			t.Fatal(processErr)
		}
		processed[run.RunID] = true
		if run.State == "completed" {
			completed++
		} else if run.State == "uncertain" {
			uncertain++
		} else {
			t.Fatalf("unexpected failover run state=%q", run.State)
		}
	}
	if completed != fixture.ExpectedCompleted || uncertain != fixture.ExpectedUncertain || effects.Load() != fixture.ExpectedExternalEffects {
		t.Fatalf("failover completed=%d uncertain=%d effects=%d, want %d/%d/%d", completed, uncertain, effects.Load(), fixture.ExpectedCompleted, fixture.ExpectedUncertain, fixture.ExpectedExternalEffects)
	}
	if len(processed) != len(accepted) {
		t.Fatalf("new owner processed %d runs, want %d", len(processed), len(accepted))
	}
	effectOrderMu.Lock()
	order := append([]string(nil), effectOrder[1:]...)
	effectOrderMu.Unlock()
	firstRound := map[string]bool{}
	for _, tenant := range order[:fixture.ExpectedFirstRoundTenants] {
		firstRound[tenant] = true
	}
	if len(firstRound) != fixture.ExpectedFirstRoundTenants {
		t.Fatalf("first post-takeover effect round served %d tenants, want %d; order=%v", len(firstRound), fixture.ExpectedFirstRoundTenants, order)
	}
	close(release)
	if err := <-oldDone; !errors.Is(err, distributed.ErrOwnershipLost) {
		t.Fatalf("paused owner's resumed result=%v, want %s", err, fixture.ExpectedStaleResult)
	}
	for _, candidate := range accepted {
		run, err := newRuntime.GetRun(ctx, candidate.tenant, candidate.runID)
		if err != nil {
			t.Fatal(err)
		}
		if run.State != "completed" && run.State != "uncertain" {
			t.Fatalf("run %s remained in state %q", run.RunID, run.State)
		}
	}
}

func TestTimerAndSignalRaceHasOneFencedWinner(t *testing.T) {
	fixtureBytes, err := os.ReadFile("../../testdata/distributed/runtime-timer-signal-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		UnauthorizedError         string `json:"unauthorizedError"`
		SignalWinnerState         string `json:"signalWinnerState"`
		TimerWinnerState          string `json:"timerWinnerState"`
		ExpectedTransitionWinners int    `json:"expectedTransitionWinners"`
		ExpectedRunEffects        int64  `json:"expectedRunEffects"`
	}
	if err := json.Unmarshal(fixtureBytes, &fixture); err != nil {
		t.Fatal(err)
	}
	store := integrationDistributedStore(t)
	started := make(chan struct{})
	release := make(chan struct{})
	var effects atomic.Int64
	definition := node.MustDefine("fixture/wait-run", "1.0.0", func(_ context.Context, input integrationInput) (integrationOutput, error) {
		effects.Add(1)
		close(started)
		<-release
		return integrationOutput{Value: input.Value}, nil
	}, node.Description("timer/signal owner-race fixture"), node.Schemas(
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
	), node.Effects("fixture:wait-race")).Any()
	program := contract.InternalProgram{WorkflowID: "timer-signal-fixture", Digest: "sha256:" + strings.Repeat("c", 64), Instructions: []contract.InternalInstruction{{Index: 0, ID: "wait", Kind: "call", Node: "fixture/wait-run"}}}
	workflow := Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input integrationInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	runtime, err := New(store, engine.New(map[string]node.Any{"fixture/wait-run": definition}), map[string]Workflow{"timer-signal-fixture": workflow}, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "timer-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "timer-run", Workflow: "timer-signal-fixture", Input: json.RawMessage(`{"value":9}`)})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.Acquire(ctx, partition, "timer-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	processResult := make(chan error, 1)
	go func() { _, err := runtime.processOne(ctx, owner); processResult <- err }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("workflow did not start")
	}
	waitID := "wait-for-signal"
	if _, err := runtime.ScheduleWait(ctx, owner, admission.RunID, waitID, "ready", time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.DeliverSignal(ctx, tenant, waitID, "unauthorized-id", "principal", json.RawMessage(`{"value":"x"}`), false); !errors.Is(err, ErrUnauthorizedSignal) || fixture.UnauthorizedError != "signal_unauthorized" {
		t.Fatalf("unauthorized signal error=%v, want fixture %s", err, fixture.UnauthorizedError)
	}
	start := make(chan struct{})
	type signalOutcome struct {
		result SignalResult
		err    error
	}
	signalResult := make(chan signalOutcome, 1)
	timerResult := make(chan []WaitRecord, 1)
	timerErr := make(chan error, 1)
	go func() {
		<-start
		result, err := runtime.DeliverSignal(ctx, tenant, waitID, "race-signal", "principal", json.RawMessage(`{"value":"ok"}`), true)
		signalResult <- signalOutcome{result, err}
	}()
	go func() {
		<-start
		fired, err := runtime.FireDueWaits(ctx, owner, time.Now().UTC(), 8)
		timerResult <- fired
		timerErr <- err
	}()
	close(start)
	signal := <-signalResult
	fired := <-timerResult
	if err := <-timerErr; err != nil {
		t.Fatal(err)
	}
	if signal.err != nil {
		t.Fatal(signal.err)
	}
	winners := len(fired)
	if signal.result.Accepted {
		winners++
	}
	if winners != fixture.ExpectedTransitionWinners {
		t.Fatalf("signal/timer winners=%d, expected %d", winners, fixture.ExpectedTransitionWinners)
	}
	wait, err := runtime.GetWait(ctx, tenant, waitID)
	if err != nil {
		t.Fatal(err)
	}
	if wait.State == fixture.SignalWinnerState {
		if !signal.result.Accepted || len(fired) != 0 {
			t.Fatalf("signal winner=%+v timerFired=%d wait=%+v", signal.result, len(fired), wait)
		}
	} else if wait.State == fixture.TimerWinnerState {
		if !signal.result.Late || len(fired) != 1 {
			t.Fatalf("timer winner=%+v timerFired=%d wait=%+v", signal.result, len(fired), wait)
		}
	} else {
		t.Fatalf("wait state=%q, want one of %q or %q", wait.State, fixture.SignalWinnerState, fixture.TimerWinnerState)
	}
	if wait.State == fixture.SignalWinnerState {
		duplicate, err := runtime.DeliverSignal(ctx, tenant, waitID, "race-signal", "principal", json.RawMessage(`{"value":"ok"}`), true)
		if err != nil || !duplicate.Duplicate {
			t.Fatalf("duplicate signal=%+v err=%v, want deduplicated", duplicate, err)
		}
	} else {
		late, err := runtime.DeliverSignal(ctx, tenant, waitID, "race-signal", "principal", json.RawMessage(`{"value":"ok"}`), true)
		if err != nil || !late.Late {
			t.Fatalf("late duplicate signal=%+v err=%v, want stable late result", late, err)
		}
	}
	close(release)
	if err := <-processResult; err != nil {
		t.Fatal(err)
	}
	if effects.Load() != fixture.ExpectedRunEffects {
		t.Fatalf("run effect count=%d, want %d", effects.Load(), fixture.ExpectedRunEffects)
	}
}

func integrationDistributedStore(t *testing.T) *distributed.Store {
	t.Helper()
	endpoints := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ENDPOINTS"), ",")
	if endpoints[0] == "" {
		t.Skip("set BLOK_DISTRIBUTED_ENDPOINTS to run real etcd cluster execution tests")
	}
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	response, err := client.Get(context.Background(), "/blok/v1/cluster-incarnation")
	if err != nil || len(response.Kvs) == 0 {
		t.Fatalf("read existing cluster incarnation: response=%v err=%v", response, err)
	}
	store, err := distributed.New(context.Background(), client, string(response.Kvs[0].Value))
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func partitionWithoutPendingRuns(ctx context.Context, store *distributed.Store, partitions int) (string, error) {
	for partitionIndex := 0; partitionIndex < partitions; partitionIndex++ {
		partition := fmt.Sprintf("p-%04d", partitionIndex)
		if _, err := store.CurrentOwner(ctx, partition); err == nil {
			continue
		} else if !errors.Is(err, distributed.ErrOwnershipLost) {
			return "", err
		}
		events, err := store.ListEvents(ctx, partition)
		if err != nil {
			return "", err
		}
		pending := false
		for _, event := range events {
			if event.Kind != "run.accepted" || !strings.HasPrefix(event.ID, "accepted-run-") {
				continue
			}
			runID := strings.TrimPrefix(event.ID, "accepted-")
			data, _, err := store.ReadState(ctx, partition, runID)
			if err != nil {
				return "", err
			}
			var run RunRecord
			if err := json.Unmarshal(data, &run); err != nil {
				return "", err
			}
			if run.State == "accepted" || run.State == "running" {
				pending = true
				break
			}
		}
		if !pending {
			return partition, nil
		}
	}
	return "", errors.New("cluster integration: no empty partition available for isolated fixture")
}

func tenantForEmptyPartition(t *testing.T, ctx context.Context, store *distributed.Store, runtime *Runtime, prefix string) (string, string) {
	t.Helper()
	partition, err := partitionWithoutPendingRuns(ctx, store, runtime.limits.Partitions)
	if err != nil {
		t.Skipf("no isolated partition available for runtime fixture: %v", err)
	}
	base := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	for candidate := 0; ; candidate++ {
		tenant := fmt.Sprintf("%s-%d", base, candidate)
		if runtime.Partition(tenant) == partition {
			return tenant, partition
		}
	}
}
