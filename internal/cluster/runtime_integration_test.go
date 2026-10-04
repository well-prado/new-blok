package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
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

type waitInput struct {
	Value int `json:"value"`
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
	releaseOwnerOnCleanup(t, storeA, owner)
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
	releaseOwnerOnCleanup(t, store, oldOwner)
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
	releaseOwnerOnCleanup(t, store, newOwner)
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
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
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
	releaseOwnerOnCleanup(t, store, oldOwner)
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
	releaseOwnerOnCleanup(t, store, newOwner)
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
	unblock()
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
	releaseOwnerOnCleanup(t, store, owner)
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

func TestSignalResumesDistributedRunFromCommittedPrefix(t *testing.T) {
	fixture := readContinuationFixture(t)
	store := integrationDistributedStore(t)
	var prefixEffects, suffixEffects atomic.Int64
	prefix := node.MustDefine("fixture/wait-prefix", "1.0.0", func(_ context.Context, input waitInput) (integrationOutput, error) {
		prefixEffects.Add(1)
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("counted pre-wait effect"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema)), node.Effects("fixture:prefix")).Any()
	suffix := node.MustDefine("fixture/wait-suffix", "1.0.0", func(_ context.Context, input engine.WaitResult) (integrationOutput, error) {
		suffixEffects.Add(1)
		if input.SignalID != fixture.SignalID || string(input.Payload) != fixture.SignalPayload || input.TimedOut {
			return integrationOutput{}, fmt.Errorf("unexpected resumed signal input: %+v", input)
		}
		return integrationOutput{Value: 99}, nil
	}, node.Description("consumes committed signal wait outcome"), node.Schemas([]byte(waitConsumerSchema), []byte(outputSchema)), node.Effects("fixture:suffix")).Any()
	runtime := newWaitIntegrationRuntime(t, store, "signal-continuation", []contract.InternalInstruction{
		{Index: 0, ID: "prefix", Kind: "call", Node: "fixture/wait-prefix"},
		{Index: 1, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
		{Index: 2, ID: "suffix", Kind: "call", Node: "fixture/wait-suffix", References: []contract.Reference{{Step: "approval"}}},
		{Index: 3, ID: "output", Kind: "output", References: []contract.Reference{{Step: "suffix"}}},
	}, map[string]node.Any{"fixture/wait-prefix": prefix, "fixture/wait-suffix": suffix})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "wait-signal-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "signal-continuation", Workflow: "signal-continuation", Input: json.RawMessage(`{"value":40}`)})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bounded continuation state: partition=%s tenant=%s run=%s wait=%s", partition, tenant, admission.RunID, WaitIDFor(admission.RunID, "approval"))
	owner, err := store.Acquire(ctx, partition, "signal-continuation-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, owner)
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
	payload := json.RawMessage(fixture.SignalPayload)
	if result, err := runtime.DeliverSignal(ctx, tenant, waitID, fixture.SignalID, "synthetic-principal", payload, true); err != nil || !result.Accepted {
		t.Fatalf("signal result=%+v err=%v", result, err)
	}
	run, err = runtime.GetRun(ctx, tenant, admission.RunID)
	if err != nil || run.State != "accepted" {
		t.Fatalf("signal did not atomically resume run: %+v err=%v", run, err)
	}
	completed, err := runtime.processOne(ctx, owner)
	if err != nil || completed.State != "completed" || string(completed.Output) != fixture.ExpectedOutput {
		t.Fatalf("resumed execution=%+v err=%v", completed, err)
	}
	if prefixEffects.Load() != fixture.ExpectedPrefixEffects || suffixEffects.Load() != fixture.ExpectedSuffixEffects {
		t.Fatalf("effect counts prefix=%d suffix=%d; expected %d/%d", prefixEffects.Load(), suffixEffects.Load(), fixture.ExpectedPrefixEffects, fixture.ExpectedSuffixEffects)
	}
}

func TestTimerResumesDistributedRunFromCommittedPrefix(t *testing.T) {
	fixture := readContinuationFixture(t)
	store := integrationDistributedStore(t)
	var prefixEffects, suffixEffects atomic.Int64
	prefix := node.MustDefine("fixture/timer-prefix", "1.0.0", func(_ context.Context, input waitInput) (integrationOutput, error) {
		prefixEffects.Add(1)
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("counted pre-timer effect"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema)), node.Effects("fixture:timer-prefix")).Any()
	suffix := node.MustDefine("fixture/timer-suffix", "1.0.0", func(_ context.Context, input engine.WaitResult) (integrationOutput, error) {
		suffixEffects.Add(1)
		if !input.TimedOut || input.SignalID != "" {
			return integrationOutput{}, fmt.Errorf("unexpected resumed timer input: %+v", input)
		}
		return integrationOutput{Value: 100}, nil
	}, node.Description("consumes committed timer wait outcome"), node.Schemas([]byte(waitConsumerSchema), []byte(outputSchema)), node.Effects("fixture:timer-suffix")).Any()
	runtime := newWaitIntegrationRuntime(t, store, "timer-continuation", []contract.InternalInstruction{
		{Index: 0, ID: "prefix", Kind: "call", Node: "fixture/timer-prefix"},
		{Index: 1, ID: "delay", Kind: "wait", Wait: &contract.WaitInstruction{Name: "delay", TimeoutMillis: 1}},
		{Index: 2, ID: "suffix", Kind: "call", Node: "fixture/timer-suffix", References: []contract.Reference{{Step: "delay"}}},
		{Index: 3, ID: "output", Kind: "output", References: []contract.Reference{{Step: "suffix"}}},
	}, map[string]node.Any{"fixture/timer-prefix": prefix, "fixture/timer-suffix": suffix})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "wait-timer-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "timer-continuation", Workflow: "timer-continuation", Input: json.RawMessage(`{"value":40}`)})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("bounded continuation state: partition=%s tenant=%s run=%s wait=%s", partition, tenant, admission.RunID, WaitIDFor(admission.RunID, "delay"))
	owner, err := store.Acquire(ctx, partition, "timer-continuation-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, owner)
	if _, err := runtime.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
		t.Fatalf("initial process error=%v, want suspended work yield", err)
	}
	if fired, err := runtime.FireDueWaits(ctx, owner, time.Now().UTC().Add(time.Second), 8); err != nil || len(fired) != 1 {
		t.Fatalf("due timers=%d err=%v, want exactly one", len(fired), err)
	}
	run, err := runtime.GetRun(ctx, tenant, admission.RunID)
	if err != nil || run.State != "accepted" {
		t.Fatalf("timer did not atomically resume run: %+v err=%v", run, err)
	}
	completed, err := runtime.processOne(ctx, owner)
	if err != nil || completed.State != "completed" || string(completed.Output) != fixture.ExpectedTimerOutput {
		t.Fatalf("resumed execution=%+v err=%v", completed, err)
	}
	if prefixEffects.Load() != fixture.ExpectedPrefixEffects || suffixEffects.Load() != fixture.ExpectedSuffixEffects {
		t.Fatalf("effect counts prefix=%d suffix=%d; expected %d/%d", prefixEffects.Load(), suffixEffects.Load(), fixture.ExpectedPrefixEffects, fixture.ExpectedSuffixEffects)
	}
}

func TestPureStepDispatchCanRetryAfterOwnerTakeover(t *testing.T) {
	var fixture struct {
		ExpectedAttempts        int  `json:"expectedAttempts"`
		ExpectedDifferentIDs    bool `json:"expectedDifferentAttemptIDs"`
		ExpectedExternalEffects int  `json:"expectedExternalEffects"`
	}
	fixtureData, err := os.ReadFile("../../testdata/distributed/pure-step-takeover-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fixtureData, &fixture); err != nil {
		t.Fatal(err)
	}
	store := integrationDistributedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	partition := fmt.Sprintf("p-pure-retry-%d", time.Now().UnixNano())
	firstOwner, err := store.Acquire(ctx, partition, "pure-retry-old-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, firstOwner)
	runtime := &Runtime{store: store}
	run := RunRecord{RunID: "run-pure-retry", ArtifactDigest: "sha256:" + strings.Repeat("f", 64)}
	input := json.RawMessage(`{"value":1}`)
	identity := engine.StepIdentity{RunID: run.RunID, ArtifactDigest: run.ArtifactDigest, StepID: "pure-step", InputDigest: digest(input), OperationKey: "op:" + strings.Repeat("a", 64)}
	firstJournal := &runStepJournal{runtime: runtime, owner: firstOwner, record: run}
	firstAttempt, err := firstJournal.Begin(ctx, identity, input, nil)
	if err != nil {
		t.Fatalf("first pure dispatch: %v", err)
	}
	if err := store.Release(ctx, firstOwner); err != nil {
		t.Fatalf("release old owner before simulated crash takeover: %v", err)
	}
	newOwner, err := store.Acquire(ctx, partition, "pure-retry-new-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, newOwner)
	t.Logf("pure-step takeover state: partition=%s run=%s state=step-<sha256:...> oldFence=%d newFence=%d", partition, run.RunID, firstOwner.Token, newOwner.Token)
	newJournal := &runStepJournal{runtime: runtime, owner: newOwner, record: run}
	if _, completed, err := newJournal.Load(ctx, identity); err != nil || completed {
		t.Fatalf("pure dispatched step replay state completed=%v err=%v; want safe redispatch", completed, err)
	}
	secondAttempt, err := newJournal.Begin(ctx, identity, input, nil)
	if err != nil {
		t.Fatalf("retry pure dispatch after takeover: %v", err)
	}
	data, _, err := store.ReadState(ctx, partition, stepStateID(identity.OperationKey))
	if err != nil {
		t.Fatal(err)
	}
	var persisted stepRecord
	if err := json.Unmarshal(data, &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.AttemptNumber != fixture.ExpectedAttempts || (firstAttempt.AttemptID != secondAttempt.AttemptID) != fixture.ExpectedDifferentIDs || fixture.ExpectedExternalEffects != 0 {
		t.Fatalf("attempts=%d first=%s second=%s expected attempts=%d distinct=%v effects=%d", persisted.AttemptNumber, firstAttempt.AttemptID, secondAttempt.AttemptID, fixture.ExpectedAttempts, fixture.ExpectedDifferentIDs, fixture.ExpectedExternalEffects)
	}
}

func TestTenantFairCursorSurvivesPartitionOwnerTakeover(t *testing.T) {
	var fixture struct {
		CandidateTenants  []string `json:"candidateTenants"`
		ExpectedFirstTurn string   `json:"expectedFirstTurn"`
		ExpectedNextTurn  string   `json:"expectedNextTurn"`
	}
	data, err := os.ReadFile("../../testdata/distributed/tenant-fairness-takeover-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	store := integrationDistributedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	partition := fmt.Sprintf("p-fair-cursor-%d", time.Now().UnixNano())
	oldOwner, err := store.Acquire(ctx, partition, "fair-cursor-old-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, oldOwner)
	candidates := make([]RunRecord, 0, len(fixture.CandidateTenants))
	for index, tenant := range fixture.CandidateTenants {
		candidates = append(candidates, RunRecord{RunID: fmt.Sprintf("run-%02d", index), Tenant: tenant})
	}
	oldRuntime := &Runtime{store: store}
	first, err := oldRuntime.fairCandidate(ctx, oldOwner, candidates)
	if err != nil || first.Tenant != fixture.ExpectedFirstTurn {
		t.Fatalf("first tenant turn=%q err=%v, want %q", first.Tenant, err, fixture.ExpectedFirstTurn)
	}
	if err := store.Release(ctx, oldOwner); err != nil {
		t.Fatal(err)
	}
	newOwner, err := store.Acquire(ctx, partition, "fair-cursor-new-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, newOwner)
	newRuntime := &Runtime{store: store}
	second, err := newRuntime.fairCandidate(ctx, newOwner, candidates)
	t.Logf("fair cursor takeover state: partition=%s priorTenant=%s resumedTenant=%s oldFence=%d newFence=%d", partition, first.Tenant, second.Tenant, oldOwner.Token, newOwner.Token)
	if err != nil || second.Tenant != fixture.ExpectedNextTurn {
		t.Fatalf("post-takeover tenant turn=%q err=%v, want %q", second.Tenant, err, fixture.ExpectedNextTurn)
	}
}

func TestScheduleWaitClassifiesQuorumLossAsUnavailable(t *testing.T) {
	store := integrationDistributedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	partition := fmt.Sprintf("p-wait-quorum-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "wait-quorum-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, owner)
	t.Logf("quorum-loss probe state: voters=[blok-distributed-spike-etcd1-1 blok-distributed-spike-etcd2-1 blok-distributed-spike-etcd3-1] paused=[blok-distributed-spike-etcd2-1 blok-distributed-spike-etcd3-1] partition=%s run=absent wait=wait-absent", partition)
	paused := make([]string, 0, 2)
	defer func() {
		for index := len(paused) - 1; index >= 0; index-- {
			_ = exec.Command("docker", "unpause", paused[index]).Run()
		}
	}()
	for _, container := range []string{"blok-distributed-spike-etcd2-1", "blok-distributed-spike-etcd3-1"} {
		output, err := exec.CommandContext(ctx, "docker", "pause", container).CombinedOutput()
		if err != nil {
			t.Fatalf("pause voter %s: %v: %s", container, err, output)
		}
		paused = append(paused, container)
	}
	blockedCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	_, err = (&Runtime{store: store}).scheduleWait(blockedCtx, owner, "run-absent", "wait-absent", "approval", time.Now().Add(time.Minute), engine.StepIdentity{}, 0)
	stop()
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, distributed.ErrOwnershipLost) {
		t.Fatalf("quorum-loss schedule error=%v, want retryable ErrUnavailable and not ErrOwnershipLost", err)
	}
	for index := len(paused) - 1; index >= 0; index-- {
		output, err := exec.CommandContext(ctx, "docker", "unpause", paused[index]).CombinedOutput()
		if err != nil {
			t.Fatalf("restore voter %s: %v: %s", paused[index], err, output)
		}
		paused = paused[:index]
	}
	if _, _, err := store.ReadState(ctx, partition, "recovery-check"); err != nil {
		t.Fatalf("quorum did not recover after restoring all voters: %v", err)
	}
}

func TestStepJournalQuorumLossDefersAcceptedRun(t *testing.T) {
	var fixture struct {
		ExpectedFailureClass      string `json:"expectedFailureClass"`
		ExpectedPrefixInvocations int64  `json:"expectedPrefixInvocations"`
		ExpectedExternalEffects   int64  `json:"expectedExternalEffects"`
		ExpectedFinalState        string `json:"expectedFinalState"`
		ExpectedActiveRuns        int    `json:"expectedActiveRuns"`
		ExpectedOutput            string `json:"expectedOutput"`
	}
	fixtureData, err := os.ReadFile("../../testdata/distributed/runtime-quorum-defer-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(fixtureData, &fixture); err != nil {
		t.Fatal(err)
	}
	store := integrationDistributedStore(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	var prefixInvocations, effects atomic.Int64
	prefix := node.MustDefine("fixture/quorum-prefix", "1.0.0", func(_ context.Context, input integrationInput) (integrationOutput, error) {
		if prefixInvocations.Add(1) == 1 {
			close(entered)
			<-release
		}
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("pure step held across a real quorum loss"), node.Schemas(
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
	)).Any()
	effect := node.MustDefine("fixture/quorum-effect", "1.0.0", func(_ context.Context, input integrationInput) (integrationOutput, error) {
		effects.Add(1)
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("counted synthetic external effect after journal recovery"), node.Schemas(
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
	), node.Effects("fixture:quorum-counter")).Any()
	program := contract.InternalProgram{WorkflowID: "quorum-defer-fixture", Digest: "sha256:" + strings.Repeat("9", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "prefix", Kind: "call", Node: "fixture/quorum-prefix"},
		{Index: 1, ID: "effect", Kind: "call", Node: "fixture/quorum-effect"},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "effect"}}},
	}}
	workflow := Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input integrationInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	runtime, err := New(store, engine.New(map[string]node.Any{"fixture/quorum-prefix": prefix, "fixture/quorum-effect": effect}), map[string]Workflow{"quorum-defer-fixture": workflow}, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "quorum-defer-tenant")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "quorum-defer", Workflow: "quorum-defer-fixture", Input: json.RawMessage(`{"value":40}`)})
	if err != nil {
		t.Fatal(err)
	}
	owner, err := store.Acquire(ctx, partition, "quorum-defer-owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, owner)
	t.Logf("quorum journal recovery state: voters=[blok-distributed-spike-etcd1-1 blok-distributed-spike-etcd2-1 blok-distributed-spike-etcd3-1] paused=[blok-distributed-spike-etcd2-1 blok-distributed-spike-etcd3-1] partition=%s run=%s", partition, admission.RunID)
	type processResult struct {
		record RunRecord
		err    error
	}
	processCtx, cancelProcess := context.WithTimeout(ctx, 4*time.Second)
	defer cancelProcess()
	oldDone := make(chan processResult, 1)
	go func() {
		record, processErr := runtime.processOne(processCtx, owner)
		oldDone <- processResult{record: record, err: processErr}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("prefix did not reach quorum-loss barrier")
	}
	containers := []string{"blok-distributed-spike-etcd2-1", "blok-distributed-spike-etcd3-1"}
	paused := make([]string, 0, len(containers))
	restoreVoters := func() {
		for index := len(paused) - 1; index >= 0; index-- {
			_ = exec.Command("docker", "unpause", paused[index]).Run()
		}
	}
	t.Cleanup(restoreVoters)
	for _, container := range containers {
		output, pauseErr := exec.CommandContext(ctx, "docker", "pause", container).CombinedOutput()
		if pauseErr != nil {
			t.Fatalf("pause voter %s: %v: %s", container, pauseErr, output)
		}
		paused = append(paused, container)
	}
	unblock()
	var oldResult processResult
	select {
	case oldResult = <-oldDone:
	case <-ctx.Done():
		t.Fatal("journal commit did not surface quorum loss")
	}
	var engineErr *engine.Error
	if !errors.As(oldResult.err, &engineErr) || engineErr.Class != fixture.ExpectedFailureClass {
		t.Fatalf("journal outage result=%v; want failure class %q", oldResult.err, fixture.ExpectedFailureClass)
	}
	for index := len(paused) - 1; index >= 0; index-- {
		output, unpauseErr := exec.CommandContext(ctx, "docker", "unpause", paused[index]).CombinedOutput()
		if unpauseErr != nil {
			t.Fatalf("restore voter %s: %v: %s", paused[index], unpauseErr, output)
		}
		paused = paused[:index]
	}
	var newOwner distributed.Owner
	for newOwner.ID == "" {
		newOwner, err = store.Acquire(ctx, partition, "quorum-defer-recovery-owner", time.Second)
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
	releaseOwnerOnCleanup(t, store, newOwner)
	recovered, err := runtime.processOne(ctx, newOwner)
	if err != nil || recovered.State != fixture.ExpectedFinalState || string(recovered.Output) != fixture.ExpectedOutput {
		t.Fatalf("recovered=%+v err=%v; expected %s / %s", recovered, err, fixture.ExpectedFinalState, fixture.ExpectedOutput)
	}
	active, err := store.ListActiveRunIDs(ctx, partition, runtime.limits.PartitionAdmissions)
	if err != nil || len(active) != fixture.ExpectedActiveRuns || prefixInvocations.Load() != fixture.ExpectedPrefixInvocations || effects.Load() != fixture.ExpectedExternalEffects {
		t.Fatalf("active=%v err=%v prefixInvocations=%d effects=%d; expected active=%d prefix=%d effects=%d", active, err, prefixInvocations.Load(), effects.Load(), fixture.ExpectedActiveRuns, fixture.ExpectedPrefixInvocations, fixture.ExpectedExternalEffects)
	}
}

const waitInputSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`
const outputSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`
const waitConsumerSchema = `{"type":"object"}`

func readContinuationFixture(t *testing.T) struct {
	SignalID              string `json:"signalId"`
	SignalPayload         string `json:"signalPayload"`
	ExpectedOutput        string `json:"expectedOutput"`
	ExpectedTimerOutput   string `json:"expectedTimerOutput"`
	ExpectedPrefixEffects int64  `json:"expectedPrefixEffects"`
	ExpectedSuffixEffects int64  `json:"expectedSuffixEffects"`
} {
	t.Helper()
	data, err := os.ReadFile("../../testdata/distributed/runtime-continuation-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		SignalID              string `json:"signalId"`
		SignalPayload         string `json:"signalPayload"`
		ExpectedOutput        string `json:"expectedOutput"`
		ExpectedTimerOutput   string `json:"expectedTimerOutput"`
		ExpectedPrefixEffects int64  `json:"expectedPrefixEffects"`
		ExpectedSuffixEffects int64  `json:"expectedSuffixEffects"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func newWaitIntegrationRuntime(t *testing.T, store *distributed.Store, workflowName string, instructions []contract.InternalInstruction, nodes map[string]node.Any) *Runtime {
	t.Helper()
	program := contract.InternalProgram{WorkflowID: workflowName, Digest: "sha256:" + strings.Repeat("e", 64), Instructions: instructions}
	runtime, err := New(store, engine.New(nodes), map[string]Workflow{workflowName: {Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input waitInput
		decoder := json.NewDecoder(strings.NewReader(string(raw)))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			return nil, err
		}
		return input, nil
	}}}, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return runtime
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
		runs, err := store.ListActiveRunIDs(ctx, partition, maxPartitionAdmissions)
		if err != nil {
			return "", err
		}
		if len(runs) == 0 {
			return partition, nil
		}
	}
	return "", errors.New("cluster integration: no empty partition available for isolated fixture")
}

func releaseOwnerOnCleanup(t *testing.T, store *distributed.Store, owner distributed.Owner) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := store.Release(ctx, owner); err != nil && !errors.Is(err, distributed.ErrOwnershipLost) {
			t.Logf("release integration owner %s/%s: %v", owner.Partition, owner.ID, err)
		}
	})
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
