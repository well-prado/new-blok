package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/clustertest"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
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
		ConflictingIngressRequests int    `json:"conflictingIngressRequests"`
		ExpectedConflictAccepted   int    `json:"expectedConflictAccepted"`
		ExpectedConflictDuplicates int    `json:"expectedConflictDuplicates"`
		ExpectedConflictRejections int    `json:"expectedConflictRejections"`
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

	// The same request key with two different inputs, raced concurrently
	// through both ingress clients: exactly one input is accepted, every
	// identical retry reconciles to it and every different input conflicts.
	type conflictOutcome struct {
		input     string
		admission Admission
		err       error
	}
	conflictStart := make(chan struct{})
	conflictResults := make(chan conflictOutcome, fixture.ConflictingIngressRequests)
	var conflictReady sync.WaitGroup
	for index := 0; index < fixture.ConflictingIngressRequests; index++ {
		conflictReady.Add(1)
		go func(index int) {
			defer conflictReady.Done()
			input := fmt.Sprintf(`{"value":%d}`, 50+index%2)
			runtime := first
			if (index/2)%2 == 1 {
				runtime = second
			}
			<-conflictStart
			admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "request-conflict", Workflow: "distributed-fixture", Input: json.RawMessage(input)})
			conflictResults <- conflictOutcome{input, admission, err}
		}(index)
	}
	close(conflictStart)
	conflictReady.Wait()
	close(conflictResults)
	winner := ""
	outcomes := make([]conflictOutcome, 0, fixture.ConflictingIngressRequests)
	for outcome := range conflictResults {
		outcomes = append(outcomes, outcome)
		if outcome.err == nil && outcome.admission.Accepted {
			if winner != "" {
				t.Fatalf("two inputs accepted for one request key: %s and %s", winner, outcome.input)
			}
			winner = outcome.input
		}
	}
	var conflictAccepted, conflictDuplicates, conflictRejected int
	for _, outcome := range outcomes {
		switch {
		case outcome.err == nil && outcome.admission.Accepted:
			conflictAccepted++
		case outcome.err == nil && outcome.input == winner:
			conflictDuplicates++
		case errors.Is(outcome.err, ErrRequestConflict) && outcome.input != winner:
			conflictRejected++
		default:
			t.Fatalf("conflicting ingress outcome input=%s admission=%+v err=%v winner=%s", outcome.input, outcome.admission, outcome.err, winner)
		}
	}
	if conflictAccepted != fixture.ExpectedConflictAccepted || conflictDuplicates != fixture.ExpectedConflictDuplicates || conflictRejected != fixture.ExpectedConflictRejections {
		t.Fatalf("conflicting ingress accepted=%d duplicates=%d conflicts=%d; fixture %d/%d/%d", conflictAccepted, conflictDuplicates, conflictRejected, fixture.ExpectedConflictAccepted, fixture.ExpectedConflictDuplicates, fixture.ExpectedConflictRejections)
	}
	effectsBefore := effects.Load()
	conflictRun, err := first.processOne(ctx, owner)
	if err != nil || conflictRun.State != fixture.ExpectedFinalState || string(conflictRun.Input) != winner || effects.Load()-effectsBefore != int64(fixture.ExpectedEffects) {
		t.Fatalf("conflict winner run=%+v err=%v winner=%s effect delta=%d", conflictRun, err, winner, effects.Load()-effectsBefore)
	}
	t.Logf("concurrent same-key race: winner input %s accepted once, %d duplicates, %d conflicts", winner, conflictDuplicates, conflictRejected)
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
	definition := node.MustDefine("fixture/load-effect", "1.0.0", func(_ context.Context, input loadInput) (loadOutput, error) {
		invocation := effects.Add(1)
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
		t.Fatalf("no isolated partition available for sustained-load fixture: %v", err)
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
	processedTenants := []string{firstRecovered.Tenant}
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
		processedTenants = append(processedTenants, run.Tenant)
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
	firstRound := map[string]bool{}
	for _, tenant := range processedTenants[:fixture.ExpectedFirstRoundTenants] {
		firstRound[tenant] = true
	}
	if len(firstRound) != fixture.ExpectedFirstRoundTenants {
		t.Fatalf("first post-takeover run round served %d tenants, want %d; order=%v", len(firstRound), fixture.ExpectedFirstRoundTenants, processedTenants)
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

// TestPureStepDispatchCanRetryAfterOwnerTakeover moves ownership after the
// first owner durably dispatched a pure step and before it invokes the node.
// The successor re-dispatches under a new attempt and completes the run; the
// stale owner still runs its pure node but cannot publish the result.
func TestPureStepDispatchCanRetryAfterOwnerTakeover(t *testing.T) {
	var fixture struct {
		ExpectedAttempts        int    `json:"expectedAttempts"`
		ExpectedDifferentIDs    bool   `json:"expectedDifferentAttemptIDs"`
		ExpectedPureInvocations int    `json:"expectedPureInvocations"`
		ExpectedExternalEffects int    `json:"expectedExternalEffects"`
		ExpectedFinalState      string `json:"expectedFinalState"`
		ExpectedOutput          string `json:"expectedOutput"`
		ExpectedStaleResult     string `json:"expectedStaleResult"`
	}
	readDistributedFixture(t, "pure-step-takeover-fixtures.json", &fixture)
	ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
	direct := integrationDistributedStore(t)
	hooked := &hookedClient{Client: integrationClient(t)}
	staleRuntime, err := ownerFaultRuntime(integrationStoreFor(t, hooked), ledger, nil)
	if err != nil {
		t.Fatal(err)
	}
	successorRuntime, err := ownerFaultRuntime(direct, ledger, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := successorRuntime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	const partition = "p-0000"
	tenant := tenantsInPartition(successorRuntime, partition, "pure-retry", 1)[0]
	admission, err := successorRuntime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "pure-retry", Workflow: "owner-fault", Input: json.RawMessage(`{"value":40}`)})
	if err != nil {
		t.Fatal(err)
	}
	oldOwner := acquireWhenFree(t, ctx, direct, partition, "pure-retry-old-owner", 30*time.Second)
	var newOwner distributed.Owner
	var successor RunRecord
	var successorErr error
	hooked.arm("/events/dispatch-", 0, true, func() {
		if err := direct.Release(ctx, oldOwner); err != nil {
			t.Errorf("release old owner after pure dispatch: %v", err)
		}
		newOwner = acquireWhenFree(t, ctx, direct, partition, "pure-retry-new-owner", 30*time.Second)
		successor, successorErr = successorRuntime.processOne(ctx, newOwner)
	})
	_, staleErr := staleRuntime.processOne(ctx, oldOwner)
	staleResult := "accepted"
	if errors.Is(staleErr, distributed.ErrOwnershipLost) {
		staleResult = "ownership_lost"
	} else if staleErr != nil {
		staleResult = "error: " + staleErr.Error()
	}
	events, err := direct.ListEvents(ctx, partition)
	if err != nil {
		t.Fatal(err)
	}
	attempts := map[int64]stepRecord{}
	for _, event := range events {
		var step stepRecord
		if event.Kind == "step.dispatched" && json.Unmarshal(event.Payload, &step) == nil && step.Identity.StepID == "prepare" {
			attempts[event.Fence] = step
		}
	}
	final, err := successorRuntime.GetRun(ctx, tenant, admission.RunID)
	if err != nil {
		t.Fatal(err)
	}
	successorAttempt := attempts[newOwner.Token]
	t.Logf("pure-step takeover: oldFence=%d newFence=%d attempts=%+v stale=%s final=%s output=%s pure=%d effects=%d", oldOwner.Token, newOwner.Token, attempts, staleResult, final.State, final.Output, ledger.total("pure"), ledger.total("effect"))
	if successorErr != nil || successor.State != fixture.ExpectedFinalState || final.State != fixture.ExpectedFinalState || string(final.Output) != fixture.ExpectedOutput {
		t.Fatalf("successor=%+v err=%v final=%+v; fixture %s %s", successor, successorErr, final, fixture.ExpectedFinalState, fixture.ExpectedOutput)
	}
	if successorAttempt.AttemptNumber != fixture.ExpectedAttempts || (attempts[oldOwner.Token].CurrentAttempt != successorAttempt.CurrentAttempt) != fixture.ExpectedDifferentIDs || len(attempts) != 2 {
		t.Fatalf("pure dispatch attempts=%+v; fixture attempts=%d distinct=%v", attempts, fixture.ExpectedAttempts, fixture.ExpectedDifferentIDs)
	}
	if ledger.total("pure") != fixture.ExpectedPureInvocations || ledger.total("effect") != fixture.ExpectedExternalEffects || staleResult != fixture.ExpectedStaleResult {
		t.Fatalf("pure=%d effects=%d stale=%s; fixture %d/%d/%s", ledger.total("pure"), ledger.total("effect"), staleResult, fixture.ExpectedPureInvocations, fixture.ExpectedExternalEffects, fixture.ExpectedStaleResult)
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
	// This test pauses voters: it runs alone on the shared cluster.
	clustertest.Disrupt(t)
	store := integrationDistributedStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	partition := fmt.Sprintf("p-wait-quorum-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "wait-quorum-owner", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	releaseOwnerOnCleanup(t, store, owner)
	voters := clustertest.Voters()
	t.Logf("quorum-loss probe state: voters=%v paused=%v partition=%s run=absent wait=wait-absent", voters, voters[1:], partition)
	restore := clustertest.PauseQuorum(t)
	blockedCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	_, err = (&Runtime{store: store}).scheduleWait(blockedCtx, owner, "run-absent", "wait-absent", "approval", time.Now().Add(time.Minute), engine.StepIdentity{}, 0)
	stop()
	if !errors.Is(err, ErrUnavailable) || errors.Is(err, distributed.ErrOwnershipLost) {
		t.Fatalf("quorum-loss schedule error=%v, want retryable ErrUnavailable and not ErrOwnershipLost", err)
	}
	restore()
	var recoveryErr error
	for ctx.Err() == nil {
		if _, _, recoveryErr = store.ReadState(ctx, partition, "recovery-check"); recoveryErr == nil {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(100 * time.Millisecond):
		}
	}
	t.Fatalf("quorum did not recover after restoring all voters: last read error=%v context=%v", recoveryErr, ctx.Err())
}

func TestStepJournalQuorumLossDefersAcceptedRun(t *testing.T) {
	// This test pauses voters: it runs alone on the shared cluster.
	clustertest.Disrupt(t)
	var fixture struct {
		ExpectedFailureClass      string `json:"expectedFailureClass"`
		ExpectedPrefixInvocations int64  `json:"expectedPrefixInvocations"`
		ExpectedExternalEffects   int64  `json:"expectedExternalEffects"`
		ExpectedFinalState        string `json:"expectedFinalState"`
		ExpectedActiveRuns        int    `json:"expectedActiveRuns"`
		ExpectedOutput            string `json:"expectedOutput"`
		ExpectedStateAfterOutage  string `json:"expectedStateAfterOutage"`
		ExpectedActiveAfterOutage int    `json:"expectedActiveRunsAfterOutage"`
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
	voters := clustertest.Voters()
	t.Logf("quorum journal recovery state: voters=%v paused=%v partition=%s run=%s", voters, voters[1:], partition, admission.RunID)
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
	restoreVoters := clustertest.PauseQuorum(t)
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
	restoreVoters()
	afterOutage, err := runtime.GetRun(ctx, tenant, admission.RunID)
	if err != nil {
		t.Fatal(err)
	}
	activeAfterOutage, err := store.ListActiveRunIDs(ctx, partition, runtime.limits.PartitionAdmissions)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("after the journal outage: run state=%s owner=%s active=%d", afterOutage.State, afterOutage.OwnerID, len(activeAfterOutage))
	if afterOutage.State != fixture.ExpectedStateAfterOutage || afterOutage.OwnerID != owner.ID || len(activeAfterOutage) != fixture.ExpectedActiveAfterOutage {
		t.Fatalf("after outage run=%+v active=%v; fixture state=%s active=%d owned by the failed owner", afterOutage, activeAfterOutage, fixture.ExpectedStateAfterOutage, fixture.ExpectedActiveAfterOutage)
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
		t.Fatalf("no isolated partition available for runtime fixture: %v", err)
	}
	base := fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
	for candidate := 0; ; candidate++ {
		tenant := fmt.Sprintf("%s-%d", base, candidate)
		if runtime.Partition(tenant) == partition {
			return tenant, partition
		}
	}
}
