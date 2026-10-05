package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
)

// The tests in this file run against the real three-voter etcd cluster named
// by BLOK_DISTRIBUTED_ENDPOINTS. Each uses a private key namespace, so every
// expected count below is measured from an empty partition space and compared
// with a predeclared fixture under testdata/distributed.

func readDistributedFixture(t *testing.T, name string, into any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "distributed", name))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(into); err != nil {
		t.Fatalf("decode fixture %s: %v", name, err)
	}
}

func eventCount(t *testing.T, ctx context.Context, store *distributed.Store, partition, kind string) int {
	t.Helper()
	events, err := store.ListEvents(ctx, partition)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, event := range events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func decodeTyped[T any](raw json.RawMessage) (any, error) {
	var value T
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

// newCountedEffectRuntime runs one effectful step whose every invocation is
// appended to ledger before it returns, then publishes its output.
func newCountedEffectRuntime(t *testing.T, store *distributed.Store, ledger effectLedger, limits Limits) *Runtime {
	t.Helper()
	effect := node.MustDefine("fixture/acceptance-effect", "1.0.0", func(_ context.Context, input integrationInput) (integrationOutput, error) {
		if err := ledger.append("effect", fmt.Sprint(input.Value)); err != nil {
			return integrationOutput{}, err
		}
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("counted synthetic external effect"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema)), node.Effects("fixture:acceptance-effect")).Any()
	program := contract.InternalProgram{WorkflowID: "acceptance-effect", Digest: "sha256:" + strings.Repeat("1", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "effect", Kind: "call", Node: "fixture/acceptance-effect"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "effect"}}},
	}}
	runtime, err := New(store, engine.New(map[string]node.Any{"fixture/acceptance-effect": effect}), map[string]Workflow{"acceptance-effect": {Program: program, DecodeInput: decodeTyped[integrationInput]}}, limits)
	if err != nil {
		t.Fatal(err)
	}
	return runtime
}

// newCountedWaitRuntime runs prefix effect -> durable wait -> suffix effect.
// The suffix publishes 1 when resumed by a signal and 2 when resumed by the
// wait's timeout, so the final output identifies which transition won.
func newCountedWaitRuntime(t *testing.T, store *distributed.Store, ledger effectLedger, timeoutMillis int64) *Runtime {
	t.Helper()
	prefix := node.MustDefine("fixture/acceptance-prefix", "1.0.0", func(_ context.Context, input waitInput) (integrationOutput, error) {
		if err := ledger.append("prefix", fmt.Sprint(input.Value)); err != nil {
			return integrationOutput{}, err
		}
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("counted pre-wait effect"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema)), node.Effects("fixture:acceptance-prefix")).Any()
	suffix := node.MustDefine("fixture/acceptance-suffix", "1.0.0", func(_ context.Context, input engine.WaitResult) (integrationOutput, error) {
		outcome := 1
		if input.TimedOut {
			outcome = 2
		}
		if err := ledger.append("suffix", fmt.Sprint(outcome)); err != nil {
			return integrationOutput{}, err
		}
		return integrationOutput{Value: outcome}, nil
	}, node.Description("counted post-wait effect"), node.Schemas([]byte(waitConsumerSchema), []byte(outputSchema)), node.Effects("fixture:acceptance-suffix")).Any()
	return newWaitIntegrationRuntime(t, store, "acceptance-wait", []contract.InternalInstruction{
		{Index: 0, ID: "prefix", Kind: "call", Node: "fixture/acceptance-prefix"},
		{Index: 1, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval", TimeoutMillis: timeoutMillis}},
		{Index: 2, ID: "suffix", Kind: "call", Node: "fixture/acceptance-suffix", References: []contract.Reference{{Step: "approval"}}},
		{Index: 3, ID: "output", Kind: "output", References: []contract.Reference{{Step: "suffix"}}},
	}, map[string]node.Any{"fixture/acceptance-prefix": prefix, "fixture/acceptance-suffix": suffix})
}

func TestAdmissionCapsBoundTenantAndPartitionAcrossIngressAndTakeover(t *testing.T) {
	var fixture struct {
		FixtureVersion int    `json:"fixtureVersion"`
		Synthetic      bool   `json:"synthetic"`
		Name           string `json:"name"`
		Limits         struct {
			Partitions          int `json:"partitions"`
			PartitionAdmissions int `json:"partitionAdmissions"`
			TenantAdmissions    int `json:"tenantAdmissions"`
		} `json:"limits"`
		MonopolizerSubmits int `json:"monopolizingTenantConcurrentSubmits"`
		SecondSubmits      int `json:"secondTenantConcurrentSubmits"`
		Expected           struct {
			MonopolizerAccepted   int    `json:"monopolizerAccepted"`
			MonopolizerFull       int    `json:"monopolizerAdmissionFull"`
			SecondAccepted        int    `json:"secondTenantAccepted"`
			SecondFull            int    `json:"secondTenantAdmissionFull"`
			ThirdWithRoom         string `json:"thirdTenantWhilePartitionHasRoom"`
			FourthWhileFull       string `json:"fourthTenantWhilePartitionFull"`
			DuplicateWhileFull    string `json:"duplicateWhileFull"`
			ActiveWhenFull        int    `json:"activeRunsWhenFull"`
			ActiveAfterTakeover   int    `json:"activeRunsAfterTakeover"`
			FirstFinishedTenant   string `json:"firstFinishedTenant"`
			ActiveAfterOneFinish  int    `json:"activeRunsAfterOneFinish"`
			MonopolizerReadmitted string `json:"monopolizerReadmittedAfterFinish"`
			FourthAfterRefill     string `json:"fourthTenantAfterRefill"`
			ExternalEffects       int    `json:"externalEffects"`
			HTTPOverCap           int    `json:"httpOverTenantCapStatus"`
			HTTPRetryAfter        string `json:"httpRetryAfter"`
			HTTPQuorumLoss        int    `json:"httpQuorumLossStatus"`
			HTTPAccepted          int    `json:"httpAcceptedStatus"`
			HTTPConflict          int    `json:"httpConflictStatus"`
			HTTPSignalQuorumLoss  int    `json:"httpSignalQuorumLossStatus"`
			HTTPRecovered         string `json:"httpRecoveredAdmission"`
		} `json:"expected"`
	}
	readDistributedFixture(t, "admission-capacity-fixtures.json", &fixture)
	limits := Limits{Partitions: fixture.Limits.Partitions, PartitionAdmissions: fixture.Limits.PartitionAdmissions, TenantAdmissions: fixture.Limits.TenantAdmissions, OwnerTTL: 2 * time.Second}
	ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
	direct := integrationDistributedStore(t)
	ingress := []*Runtime{
		newCountedEffectRuntime(t, direct, ledger, limits),
		newCountedEffectRuntime(t, integrationDistributedStore(t), ledger, limits),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := ingress[0].Check(ctx); err != nil {
		t.Fatal(err)
	}
	const partition = "p-0000"
	tenants := tenantsInPartition(ingress[0], partition, "capacity-tenant", 4)
	monopolizer, second, third, fourth := tenants[0], tenants[1], tenants[2], tenants[3]
	type submission struct {
		tenant, key string
		admission   Admission
		err         error
	}
	planned := make([]submission, 0, fixture.MonopolizerSubmits+fixture.SecondSubmits)
	for index := 0; index < fixture.MonopolizerSubmits; index++ {
		planned = append(planned, submission{tenant: monopolizer, key: fmt.Sprintf("monopolizer-%d", index)})
	}
	for index := 0; index < fixture.SecondSubmits; index++ {
		planned = append(planned, submission{tenant: second, key: fmt.Sprintf("second-%d", index)})
	}
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := range planned {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			<-start
			request := planned[index]
			planned[index].admission, planned[index].err = ingress[index%2].Admit(ctx, Submission{Tenant: request.tenant, RequestKey: request.key, Workflow: "acceptance-effect", Input: json.RawMessage(fmt.Sprintf(`{"value":%d}`, index))})
		}(index)
	}
	close(start)
	group.Wait()
	accepted := map[string]int{}
	full := map[string]int{}
	acceptedKeys := map[string][]string{}
	for _, result := range planned {
		switch {
		case result.err == nil && result.admission.Accepted:
			accepted[result.tenant]++
			acceptedKeys[result.tenant] = append(acceptedKeys[result.tenant], result.key)
		case errors.Is(result.err, distributed.ErrAdmissionFull):
			full[result.tenant]++
		default:
			t.Fatalf("unexpected concurrent admission for %s/%s: %+v err=%v", result.tenant, result.key, result.admission, result.err)
		}
	}
	t.Logf("concurrent admission across two ingress clients: accepted=%v full=%v", accepted, full)
	if accepted[monopolizer] != fixture.Expected.MonopolizerAccepted || full[monopolizer] != fixture.Expected.MonopolizerFull || accepted[second] != fixture.Expected.SecondAccepted || full[second] != fixture.Expected.SecondFull {
		t.Fatalf("monopolizer accepted/full=%d/%d second=%d/%d; fixture %d/%d and %d/%d", accepted[monopolizer], full[monopolizer], accepted[second], full[second], fixture.Expected.MonopolizerAccepted, fixture.Expected.MonopolizerFull, fixture.Expected.SecondAccepted, fixture.Expected.SecondFull)
	}
	outcome := func(admission Admission, err error) string {
		switch {
		case errors.Is(err, distributed.ErrAdmissionFull):
			return "admission_full"
		case err != nil:
			return "error: " + err.Error()
		case admission.Accepted:
			return "accepted"
		default:
			return "duplicate"
		}
	}
	// The monopolizer's rejected backlog leaves partition room for others.
	if got := outcome(ingress[1].Admit(ctx, Submission{Tenant: third, RequestKey: "third-0", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":100}`)})); got != fixture.Expected.ThirdWithRoom {
		t.Fatalf("third tenant while partition has room=%s, fixture %s", got, fixture.Expected.ThirdWithRoom)
	}
	if got := outcome(ingress[0].Admit(ctx, Submission{Tenant: fourth, RequestKey: "fourth-0", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":110}`)})); got != fixture.Expected.FourthWhileFull {
		t.Fatalf("fourth tenant while partition full=%s, fixture %s", got, fixture.Expected.FourthWhileFull)
	}
	firstKey := acceptedKeys[monopolizer][0]
	var firstInput string
	for index, result := range planned {
		if result.key == firstKey {
			firstInput = fmt.Sprintf(`{"value":%d}`, index)
		}
	}
	if got := outcome(ingress[0].Admit(ctx, Submission{Tenant: monopolizer, RequestKey: firstKey, Workflow: "acceptance-effect", Input: json.RawMessage(firstInput)})); got != fixture.Expected.DuplicateWhileFull {
		t.Fatalf("duplicate of accepted key while full=%s, fixture %s", got, fixture.Expected.DuplicateWhileFull)
	}
	before, err := direct.ListActiveRunIDs(ctx, partition, limits.PartitionAdmissions)
	if err != nil || len(before) != fixture.Expected.ActiveWhenFull {
		t.Fatalf("active runs when full=%v err=%v, fixture %d", before, err, fixture.Expected.ActiveWhenFull)
	}

	firstOwner := acquireWhenFree(t, ctx, direct, partition, "capacity-owner-1", 2*time.Second)
	if err := direct.Release(ctx, firstOwner); err != nil {
		t.Fatal(err)
	}
	successor := acquireWhenFree(t, ctx, direct, partition, "capacity-owner-2", 30*time.Second)
	after, err := direct.ListActiveRunIDs(ctx, partition, limits.PartitionAdmissions)
	if err != nil || len(after) != fixture.Expected.ActiveAfterTakeover || strings.Join(after, ",") != strings.Join(before, ",") {
		t.Fatalf("active runs after takeover=%v err=%v; before=%v fixture %d", after, err, before, fixture.Expected.ActiveAfterTakeover)
	}
	if got := outcome(ingress[1].Admit(ctx, Submission{Tenant: fourth, RequestKey: "fourth-1", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":111}`)})); got != fixture.Expected.FourthWhileFull {
		t.Fatalf("fourth tenant after takeover=%s, fixture %s", got, fixture.Expected.FourthWhileFull)
	}
	finished, err := ingress[0].processOne(ctx, successor)
	if err != nil || finished.State != "completed" {
		t.Fatalf("successor finished=%+v err=%v", finished, err)
	}
	finishedTenant := map[string]string{monopolizer: "monopolizer", second: "second", third: "third", fourth: "fourth"}[finished.Tenant]
	if finishedTenant != fixture.Expected.FirstFinishedTenant {
		t.Fatalf("first finished tenant=%s, fixture %s", finishedTenant, fixture.Expected.FirstFinishedTenant)
	}
	remaining, err := direct.ListActiveRunIDs(ctx, partition, limits.PartitionAdmissions)
	if err != nil || len(remaining) != fixture.Expected.ActiveAfterOneFinish {
		t.Fatalf("active runs after one finish=%v err=%v, fixture %d", remaining, err, fixture.Expected.ActiveAfterOneFinish)
	}
	if got := outcome(ingress[1].Admit(ctx, Submission{Tenant: monopolizer, RequestKey: "monopolizer-after-finish", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":200}`)})); got != fixture.Expected.MonopolizerReadmitted {
		t.Fatalf("monopolizer after its slot was released=%s, fixture %s", got, fixture.Expected.MonopolizerReadmitted)
	}
	if got := outcome(ingress[0].Admit(ctx, Submission{Tenant: fourth, RequestKey: "fourth-2", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":112}`)})); got != fixture.Expected.FourthAfterRefill {
		t.Fatalf("fourth tenant after refill=%s, fixture %s", got, fixture.Expected.FourthAfterRefill)
	}
	if got := ledger.total("effect"); got != fixture.Expected.ExternalEffects {
		t.Fatalf("external effects=%d, fixture %d", got, fixture.Expected.ExternalEffects)
	}
}

func TestSignalAcrossOwnerChangeIsRetryableAndResumesOnce(t *testing.T) {
	var fixture signalTimerOwnershipFixture
	readDistributedFixture(t, "signal-timer-ownership-fixtures.json", &fixture)
	expected := fixture.SignalAcrossOwnerChange
	ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
	direct := integrationDistributedStore(t)
	hooked := &hookedClient{Client: integrationClient(t)}
	owners := newCountedWaitRuntime(t, direct, ledger, 0)
	ingress := newCountedWaitRuntime(t, integrationStoreFor(t, hooked), ledger, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := owners.Check(ctx); err != nil {
		t.Fatal(err)
	}
	const partition = "p-0000"
	tenant := tenantsInPartition(owners, partition, "signal-owner-change", 1)[0]
	admission, err := owners.Admit(ctx, Submission{Tenant: tenant, RequestKey: "owner-change", Workflow: "acceptance-wait", Input: json.RawMessage(`{"value":10}`)})
	if err != nil {
		t.Fatal(err)
	}
	oldOwner := acquireWhenFree(t, ctx, direct, partition, "signal-owner-old", 30*time.Second)
	if _, err := owners.processOne(ctx, oldOwner); !errors.Is(err, ErrNoWork) {
		t.Fatalf("initial process=%v, want suspended wait", err)
	}
	waitID := WaitIDFor(admission.RunID, "approval")
	var newOwner distributed.Owner
	// Between ingress reading the current owner and sending its fenced
	// transaction, ownership moves to a new owner with a higher fence.
	hooked.arm("/events/signal-", 0, false, func() {
		if err := direct.Release(ctx, oldOwner); err != nil {
			t.Errorf("release old owner inside signal window: %v", err)
		}
		newOwner = acquireWhenFree(t, ctx, direct, partition, "signal-owner-new", 30*time.Second)
	})
	payload := json.RawMessage(`{"approved":true}`)
	unauthorized, err := ingress.DeliverSignal(ctx, tenant, waitID, "unauthorized-signal", "synthetic-principal", payload, false)
	unauthorizedLabel := signalOutcome(unauthorized, err)
	if errors.Is(err, ErrUnauthorizedSignal) {
		unauthorizedLabel = "signal_unauthorized"
	}
	if unauthorizedLabel != expected.Unauthorized {
		t.Fatalf("unauthorized signal=%s err=%v, fixture %s", unauthorizedLabel, err, expected.Unauthorized)
	}
	result, err := ingress.DeliverSignal(ctx, tenant, waitID, "owner-change-signal", "synthetic-principal", payload, true)
	got := "accepted"
	if errors.Is(err, ErrUnavailable) {
		got = "unavailable"
	} else if err != nil {
		got = "error: " + err.Error()
	}
	if got != expected.FirstAttempt || result.Accepted || result.Late || result.Duplicate || newOwner.Token <= oldOwner.Token {
		t.Fatalf("signal across owner change=%s result=%+v err=%v fences old=%d new=%d; fixture %s", got, result, err, oldOwner.Token, newOwner.Token, expected.FirstAttempt)
	}
	wait, err := owners.GetWait(ctx, tenant, waitID)
	if err != nil || wait.State != expected.WaitAfterFirstAttempt {
		t.Fatalf("wait after unacknowledged signal=%+v err=%v, fixture %s", wait, err, expected.WaitAfterFirstAttempt)
	}
	run, err := owners.GetRun(ctx, tenant, admission.RunID)
	if err != nil || run.State != expected.RunAfterFirstAttempt {
		t.Fatalf("run after unacknowledged signal=%+v err=%v, fixture %s", run, err, expected.RunAfterFirstAttempt)
	}
	if got := eventCount(t, ctx, direct, partition, "wait.signaled"); got != expected.SignaledEventsAfterFirstAttempt {
		t.Fatalf("signaled events after stale-fence attempt=%d, fixture %d", got, expected.SignaledEventsAfterFirstAttempt)
	}
	retried, err := ingress.DeliverSignal(ctx, tenant, waitID, "owner-change-signal", "synthetic-principal", payload, true)
	if err != nil || !retried.Accepted || retried.Duplicate || expected.Retry != "accepted" {
		t.Fatalf("retry under new owner=%+v err=%v, fixture %s", retried, err, expected.Retry)
	}
	completed, err := owners.processOne(ctx, newOwner)
	if err != nil || completed.State != expected.FinalState || string(completed.Output) != expected.FinalOutput {
		t.Fatalf("resumed run=%+v err=%v, fixture %s %s", completed, err, expected.FinalState, expected.FinalOutput)
	}
	if ledger.total("prefix") != expected.PrefixEffects || ledger.total("suffix") != expected.SuffixEffects {
		t.Fatalf("effects prefix=%d suffix=%d, fixture %d/%d", ledger.total("prefix"), ledger.total("suffix"), expected.PrefixEffects, expected.SuffixEffects)
	}
}

type signalTimerOwnershipFixture struct {
	FixtureVersion          int    `json:"fixtureVersion"`
	Synthetic               bool   `json:"synthetic"`
	Name                    string `json:"name"`
	SignalAcrossOwnerChange struct {
		Unauthorized                    string `json:"unauthorized"`
		FirstAttempt                    string `json:"firstAttempt"`
		WaitAfterFirstAttempt           string `json:"waitAfterFirstAttempt"`
		RunAfterFirstAttempt            string `json:"runAfterFirstAttempt"`
		SignaledEventsAfterFirstAttempt int    `json:"signaledEventsAfterFirstAttempt"`
		Retry                           string `json:"retry"`
		FinalState                      string `json:"finalState"`
		FinalOutput                     string `json:"finalOutput"`
		PrefixEffects                   int    `json:"prefixEffects"`
		SuffixEffects                   int    `json:"suffixEffects"`
	} `json:"signalAcrossOwnerChange"`
	StaleOwnerTimer struct {
		StaleResult        string `json:"staleResult"`
		WaitAfterStaleFire string `json:"waitAfterStaleFire"`
		DueTimersAfter     int    `json:"dueTimersAfterStaleFire"`
		SuccessorFired     int    `json:"successorFired"`
		FinalState         string `json:"finalState"`
		FinalOutput        string `json:"finalOutput"`
		PrefixEffects      int    `json:"prefixEffects"`
		SuffixEffects      int    `json:"suffixEffects"`
	} `json:"staleOwnerTimer"`
	ThreeWayRace struct {
		Iterations          int    `json:"iterations"`
		WinnersPerWait      int    `json:"winnersPerWait"`
		StaleOwnerFires     int    `json:"staleOwnerFires"`
		PrefixEffectsPerRun int    `json:"prefixEffectsPerRun"`
		SuffixEffectsPerRun int    `json:"suffixEffectsPerRun"`
		SignalOutput        string `json:"signalOutput"`
		TimerOutput         string `json:"timerOutput"`
	} `json:"threeWayRace"`
}

func TestStaleOwnerTimerIsFencedAndKeepsTimerIndex(t *testing.T) {
	var fixture signalTimerOwnershipFixture
	readDistributedFixture(t, "signal-timer-ownership-fixtures.json", &fixture)
	expected := fixture.StaleOwnerTimer
	ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
	store := integrationDistributedStore(t)
	runtime := newCountedWaitRuntime(t, store, ledger, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	const partition = "p-0000"
	tenant := tenantsInPartition(runtime, partition, "stale-timer", 1)[0]
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "stale-timer", Workflow: "acceptance-wait", Input: json.RawMessage(`{"value":20}`)})
	if err != nil {
		t.Fatal(err)
	}
	oldOwner := acquireWhenFree(t, ctx, store, partition, "timer-owner-old", 30*time.Second)
	if _, err := runtime.processOne(ctx, oldOwner); !errors.Is(err, ErrNoWork) {
		t.Fatalf("initial process=%v, want suspended wait", err)
	}
	if err := store.Release(ctx, oldOwner); err != nil {
		t.Fatal(err)
	}
	newOwner := acquireWhenFree(t, ctx, store, partition, "timer-owner-new", 30*time.Second)
	due := time.Now().UTC().Add(time.Second)
	fired, err := runtime.FireDueWaits(ctx, oldOwner, due, 8)
	staleResult := "fired"
	if errors.Is(err, distributed.ErrOwnershipLost) {
		staleResult = "ownership_lost"
	} else if err != nil {
		staleResult = "error: " + err.Error()
	}
	if staleResult != expected.StaleResult || len(fired) != 0 {
		t.Fatalf("stale owner timer=%s fired=%d err=%v, fixture %s", staleResult, len(fired), err, expected.StaleResult)
	}
	wait, err := runtime.GetWait(ctx, tenant, WaitIDFor(admission.RunID, "approval"))
	if err != nil || wait.State != expected.WaitAfterStaleFire {
		t.Fatalf("wait after stale timer=%+v err=%v, fixture %s", wait, err, expected.WaitAfterStaleFire)
	}
	timers, err := store.ListDueTimers(ctx, partition, due, 8)
	if err != nil || len(timers) != expected.DueTimersAfter {
		t.Fatalf("timer index after stale fire=%v err=%v, fixture %d", timers, err, expected.DueTimersAfter)
	}
	fired, err = runtime.FireDueWaits(ctx, newOwner, due, 8)
	if err != nil || len(fired) != expected.SuccessorFired {
		t.Fatalf("successor fired=%d err=%v, fixture %d", len(fired), err, expected.SuccessorFired)
	}
	completed, err := runtime.processOne(ctx, newOwner)
	if err != nil || completed.State != expected.FinalState || string(completed.Output) != expected.FinalOutput {
		t.Fatalf("resumed run=%+v err=%v, fixture %s %s", completed, err, expected.FinalState, expected.FinalOutput)
	}
	if ledger.total("prefix") != expected.PrefixEffects || ledger.total("suffix") != expected.SuffixEffects {
		t.Fatalf("effects prefix=%d suffix=%d, fixture %d/%d", ledger.total("prefix"), ledger.total("suffix"), expected.PrefixEffects, expected.SuffixEffects)
	}
}

// TestSignalTimerAndStaleOwnerRaceHasOneWinnerPerWait races, for each run, a
// signal through a second ingress client, the current owner's timer, and the
// previous owner's timer against the same journaled wait.
func TestSignalTimerAndStaleOwnerRaceHasOneWinnerPerWait(t *testing.T) {
	var fixture signalTimerOwnershipFixture
	readDistributedFixture(t, "signal-timer-ownership-fixtures.json", &fixture)
	expected := fixture.ThreeWayRace
	ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
	store := integrationDistributedStore(t)
	owners := newCountedWaitRuntime(t, store, ledger, 1)
	ingress := newCountedWaitRuntime(t, integrationDistributedStore(t), ledger, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := owners.Check(ctx); err != nil {
		t.Fatal(err)
	}
	const partition = "p-0000"
	tenant := tenantsInPartition(owners, partition, "three-way-race", 1)[0]
	signalWins, timerWins := 0, 0
	for iteration := 0; iteration < expected.Iterations; iteration++ {
		prefixBefore, suffixBefore := ledger.total("prefix"), ledger.total("suffix")
		admission, err := owners.Admit(ctx, Submission{Tenant: tenant, RequestKey: fmt.Sprintf("race-%d", iteration), Workflow: "acceptance-wait", Input: json.RawMessage(fmt.Sprintf(`{"value":%d}`, iteration))})
		if err != nil || !admission.Accepted {
			t.Fatalf("admission=%+v err=%v", admission, err)
		}
		oldOwner, err := store.Acquire(ctx, partition, fmt.Sprintf("race-old-%d", iteration), 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := owners.processOne(ctx, oldOwner); !errors.Is(err, ErrNoWork) {
			t.Fatalf("iteration %d initial process=%v, want suspended wait", iteration, err)
		}
		if err := store.Release(ctx, oldOwner); err != nil {
			t.Fatal(err)
		}
		newOwner, err := store.Acquire(ctx, partition, fmt.Sprintf("race-new-%d", iteration), 30*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		waitID := WaitIDFor(admission.RunID, "approval")
		due := time.Now().UTC().Add(time.Second)
		start := make(chan struct{})
		var group sync.WaitGroup
		var signal SignalResult
		var signalErr, timerErr, staleErr error
		var timerFired, staleFired []WaitRecord
		group.Add(3)
		go func() {
			defer group.Done()
			<-start
			signal, signalErr = ingress.DeliverSignal(ctx, tenant, waitID, fmt.Sprintf("race-signal-%d", iteration), "synthetic-principal", json.RawMessage(`{"approved":true}`), true)
		}()
		go func() {
			defer group.Done()
			<-start
			timerFired, timerErr = owners.FireDueWaits(ctx, newOwner, due, 8)
		}()
		go func() {
			defer group.Done()
			<-start
			staleFired, staleErr = owners.FireDueWaits(ctx, oldOwner, due, 8)
		}()
		close(start)
		group.Wait()
		if signalErr != nil || timerErr != nil {
			t.Fatalf("iteration %d signal=%+v err=%v timer err=%v", iteration, signal, signalErr, timerErr)
		}
		if len(staleFired) != expected.StaleOwnerFires || (staleErr != nil && !errors.Is(staleErr, distributed.ErrOwnershipLost)) {
			t.Fatalf("iteration %d stale owner fired=%d err=%v, fixture %d", iteration, len(staleFired), staleErr, expected.StaleOwnerFires)
		}
		winners := len(timerFired)
		if signal.Accepted {
			winners++
		}
		wait, err := owners.GetWait(ctx, tenant, waitID)
		if err != nil || winners != expected.WinnersPerWait {
			t.Fatalf("iteration %d winners=%d (signal=%+v timer=%d) wait=%+v err=%v, fixture %d", iteration, winners, signal, len(timerFired), wait, err, expected.WinnersPerWait)
		}
		wantOutput := expected.SignalOutput
		switch {
		case signal.Accepted && wait.State == "signaled":
			signalWins++
		case signal.Late && wait.State == "timed_out":
			timerWins++
			wantOutput = expected.TimerOutput
		default:
			t.Fatalf("iteration %d inconsistent winner: signal=%+v timer=%d wait=%+v", iteration, signal, len(timerFired), wait)
		}
		completed, err := owners.processOne(ctx, newOwner)
		if err != nil || completed.State != "completed" || string(completed.Output) != wantOutput {
			t.Fatalf("iteration %d resumed=%+v err=%v, want completed %s", iteration, completed, err, wantOutput)
		}
		if ledger.total("prefix")-prefixBefore != expected.PrefixEffectsPerRun || ledger.total("suffix")-suffixBefore != expected.SuffixEffectsPerRun {
			t.Fatalf("iteration %d effect deltas prefix=%d suffix=%d, fixture %d/%d", iteration, ledger.total("prefix")-prefixBefore, ledger.total("suffix")-suffixBefore, expected.PrefixEffectsPerRun, expected.SuffixEffectsPerRun)
		}
		if err := store.Release(ctx, newOwner); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("three-way race over %d waits: signal won %d, current-owner timer won %d, stale-owner timer won 0", expected.Iterations, signalWins, timerWins)
}

type quorumTransitionFixture struct {
	FixtureVersion    int    `json:"fixtureVersion"`
	Synthetic         bool   `json:"synthetic"`
	Name              string `json:"name"`
	AdmitBeforeCommit struct {
		Result             string `json:"result"`
		RecordAfterRestore string `json:"recordAfterRestore"`
		Retry              string `json:"retry"`
		SecondRetry        string `json:"secondRetry"`
		AcceptedEvents     int    `json:"acceptedEvents"`
		ActiveRuns         int    `json:"activeRuns"`
	} `json:"admitBeforeCommit"`
	AdmitInFlight struct {
		Result         string   `json:"result"`
		RetryOneOf     []string `json:"retryOneOf"`
		AcceptedEvents int      `json:"acceptedEvents"`
		ActiveRuns     int      `json:"activeRuns"`
	} `json:"admitInFlight"`
	SignalBeforeCommit struct {
		Result           string `json:"result"`
		WaitAfterRestore string `json:"waitAfterRestore"`
		Retry            string `json:"retry"`
		SignaledEvents   int    `json:"signaledEvents"`
		FinalState       string `json:"finalState"`
		SuffixEffects    int    `json:"suffixEffects"`
	} `json:"signalBeforeCommit"`
	SignalInFlight struct {
		Result         string   `json:"result"`
		RetryOneOf     []string `json:"retryOneOf"`
		SignaledEvents int      `json:"signaledEvents"`
		FinalState     string   `json:"finalState"`
		SuffixEffects  int      `json:"suffixEffects"`
	} `json:"signalInFlight"`
	TimerBeforeCommit struct {
		Result         string `json:"result"`
		DueAfter       int    `json:"dueTimersAfterRestore"`
		RetryFired     int    `json:"retryFired"`
		TimedOutEvents int    `json:"timedOutEvents"`
		FinalState     string `json:"finalState"`
		SuffixEffects  int    `json:"suffixEffects"`
	} `json:"timerBeforeCommit"`
	TimerInFlight struct {
		Result          string `json:"result"`
		RetryFiredOneOf []int  `json:"retryFiredOneOf"`
		TimedOutEvents  int    `json:"timedOutEvents"`
		FinalState      string `json:"finalState"`
		SuffixEffects   int    `json:"suffixEffects"`
	} `json:"timerInFlight"`
	FinishInFlight struct {
		Result          string `json:"result"`
		Consistent      bool   `json:"consistentSlotsAndState"`
		FinalState      string `json:"finalState"`
		CompletedEvents int    `json:"completedEvents"`
		ActiveRuns      int    `json:"activeRuns"`
		ExternalEffects int    `json:"externalEffects"`
	} `json:"finishInFlight"`
	EffectCommitInFlight struct {
		Result           string   `json:"result"`
		FinalStateOneOf  []string `json:"finalStateOneOf"`
		ExternalEffects  int      `json:"externalEffects"`
		EffectDispatches int      `json:"effectDispatches"`
		ActiveRuns       int      `json:"activeRuns"`
	} `json:"effectCommitInFlight"`
}

func admissionOutcome(admission Admission, err error) string {
	switch {
	case errors.Is(err, ErrUnavailable):
		return "unavailable"
	case err != nil:
		return "error: " + err.Error()
	case admission.Accepted:
		return "accepted"
	default:
		return "duplicate"
	}
}

func signalOutcome(result SignalResult, err error) string {
	switch {
	case errors.Is(err, ErrUnavailable) && !result.Accepted && !result.Late && !result.Duplicate:
		return "unavailable"
	case err != nil:
		return "error: " + err.Error()
	case result.Duplicate:
		return "duplicate"
	case result.Accepted:
		return "accepted"
	case result.Late:
		return "late"
	default:
		return "unknown"
	}
}

func oneOf[T comparable](value T, allowed []T) bool {
	for _, candidate := range allowed {
		if candidate == value {
			return true
		}
	}
	return false
}

// TestQuorumLossAtEachRuntimeTransition removes etcd quorum (two of three
// voters paused) either before a transition is attempted or while its fenced
// transaction is in flight. Before-commit outages must reject without any
// durable change. In-flight outages have an unknown outcome; the declared
// behavior is that the caller is never acknowledged and a retry with the same
// identity reconciles to exactly one committed transition.
func TestQuorumLossAtEachRuntimeTransition(t *testing.T) {
	var fixture quorumTransitionFixture
	readDistributedFixture(t, "quorum-transition-fixtures.json", &fixture)
	const partition = "p-0000"
	const ownerTTL = 30 * time.Second

	t.Run("admit-before-commit", func(t *testing.T) {
		expected := fixture.AdmitBeforeCommit
		ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
		store := integrationDistributedStore(t)
		runtime := newCountedEffectRuntime(t, store, ledger, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 2 * time.Second})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := runtime.Check(ctx); err != nil {
			t.Fatal(err)
		}
		tenant := tenantsInPartition(runtime, partition, "quorum-admit", 1)[0]
		request := Submission{Tenant: tenant, RequestKey: "quorum-admit", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":1}`)}
		restore := pauseQuorum(t, store)
		blocked, stop := context.WithTimeout(ctx, 3*time.Second)
		got := admissionOutcome(runtime.Admit(blocked, request))
		stop()
		restore()
		if got != expected.Result {
			t.Fatalf("admit during quorum loss=%s, fixture %s", got, expected.Result)
		}
		record := "present"
		if active, err := store.ListActiveRunIDs(ctx, partition, 64); err != nil {
			t.Fatal(err)
		} else if len(active) == 0 && eventCount(t, ctx, store, partition, "run.accepted") == 0 {
			record = "absent"
		}
		if record != expected.RecordAfterRestore {
			t.Fatalf("record after restore=%s, fixture %s", record, expected.RecordAfterRestore)
		}
		if got := admissionOutcome(runtime.Admit(ctx, request)); got != expected.Retry {
			t.Fatalf("retry after restore=%s, fixture %s", got, expected.Retry)
		}
		if got := admissionOutcome(runtime.Admit(ctx, request)); got != expected.SecondRetry {
			t.Fatalf("second retry=%s, fixture %s", got, expected.SecondRetry)
		}
		active, err := store.ListActiveRunIDs(ctx, partition, 64)
		if got := eventCount(t, ctx, store, partition, "run.accepted"); got != expected.AcceptedEvents || err != nil || len(active) != expected.ActiveRuns {
			t.Fatalf("accepted events=%d active=%v err=%v, fixture %d/%d", got, active, err, expected.AcceptedEvents, expected.ActiveRuns)
		}
	})

	t.Run("admit-in-flight", func(t *testing.T) {
		expected := fixture.AdmitInFlight
		ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
		store := integrationDistributedStore(t)
		hooked := &hookedClient{Client: integrationClient(t)}
		runtime := newCountedEffectRuntime(t, integrationStoreFor(t, hooked), ledger, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 2 * time.Second})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := runtime.Check(ctx); err != nil {
			t.Fatal(err)
		}
		tenant := tenantsInPartition(runtime, partition, "quorum-admit-flight", 1)[0]
		request := Submission{Tenant: tenant, RequestKey: "quorum-admit-flight", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":2}`)}
		var restore func()
		hooked.arm("/events/accepted-", 0, false, func() { restore = pauseQuorum(t, store) })
		blocked, stop := context.WithTimeout(ctx, 4*time.Second)
		got := admissionOutcome(runtime.Admit(blocked, request))
		stop()
		if restore == nil {
			t.Fatal("admission transaction was never attempted")
		}
		restore()
		if got != expected.Result {
			t.Fatalf("in-flight admission=%s, fixture %s", got, expected.Result)
		}
		retry := admissionOutcome(runtime.Admit(ctx, request))
		active, err := store.ListActiveRunIDs(ctx, partition, 64)
		events := eventCount(t, ctx, store, partition, "run.accepted")
		t.Logf("in-flight admission reconciled on retry as %s (accepted events=%d)", retry, events)
		if !oneOf(retry, expected.RetryOneOf) || events != expected.AcceptedEvents || err != nil || len(active) != expected.ActiveRuns {
			t.Fatalf("retry=%s accepted events=%d active=%v err=%v; fixture one of %v, %d/%d", retry, events, active, err, expected.RetryOneOf, expected.AcceptedEvents, expected.ActiveRuns)
		}
	})

	signalCase := func(t *testing.T, inFlight bool) {
		ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
		store := integrationDistributedStore(t)
		hooked := &hookedClient{Client: integrationClient(t)}
		owners := newCountedWaitRuntime(t, store, ledger, 0)
		ingress := newCountedWaitRuntime(t, integrationStoreFor(t, hooked), ledger, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := owners.Check(ctx); err != nil {
			t.Fatal(err)
		}
		tenant := tenantsInPartition(owners, partition, "quorum-signal", 1)[0]
		admission, err := owners.Admit(ctx, Submission{Tenant: tenant, RequestKey: "quorum-signal", Workflow: "acceptance-wait", Input: json.RawMessage(`{"value":3}`)})
		if err != nil {
			t.Fatal(err)
		}
		owner := acquireWhenFree(t, ctx, store, partition, "quorum-signal-owner", ownerTTL)
		if _, err := owners.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
			t.Fatalf("initial process=%v, want suspended wait", err)
		}
		waitID := WaitIDFor(admission.RunID, "approval")
		payload := json.RawMessage(`{"approved":true}`)
		var restore func()
		if inFlight {
			hooked.arm("/events/signal-", 0, false, func() { restore = pauseQuorum(t, store) })
		} else {
			restore = pauseQuorum(t, store)
		}
		blocked, stop := context.WithTimeout(ctx, 4*time.Second)
		got := signalOutcome(ingress.DeliverSignal(blocked, tenant, waitID, "quorum-signal", "synthetic-principal", payload, true))
		stop()
		if restore == nil {
			t.Fatal("signal transaction was never attempted")
		}
		restore()
		wait, err := owners.GetWait(ctx, tenant, waitID)
		if err != nil {
			t.Fatal(err)
		}
		retry := signalOutcome(ingress.DeliverSignal(ctx, tenant, waitID, "quorum-signal", "synthetic-principal", payload, true))
		completed, processErr := owners.processOne(ctx, owner)
		signaled := eventCount(t, ctx, store, partition, "wait.signaled")
		t.Logf("signal outage inFlight=%v: first=%s wait after restore=%s retry=%s signaled events=%d", inFlight, got, wait.State, retry, signaled)
		if inFlight {
			expected := fixture.SignalInFlight
			if got != expected.Result || !oneOf(retry, expected.RetryOneOf) || signaled != expected.SignaledEvents || processErr != nil || completed.State != expected.FinalState || ledger.total("suffix") != expected.SuffixEffects {
				t.Fatalf("first=%s retry=%s signaled=%d completed=%+v err=%v suffix=%d; fixture %+v", got, retry, signaled, completed, processErr, ledger.total("suffix"), expected)
			}
			return
		}
		expected := fixture.SignalBeforeCommit
		if got != expected.Result || wait.State != expected.WaitAfterRestore || retry != expected.Retry || signaled != expected.SignaledEvents || processErr != nil || completed.State != expected.FinalState || ledger.total("suffix") != expected.SuffixEffects {
			t.Fatalf("first=%s wait=%s retry=%s signaled=%d completed=%+v err=%v suffix=%d; fixture %+v", got, wait.State, retry, signaled, completed, processErr, ledger.total("suffix"), expected)
		}
	}
	t.Run("signal-before-commit", func(t *testing.T) { signalCase(t, false) })
	t.Run("signal-in-flight", func(t *testing.T) { signalCase(t, true) })

	timerCase := func(t *testing.T, inFlight bool) {
		ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
		store := integrationDistributedStore(t)
		hooked := &hookedClient{Client: integrationClient(t)}
		owners := newCountedWaitRuntime(t, store, ledger, 1)
		timers := newCountedWaitRuntime(t, integrationStoreFor(t, hooked), ledger, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := owners.Check(ctx); err != nil {
			t.Fatal(err)
		}
		tenant := tenantsInPartition(owners, partition, "quorum-timer", 1)[0]
		if _, err := owners.Admit(ctx, Submission{Tenant: tenant, RequestKey: "quorum-timer", Workflow: "acceptance-wait", Input: json.RawMessage(`{"value":4}`)}); err != nil {
			t.Fatal(err)
		}
		owner := acquireWhenFree(t, ctx, store, partition, "quorum-timer-owner", ownerTTL)
		if _, err := owners.processOne(ctx, owner); !errors.Is(err, ErrNoWork) {
			t.Fatalf("initial process=%v, want suspended wait", err)
		}
		due := time.Now().UTC().Add(time.Second)
		var restore func()
		if inFlight {
			hooked.arm("/events/timer-", 0, false, func() { restore = pauseQuorum(t, store) })
		} else {
			restore = pauseQuorum(t, store)
		}
		blocked, stop := context.WithTimeout(ctx, 4*time.Second)
		fired, fireErr := timers.FireDueWaits(blocked, owner, due, 8)
		stop()
		if restore == nil {
			t.Fatal("timer transaction was never attempted")
		}
		restore()
		got := "fired"
		if fireErr != nil && len(fired) == 0 {
			got = "error"
		}
		dueAfter, err := store.ListDueTimers(ctx, partition, due, 8)
		if err != nil {
			t.Fatal(err)
		}
		retried, retryErr := timers.FireDueWaits(ctx, owner, due, 8)
		if retryErr != nil {
			t.Fatal(retryErr)
		}
		completed, processErr := owners.processOne(ctx, owner)
		timedOut := eventCount(t, ctx, store, partition, "wait.timed_out")
		t.Logf("timer outage inFlight=%v: first=%s (%v) due after restore=%d retry fired=%d timed_out events=%d", inFlight, got, fireErr, len(dueAfter), len(retried), timedOut)
		if inFlight {
			expected := fixture.TimerInFlight
			if got != expected.Result || !oneOf(len(retried), expected.RetryFiredOneOf) || timedOut != expected.TimedOutEvents || processErr != nil || completed.State != expected.FinalState || ledger.total("suffix") != expected.SuffixEffects {
				t.Fatalf("first=%s retry=%d timed_out=%d completed=%+v err=%v suffix=%d; fixture %+v", got, len(retried), timedOut, completed, processErr, ledger.total("suffix"), expected)
			}
			return
		}
		expected := fixture.TimerBeforeCommit
		if got != expected.Result || len(dueAfter) != expected.DueAfter || len(retried) != expected.RetryFired || timedOut != expected.TimedOutEvents || processErr != nil || completed.State != expected.FinalState || ledger.total("suffix") != expected.SuffixEffects {
			t.Fatalf("first=%s due=%d retry=%d timed_out=%d completed=%+v err=%v suffix=%d; fixture %+v", got, len(dueAfter), len(retried), timedOut, completed, processErr, ledger.total("suffix"), expected)
		}
	}
	t.Run("timer-before-commit", func(t *testing.T) { timerCase(t, false) })
	t.Run("timer-in-flight", func(t *testing.T) { timerCase(t, true) })

	t.Run("finish-in-flight", func(t *testing.T) {
		expected := fixture.FinishInFlight
		ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
		store := integrationDistributedStore(t)
		hooked := &hookedClient{Client: integrationClient(t)}
		limits := Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 2 * time.Second}
		runtime := newCountedEffectRuntime(t, integrationStoreFor(t, hooked), ledger, limits)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := runtime.Check(ctx); err != nil {
			t.Fatal(err)
		}
		tenant := tenantsInPartition(runtime, partition, "quorum-finish", 1)[0]
		admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "quorum-finish", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":5}`)})
		if err != nil {
			t.Fatal(err)
		}
		owner := acquireWhenFree(t, ctx, store, partition, "quorum-finish-owner", ownerTTL)
		var restore func()
		hooked.arm("/events/finish-", 0, false, func() { restore = pauseQuorum(t, store) })
		blocked, stop := context.WithTimeout(ctx, 5*time.Second)
		_, processErr := runtime.processOne(blocked, owner)
		stop()
		if restore == nil {
			t.Fatal("finish transaction was never attempted")
		}
		restore()
		got := "error"
		if processErr == nil {
			got = "acknowledged"
		}
		run, err := runtime.GetRun(ctx, tenant, admission.RunID)
		if err != nil {
			t.Fatal(err)
		}
		active, err := store.ListActiveRunIDs(ctx, partition, 64)
		if err != nil {
			t.Fatal(err)
		}
		held := len(active) == 1 && active[0] == admission.RunID
		consistent := (run.State == "running" && held) || (run.State == "completed" && len(active) == 0)
		t.Logf("finish outage: process err=%v; after restore state=%s slots held=%v", processErr, run.State, held)
		recovered, recoverErr := runtime.processOne(ctx, owner)
		if recoverErr != nil && !errors.Is(recoverErr, ErrNoWork) {
			t.Fatal(recoverErr)
		}
		final, err := runtime.GetRun(ctx, tenant, admission.RunID)
		if err != nil {
			t.Fatal(err)
		}
		active, err = store.ListActiveRunIDs(ctx, partition, 64)
		completedEvents := eventCount(t, ctx, store, partition, "run.completed")
		if got != expected.Result || consistent != expected.Consistent || final.State != expected.FinalState || completedEvents != expected.CompletedEvents || err != nil || len(active) != expected.ActiveRuns || ledger.total("effect") != expected.ExternalEffects {
			t.Fatalf("first=%s consistent=%v recovered=%+v final=%+v completed events=%d active=%v err=%v effects=%d; fixture %+v", got, consistent, recovered, final, completedEvents, active, err, ledger.total("effect"), expected)
		}
	})

	t.Run("effect-commit-in-flight", func(t *testing.T) {
		expected := fixture.EffectCommitInFlight
		ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
		store := integrationDistributedStore(t)
		hooked := &hookedClient{Client: integrationClient(t)}
		limits := Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 2 * time.Second}
		runtime := newCountedEffectRuntime(t, integrationStoreFor(t, hooked), ledger, limits)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		if err := runtime.Check(ctx); err != nil {
			t.Fatal(err)
		}
		tenant := tenantsInPartition(runtime, partition, "quorum-effect", 1)[0]
		admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "quorum-effect", Workflow: "acceptance-effect", Input: json.RawMessage(`{"value":6}`)})
		if err != nil {
			t.Fatal(err)
		}
		owner := acquireWhenFree(t, ctx, store, partition, "quorum-effect-owner", ownerTTL)
		var restore func()
		hooked.arm("/events/committed-", 0, false, func() { restore = pauseQuorum(t, store) })
		blocked, stop := context.WithTimeout(ctx, 5*time.Second)
		_, processErr := runtime.processOne(blocked, owner)
		stop()
		if restore == nil {
			t.Fatal("effect result commit was never attempted")
		}
		restore()
		got := "error"
		if processErr == nil {
			got = "acknowledged"
		}
		recovered, recoverErr := runtime.processOne(ctx, owner)
		final, err := runtime.GetRun(ctx, tenant, admission.RunID)
		if err != nil {
			t.Fatal(err)
		}
		active, err := store.ListActiveRunIDs(ctx, partition, 64)
		dispatches := eventCount(t, ctx, store, partition, "step.dispatched")
		t.Logf("effect commit outage: first err=%v; recovery=%+v err=%v; final=%s output=%s dispatches=%d effects=%d", processErr, recovered, recoverErr, final.State, final.Output, dispatches, ledger.total("effect"))
		if got != expected.Result || !oneOf(final.State, expected.FinalStateOneOf) || (final.State == "uncertain" && len(final.Output) != 0) || ledger.total("effect") != expected.ExternalEffects || dispatches != expected.EffectDispatches || err != nil || len(active) != expected.ActiveRuns {
			t.Fatalf("first=%s final=%+v effects=%d dispatches=%d active=%v err=%v; fixture %+v", got, final, ledger.total("effect"), dispatches, active, err, expected)
		}
	})
}

// TestConcurrentAdmissionWithFreeCapacityIsNeverRejected admits one request
// from each of several distinct tenants at the same instant, through two
// ingress clients, into one partition with spare capacity. Capacity is never
// exhausted, so no request may be told the partition is full.
func TestConcurrentAdmissionWithFreeCapacityIsNeverRejected(t *testing.T) {
	var fixture struct {
		FixtureVersion int    `json:"fixtureVersion"`
		Synthetic      bool   `json:"synthetic"`
		Name           string `json:"name"`
		Limits         struct {
			Partitions          int `json:"partitions"`
			PartitionAdmissions int `json:"partitionAdmissions"`
			TenantAdmissions    int `json:"tenantAdmissions"`
		} `json:"limits"`
		Tenants        int `json:"tenants"`
		IngressClients int `json:"ingressClients"`
		Rounds         int `json:"rounds"`
		Expected       struct {
			Accepted      int `json:"acceptedPerRound"`
			Full          int `json:"admissionFullPerRound"`
			Unavailable   int `json:"unavailablePerRound"`
			Active        int `json:"activeRunsPerRound"`
			DistinctSlots int `json:"distinctGlobalSlotsPerRound"`
		} `json:"expected"`
	}
	readDistributedFixture(t, "admission-contention-fixtures.json", &fixture)
	limits := Limits{Partitions: fixture.Limits.Partitions, PartitionAdmissions: fixture.Limits.PartitionAdmissions, TenantAdmissions: fixture.Limits.TenantAdmissions, OwnerTTL: 2 * time.Second}
	for round := 0; round < fixture.Rounds; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			ledger := effectLedger(filepath.Join(t.TempDir(), "ledger"))
			direct := integrationDistributedStore(t)
			ingress := make([]*Runtime, fixture.IngressClients)
			for index := range ingress {
				ingress[index] = newCountedEffectRuntime(t, integrationDistributedStore(t), ledger, limits)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := ingress[0].Check(ctx); err != nil {
				t.Fatal(err)
			}
			const partition = "p-0000"
			tenants := tenantsInPartition(ingress[0], partition, "contention-tenant", fixture.Tenants)
			start := make(chan struct{})
			outcomes := make([]string, len(tenants))
			var group sync.WaitGroup
			for index, tenant := range tenants {
				group.Add(1)
				go func(index int, tenant string) {
					defer group.Done()
					<-start
					admission, err := ingress[index%len(ingress)].Admit(ctx, Submission{Tenant: tenant, RequestKey: "contended", Workflow: "acceptance-effect", Input: json.RawMessage(fmt.Sprintf(`{"value":%d}`, index))})
					switch {
					case errors.Is(err, distributed.ErrAdmissionFull):
						outcomes[index] = "admission_full"
					case errors.Is(err, ErrUnavailable):
						outcomes[index] = "unavailable"
					case err != nil:
						outcomes[index] = "error: " + err.Error()
					case admission.Accepted:
						outcomes[index] = "accepted"
					default:
						outcomes[index] = "duplicate"
					}
				}(index, tenant)
			}
			close(start)
			group.Wait()
			counts := map[string]int{}
			for _, outcome := range outcomes {
				counts[outcome]++
			}
			active, err := direct.ListActiveRunIDs(ctx, partition, limits.PartitionAdmissions)
			if err != nil {
				t.Fatal(err)
			}
			slots := map[string]bool{}
			for _, tenant := range tenants {
				runID := admissionRunID(tenant, "contended")
				record, err := ingress[0].GetRun(ctx, tenant, runID)
				if err == nil {
					slots[record.GlobalSlot] = true
				}
			}
			t.Logf("concurrent distinct-tenant admission: outcomes=%v active=%d distinct slots=%d", counts, len(active), len(slots))
			if counts["accepted"] != fixture.Expected.Accepted || counts["admission_full"] != fixture.Expected.Full || counts["unavailable"] != fixture.Expected.Unavailable || len(active) != fixture.Expected.Active || len(slots) != fixture.Expected.DistinctSlots {
				t.Fatalf("outcomes=%v active=%d slots=%d; fixture accepted=%d full=%d unavailable=%d active=%d slots=%d", counts, len(active), len(slots), fixture.Expected.Accepted, fixture.Expected.Full, fixture.Expected.Unavailable, fixture.Expected.Active, fixture.Expected.DistinctSlots)
			}
		})
	}
}
