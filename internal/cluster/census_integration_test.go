package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/tooling/promrule"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/slo"
	"github.com/well-prado/new-blok/store/distributed"
)

var monitoringFixtures = filepath.Join("..", "..", "examples", "monitoring", "testdata")

func censusExposition(t *testing.T, ctx context.Context, runtime *Runtime, now time.Time) (slo.Snapshot, string) {
	t.Helper()
	snapshot, err := runtime.Census(ctx, now, 256)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := slo.WriteText(&b, snapshot); err != nil {
		t.Fatal(err)
	}
	return snapshot, b.String()
}

func checkCensusScenario(t *testing.T, id, before, after string) {
	t.Helper()
	scenarios, err := promrule.LoadScenarios(filepath.Join(monitoringFixtures, "scenarios.json"))
	if err != nil {
		t.Fatal(err)
	}
	scenario, ok := scenarios.Find(id)
	if !ok {
		t.Fatalf("scenario %s is not declared", id)
	}
	parse := func(text string) []promrule.ExpositionSample {
		samples, err := promrule.ParseText(strings.NewReader(text))
		if err != nil {
			t.Fatal(err)
		}
		return samples
	}
	if err := scenario.Check(parse(before), parse(after)); err != nil {
		t.Fatalf("predeclared signals not observed:\n%v\n%s", err, after)
	}
	if os.Getenv("BLOK_RECORD_FIXTURES") == "1" {
		path := filepath.Join(monitoringFixtures, "recorded", id+".prom")
		if err := promrule.WriteRecording(path, promrule.Recording{Scenario: id, Source: "internal/cluster Runtime.Census over real etcd v3.6.5, slo.WriteText", Before: before, After: after}); err != nil {
			t.Fatal(err)
		}
	}
}

func waitOwnerGone(t *testing.T, ctx context.Context, store *distributed.Store, partition string) {
	t.Helper()
	for {
		_, err := store.CurrentOwner(ctx, partition)
		if errors.Is(err, distributed.ErrOwnershipLost) {
			return
		}
		if err != nil {
			t.Fatal(err)
		}
		if waitErr := waitContext(ctx, 100*time.Millisecond); waitErr != nil {
			t.Fatalf("lease of %s never expired: %v", partition, waitErr)
		}
	}
}

// TestCensusOwnerDeathWaitingAndTimerLag runs the cluster census against a
// real etcd while partition owners die by lease expiry (they stop renewing;
// nothing releases their fence):
//
//  1. a run whose owner died mid-step is stalled, while a run suspended on a
//     signal in another partition is waiting;
//  2. once a successor finishes the stalled run, the waiting run whose own
//     partition owner died is still waiting, not stalled;
//  3. a timer due in a partition without an owner shows its lag, read 90s
//     after it was due.
func TestCensusOwnerDeathWaitingAndTimerLag(t *testing.T) {
	store := integrationDistributedStore(t)
	release := make(chan struct{})
	blocked := make(chan struct{}, 1)
	hold := node.MustDefine("fixture/census-hold", "1.0.0", func(ctx context.Context, input waitInput) (integrationOutput, error) {
		select {
		case blocked <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return integrationOutput{Value: input.Value}, nil
	}, node.Description("held pure step"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema)), node.Pure()).Any()
	program := func(name, digit string, instructions ...contract.InternalInstruction) Workflow {
		return Workflow{Program: contract.InternalProgram{WorkflowID: name, Digest: "sha256:" + strings.Repeat(digit, 64), Instructions: instructions}, DecodeInput: func(raw json.RawMessage) (any, error) {
			var input waitInput
			err := json.Unmarshal(raw, &input)
			return input, err
		}}
	}
	runtime, err := New(store, engine.New(map[string]node.Any{"fixture/census-hold": hold}), map[string]Workflow{
		"census-hold":   program("census-hold", "1", contract.InternalInstruction{Index: 0, ID: "hold", Kind: "call", Node: "fixture/census-hold"}, contract.InternalInstruction{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "hold"}}}),
		"census-signal": program("census-signal", "2", contract.InternalInstruction{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}}, contract.InternalInstruction{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}}),
		"census-timer":  program("census-timer", "3", contract.InternalInstruction{Index: 0, ID: "delay", Kind: "wait", Wait: &contract.WaitInstruction{Name: "delay", TimeoutMillis: 1}}, contract.InternalInstruction{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "delay"}}}),
	}, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// Three tenants in three distinct partitions.
	tenants, partitions := []string{}, []string{}
	for i := 0; len(tenants) < 3; i++ {
		tenant := fmt.Sprintf("census-tenant-%d", i)
		partition := runtime.Partition(tenant)
		if !containsString(partitions, partition) {
			tenants, partitions = append(tenants, tenant), append(partitions, partition)
		}
	}
	stallTenant, waitTenant, timerTenant := tenants[0], tenants[1], tenants[2]
	stallPartition, waitPartition, timerPartition := partitions[0], partitions[1], partitions[2]
	// A healthy replica owns every other partition for the whole test; the
	// three under test get owners that die by never renewing their 2s lease.
	for index := 0; index < 8; index++ {
		partition := fmt.Sprintf("p-%04d", index)
		if !containsString(partitions, partition) {
			acquireWhenFree(t, ctx, store, partition, "replica-healthy", 60*time.Second)
		}
	}
	stallOwner := acquireWhenFree(t, ctx, store, stallPartition, "replica-dies", 2*time.Second)
	waitOwner := acquireWhenFree(t, ctx, store, waitPartition, "replica-dies", 2*time.Second)
	timerOwner := acquireWhenFree(t, ctx, store, timerPartition, "replica-timer", 60*time.Second)

	admit := func(tenant, workflow string) Admission {
		admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: workflow, Workflow: workflow, Input: json.RawMessage(`{"value":1}`)})
		if err != nil {
			t.Fatal(err)
		}
		return admission
	}
	stalled := admit(stallTenant, "census-hold")
	admit(waitTenant, "census-signal")
	if _, err := runtime.processOne(ctx, waitOwner); !errors.Is(err, ErrNoWork) {
		t.Fatalf("signal run did not suspend: %v", err)
	}
	finished := make(chan error, 1)
	go func() {
		_, err := runtime.processOne(ctx, stallOwner)
		finished <- err
	}()
	<-blocked
	snapshot, before := censusExposition(t, ctx, runtime, time.Now().UTC())
	if w := snapshot.Work[0]; w.Active != 1 || w.Waiting != 1 || w.Stalled != 0 || snapshot.Partitions.Owned != 8 {
		t.Fatalf("live owners: %+v %+v", w, *snapshot.Partitions)
	}

	// Phase 1: both 2s leases expire; nothing released them.
	waitOwnerGone(t, ctx, store, stallPartition)
	waitOwnerGone(t, ctx, store, waitPartition)
	snapshot, after := censusExposition(t, ctx, runtime, time.Now().UTC())
	if w := snapshot.Work[0]; w.Stalled != 1 || w.Waiting != 1 || w.Active != 0 {
		t.Fatalf("after owner death: %+v", w)
	}
	checkCensusScenario(t, "cluster-owner-death", before, after)

	// Phase 2: a successor takes the stalled run's partition and finishes it
	// (its step is pure, so it is re-dispatched). The waiting run's partition
	// still has no owner, and the waiting run is still only waiting.
	close(release)
	if err := <-finished; err == nil {
		t.Fatal("the dead owner's result was accepted after its lease expired")
	}
	successor := acquireWhenFree(t, ctx, store, stallPartition, "replica-successor", 60*time.Second)
	if run, err := runtime.processOne(ctx, successor); err != nil || run.State != "completed" || run.RunID != stalled.RunID {
		t.Fatalf("successor: %+v %v", run, err)
	}
	// Every phase is replayed after the same healthy baseline: all
	// partitions owned, nothing stalled.
	snapshot, after = censusExposition(t, ctx, runtime, time.Now().UTC())
	if w := snapshot.Work[0]; w.Stalled != 0 || w.Waiting != 1 {
		t.Fatalf("waiting run with a dead partition owner: %+v", w)
	}
	checkCensusScenario(t, "cluster-waiting-owner-died", before, after)

	// Phase 3: a run suspends on a 1ms timer, then its owner leaves; read
	// 90s later, the census reports the overdue timer and its lag.
	admit(timerTenant, "census-timer")
	if _, err := runtime.processOne(ctx, timerOwner); !errors.Is(err, ErrNoWork) {
		t.Fatalf("timer run did not suspend: %v", err)
	}
	releaseCtx, cancelRelease := context.WithTimeout(ctx, 2*time.Second)
	if err := store.Release(releaseCtx, timerOwner); err != nil {
		t.Fatal(err)
	}
	cancelRelease()
	snapshot, after = censusExposition(t, ctx, runtime, time.Now().UTC().Add(90*time.Second))
	if timers := snapshot.Timers[0]; timers.Overdue != 1 || timers.Lag < 89*time.Second {
		t.Fatalf("timer census: %+v", timers)
	}
	checkCensusScenario(t, "timer-lag", before, after)

	// The census refuses an unbounded read and reports its own truncation.
	if _, err := runtime.Census(ctx, time.Now(), MaxCensusReads+1); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unbounded census: %v", err)
	}
	if truncated, err := runtime.Census(ctx, time.Now(), 1); err != nil || !truncated.Work[0].Truncated || truncated.Work[0].Waiting+truncated.Work[0].Stalled > 1 {
		t.Fatalf("bounded census: %+v %v", truncated, err)
	}
}

func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}
