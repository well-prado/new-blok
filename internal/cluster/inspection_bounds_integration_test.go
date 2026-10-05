package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// countingClient counts the etcd transactions the store sends; every store
// read is one. With failAt set, that transaction and every later one fail
// like a storage outage, without any context error.
type countingClient struct {
	*clientv3.Client
	txns   atomic.Int64
	failAt atomic.Int64
}

var errSyntheticStoreOutage = errors.New("synthetic store outage")

func (c *countingClient) Txn(ctx context.Context) clientv3.Txn {
	n := c.txns.Add(1)
	if at := c.failAt.Load(); at > 0 && n >= at {
		return failingTxn{c.Client.Txn(ctx)}
	}
	return c.Client.Txn(ctx)
}

type failingTxn struct{ clientv3.Txn }

func (t failingTxn) If(compares ...clientv3.Cmp) clientv3.Txn {
	return failingTxn{t.Txn.If(compares...)}
}
func (t failingTxn) Then(ops ...clientv3.Op) clientv3.Txn { return failingTxn{t.Txn.Then(ops...)} }
func (t failingTxn) Else(ops ...clientv3.Op) clientv3.Txn { return failingTxn{t.Txn.Else(ops...)} }
func (failingTxn) Commit() (*clientv3.TxnResponse, error) { return nil, errSyntheticStoreOutage }

// completedChainRun executes, to completion, a pure workflow of steps
// chained step to step, and returns its runtime, client and run.
func completedChainRun(t *testing.T, steps int) (*Runtime, *countingClient, string, string) {
	t.Helper()
	client := &countingClient{Client: integrationClient(t)}
	store := integrationStoreFor(t, client)
	pass := node.MustDefine("fixture/inspect-pass", "1.0.0", func(_ context.Context, input integrationOutput) (integrationOutput, error) {
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("pure chained step"), node.Schemas([]byte(outputSchema), []byte(outputSchema))).Any()
	instructions := make([]contract.InternalInstruction, 0, steps+1)
	for index := 0; index < steps; index++ {
		instruction := contract.InternalInstruction{Index: index, ID: fmt.Sprintf("s%04d", index), Kind: "call", Node: "fixture/inspect-pass"}
		if index > 0 {
			instruction.References = []contract.Reference{{Step: fmt.Sprintf("s%04d", index-1)}}
		}
		instructions = append(instructions, instruction)
	}
	instructions = append(instructions, contract.InternalInstruction{Index: steps, ID: "output", Kind: "output", References: []contract.Reference{{Step: fmt.Sprintf("s%04d", steps-1)}}})
	program := contract.InternalProgram{WorkflowID: "chain", Digest: "sha256:" + strings.Repeat("c", 64), Instructions: instructions}
	runtime, err := New(store, engine.New(map[string]node.Any{"fixture/inspect-pass": pass}), map[string]Workflow{"chain": {Program: program, DecodeInput: decodeTyped[integrationOutput]}}, Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "inspect-chain")
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "chain", Workflow: "chain", Input: json.RawMessage(`{"value":0}`)})
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireWhenFree(t, ctx, store, partition, "inspect-chain-owner", 30*time.Second)
	run, err := runtime.processOne(ctx, owner)
	if err != nil || run.State != "completed" || string(run.Output) != fmt.Sprintf(`{"value":%d}`, steps) {
		t.Fatalf("chain run=%+v err=%v", run, err)
	}
	return runtime, client, tenant, admission.RunID
}

// TestTimedOutReplayIsAFailedReadNotAShortPage is review finding F1 on #278:
// a replay cut short by its deadline (or any store error) used to return a
// short page as a successful read, with no next cursor and no truncation
// mark, so a poll could send it as the final reconstruction of a terminal
// run. Every read of a completed run is now either a full first page or an
// error, and a deadline is never mistaken for a refusal.
func TestTimedOutReplayIsAFailedReadNotAShortPage(t *testing.T) {
	runtime, client, tenant, runID := completedChainRun(t, 40)
	source := runtime.InspectionSource(nil)
	full, steps, total, err := source.ReadInspection(context.Background(), tenant, runID, "", 0, 20, nil, 1024)
	if err != nil || full.Status != inspection.StatusCompleted || len(steps) != 20 || total != 21 {
		t.Fatalf("unbounded read: run=%+v steps=%d total=%d err=%v", full, len(steps), total, err)
	}
	var failed, complete int
	for deadline := time.Millisecond; deadline <= 60*time.Millisecond; deadline += time.Millisecond {
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		_, steps, total, err := source.ReadInspection(ctx, tenant, runID, "", 0, 20, nil, 1024)
		cancel()
		switch {
		case err != nil && source.Refused(err):
			t.Fatalf("deadline %s: a failed read was classified as a refusal: %v", deadline, err)
		case err != nil:
			failed++
		case len(steps) == 20 && total == 21:
			complete++
		default:
			t.Fatalf("deadline %s: a short page (%d of 20 steps, total %d) was returned as a successful read", deadline, len(steps), total)
		}
	}
	if failed == 0 {
		t.Fatal("no read hit its deadline; the check would be vacuous")
	}
	t.Logf("%d reads failed at their deadline, %d were complete; none was short", failed, complete)
	// A store failure without any deadline, at each read the page needs:
	// the run record, then each of the 21 step records.
	for at := int64(1); at <= 22; at++ {
		client.txns.Store(0)
		client.failAt.Store(at)
		_, steps, total, err := source.ReadInspection(context.Background(), tenant, runID, "", 0, 20, nil, 1024)
		client.failAt.Store(0)
		if !errors.Is(err, errSyntheticStoreOutage) || source.Refused(err) {
			t.Fatalf("store failure at read %d: %d steps total=%d err=%v", at, len(steps), total, err)
		}
	}
}

// TestInspectionDepthIsBoundedPerRead is review finding F2 on #278: a deep
// page used to replay every step before it (about 1000 linearizable reads
// for offset 980). A page must now start within MaxInspectionSteps, so a
// read costs at most MaxInspectionSteps+limit+1 step reads; paging by
// cursor reaches the bound, the page there is marked truncated and offers no
// cursor that would be refused, and a page beyond it is refused before any
// read. A run whose registered artifact no longer matches says so instead of
// looking like a run without steps.
func TestInspectionDepthIsBoundedPerRead(t *testing.T) {
	const steps, limit = 300, 20
	runtime, client, tenant, runID := completedChainRun(t, steps)
	source := runtime.InspectionSource(nil)
	ctx := context.Background()
	policy := inspection.Policy{MaxPageSize: limit}
	var seen []string
	var pages int
	for cursor := ""; ; pages++ {
		page, err := inspect.InspectSource(ctx, source, tenant, policy, inspection.Query{Version: inspection.Version, RunID: runID, Limit: limit, Cursor: cursor})
		if err != nil {
			t.Fatalf("page %d: %v", pages, err)
		}
		for _, step := range page.Steps {
			seen = append(seen, step.ID)
		}
		last := page.Next == ""
		if page.Truncated != last {
			t.Fatalf("page %d truncated=%t next=%q", pages, page.Truncated, page.Next)
		}
		if last {
			break
		}
		cursor = page.Next
	}
	want := make([]string, 0, MaxInspectionSteps+limit)
	for index := 0; index < MaxInspectionSteps-MaxInspectionSteps%limit+limit; index++ {
		want = append(want, fmt.Sprintf("s%04d", index))
	}
	if !slices.Equal(seen, want) {
		t.Fatalf("paged %d steps over %d pages, want s0000..%s", len(seen), pages, want[len(want)-1])
	}
	offset := MaxInspectionSteps - MaxInspectionSteps%limit
	client.txns.Store(0)
	started := time.Now()
	if _, deep, total, err := source.ReadInspection(ctx, tenant, runID, "", offset, limit, nil, 1024); err != nil || len(deep) != limit+1 || total != offset+limit {
		t.Fatalf("deepest page: %d steps total=%d err=%v", len(deep), total, err)
	}
	reads := client.txns.Load()
	if reads > int64(1+offset+limit+1) {
		t.Fatalf("deepest page cost %d store reads; bound is %d", reads, 1+offset+limit+1)
	}
	t.Logf("deepest page (offset %d) cost %d store reads in %s", offset, reads, time.Since(started))
	client.txns.Store(0)
	if _, _, _, err := source.ReadInspection(ctx, tenant, runID, "", MaxInspectionSteps, limit, nil, 1024); !errors.Is(err, ErrInspectionDepth) || source.Refused(err) {
		t.Fatalf("page beyond the bound: %v", err)
	}
	if reads := client.txns.Load(); reads != 0 {
		t.Fatalf("a refused deep page still read the store %d times", reads)
	}

	// A step filter is bounded the same way (review F2b): every step the
	// replay reads counts, matching or not. A step within the bound is
	// found; one beyond it, or one the bound cannot rule out, is refused.
	for _, filter := range []struct {
		step  string
		found bool
	}{{"s0005", true}, {fmt.Sprintf("s%04d", MaxInspectionSteps-1), true}, {fmt.Sprintf("s%04d", steps-1), false}, {"missing", false}} {
		client.txns.Store(0)
		_, matched, total, err := source.ReadInspection(ctx, tenant, runID, filter.step, 0, limit, nil, 1024)
		reads := client.txns.Load()
		switch {
		case filter.found && (err != nil || len(matched) != 1 || matched[0].ID != filter.step || total != 1):
			t.Fatalf("filter %s: %d steps total=%d err=%v", filter.step, len(matched), total, err)
		case !filter.found && !errors.Is(err, ErrInspectionDepth):
			t.Fatalf("filter %s beyond the bound: %d steps err=%v", filter.step, len(matched), err)
		}
		if reads > int64(1+MaxInspectionSteps+1) {
			t.Fatalf("filter %s cost %d store reads; bound is %d", filter.step, reads, 1+MaxInspectionSteps+1)
		}
	}

	// The run's input no longer reproduces it: a decoder that refuses the
	// stored input, and one that decodes it to a different value.
	for name, decode := range map[string]func(json.RawMessage) (any, error){
		"refused":   func(json.RawMessage) (any, error) { return nil, errors.New("input not retained") },
		"different": func(json.RawMessage) (any, error) { return integrationOutput{Value: -1}, nil },
	} {
		changed := runtime.workflows["chain"]
		changed.DecodeInput = decode
		lost, err := New(runtime.store, runtime.engine, map[string]Workflow{"chain": changed}, runtime.limits)
		if err != nil {
			t.Fatal(err)
		}
		run, none, _, notes, err := lost.InspectionSource(nil).ReadInspectionUnavailable(ctx, tenant, runID, "", 0, limit, nil, 1024)
		if err != nil || run.Status != inspection.StatusCompleted || len(none) != 0 || !slices.Contains(notes, unavailableInput) {
			t.Fatalf("input %s: run=%+v steps=%d notes=%v err=%v", name, run, len(none), notes, err)
		}
	}

	// The registered artifact no longer matches the run.
	other := runtime.workflows["chain"]
	other.Program.Digest = "sha256:" + strings.Repeat("d", 64)
	mismatched, err := New(runtime.store, runtime.engine, map[string]Workflow{"chain": other}, runtime.limits)
	if err != nil {
		t.Fatal(err)
	}
	run, none, _, notes, err := mismatched.InspectionSource(nil).ReadInspectionUnavailable(ctx, tenant, runID, "", 0, limit, nil, 1024)
	if err != nil || run.Status != inspection.StatusCompleted || len(none) != 0 || !slices.Contains(notes, unavailableArtifact) {
		t.Fatalf("artifact mismatch: run=%+v steps=%d notes=%v err=%v", run, len(none), notes, err)
	}
	if _, _, _, notes, err := source.ReadInspectionUnavailable(ctx, tenant, runID, "", 0, limit, nil, 1024); err != nil || slices.Contains(notes, unavailableArtifact) {
		t.Fatalf("matching artifact notes=%v err=%v", notes, err)
	}
}
