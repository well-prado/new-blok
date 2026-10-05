package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// latencyClient adds a fixed delay before every transaction reaches etcd: a
// model of the round trip to a networked three-voter cluster, which a
// single-node loopback etcd does not have.
type latencyClient struct {
	*clientv3.Client
	delay time.Duration
}

func (c *latencyClient) Txn(ctx context.Context) clientv3.Txn {
	return &latencyTxn{Txn: c.Client.Txn(ctx), delay: c.delay}
}

type latencyTxn struct {
	clientv3.Txn
	delay time.Duration
}

func (t *latencyTxn) If(cs ...clientv3.Cmp) clientv3.Txn   { t.Txn = t.Txn.If(cs...); return t }
func (t *latencyTxn) Then(ops ...clientv3.Op) clientv3.Txn { t.Txn = t.Txn.Then(ops...); return t }
func (t *latencyTxn) Else(ops ...clientv3.Op) clientv3.Txn { t.Txn = t.Txn.Else(ops...); return t }
func (t *latencyTxn) Commit() (*clientv3.TxnResponse, error) {
	time.Sleep(t.delay)
	return t.Txn.Commit()
}

// serialCensus is the census as it was first written: three transactions
// per partition and one per run, one after another. It is kept here only to
// measure what the batched, parallel census replaced.
func serialCensus(ctx context.Context, r *Runtime, now time.Time) (int, error) {
	runs := 0
	for index := 0; index < r.limits.Partitions; index++ {
		partition := fmt.Sprintf("p-%04d", index)
		if _, err := r.store.CurrentOwner(ctx, partition); err != nil && !errors.Is(err, distributed.ErrOwnershipLost) {
			return 0, err
		}
		if _, err := r.store.ListDueTimers(ctx, partition, now, 4096); err != nil {
			return 0, err
		}
		ids, err := r.store.ListActiveRunIDs(ctx, partition, r.limits.PartitionAdmissions)
		if err != nil {
			return 0, err
		}
		for _, id := range ids {
			if _, _, err := r.readRun(ctx, partition, id); err != nil {
				return 0, err
			}
			runs++
		}
	}
	return runs, nil
}

// TestCensusCostAtScale (BLOK_CLUSTER_CENSUS_SCALE=1) admits 4096 runs over
// 256 partitions and times the census on loopback etcd and with a modeled
// 2ms round trip, against the serial census it replaced. The batched census
// must stay under 400ms under the modeled round trip (its source budget is
// 5s); the serial one does not fit the default 1s budget.
func TestCensusCostAtScale(t *testing.T) {
	if os.Getenv("BLOK_CLUSTER_CENSUS_SCALE") != "1" {
		t.Skip("set BLOK_CLUSTER_CENSUS_SCALE=1 (with BLOK_DISTRIBUTED_ENDPOINTS) to measure the census at 256 partitions and 4096 runs")
	}
	base := integrationClient(t)
	direct := integrationStoreFor(t, base)
	slow := integrationStoreFor(t, &latencyClient{Client: base, delay: 2 * time.Millisecond})
	nodes := map[string]node.Any{}
	program := contract.InternalProgram{WorkflowID: "census-scale", Digest: "sha256:" + strings.Repeat("4", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}}
	workflows := map[string]Workflow{"census-scale": {Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input waitInput
		return input, json.Unmarshal(raw, &input)
	}}}
	limits := Limits{Partitions: 256, PartitionAdmissions: 256, TenantAdmissions: 16, OwnerTTL: 5 * time.Second}
	runtime, err := New(direct, engine.New(nodes), workflows, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	jobs := make(chan int)
	var wg sync.WaitGroup
	var failures sync.Map
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				tenant := fmt.Sprintf("scale-tenant-%d", i/16)
				if _, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: fmt.Sprintf("scale-%d", i), Workflow: "census-scale", Input: json.RawMessage(`{"value":1}`)}); err != nil {
					failures.Store(i, err)
				}
			}
		}()
	}
	for i := 0; i < 4096; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	admitted := 4096
	failures.Range(func(_, v any) bool { admitted--; return true })
	if admitted < 3900 {
		t.Fatalf("only %d of 4096 runs admitted", admitted)
	}
	measure := func(label string, census func() (int, error)) time.Duration {
		best := time.Duration(1 << 62)
		for i := 0; i < 3; i++ {
			start := time.Now()
			runs, err := census()
			elapsed := time.Since(start)
			if err != nil || runs < admitted {
				t.Fatalf("%s: %d runs %v", label, runs, err)
			}
			best = min(best, elapsed)
		}
		t.Logf("%s: best of 3 = %v (%d runs, 256 partitions)", label, best, admitted)
		return best
	}
	batched := func(store *distributed.Store) func() (int, error) {
		return func() (int, error) {
			r, _ := New(store, engine.New(nodes), workflows, limits)
			s, err := r.Census(ctx, time.Now(), MaxCensusReads)
			if err != nil {
				return 0, err
			}
			w := s.Work[0]
			return w.Pending + w.Active + w.Waiting + w.Stalled, nil
		}
	}
	serial := func(store *distributed.Store) func() (int, error) {
		return func() (int, error) {
			r, _ := New(store, engine.New(nodes), workflows, limits)
			return serialCensus(ctx, r, time.Now())
		}
	}
	measure("batched, loopback", batched(direct))
	measure("serial, loopback", serial(direct))
	fast := measure("batched, 2ms modeled round trip", batched(slow))
	old := measure("serial, 2ms modeled round trip", serial(slow))
	if fast > 400*time.Millisecond || old < 5*time.Second {
		t.Fatalf("batched %v must fit well inside its 5s budget; serial %v was the cost to replace", fast, old)
	}
}
