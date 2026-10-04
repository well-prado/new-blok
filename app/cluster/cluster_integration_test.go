package clusterapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/cluster"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type distributedHTTPInput struct {
	Value int `json:"value"`
}

type distributedHTTPOutput struct {
	Value int `json:"value"`
}

func TestDistributedHTTPAdmissionAndLifecycleWorker(t *testing.T) {
	endpoints := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ENDPOINTS"), ",")
	if endpoints[0] == "" {
		t.Skip("set BLOK_DISTRIBUTED_ENDPOINTS to run the app composition test against real etcd")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	incarnation, err := client.Get(ctx, "/blok/v1/cluster-incarnation")
	if err != nil || len(incarnation.Kvs) != 1 {
		t.Fatalf("read existing cluster incarnation: err=%v", err)
	}
	store, err := distributed.New(ctx, client, string(incarnation.Kvs[0].Value))
	if err != nil {
		t.Fatal(err)
	}
	var effects atomic.Int64
	definition := node.MustDefine("fixture/http-effect", "1.0.0", func(_ context.Context, input distributedHTTPInput) (distributedHTTPOutput, error) {
		effects.Add(1)
		return distributedHTTPOutput{Value: input.Value + 1}, nil
	}, node.Description("synthetic app composition effect"), node.Schemas(
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
	), node.Effects("fixture:http-counter")).Any()
	program := contract.InternalProgram{WorkflowID: "http-composition-fixture", Digest: "sha256:" + strings.Repeat("e", 64), Instructions: []contract.InternalInstruction{{Index: 0, ID: "effect", Kind: "call", Node: "fixture/http-effect"}}}
	workflow := cluster.Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input distributedHTTPInput
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	limits := cluster.Limits{Partitions: 8, PartitionAdmissions: 64, TenantAdmissions: 8, OwnerTTL: 5 * time.Second}
	runtime, err := cluster.New(store, engine.New(map[string]node.Any{"fixture/http-effect": definition}), map[string]cluster.Workflow{"http-composition-fixture": workflow}, limits)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	partition := emptyDistributedPartition(t, ctx, store, limits.Partitions)
	tenant := tenantForPartition(runtime, partition)
	worker := DistributedWorkerDependency(runtime, fmt.Sprintf("app-integration-%d", time.Now().UnixNano()))
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := worker.Close(closeCtx); err != nil {
			t.Errorf("stop distributed worker dependency: %v", err)
		}
	})
	handler := NewDistributedWorkflowHandler(runtime, "http-composition-fixture", func(*http.Request) (string, error) { return tenant, nil })
	request := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		httpRequest := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(`{"value":41}`))
		httpRequest.Header.Set("Idempotency-Key", "one-request")
		handler.ServeHTTP(recorder, httpRequest)
		return recorder
	}
	first := request()
	if first.Code != http.StatusAccepted {
		t.Fatalf("first ingress status=%d body=%s, want 202 after durable commit", first.Code, first.Body.String())
	}
	var accepted cluster.Admission
	if err := json.Unmarshal(first.Body.Bytes(), &accepted); err != nil || !accepted.Accepted || accepted.RunID == "" {
		t.Fatalf("first durable admission=%+v err=%v", accepted, err)
	}
	duplicate := request()
	var duplicateAdmission cluster.Admission
	if duplicate.Code != http.StatusAccepted || json.Unmarshal(duplicate.Body.Bytes(), &duplicateAdmission) != nil || duplicateAdmission.Accepted || duplicateAdmission.RunID != accepted.RunID {
		t.Fatalf("duplicate ingress status=%d admission=%+v body=%s", duplicate.Code, duplicateAdmission, duplicate.Body.String())
	}
	for ctx.Err() == nil {
		run, err := runtime.GetRun(ctx, tenant, accepted.RunID)
		if err == nil && (run.State == "completed" || run.State == "failed" || run.State == "uncertain") {
			if run.State != "completed" || effects.Load() != 1 {
				t.Fatalf("composed worker run=%+v effects=%d, want completed/1", run, effects.Load())
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("lifecycle-managed worker did not complete durable HTTP admission")
}

func emptyDistributedPartition(t *testing.T, ctx context.Context, store *distributed.Store, partitions int) string {
	t.Helper()
	for index := 0; index < partitions; index++ {
		partition := fmt.Sprintf("p-%04d", index)
		if _, err := store.CurrentOwner(ctx, partition); err == nil {
			continue
		} else if err != distributed.ErrOwnershipLost {
			t.Fatalf("inspect owner for %s: %v", partition, err)
		}
		events, err := store.ListEvents(ctx, partition)
		if err != nil {
			t.Fatal(err)
		}
		pending := false
		for _, event := range events {
			if event.Kind != "run.accepted" || !strings.HasPrefix(event.ID, "accepted-run-") {
				continue
			}
			runID := strings.TrimPrefix(event.ID, "accepted-")
			data, _, err := store.ReadState(ctx, partition, runID)
			if err != nil {
				t.Fatal(err)
			}
			var state struct {
				State string `json:"state"`
			}
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatal(err)
			}
			if state.State == "accepted" || state.State == "running" {
				pending = true
				break
			}
		}
		if !pending {
			return partition
		}
	}
	t.Skip("no empty partition is available for isolated app composition fixture")
	return ""
}

func tenantForPartition(runtime *cluster.Runtime, partition string) string {
	base := fmt.Sprintf("app-tenant-%d", time.Now().UnixNano())
	for index := 0; ; index++ {
		tenant := fmt.Sprintf("%s-%d", base, index)
		if runtime.Partition(tenant) == partition {
			return tenant
		}
	}
}
