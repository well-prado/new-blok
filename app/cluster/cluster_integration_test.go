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
	"github.com/well-prado/new-blok/internal/clustertest"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"
)

type distributedHTTPInput struct {
	Value int `json:"value"`
}

type distributedHTTPOutput struct {
	Value int `json:"value"`
}

func TestDistributedHTTPAdmissionAndLifecycleWorker(t *testing.T) {
	// Wait for the cluster lock before this test's deadlines start.
	clustertest.Endpoints(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	store := namespacedDistributedStore(t)
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
	tenant := tenantForPartition(runtime, "p-0000")
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

func tenantForPartition(runtime *cluster.Runtime, partition string) string {
	base := fmt.Sprintf("app-tenant-%d", time.Now().UnixNano())
	for index := 0; ; index++ {
		tenant := fmt.Sprintf("%s-%d", base, index)
		if runtime.Partition(tenant) == partition {
			return tenant
		}
	}
}

// namespacedDistributedStore opens a real etcd client whose keys live under a
// private prefix, so the test starts from empty partitions and its own
// immutable runtime settings on the shared real cluster.
func namespacedDistributedStore(t *testing.T) *distributed.Store {
	t.Helper()
	endpoints := clustertest.Endpoints(t)
	client, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 2 * time.Second, DialKeepAliveTime: time.Second, DialKeepAliveTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	prefix := fmt.Sprintf("/blok-it/app-%d-%d/", os.Getpid(), time.Now().UnixNano())
	client.KV = namespace.NewKV(client.KV, prefix)
	client.Lease = namespace.NewLease(client.Lease, prefix)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	store, err := distributed.New(ctx, client, "integration-incarnation-v1")
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestDistributedHTTPStatusMappingForCapacityConflictAndOutage(t *testing.T) {
	// This test pauses voters: it runs alone on the shared cluster.
	clustertest.Disrupt(t)
	var fixture struct {
		Limits struct {
			Partitions          int `json:"partitions"`
			PartitionAdmissions int `json:"partitionAdmissions"`
			TenantAdmissions    int `json:"tenantAdmissions"`
		} `json:"limits"`
		Expected struct {
			HTTPAccepted         int    `json:"httpAcceptedStatus"`
			HTTPConflict         int    `json:"httpConflictStatus"`
			HTTPOverCap          int    `json:"httpOverTenantCapStatus"`
			HTTPRetryAfter       string `json:"httpRetryAfter"`
			HTTPQuorumLoss       int    `json:"httpQuorumLossStatus"`
			HTTPSignalQuorumLoss int    `json:"httpSignalQuorumLossStatus"`
			HTTPRecovered        string `json:"httpRecoveredAdmission"`
		} `json:"expected"`
	}
	data, err := os.ReadFile("../../testdata/distributed/admission-capacity-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	store := namespacedDistributedStore(t)
	definition := node.MustDefine("fixture/http-status-effect", "1.0.0", func(_ context.Context, input distributedHTTPInput) (distributedHTTPOutput, error) {
		return distributedHTTPOutput{Value: input.Value + 1}, nil
	}, node.Description("synthetic effect; no worker runs in this test"), node.Schemas(
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
		[]byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`),
	), node.Effects("fixture:http-status")).Any()
	program := contract.InternalProgram{WorkflowID: "http-status-fixture", Digest: "sha256:" + strings.Repeat("4", 64), Instructions: []contract.InternalInstruction{{Index: 0, ID: "effect", Kind: "call", Node: "fixture/http-status-effect"}}}
	workflow := cluster.Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input distributedHTTPInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	limits := cluster.Limits{Partitions: fixture.Limits.Partitions, PartitionAdmissions: fixture.Limits.PartitionAdmissions, TenantAdmissions: fixture.Limits.TenantAdmissions, OwnerTTL: 2 * time.Second}
	runtime, err := cluster.New(store, engine.New(map[string]node.Any{"fixture/http-status-effect": definition}), map[string]cluster.Workflow{"http-status-fixture": workflow}, limits)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	handler := NewDistributedWorkflowHandler(runtime, "http-status-fixture", func(request *http.Request) (string, error) { return request.Header.Get("X-Test-Tenant"), nil })
	signals := NewDistributedSignalHandler(runtime,
		func(request *http.Request) (string, error) { return request.Header.Get("X-Test-Tenant"), nil },
		func(*http.Request) (string, error) { return "synthetic-principal", nil },
		func(*http.Request, string, string) bool { return true })
	post := func(requestCtx context.Context, tenant, key, body string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body)).WithContext(requestCtx)
		request.Header.Set("Idempotency-Key", key)
		request.Header.Set("X-Test-Tenant", tenant)
		handler.ServeHTTP(recorder, request)
		return recorder
	}
	first := tenantForPartition(runtime, "p-0000")
	second := tenantForPartition(runtime, "p-0000")
	for second == first {
		second = tenantForPartition(runtime, "p-0000")
	}
	if response := post(ctx, first, "k1", `{"value":1}`); response.Code != fixture.Expected.HTTPAccepted {
		t.Fatalf("first admission status=%d body=%s", response.Code, response.Body.String())
	}
	if response := post(ctx, first, "k1", `{"value":2}`); response.Code != fixture.Expected.HTTPConflict {
		t.Fatalf("same key different body status=%d body=%s, fixture %d", response.Code, response.Body.String(), fixture.Expected.HTTPConflict)
	}
	for index := 2; index <= fixture.Limits.TenantAdmissions; index++ {
		if response := post(ctx, first, fmt.Sprintf("k%d", index), `{"value":1}`); response.Code != fixture.Expected.HTTPAccepted {
			t.Fatalf("admission %d status=%d body=%s", index, response.Code, response.Body.String())
		}
	}
	overCap := post(ctx, first, "over-cap", `{"value":1}`)
	if overCap.Code != fixture.Expected.HTTPOverCap || overCap.Header().Get("Retry-After") != fixture.Expected.HTTPRetryAfter {
		t.Fatalf("over tenant cap status=%d Retry-After=%q, fixture %d %q", overCap.Code, overCap.Header().Get("Retry-After"), fixture.Expected.HTTPOverCap, fixture.Expected.HTTPRetryAfter)
	}
	if response := post(ctx, second, "other-1", `{"value":1}`); response.Code != fixture.Expected.HTTPAccepted {
		t.Fatalf("second tenant was blocked by the first tenant's backlog: status=%d body=%s", response.Code, response.Body.String())
	}
	outageTenant := tenantForPartition(runtime, "p-0001")
	restore := clustertest.PauseQuorum(t)
	outageCtx, stop := context.WithTimeout(ctx, 3*time.Second)
	outage := post(outageCtx, outageTenant, "during-outage", `{"value":1}`)
	stop()
	signalCtx, stopSignal := context.WithTimeout(ctx, 3*time.Second)
	signalRecorder := httptest.NewRecorder()
	signalRequest := httptest.NewRequest(http.MethodPost, "/signals", strings.NewReader(`{"waitId":"wait-absent","signalId":"s1","payload":{}}`)).WithContext(signalCtx)
	signalRequest.Header.Set("X-Test-Tenant", outageTenant)
	signals.ServeHTTP(signalRecorder, signalRequest)
	stopSignal()
	restore()
	if outage.Code != fixture.Expected.HTTPQuorumLoss || outage.Header().Get("Retry-After") != fixture.Expected.HTTPRetryAfter {
		t.Fatalf("admission during quorum loss status=%d Retry-After=%q body=%s, fixture %d", outage.Code, outage.Header().Get("Retry-After"), outage.Body.String(), fixture.Expected.HTTPQuorumLoss)
	}
	if signalRecorder.Code != fixture.Expected.HTTPSignalQuorumLoss || signalRecorder.Header().Get("Retry-After") != fixture.Expected.HTTPRetryAfter {
		t.Fatalf("signal during quorum loss status=%d Retry-After=%q body=%s, fixture %d", signalRecorder.Code, signalRecorder.Header().Get("Retry-After"), signalRecorder.Body.String(), fixture.Expected.HTTPSignalQuorumLoss)
	}
	var recovered *httptest.ResponseRecorder
	for attempt := 0; attempt < 100; attempt++ {
		recovered = post(ctx, outageTenant, "during-outage", `{"value":1}`)
		if recovered.Code != http.StatusServiceUnavailable {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	var admission cluster.Admission
	if recovered.Code != fixture.Expected.HTTPAccepted || json.Unmarshal(recovered.Body.Bytes(), &admission) != nil || !admission.Accepted || fixture.Expected.HTTPRecovered != "accepted" {
		t.Fatalf("retry of the unacknowledged key after recovery status=%d body=%s, fixture %s", recovered.Code, recovered.Body.String(), fixture.Expected.HTTPRecovered)
	}
}

// TestDistributedLateSignalDuringOutageIsRetryable delivers a signal through
// the HTTP handler to a wait that already timed out, while etcd has lost
// quorum. The late-signal record cannot be written, so the response must be
// a retryable 503, and the same signal after recovery is recorded as late.
func TestDistributedLateSignalDuringOutageIsRetryable(t *testing.T) {
	// This test pauses voters: it runs alone on the shared cluster.
	clustertest.Disrupt(t)
	store := namespacedDistributedStore(t)
	program := contract.InternalProgram{WorkflowID: "late-signal-fixture", Digest: "sha256:" + strings.Repeat("5", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval", TimeoutMillis: 1}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}}
	workflow := cluster.Workflow{Program: program, DecodeInput: func(raw json.RawMessage) (any, error) {
		var input distributedHTTPInput
		if err := json.Unmarshal(raw, &input); err != nil {
			return nil, err
		}
		return input, nil
	}}
	runtime, err := cluster.New(store, engine.New(map[string]node.Any{}), map[string]cluster.Workflow{"late-signal-fixture": workflow}, cluster.Limits{Partitions: 8, PartitionAdmissions: 8, TenantAdmissions: 2, OwnerTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tenant := tenantForPartition(runtime, "p-0000")
	worker := DistributedWorkerDependency(runtime, "late-signal-worker")
	if err := worker.Start(ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stopWorker := func() {
		if stopped {
			return
		}
		stopped = true
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := worker.Close(closeCtx); err != nil {
			t.Errorf("stop distributed worker dependency: %v", err)
		}
	}
	t.Cleanup(stopWorker)
	admission, err := runtime.Admit(ctx, cluster.Submission{Tenant: tenant, RequestKey: "late", Workflow: "late-signal-fixture", Input: json.RawMessage(`{"value":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	waitID := cluster.WaitIDFor(admission.RunID, "approval", "")
	for ctx.Err() == nil {
		if wait, err := runtime.GetWait(ctx, tenant, waitID); err == nil && wait.State == "timed_out" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	// The worker keeps running: a signal is routed through the current
	// partition owner, which it re-acquires after the outage.
	signals := NewDistributedSignalHandler(runtime,
		func(*http.Request) (string, error) { return tenant, nil },
		func(*http.Request) (string, error) { return "synthetic-principal", nil },
		func(*http.Request, string, string) bool { return true })
	send := func(requestCtx context.Context) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		body := fmt.Sprintf(`{"waitId":%q,"signalId":"late-1","payload":{"approved":true}}`, waitID)
		signals.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/signals", strings.NewReader(body)).WithContext(requestCtx))
		return recorder
	}
	restore := clustertest.PauseQuorum(t)
	outageCtx, stopOutage := context.WithTimeout(ctx, 3*time.Second)
	during := send(outageCtx)
	stopOutage()
	restore()
	if during.Code != http.StatusServiceUnavailable || during.Header().Get("Retry-After") != "1" {
		t.Fatalf("late signal during quorum loss status=%d Retry-After=%q body=%s, want 503 with Retry-After", during.Code, during.Header().Get("Retry-After"), during.Body.String())
	}
	var after *httptest.ResponseRecorder
	for attempt := 0; attempt < 300; attempt++ {
		after = send(ctx)
		if after.Code != http.StatusServiceUnavailable {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	var result cluster.SignalResult
	if after.Code != http.StatusAccepted || json.Unmarshal(after.Body.Bytes(), &result) != nil || !result.Late {
		t.Fatalf("late signal after recovery status=%d body=%s, want 202 late", after.Code, after.Body.String())
	}
}
