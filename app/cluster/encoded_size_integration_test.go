package clusterapp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/cluster"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
)

type distributedTextInput struct {
	Text string `json:"text"`
}

// TestDistributedEncodedOversizeIsDefiniteNotUnavailable covers issue #254.
// encoding/json HTML-escapes '<', '>' and '&' to six bytes each, so a body the
// HTTP edge accepts can encode into a record over the store's bound. That is a
// property of the request, not of the cluster: it must be a definite 400 with
// no Retry-After on admission and on both signal paths, never a 503 a client
// would retry forever. A genuine quorum loss on the same wait must still be a
// retryable 503 + Retry-After.
func TestDistributedEncodedOversizeIsDefiniteNotUnavailable(t *testing.T) {
	store := namespacedDistributedStore(t)
	decode := func(raw json.RawMessage) (any, error) {
		var input distributedTextInput
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			return nil, err
		}
		return input, nil
	}
	waitProgram := func(id, digit string, timeoutMillis int64) cluster.Workflow {
		return cluster.Workflow{Program: contract.InternalProgram{WorkflowID: id, Digest: "sha256:" + strings.Repeat(digit, 64), Instructions: []contract.InternalInstruction{
			{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval", TimeoutMillis: timeoutMillis}},
			{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
		}}, DecodeInput: decode}
	}
	workflows := map[string]cluster.Workflow{
		"encoded-size-waiting": waitProgram("encoded-size-waiting", "6", 0),
		"encoded-size-late":    waitProgram("encoded-size-late", "7", 1),
	}
	runtime, err := cluster.New(store, engine.New(map[string]node.Any{}), workflows, cluster.Limits{Partitions: 8, PartitionAdmissions: 16, TenantAdmissions: 8, OwnerTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	tenant := tenantForPartition(runtime, "p-0000")
	worker := DistributedWorkerDependency(runtime, fmt.Sprintf("encoded-size-worker-%d", time.Now().UnixNano()))
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
	tenantOf := func(*http.Request) (string, error) { return tenant, nil }
	admit := func(workflow, key, body string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body)).WithContext(ctx)
		request.Header.Set("Idempotency-Key", key)
		NewDistributedWorkflowHandler(runtime, workflow, tenantOf).ServeHTTP(recorder, request)
		return recorder
	}
	signals := NewDistributedSignalHandler(runtime, tenantOf,
		func(*http.Request) (string, error) { return "synthetic-principal", nil },
		func(*http.Request, string, string) bool { return true })
	signal := func(requestCtx context.Context, waitID, signalID string, payload []byte) *httptest.ResponseRecorder {
		// Built by hand: json.Marshal would HTML-escape the payload, and the
		// edge must see the exact bytes a client sends.
		body := []byte(fmt.Sprintf(`{"waitId":%q,"signalId":%q,"payload":%s}`, waitID, signalID, payload))
		if len(body) > cluster.MaxInputBytes {
			t.Fatalf("fixture body is %d bytes, over the edge bound %d; it would not reach the store", len(body), cluster.MaxInputBytes)
		}
		recorder := httptest.NewRecorder()
		signals.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/signals", bytes.NewReader(body)).WithContext(requestCtx))
		return recorder
	}
	assertDefinite := func(t *testing.T, label string, response *httptest.ResponseRecorder) {
		t.Helper()
		if response.Code != http.StatusBadRequest || response.Header().Get("Retry-After") != "" {
			t.Fatalf("%s: status=%d Retry-After=%q body=%q, want a definite 400 with no Retry-After (never a retryable 503)", label, response.Code, response.Header().Get("Retry-After"), strings.TrimSpace(response.Body.String()))
		}
	}

	waiting := admit("encoded-size-waiting", "waiting-run", `{"text":"small"}`)
	late := admit("encoded-size-late", "late-run", `{"text":"small"}`)
	var waitingRun, lateRun cluster.Admission
	if waiting.Code != http.StatusAccepted || json.Unmarshal(waiting.Body.Bytes(), &waitingRun) != nil || late.Code != http.StatusAccepted || json.Unmarshal(late.Body.Bytes(), &lateRun) != nil {
		t.Fatalf("fixture admissions: waiting=%d %s late=%d %s", waiting.Code, waiting.Body.String(), late.Code, late.Body.String())
	}
	waitingID, lateID := cluster.WaitIDFor(waitingRun.RunID, "approval"), cluster.WaitIDFor(lateRun.RunID, "approval")
	awaitWait := func(waitID, state string) {
		for ctx.Err() == nil {
			if wait, err := runtime.GetWait(ctx, tenant, waitID); err == nil && wait.State == state {
				return
			}
			time.Sleep(25 * time.Millisecond)
		}
		t.Fatalf("wait %s never reached state %q", waitID, state)
	}
	awaitWait(waitingID, "waiting")
	awaitWait(lateID, "timed_out")

	// Sizes span (limit/6, edge limit]: every one passes the HTTP edge, and
	// every one encodes past distributed.MaxPayloadBytes once escaped.
	sizes := []int{distributed.MaxPayloadBytes/6 + 64, 300_000, cluster.MaxInputBytes - 256}
	htmlString := func(n int) []byte { return []byte(`"` + strings.Repeat("<", n) + `"`) }
	for _, n := range sizes {
		if escaped, _ := json.Marshal(json.RawMessage(htmlString(n))); len(escaped) <= distributed.MaxPayloadBytes {
			t.Fatalf("fixture size %d escapes to %d bytes, not over the store bound", n, len(escaped))
		}
	}

	t.Run("admission near the limit", func(t *testing.T) {
		// The canonical input fits the store bound by a few hundred bytes;
		// only the run record around it does not.
		n := (distributed.MaxPayloadBytes - len(`{"text":""}`) - 100) / 6
		body := `{"text":"` + strings.Repeat("<", n) + `"}`
		canonical, _ := json.Marshal(distributedTextInput{Text: strings.Repeat("<", n)})
		if len(body) > cluster.MaxInputBytes || len(canonical) > distributed.MaxPayloadBytes || len(canonical) < distributed.MaxPayloadBytes-256 {
			t.Fatalf("fixture body=%d canonical=%d, want body under the edge and canonical just under the store bound", len(body), len(canonical))
		}
		for attempt := 1; attempt <= 3; attempt++ {
			assertDefinite(t, fmt.Sprintf("oversized admission attempt %d", attempt), admit("encoded-size-waiting", "html-admission", body))
		}
		// Nothing durable was written under the key: the same key with a
		// small input is a fresh acceptance, not a duplicate or a conflict.
		fresh := admit("encoded-size-waiting", "html-admission", `{"text":"small"}`)
		var admission cluster.Admission
		if fresh.Code != http.StatusAccepted || json.Unmarshal(fresh.Body.Bytes(), &admission) != nil || !admission.Accepted {
			t.Fatalf("same key after the rejected admission status=%d body=%s, want a fresh 202 accepted", fresh.Code, fresh.Body.String())
		}
	})

	t.Run("late signal", func(t *testing.T) {
		for _, n := range sizes {
			for attempt := 1; attempt <= 2; attempt++ {
				assertDefinite(t, fmt.Sprintf("late signal of %d '<' attempt %d", n, attempt), signal(ctx, lateID, fmt.Sprintf("late-html-%d", n), htmlString(n)))
			}
		}
		accepted := signal(ctx, lateID, "late-small", []byte(`{"approved":true}`))
		var result cluster.SignalResult
		if accepted.Code != http.StatusAccepted || json.Unmarshal(accepted.Body.Bytes(), &result) != nil || !result.Late {
			t.Fatalf("small late signal after the rejections status=%d body=%s, want 202 late", accepted.Code, accepted.Body.String())
		}
	})

	t.Run("waiting signal", func(t *testing.T) {
		for _, n := range sizes {
			for attempt := 1; attempt <= 2; attempt++ {
				assertDefinite(t, fmt.Sprintf("waiting signal of %d '<' attempt %d", n, attempt), signal(ctx, waitingID, fmt.Sprintf("waiting-html-%d", n), htmlString(n)))
			}
		}
		if wait, err := runtime.GetWait(ctx, tenant, waitingID); err != nil || wait.State != "waiting" {
			t.Fatalf("wait after rejected oversize signals=%+v err=%v, want still waiting", wait, err)
		}
	})

	t.Run("genuine outage stays retryable", func(t *testing.T) {
		restore := pauseQuorum(t)
		outageCtx, stop := context.WithTimeout(ctx, 3*time.Second)
		during := signal(outageCtx, waitingID, "waiting-small", []byte(`{"approved":true}`))
		stop()
		restore()
		if during.Code != http.StatusServiceUnavailable || during.Header().Get("Retry-After") != "1" {
			t.Fatalf("waiting signal during quorum loss status=%d Retry-After=%q body=%s, want 503 with Retry-After 1", during.Code, during.Header().Get("Retry-After"), during.Body.String())
		}
		var after *httptest.ResponseRecorder
		for attempt := 0; attempt < 300; attempt++ {
			after = signal(ctx, waitingID, "waiting-small", []byte(`{"approved":true}`))
			if after.Code != http.StatusServiceUnavailable {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		var result cluster.SignalResult
		if after.Code != http.StatusAccepted || json.Unmarshal(after.Body.Bytes(), &result) != nil || !result.Accepted {
			t.Fatalf("waiting signal after recovery status=%d body=%s, want 202 accepted", after.Code, after.Body.String())
		}
	})
}

// pauseQuorum pauses two of the three etcd voters and returns an idempotent
// restore, also registered as cleanup.
func pauseQuorum(t *testing.T) func() {
	t.Helper()
	voters := strings.Split(os.Getenv("BLOK_DISTRIBUTED_ETCD_VOTERS"), ",")
	if len(voters) != 3 || voters[0] == "" {
		t.Fatal("BLOK_DISTRIBUTED_ETCD_VOTERS must name the three voter containers")
	}
	paused := make([]string, 0, 2)
	restore := func() {
		for index := len(paused) - 1; index >= 0; index-- {
			if output, err := exec.Command("docker", "unpause", paused[index]).CombinedOutput(); err != nil {
				t.Errorf("restore voter %s: %v: %s", paused[index], err, output)
			}
		}
		paused = paused[:0]
	}
	t.Cleanup(restore)
	for _, voter := range voters[1:] {
		if output, err := exec.Command("docker", "pause", voter).CombinedOutput(); err != nil {
			restore()
			t.Fatalf("pause voter %s: %v: %s", voter, err, output)
		}
		paused = append(paused, voter)
	}
	return restore
}
