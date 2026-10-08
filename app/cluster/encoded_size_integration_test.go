package clusterapp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/cluster"
	"github.com/well-prado/new-blok/internal/clustertest"
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
	// Its genuine-outage subtest pauses voters: the test runs alone on the
	// shared cluster.
	clustertest.Disrupt(t)
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
	waitingID, lateID := cluster.WaitIDFor(waitingRun.RunID, "approval", ""), cluster.WaitIDFor(lateRun.RunID, "approval", "")
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
		restore := clustertest.PauseQuorum(t)
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

// encodedSizeFixture is a runtime with one indefinite-wait workflow whose
// input is {"text": "..."}, for probing the record bounds of #254.
type encodedSizeFixture struct {
	store   *distributed.Store
	runtime *cluster.Runtime
	tenant  string
	ctx     context.Context
}

func newEncodedSizeFixture(t *testing.T, partitions int) *encodedSizeFixture {
	t.Helper()
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
	workflow := cluster.Workflow{Program: contract.InternalProgram{WorkflowID: "encoded-size-bound", Digest: "sha256:" + strings.Repeat("9", 64), Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}}, DecodeInput: decode}
	runtime, err := cluster.New(store, engine.New(map[string]node.Any{}), map[string]cluster.Workflow{"encoded-size-bound": workflow}, cluster.Limits{Partitions: partitions, PartitionAdmissions: 128, TenantAdmissions: 128, OwnerTTL: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Second)
	t.Cleanup(cancel)
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	return &encodedSizeFixture{store: store, runtime: runtime, tenant: tenantForPartition(runtime, "p-0000"), ctx: ctx}
}

func (f *encodedSizeFixture) startWorker(t *testing.T, ownerID string) func() {
	t.Helper()
	worker := DistributedWorkerDependency(f.runtime, ownerID)
	if err := worker.Start(f.ctx); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := worker.Close(closeCtx); err != nil {
			t.Errorf("stop worker %.20q: %v", ownerID, err)
		}
	}
	t.Cleanup(stop)
	return stop
}

func (f *encodedSizeFixture) admit(key, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body)).WithContext(f.ctx)
	request.Header.Set("Idempotency-Key", key)
	NewDistributedWorkflowHandler(f.runtime, "encoded-size-bound", func(*http.Request) (string, error) { return f.tenant, nil }).ServeHTTP(recorder, request)
	return recorder
}

func (f *encodedSizeFixture) signal(waitID, signalID string, payload []byte) *httptest.ResponseRecorder {
	body := []byte(fmt.Sprintf(`{"waitId":%q,"signalId":%q,"payload":%s}`, waitID, signalID, payload))
	recorder := httptest.NewRecorder()
	NewDistributedSignalHandler(f.runtime, func(*http.Request) (string, error) { return f.tenant, nil },
		func(*http.Request) (string, error) { return "synthetic-principal", nil },
		func(*http.Request, string, string) bool { return true }).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/signals", bytes.NewReader(body)).WithContext(f.ctx))
	return recorder
}

// awaitRun polls until the run reaches state or d elapses, returning the
// last state seen.
func (f *encodedSizeFixture) awaitRun(runID, state string, d time.Duration) string {
	deadline, last := time.Now().Add(d), ""
	for time.Now().Before(deadline) && f.ctx.Err() == nil {
		if run, err := f.runtime.GetRun(f.ctx, f.tenant, runID); err == nil {
			if last = run.State; last == state {
				return last
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	return last
}

// TestDistributedNearLimitAdmissionStaysClaimableAndSignalable covers the
// three gaps review found in #254's first fix, over HTTP on real etcd:
//   - an admission is accepted only if the run still fits after the runtime
//     adds its largest owner ID and fence, so every 202 run is claimable;
//   - after a takeover by an owner with the longest, worst-encoding ID the
//     store accepts, a tiny signal to that run is accepted, not a 400; and
//   - a signal that makes the wait+run+event transaction exceed etcd's
//     request bound is a definite 400, never a 503.
func TestDistributedNearLimitAdmissionStaysClaimableAndSignalable(t *testing.T) {
	f := newEncodedSizeFixture(t, 2)
	parent := t
	stopShort := f.startWorker(t, "w1")
	body := func(n int) string { return `{"text":"` + strings.Repeat("<", n) + `"}` }
	// Binary-search the largest admissible count of '<'. Every probe must be
	// definite: 202 or 400, never a retryable 503.
	lo, hi, largest := 80_000, distributed.MaxPayloadBytes/6, ""
	for probe := 0; lo < hi; probe++ {
		mid := (lo + hi + 1) / 2
		response := f.admit(fmt.Sprintf("probe-%d", probe), body(mid))
		switch response.Code {
		case http.StatusAccepted:
			var admission cluster.Admission
			if err := json.Unmarshal(response.Body.Bytes(), &admission); err != nil || !admission.Accepted {
				t.Fatalf("n=%d admission=%s err=%v", mid, response.Body.String(), err)
			}
			lo, largest = mid, admission.RunID
		case http.StatusBadRequest:
			if response.Header().Get("Retry-After") != "" {
				t.Fatalf("n=%d rejected with Retry-After %q", mid, response.Header().Get("Retry-After"))
			}
			hi = mid - 1
		default:
			t.Fatalf("n=%d status=%d Retry-After=%q body=%q, want only 202 or a definite 400", mid, response.Code, response.Header().Get("Retry-After"), strings.TrimSpace(response.Body.String()))
		}
	}
	if largest == "" {
		t.Fatal("no admission near the limit was accepted")
	}
	t.Logf("largest admissible input: %d '<' (run %s)", lo, largest)
	if state := f.awaitRun(largest, "waiting", 15*time.Second); state != "waiting" {
		t.Fatalf("largest 202-accepted run state=%q after 15s, want claimed and suspended at its wait", state)
	}

	t.Run("takeover by the longest owner ID keeps a tiny signal valid", func(t *testing.T) {
		stopShort()
		// Registered on the parent: the next subtest runs under this owner.
		f.startWorker(parent, strings.Repeat("<", 180))
		waitID := cluster.WaitIDFor(largest, "approval", "")
		var response *httptest.ResponseRecorder
		for attempt := 0; attempt < 100; attempt++ {
			// 503 is expected only while the new owner acquires the partition.
			if response = f.signal(waitID, "tiny", []byte(`{"approved":true}`)); response.Code != http.StatusServiceUnavailable {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		var result cluster.SignalResult
		if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.Accepted {
			t.Fatalf("17-byte signal after takeover status=%d body=%q, want 202 accepted", response.Code, strings.TrimSpace(response.Body.String()))
		}
		// The signal re-admits the run and the long-ID owner claims it: the
		// claimed record still fits. (Its terminal record adds output on top
		// of the non-terminal headroom; #265 covers how it finishes.)
		deadline := time.Now().Add(15 * time.Second)
		for {
			run, err := f.runtime.GetRun(f.ctx, f.tenant, largest)
			if err == nil && run.OwnerID == strings.Repeat("<", 180) && run.State != "accepted" {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("signalled near-limit run=%q owner=%.12q err=%v, want claimed by the long-ID owner", run.State, run.OwnerID, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	})

	t.Run("signal over the etcd request bound is definite", func(t *testing.T) {
		// A fresh partition, isolated from the runs probed above. A longer
		// request key and tenant than the probes' cost a few bytes of
		// record, hence lo-10.
		f.tenant = tenantForPartition(f.runtime, "p-0001")
		response := f.admit("near-limit-for-signal", body(lo-10))
		var admission cluster.Admission
		if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &admission) != nil {
			t.Fatalf("fixture admission status=%d body=%s", response.Code, response.Body.String())
		}
		if state := f.awaitRun(admission.RunID, "waiting", 15*time.Second); state != "waiting" {
			t.Fatalf("fixture run state=%q, want waiting", state)
		}
		waitID := cluster.WaitIDFor(admission.RunID, "approval", "")
		// Walk the signal down from the record bound one '<' at a time. Each
		// one is under the edge bound; near the top the wait record itself is
		// over, then the three-record transaction is, then it fits.
		outcomes := map[string]int{}
		requestLevel := 0
		accepted := false
		for k := distributed.MaxPayloadBytes / 6; k > distributed.MaxPayloadBytes/6-1000 && !accepted; k-- {
			_, err := f.runtime.DeliverSignal(f.ctx, f.tenant, waitID, fmt.Sprintf("sweep-%d", k), "synthetic-principal", json.RawMessage(`"`+strings.Repeat("<", k)+`"`), true)
			var tooLarge *distributed.RecordTooLargeError
			switch {
			case err == nil:
				accepted = true
				outcomes["accepted"]++
			case errors.Is(err, cluster.ErrInvalid) && !errors.Is(err, cluster.ErrUnavailable):
				outcomes["invalid"]++
				if errors.As(err, &tooLarge) && tooLarge.Record == "request" {
					if requestLevel++; requestLevel == 1 {
						// The same class of signal over HTTP: 400, no Retry-After.
						assertNoRetry400(t, fmt.Sprintf("request-level signal of %d '<'", k), f.signal(waitID, fmt.Sprintf("sweep-http-%d", k), []byte(`"`+strings.Repeat("<", k)+`"`)))
					}
				}
			default:
				t.Fatalf("signal of %d '<': err=%v, want accepted or a definite ErrInvalid (never ErrUnavailable)", k, err)
			}
		}
		t.Logf("sweep outcomes=%v request-level rejections=%d", outcomes, requestLevel)
		if !accepted || requestLevel == 0 {
			t.Fatalf("sweep outcomes=%v request-level=%d, want the request-level window exercised and a signal finally accepted", outcomes, requestLevel)
		}
	})
}

// TestDistributedRunMetadataOverflowIsNotTheCallersFault forces the case the
// admission headroom makes unreachable for new runs: a stored run record
// (here written as a pre-headroom version could have) that only exceeds the
// bound once the runtime writes a longer current-owner ID. A tiny signal must
// not be blamed for it (no 400) and must not be told to retry (no 503): it is
// an internal 500 carrying ErrRecordOverflow.
func TestDistributedRunMetadataOverflowIsNotTheCallersFault(t *testing.T) {
	f := newEncodedSizeFixture(t, 1)
	stop := f.startWorker(t, "w1")
	response := f.admit("overflow", `{"text":"small"}`)
	var admission cluster.Admission
	if response.Code != http.StatusAccepted || json.Unmarshal(response.Body.Bytes(), &admission) != nil {
		t.Fatalf("fixture admission status=%d body=%s", response.Code, response.Body.String())
	}
	if state := f.awaitRun(admission.RunID, "waiting", 15*time.Second); state != "waiting" {
		t.Fatalf("fixture run state=%q, want waiting", state)
	}
	stop()
	acquire := func(ownerID string) distributed.Owner {
		for f.ctx.Err() == nil {
			if owner, err := f.store.Acquire(f.ctx, "p-0000", ownerID, 20*time.Second); err == nil {
				return owner
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatal("partition never became free")
		return distributed.Owner{}
	}
	short := acquire("s")
	data, revision, err := f.store.ReadState(f.ctx, "p-0000", admission.RunID)
	if err != nil || revision == 0 {
		t.Fatalf("read run: revision=%d err=%v", revision, err)
	}
	var run cluster.RunRecord
	if err := json.Unmarshal(data, &run); err != nil {
		t.Fatal(err)
	}
	// Inflate the input until the record sits just under the bound with the
	// one-byte owner ID "s".
	run.OwnerID, run.Fence = "s", short.Token
	base, _ := json.Marshal(run)
	run.Input = json.RawMessage(`"` + strings.Repeat("a", distributed.MaxPayloadBytes-len(base)-64) + `"`)
	inflated, _ := json.Marshal(run)
	if len(inflated) > distributed.MaxPayloadBytes || len(inflated)+179*6 <= distributed.MaxPayloadBytes {
		t.Fatalf("inflated run is %d bytes; want it under the bound and over it with a 180-byte '<' owner ID", len(inflated))
	}
	if _, err := f.store.CommitFencedState(f.ctx, short, admission.RunID, revision, "test-inflate-run", "test.inflate", inflated, inflated); err != nil {
		t.Fatal(err)
	}
	if err := f.store.Release(f.ctx, short); err != nil {
		t.Fatal(err)
	}
	long := acquire(strings.Repeat("<", 180))
	t.Cleanup(func() { _ = f.store.Release(context.Background(), long) })
	waitID := cluster.WaitIDFor(admission.RunID, "approval", "")
	_, err = f.runtime.DeliverSignal(f.ctx, f.tenant, waitID, "tiny", "synthetic-principal", json.RawMessage(`{"approved":true}`), true)
	if !errors.Is(err, cluster.ErrRecordOverflow) || errors.Is(err, cluster.ErrInvalid) || errors.Is(err, cluster.ErrUnavailable) {
		t.Fatalf("tiny signal to a run whose own metadata overflows: err=%v, want ErrRecordOverflow, neither ErrInvalid nor ErrUnavailable", err)
	}
	response = f.signal(waitID, "tiny-http", []byte(`{"approved":true}`))
	if response.Code != http.StatusInternalServerError || response.Header().Get("Retry-After") != "" {
		t.Fatalf("tiny signal over HTTP status=%d Retry-After=%q body=%q, want 500 with no Retry-After (not the caller's 400, not a retryable 503)", response.Code, response.Header().Get("Retry-After"), strings.TrimSpace(response.Body.String()))
	}
	if wait, err := f.runtime.GetWait(f.ctx, f.tenant, waitID); err != nil || wait.State != "waiting" {
		t.Fatalf("wait after the refused signal=%+v err=%v, want still waiting (nothing written)", wait, err)
	}
}

func assertNoRetry400(t *testing.T, label string, response *httptest.ResponseRecorder) {
	t.Helper()
	if response.Code != http.StatusBadRequest || response.Header().Get("Retry-After") != "" {
		t.Fatalf("%s: status=%d Retry-After=%q body=%q, want a definite 400 with no Retry-After", label, response.Code, response.Header().Get("Retry-After"), strings.TrimSpace(response.Body.String()))
	}
}
