package parity

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

type contracts struct {
	Workloads []workload `json:"workloads"`
}

func TestWorkerRecoveryUsesPublishedOldAndNativeQueueAPIs(t *testing.T) {
	oldResult := runOldWorkerAdapterReset(t, map[string]any{"jobId": "recovery-108", "orderId": "order-108"})
	var old struct {
		Engine        string `json:"engine"`
		Version       string `json:"version"`
		Adapter       string `json:"adapter"`
		Accepted      bool   `json:"accepted"`
		BeforeRestart struct {
			Waiting int `json:"waiting"`
		} `json:"beforeRestart"`
		AfterRestart struct {
			Waiting int `json:"waiting"`
		} `json:"afterRestart"`
	}
	if err := json.Unmarshal(oldResult, &old); err != nil {
		t.Fatal(err)
	}
	if !old.Accepted || old.Adapter != "InMemoryAdapter" || old.BeforeRestart.Waiting != 1 || old.AfterRestart.Waiting != 0 {
		t.Fatalf("old published worker adapter recovery probe = %+v", old)
	}

	databasePath := filepath.Join(t.TempDir(), "recovery.db")
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), worker.EnqueueRequest{RequestKey: "recovery-108", Kind: "order.create", Payload: []byte(`{"jobId":"recovery-108","orderId":"order-108"}`), MaxAttempts: 3}); err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE recovered_effects (request_key TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err = worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx worker.Tx, job worker.Job) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO recovered_effects (request_key) VALUES (?)`, job.RequestKey)
		return err
	})
	if err != nil || !processed {
		t.Fatalf("new durable queue after database reopen processed=%t err=%v", processed, err)
	}
	job, err := queue.Get(context.Background(), "recovery-108")
	if err != nil || job.State != worker.StateCompleted || job.Attempt != 1 {
		t.Fatalf("new recovered job=%+v err=%v", job, err)
	}
	var effects int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM recovered_effects`).Scan(&effects)
	}); err != nil {
		t.Fatal(err)
	}
	if effects != 1 {
		t.Fatalf("recovered business effects=%d, want exactly one", effects)
	}
	t.Logf("E20-T01 recovery old=%s new_adapter=trigger/worker.Queue database=reopened job_state=%s job_attempt=%d committed_effects=%d", string(oldResult), job.State, job.Attempt, effects)
}

type workload struct {
	ID         string         `json:"id"`
	Operation  string         `json:"operation"`
	Request    map[string]any `json:"request"`
	Deliveries int            `json:"deliveries,omitempty"`
	Retry      *struct {
		MaxAttempts int `json:"maxAttempts"`
	} `json:"retry,omitempty"`
	Expected struct {
		Output           json.RawMessage `json:"output"`
		ErrorCode        string          `json:"errorCode"`
		NormalizedOutput json.RawMessage `json:"normalizedOutput"`
		ProviderCalls    int             `json:"providerCalls"`
		CommittedEffects int             `json:"committedEffects"`
		OldRunner        *expectedRun    `json:"oldRunner"`
		NewNativeEngine  *expectedRun    `json:"newNativeEngine"`
		NewWorkerQueue   *struct {
			Output           json.RawMessage `json:"output"`
			ProviderCalls    int             `json:"providerCalls"`
			CommittedEffects int             `json:"committedEffects"`
		} `json:"newWorkerQueue"`
	} `json:"expected"`
}

type expectedRun struct {
	OK               bool   `json:"ok"`
	ErrorCode        string `json:"errorCode"`
	ErrorMessage     string `json:"errorMessage"`
	ProviderCalls    int    `json:"providerCalls"`
	CommittedEffects int    `json:"committedEffects"`
}

type runResult struct {
	Engine   string          `json:"engine"`
	Version  string          `json:"version"`
	OK       bool            `json:"ok"`
	Response json.RawMessage `json:"response"`
	Error    struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Steps []struct {
		ID      string `json:"id"`
		Calls   int    `json:"calls"`
		Attempt int    `json:"attempt"`
	} `json:"steps"`
}

type providerLedger struct {
	mu      sync.Mutex
	calls   int
	effects int
	seen    map[string]bool
	attempt map[string]int
	gates   map[string]*providerGate
}

type providerGate struct {
	entered chan struct{}
	release chan struct{}
	used    bool
	once    sync.Once
}

func (l *ledgerServer) gateSuccessfulRetry(key string) *providerGate {
	l.ledger.mu.Lock()
	defer l.ledger.mu.Unlock()
	gate := &providerGate{entered: make(chan struct{}), release: make(chan struct{})}
	l.ledger.gates[key] = gate
	return gate
}

func (g *providerGate) waitForCall() {
	close(g.entered)
	<-g.release
}

func (g *providerGate) unblock() { g.once.Do(func() { close(g.release) }) }

func (l *ledgerServer) attemptsFor(operation, key string) int {
	l.ledger.mu.Lock()
	defer l.ledger.mu.Unlock()
	return l.ledger.attempt[operation+"\x00"+key]
}

func TestExecutableOldAndNewEngineWorkloads(t *testing.T) {
	fixturePath := filepath.Join("..", "..", "testdata", "parity", "contracts.json")
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var fixture contracts
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, tc := range fixture.Workloads {
		if tc.ID != "quote-success" && tc.ID != "quote-invalid-sku" && tc.ID != "order-duplicate-delivery" && tc.ID != "job-retry" {
			continue
		}
		t.Run(tc.ID, func(t *testing.T) {
			oldProvider := newProvider(t)
			oldStarted := time.Now()
			oldRun := runOldEngine(t, oldProvider.URL, tc)
			for delivery := 1; delivery < max(1, tc.Deliveries); delivery++ {
				oldRun = runOldEngine(t, oldProvider.URL, tc)
			}
			wantOld := expectedRun{OK: tc.Expected.ErrorCode == "", ErrorCode: tc.Expected.ErrorCode, ProviderCalls: tc.Expected.ProviderCalls, CommittedEffects: tc.Expected.CommittedEffects}
			if tc.Expected.OldRunner != nil {
				wantOld = *tc.Expected.OldRunner
			}
			assertRun(t, oldRun, wantOld, oldProvider.ledger)
			oldDuration := time.Since(oldStarted)

			newProvider := newProvider(t)
			newStarted := time.Now()
			newRun := runNewNativeEngine(t, newProvider.URL, tc)
			for delivery := 1; delivery < max(1, tc.Deliveries); delivery++ {
				newRun = runNewNativeEngine(t, newProvider.URL, tc)
			}
			wantNew := expectedRun{OK: tc.Expected.ErrorCode == "", ErrorCode: tc.Expected.ErrorCode, ProviderCalls: tc.Expected.ProviderCalls, CommittedEffects: tc.Expected.CommittedEffects}
			if tc.Expected.NewNativeEngine != nil {
				wantNew = *tc.Expected.NewNativeEngine
			}
			assertRun(t, newRun, wantNew, newProvider.ledger)
			newDuration := time.Since(newStarted)
			t.Logf("E20-T01 raw old=%s new=%s old_provider_calls=%d old_committed_effects=%d new_provider_calls=%d new_committed_effects=%d old_wall=%s new_wall=%s", mustJSON(t, oldRun), mustJSON(t, newRun), oldProvider.ledger.calls, oldProvider.ledger.effects, newProvider.ledger.calls, newProvider.ledger.effects, oldDuration, newDuration)
			if wantOld.OK && wantNew.OK {
				expected := tc.Expected.Output
				if len(expected) == 0 {
					expected = tc.Expected.NormalizedOutput
				}
				if len(expected) == 0 || !equalJSON(expected, oldRun.Response) || !equalJSON(expected, newRun.Response) {
					t.Fatalf("observed outputs do not match predeclared expected output %s\nold: %s\nnew: %s", expected, oldRun.Response, newRun.Response)
				}
			}

			if tc.ID != "job-retry" && wantOld.OK {
				if !equalJSON(oldRun.Response, newRun.Response) {
					t.Fatalf("business output differs\nold: %s\nnew: %s", oldRun.Response, newRun.Response)
				}
			}
		})
	}
}

func TestOldWebhookTriggerDuplicateDelivery(t *testing.T) {
	tc := findWorkload(t, "webhook-duplicate")
	oldProvider := newProvider(t)
	raw := runOldWebhookTrigger(t, oldProvider.URL, tc)
	var observed struct {
		Engine    string `json:"engine"`
		Version   string `json:"version"`
		Trigger   string `json:"trigger"`
		Responses []struct {
			Status int `json:"status"`
			Body   struct {
				Status string `json:"status"`
			} `json:"body"`
		} `json:"responses"`
	}
	if err := json.Unmarshal(raw, &observed); err != nil {
		t.Fatalf("decode old webhook result %s: %v", raw, err)
	}
	calls, effects := oldProvider.ledger.snapshot()
	if observed.Trigger != "WebhookTrigger" || len(observed.Responses) != 2 || observed.Responses[0].Status != http.StatusOK || observed.Responses[1].Status != http.StatusOK || observed.Responses[1].Body.Status != "duplicate" || calls != tc.Expected.ProviderCalls || effects != tc.Expected.CommittedEffects {
		t.Fatalf("old webhook raw=%s provider calls=%d effects=%d", raw, calls, effects)
	}
	newProvider := newProvider(t)
	newResponses := runNewWebhookTrigger(t, newProvider.URL, tc)
	t.Logf("E20-T01 new webhook admission raw=%s", mustJSON(t, newResponses))
	newCalls, newEffects := newProvider.ledger.snapshot()
	if newResponses[0].HTTPStatus != http.StatusAccepted || newResponses[0].Status != "accepted" || newResponses[1].HTTPStatus != http.StatusOK || newResponses[1].Status != "duplicate" || newCalls != tc.Expected.ProviderCalls || newEffects != tc.Expected.CommittedEffects || !equalJSON(tc.Expected.NormalizedOutput, newResponses[0].ProviderOutput) {
		t.Fatalf("new webhook responses=%+v provider calls=%d effects=%d", newResponses, newCalls, newEffects)
	}
	t.Logf("E20-T01 webhook old_raw=%s old_provider_calls=%d old_effects=%d new_responses=%s new_provider_calls=%d new_effects=%d", string(raw), calls, effects, mustJSON(t, newResponses), newCalls, newEffects)
}

func TestWorkerTriggerRetryUsesRealPublishedTriggerAndInMemoryAdapter(t *testing.T) {
	tc := findWorkload(t, "job-retry")
	provider := newProvider(t)
	raw := runOldWorkerTriggerRetry(t, provider.URL, tc)
	var old struct {
		Trigger string `json:"trigger"`
		Adapter string `json:"adapter"`
		JobID   string `json:"jobId"`
		Stats   struct {
			Completed int `json:"completed"`
			Failed    int `json:"failed"`
		} `json:"stats"`
	}
	if err := json.Unmarshal(raw, &old); err != nil {
		t.Fatalf("decode old WorkerTrigger retry result %s: %v", raw, err)
	}
	calls, effects := provider.ledger.snapshot()
	if old.Trigger != "WorkerTrigger" || old.Adapter != "InMemoryAdapter" || old.JobID != tc.Request["jobId"] || old.Stats.Completed != 1 || old.Stats.Failed != 0 || calls != 2 || effects != 1 {
		t.Fatalf("old WorkerTrigger retry raw=%s calls=%d effects=%d", raw, calls, effects)
	}
	t.Logf("E20-T01 old actual worker retry=%s provider_calls=%d committed_effects=%d (first provider response is predeclared synthetic 503; adapter redelivery is real)", raw, calls, effects)

	newProvider := newProvider(t)
	newCalls, newEffects, finalOutput := runNewDurableQueueRetry(t, newProvider, tc)
	wantQueue := tc.Expected.NewWorkerQueue
	if newCalls != wantQueue.ProviderCalls || newEffects != wantQueue.CommittedEffects || !equalJSON(json.RawMessage(wantQueue.Output), finalOutput) {
		t.Fatalf("new durable queue retry calls=%d effects=%d output=%s; expected %+v", newCalls, newEffects, finalOutput, wantQueue)
	}
	t.Logf("E20-T01 new actual durable queue retry output=%s provider_calls=%d committed_effects=%d", finalOutput, newCalls, newEffects)
}

func TestSSETriggerEmitsExpectedEventThroughRealAdapters(t *testing.T) {
	tc := findWorkload(t, "stream-event-and-disconnect")
	oldRaw := runOldSSETrigger(t, tc)
	var old struct {
		Engine      string `json:"engine"`
		Version     string `json:"version"`
		Trigger     string `json:"trigger"`
		Status      int    `json:"status"`
		ContentType string `json:"contentType"`
		Stream      string `json:"stream"`
	}
	if err := json.Unmarshal(oldRaw, &old); err != nil {
		t.Fatalf("decode old SSE trigger output %s: %v", oldRaw, err)
	}
	oldEvents := parseSSE(t, old.Stream)
	if old.Trigger != "SSETrigger" || old.Status != http.StatusOK || len(oldEvents) != 1 || oldEvents[0].ID != "1" || oldEvents[0].Event != "order" || !equalJSON(json.RawMessage(oldEvents[0].Data), rawEventData(tc)) {
		t.Fatalf("old SSE stream=%+v frames=%+v", old, oldEvents)
	}
	newWire, accepted, duplicate, newContinued := runNewSSETrigger(t, tc)
	newEvents := parseSSE(t, newWire)
	if accepted != http.StatusAccepted || duplicate != http.StatusOK || len(newEvents) != 1 || newEvents[0].Event != "order" || !equalJSON(json.RawMessage(newEvents[0].Data), rawEventData(tc)) {
		t.Fatalf("new SSE accepted=%d duplicate=%d wire=%q events=%+v", accepted, duplicate, newWire, newEvents)
	}
	oldDisconnect := runOldSSEDisconnect(t)
	if !oldDisconnect {
		t.Fatal("old SSE workflow did not observe client disconnect cancellation")
	}
	if !newContinued {
		t.Fatal("new SSE disconnect probe did not process its durable job")
	}
	t.Logf("E20-T01 SSE old_raw=%s old_events=%s new_start_status=%d new_duplicate_status=%d new_wire=%q new_events=%s old_disconnect_cancelled_work=%t new_disconnect_preserved_work=%t guarantee_difference=%q", string(oldRaw), mustJSON(t, oldEvents), accepted, duplicate, newWire, mustJSON(t, newEvents), oldDisconnect, newContinued, "old GET opens and runs workflow; new POST durably admits work and GET follows its hub stream")
}

type streamEvent struct {
	ID    string `json:"id"`
	Event string `json:"event"`
	Data  string `json:"data"`
}

func parseSSE(t *testing.T, wire string) []streamEvent {
	t.Helper()
	var events []streamEvent
	var current streamEvent
	scanner := bufio.NewScanner(strings.NewReader(wire))
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			if current.Event != "" || current.Data != "" {
				events = append(events, current)
			}
			current = streamEvent{}
			continue
		}
		field, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		value = strings.TrimPrefix(value, " ")
		switch field {
		case "id":
			current.ID = value
		case "event":
			current.Event = value
		case "data":
			current.Data = value
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if current.Event != "" || current.Data != "" {
		events = append(events, current)
	}
	return events
}

func rawEventData(tc workload) json.RawMessage {
	data, _ := json.Marshal(tc.Request["events"].([]any)[0].(map[string]any)["data"])
	return data
}

func runNewSSETrigger(t *testing.T, tc workload) (wire string, acceptedStatus int, duplicateStatus int, disconnectContinued bool) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "sse.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	principal := trigger.Principal{ID: "synthetic:sse-client"}
	authenticate := func(request *http.Request) (trigger.Principal, error) {
		if request.Header.Get("Authorization") != "Bearer synthetic" {
			return trigger.Principal{}, errors.New("unauthenticated")
		}
		return principal, nil
	}
	hub, err := sse.NewHub(sse.HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	endpoint := sse.Endpoint{Name: "orders", Path: "/sse/orders", Kind: "order.stream", Submit: queue, Tracker: queue, Authenticate: authenticate, InputSchema: []byte(`{"type":"object","properties":{"requestKey":{"type":"string"},"streamId":{"type":"string"},"events":{"type":"array","items":{"type":"object","properties":{"id":{"type":"string"},"event":{"type":"string"},"data":{"type":"object","additionalProperties":true}},"required":["id","event","data"]}}},"required":["requestKey","streamId","events"],"additionalProperties":true}`)}
	serverHandler, err := sse.New(application, hub, []sse.Endpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(serverHandler)
	defer server.Close()
	body, err := json.Marshal(tc.Request)
	if err != nil {
		t.Fatal(err)
	}
	start := func(key string) (int, string) {
		request, err := http.NewRequest(http.MethodPost, server.URL+endpoint.Path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer synthetic")
		request.Header.Set("Idempotency-Key", key)
		request.Header.Set("Content-Type", "application/json")
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		var result struct {
			Stream string `json:"stream"`
		}
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return response.StatusCode, result.Stream
	}
	acceptedStatus, streamID := start("stream-108")
	duplicateStatus, duplicateStream := start("stream-108")
	if duplicateStream != streamID {
		t.Fatalf("duplicate start changed stream identity: %q -> %q", streamID, duplicateStream)
	}
	submissionKey := sse.SubmissionKey(endpoint.Name, principal, "stream-108")
	if streamID != sse.StreamID(submissionKey) {
		t.Fatalf("stream id %q does not match actual submission identity", streamID)
	}
	_, disconnectStreamID := start("disconnect-108")
	disconnectRequest, err := http.NewRequest(http.MethodGet, server.URL+endpoint.Path+"/"+disconnectStreamID, nil)
	if err != nil {
		t.Fatal(err)
	}
	disconnectRequest.Header.Set("Authorization", "Bearer synthetic")
	disconnectResponse, err := server.Client().Do(disconnectRequest)
	if err != nil {
		t.Fatal(err)
	}
	if err := disconnectResponse.Body.Close(); err != nil {
		t.Fatal(err)
	}
	_, err = queue.ProcessOnce(context.Background(), func(_ context.Context, _ worker.Tx, job worker.Job) error {
		var payload struct {
			Events []struct {
				ID    string          `json:"id"`
				Event string          `json:"event"`
				Data  json.RawMessage `json:"data"`
			} `json:"events"`
		}
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		if len(payload.Events) != 1 {
			return errors.New("expected one predeclared event")
		}
		_, err := hub.Finish(streamID, sse.Event{Type: payload.Events[0].Event, Data: payload.Events[0].Data})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	processedAfterDisconnect, err := queue.ProcessOnce(context.Background(), func(_ context.Context, _ worker.Tx, _ worker.Job) error { return nil })
	if err != nil || !processedAfterDisconnect {
		t.Fatalf("durable job after SSE client disconnect processed=%t err=%v", processedAfterDisconnect, err)
	}
	disconnectedJob, err := queue.Get(context.Background(), sse.SubmissionKey(endpoint.Name, principal, "disconnect-108"))
	if err != nil || disconnectedJob.State != worker.StateCompleted {
		t.Fatalf("new queued job after stream disconnect=%+v err=%v", disconnectedJob, err)
	}
	request, err := http.NewRequest(http.MethodGet, server.URL+endpoint.Path+"/"+streamID, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer synthetic")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded), acceptedStatus, duplicateStatus, disconnectedJob.State == worker.StateCompleted
}

func runOldSSEDisconnect(t *testing.T) bool {
	t.Helper()
	command := exec.Command("node", "run.mjs")
	command.Dir = "old-engine"
	command.Stdin = strings.NewReader(`{"mode":"sse-disconnect"}`)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("pinned Blok 2.5.0 SSE disconnect probe failed: %v\nstderr: %s\nstdout: %s", err, stderr.String(), stdout.String())
	}
	var result struct {
		WorkflowCancelled bool `json:"workflowCancelled"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		t.Fatalf("decode old SSE disconnect output %q: %v", stdout.String(), err)
	}
	return result.WorkflowCancelled
}

type webhookObservation struct {
	HTTPStatus     int             `json:"httpStatus"`
	Status         string          `json:"status"`
	Error          string          `json:"error,omitempty"`
	ProviderOutput json.RawMessage `json:"providerOutput,omitempty"`
}

func runNewWebhookTrigger(t *testing.T, providerURL string, tc workload) []webhookObservation {
	t.Helper()
	keyBytes := []byte("synthetic-webhook-signing-secret")
	secret, err := webhook.NewSecret(keyBytes)
	if err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(t.TempDir(), "webhook.db")
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	endpoint := webhook.Endpoint{
		Path: "/hooks/orders", Provider: "svix", Principal: trigger.Principal{ID: "synthetic:provider"}, Kind: "order.event",
		Verifier: webhook.StandardWebhooks{Keys: []webhook.Key{{ID: "synthetic-key", Secret: secret}}}, Submit: queue,
		InputSchema: []byte(`{"type":"object","properties":{"requestKey":{"type":"string"},"eventId":{"type":"string"},"type":{"type":"string"},"kind":{"type":"string"},"orderId":{"type":"string"}},"required":["requestKey","eventId","type","kind","orderId"]}`),
	}
	handler, err := webhook.New(application, nil, []webhook.Endpoint{endpoint})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	body, err := json.Marshal(tc.Request)
	if err != nil {
		t.Fatal(err)
	}
	eventID, _ := tc.Request["eventId"].(string)
	stamp := time.Now().UTC().Truncate(time.Second)
	observed := make([]webhookObservation, 0, tc.Deliveries)
	for range tc.Deliveries {
		request, err := http.NewRequest(http.MethodPost, server.URL+endpoint.Path, bytes.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("webhook-id", eventID)
		request.Header.Set("webhook-timestamp", strconv.FormatInt(stamp.Unix(), 10))
		request.Header.Set("webhook-signature", webhook.SignStandard(secret, eventID, stamp, body))
		response, err := server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		var bodyResult map[string]string
		if err := json.NewDecoder(response.Body).Decode(&bodyResult); err != nil {
			response.Body.Close()
			t.Fatal(err)
		}
		response.Body.Close()
		observed = append(observed, webhookObservation{HTTPStatus: response.StatusCode, Status: bodyResult["status"], Error: bodyResult["error"]})
	}
	t.Logf("native webhook admissions=%s", mustJSON(t, observed))
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(context.Background(), `CREATE TABLE webhook_business_effects (request_key TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, tx worker.Tx, job worker.Job) error {
		var payload map[string]any
		if err := json.Unmarshal(job.Payload, &payload); err != nil {
			return err
		}
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/webhook", bytes.NewReader(encoded))
		if err != nil {
			return err
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("idempotency-key", fmt.Sprint(payload["requestKey"]))
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		var result map[string]any
		if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
			return err
		}
		observed[0].ProviderOutput, _ = json.Marshal(result)
		_, err = tx.ExecContext(ctx, `INSERT INTO webhook_business_effects (request_key) VALUES (?)`, fmt.Sprint(payload["requestKey"]))
		return err
	})
	if err != nil || !processed {
		t.Fatalf("native durable webhook job processed=%t err=%v", processed, err)
	}
	return observed
}

func findWorkload(t *testing.T, id string) workload {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", "contracts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture contracts
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	for _, item := range fixture.Workloads {
		if item.ID == id {
			return item
		}
	}
	t.Fatalf("workload %q missing from contract fixture", id)
	return workload{}
}

func runOldEngine(t *testing.T, providerURL string, tc workload) runResult {
	t.Helper()
	result, err := runOldEngineRaw(providerURL, tc)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func runOldEngineRaw(providerURL string, tc workload) (runResult, error) {
	input := map[string]any{"operation": tc.Operation, "payload": tc.Request}
	if tc.Retry != nil {
		input["retry"] = tc.Retry
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return runResult{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "run.mjs")
	command.Dir = filepath.Join("old-engine")
	command.Env = append(os.Environ(), "BLOK_PARITY_PROVIDER_URL="+providerURL)
	command.Stdin = bytes.NewReader(encoded)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		return runResult{}, fmt.Errorf("pinned Blok 2.5.0 runner failed: %w\nstderr: %s\nstdout: %s", err, stderr.String(), stdout.String())
	}
	var result runResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		return runResult{}, fmt.Errorf("decode old runner output %q: %w", stdout.String(), err)
	}
	return result, nil
}

func runOldWorkerAdapterReset(t *testing.T, payload map[string]any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"mode": "worker-adapter-reset", "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "run.mjs")
	command.Dir = "old-engine"
	command.Stdin = bytes.NewReader(encoded)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("pinned Blok 2.5.0 worker adapter probe failed: %v\nstderr: %s\nstdout: %s", err, stderr.String(), stdout.String())
	}
	return bytes.TrimSpace(stdout.Bytes())
}

func runOldWorkerTriggerRetry(t *testing.T, providerURL string, tc workload) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"mode": "worker-trigger-retry", "payload": tc.Request})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "run.mjs")
	command.Dir = "old-engine"
	command.Env = append(os.Environ(), "BLOK_PARITY_PROVIDER_URL="+providerURL)
	command.Stdin = bytes.NewReader(encoded)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("pinned Blok 2.5.0 WorkerTrigger retry failed: %v\nstderr: %s\nstdout: %s", err, stderr.String(), stdout.String())
	}
	return bytes.TrimSpace(stdout.Bytes())
}

func runOldWebhookTrigger(t *testing.T, providerURL string, tc workload) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"mode": "webhook-trigger", "payload": tc.Request, "deliveries": tc.Deliveries})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "run.mjs")
	command.Dir = "old-engine"
	command.Env = append(os.Environ(), "BLOK_PARITY_PROVIDER_URL="+providerURL, "BLOK_PARITY_WEBHOOK_SECRET=whsec_"+base64.StdEncoding.EncodeToString([]byte("synthetic-webhook-signing-secret")))
	command.Stdin = bytes.NewReader(encoded)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("pinned Blok 2.5.0 webhook trigger failed: %v\nstderr: %s\nstdout: %s", err, stderr.String(), stdout.String())
	}
	return bytes.TrimSpace(stdout.Bytes())
}

func runOldSSETrigger(t *testing.T, tc workload) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"mode": "sse-trigger", "payload": tc.Request})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "run.mjs")
	command.Dir = "old-engine"
	command.Stdin = bytes.NewReader(encoded)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("pinned Blok 2.5.0 SSE trigger failed: %v\nstderr: %s\nstdout: %s", err, stderr.String(), stdout.String())
	}
	return bytes.TrimSpace(stdout.Bytes())
}

func runNewNativeEngine(t *testing.T, providerURL string, tc workload) runResult {
	t.Helper()
	inputSchema, outputSchema := schemasForOperation(tc.Operation)
	definition, err := node.Define[map[string]any, map[string]any](
		"parity-provider-call",
		"1.0.0",
		func(ctx context.Context, input map[string]any) (map[string]any, error) {
			encoded, err := json.Marshal(input)
			if err != nil {
				return nil, err
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/"+tc.Operation, bytes.NewReader(encoded))
			if err != nil {
				return nil, err
			}
			request.Header.Set("Content-Type", "application/json")
			if key, ok := input["requestKey"].(string); ok {
				request.Header.Set("idempotency-key", key)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			var body map[string]any
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				return nil, err
			}
			if response.StatusCode >= 400 {
				code, _ := body["code"].(string)
				if code == "" {
					code = "provider_error"
				}
				message, _ := body["message"].(string)
				if message == "" {
					message = code
				}
				return nil, &node.DomainError{Code: code, Class: "provider", Err: errors.New(message)}
			}
			return map[string]any{"status": response.StatusCode, "body": body}, nil
		},
		node.Description("Shared loopback provider call in the E20-T01 synthetic workload"),
		node.Schemas(inputSchema, outputSchema),
		node.Effects("network"),
	)
	if err != nil {
		t.Fatal(err)
	}
	definitionFlow, err := flow.Define(flow.Spec{Name: "parity-" + tc.Operation, Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[map[string]any]) flow.Ref[map[string]any] {
		return flow.Call(builder, "provider", definition, input)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := definitionFlow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	result, runErr := engine.New(map[string]node.Any{"parity-provider-call": definition.Any()}).Run(context.Background(), program, tc.Request)
	var output runResult
	output.Engine, output.Version, output.OK = "new-blok-native", "0.1.0-dev", runErr == nil
	if runErr == nil {
		output.Response, _ = json.Marshal(result.Output)
		return output
	}
	var classified interface{ ErrorCode() string }
	if errors.As(runErr, &classified) {
		output.Error.Code = classified.ErrorCode()
	}
	output.Error.Message = runErr.Error()
	return output
}

func runNewDurableQueueRetry(t *testing.T, provider *ledgerServer, tc workload) (int, int, json.RawMessage) {
	t.Helper()
	inputSchema, outputSchema := schemasForOperation(tc.Operation)
	providerNode, err := node.Define[map[string]any, map[string]any](
		"parity-provider-call",
		"1.0.0",
		func(ctx context.Context, input map[string]any) (map[string]any, error) {
			encoded, err := json.Marshal(input)
			if err != nil {
				return nil, err
			}
			request, err := http.NewRequestWithContext(ctx, http.MethodPost, provider.URL+"/"+tc.Operation, bytes.NewReader(encoded))
			if err != nil {
				return nil, err
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("idempotency-key", fmt.Sprint(input["requestKey"]))
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				return nil, err
			}
			defer response.Body.Close()
			var body map[string]any
			if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
				return nil, err
			}
			if response.StatusCode >= http.StatusBadRequest {
				code, _ := body["code"].(string)
				message, _ := body["message"].(string)
				return nil, &node.DomainError{Code: code, Class: "provider", Err: errors.New(message)}
			}
			return map[string]any{"status": response.StatusCode, "body": body}, nil
		},
		node.Description("Real new native engine node executed by trigger/worker.Queue on each delivery"),
		node.Schemas(inputSchema, outputSchema),
		node.Effects("network"),
	)
	if err != nil {
		t.Fatal(err)
	}
	definition, err := flow.Define(flow.Spec{Name: "parity-job-retry", Version: "1.0.0"}, func(builder *flow.Builder, request flow.Ref[map[string]any]) flow.Ref[map[string]any] {
		return flow.Call(builder, "provider", providerNode, request)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := definition.Lower()
	if err != nil {
		t.Fatal(err)
	}
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "retry.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	clock := time.Unix(1_800_000_000, 0)
	queue, err := worker.New(context.Background(), database, func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(tc.Request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(context.Background(), worker.EnqueueRequest{RequestKey: "job-retry", Kind: "order.create", Payload: payload, MaxAttempts: tc.Retry.MaxAttempts}); err != nil {
		t.Fatal(err)
	}
	var finalOutput json.RawMessage
	for attempt := 0; attempt < tc.Retry.MaxAttempts; attempt++ {
		processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, _ worker.Tx, job worker.Job) error {
			var input map[string]any
			if err := json.Unmarshal(job.Payload, &input); err != nil {
				return err
			}
			result, err := engine.New(map[string]node.Any{"parity-provider-call": providerNode.Any()}).Run(ctx, program, input)
			if err != nil {
				return &worker.HandlerError{Retryable: true, Message: err.Error()}
			}
			finalOutput, err = json.Marshal(result.Output)
			return err
		})
		if err != nil || !processed {
			t.Fatalf("new durable job attempt %d processed=%t err=%v", attempt+1, processed, err)
		}
		clock = clock.Add(time.Second)
	}
	job, err := queue.Get(context.Background(), "job-retry")
	if err != nil || job.State != worker.StateCompleted || job.Attempt != tc.Retry.MaxAttempts {
		t.Fatalf("new durable retry job=%+v err=%v", job, err)
	}
	calls, effects := provider.ledger.snapshot()
	return calls, effects, finalOutput
}

func schemasForOperation(operation string) ([]byte, []byte) {
	stringField := func() map[string]any { return map[string]any{"type": "string"} }
	integerField := func() map[string]any { return map[string]any{"type": "integer"} }
	inputProperties := map[string]any{"requestKey": stringField()}
	inputRequired := []string{"requestKey"}
	outputProperties := map[string]any{}
	outputRequired := []string{}
	switch operation {
	case "quote":
		inputProperties["sku"], inputProperties["quantity"] = stringField(), integerField()
		inputRequired = append(inputRequired, "sku", "quantity")
		outputProperties = map[string]any{"sku": stringField(), "quantity": integerField(), "totalCents": integerField(), "currency": stringField()}
		outputRequired = []string{"sku", "quantity", "totalCents", "currency"}
	case "order":
		inputProperties["sku"], inputProperties["quantity"] = stringField(), integerField()
		inputRequired = append(inputRequired, "sku", "quantity")
		outputProperties = map[string]any{"requestKey": stringField(), "sku": stringField(), "quantity": integerField(), "totalCents": integerField()}
		outputRequired = []string{"requestKey", "sku", "quantity", "totalCents"}
	case "job-retry":
		inputProperties["jobId"], inputProperties["sku"], inputProperties["quantity"] = stringField(), stringField(), integerField()
		inputRequired = append(inputRequired, "jobId", "sku", "quantity")
		outputProperties = map[string]any{"jobId": stringField(), "state": stringField(), "totalCents": integerField()}
		outputRequired = []string{"jobId", "state", "totalCents"}
	default:
		outputProperties = map[string]any{}
	}
	wrap := func(properties map[string]any, required []string) []byte {
		encoded, _ := json.Marshal(map[string]any{"type": "object", "properties": properties, "required": required})
		return encoded
	}
	bodySchema := map[string]any{"type": "object", "properties": outputProperties, "required": outputRequired}
	responseProperties := map[string]any{"status": integerField(), "body": bodySchema}
	return wrap(inputProperties, inputRequired), wrap(responseProperties, []string{"status", "body"})
}

func newProvider(t *testing.T) *ledgerServer {
	t.Helper()
	ledger := &providerLedger{seen: map[string]bool{}, attempt: map[string]int{}, gates: map[string]*providerGate{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var payload map[string]any
		if err := json.NewDecoder(io.LimitReader(request.Body, 1<<20)).Decode(&payload); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		operation := strings.TrimPrefix(request.URL.Path, "/")
		key := request.Header.Get("idempotency-key")
		ledger.mu.Lock()
		ledger.calls++
		ledger.attempt[operation+"\x00"+key]++
		attempt := ledger.attempt[operation+"\x00"+key]
		if operation == "job-retry" && attempt == 1 {
			ledger.mu.Unlock()
			writeProviderJSON(w, http.StatusServiceUnavailable, map[string]any{"code": "temporary_provider_error", "message": "synthetic first-attempt failure"})
			return
		}
		if operation == "quote" && payload["sku"] != "coffee" {
			ledger.mu.Unlock()
			writeProviderJSON(w, http.StatusUnprocessableEntity, map[string]any{"code": "unknown_sku", "message": "synthetic unknown sku"})
			return
		}
		if key == "" || !ledger.seen[key] {
			ledger.effects++
			if key != "" {
				ledger.seen[key] = true
			}
		}
		ledger.mu.Unlock()
		if operation == "job-retry" && attempt == 2 {
			ledger.mu.Lock()
			gate := ledger.gates[key]
			if gate != nil && !gate.used {
				gate.used = true
			} else {
				gate = nil
			}
			ledger.mu.Unlock()
			if gate != nil {
				gate.waitForCall()
			}
		}

		switch operation {
		case "quote":
			quantity := int(payload["quantity"].(float64))
			writeProviderJSON(w, http.StatusOK, map[string]any{"sku": "coffee", "quantity": quantity, "totalCents": 1500 * quantity, "currency": "USD"})
		case "order":
			writeProviderJSON(w, http.StatusOK, map[string]any{"requestKey": key, "sku": payload["sku"], "quantity": payload["quantity"], "totalCents": 3000})
		case "job-retry":
			writeProviderJSON(w, http.StatusOK, map[string]any{"jobId": payload["jobId"], "state": "completed", "totalCents": 3000})
		default:
			writeProviderJSON(w, http.StatusOK, payload)
		}
	}))
	t.Cleanup(server.Close)
	return &ledgerServer{Server: server, ledger: ledger}
}

type ledgerServer struct {
	*httptest.Server
	ledger *providerLedger
}

func writeProviderJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func (l *providerLedger) snapshot() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.calls, l.effects
}

func assertRun(t *testing.T, got runResult, want expectedRun, ledger *providerLedger) {
	t.Helper()
	calls, effects := ledger.snapshot()
	if got.OK != want.OK || got.Error.Code != want.ErrorCode || (want.ErrorMessage != "" && got.Error.Message != want.ErrorMessage) || calls != want.ProviderCalls || effects != want.CommittedEffects {
		t.Fatalf("got engine=%s version=%s ok=%t error=%q message=%q calls=%d effects=%d response=%s steps=%+v; want %+v", got.Engine, got.Version, got.OK, got.Error.Code, got.Error.Message, calls, effects, got.Response, got.Steps, want)
	}
}

func equalJSON(left, right json.RawMessage) bool {
	var leftValue, rightValue any
	if json.Unmarshal(left, &leftValue) != nil || json.Unmarshal(right, &rightValue) != nil {
		return bytes.Equal(left, right)
	}
	leftBytes, _ := json.Marshal(leftValue)
	rightBytes, _ := json.Marshal(rightValue)
	return bytes.Equal(leftBytes, rightBytes)
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
