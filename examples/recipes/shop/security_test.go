package shop

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

// startedRecipe opens a recipe on a fresh database, starts it and serves its
// handler. Everything is torn down when the test ends.
func startedRecipe(t *testing.T, name string) (*Application, *httptest.Server) {
	t.Helper()
	application, closeDatabase := openRecipe(t, filepath.Join(t.TempDir(), name))
	if err := application.Start(context.Background()); err != nil {
		closeDatabase()
		t.Fatal(err)
	}
	server := httptest.NewServer(application.Handler)
	t.Cleanup(func() {
		server.Close()
		_ = application.Shutdown(context.Background())
		closeDatabase()
	})
	return application, server
}

func startStream(t *testing.T, client *http.Client, baseURL, token, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/jobs/stream", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", key)
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result struct {
		Stream string `json:"stream"`
	}
	_ = json.NewDecoder(response.Body).Decode(&result)
	return response.StatusCode, result.Stream
}

// readStream follows a stream for at most wait and returns whatever arrived.
// A stream that never finishes is cut off by the deadline, not failed.
func readStream(t *testing.T, client *http.Client, baseURL, token, streamID string, wait time.Duration) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/jobs/stream/"+streamID, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(req)
	if err != nil {
		return 0, ""
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(response.Body)
	return response.StatusCode, string(data)
}

func processAll(t *testing.T, application *Application) int {
	t.Helper()
	processed := 0
	for range 16 {
		ok, err := application.ProcessOne(context.Background())
		if err != nil {
			t.Fatalf("process job: %v", err)
		}
		if !ok {
			return processed
		}
		processed++
	}
	t.Fatal("queue did not drain")
	return processed
}

func jobPrincipal(t *testing.T, application *Application, requestKey string) string {
	t.Helper()
	job, err := application.Queue.Get(context.Background(), requestKey)
	if err != nil {
		t.Fatalf("get job %q: %v", requestKey, err)
	}
	return job.Principal.ID
}

// A caller-chosen /jobs key must not occupy a webhook event's durable
// identity: otherwise a signed provider delivery is refused forever.
func TestJobKeysCannotSquatWebhookEvents(t *testing.T) {
	application, server := startedRecipe(t, "squat-webhook.db")
	expected := fixture(t, "job-key-squats-webhook")
	webhookKey := webhook.SubmissionKey("synthetic", "evt-9")
	squat := request(t, server.Client(), http.MethodPost, server.URL+"/jobs", bobToken, `{"requestKey":"`+webhookKey+`","recordId":"bob-squat","value":"squatted"}`)
	if squat.StatusCode != http.StatusOK {
		t.Fatalf("bob job status=%d body=%s", squat.StatusCode, responseBody(t, squat))
	}
	_ = squat.Body.Close()
	body := `{"requestKey":"hook:evt-9","recordId":"webhook-record-9","value":"signed"}`
	delivery := sendWebhook(t, server.Client(), server.URL, "evt-9", []byte(body))
	if delivery.StatusCode != expected.ExpectedStatus {
		t.Fatalf("signed delivery status=%d body=%s, want %d", delivery.StatusCode, responseBody(t, delivery), expected.ExpectedStatus)
	}
	_ = delivery.Body.Close()
	if count := jobCount(t, application, webhookKey); count != expected.ExpectedAccepted {
		t.Fatalf("jobs under the webhook key=%d, want %d", count, expected.ExpectedAccepted)
	}
	if owner := jobPrincipal(t, application, webhookKey); owner != "provider:synthetic" {
		t.Fatalf("webhook key owned by %q, want provider:synthetic", owner)
	}
}

// Bob can compute Alice's SSE submission key (it is sha256 of a public
// principal ID). Posting it to /jobs must neither block Alice's start nor put
// Bob's output on Alice's stream.
func TestJobKeysCannotSquatAnotherPrincipalsStream(t *testing.T) {
	application, server := startedRecipe(t, "squat-stream.db")
	expected := fixture(t, "job-key-squats-stream")
	aliceKey := sse.SubmissionKey(streamEndpoint, trigger.Principal{ID: "alice"}, "alice-stream-1")
	squat := request(t, server.Client(), http.MethodPost, server.URL+"/jobs", bobToken, `{"requestKey":"`+aliceKey+`","recordId":"bob-stream-squat","value":"bob-content"}`)
	if squat.StatusCode != http.StatusOK {
		t.Fatalf("bob job status=%d body=%s", squat.StatusCode, responseBody(t, squat))
	}
	_ = squat.Body.Close()
	status, streamID := startStream(t, server.Client(), server.URL, aliceToken, "alice-stream-1", `{"requestKey":"alice-stream-1","recordId":"alice-stream-record","value":"alice-content"}`)
	if status != expected.ExpectedStatus {
		t.Fatalf("alice stream start status=%d, want %d", status, expected.ExpectedStatus)
	}
	if owner := jobPrincipal(t, application, aliceKey); owner != "alice" {
		t.Fatalf("alice's stream key owned by %q", owner)
	}
	processAll(t, application)
	_, events := readStream(t, server.Client(), server.URL, aliceToken, streamID, 2*time.Second)
	if strings.Contains(events, "bob") {
		t.Fatalf("alice's stream carried bob's output: %s", events)
	}
	if !strings.Contains(events, `"owner":"alice"`) || !strings.Contains(events, "alice-content") {
		t.Fatalf("alice's stream did not carry her result: %q", events)
	}
	if leaks := strings.Count(events, "bob-content"); leaks != expected.ExpectedEffects {
		t.Fatalf("cross-principal stream effects=%d, want %d", leaks, expected.ExpectedEffects)
	}
}

// Even a trusted in-process producer that enqueues another principal's SSE
// key directly (bypassing the HTTP namespace) must not finish that
// principal's stream: the worker checks the job's stored principal.
func TestWorkerFinishesOnlyStreamsTheJobPrincipalOwns(t *testing.T) {
	application, server := startedRecipe(t, "stream-owner.db")
	aliceKey := sse.SubmissionKey(streamEndpoint, trigger.Principal{ID: "alice"}, "alice-stream-2")
	payload := `{"requestKey":"x","recordId":"bob-direct","value":"bob-content"}`
	if _, err := application.Queue.Enqueue(context.Background(), worker.EnqueueRequest{RequestKey: aliceKey, Kind: JobKind, Payload: []byte(payload), Principal: trigger.Principal{ID: "bob"}}); err != nil {
		t.Fatal(err)
	}
	// Alice's start conflicts with the occupied key, but her stream exists.
	status, streamID := startStream(t, server.Client(), server.URL, aliceToken, "alice-stream-2", `{"requestKey":"alice-stream-2","recordId":"alice-direct","value":"alice-content"}`)
	if status != http.StatusConflict {
		t.Fatalf("alice start over a directly occupied key status=%d, want 409", status)
	}
	if streamID == "" {
		streamID = sse.StreamID(aliceKey)
	}
	processAll(t, application)
	_, events := readStream(t, server.Client(), server.URL, aliceToken, streamID, 1500*time.Millisecond)
	if strings.Contains(events, "bob") {
		t.Fatalf("worker finished alice's stream with bob's job output: %s", events)
	}
	if _, owned := ownedStreamID(worker.Job{RequestKey: aliceKey, Principal: trigger.Principal{ID: "bob"}}); owned {
		t.Fatal("ownedStreamID accepted another principal's stream key")
	}
	if id, owned := ownedStreamID(worker.Job{RequestKey: aliceKey, Principal: trigger.Principal{ID: "alice"}}); !owned || id != sse.StreamID(aliceKey) {
		t.Fatalf("ownedStreamID refused the owner's own key: %q %v", id, owned)
	}
	for _, key := range []string{HTTPJobKey(trigger.Principal{ID: "alice"}, "k"), webhook.SubmissionKey("synthetic", "evt-1"), "sse:jobs:not-a-digest"} {
		if _, owned := ownedStreamID(worker.Job{RequestKey: key, Principal: trigger.Principal{ID: "alice"}}); owned {
			t.Fatalf("ownedStreamID accepted non-stream key %q", key)
		}
	}
}

// Creating an ID another principal already uses must look exactly like
// creating a fresh ID: record IDs are scoped by owner.
func TestCreateDoesNotRevealAnotherPrincipalsRecordIDs(t *testing.T) {
	application, server := startedRecipe(t, "create-oracle.db")
	expected := fixture(t, "create-existing-elsewhere")
	alice := request(t, server.Client(), http.MethodPost, server.URL+"/records", aliceToken, `{"id":"secret-a1","value":"alice-secret"}`)
	if alice.StatusCode != http.StatusOK {
		t.Fatalf("alice create status=%d", alice.StatusCode)
	}
	_ = alice.Body.Close()
	type outcome struct {
		status int
		record Record
	}
	create := func(id string) outcome {
		response := request(t, server.Client(), http.MethodPost, server.URL+"/records", bobToken, `{"id":"`+id+`","value":"bob-value"}`)
		defer response.Body.Close()
		var record Record
		_ = json.NewDecoder(response.Body).Decode(&record)
		return outcome{status: response.StatusCode, record: record}
	}
	elsewhere, fresh := create("secret-a1"), create("fresh-b1")
	if elsewhere.status != expected.ExpectedStatus || fresh.status != expected.ExpectedStatus {
		t.Fatalf("create statuses: existing-elsewhere=%d fresh=%d, want both %d", elsewhere.status, fresh.status, expected.ExpectedStatus)
	}
	if elsewhere.record.Owner != "bob" || fresh.record.Owner != "bob" || elsewhere.record.Value != fresh.record.Value {
		t.Fatalf("create bodies differ: existing-elsewhere=%+v fresh=%+v", elsewhere.record, fresh.record)
	}
	read := request(t, server.Client(), http.MethodGet, server.URL+"/records/secret-a1", aliceToken, "")
	var aliceRecord Record
	_ = json.NewDecoder(read.Body).Decode(&aliceRecord)
	_ = read.Body.Close()
	if aliceRecord.Value != "alice-secret" || aliceRecord.Owner != "alice" {
		t.Fatalf("bob's create changed alice's record: %+v", aliceRecord)
	}
	if records, _ := recipeCounts(t, application); records != expected.ExpectedRecords {
		t.Fatalf("records=%d, want %d", records, expected.ExpectedRecords)
	}
}

// create → delete → create of one ID succeeds, and each incarnation gets its
// own stable, deduplicable outbox event.
func TestDeletedRecordCanBeRecreated(t *testing.T) {
	application, server := startedRecipe(t, "recreate.db")
	expected := fixture(t, "recreate-after-delete")
	for step, call := range []struct{ method, path, body string }{
		{http.MethodPost, "/records", `{"id":"again-1","value":"first"}`},
		{http.MethodDelete, "/records/again-1", ""},
		{http.MethodPost, "/records", `{"id":"again-1","value":"second"}`},
	} {
		response := request(t, server.Client(), call.method, server.URL+call.path, aliceToken, call.body)
		if response.StatusCode != expected.ExpectedStatus {
			t.Fatalf("step %d %s %s status=%d body=%s, want %d", step, call.method, call.path, response.StatusCode, responseBody(t, response), expected.ExpectedStatus)
		}
		_ = response.Body.Close()
	}
	var eventIDs []string
	if err := application.Database.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT event_id FROM shop_outbox ORDER BY created_at`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			eventIDs = append(eventIDs, id)
		}
		return rows.Err()
	}); err != nil {
		t.Fatal(err)
	}
	if len(eventIDs) != expected.ExpectedOutbox || eventIDs[0] == eventIDs[1] {
		t.Fatalf("outbox event IDs=%q, want %d distinct", eventIDs, expected.ExpectedOutbox)
	}
	for range eventIDs {
		if _, err := application.DrainOutbox(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if sent := sinkCount(t, application); sent != expected.ExpectedEffects {
		t.Fatalf("sink events=%d, want %d", sent, expected.ExpectedEffects)
	}
}

// Update idempotency keys belong to the caller: Bob's key k must not block
// Alice's key k.
func TestUpdateIdempotencyKeysAreScopedPerPrincipal(t *testing.T) {
	_, server := startedRecipe(t, "update-keys.db")
	expected := fixture(t, "update-key-per-principal")
	for _, call := range []struct{ token, method, path, body string }{
		{bobToken, http.MethodPost, "/records", `{"id":"bob-1","value":"b"}`},
		{bobToken, http.MethodPut, "/records/bob-1", `{"requestKey":"k","value":"bob-updated"}`},
		{aliceToken, http.MethodPost, "/records", `{"id":"alice-1","value":"a"}`},
		{aliceToken, http.MethodPut, "/records/alice-1", `{"requestKey":"k","value":"alice-updated"}`},
	} {
		response := request(t, server.Client(), call.method, server.URL+call.path, call.token, call.body)
		if response.StatusCode != expected.ExpectedStatus {
			t.Fatalf("%s %s status=%d body=%s, want %d", call.method, call.path, response.StatusCode, responseBody(t, response), expected.ExpectedStatus)
		}
		_ = response.Body.Close()
	}
	read := request(t, server.Client(), http.MethodGet, server.URL+"/records/alice-1", aliceToken, "")
	var record Record
	_ = json.NewDecoder(read.Body).Decode(&record)
	_ = read.Body.Close()
	if record.Value != expected.ExpectedValue {
		t.Fatalf("alice record value=%q, want %q", record.Value, expected.ExpectedValue)
	}
}

func sinkCount(t *testing.T, application *Application) int {
	t.Helper()
	sink := application.publisher.(*SyntheticSink)
	var count int
	if err := sink.database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT COUNT(*) FROM synthetic_sink_events`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

// failingPublisher never accepts an event, like a receiver that is down.
type failingPublisher struct{ calls atomic.Int32 }

func (p *failingPublisher) Publish(context.Context, string, []byte) error {
	p.calls.Add(1)
	return errors.New("synthetic receiver unavailable")
}

// Delivery is retried a bounded number of times; the event is then parked as
// 'dead', reported once with ErrOutboxDead and never published again.
func TestOutboxRetriesAreBoundedAndDeadLettered(t *testing.T) {
	expected := fixture(t, "outbox-dead")
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "dead.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	publisher := &failingPublisher{}
	const lease = 100 * time.Millisecond
	application, err := New(context.Background(), Config{
		Database: database, Tokens: map[string]string{"alice": aliceToken, "bob": bobToken},
		WebhookKey: []byte(webhookKey), Publisher: publisher,
		OutboxLease: lease, OutboxMaxAttempts: expected.ExpectedPublishAttempts,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	created := request(t, server.Client(), http.MethodPost, server.URL+"/records", aliceToken, `{"id":"undeliverable","value":"v"}`)
	if created.StatusCode != http.StatusOK {
		t.Fatalf("create status=%d", created.StatusCode)
	}
	_ = created.Body.Close()
	var deadReported int
	for range expected.ExpectedPublishAttempts + 3 {
		_, err := application.DrainOutbox(context.Background())
		if errors.Is(err, ErrOutboxDead) {
			deadReported++
			if !strings.Contains(err.Error(), "record.created:") {
				t.Fatalf("dead report does not name the event: %v", err)
			}
		}
		time.Sleep(lease + 30*time.Millisecond)
	}
	if calls := int(publisher.calls.Load()); calls != expected.ExpectedPublishAttempts {
		t.Fatalf("publish attempts=%d, want %d", calls, expected.ExpectedPublishAttempts)
	}
	if deadReported != 1 {
		t.Fatalf("ErrOutboxDead reported %d times, want 1", deadReported)
	}
	var state string
	var attempts int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT state, attempts FROM shop_outbox`).Scan(&state, &attempts)
	}); err != nil {
		t.Fatal(err)
	}
	if state != expected.ExpectedOutboxState || attempts != expected.ExpectedPublishAttempts {
		t.Fatalf("outbox state=%q attempts=%d, want %q and %d", state, attempts, expected.ExpectedOutboxState, expected.ExpectedPublishAttempts)
	}
	if _, err := New(context.Background(), Config{Database: database, Tokens: map[string]string{"alice": aliceToken, "bob": bobToken}, WebhookKey: []byte(webhookKey), Publisher: publisher, OutboxMaxAttempts: maxOutboxAttempts + 1}); err == nil {
		t.Fatal("New accepted an unbounded outbox attempt count")
	}
}

// parkedWriter is an http.ResponseWriter whose first write (the stream's
// opening frame) succeeds and whose later writes park until a write deadline
// that has already passed is set, which is how the SSE server interrupts a
// subscriber it disconnects. It makes the subscriber's queue the only buffer
// between the hub and the client, independent of kernel socket buffers.
type parkedWriter struct {
	header    http.Header
	mu        sync.Mutex
	writes    int
	opened    chan struct{}
	parked    chan struct{}
	interrupt chan struct{}
	once      sync.Once
}

func newParkedWriter() *parkedWriter {
	return &parkedWriter{header: http.Header{}, opened: make(chan struct{}), parked: make(chan struct{}), interrupt: make(chan struct{})}
}

func (w *parkedWriter) Header() http.Header { return w.header }
func (w *parkedWriter) WriteHeader(int)     {}
func (w *parkedWriter) Flush()              {}

func (w *parkedWriter) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		w.once.Do(func() { close(w.interrupt) })
	}
	return nil
}

func (w *parkedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	w.writes++
	n := w.writes
	w.mu.Unlock()
	switch n {
	case 1:
		close(w.opened)
		return len(data), nil
	case 2:
		close(w.parked)
	}
	<-w.interrupt
	return 0, os.ErrDeadlineExceeded
}

// The number of events a stalled subscriber may have waiting is exactly the
// endpoint's configured QueueDepth, the fixture's maxSubscriberQueue: one
// more is a slow-subscriber disconnect.
func TestSSESlowSubscriberQueueIsTheConfiguredBound(t *testing.T) {
	application, server := startedRecipe(t, "pinned-queue.db")
	expected := fixture(t, "slow-subscriber")
	status, streamID := startStream(t, server.Client(), server.URL, aliceToken, "pinned-queue-job", `{"requestKey":"pinned-queue-job","recordId":"pinned-queue-record","value":"waiting"}`)
	if status != http.StatusAccepted {
		t.Fatalf("SSE start status=%d", status)
	}
	writer := newParkedWriter()
	subscribe := httptest.NewRequest(http.MethodGet, "/jobs/stream/"+streamID, nil)
	subscribe.Header.Set("Authorization", "Bearer "+aliceToken)
	served := make(chan struct{})
	go func() {
		defer close(served)
		application.SSE.ServeHTTP(writer, subscribe)
	}()
	waitFor := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s", what)
		}
	}
	waitFor(writer.opened, "the stream's opening frame")
	before := application.Hub.Stats().SlowSubscribers
	// The first event is taken by the subscriber, which then parks writing it.
	if _, err := application.Hub.Publish(streamID, sse.Event{Type: "progress", Data: []byte(`{"n":0}`)}); err != nil {
		t.Fatal(err)
	}
	waitFor(writer.parked, "the subscriber to park in a write")
	queued := 0
	for application.Hub.Stats().SlowSubscribers == before {
		if queued > 1100 {
			t.Fatal("subscriber was never disconnected")
		}
		if _, err := application.Hub.Publish(streamID, sse.Event{Type: "progress", Data: []byte(`{"n":1}`)}); err != nil {
			t.Fatal(err)
		}
		queued++
	}
	// queued counts the publishes that fit plus the one that overflowed.
	if waiting := queued - 1; waiting != expected.MaxSubscriberQueue {
		t.Fatalf("stalled subscriber held %d waiting events before disconnect, want the configured bound %d", waiting, expected.MaxSubscriberQueue)
	}
	waitFor(served, "the subscription handler to return")
	select {
	case reason := <-application.streamClosures:
		if reason != expected.ExpectedClose {
			t.Fatalf("close reason=%q, want %q", reason, expected.ExpectedClose)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no close reason reported")
	}
}
