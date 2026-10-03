package trigger_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"os"
	"os/exec"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	"github.com/well-prado/new-blok/trigger/pubsub"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
	"github.com/well-prado/new-blok/trigger/webhook"
	"github.com/well-prado/new-blok/trigger/worker"
)

// initialize is a raw MCP initialize request, for checking refusals.
const initialize = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"late","version":"1"}}}`

// TestNineTriggersShareOneApplication drives one real request through each
// of the nine triggers in one application, follows the SSE result live,
// offers every durable source a duplicate, stops the application, requires
// every trigger to refuse or stop with it, restarts over the same store and
// requires no goroutine to outlive it.
func TestNineTriggersShareOneApplication(t *testing.T) {
	baseline := goroutineSet()
	n := newNine(t, 1)
	t.Logf("pubsub broker: %s", n.broker.name())
	ctx := context.Background()

	// In-band triggers answer with the workflow's output.
	code, _, body := n.post("/quotes", auth("alice"), order)
	if code != http.StatusOK {
		t.Fatalf("http: %d %s", code, body)
	}
	cents(t, "http", body)
	reply, err := n.callGRPC(ctx, "alice")
	if err != nil || n.grpcCents(reply) != wantCents {
		t.Fatalf("grpc: %v %v", reply, err)
	}
	socket, _, err := n.dialWS(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	output, err := wsQuote(ctx, socket, "q-1")
	if err != nil {
		t.Fatalf("websocket: %v", err)
	}
	cents(t, "websocket", output)
	_ = socket.Close(websocket.StatusNormalClosure, "")
	session, err := n.mcpSession(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if output, err = mcpQuote(ctx, session); err != nil {
		t.Fatalf("mcp: %v", err)
	}
	cents(t, "mcp", output)
	_ = session.Close()

	// Durable triggers commit a submission; the worker pool is held until
	// the SSE stream is followed, so its result arrives live.
	if status, _, body := n.post("/webhooks/shop", n.signed("evt-1"), order); status != http.StatusAccepted {
		t.Fatalf("webhook: %d %s", status, body)
	}
	code, _, body = n.sseStart("alice", "order-1")
	var started struct {
		Stream string `json:"stream"`
	}
	if err := json.Unmarshal(body, &started); err != nil || code != http.StatusAccepted {
		t.Fatalf("sse start: %d %s", code, body)
	}
	live := n.subscribe(ctx, "alice", started.Stream)
	n.clock.set(occurrence)
	select {
	case results := <-n.cronRuns:
		if len(results) != 1 || len(results[0].Submitted) != 1 {
			t.Fatalf("cron: %+v", results)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cron never fired its occurrence")
	}
	n.broker.publish(t, "m-1", []byte(order))
	n.broker.settled(t)
	if enqueued, err := n.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "worker:quote-1", Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || !enqueued.Accepted {
		t.Fatalf("worker: %+v %v", enqueued, err)
	}
	keys := map[string]string{
		"webhook": webhook.SubmissionKey("shop", "evt-1"),
		"sse":     sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "order-1"),
		"cron":    cron.SubmissionKey("nightly-quote", occurrence),
		"pubsub":  pubsub.SubmissionKey("orders", n.broker.messageID("m-1")),
		"worker":  "worker:quote-1",
	}
	close(n.begin)
	n.waitSettled(slices.Collect(maps.Values(keys))...)
	for via, key := range keys {
		cents(t, via, n.output(key))
	}
	select {
	case data := <-live:
		cents(t, "sse stream", data)
	case <-time.After(10 * time.Second):
		t.Fatal("the SSE result never reached the live subscriber")
	}

	// Every durable source deduplicates a repeat at the shared port.
	if status, _, body := n.post("/webhooks/shop", n.signed("evt-1"), order); status != http.StatusOK {
		t.Fatalf("webhook duplicate: %d %s", status, body)
	}
	code, _, body = n.sseStart("alice", "order-1")
	if code != http.StatusOK || !strings.Contains(string(body), `"duplicate":true`) {
		t.Fatalf("sse duplicate: %d %s", code, body)
	}
	n.broker.publish(t, "m-1", []byte(order))
	n.broker.settled(t)
	// The port deduplicates a repeated occurrence: the very submission cron
	// committed (same key, normalized payload and principal) is a duplicate.
	// That cron itself never repeats one is proven by the restart in
	// TestHostShutdownOrderAndRestart.
	committed, err := n.queue.Get(ctx, keys["cron"])
	if err != nil {
		t.Fatal(err)
	}
	if accepted, err := n.queue.Submit(ctx, trigger.Submission{Key: keys["cron"], Kind: "quote.cron", Payload: committed.Payload, Principal: committed.Principal}); err != nil || accepted {
		t.Fatalf("cron duplicate: accepted=%v err=%v", accepted, err)
	}
	if enqueued, err := n.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "worker:quote-1", Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || enqueued.Accepted {
		t.Fatalf("worker duplicate: %+v %v", enqueued, err)
	}
	once := map[string]int{"http": 1, "grpc": 1, "websocket": 1, "mcp": 1, "webhook": 1, "sse": 1, "cron": 1, "pubsub": 1, "worker": 1}
	if got := n.flow.counts(); !maps.Equal(got, once) {
		t.Fatalf("workflow runs by trigger %v, want %v", got, once)
	}
	if errs := n.workerErrs.Load(); errs != 0 {
		t.Fatalf("the worker pool saw %d errors; first %v", errs, n.firstErr.Load())
	}

	// Stop the application. Every application-gated trigger refuses, and
	// cron, pubsub and the worker pool stop with it.
	if err := n.app.Shutdown(ctx); err != nil {
		t.Fatalf("application shutdown: %v", err)
	}
	fetches := n.broker.fetches()
	refusals := map[string][2]string{}
	record := func(via string, code int, retry string) { refusals[via] = [2]string{fmt.Sprint(code), retry} }
	code, retry, _ := n.post("/quotes", auth("alice"), order)
	record("http", code, retry)
	code, retry, _ = n.post("/webhooks/shop", n.signed("evt-2"), order)
	record("webhook", code, retry)
	code, retry, _ = n.sseStart("alice", "order-2")
	record("sse", code, retry)
	headers := auth("alice")
	headers["Accept"] = "application/json, text/event-stream"
	code, retry, _ = n.post("/mcp", headers, initialize)
	record("mcp", code, retry)
	if _, response, err := n.dialWS(ctx, "alice"); err == nil {
		t.Fatal("websocket: a stopped application accepted a connection")
	} else if response != nil {
		record("websocket", response.StatusCode, response.Header.Get("Retry-After"))
	}
	for _, via := range []string{"http", "webhook", "sse", "mcp", "websocket"} {
		got, ok := refusals[via]
		if !ok || got[0] != "503" {
			t.Errorf("%s: a stopped application answered %v, want 503", via, got)
		}
		if got[1] != "1" {
			t.Errorf("%s: refusal without Retry-After: %v", via, got)
		}
	}
	if _, err := n.callGRPC(ctx, "alice"); status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "unavailable" {
		t.Errorf("grpc: a stopped application answered %v, want Unavailable from the adapter", err)
	}
	n.broker.publish(t, "m-late", []byte(order))
	n.clock.set(time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))
	if enqueued, err := n.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "worker:after-stop", Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || !enqueued.Accepted {
		t.Fatalf("enqueue after stop: %+v %v", enqueued, err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := n.queue.Get(ctx, pubsub.SubmissionKey("orders", n.broker.messageID("m-late"))); !errors.Is(err, worker.ErrNotFound) {
		t.Errorf("pubsub consumed after the application stopped: %v", err)
	}
	if fetches >= 0 && n.broker.fetches() != fetches {
		t.Errorf("pubsub fetched after the application stopped (%d → %d)", fetches, n.broker.fetches())
	}
	if _, err := n.queue.Get(ctx, cron.SubmissionKey("nightly-quote", time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC))); !errors.Is(err, worker.ErrNotFound) {
		t.Errorf("cron submitted after the application stopped: %v", err)
	}
	if job, err := n.queue.Get(ctx, "worker:after-stop"); err != nil || job.State != worker.StatePending {
		t.Errorf("the worker pool ran after the application stopped: %+v %v", job, err)
	}
	if got := n.flow.counts(); !maps.Equal(got, once) {
		t.Errorf("the workflow ran after the stop: %v", got)
	}
	n.stop()

	// Restart over the same store: committed work stays settled, repeats
	// stay duplicates, and the job left pending runs.
	database, err := (sqlite.Backend{}).Open(ctx, n.path)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := worker.New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	for via, key := range keys {
		if job, err := queue.Get(ctx, key); err != nil || job.State != worker.StateCompleted {
			t.Errorf("%s after restart: %+v %v", via, job, err)
		}
	}
	if err := queue.RegisterKind("quote.worker", quote.InputSchema); err != nil {
		t.Fatal(err)
	}
	if enqueued, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "worker:quote-1", Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || enqueued.Accepted {
		t.Errorf("a repeat after restart was accepted: %+v %v", enqueued, err)
	}
	ran := ""
	if processed, err := queue.ProcessOnce(ctx, func(_ context.Context, _ worker.Tx, job worker.Job) error { ran = job.RequestKey; return nil }); err != nil || !processed || ran != "worker:after-stop" {
		t.Errorf("the pending job after restart: processed=%v ran=%q err=%v", processed, ran, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	noLeaks(t, baseline)
}

// TestNineTriggersDrainTogether holds work in flight on six triggers at
// once: an HTTP request, a gRPC call, a WebSocket message, an MCP call, a
// webhook submission and an SSE submission. The application must keep
// draining while they run, refuse new work on every trigger meanwhile, let
// every held request complete, and commit the durable ones.
func TestNineTriggersDrainTogether(t *testing.T) {
	baseline := goroutineSet()
	n := newNine(t, 1)
	ctx := context.Background()
	c := n.clients(ctx)
	defer c.close()
	points := slices.Collect(maps.Values(gated))
	n.gate.holding(points...)
	outcomes := map[string]<-chan error{}
	for via := range gated {
		outcomes[via] = n.request(ctx, c, via, "held")
	}
	n.waitEntered(points...)
	drained := make(chan error, 1)
	go func() { drained <- n.app.Shutdown(context.Background()) }()
	n.waitDraining()
	if status, retry, _ := n.post("/quotes", auth("bob"), order); status != http.StatusServiceUnavailable || retry != "1" {
		t.Errorf("http while draining: %d Retry-After %q", status, retry)
	}
	if status, retry, _ := n.post("/webhooks/shop", n.signed("evt-late"), order); status != http.StatusServiceUnavailable || retry != "1" {
		t.Errorf("webhook while draining: %d Retry-After %q", status, retry)
	}
	if status, retry, _ := n.sseStart("bob", "late"); status != http.StatusServiceUnavailable || retry != "1" {
		t.Errorf("sse while draining: %d Retry-After %q", status, retry)
	}
	headers := auth("bob")
	headers["Accept"] = "application/json, text/event-stream"
	if status, retry, body := n.post("/mcp", headers, initialize); status != http.StatusServiceUnavailable || retry != "1" || strings.TrimSpace(string(body)) != "unavailable" {
		t.Errorf("mcp while draining: %d Retry-After %q %s", status, retry, body)
	}
	if _, response, err := n.dialWS(ctx, "bob"); err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("websocket while draining: %v %v", response, err)
	}
	if _, err := n.callGRPC(ctx, "bob"); status.Code(err) != codes.Unavailable || status.Convert(err).Message() != "unavailable" {
		t.Errorf("grpc while draining: %v", err)
	}
	if state := n.app.State(); state != app.DrainingState {
		t.Fatalf("the application is %s with six requests in flight; want draining", state)
	}
	close(n.gate.release)
	for via, outcome := range outcomes {
		select {
		case err := <-outcome:
			if err != nil {
				t.Errorf("%s did not complete: %v", via, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never completed", via)
		}
	}
	if err := <-drained; err != nil {
		t.Fatalf("drain: %v", err)
	}
	for via, key := range map[string]string{"webhook": webhook.SubmissionKey("shop", "held"), "sse": sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "held")} {
		if _, err := n.queue.Get(ctx, key); err != nil {
			t.Errorf("%s was answered but not committed: %v", via, err)
		}
	}
	if errs := n.workerErrs.Load(); errs != 0 {
		t.Fatalf("the worker pool saw %d errors; first %v", errs, n.firstErr.Load())
	}
	c.close()
	n.stop()
	noLeaks(t, baseline)
}

// TestEachTriggerAloneHoldsTheApplication holds work on one gated trigger
// at a time: that work alone must keep the application draining until it
// completes. Holding all six at once cannot show this, because any one of
// them keeps the application open for the rest.
func TestEachTriggerAloneHoldsTheApplication(t *testing.T) {
	for _, via := range slices.Sorted(maps.Keys(gated)) {
		t.Run(via, func(t *testing.T) {
			n := newNine(t, 1)
			ctx := context.Background()
			c := n.clients(ctx)
			defer c.close()
			n.gate.holding(gated[via])
			outcome := n.request(ctx, c, via, "alone")
			n.waitEntered(gated[via])
			drained := make(chan error, 1)
			go func() { drained <- n.app.Shutdown(context.Background()) }()
			n.waitDraining()
			time.Sleep(100 * time.Millisecond)
			if state := n.app.State(); state != app.DrainingState {
				t.Fatalf("with only %s in flight the application is %s; want draining", via, state)
			}
			close(n.gate.release)
			if err := <-outcome; err != nil {
				t.Fatalf("%s did not complete: %v", via, err)
			}
			if err := <-drained; err != nil {
				t.Fatalf("drain: %v", err)
			}
		})
	}
}

// TestHostShutdownOrderAndRestart shuts a host down in the order ADR 0005
// documents, with work in flight, then restarts it over the same store:
//   - step 1 (the adapters): held HTTP, gRPC, MCP, webhook and SSE work
//     completes and is answered while the application stays ready, and the
//     long-lived WebSocket connection is closed as going away;
//   - step 2 (the application): a worker job in flight is canceled with its
//     consumer, rolled back and left pending without spending an attempt;
//   - after the restart, that job runs exactly once, repeats of committed
//     webhook and SSE work are duplicates, a recreated stream of settled
//     work expires, and cron does not fire an occurrence it already fired.
func TestHostShutdownOrderAndRestart(t *testing.T) {
	baseline := goroutineSet()
	n := newNine(t, 1)
	ctx := context.Background()
	close(n.begin)

	// Cron fires its occurrence and the job completes.
	n.clock.set(occurrence)
	select {
	case results := <-n.cronRuns:
		if len(results) != 1 || len(results[0].Submitted) != 1 {
			t.Fatalf("cron: %+v", results)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("cron never fired its occurrence")
	}
	n.waitSettled(cron.SubmissionKey("nightly-quote", occurrence))

	// Step 1 with HTTP, gRPC, MCP, webhook and SSE work held.
	c := n.clients(ctx)
	defer c.close()
	held := []string{"http", "grpc", "mcp", "webhook", "sse"}
	points := make([]string, 0, len(held))
	outcomes := map[string]<-chan error{}
	for _, via := range held {
		points = append(points, gated[via])
	}
	n.gate.holding(points...)
	for _, via := range held {
		outcomes[via] = n.request(ctx, c, via, "in-flight")
	}
	n.waitEntered(points...)
	// The WebSocket client reads, as a real one does, so it answers the
	// server's closing handshake.
	wsClosed := make(chan error, 1)
	go func() {
		_, _, err := c.socket.Read(ctx)
		wsClosed <- err
	}()
	adapters := make(chan struct{})
	go func() { defer close(adapters); n.stopAdapters() }()
	time.Sleep(100 * time.Millisecond)
	select {
	case <-adapters:
		t.Fatal("the adapters stopped with work still held")
	default:
	}
	if state := n.app.State(); state != app.ReadyState {
		t.Fatalf("the application is %s while the adapters finish their work; want ready", state)
	}
	close(n.gate.release)
	for via, outcome := range outcomes {
		select {
		case err := <-outcome:
			if err != nil {
				t.Errorf("%s did not complete: %v", via, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s never completed", via)
		}
	}
	select {
	case <-adapters:
	case <-time.After(20 * time.Second):
		t.Fatal("the adapters never stopped")
	}
	var closing websocket.CloseError
	if err := <-wsClosed; !errors.As(err, &closing) || closing.Code != websocket.StatusGoingAway {
		t.Errorf("the WebSocket connection ended with %v; want 1001 going away", err)
	}
	webhookKey := webhook.SubmissionKey("shop", "in-flight")
	sseKey := sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "in-flight")
	n.waitSettled(webhookKey, sseKey)

	// Step 2 with a worker job in flight.
	n.gate.holdingUntilCanceled("worker")
	if enqueued, err := n.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "worker:in-flight", Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || !enqueued.Accepted {
		t.Fatalf("worker: %+v %v", enqueued, err)
	}
	n.waitEntered("worker")
	n.stopped = true
	n.stopApplication()
	once := map[string]int{"cron": 1, "http": 1, "grpc": 1, "mcp": 1, "webhook": 1, "sse": 1}
	if got := n.flow.counts(); !maps.Equal(got, once) {
		t.Fatalf("workflow runs by trigger %v, want %v (the worker job must not have run)", got, once)
	}

	// Restart over the same store.
	r := newNineAt(t, 1, n.path)
	job, err := r.queue.Get(ctx, "worker:in-flight")
	if err != nil || job.State != worker.StatePending || job.Attempt != 0 || job.Deferrals != 1 {
		t.Fatalf("the job in flight at shutdown: %+v %v; want pending at attempt 0 with one deferral", job, err)
	}
	if status, _, body := r.post("/webhooks/shop", r.signed("in-flight"), order); status != http.StatusOK {
		t.Errorf("webhook repeat after restart: %d %s", status, body)
	}
	code, _, body := r.sseStart("alice", "in-flight")
	if code != http.StatusOK || !strings.Contains(string(body), `"duplicate":true`) {
		t.Errorf("sse repeat after restart: %d %s", code, body)
	}
	if event := r.finalEvent(ctx, "alice", sse.StreamID(sseKey)); event != sse.TypeExpired {
		t.Errorf("the recreated stream of settled work ended with %q; want %s", event, sse.TypeExpired)
	}
	// The job deferred at shutdown is redelivered after its backoff; make
	// it due now.
	if err := r.database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET available_at = 0 WHERE state = ?`, worker.StatePending)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	r.clock.set(occurrence)
	close(r.begin)
	r.waitSettled("worker:in-flight")
	// Cron's cursor is past the occurrence it fired before the restart.
	time.Sleep(200 * time.Millisecond)
	for drained := false; !drained; {
		select {
		case results := <-r.cronRuns:
			for _, result := range results {
				if len(result.Submitted) != 0 {
					t.Errorf("cron fired again after the restart: %+v", result)
				}
			}
		default:
			drained = true
		}
	}
	if got, want := r.flow.counts(), map[string]int{"worker": 1}; !maps.Equal(got, want) {
		t.Fatalf("workflow runs after the restart %v, want %v", got, want)
	}
	if errs := n.workerErrs.Load() + r.workerErrs.Load(); errs != 0 {
		t.Fatalf("the worker pools saw %d errors; first %v %v", errs, n.firstErr.Load(), r.firstErr.Load())
	}
	c.close()
	r.stop()
	noLeaks(t, baseline)
}

// TestNineTriggersUnderMixedLoad sends concurrent traffic through every
// trigger at once over the one store and a pool of workers: every request
// succeeds, every durable submission runs exactly once, and no worker sees
// an error. Clients keep at most clientConcurrency requests in flight, as
// real clients do: an unbounded burst of durable writes queues on SQLite's
// single writer past its busy timeout, which is the saturation case, not
// this one.
func TestNineTriggersUnderMixedLoad(t *testing.T) {
	baseline := goroutineSet()
	const inBandLoad, durableLoad, clientConcurrency = 8, 40, 16
	n := newNine(t, 4)
	close(n.begin)
	ctx := context.Background()
	slots := make(chan struct{}, clientConcurrency)
	var group sync.WaitGroup
	// limited runs one client request within the concurrency bound.
	limited := func(request func()) {
		defer group.Done()
		slots <- struct{}{}
		defer func() { <-slots }()
		request()
	}
	failures := make(chan string, 4*inBandLoad+4*durableLoad)
	fail := func(format string, args ...any) { failures <- fmt.Sprintf(format, args...) }
	var keys []string
	var keysMu sync.Mutex
	key := func(k string) {
		keysMu.Lock()
		keys = append(keys, k)
		keysMu.Unlock()
	}
	for i := range inBandLoad {
		user := fmt.Sprintf("user-%d", i)
		group.Add(4)
		go limited(func() {
			if status, _, body := n.post("/quotes", auth(user), order); status != http.StatusOK {
				fail("http %s: %d %s", user, status, body)
			}
		})
		go limited(func() {
			if reply, err := n.callGRPC(ctx, user); err != nil || n.grpcCents(reply) != wantCents {
				fail("grpc %s: %v", user, err)
			}
		})
		go limited(func() {
			socket, _, err := n.dialWS(ctx, user)
			if err != nil {
				fail("websocket %s: %v", user, err)
				return
			}
			defer socket.CloseNow()
			if _, err := wsQuote(ctx, socket, "load"); err != nil {
				fail("websocket %s: %v", user, err)
			}
		})
		go limited(func() {
			session, err := n.mcpSession(ctx, user)
			if err != nil {
				fail("mcp %s: %v", user, err)
				return
			}
			defer session.Close()
			if _, err := mcpQuote(ctx, session); err != nil {
				fail("mcp %s: %v", user, err)
			}
		})
	}
	for i := range durableLoad {
		user := fmt.Sprintf("user-%d", i)
		group.Add(4)
		go limited(func() {
			id := fmt.Sprintf("evt-%d", i)
			if status, _, body := n.post("/webhooks/shop", n.signed(id), order); status != http.StatusAccepted {
				fail("webhook %s: %d %s", id, status, body)
				return
			}
			key(webhook.SubmissionKey("shop", id))
		})
		go limited(func() {
			if status, _, body := n.sseStart(user, "load"); status != http.StatusAccepted {
				fail("sse %s: %d %s", user, status, body)
				return
			}
			key(sse.SubmissionKey("orders", trigger.Principal{ID: user}, "load"))
		})
		go limited(func() {
			id := fmt.Sprintf("m-%d", i)
			n.broker.publish(t, id, []byte(order))
			key(pubsub.SubmissionKey("orders", n.broker.messageID(id)))
		})
		go limited(func() {
			requestKey := fmt.Sprintf("worker:load-%d", i)
			if enqueued, err := n.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: requestKey, Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || !enqueued.Accepted {
				fail("worker %s: %+v %v", requestKey, enqueued, err)
				return
			}
			key(requestKey)
		})
	}
	group.Wait()
	close(failures)
	for failure := range failures {
		t.Error(failure)
	}
	if t.Failed() {
		t.FailNow()
	}
	n.broker.settled(t)
	n.waitSettled(keys...)
	for _, k := range keys {
		cents(t, k, n.output(k))
	}
	want := map[string]int{"http": inBandLoad, "grpc": inBandLoad, "websocket": inBandLoad, "mcp": inBandLoad, "webhook": durableLoad, "sse": durableLoad, "pubsub": durableLoad, "worker": durableLoad}
	if got := n.flow.counts(); !maps.Equal(got, want) {
		t.Fatalf("workflow runs by trigger %v, want %v", got, want)
	}
	if errs := n.workerErrs.Load(); errs != 0 {
		t.Fatalf("the worker pool saw %d errors under load; first %v", errs, n.firstErr.Load())
	}
	n.stop()
	noLeaks(t, baseline)
}

// adapters are the trigger adapters this repository ships. The test
// discovers trigger/... and requires this exact set, so a new adapter cannot
// escape the removal rules below.
var adapters = []string{"cron", "grpc", "http", "mcp", "pubsub", "pubsub/natsjs", "sse", "webhook", "websocket", "worker"}

// owners lists, for every third-party module an adapter may link, the
// adapters allowed to link it. The check is closed: a module missing here
// fails it.
var owners = map[string][]string{
	"github.com/coder/websocket":                {"websocket"},
	"github.com/nats-io/nats.go":                {"pubsub/natsjs"},
	"github.com/nats-io/nkeys":                  {"pubsub/natsjs"},
	"github.com/nats-io/nuid":                   {"pubsub/natsjs"},
	"github.com/klauspost/compress":             {"pubsub/natsjs"},
	"golang.org/x/crypto":                       {"pubsub/natsjs"},
	"google.golang.org/grpc":                    {"grpc"},
	"google.golang.org/protobuf":                {"grpc"},
	"google.golang.org/genproto/googleapis/rpc": {"grpc"},
	"golang.org/x/net":                          {"grpc"},
	"golang.org/x/text":                         {"grpc"},
	"github.com/modelcontextprotocol/go-sdk":    {"mcp"},
	"github.com/google/jsonschema-go":           {"mcp"},
	"github.com/segmentio/asm":                  {"mcp"},
	"github.com/segmentio/encoding":             {"mcp"},
	"github.com/yosida95/uritemplate/v3":        {"mcp"},
	"golang.org/x/oauth2":                       {"mcp"},
	"golang.org/x/sync":                         {"mcp"},
	"golang.org/x/time":                         {"mcp"},
	"golang.org/x/sys":                          {"grpc", "mcp", "pubsub/natsjs"},
}

// TestAdaptersAreIndependentlyRemovable proves, for Linux, macOS and
// Windows builds: no adapter links another adapter (except the NATS driver
// on trigger/pubsub), only worker and cron link the store, and every
// third-party module is linked only by the adapters that own it. Any
// adapter can therefore be left out of a binary without dragging another in.
func TestAdaptersAreIndependentlyRemovable(t *testing.T) {
	discovered, err := exec.Command(goTool(t), "list", module+"/trigger/...").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, discovered)
	}
	var found []string
	for _, path := range strings.Fields(string(discovered)) {
		rel, ok := strings.CutPrefix(path, module+"/trigger/")
		if !ok || strings.HasPrefix(rel, "testdata/") || slices.Contains(strings.Split(rel, "/"), "internal") {
			continue
		}
		found = append(found, rel)
	}
	sort.Strings(found)
	if !slices.Equal(found, adapters) {
		t.Fatalf("adapters %v, want %v: a new adapter needs ownership rules here", found, adapters)
	}
	declared := map[string][]string{"pubsub/natsjs": {"pubsub"}}
	store := map[string]bool{"worker": true, "cron": true}
	for _, platform := range [][2]string{{"linux", "amd64"}, {"linux", "arm64"}, {"darwin", "arm64"}, {"windows", "amd64"}} {
		goos, goarch := platform[0], platform[1]
		footprint := map[string]int{}
		for _, adapter := range adapters {
			command := exec.Command(goTool(t), "list", "-deps", "-f", "{{.ImportPath}} {{with .Module}}{{.Path}}{{end}}", module+"/trigger/"+adapter)
			command.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
			output, err := command.CombinedOutput()
			if err != nil {
				t.Fatalf("go list %s (%s): %v\n%s", adapter, goos, err, output)
			}
			lines := strings.Split(strings.TrimSpace(string(output)), "\n")
			footprint[adapter] = len(lines)
			for _, line := range lines {
				path, mod, _ := strings.Cut(line, " ")
				if rel, ok := strings.CutPrefix(path, module+"/trigger/"); ok && rel != adapter && !slices.Contains(strings.Split(rel, "/"), "internal") && !slices.Contains(declared[adapter], rel) {
					t.Errorf("%s: trigger/%s links trigger/%s", goos, adapter, rel)
				}
				if path == module+"/contract/conformance" {
					t.Errorf("%s/%s: trigger/%s links the conformance harness, which is for tests only", goos, goarch, adapter)
				}
				if (path == "database/sql" || path == module+"/store" || strings.HasPrefix(path, module+"/store/")) && !store[adapter] {
					t.Errorf("%s: trigger/%s links %s; only worker and cron own durable state", goos, adapter, path)
				}
				if mod == "" || mod == module {
					continue
				}
				allowed, known := owners[mod]
				if !known {
					t.Errorf("%s: trigger/%s links module %s, which has no declared owner", goos, adapter, mod)
				} else if !slices.Contains(allowed, adapter) {
					t.Errorf("%s: trigger/%s links module %s, which only %v may", goos, adapter, mod, allowed)
				}
			}
		}
		names := slices.Clone(adapters)
		sort.Slice(names, func(i, j int) bool { return footprint[names[i]] < footprint[names[j]] })
		report := make([]string, 0, len(names))
		for _, name := range names {
			report = append(report, fmt.Sprintf("%s=%d", name, footprint[name]))
		}
		t.Logf("%s/%s packages linked per adapter: %s", goos, goarch, strings.Join(report, " "))
	}
}
