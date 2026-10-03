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
	// A repeated occurrence reaches the port as the very submission cron
	// committed: same key, normalized payload and principal.
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
	initialize := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"late","version":"1"}}}`
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
		if via != "http" && got[1] != "1" {
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
	if processed, err := queue.ProcessOnce(ctx, func(_ context.Context, _ *sql.Tx, job worker.Job) error { ran = job.RequestKey; return nil }); err != nil || !processed || ran != "worker:after-stop" {
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
	session, err := n.mcpSession(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	socket, _, err := n.dialWS(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	defer socket.CloseNow()
	n.gate.holding("http", "grpc", "websocket", "mcp", "submit:webhook", "submit:sse")
	type outcome struct {
		via string
		err error
	}
	outcomes := make(chan outcome, 6)
	expect := func(via string, err error) { outcomes <- outcome{via, err} }
	go func() {
		status, _, body := n.post("/quotes", auth("alice"), order)
		if status != http.StatusOK {
			expect("http", fmt.Errorf("%d %s", status, body))
			return
		}
		expect("http", nil)
	}()
	go func() {
		reply, err := n.callGRPC(ctx, "alice")
		if err == nil && n.grpcCents(reply) != wantCents {
			err = fmt.Errorf("totalCents %d", n.grpcCents(reply))
		}
		expect("grpc", err)
	}()
	go func() { _, err := wsQuote(ctx, socket, "held"); expect("websocket", err) }()
	go func() { _, err := mcpQuote(ctx, session); expect("mcp", err) }()
	go func() {
		status, _, body := n.post("/webhooks/shop", n.signed("evt-held"), order)
		if status != http.StatusAccepted {
			expect("webhook", fmt.Errorf("%d %s", status, body))
			return
		}
		expect("webhook", nil)
	}()
	go func() {
		status, _, body := n.sseStart("alice", "held")
		if status != http.StatusAccepted {
			expect("sse", fmt.Errorf("%d %s", status, body))
			return
		}
		expect("sse", nil)
	}()
	held := map[string]bool{}
	for len(held) < 6 {
		select {
		case via := <-n.gate.entered:
			held[via] = true
		case <-time.After(10 * time.Second):
			t.Fatalf("only %v reached their hold", held)
		}
	}
	drained := make(chan error, 1)
	go func() { drained <- n.app.Shutdown(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for n.app.State() == app.ReadyState {
		if time.Now().After(deadline) {
			t.Fatal("the application never began draining")
		}
		time.Sleep(time.Millisecond)
	}
	if status, _, _ := n.post("/quotes", auth("bob"), order); status != http.StatusServiceUnavailable {
		t.Errorf("http while draining: %d", status)
	}
	if status, _, _ := n.post("/webhooks/shop", n.signed("evt-late"), order); status != http.StatusServiceUnavailable {
		t.Errorf("webhook while draining: %d", status)
	}
	if status, _, _ := n.sseStart("bob", "late"); status != http.StatusServiceUnavailable {
		t.Errorf("sse while draining: %d", status)
	}
	if _, err := n.mcpSession(ctx, "bob"); err == nil {
		t.Error("mcp: a draining application opened a session")
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
	for range 6 {
		select {
		case o := <-outcomes:
			if o.err != nil {
				t.Errorf("%s did not complete: %v", o.via, o.err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a held request never completed")
		}
	}
	if err := <-drained; err != nil {
		t.Fatalf("drain: %v", err)
	}
	for via, key := range map[string]string{"webhook": webhook.SubmissionKey("shop", "evt-held"), "sse": sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "held")} {
		if _, err := n.queue.Get(ctx, key); err != nil {
			t.Errorf("%s was answered but not committed: %v", via, err)
		}
	}
	_ = session.Close()
	socket.CloseNow()
	n.stop()
	noLeaks(t, baseline)
}

// TestNineTriggersUnderMixedLoad sends concurrent traffic through every
// trigger at once over the one store and a pool of workers: every request
// succeeds, every durable submission runs exactly once, and no worker sees
// an error.
func TestNineTriggersUnderMixedLoad(t *testing.T) {
	baseline := goroutineSet()
	const inBandLoad, durableLoad = 8, 40
	n := newNine(t, 4)
	close(n.begin)
	ctx := context.Background()
	var group sync.WaitGroup
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
		go func() {
			defer group.Done()
			if status, _, body := n.post("/quotes", auth(user), order); status != http.StatusOK {
				fail("http %s: %d %s", user, status, body)
			}
		}()
		go func() {
			defer group.Done()
			if reply, err := n.callGRPC(ctx, user); err != nil || n.grpcCents(reply) != wantCents {
				fail("grpc %s: %v", user, err)
			}
		}()
		go func() {
			defer group.Done()
			socket, _, err := n.dialWS(ctx, user)
			if err != nil {
				fail("websocket %s: %v", user, err)
				return
			}
			defer socket.CloseNow()
			if _, err := wsQuote(ctx, socket, "load"); err != nil {
				fail("websocket %s: %v", user, err)
			}
		}()
		go func() {
			defer group.Done()
			session, err := n.mcpSession(ctx, user)
			if err != nil {
				fail("mcp %s: %v", user, err)
				return
			}
			defer session.Close()
			if _, err := mcpQuote(ctx, session); err != nil {
				fail("mcp %s: %v", user, err)
			}
		}()
	}
	for i := range durableLoad {
		user := fmt.Sprintf("user-%d", i)
		group.Add(4)
		go func() {
			defer group.Done()
			id := fmt.Sprintf("evt-%d", i)
			if status, _, body := n.post("/webhooks/shop", n.signed(id), order); status != http.StatusAccepted {
				fail("webhook %s: %d %s", id, status, body)
				return
			}
			key(webhook.SubmissionKey("shop", id))
		}()
		go func() {
			defer group.Done()
			if status, _, body := n.sseStart(user, "load"); status != http.StatusAccepted {
				fail("sse %s: %d %s", user, status, body)
				return
			}
			key(sse.SubmissionKey("orders", trigger.Principal{ID: user}, "load"))
		}()
		go func() {
			defer group.Done()
			id := fmt.Sprintf("m-%d", i)
			n.broker.publish(t, id, []byte(order))
			key(pubsub.SubmissionKey("orders", n.broker.messageID(id)))
		}()
		go func() {
			defer group.Done()
			requestKey := fmt.Sprintf("worker:load-%d", i)
			if enqueued, err := n.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: requestKey, Kind: "quote.worker", Payload: []byte(order), MaxAttempts: 3}); err != nil || !enqueued.Accepted {
				fail("worker %s: %+v %v", requestKey, enqueued, err)
				return
			}
			key(requestKey)
		}()
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
	for _, goos := range []string{"linux", "darwin", "windows"} {
		footprint := map[string]int{}
		for _, adapter := range adapters {
			command := exec.Command(goTool(t), "list", "-deps", "-f", "{{.ImportPath}} {{with .Module}}{{.Path}}{{end}}", module+"/trigger/"+adapter)
			command.Env = append(os.Environ(), "GOOS="+goos, "GOARCH=amd64", "CGO_ENABLED=0")
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
		t.Logf("%s packages linked per adapter: %s", goos, strings.Join(report, " "))
	}
}
