package pubsub_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/pubsub"
	"github.com/well-prado/new-blok/trigger/worker"
)

type fixture struct {
	InputSchema     json.RawMessage `json:"inputSchema"`
	MaxMessageBytes int             `json:"maxMessageBytes"`
	MaxDeliver      int             `json:"maxDeliver"`
	Cases           []struct {
		Name        string `json:"name"`
		ID          string `json:"id"`
		Data        string `json:"data"`
		Store       string `json:"store"`
		DeadLetter  string `json:"deadLetter"`
		Ack         string `json:"ack"`
		Attempt     int    `json:"attempt"`
		Result      string `json:"result"`
		Reason      string `json:"reason"`
		Acked       int    `json:"acked"`
		Termed      int    `json:"termed"`
		Naks        int    `json:"naks"`
		DeadLetters int    `json:"deadLetters"`
		Submissions int    `json:"submissions"`
	} `json:"cases"`
	Expected map[string]int `json:"expected"`
}

func loadFixture(t *testing.T) fixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "pubsub", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func openQueue(t *testing.T) (*worker.Queue, store.Database) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	return queue, database
}

func jobs(t *testing.T, database store.Database) int {
	t.Helper()
	var count int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs`).Scan(&count)
	}); err != nil {
		t.Fatal(err)
	}
	return count
}

// switchable lets a case make the durable store report saturation or fail.
type switchable struct {
	inner trigger.Submitter
	mode  string
}

func (s *switchable) Submit(ctx context.Context, sub trigger.Submission) (bool, error) {
	switch s.mode {
	case "saturated":
		return false, trigger.ErrSaturated
	case "unavailable":
		return false, errors.New("store unavailable")
	}
	return s.inner.Submit(ctx, sub)
}

var principal = trigger.Principal{ID: "subscription:orders"}

func TestDeliveryTransferFixtures(t *testing.T) {
	f := loadFixture(t)
	queue, database := openQueue(t)
	broker := newBroker(16)
	submitter := &switchable{inner: queue}
	consumer, err := pubsub.New(broker, pubsub.Subscription{Name: "orders", Principal: principal, Kind: "order.event", Submit: submitter, InputSchema: f.InputSchema, MaxMessageBytes: f.MaxMessageBytes, MaxDeliver: f.MaxDeliver})
	if err != nil {
		t.Fatal(err)
	}
	var last pubsub.Outcome
	consumer.Observe(func(o pubsub.Outcome) { last = o })
	totals := map[string]int{}
	var deferred []*memMessage
	var dlqDown, ackLost *memMessage
	for _, tc := range f.Cases {
		data := tc.Data
		if data == "LARGE" {
			data = `{"sku":"` + strings.Repeat("c", f.MaxMessageBytes) + `","quantity":2}`
		}
		message := broker.publish(tc.ID, []byte(data))
		if tc.Attempt > 0 {
			message.delivered = tc.Attempt - 1
		}
		submitter.mode = tc.Store
		broker.failDLQ = tc.DeadLetter == "unavailable"
		broker.failAck[tc.ID] = tc.Ack == "fail"
		jobsBefore, dlqBefore := jobs(t, database), len(broker.deadLetters)
		processed, err := consumer.ProcessBatch(context.Background())
		if err != nil || processed != 1 {
			t.Fatalf("%s: processed=%d err=%v", tc.Name, processed, err)
		}
		if last.Result != tc.Result || last.Reason != tc.Reason || last.MessageID != tc.ID {
			t.Fatalf("%s: outcome=%+v, want %s/%s", tc.Name, last, tc.Result, tc.Reason)
		}
		if message.acks != tc.Acked || message.terms != tc.Termed || message.naks != tc.Naks || len(broker.deadLetters)-dlqBefore != tc.DeadLetters || jobs(t, database)-jobsBefore != tc.Submissions {
			t.Fatalf("%s: acks=%d terms=%d naks=%d deadLetters=%d submissions=%d", tc.Name, message.acks, message.terms, message.naks, len(broker.deadLetters)-dlqBefore, jobs(t, database)-jobsBefore)
		}
		totals[last.Result]++
		if last.Result == pubsub.OutcomeDeferred {
			deferred = append(deferred, message)
		}
		switch tc.Name {
		case "dead-letter destination unavailable":
			dlqDown = message
		case "ack lost after commit":
			ackLost = message
		}
	}
	submitter.mode, broker.failDLQ = "", false
	broker.failAck = map[string]bool{}
	want := map[string]int{"accepted": f.Expected["accepted"], "duplicate": f.Expected["duplicates"], "dead_lettered": f.Expected["deadLettered"], "deferred": f.Expected["deferred"], "dead_letter_failed": f.Expected["deadLetterFailed"], "ack_failed": f.Expected["ackFailed"]}
	for result, count := range want {
		if totals[result] != count {
			t.Fatalf("totals=%v, want %v", totals, want)
		}
	}
	if jobs(t, database) != f.Expected["submissions"] {
		t.Fatalf("submissions=%d, want %d", jobs(t, database), f.Expected["submissions"])
	}

	// Follow-ups: what was left with the broker is completed by redelivery.
	broker.advance(time.Minute)
	results := map[string]string{}
	consumer.Observe(func(o pubsub.Outcome) { results[o.MessageID] = o.Result })
	for {
		processed, err := consumer.ProcessBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if processed == 0 {
			break
		}
	}
	for _, message := range deferred {
		if results[message.id] != "accepted" || message.acks != 1 {
			t.Fatalf("deferred message %s after recovery: %q acks=%d", message.id, results[message.id], message.acks)
		}
	}
	if results[dlqDown.id] != "dead_lettered" || dlqDown.terms != 1 {
		t.Fatalf("poison message after the dead-letter stream recovered: %q terms=%d", results[dlqDown.id], dlqDown.terms)
	}
	if results[ackLost.id] != "duplicate" || ackLost.acks != 1 {
		t.Fatalf("message whose ack was lost: %q acks=%d", results[ackLost.id], ackLost.acks)
	}
	if jobs(t, database) != f.Expected["submissions"]+len(deferred) {
		t.Fatalf("redelivery created extra runs: jobs=%d", jobs(t, database))
	}
}

func TestSlowConsumerStaysWithinTheInFlightBound(t *testing.T) {
	queue, database := openQueue(t)
	broker := newBroker(4)
	slow := submitFunc(func(ctx context.Context, s trigger.Submission) (bool, error) {
		time.Sleep(time.Millisecond)
		return queue.Submit(ctx, s)
	})
	consumer, err := pubsub.New(broker, pubsub.Subscription{Name: "orders", Principal: principal, Kind: "order.event", Submit: slow, InputSchema: []byte(`{"type":"object"}`), MaxInFlight: 4})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		broker.publish("m-"+string(rune('a'+i%26))+strings.Repeat("x", i/26), []byte(`{}`))
	}
	for {
		processed, err := consumer.ProcessBatch(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if processed == 0 {
			break
		}
		if processed > 4 {
			t.Fatalf("batch of %d exceeds the bound", processed)
		}
	}
	if broker.maxOutstanding > 4 || jobs(t, database) != 100 {
		t.Fatalf("max outstanding=%d jobs=%d", broker.maxOutstanding, jobs(t, database))
	}
}

type submitFunc func(context.Context, trigger.Submission) (bool, error)

func (f submitFunc) Submit(ctx context.Context, s trigger.Submission) (bool, error) { return f(ctx, s) }

// overfull returns more messages than asked, which a consumer must refuse.
type overfull struct{ *memBroker }

func (o overfull) Fetch(ctx context.Context, _ int, wait time.Duration) ([]pubsub.Message, error) {
	return o.memBroker.Fetch(ctx, 1000, wait)
}

func TestSubscriptionValidatesBeforeConsuming(t *testing.T) {
	queue, _ := openQueue(t)
	valid := pubsub.Subscription{Name: "orders", Principal: principal, Kind: "order.event", Submit: queue, InputSchema: []byte(`{"type":"object"}`)}
	for name, mutate := range map[string]func(*pubsub.Subscription){
		"missing principal":   func(s *pubsub.Subscription) { s.Principal = trigger.Principal{} },
		"missing submitter":   func(s *pubsub.Subscription) { s.Submit = nil },
		"missing kind":        func(s *pubsub.Subscription) { s.Kind = "" },
		"invalid name":        func(s *pubsub.Subscription) { s.Name = "Orders Queue" },
		"invalid schema":      func(s *pubsub.Subscription) { s.InputSchema = []byte(`{"type":"nope"}`) },
		"unbounded in-flight": func(s *pubsub.Subscription) { s.MaxInFlight = pubsub.MaxInFlightLimit + 1 },
		"unbounded size":      func(s *pubsub.Subscription) { s.MaxMessageBytes = pubsub.MaxMessageBytesLimit + 1 },
		"unbounded deliver":   func(s *pubsub.Subscription) { s.MaxDeliver = pubsub.MaxDeliverLimit + 1 },
	} {
		broker := newBroker(16)
		broker.publish("m-1", []byte(`{}`))
		sub := valid
		mutate(&sub)
		if _, err := pubsub.New(broker, sub); err == nil {
			t.Fatalf("%s accepted", name)
		}
		if broker.messages[0].delivered != 0 {
			t.Fatalf("%s: a message was consumed before validation", name)
		}
	}
	broker := newBroker(1000)
	for i := 0; i < 20; i++ {
		broker.publish("m-"+string(rune('a'+i)), []byte(`{}`))
	}
	consumer, err := pubsub.New(overfull{broker}, valid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := consumer.ProcessBatch(context.Background()); err == nil || !strings.Contains(err.Error(), "more than the bound") {
		t.Fatalf("over-full fetch err=%v", err)
	}
	if errors.Is(err, context.Canceled) {
		t.Fatal("unexpected cancellation")
	}
}

func TestSubscriptionsNamespaceMessageIdentities(t *testing.T) {
	queue, database := openQueue(t)
	for _, name := range []string{"orders", "refunds"} {
		broker := newBroker(16)
		broker.publish("m-1", []byte(`{}`))
		consumer, err := pubsub.New(broker, pubsub.Subscription{Name: name, Principal: principal, Kind: "event", Submit: queue, InputSchema: []byte(`{"type":"object"}`)})
		if err != nil {
			t.Fatal(err)
		}
		var result string
		consumer.Observe(func(o pubsub.Outcome) { result = o.Result })
		if _, err := consumer.ProcessBatch(context.Background()); err != nil || result != pubsub.OutcomeAccepted {
			t.Fatalf("%s: result=%q err=%v; the same id on another subscription must be a new message", name, result, err)
		}
	}
	if jobs(t, database) != 2 {
		t.Fatalf("jobs=%d, want one per subscription", jobs(t, database))
	}
}

// TestRunStopsWhenCanceledWhileBatchesSucceed: cancellation must end Run even
// when no batch ever fails.
func TestRunStopsWhenCanceledWhileBatchesSucceed(t *testing.T) {
	queue, _ := openQueue(t)
	consumer, err := pubsub.New(newBroker(16), pubsub.Subscription{Name: "orders", Principal: principal, Kind: "event", Submit: queue, InputSchema: []byte(`{"type":"object"}`)})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- consumer.Run(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}
}

// partial returns the messages it fetched together with a broker error.
type partial struct{ *memBroker }

func (p partial) Fetch(ctx context.Context, limit int, wait time.Duration) ([]pubsub.Message, error) {
	messages, _ := p.memBroker.Fetch(ctx, limit, wait)
	return messages, errors.New("connection lost mid-fetch")
}

// TestPartialFetchIsStillProcessed: messages a fetch returned before failing
// are transferred, not left to burn a delivery, and the error is reported.
func TestPartialFetchIsStillProcessed(t *testing.T) {
	queue, database := openQueue(t)
	broker := newBroker(16)
	broker.publish("m-1", []byte(`{}`))
	broker.publish("m-2", []byte(`{}`))
	consumer, err := pubsub.New(partial{broker}, pubsub.Subscription{Name: "orders", Principal: principal, Kind: "event", Submit: queue, InputSchema: []byte(`{"type":"object"}`)})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := consumer.ProcessBatch(context.Background())
	if err == nil || processed != 2 || jobs(t, database) != 2 || broker.messages[0].acks != 1 || broker.messages[1].acks != 1 {
		t.Fatalf("processed=%d err=%v jobs=%d; want both messages admitted and the fetch error reported", processed, err, jobs(t, database))
	}
}
