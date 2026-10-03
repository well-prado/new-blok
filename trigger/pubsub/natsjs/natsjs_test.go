package natsjs_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/pubsub"
	"github.com/well-prado/new-blok/trigger/pubsub/natsjs"
	"github.com/well-prado/new-blok/trigger/worker"
)

const orderSchema = `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`

var subscriptionPrincipal = trigger.Principal{ID: "subscription:orders"}

// brokerURL returns the integration broker. The suite needs a real NATS
// JetStream server; without one it reports a skip rather than passing.
func brokerURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("NEWBLOK_NATS_URL")
	if url == "" {
		t.Skip("NEWBLOK_NATS_URL is not set: this suite runs only against a real NATS JetStream broker (ADR 0007)")
	}
	return url
}

// broker is one test's isolated stream pair on the shared server.
type broker struct {
	url, prefix string
	nc          *nats.Conn
	js          jetstream.JetStream
}

func newBroker(t *testing.T) *broker {
	t.Helper()
	url := brokerURL(t)
	var random [4]byte
	_, _ = rand.Read(random[:])
	prefix := "t" + hex.EncodeToString(random[:])
	return openBroker(t, url, prefix, true)
}

func openBroker(t *testing.T, url, prefix string, create bool) *broker {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	b := &broker{url: url, prefix: prefix, nc: nc, js: js}
	if create {
		ctx := context.Background()
		for name, subject := range map[string]string{prefix + "_orders": prefix + ".orders", prefix + "_dlq": prefix + ".dlq"} {
			if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{subject}, Storage: jetstream.FileStorage}); err != nil {
				t.Fatal(err)
			}
			stream := name
			t.Cleanup(func() { _ = js.DeleteStream(context.Background(), stream) })
		}
	}
	return b
}

func (b *broker) config() natsjs.Config {
	return natsjs.Config{Stream: b.prefix + "_orders", Consumer: b.prefix + "_c", Subject: b.prefix + ".orders", DeadLetterSubject: b.prefix + ".dlq", AckWait: time.Second, IDHeader: "Blok-Message-Id"}
}

func (b *broker) publish(t *testing.T, id string, data []byte) {
	t.Helper()
	msg := nats.NewMsg(b.prefix + ".orders")
	msg.Data = data
	if id != "" {
		msg.Header.Set("Blok-Message-Id", id)
	}
	if _, err := b.js.PublishMsg(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
}

func (b *broker) deadLetters(t *testing.T) map[string]string {
	t.Helper()
	consumer, err := b.js.OrderedConsumer(context.Background(), b.prefix+"_dlq", jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[string]string{}
	batch, err := consumer.Fetch(100, jetstream.FetchMaxWait(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	for msg := range batch.Messages() {
		reasons[msg.Headers().Get(natsjs.HeaderOriginalID)] = msg.Headers().Get(natsjs.HeaderReason)
	}
	return reasons
}

func (b *broker) consumerInfo(t *testing.T) *jetstream.ConsumerInfo {
	t.Helper()
	consumer, err := b.js.Consumer(context.Background(), b.prefix+"_orders", b.prefix+"_c")
	if err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return info
}

func openStore(t *testing.T, path string) (store.Database, *worker.Queue) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	return database, queue
}

func countJobs(t *testing.T, database store.Database) int {
	t.Helper()
	var n int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM worker_jobs`).Scan(&n)
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

func subscription(submit trigger.Submitter, inFlight int) pubsub.Subscription {
	return pubsub.Subscription{Name: "orders", Principal: subscriptionPrincipal, Kind: "order.event", Submit: submit, InputSchema: []byte(orderSchema), MaxInFlight: inFlight, MaxMessageBytes: 512, MaxDeliver: 5, FetchWait: 200 * time.Millisecond}
}

type tally struct {
	mu      sync.Mutex
	results map[string]int
	byID    map[string][]string
}

func newTally() *tally { return &tally{results: map[string]int{}, byID: map[string][]string{}} }
func (t *tally) observe(o pubsub.Outcome) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.results[o.Result]++
	t.byID[o.MessageID] = append(t.byID[o.MessageID], o.Result)
}
func (t *tally) count(result string) int { t.mu.Lock(); defer t.mu.Unlock(); return t.results[result] }

// drain processes batches until want messages were admitted (accepted or
// duplicate) or the deadline passes, tolerating transient fetch errors.
func drain(t *testing.T, consumer *pubsub.Consumer, done func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatal("messages were not all admitted before the deadline")
		}
		if _, err := consumer.ProcessBatch(context.Background()); err != nil {
			time.Sleep(50 * time.Millisecond)
		}
	}
}

func TestNATSPoisonOversizedAndDuplicateMessages(t *testing.T) {
	b := newBroker(t)
	database, queue := openStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	consumer, _, err := natsjs.NewConsumer(context.Background(), b.js, b.config(), subscription(queue, 16))
	if err != nil {
		t.Fatal(err)
	}
	seen := newTally()
	consumer.Observe(seen.observe)
	messages := []struct {
		id, data, result, reason string
	}{
		{"m-valid", `{"sku":"coffee","quantity":2}`, pubsub.OutcomeAccepted, ""},
		{"m-json", `{"sku":`, pubsub.OutcomeDeadLettered, pubsub.ReasonInvalidInput},
		{"m-type", `{"sku":"coffee","quantity":"two"}`, pubsub.OutcomeDeadLettered, pubsub.ReasonInvalidInput},
		{"m-large", `{"sku":"` + strings.Repeat("c", 600) + `","quantity":1}`, pubsub.OutcomeDeadLettered, pubsub.ReasonTooLarge},
		{"m-bad id", `{"sku":"coffee","quantity":1}`, pubsub.OutcomeDeadLettered, pubsub.ReasonInvalidID},
		// A republish of an admitted id outside any broker duplicate window.
		{"m-valid", `{"sku":"coffee","quantity":2}`, pubsub.OutcomeDuplicate, ""},
		{"m-valid", `{"sku":"coffee","quantity":7}`, pubsub.OutcomeDeadLettered, pubsub.ReasonConflict},
	}
	for _, m := range messages {
		b.publish(t, m.id, []byte(m.data))
	}
	drain(t, consumer, func() bool {
		return seen.count(pubsub.OutcomeAccepted)+seen.count(pubsub.OutcomeDuplicate)+seen.count(pubsub.OutcomeDeadLettered) >= len(messages)
	}, 10*time.Second)
	results := seen.byID
	for _, m := range messages {
		found := false
		for _, result := range results["h:"+m.id] {
			found = found || result == m.result
		}
		if !found {
			t.Fatalf("%s: results=%v, want %s", m.id, results["h:"+m.id], m.result)
		}
	}
	reasons := b.deadLetters(t)
	for _, m := range messages {
		if m.reason != "" && m.reason != pubsub.ReasonConflict && reasons["h:"+m.id] != m.reason {
			t.Fatalf("dead letter for %s: %q, want %q (all: %v)", m.id, reasons["h:"+m.id], m.reason, reasons)
		}
	}
	if len(reasons) != 5 || countJobs(t, database) != 1 {
		t.Fatalf("dead letters=%v jobs=%d; want 5 dead-lettered ids and one run", reasons, countJobs(t, database))
	}
	info := b.consumerInfo(t)
	if info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("consumer left work behind: ackPending=%d pending=%d", info.NumAckPending, info.NumPending)
	}
}

// blockingSubmitter pauses a child process on one side of the commit.
type blockingSubmitter struct {
	inner  trigger.Submitter
	marker string
	before bool
}

func (s blockingSubmitter) Submit(ctx context.Context, sub trigger.Submission) (bool, error) {
	if s.before {
		_ = os.WriteFile(s.marker, []byte("before"), 0o600)
		time.Sleep(30 * time.Second)
	}
	accepted, err := s.inner.Submit(ctx, sub)
	if !s.before && err == nil {
		_ = os.WriteFile(s.marker, []byte("after"), 0o600)
		time.Sleep(30 * time.Second)
	}
	return accepted, err
}

func TestNATSCrashAtAdmissionAckAndCursorBoundaries(t *testing.T) {
	if mode := os.Getenv("NEWBLOK_NATS_CHILD"); mode != "" {
		runCrashChild(t, mode)
		return
	}
	brokerURL(t)
	const total = 6
	for _, tc := range []struct {
		mode       string
		duplicates int
	}{
		{"before-commit", 0},
		{"after-commit", 1},
		{"after-ack", 0},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			b := newBroker(t)
			directory := t.TempDir()
			path, marker := filepath.Join(directory, "jobs.db"), filepath.Join(directory, "marker")
			openStore(t, path)
			for i := 0; i < total; i++ {
				b.publish(t, fmt.Sprintf("m-%d", i), []byte(`{"sku":"coffee","quantity":1}`))
			}
			command := exec.Command(os.Args[0], "-test.run=^TestNATSCrashAtAdmissionAckAndCursorBoundaries$")
			command.Env = append(os.Environ(), "NEWBLOK_NATS_CHILD="+tc.mode, "NEWBLOK_NATS_PREFIX="+b.prefix, "NEWBLOK_NATS_DB="+path, "NEWBLOK_NATS_MARKER="+marker)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = command.Process.Kill(); _ = command.Wait() }()
			deadline := time.Now().Add(20 * time.Second)
			for {
				if _, err := os.Stat(marker); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child never reached the crash point")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()

			database, queue := openStore(t, path)
			before := countJobs(t, database)
			consumer, _, err := natsjs.NewConsumer(context.Background(), b.js, b.config(), subscription(queue, 16))
			if err != nil {
				t.Fatal(err)
			}
			seen := newTally()
			consumer.Observe(seen.observe)
			drain(t, consumer, func() bool {
				return countJobs(t, database) == total && b.consumerInfo(t).NumAckPending == 0 && b.consumerInfo(t).NumPending == 0
			}, 20*time.Second)
			if seen.count(pubsub.OutcomeDuplicate) != tc.duplicates || seen.count(pubsub.OutcomeAccepted) != total-before {
				t.Fatalf("after restart: accepted=%d duplicates=%d (jobs before restart=%d), want duplicates=%d", seen.count(pubsub.OutcomeAccepted), seen.count(pubsub.OutcomeDuplicate), before, tc.duplicates)
			}
			runs := 0
			for {
				processed, err := queue.ProcessOnce(context.Background(), func(context.Context, *sql.Tx, worker.Job) error { runs++; return nil })
				if err != nil {
					t.Fatal(err)
				}
				if !processed {
					break
				}
			}
			if runs != total {
				t.Fatalf("runs=%d, want exactly one per message", runs)
			}
		})
	}
}

func runCrashChild(t *testing.T, mode string) {
	b := openBroker(t, brokerURL(t), os.Getenv("NEWBLOK_NATS_PREFIX"), false)
	_, queue := openStore(t, os.Getenv("NEWBLOK_NATS_DB"))
	marker := os.Getenv("NEWBLOK_NATS_MARKER")
	var submit trigger.Submitter = queue
	if mode != "after-ack" {
		submit = blockingSubmitter{inner: queue, marker: marker, before: mode == "before-commit"}
	}
	consumer, _, err := natsjs.NewConsumer(context.Background(), b.js, b.config(), subscription(submit, 16))
	if err != nil {
		t.Fatal(err)
	}
	acked := 0
	consumer.Observe(func(o pubsub.Outcome) {
		if o.Result == pubsub.OutcomeAccepted {
			acked++
			if mode == "after-ack" && acked == 2 {
				// Two messages acknowledged; the rest of the batch is
				// fetched but not acknowledged.
				_ = os.WriteFile(marker, []byte("after-ack"), 0o600)
				time.Sleep(30 * time.Second)
			}
		}
	})
	for {
		_, _ = consumer.ProcessBatch(context.Background())
	}
}

func TestNATSRebalanceTakesOverUnacknowledgedWork(t *testing.T) {
	b := newBroker(t)
	database, queue := openStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	const total = 40
	for i := 0; i < total; i++ {
		b.publish(t, fmt.Sprintf("m-%d", i), []byte(`{"sku":"coffee","quantity":1}`))
	}
	// Instance A takes a batch and dies without acknowledging it.
	dying := openBroker(t, b.url, b.prefix, false)
	_, sourceA, err := natsjs.NewConsumer(context.Background(), dying.js, b.config(), subscription(queue, 8))
	if err != nil {
		t.Fatal(err)
	}
	taken, err := sourceA.Fetch(context.Background(), 8, time.Second)
	if err != nil || len(taken) == 0 {
		t.Fatalf("instance A fetched %d err=%v", len(taken), err)
	}
	dying.nc.Close()
	// Instances B and C share the durable consumer and drain everything,
	// including A's unacknowledged messages once their ack wait expires.
	seen := newTally()
	var group sync.WaitGroup
	running, stop := context.WithCancel(context.Background())
	runErrs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		peer := openBroker(t, b.url, b.prefix, false)
		consumer, _, err := natsjs.NewConsumer(context.Background(), peer.js, b.config(), subscription(queue, 8))
		if err != nil {
			t.Fatal(err)
		}
		consumer.Observe(seen.observe)
		group.Add(1)
		go func() {
			defer group.Done()
			runErrs <- consumer.Run(running)
		}()
	}
	deadline := time.Now().Add(20 * time.Second)
	for countJobs(t, database) < total || b.consumerInfo(t).NumAckPending != 0 {
		if time.Now().After(deadline) {
			stop()
			group.Wait()
			t.Fatalf("jobs=%d ackPending=%d", countJobs(t, database), b.consumerInfo(t).NumAckPending)
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	group.Wait()
	close(runErrs)
	for err := range runErrs {
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run ended with %v, want context.Canceled after stop", err)
		}
	}
	takenIDs := map[string]bool{}
	for _, m := range taken {
		takenIDs[m.ID()] = true
	}
	for id := range takenIDs {
		if len(seen.byID[id]) == 0 {
			t.Fatalf("message %s held by the lost instance was never redelivered", id)
		}
	}
	if countJobs(t, database) != total || seen.count(pubsub.OutcomeAccepted) != total {
		t.Fatalf("jobs=%d accepted=%d, want %d each", countJobs(t, database), seen.count(pubsub.OutcomeAccepted), total)
	}
}

// proxy forwards TCP connections to the broker and can cut them all.
type proxy struct {
	listener net.Listener
	target   string
	mu       sync.Mutex
	conns    []net.Conn
	drops    atomic.Int32
}

func newProxy(t *testing.T, target string) *proxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &proxy{listener: listener, target: target}
	t.Cleanup(func() { listener.Close(); p.drop() })
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			upstream, err := net.Dial("tcp", target)
			if err != nil {
				client.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, client, upstream)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(upstream, client); upstream.Close() }()
			go func() { _, _ = io.Copy(client, upstream); client.Close() }()
		}
	}()
	return p
}

func (p *proxy) drop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		c.Close()
	}
	p.conns = nil
	p.drops.Add(1)
}

func TestNATSConnectionLossAndReconnect(t *testing.T) {
	b := newBroker(t)
	host := strings.TrimPrefix(b.url, "nats://")
	p := newProxy(t, host)
	nc, err := nats.Connect("nats://"+p.listener.Addr().String(), nats.MaxReconnects(-1), nats.ReconnectWait(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	database, queue := openStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	const total = 80
	for i := 0; i < total; i++ {
		b.publish(t, fmt.Sprintf("m-%d", i), []byte(`{"sku":"coffee","quantity":1}`))
	}
	consumer, _, err := natsjs.NewConsumer(context.Background(), js, b.config(), subscription(queue, 8))
	if err != nil {
		t.Fatal(err)
	}
	seen := newTally()
	cut := false
	consumer.Observe(func(o pubsub.Outcome) {
		seen.observe(o)
		if !cut && seen.count(pubsub.OutcomeAccepted) == total/3 {
			cut = true
			p.drop()
		}
	})
	drain(t, consumer, func() bool {
		return countJobs(t, database) == total && b.consumerInfo(t).NumAckPending == 0 && b.consumerInfo(t).NumPending == 0
	}, 30*time.Second)
	if p.drops.Load() == 0 || nc.Stats().Reconnects == 0 {
		t.Fatalf("the connection was never cut and re-established: drops=%d reconnects=%d", p.drops.Load(), nc.Stats().Reconnects)
	}
	// A cut can lose an acknowledgment after a committed submission; that
	// message is redelivered as a duplicate. Either way it ran once.
	if countJobs(t, database) != total || seen.count(pubsub.OutcomeAccepted)+seen.count(pubsub.OutcomeAckFailed) != total {
		t.Fatalf("jobs=%d accepted=%d ackFailed=%d duplicates=%d", countJobs(t, database), seen.count(pubsub.OutcomeAccepted), seen.count(pubsub.OutcomeAckFailed), seen.count(pubsub.OutcomeDuplicate))
	}
}

func TestNATSSlowConsumerIsBoundedByTheBroker(t *testing.T) {
	b := newBroker(t)
	database, queue := openStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	const total, inFlight = 60, 4
	for i := 0; i < total; i++ {
		b.publish(t, fmt.Sprintf("m-%d", i), []byte(`{"sku":"coffee","quantity":1}`))
	}
	slow := submitFunc(func(ctx context.Context, s trigger.Submission) (bool, error) {
		time.Sleep(5 * time.Millisecond)
		return queue.Submit(ctx, s)
	})
	// Two instances share the durable consumer. Each fetches up to the
	// bound on its own, so only the broker can hold their total to it.
	var consumers []*pubsub.Consumer
	for i := 0; i < 2; i++ {
		peer := openBroker(t, b.url, b.prefix, false)
		consumer, _, err := natsjs.NewConsumer(context.Background(), peer.js, b.config(), subscription(slow, inFlight))
		if err != nil {
			t.Fatal(err)
		}
		consumers = append(consumers, consumer)
	}
	if info := b.consumerInfo(t); info.Config.MaxAckPending != inFlight {
		t.Fatalf("broker MaxAckPending=%d, want %d", info.Config.MaxAckPending, inFlight)
	}
	var peak atomic.Int64
	stop := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if pending := int64(b.consumerInfo(t).NumAckPending); pending > peak.Load() {
				peak.Store(pending)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	var group sync.WaitGroup
	for _, consumer := range consumers {
		group.Add(1)
		go func() {
			defer group.Done()
			deadline := time.Now().Add(30 * time.Second)
			for countJobs(t, database) < total && time.Now().Before(deadline) {
				_, _ = consumer.ProcessBatch(context.Background())
			}
		}()
	}
	group.Wait()
	close(stop)
	<-sampled
	if countJobs(t, database) != total {
		t.Fatalf("jobs=%d, want %d", countJobs(t, database), total)
	}
	if peak.Load() > inFlight || peak.Load() == 0 {
		t.Fatalf("peak outstanding=%d, want 1..%d", peak.Load(), inFlight)
	}
}

type submitFunc func(context.Context, trigger.Submission) (bool, error)

func (f submitFunc) Submit(ctx context.Context, s trigger.Submission) (bool, error) { return f(ctx, s) }

func TestNATSConfigurationValidatesBeforeConsuming(t *testing.T) {
	b := newBroker(t)
	_, queue := openStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	b.publish(t, "m-1", []byte(`{"sku":"coffee","quantity":1}`))
	ctx := context.Background()
	refuse := func(name string, js jetstream.JetStream, config natsjs.Config, sub pubsub.Subscription) {
		t.Helper()
		if _, _, err := natsjs.NewConsumer(ctx, js, config, sub); err == nil {
			t.Fatalf("%s: consumer bound", name)
		}
	}
	config := b.config()
	missingDLQ := config
	missingDLQ.DeadLetterSubject = b.prefix + "-nowhere.dlq"
	refuse("dead-letter subject without a stream", b.js, missingDLQ, subscription(queue, 16))
	// One stream stores both subjects, so only the overlap rule can refuse.
	if _, err := b.js.CreateStream(ctx, jetstream.StreamConfig{Name: b.prefix + "_all", Subjects: []string{b.prefix + "-all.>"}, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.js.DeleteStream(context.Background(), b.prefix+"_all") })
	selfConsuming := natsjs.Config{Stream: b.prefix + "_all", Consumer: b.prefix + "_self", Subject: b.prefix + "-all.>", DeadLetterSubject: b.prefix + "-all.dlq", AckWait: time.Second}
	refuse("filter that matches its own dead letters", b.js, selfConsuming, subscription(queue, 16))
	disjoint := selfConsuming
	disjoint.Subject = b.prefix + "-all.orders"
	if _, _, err := natsjs.NewConsumer(ctx, b.js, disjoint, subscription(queue, 16)); err != nil {
		t.Fatalf("a filter that excludes the dead-letter subject was refused: %v", err)
	}
	// A stream does store the wildcard subject, so only the literal rule can
	// refuse it.
	if _, err := b.js.CreateStream(ctx, jetstream.StreamConfig{Name: b.prefix + "_wdlq", Subjects: []string{b.prefix + "-wdlq.*"}, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.js.DeleteStream(context.Background(), b.prefix+"_wdlq") })
	wildcardDLQ := config
	wildcardDLQ.DeadLetterSubject = b.prefix + "-wdlq.*"
	refuse("dead-letter subject with a wildcard", b.js, wildcardDLQ, subscription(queue, 16))
	wrongStream := config
	wrongStream.Stream = b.prefix + "_dlq"
	refuse("subject stored by another stream", b.js, wrongStream, subscription(queue, 16))
	noPrincipal := subscription(queue, 16)
	noPrincipal.Principal = trigger.Principal{}
	refuse("subscription without principal", b.js, config, noPrincipal)
	shortWait := config
	shortWait.AckWait = time.Millisecond
	refuse("ack wait below the bound", b.js, shortWait, subscription(queue, 16))
	// An existing durable consumer that departs from the needed settings in
	// any one delivery-relevant field is refused and left unchanged.
	matching := jetstream.ConsumerConfig{Durable: config.Consumer, AckPolicy: jetstream.AckExplicitPolicy, AckWait: time.Second, FilterSubject: config.Subject, MaxDeliver: -1, MaxAckPending: 16, MaxRequestBatch: 16}
	for name, mutate := range map[string]func(*jetstream.ConsumerConfig){
		"another in-flight bound": func(c *jetstream.ConsumerConfig) { c.MaxAckPending = 1000 },
		"a broker delivery cap":   func(c *jetstream.ConsumerConfig) { c.MaxDeliver = 6 },
		"headers only":            func(c *jetstream.ConsumerConfig) { c.HeadersOnly = true },
		"a smaller request batch": func(c *jetstream.ConsumerConfig) { c.MaxRequestBatch = 1 },
		"a backoff schedule": func(c *jetstream.ConsumerConfig) {
			c.MaxDeliver = 10
			c.BackOff = []time.Duration{time.Second, 2 * time.Second}
		},
		"another ack wait": func(c *jetstream.ConsumerConfig) { c.AckWait = 5 * time.Second },
		"a start sequence": func(c *jetstream.ConsumerConfig) {
			c.DeliverPolicy = jetstream.DeliverByStartSequencePolicy
			c.OptStartSeq = 1
		},
	} {
		existing := matching
		mutate(&existing)
		if _, err := b.js.CreateConsumer(ctx, config.Stream, existing); err != nil {
			t.Fatalf("%s: create: %v", name, err)
		}
		refuse("existing consumer with "+name, b.js, config, subscription(queue, 16))
		info := b.consumerInfo(t)
		if info.Delivered.Consumer != 0 || info.NumPending != 1 {
			t.Fatalf("%s: a refused configuration consumed work: delivered=%d pending=%d", name, info.Delivered.Consumer, info.NumPending)
		}
		if info.Config.HeadersOnly != existing.HeadersOnly || info.Config.MaxAckPending != existing.MaxAckPending {
			t.Fatalf("%s: the refused binding changed the existing consumer", name)
		}
		if err := b.js.DeleteConsumer(ctx, config.Stream, config.Consumer); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.js.CreateConsumer(ctx, config.Stream, matching); err != nil {
		t.Fatal(err)
	}
	if _, _, err := natsjs.NewConsumer(ctx, b.js, config, subscription(queue, 16)); err != nil {
		t.Fatalf("an existing consumer with exactly the needed settings was refused: %v", err)
	}
	if errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("unexpected cancellation")
	}
}

// TestNATSDeliveryBudgetDeadLetters keeps the store unavailable. The message
// is retried within its budget and then dead-lettered by the consumer; the
// broker itself never caps deliveries.
func TestNATSDeliveryBudgetDeadLetters(t *testing.T) {
	b := newBroker(t)
	unavailable := submitFunc(func(context.Context, trigger.Submission) (bool, error) { return false, errors.New("store unavailable") })
	sub := subscription(unavailable, 4)
	sub.MaxDeliver = 2
	consumer, _, err := natsjs.NewConsumer(context.Background(), b.js, b.config(), sub)
	if err != nil {
		t.Fatal(err)
	}
	if info := b.consumerInfo(t); info.Config.MaxDeliver != -1 {
		t.Fatalf("broker MaxDeliver=%d, want -1 (the consumer owns the budget)", info.Config.MaxDeliver)
	}
	seen := newTally()
	consumer.Observe(seen.observe)
	b.publish(t, "m-budget", []byte(`{"sku":"coffee","quantity":1}`))
	drain(t, consumer, func() bool { return seen.count(pubsub.OutcomeDeadLettered) == 1 }, 20*time.Second)
	if got := seen.byID["h:m-budget"]; len(got) != 2 || got[0] != pubsub.OutcomeDeferred || got[1] != pubsub.OutcomeDeadLettered {
		t.Fatalf("outcomes=%v, want one deferral then a dead letter", got)
	}
	if reasons := b.deadLetters(t); reasons["h:m-budget"] != pubsub.ReasonBudgetExhausted {
		t.Fatalf("dead letters=%v", reasons)
	}
	if info := b.consumerInfo(t); info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("budgeted message left with the broker: %+v", info)
	}
}

// TestNATSDeadLetterOutageDoesNotLoseMessages removes the dead-letter stream
// so every attempt to record a poison message fails, for more deliveries than
// the budget. The message must stay with the broker, and be recorded once the
// stream is back; nothing may be dropped silently.
func TestNATSDeadLetterOutageDoesNotLoseMessages(t *testing.T) {
	b := newBroker(t)
	_, queue := openStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	sub := subscription(queue, 4)
	sub.MaxDeliver = 2
	consumer, _, err := natsjs.NewConsumer(context.Background(), b.js, b.config(), sub)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := b.js.DeleteStream(ctx, b.prefix+"_dlq"); err != nil {
		t.Fatal(err)
	}
	seen := newTally()
	consumer.Observe(seen.observe)
	b.publish(t, "m-poison", []byte(`{"sku":`))
	drain(t, consumer, func() bool { return seen.count(pubsub.OutcomeDeadLetterRetry) >= 3 }, 20*time.Second)
	if info := b.consumerInfo(t); info.NumPending+uint64(info.NumAckPending)+uint64(info.NumRedelivered) == 0 && info.AckFloor.Stream != 0 {
		t.Fatalf("the poison message left the broker without a record: %+v", info)
	}
	if _, err := b.js.CreateStream(ctx, jetstream.StreamConfig{Name: b.prefix + "_dlq", Subjects: []string{b.prefix + ".dlq"}, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	drain(t, consumer, func() bool { return seen.count(pubsub.OutcomeDeadLettered) == 1 }, 30*time.Second)
	if reasons := b.deadLetters(t); reasons["h:m-poison"] != pubsub.ReasonInvalidInput {
		t.Fatalf("dead letters=%v", reasons)
	}
	if info := b.consumerInfo(t); info.NumAckPending != 0 || info.NumPending != 0 {
		t.Fatalf("message still pending after it was recorded: %+v", info)
	}
}

// TestNATSStreamRecreationDoesNotCollide: messages without a publisher id are
// identified by their stored position, including the stream's incarnation, so
// a recreated stream's restarted sequence numbers are new messages, not
// duplicates of old ones.
func TestNATSStreamRecreationDoesNotCollide(t *testing.T) {
	b := newBroker(t)
	database, queue := openStore(t, filepath.Join(t.TempDir(), "jobs.db"))
	ctx := context.Background()
	for round := 0; round < 2; round++ {
		if round == 1 {
			if err := b.js.DeleteStream(ctx, b.prefix+"_orders"); err != nil {
				t.Fatal(err)
			}
			time.Sleep(10 * time.Millisecond)
			if _, err := b.js.CreateStream(ctx, jetstream.StreamConfig{Name: b.prefix + "_orders", Subjects: []string{b.prefix + ".orders"}, Storage: jetstream.FileStorage}); err != nil {
				t.Fatal(err)
			}
		}
		consumer, _, err := natsjs.NewConsumer(ctx, b.js, b.config(), subscription(queue, 4))
		if err != nil {
			t.Fatal(err)
		}
		seen := newTally()
		consumer.Observe(seen.observe)
		b.publish(t, "", []byte(`{"sku":"coffee","quantity":1}`))
		drain(t, consumer, func() bool { return seen.count(pubsub.OutcomeAccepted)+seen.count(pubsub.OutcomeDuplicate) == 1 }, 10*time.Second)
		if seen.count(pubsub.OutcomeAccepted) != 1 {
			t.Fatalf("round %d: the first message of the stream was treated as a duplicate (%v)", round, seen.byID)
		}
	}
	if countJobs(t, database) != 2 {
		t.Fatalf("jobs=%d, want one per stream incarnation", countJobs(t, database))
	}
}
