package pubsub_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/pubsub"
	"github.com/well-prado/new-blok/trigger/worker"
)

const conformanceKind = "conformance.order"

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time           { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) Advance(by time.Duration) { c.mu.Lock(); c.now = c.now.Add(by); c.mu.Unlock() }

// Driver is the shared conformance driver: it publishes to a broker Source,
// lets the real Consumer transfer the message into a real SQLite queue, and
// runs the queue so its handler invokes the harness workflow. publish and
// advance adapt it to a broker; the NATS JetStream package reuses the shape.
type memDriver struct {
	path      string
	env       conformance.TriggerEnv
	clock     *fakeClock
	broker    *memBroker
	database  store.Database
	queue     *worker.Queue
	consumer  *pubsub.Consumer
	principal trigger.Principal
	last      pubsub.Outcome
	stopped   bool
}

func newMemDriver(t *testing.T) *memDriver {
	t.Helper()
	d := &memDriver{path: filepath.Join(t.TempDir(), "jobs.db"), clock: &fakeClock{now: time.Unix(1_800_000_000, 0)}, broker: newBroker(16)}
	database, err := (sqlite.Backend{}).Open(context.Background(), d.path)
	if err != nil {
		t.Fatal(err)
	}
	d.database = database
	return d
}

func (*memDriver) Declaration() trigger.Declaration { return pubsub.Declaration }

func (d *memDriver) ConfiguredPrincipal() trigger.Principal { return d.principal }

func (d *memDriver) Open(ctx context.Context, env conformance.TriggerEnv) error {
	d.env = env
	principal, err := env.Authenticate(conformance.ValidCredential)
	if err != nil {
		return err
	}
	d.principal = principal
	return d.construct(ctx)
}

func (d *memDriver) construct(ctx context.Context) error {
	queue, err := worker.New(ctx, d.database, d.clock.Now)
	if err != nil {
		return err
	}
	if err := queue.RegisterKind(conformanceKind, d.env.InputSchema); err != nil {
		return err
	}
	if err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS conformance_effects (request_key TEXT PRIMARY KEY)`)
		return err
	}); err != nil {
		return err
	}
	consumer, err := pubsub.New(d.broker, pubsub.Subscription{Name: "conformance", Principal: d.principal, Kind: conformanceKind, Submit: queue, InputSchema: d.env.InputSchema})
	if err != nil {
		return err
	}
	consumer.Observe(func(o pubsub.Outcome) { d.last = o })
	d.queue, d.consumer = queue, consumer
	return nil
}

func (d *memDriver) Start(context.Context) error { d.stopped = false; return nil }

func (d *memDriver) handle(ctx context.Context, tx worker.Tx, job worker.Job) error {
	if _, err := d.env.Workflow(ctx, conformance.Call{Input: job.Payload, Principal: job.Principal}); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO conformance_effects (request_key) VALUES (?)`, job.RequestKey)
	return err
}

func (d *memDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	if d.stopped {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	d.broker.publish(delivery.Key, delivery.Payload)
	if _, err := d.consumer.ProcessBatch(ctx); err != nil {
		return conformance.Outcome{}, err
	}
	return transferred(ctx, d.last, d.queue, d.handle, delivery)
}

// transferred maps what the consumer did with a message to what the source
// can observe, running the queue when the message was admitted.
func transferred(ctx context.Context, last pubsub.Outcome, queue *worker.Queue, handle worker.Handler, delivery conformance.Delivery) (conformance.Outcome, error) {
	switch last.Result {
	case pubsub.OutcomeDuplicate:
		return conformance.Outcome{Kind: conformance.Duplicate}, nil
	case pubsub.OutcomeDeadLettered:
		return conformance.Outcome{Kind: conformance.Rejected, Code: last.Reason}, nil
	case pubsub.OutcomeDeferred:
		return conformance.Outcome{Kind: conformance.Deferred}, nil
	case pubsub.OutcomeAccepted:
	default:
		return conformance.Outcome{}, errors.New("unexpected consumer outcome " + last.Result)
	}
	consumer, cancel := context.WithCancel(ctx)
	defer cancel()
	if delivery.Disconnect != nil {
		go func() {
			select {
			case <-delivery.Disconnect:
				cancel()
			case <-consumer.Done():
			}
		}()
	}
	if _, err := queue.ProcessOnce(consumer, handle); err != nil {
		if errors.Is(err, worker.ErrConsumerLost) && ctx.Err() == nil {
			return conformance.Outcome{Kind: conformance.Disconnected}, nil
		}
		return conformance.Outcome{}, err
	}
	return jobOutcome(ctx, queue, pubsub.SubmissionKey("conformance", last.MessageID))
}

func jobOutcome(ctx context.Context, queue *worker.Queue, key string) (conformance.Outcome, error) {
	job, err := queue.Get(ctx, key)
	if err != nil {
		return conformance.Outcome{}, err
	}
	switch job.State {
	case worker.StateCompleted:
		return conformance.Outcome{Kind: conformance.Completed}, nil
	case worker.StatePending:
		return conformance.Outcome{Kind: conformance.Deferred}, nil
	case worker.StateDead:
		code := job.Error
		if code == "job handler failed" {
			code = "internal"
		}
		return conformance.Outcome{Kind: conformance.Rejected, Code: code, Message: job.Error}, nil
	}
	return conformance.Outcome{}, errors.New("job left in state " + job.State)
}

func (d *memDriver) Recover(ctx context.Context) (conformance.Outcome, error) {
	d.clock.Advance(2 * time.Minute)
	var key string
	if _, err := d.queue.ProcessOnce(ctx, func(ctx context.Context, tx worker.Tx, job worker.Job) error {
		key = job.RequestKey
		return d.handle(ctx, tx, job)
	}); err != nil {
		return conformance.Outcome{}, err
	}
	if key == "" {
		return conformance.Outcome{}, errors.New("nothing was pending redelivery")
	}
	return jobOutcome(ctx, d.queue, key)
}

func (d *memDriver) CommittedEffects(ctx context.Context) (int, error) {
	var count int
	err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM conformance_effects`).Scan(&count)
	})
	return count, err
}

// Restart rebuilds the consumer and queue over the same database; the broker
// keeps its stored messages, as a durable broker would.
func (d *memDriver) Restart(ctx context.Context) error {
	if err := d.database.Close(); err != nil {
		return err
	}
	database, err := (sqlite.Backend{}).Open(ctx, d.path)
	if err != nil {
		return err
	}
	d.database = database
	return d.construct(ctx)
}

func (d *memDriver) Stop(context.Context) error {
	d.stopped = true
	return d.database.Close()
}

func TestPubSubConsumerPassesTriggerConformanceOverModelBroker(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), newMemDriver(t), corpus, conformance.TriggerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, result := range report.Results {
		if result.Applicable {
			ran++
		}
	}
	if ran != 12 {
		t.Fatalf("ran=%d report=%+v", ran, report)
	}
}
