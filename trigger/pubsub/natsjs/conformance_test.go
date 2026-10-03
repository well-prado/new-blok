package natsjs_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/pubsub"
	"github.com/well-prado/new-blok/trigger/pubsub/natsjs"
	"github.com/well-prado/new-blok/trigger/worker"
)

const conformanceKind = "conformance.order"

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time           { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(by time.Duration) { c.mu.Lock(); c.now = c.now.Add(by); c.mu.Unlock() }

// natsDriver publishes to a real JetStream stream; the real consumer
// transfers each message into a real SQLite queue, whose handler invokes the
// harness workflow and commits an effect row in the acknowledgment
// transaction.
type natsDriver struct {
	b         *broker
	path      string
	env       conformance.TriggerEnv
	clock     *clock
	database  store.Database
	queue     *worker.Queue
	consumer  *pubsub.Consumer
	principal trigger.Principal
	last      pubsub.Outcome
}

func newNATSDriver(t *testing.T) *natsDriver {
	t.Helper()
	d := &natsDriver{b: newBroker(t), path: filepath.Join(t.TempDir(), "jobs.db"), clock: &clock{now: time.Unix(1_800_000_000, 0)}}
	database, err := (sqlite.Backend{}).Open(context.Background(), d.path)
	if err != nil {
		t.Fatal(err)
	}
	d.database = database
	return d
}

func (*natsDriver) Declaration() trigger.Declaration         { return natsjs.Declaration }
func (d *natsDriver) ConfiguredPrincipal() trigger.Principal { return d.principal }

func (d *natsDriver) Open(ctx context.Context, env conformance.TriggerEnv) error {
	d.env = env
	principal, err := env.Authenticate(conformance.ValidCredential)
	if err != nil {
		return err
	}
	d.principal = principal
	return d.construct(ctx)
}

func (d *natsDriver) construct(ctx context.Context) error {
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
	consumer, _, err := natsjs.NewConsumer(ctx, d.b.js, d.b.config(), pubsub.Subscription{Name: "conformance", Principal: d.principal, Kind: conformanceKind, Submit: queue, InputSchema: d.env.InputSchema, FetchWait: 300 * time.Millisecond})
	if err != nil {
		return err
	}
	consumer.Observe(func(o pubsub.Outcome) { d.last = o })
	d.queue, d.consumer = queue, consumer
	return nil
}

func (d *natsDriver) Start(context.Context) error { return nil }

func (d *natsDriver) handle(ctx context.Context, tx *sql.Tx, job worker.Job) error {
	if _, err := d.env.Workflow(ctx, conformance.Call{Input: job.Payload, Principal: job.Principal}); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO conformance_effects (request_key) VALUES (?)`, job.RequestKey)
	return err
}

func (d *natsDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	msg := nats.NewMsg(d.b.prefix + ".orders")
	msg.Data = delivery.Payload
	msg.Header.Set("Blok-Message-Id", delivery.Key)
	if _, err := d.b.js.PublishMsg(ctx, msg); err != nil {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	d.last = pubsub.Outcome{}
	processed, err := d.consumer.ProcessBatch(ctx)
	if err != nil {
		return conformance.Outcome{}, err
	}
	if processed != 1 {
		return conformance.Outcome{}, errors.New("the published message was not delivered")
	}
	switch d.last.Result {
	case pubsub.OutcomeDuplicate:
		return conformance.Outcome{Kind: conformance.Duplicate}, nil
	case pubsub.OutcomeDeadLettered:
		return conformance.Outcome{Kind: conformance.Rejected, Code: d.last.Reason}, nil
	case pubsub.OutcomeAccepted:
	default:
		return conformance.Outcome{}, errors.New("unexpected consumer outcome " + d.last.Result)
	}
	worker, cancel := context.WithCancel(ctx)
	defer cancel()
	if delivery.Disconnect != nil {
		go func() {
			select {
			case <-delivery.Disconnect:
				cancel()
			case <-worker.Done():
			}
		}()
	}
	if _, err := d.queue.ProcessOnce(worker, d.handle); err != nil {
		if errors.Is(err, workerConsumerLost) && ctx.Err() == nil {
			return conformance.Outcome{Kind: conformance.Disconnected}, nil
		}
		return conformance.Outcome{}, err
	}
	return d.outcome(ctx, pubsub.SubmissionKey("conformance", d.last.MessageID))
}

var workerConsumerLost = worker.ErrConsumerLost

func (d *natsDriver) Recover(ctx context.Context) (conformance.Outcome, error) {
	d.clock.Advance(2 * time.Minute)
	var key string
	if _, err := d.queue.ProcessOnce(ctx, func(ctx context.Context, tx *sql.Tx, job worker.Job) error {
		key = job.RequestKey
		return d.handle(ctx, tx, job)
	}); err != nil {
		return conformance.Outcome{}, err
	}
	if key == "" {
		return conformance.Outcome{}, errors.New("nothing was pending redelivery")
	}
	return d.outcome(ctx, key)
}

func (d *natsDriver) outcome(ctx context.Context, key string) (conformance.Outcome, error) {
	job, err := d.queue.Get(ctx, key)
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

func (d *natsDriver) CommittedEffects(ctx context.Context) (int, error) {
	var count int
	err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM conformance_effects`).Scan(&count)
	})
	return count, err
}

// Restart rebinds the durable consumer and reopens the store; the broker
// keeps its stream and acknowledgment floor.
func (d *natsDriver) Restart(ctx context.Context) error {
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

func (d *natsDriver) Stop(context.Context) error {
	d.b.nc.Close()
	return d.database.Close()
}

func TestNATSConsumerPassesTriggerConformance(t *testing.T) {
	brokerURL(t)
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), newNATSDriver(t), corpus, conformance.TriggerOptions{})
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
