package worker

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
)

const conformanceKind = "conformance.order"

// workerDriver drives the real SQLite-backed queue. The handler commits an
// effect row in the acknowledgment transaction, so effects are counted from
// committed state rather than from workflow returns.
type workerDriver struct {
	path     string
	env      conformance.TriggerEnv
	mu       sync.Mutex
	now      time.Time
	database store.Database
	queue    *Queue
}

func newWorkerDriver(t *testing.T) *workerDriver {
	t.Helper()
	driver := &workerDriver{path: filepath.Join(t.TempDir(), "worker.db"), now: time.Unix(1_700_000_000, 0)}
	// The store is an application dependency opened before the adapter is
	// constructed; its connection pool is not adapter footprint.
	database, err := (sqlite.Backend{}).Open(context.Background(), driver.path)
	if err != nil {
		t.Fatal(err)
	}
	driver.database = database
	return driver
}

func (*workerDriver) Declaration() trigger.Declaration { return Declaration }

func (d *workerDriver) clock() time.Time { d.mu.Lock(); defer d.mu.Unlock(); return d.now }
func (d *workerDriver) advance(by time.Duration) {
	d.mu.Lock()
	d.now = d.now.Add(by)
	d.mu.Unlock()
}

func (d *workerDriver) Open(ctx context.Context, env conformance.TriggerEnv) error {
	d.env = env
	return d.construct(ctx)
}

func (d *workerDriver) construct(ctx context.Context) error {
	queue, err := New(ctx, d.database, d.clock)
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
	d.queue = queue
	return nil
}

// Start has nothing to do: the queue claims work only when ProcessOnce runs.
func (d *workerDriver) Start(context.Context) error { return nil }

func (d *workerDriver) handler(ctx context.Context, tx Tx, job Job) error {
	if _, err := d.env.Workflow(ctx, conformance.Call{Input: job.Payload}); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO conformance_effects (request_key) VALUES (?)`, job.RequestKey)
	return err
}

func (d *workerDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	enqueued, err := d.queue.Enqueue(ctx, EnqueueRequest{RequestKey: delivery.Key, Kind: conformanceKind, Payload: delivery.Payload, MaxAttempts: 3})
	switch {
	case errors.Is(err, ErrInvalidPayload):
		return conformance.Outcome{Kind: conformance.Rejected, Code: "invalid_input"}, nil
	case errors.Is(err, ErrRequestConflict):
		return conformance.Outcome{Kind: conformance.Rejected, Code: "conflict"}, nil
	case err != nil:
		return conformance.Outcome{}, err
	case !enqueued.Accepted:
		return conformance.Outcome{Kind: conformance.Duplicate}, nil
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
	if _, err := d.queue.ProcessOnce(consumer, d.handler); err != nil {
		if consumer.Err() != nil && ctx.Err() == nil {
			return conformance.Outcome{Kind: conformance.Disconnected}, nil
		}
		return conformance.Outcome{}, err
	}
	return d.outcome(ctx, delivery.Key)
}

// Recover lets leases and backoff expire, then redelivers what is pending.
func (d *workerDriver) Recover(ctx context.Context) (conformance.Outcome, error) {
	d.advance(time.Minute)
	var key string
	if _, err := d.queue.ProcessOnce(ctx, func(ctx context.Context, tx Tx, job Job) error {
		key = job.RequestKey
		return d.handler(ctx, tx, job)
	}); err != nil {
		return conformance.Outcome{}, err
	}
	if key == "" {
		return conformance.Outcome{}, errors.New("no delivery was pending redelivery")
	}
	return d.outcome(ctx, key)
}

func (d *workerDriver) outcome(ctx context.Context, key string) (conformance.Outcome, error) {
	job, err := d.queue.Get(ctx, key)
	if err != nil {
		return conformance.Outcome{}, err
	}
	switch job.State {
	case StateCompleted:
		return conformance.Outcome{Kind: conformance.Completed}, nil
	case StatePending:
		return conformance.Outcome{Kind: conformance.Deferred}, nil
	case StateDead:
		code := job.Error
		if code == "job handler failed" {
			code = "internal"
		}
		return conformance.Outcome{Kind: conformance.Rejected, Code: code, Message: job.Error}, nil
	}
	return conformance.Outcome{}, errors.New("job left in state " + job.State)
}

func (d *workerDriver) CommittedEffects(ctx context.Context) (int, error) {
	var count int
	err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM conformance_effects`).Scan(&count)
	})
	return count, err
}

// Restart closes the database and reconstructs the adapter over the same file.
func (d *workerDriver) Restart(ctx context.Context) error {
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

// Stop closes the store; later deliveries must fail without dispatching.
func (d *workerDriver) Stop(context.Context) error { return d.database.Close() }

func TestWorkerAdapterPassesTriggerConformanceOverSQLite(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), newWorkerDriver(t), corpus, conformance.TriggerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ran, skipped := 0, 0
	for _, result := range report.Results {
		if result.Applicable {
			ran++
		} else {
			skipped++
			t.Logf("not applicable: %s (%s)", result.CaseID, result.Reason)
		}
	}
	if ran != 12 || skipped != 7 {
		t.Fatalf("ran=%d skipped=%d report=%+v", ran, skipped, report)
	}
}
