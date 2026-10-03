package cron_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	"github.com/well-prado/new-blok/trigger/worker"
)

const conformanceKind = "conformance.order"

// cronDriver maps each delivery to an occurrence: the delivery key names a
// yearly schedule carrying the payload, and the clock is moved back before
// and onto the occurrence, then ticked. A repeated delivery is therefore a
// duplicate tick of the same occurrence (after the wall clock moved
// backwards), and the payload is validated when the schedule is added. The
// queue's handler invokes the harness workflow.
type cronDriver struct {
	path      string
	env       conformance.TriggerEnv
	clock     *fakeClock
	database  store.Database
	queue     *worker.Queue
	scheduler *cron.Scheduler
	principal trigger.Principal
	stopped   bool
}

var occurrence = time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC)

func newCronDriver(t *testing.T) *cronDriver {
	t.Helper()
	d := &cronDriver{path: filepath.Join(t.TempDir(), "cron.db"), clock: newClock(occurrence.Add(-time.Minute))}
	database, err := (sqlite.Backend{}).Open(context.Background(), d.path)
	if err != nil {
		t.Fatal(err)
	}
	d.database = database
	return d
}

func (*cronDriver) Declaration() trigger.Declaration         { return cron.Declaration }
func (d *cronDriver) ConfiguredPrincipal() trigger.Principal { return d.principal }

func (d *cronDriver) Open(ctx context.Context, env conformance.TriggerEnv) error {
	d.env = env
	principal, err := env.Authenticate(conformance.ValidCredential)
	if err != nil {
		return err
	}
	d.principal = principal
	return d.construct(ctx)
}

func (d *cronDriver) construct(ctx context.Context) error {
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
	scheduler, err := cron.New(ctx, d.database, queue, queue, d.clock)
	if err != nil {
		return err
	}
	d.queue, d.scheduler = queue, scheduler
	return nil
}

func (d *cronDriver) Start(context.Context) error { d.stopped = false; return nil }

func (d *cronDriver) handle(ctx context.Context, tx worker.Tx, job worker.Job) error {
	if _, err := d.env.Workflow(ctx, conformance.Call{Input: job.Payload, Principal: job.Principal}); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO conformance_effects (request_key) VALUES (?)`, job.RequestKey)
	return err
}

func (d *cronDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	if d.stopped {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	d.clock.Set(occurrence.Add(-time.Minute))
	_, err := d.scheduler.Add(ctx, cron.Schedule{Name: delivery.Key, Spec: "0 0 1 1 *", TimeZone: "UTC", Kind: conformanceKind, Payload: delivery.Payload, InputSchema: d.env.InputSchema, Principal: d.principal, MaxCatchUp: 1})
	switch {
	case errors.Is(err, cron.ErrConflict):
		return conformance.Outcome{Kind: conformance.Rejected, Code: "conflict"}, nil
	case errors.Is(err, trigger.ErrInvalidInput):
		return conformance.Outcome{Kind: conformance.Rejected, Code: "invalid_input"}, nil
	case err != nil:
		return conformance.Outcome{}, err
	}
	d.clock.Set(occurrence)
	results, err := d.scheduler.Tick(ctx)
	if err != nil {
		return conformance.Outcome{}, err
	}
	submitted := false
	for _, r := range results {
		if r.Schedule == delivery.Key && len(r.Submitted) > 0 {
			submitted = true
		}
	}
	if !submitted {
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
	if _, err := d.queue.ProcessOnce(consumer, d.handle); err != nil {
		if errors.Is(err, worker.ErrConsumerLost) && ctx.Err() == nil {
			return conformance.Outcome{Kind: conformance.Disconnected}, nil
		}
		return conformance.Outcome{}, err
	}
	return d.outcome(ctx, cron.SubmissionKey(delivery.Key, occurrence))
}

func (d *cronDriver) Recover(ctx context.Context) (conformance.Outcome, error) {
	d.clock.Set(d.clock.Now().Add(2 * time.Minute))
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
	return d.outcome(ctx, key)
}

func (d *cronDriver) outcome(ctx context.Context, key string) (conformance.Outcome, error) {
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

func (d *cronDriver) CommittedEffects(ctx context.Context) (int, error) {
	var count int
	err := d.database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM conformance_effects`).Scan(&count)
	})
	return count, err
}

// Restart reopens the store and rebuilds the scheduler; schedules are added
// again by the next deliveries and resume from their cursors.
func (d *cronDriver) Restart(ctx context.Context) error {
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

func (d *cronDriver) Stop(context.Context) error {
	d.stopped = true
	return d.database.Close()
}

func TestCronAdapterPassesTriggerConformance(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), newCronDriver(t), corpus, conformance.TriggerOptions{})
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
