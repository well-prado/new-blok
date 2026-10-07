package order

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/worker"
)

// orderFixtures is testdata/worker/order-fixtures.json: each scenario's
// deliveries and provider behaviour, and each crash phase, with the exact
// counts a run must end with.
type orderFixtures struct {
	SchemaVersion string            `json:"schemaVersion"`
	Scenarios     []fixtureScenario `json:"scenarios"`
	Crashes       []fixtureCrash    `json:"crashes"`
	Limits        []string          `json:"limits"`
}

type fixtureScenario struct {
	Name              string      `json:"name"`
	Deliveries        []Request   `json:"deliveries"`
	Publisher         string      `json:"publisher"`
	OutboxMaxAttempts int         `json:"outboxMaxAttempts"`
	ForeignEvent      bool        `json:"foreignEvent"`
	Expect            observation `json:"expect"`
}

type fixtureCrash struct {
	Phase       string      `json:"phase"`
	AfterCrash  observation `json:"afterCrash"`
	AtCrashTime observation `json:"atCrashTime"`
	AfterLeases observation `json:"afterLeases"`
}

// observation is what one request key left behind: the deliveries accepted
// and the errors returned in this process, its rows, and every publish of
// its event in any process.
type observation struct {
	Accepted       int      `json:"accepted"`
	Errors         []string `json:"errors"`
	Jobs           int      `json:"jobs"`
	JobState       string   `json:"jobState"`
	JobAttempt     int      `json:"jobAttempt"`
	Orders         int      `json:"orders"`
	Outbox         int      `json:"outbox"`
	OutboxState    string   `json:"outboxState"`
	OutboxAttempts int      `json:"outboxAttempts"`
	Publishes      int      `json:"publishes"`
}

func loadOrderFixtures(t *testing.T) orderFixtures {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "worker", "order-fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var fixtures orderFixtures
	if err := decoder.Decode(&fixtures); err != nil {
		t.Fatalf("order fixtures: %v", err)
	}
	if fixtures.SchemaVersion != "worker-order/v2" || len(fixtures.Scenarios) == 0 || len(fixtures.Crashes) == 0 {
		t.Fatalf("order fixtures: schema %q with %d scenarios and %d crashes, want worker-order/v2 with both", fixtures.SchemaVersion, len(fixtures.Scenarios), len(fixtures.Crashes))
	}
	return fixtures
}

var errProviderDown = errors.New("provider unavailable")

// orderRun drives one request key through a service on a real SQLite file.
// Its clock is the wall clock plus an offset the run moves forward, so it
// agrees with a child process that used the wall clock.
type orderRun struct {
	t        *testing.T
	service  *Service
	database store.Database
	offset   *atomic.Int64
	key      string
	log      string
	publish  Publisher
	seen     observation
}

func openRun(t *testing.T, path, log, key, publisher string, offset *atomic.Int64, opts ...Option) *orderRun {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	clock := func() time.Time { return time.Now().Add(time.Duration(offset.Load())) }
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, clock, opts...)
	if err != nil {
		_ = database.Close()
		t.Fatal(err)
	}
	run := &orderRun{t: t, service: service, database: database, offset: offset, key: key, log: log, seen: observation{Errors: []string{}}}
	calls := 0
	switch publisher {
	case "ok":
		run.publish = func(_ context.Context, event Event) error { return appendPublish(log, event.ID) }
	case "timeout-once":
		run.publish = func(ctx context.Context, event Event) error {
			if err := appendPublish(log, event.ID); err != nil {
				return err
			}
			if calls++; calls > 1 {
				return nil
			}
			call, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
			defer cancel()
			<-call.Done()
			return call.Err()
		}
	case "fail-always":
		run.publish = func(_ context.Context, event Event) error {
			if err := appendPublish(log, event.ID); err != nil {
				return err
			}
			return errProviderDown
		}
	default:
		t.Fatalf("unknown publisher %q", publisher)
	}
	return run
}

func (r *orderRun) close() {
	if err := r.database.Close(); err != nil {
		r.t.Fatal(err)
	}
}

func (r *orderRun) record(err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, worker.ErrRequestConflict):
		r.seen.Errors = append(r.seen.Errors, "request_conflict")
	case errors.Is(err, context.DeadlineExceeded):
		r.seen.Errors = append(r.seen.Errors, "publish_timeout")
	case errors.Is(err, errProviderDown):
		r.seen.Errors = append(r.seen.Errors, "publish_failed")
	default:
		r.seen.Errors = append(r.seen.Errors, "unexpected: "+err.Error())
	}
}

func (r *orderRun) enqueue(request Request) {
	result, err := r.service.Enqueue(context.Background(), request)
	r.record(err)
	if err == nil && result.Accepted {
		r.seen.Accepted++
	}
}

// drain runs the worker and the dispatcher until neither has work, rounds
// times. With advance, each round starts two minutes later, past every
// worker and outbox lease and backoff.
func (r *orderRun) drain(rounds int, advance bool) {
	ctx := context.Background()
	for range rounds {
		if advance {
			r.offset.Add(int64(2 * time.Minute))
		}
		for pass := 0; ; pass++ {
			processed, err := r.service.ProcessOnce(ctx)
			r.record(err)
			if !processed {
				break
			}
			if pass > 20 {
				r.t.Fatal("worker kept processing")
			}
		}
		for pass := 0; ; pass++ {
			processed, err := r.service.DispatchOne(ctx, r.publish)
			r.record(err)
			if !processed {
				break
			}
			if pass > 20 {
				r.t.Fatal("dispatcher kept dispatching")
			}
		}
	}
}

// expect compares what the key left behind with a fixture's observation.
func (r *orderRun) expect(stage string, want observation) {
	r.t.Helper()
	got := r.seen
	if err := r.database.WithTx(context.Background(), func(tx *sql.Tx) error {
		ctx := context.Background()
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM worker_jobs WHERE request_key = ?`, r.key).Scan(&got.Jobs); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM orders WHERE request_key = ?`, r.key).Scan(&got.Orders); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM order_outbox WHERE order_id = ?`, "order:"+r.key).Scan(&got.Outbox); err != nil {
			return err
		}
		if err := tx.QueryRowContext(ctx, `SELECT state, attempt FROM worker_jobs WHERE request_key = ?`, r.key).Scan(&got.JobState, &got.JobAttempt); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		err := tx.QueryRowContext(ctx, `SELECT state, attempts FROM order_outbox WHERE event_id = ? AND order_id = ?`, "event:"+r.key, "order:"+r.key).Scan(&got.OutboxState, &got.OutboxAttempts)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
	if data, err := os.ReadFile(r.log); err == nil {
		got.Publishes = strings.Count(string(data), "event:"+r.key+"\n")
	} else if !errors.Is(err, os.ErrNotExist) {
		r.t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		r.t.Fatalf("%s:\n got  %+v\n want %+v", stage, got, want)
	}
}

// appendPublish records one publish durably, so a parent counts the
// publishes of a child it killed.
func appendPublish(path, eventID string) error {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(eventID + "\n"); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// Every scenario in the fixture file runs against a real store and must end
// with exactly its predeclared counts, errors included.
func TestOrderFixtures(t *testing.T) {
	for _, scenario := range loadOrderFixtures(t).Scenarios {
		t.Run(scenario.Name, func(t *testing.T) {
			if len(scenario.Deliveries) == 0 {
				t.Fatal("scenario has no deliveries")
			}
			key := scenario.Deliveries[0].RequestKey
			var opts []Option
			if scenario.OutboxMaxAttempts > 0 {
				opts = append(opts, WithOutboxMaxAttempts(scenario.OutboxMaxAttempts))
			}
			directory := t.TempDir()
			run := openRun(t, filepath.Join(directory, "orders.db"), filepath.Join(directory, "publishes"), key, scenario.Publisher, new(atomic.Int64), opts...)
			defer run.close()
			if scenario.ForeignEvent {
				// Another order's event already holds this order's event ID.
				if err := run.database.WithTx(context.Background(), func(tx *sql.Tx) error {
					_, err := tx.ExecContext(context.Background(), `INSERT INTO order_outbox (event_id, order_id, kind, payload_json, state, created_at) VALUES (?, 'order:foreign', 'foreign', x'7b7d', 'sent', 0)`, "event:"+key)
					return err
				}); err != nil {
					t.Fatal(err)
				}
			}
			for _, delivery := range scenario.Deliveries {
				run.enqueue(delivery)
			}
			run.drain(4, true)
			run.expect(scenario.Name, scenario.Expect)
		})
	}
}
