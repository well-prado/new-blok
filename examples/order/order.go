// Package order is the durable worker and transactional-outbox example.
package order

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/trigger/worker"
)

var ErrInvalidOrder = errors.New("order: sku and quantity are invalid")

type Request struct {
	RequestKey string `json:"requestKey"`
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
}

type Order struct {
	ID         string
	RequestKey string
	SKU        string
	Quantity   int
	TotalCents int64
}

type Event struct {
	ID      string
	OrderID string
	Kind    string
	Payload json.RawMessage
}

// Outbox event states. An event is claimed (dispatching) before it is
// published and settled after: sent, back to pending for another attempt, or
// dead once its attempts are spent.
const (
	EventPending     = "pending"
	EventDispatching = "dispatching"
	EventSent        = "sent"
	EventDead        = "dead"
)

const (
	// DefaultOutboxLease is how long a claimed event is kept from other
	// dispatchers, and how long its publisher may take.
	DefaultOutboxLease = 30 * time.Second
	// DefaultOutboxMaxAttempts bounds the publish attempts of one event.
	DefaultOutboxMaxAttempts = 5
)

// Outbox error codes, recorded in order_outbox.error_text instead of the
// publisher's own error text.
const (
	PublishFailed     = "publish_failed"
	PublishTimedOut   = "publish_timed_out"
	ClaimAbandoned    = "claim_abandoned"
	AttemptsExhausted = "attempts_exhausted"
)

// ErrLeaseLost reports that a published event could not be marked sent
// because its lease ran out and another dispatcher claimed it: that
// dispatcher publishes it again, under the same event ID.
var ErrLeaseLost = errors.New("order: the outbox lease expired before the event was marked sent")

type Service struct {
	database    store.Database
	queue       *worker.Queue
	prices      map[string]int64
	clock       func() time.Time
	lease       time.Duration
	maxAttempts int
}

// Option configures a Service.
type Option func(*Service)

// WithOutboxLease sets how long a claimed event is kept from other
// dispatchers (DefaultOutboxLease otherwise). It is also the publisher's
// deadline: a publish still running when it ends has failed. Keep it well
// above the store's busy timeout, which the claim and the mark-sent
// transactions may each wait out inside the lease.
func WithOutboxLease(lease time.Duration) Option {
	return func(s *Service) { s.lease = lease }
}

// WithOutboxMaxAttempts bounds the publish attempts of one event
// (DefaultOutboxMaxAttempts otherwise); the event is dead after the last.
func WithOutboxMaxAttempts(attempts int) Option {
	return func(s *Service) { s.maxAttempts = attempts }
}

func New(ctx context.Context, database store.Database, prices map[string]int64, clock func() time.Time, opts ...Option) (*Service, error) {
	if clock == nil {
		clock = time.Now
	}
	service := &Service{database: database, clock: clock, lease: DefaultOutboxLease, maxAttempts: DefaultOutboxMaxAttempts}
	for _, opt := range opts {
		if opt != nil {
			opt(service)
		}
	}
	if service.lease <= 0 || service.maxAttempts < 1 {
		return nil, errors.New("order: the outbox lease must be positive and its attempts at least one")
	}
	queue, err := worker.New(ctx, database, clock)
	if err != nil {
		return nil, err
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS orders (
			order_id TEXT PRIMARY KEY,
			request_key TEXT NOT NULL UNIQUE,
			sku TEXT NOT NULL,
			quantity INTEGER NOT NULL,
			total_cents INTEGER NOT NULL,
			created_at INTEGER NOT NULL
		)`)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS order_outbox (
			event_id TEXT PRIMARY KEY,
			order_id TEXT NOT NULL,
			kind TEXT NOT NULL,
			payload_json BLOB NOT NULL,
			state TEXT NOT NULL,
			created_at INTEGER NOT NULL,
			UNIQUE(order_id, kind)
		)`)
		if err != nil {
			return err
		}
		// The dispatch columns, added to an outbox created before them.
		for _, column := range []struct{ name, definition string }{
			{"attempts", "INTEGER NOT NULL DEFAULT 0"},
			{"lease_until", "INTEGER"},
			{"claim_token", "TEXT"},
			{"error_text", "TEXT NOT NULL DEFAULT ''"},
		} {
			var present int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('order_outbox') WHERE name = ?`, column.name).Scan(&present); err != nil {
				return err
			}
			if present == 0 {
				if _, err := tx.ExecContext(ctx, `ALTER TABLE order_outbox ADD COLUMN `+column.name+` `+column.definition); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("order: schema: %w", err)
	}
	service.queue = queue
	service.prices = make(map[string]int64, len(prices))
	for sku, cents := range prices {
		service.prices[sku] = cents
	}
	return service, nil
}

func (s *Service) Enqueue(ctx context.Context, request Request) (worker.EnqueueResult, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return worker.EnqueueResult{}, err
	}
	return s.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: request.RequestKey, Kind: "order.create", Payload: payload, MaxAttempts: 3})
}

func (s *Service) ProcessOnce(ctx context.Context) (bool, error) {
	return s.queue.ProcessOnce(ctx, func(ctx context.Context, tx worker.Tx, job worker.Job) error {
		var request Request
		if err := json.Unmarshal(job.Payload, &request); err != nil {
			return &worker.HandlerError{Message: "invalid order payload"}
		}
		price, ok := s.prices[request.SKU]
		if !ok || request.Quantity < 1 || request.Quantity > 100 {
			return &worker.HandlerError{Message: ErrInvalidOrder.Error()}
		}
		orderID := "order:" + request.RequestKey
		// A key resubmitted after its dedupe window ended (worker.Retention)
		// reaches this handler again; its order and event already exist.
		_, err := tx.ExecContext(ctx, `INSERT INTO orders (order_id, request_key, sku, quantity, total_cents, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`, orderID, request.RequestKey, request.SKU, request.Quantity, int64(request.Quantity)*price, s.now())
		if err != nil {
			return storeFailure(err)
		}
		payload, err := json.Marshal(map[string]any{"orderId": orderID, "totalCents": int64(request.Quantity) * price})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO order_outbox (event_id, order_id, kind, payload_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(order_id, kind) DO NOTHING`, "event:"+request.RequestKey, orderID, "order.created", payload, EventPending, s.now())
		if err != nil {
			return storeFailure(err)
		}
		return nil
	})
}

// storeFailure makes a failed store write a retryable job failure. Returning
// it rolls back the order and its event together. The example cannot tell a
// transient store failure from a lasting one, since only store/sqlite sees
// the driver's codes, so it retries both within the job's attempts
// (Enqueue's MaxAttempts): a lasting one is dead after the last. Validation
// failures are never retried. A busy store (store.ErrBusy) stays visible
// through the join, so the queue defers it without spending an attempt.
func storeFailure(err error) error {
	return errors.Join(&worker.HandlerError{Retryable: true, Message: "order store write failed"}, err)
}

func (s *Service) Get(ctx context.Context, requestKey string) (Order, error) {
	var result Order
	err := s.database.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT order_id, request_key, sku, quantity, total_cents FROM orders WHERE request_key = ?`, requestKey).Scan(&result.ID, &result.RequestKey, &result.SKU, &result.Quantity, &result.TotalCents)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Order{}, sql.ErrNoRows
	}
	return result, err
}

// Publisher sends one event. Its context ends with the event's lease: a
// publish still running then has failed, and the event may be claimed again.
type Publisher func(context.Context, Event) error

// DispatchOne claims the oldest pending event, publishes it, and settles it.
// Every transaction it opens writes first (#176), and the publish runs
// outside all of them: a writer that commits during the publish cannot fail
// the settlement and send the event again (D3).
//
// A published event is marked sent. A failed publish returns the event to
// pending, or dead once it has had its attempts (WithOutboxMaxAttempts); an
// attempt is not charged when ctx itself ended. A claim whose dispatcher
// died is taken over once its lease ends, or dead-lettered as
// ClaimAbandoned when it was the event's last attempt. Publishers must
// deduplicate by Event.ID: a crash after an external publish and before the
// event is marked sent is uncertain, and the event is published again.
func (s *Service) DispatchOne(ctx context.Context, publish Publisher) (bool, error) {
	if publish == nil {
		return false, errors.New("order: publisher is required")
	}
	event, token, err := s.claimEvent(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("order: dispatch: claim: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(ctx, s.lease)
	publishErr := publish(publishCtx, event)
	cancel()
	// Settle even when the caller has gone: an unsettled claim would wait out
	// its lease and publish the event again.
	settleCtx := context.WithoutCancel(ctx)
	if publishErr == nil {
		if err := s.settle(settleCtx, event.ID, token, `state = 'sent', error_text = ''`); err != nil {
			return true, fmt.Errorf("order: dispatch %s: mark sent: %w", event.ID, err)
		}
		return true, nil
	}
	code, refund := PublishFailed, 0
	if errors.Is(publishErr, context.DeadlineExceeded) {
		code = PublishTimedOut
	}
	if ctx.Err() != nil {
		refund = 1
	}
	settleErr := s.settle(settleCtx, event.ID, token, `state = CASE WHEN attempts - ? >= ? THEN 'dead' ELSE 'pending' END, attempts = attempts - ?, error_text = ?`, refund, s.maxAttempts, refund, code)
	if settleErr != nil {
		settleErr = fmt.Errorf("release: %w", settleErr)
	}
	return true, fmt.Errorf("order: dispatch %s: %w", event.ID, errors.Join(publishErr, settleErr))
}

// claimEvent leases the oldest pending event, or one whose dispatcher's lease
// ended, for one publish attempt. Its first statement writes, so it waits for
// the write lock instead of failing on a snapshot another writer made stale.
func (s *Service) claimEvent(ctx context.Context) (Event, string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return Event{}, "", err
	}
	token := hex.EncodeToString(nonce[:])
	now := s.now()
	var event Event
	claimed := false
	err := s.database.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `UPDATE order_outbox SET state = 'dead', lease_until = NULL, claim_token = NULL,
			error_text = CASE state WHEN 'dispatching' THEN ? ELSE ? END
			WHERE attempts >= ? AND (state = 'pending' OR (state = 'dispatching' AND lease_until <= ?))`, ClaimAbandoned, AttemptsExhausted, s.maxAttempts, now); err != nil {
			return err
		}
		var payload []byte
		if err := tx.QueryRowContext(ctx, `UPDATE order_outbox SET state = 'dispatching', attempts = attempts + 1, lease_until = ?, claim_token = ?
			WHERE event_id = (SELECT event_id FROM order_outbox
				WHERE state = 'pending' OR (state = 'dispatching' AND lease_until <= ?) ORDER BY created_at, event_id LIMIT 1)
			RETURNING event_id, order_id, kind, payload_json`, now+int64(s.lease), token, now).Scan(&event.ID, &event.OrderID, &event.Kind, &payload); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// Nothing to claim; the dead letters above still commit.
				return nil
			}
			return err
		}
		event.Payload = append([]byte(nil), payload...)
		claimed = true
		return nil
	})
	if err == nil && !claimed {
		err = sql.ErrNoRows
	}
	return event, token, err
}

// settle ends this dispatcher's claim on an event with the given assignments.
// The claim token fences it: once another dispatcher has taken the event
// over, it changes nothing and reports ErrLeaseLost.
func (s *Service) settle(ctx context.Context, eventID, token, assignments string, args ...any) error {
	return s.database.WithTx(store.Writer(ctx), func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE order_outbox SET `+assignments+`, lease_until = NULL, claim_token = NULL WHERE event_id = ? AND state = 'dispatching' AND claim_token = ?`, append(args, eventID, token)...)
		if err != nil {
			return err
		}
		if settled, err := result.RowsAffected(); err != nil {
			return err
		} else if settled == 0 {
			return ErrLeaseLost
		}
		return nil
	})
}

func (s *Service) now() int64 { return s.clock().UTC().UnixNano() }
