// Package order is the durable worker and transactional-outbox example.
package order

import (
	"context"
	"database/sql"
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

type Service struct {
	database store.Database
	queue    *worker.Queue
	prices   map[string]int64
}

func New(ctx context.Context, database store.Database, prices map[string]int64, clock func() time.Time) (*Service, error) {
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
		return err
	}); err != nil {
		return nil, fmt.Errorf("order: schema: %w", err)
	}
	copyPrices := make(map[string]int64, len(prices))
	for sku, cents := range prices {
		copyPrices[sku] = cents
	}
	return &Service{database: database, queue: queue, prices: copyPrices}, nil
}

func (s *Service) Enqueue(ctx context.Context, request Request) (worker.EnqueueResult, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return worker.EnqueueResult{}, err
	}
	return s.queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: request.RequestKey, Kind: "order.create", Payload: payload, MaxAttempts: 3})
}

func (s *Service) ProcessOnce(ctx context.Context) (bool, error) {
	return s.queue.ProcessOnce(ctx, func(ctx context.Context, tx *sql.Tx, job worker.Job) error {
		var request Request
		if err := json.Unmarshal(job.Payload, &request); err != nil {
			return &worker.HandlerError{Message: "invalid order payload"}
		}
		price, ok := s.prices[request.SKU]
		if !ok || request.Quantity < 1 || request.Quantity > 100 {
			return &worker.HandlerError{Message: ErrInvalidOrder.Error()}
		}
		orderID := "order:" + request.RequestKey
		_, err := tx.ExecContext(ctx, `INSERT INTO orders (order_id, request_key, sku, quantity, total_cents, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`, orderID, request.RequestKey, request.SKU, request.Quantity, int64(request.Quantity)*price, time.Now().UTC().UnixNano())
		if err != nil {
			return err
		}
		payload, err := json.Marshal(map[string]any{"orderId": orderID, "totalCents": int64(request.Quantity) * price})
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO order_outbox (event_id, order_id, kind, payload_json, state, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(order_id, kind) DO NOTHING`, "event:"+request.RequestKey, orderID, "order.created", payload, "pending", time.Now().UTC().UnixNano())
		return err
	})
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

type Publisher func(context.Context, Event) error

// DispatchOne publishes a stable event ID and marks it sent only after the
// publisher returns nil. Publishers must deduplicate by Event.ID because a
// crash after an external publish and before this commit is uncertain.
func (s *Service) DispatchOne(ctx context.Context, publish Publisher) (bool, error) {
	if publish == nil {
		return false, errors.New("order: publisher is required")
	}
	sent := false
	err := s.database.WithTx(ctx, func(tx *sql.Tx) error {
		var event Event
		var payload []byte
		if err := tx.QueryRowContext(ctx, `SELECT event_id, order_id, kind, payload_json FROM order_outbox WHERE state = 'pending' ORDER BY created_at, event_id LIMIT 1`).Scan(&event.ID, &event.OrderID, &event.Kind, &payload); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}
			return err
		}
		event.Payload = append([]byte(nil), payload...)
		sent = true
		if err := publish(ctx, event); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE order_outbox SET state = 'sent' WHERE event_id = ? AND state = 'pending'`, event.ID)
		return err
	})
	return sent, err
}
