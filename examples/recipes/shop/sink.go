package shop

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"time"

	"github.com/well-prado/new-blok/store"
)

// SyntheticSink is a durable local receiver used by the executable recipe.
// Keep it in a separate database from the app outbox to exercise the external
// acceptance/acknowledgment boundary.
type SyntheticSink struct {
	database        store.Database
	failAfterAccept atomic.Bool
}

func NewSyntheticSink(ctx context.Context, database store.Database) (*SyntheticSink, error) {
	if database == nil {
		return nil, errors.New("shop: synthetic sink database is required")
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS synthetic_sink_events (event_id TEXT PRIMARY KEY, payload_json BLOB NOT NULL, accepted_at INTEGER NOT NULL)`)
		return err
	}); err != nil {
		return nil, err
	}
	return &SyntheticSink{database: database}, nil
}

// FailAfterAcceptOnce simulates a lost response after the receiver durably
// accepts an event. Retrying the same ID must not duplicate the sink effect.
func (s *SyntheticSink) FailAfterAcceptOnce() { s.failAfterAccept.Store(true) }

func (s *SyntheticSink) Publish(ctx context.Context, eventID string, payload []byte) error {
	if eventID == "" || len(payload) == 0 {
		return errors.New("shop: synthetic sink requires event ID and payload")
	}
	err := s.database.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO synthetic_sink_events(event_id, payload_json, accepted_at) VALUES(?, ?, ?) ON CONFLICT(event_id) DO NOTHING`, eventID, payload, time.Now().UTC().UnixNano()); err != nil {
			return err
		}
		var existing []byte
		if err := tx.QueryRowContext(ctx, `SELECT payload_json FROM synthetic_sink_events WHERE event_id = ?`, eventID).Scan(&existing); err != nil {
			return err
		}
		if string(existing) != string(payload) {
			return errors.New("shop: synthetic sink event ID reused with different payload")
		}
		return nil
	})
	if err != nil {
		return err
	}
	if s.failAfterAccept.Swap(false) {
		return errors.New("shop: synthetic sink accepted event but response was lost")
	}
	return nil
}
