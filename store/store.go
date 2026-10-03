// Package store defines the replaceable durable-store port.
package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/well-prado/new-blok/trigger"
)

// ErrBusy reports that a transaction could not get, or keep, the store's
// write lock: it waited out the busy timeout, or another writer committed
// after it read. The store is contended, nothing was committed, and the same
// work may succeed when retried later. It is saturation: it matches
// trigger.ErrSaturated, so every trigger a busy store's error reaches,
// through the engine or not, answers with its saturation response (#190).
var ErrBusy = fmt.Errorf("store: busy: %w", trigger.ErrSaturated)

// Backend opens a durable database without exposing its implementation to the
// engine or journal callers.
type Backend interface {
	Open(context.Context, string) (Database, error)
}

// Database is the minimal durable boundary needed by the journal. A callback
// returns only after commit has completed, so callers can acknowledge accepted
// work after WithTx returns successfully.
type Database interface {
	WithTx(context.Context, func(*sql.Tx) error) error
	Backup(context.Context, string) error
	Integrity(context.Context) error
	Close() error
}
