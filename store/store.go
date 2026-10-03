// Package store defines the replaceable durable-store port.
package store

import (
	"context"
	"database/sql"
	"errors"
)

// ErrBusy reports that a transaction could not get the store's write lock
// within the store's busy timeout: the store is saturated, and the same
// work may succeed when retried later.
var ErrBusy = errors.New("store: busy")

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
