// Package store defines the replaceable durable-store port.
package store

import (
	"context"
	"database/sql"
	"os"

	"github.com/well-prado/new-blok/contract/capacity"
)

// ErrBusy reports that a transaction could not get, or keep, the store's
// write lock: it waited out the busy timeout, or another writer committed
// after it read. The store is contended, that transaction committed
// nothing, and it may succeed when retried later. It is saturation: it
// matches capacity.ErrSaturated (and so trigger.ErrSaturated), so a trigger
// it reaches answers with its saturation response (#190).
var ErrBusy error = busyError{}

type busyError struct{}

func (busyError) Error() string { return "store: busy" }

func (busyError) Is(target error) bool { return target == capacity.ErrSaturated }

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

// WriteDomain identifies databases that contend for the same write lock.
// Its identity is opaque to callers.
type WriteDomain struct {
	file os.FileInfo
}

// NewWriteDomain creates a stable identity token for one write-lock domain.
// Stores that share a writer lock must return the same token.
func NewWriteDomain() *WriteDomain { return &WriteDomain{} }

// NewFileWriteDomain identifies a write domain by the underlying file. Stores
// opened through different handles to the same file therefore share identity.
func NewFileWriteDomain(file os.FileInfo) *WriteDomain { return &WriteDomain{file: file} }

// SameWriteDomain reports whether two optional domain tokens describe the
// same lock domain. Tokens without file identity match only themselves.
func SameWriteDomain(left, right *WriteDomain) bool {
	if left == nil || right == nil {
		return false
	}
	if left == right {
		return true
	}
	return left.file != nil && right.file != nil && os.SameFile(left.file, right.file)
}

// WriteDomainProvider is an optional Database capability. A wrapper that
// shares its underlying database's writer lock should forward WriteDomain.
// Databases that do not implement this capability cannot participate in
// same-store nested-submission detection.
type WriteDomainProvider interface {
	WriteDomain() *WriteDomain
}

// WriteDomainOf reports a database's lock domain when it exposes one. It does
// not infer identity by comparing arbitrary Database implementations.
func WriteDomainOf(database Database) (*WriteDomain, bool) {
	provider, ok := database.(WriteDomainProvider)
	if !ok {
		return nil, false
	}
	domain := provider.WriteDomain()
	return domain, domain != nil
}
