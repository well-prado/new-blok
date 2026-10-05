// Package store defines the replaceable durable-store port.
package store

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

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
// work after WithTx returns successfully. WithTx rolls back whenever the
// transaction does not commit, including when its callback panics or calls
// runtime.Goexit; it does not recover a panic, which reaches the caller with
// its original value once the transaction has been rolled back (#267).
type Database interface {
	WithTx(context.Context, func(*sql.Tx) error) error
	Backup(context.Context, string) error
	Integrity(context.Context) error
	Close() error
}

type writerKey struct{}

// Writer marks ctx for a WithTx that writes before anything else and holds
// the write lock until it commits. A backend may then queue such
// transactions first come first served instead of leaving them to contend
// for the lock, where the longest waiters can be starved (#214). Mark only
// a callback whose first statement writes and that does no slow work, since
// it holds the queue as long as it holds the lock. Mark only the context
// passed to WithTx. An unmarked transaction is not queued and behaves as it
// always has: a read, or work done before the first write, never waits for
// the queue.
func Writer(ctx context.Context) context.Context {
	return context.WithValue(ctx, writerKey{}, true)
}

// IsWriter reports whether ctx was marked by Writer.
func IsWriter(ctx context.Context) bool {
	marked, _ := ctx.Value(writerKey{}).(bool)
	return marked
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

// BusyTimeoutProvider is an optional Database capability reporting how long
// a writer waits for the write lock before its transaction fails with
// ErrBusy. Callers that wait for something of their own ahead of a write,
// such as the worker queue's claim turn, bound that wait by it so they fail
// as the store would. A wrapper that keeps the underlying database's waits
// should forward it.
type BusyTimeoutProvider interface {
	BusyTimeout() time.Duration
}

// BusyTimeoutOf reports a database's busy timeout when it exposes a positive
// one.
func BusyTimeoutOf(database Database) (time.Duration, bool) {
	provider, ok := database.(BusyTimeoutProvider)
	if !ok {
		return 0, false
	}
	timeout := provider.BusyTimeout()
	return timeout, timeout > 0
}

// Purger is an optional Database capability for erasure (#281). Deleting a
// row removes it from queries; these remove what deletion leaves behind in
// the database's own files. A backend that zeroes deleted content as it
// deletes (SQLite's secure_delete) still keeps older copies of a page in its
// write-ahead log until the log is truncated, and a database written before
// that setting was on keeps deleted content in its free space.
type Purger interface {
	// PurgeLog writes the write-ahead log back into the database and
	// truncates it, so no older copy of a page survives there. It waits for
	// readers only briefly, since writers wait behind it, and fails with
	// ErrBusy, having truncated nothing, while a reader's snapshot still
	// needs the log; retry it later.
	PurgeLog(context.Context) error
	// PurgeFree rebuilds the database without free pages or free space and
	// then purges the log, so content deleted before deletion zeroed it is
	// gone too. It rewrites the whole database and needs free disk space of
	// its size.
	PurgeFree(context.Context) error
}

// PurgerOf reports a database's purge capability when it has one.
func PurgerOf(database Database) (Purger, bool) {
	purger, ok := database.(Purger)
	return purger, ok
}

// WithWriteDomain annotates err with the write domain it concerns, typically
// the domain whose write lock a transaction waited for before failing with
// ErrBusy. The result matches everything err matches. A nil err or domain
// returns err unchanged. Stores annotate their own busy errors so a caller
// can identify the contended domain even through a wrapper that does not
// forward WriteDomainProvider.
func WithWriteDomain(err error, domain *WriteDomain) error {
	if err == nil || domain == nil {
		return err
	}
	return &domainError{err: err, domain: domain}
}

// ErrorWriteDomain reports the write domain err, or an error it wraps, was
// annotated with by WithWriteDomain. The outermost annotation wins.
func ErrorWriteDomain(err error) (*WriteDomain, bool) {
	var annotated *domainError
	if !errors.As(err, &annotated) {
		return nil, false
	}
	return annotated.domain, true
}

// ErrorWriteDomains reports every write domain err's tree was annotated with,
// in the order errors.As visits them. Where ErrorWriteDomain reports only the
// first, this also reports annotations on the other branches of a joined
// error, such as a handler that returns errors.Join of failures from two
// stores. Within one branch the outermost annotation wins, as it does for
// ErrorWriteDomain.
func ErrorWriteDomains(err error) []*WriteDomain {
	var domains []*WriteDomain
	var visit func(error)
	visit = func(err error) {
		for err != nil {
			if e, ok := err.(*domainError); ok {
				domains = append(domains, e.domain)
				return
			}
			// errors.As consults an error's own As method before unwrapping
			// it; a wrapper exposing its cause only that way still names it.
			if x, ok := err.(interface{ As(any) bool }); ok {
				var annotated *domainError
				if x.As(&annotated) {
					domains = append(domains, annotated.domain)
					return
				}
			}
			switch e := err.(type) {
			case interface{ Unwrap() []error }:
				for _, branch := range e.Unwrap() {
					visit(branch)
				}
				return
			}
			err = errors.Unwrap(err)
		}
	}
	visit(err)
	return domains
}

type domainError struct {
	err    error
	domain *WriteDomain
}

func (e *domainError) Error() string { return e.err.Error() }

func (e *domainError) Unwrap() error { return e.err }
