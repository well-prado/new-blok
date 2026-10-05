// Package worker implements the initial local durable worker trigger.
package worker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/internal/migration"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/trigger"
)

// Declaration is the worker adapter's conformance contract: a job is
// acknowledged in the transaction that commits its handler's writes, and a
// consumer lost before that commit leaves the job to be delivered again.
// Producers are trusted application code; the worker authenticates no caller.
//
// The queue owns delivery redelivery (lease, attempt budget, dead letter). It
// does not interpret workflows or retry steps inside an invocation: each claim
// invokes the handler exactly once.
var Declaration = trigger.Declaration{Kind: trigger.Worker, Adapter: "trigger/worker", Completion: trigger.Durable, Disconnect: trigger.Redeliver, Authentication: trigger.TrustedProducer}

var (
	ErrRequestConflict = fmt.Errorf("worker: request key conflicts with existing payload or principal: %w", trigger.ErrConflict)
	ErrNotFound        = errors.New("worker: job not found")
	// ErrInvalidPayload rejects a job before durable acceptance.
	ErrInvalidPayload = fmt.Errorf("worker: payload is invalid for its kind: %w", trigger.ErrInvalidInput)
	// ErrNestedSubmission reports a handler submission to the write domain
	// held by its claim. Use the handler's Tx for atomic writes instead.
	ErrNestedSubmission = errors.New("worker: nested submission targets the store whose claim this handler holds; use the handler Tx for atomic writes")
)

const (
	StatePending    = "pending"
	StateProcessing = "processing"
	StateCompleted  = "completed"
	StateDead       = "dead"
)

type Job struct {
	ID          string
	RequestKey  string
	Kind        string
	Payload     json.RawMessage
	Attempt     int
	MaxAttempts int
	// Deferrals counts redeliveries caused by saturation or a lost consumer.
	// They do not consume attempts but have their own bounded budget.
	Deferrals int
	// Principal was established by the trusted producer at enqueue time.
	Principal trigger.Principal
	// Trace is the parent trace context the producer supplied, if any
	// (EnqueueRequest.Trace). ProcessOnce hands it to the handler as the
	// context's active trace (observe.TraceFrom), so a run the handler
	// starts joins it.
	Trace observe.TraceContext
	State string
	Error string
	// Compacted reports a finished job that Compact erased (#290): only its
	// ID, kind, state and attempt counts remain, with the RequestKey it was
	// looked up by. Payload, Principal, Trace, Error and Deferrals are empty.
	Compacted bool
}

const (
	// DefaultLease is how long a started attempt keeps its job from other
	// workers unless WithLease says otherwise. A worker that dies mid-attempt
	// leaves its lease behind, and the job is claimed again once it expires
	// (#245).
	DefaultLease = 30 * time.Second
	// defaultBusyTimeout is assumed for a store that does not report its
	// busy timeout (store.BusyTimeoutProvider); it is the SQLite backend's
	// default.
	defaultBusyTimeout = 5 * time.Second
	// ClaimAbandoned is the dead-letter code for a job whose attempts were
	// all started and never finished: every worker that claimed it died or
	// lost its lease before acknowledging, for example because the handler
	// kills its own process (#245).
	ClaimAbandoned = "claim_abandoned"
	// HandlerPanicked is the error recorded for an attempt whose handler
	// panicked or called runtime.Goexit instead of returning (#267). The
	// handler's writes were rolled back and the attempt failed: the job is
	// retried after a backoff, or dead once its attempts are spent. The
	// panic value is not recorded, and ProcessOnce does not recover it.
	HandlerPanicked = "handler_panicked"
	// MaxDeferrals bounds saturation and consumer-loss redeliveries per job.
	MaxDeferrals = 16
	// DeferralExhausted is the dead-letter code once MaxDeferrals is spent.
	DeferralExhausted = "deferral_budget_exhausted"
	maxDeferralDelay  = time.Minute
)

// deferralDelay is the backoff before redelivering a job deferred n times.
func deferralDelay(n int) time.Duration {
	if n < 1 {
		n = 1
	}
	if n > 7 {
		return maxDeferralDelay
	}
	return min(time.Second<<(n-1), maxDeferralDelay)
}

type EnqueueRequest struct {
	RequestKey  string
	Kind        string
	Payload     json.RawMessage
	MaxAttempts int
	// Principal is persisted with the job and handed to the handler. It is
	// part of the request identity: the same key with another principal
	// conflicts.
	Principal trigger.Principal
	// Trace is the optional parent trace context of the run the job
	// starts. It travels with the job record and is correlation data, not
	// identity: it is excluded from the payload digest and from the
	// duplicate comparison, so the same key with another (or no) trace is a
	// duplicate that keeps the first committed trace. An invalid context is
	// not stored; an invalid tracestate is dropped.
	Trace observe.TraceContext
}

type EnqueueResult struct {
	Job      Job
	Accepted bool
}

type HandlerError struct {
	Retryable bool
	Message   string
}

func (e *HandlerError) Error() string { return e.Message }

// Handler processes one job. Its writes go through tx, in the same
// transaction as the job's acknowledgment, so they commit only if the job
// does. ctx is the consumer's: it is canceled when the consumer is lost. The
// context also carries the claimed write domain when the store exposes one,
// allowing Queue.Enqueue/Submit to reject a nested write to that same domain
// before it waits. Preserve ctx when submitting: a nested submission made
// with context.Background (or through a wrapper that hides the write domain)
// still fails the job as ErrNestedSubmission, but only after waiting out the
// store's busy timeout once.
type Handler func(ctx context.Context, tx Tx, job Job) error

// ErrClaimLost reports that the claim's transaction ended while a handler
// was running: SQLite rolls a whole transaction back when one of its
// statements is interrupted, fails for want of space, memory or I/O, or
// hits an ON CONFLICT ROLLBACK constraint or a trigger's RAISE(ROLLBACK).
// Nothing the handler wrote was committed, and no further statement runs.
// ProcessOnce also returns it, without running the handler, when another
// worker claimed the job after the attempt's lease expired and before the
// handler could start (#245).
var ErrClaimLost = errors.New("worker: the claim's transaction ended")

// Tx is the claim's transaction as a handler sees it. It fails closed: when
// a statement fails, Tx checks that the transaction still holds this claim,
// and once it does not, every further statement returns ErrClaimLost. A
// transaction SQLite rolled back is otherwise invisible to database/sql, and
// a handler that went on writing would commit outside the claim, and again
// on redelivery (#180). ProcessOnce counts a lost claim as a failed attempt.
// Tx is safe for concurrent use. It refuses statements that would begin,
// end or nest the transaction (ErrTransactionControl); a handler must not
// hide one inside a multi-statement string.
type Tx struct{ claim *claimTx }

// claimTx is the claim's transaction and what identifies the claim in it.
type claimTx struct {
	tx    *sql.Tx
	jobID string
	lease int64
	// mu is held across each statement and the check after it, so no
	// statement, from any goroutine, starts on a transaction SQLite has
	// ended before the claim is known to be lost.
	mu   sync.Mutex
	dead bool
}

func (c *claimTx) lost() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.dead }

// do runs one statement in the claim's transaction. It refuses to run once
// the claim is lost. When the statement fails, the failure may have ended
// the whole transaction: the claim is still held only if the transaction
// still sees its own uncommitted lease on the job (read on the claim's own
// connection; once SQLite has rolled back, that read sees the job as it was
// before the claim).
func (c *claimTx) do(statement func() error) error {
	if c == nil {
		return ErrClaimLost
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dead {
		return ErrClaimLost
	}
	err := statement()
	if err == nil || errors.Is(err, sql.ErrNoRows) {
		return err
	}
	var state string
	var lease sql.NullInt64
	probe := c.tx.QueryRowContext(context.Background(), `SELECT state, lease_until FROM worker_jobs WHERE job_id = ?`, c.jobID).Scan(&state, &lease)
	if probe != nil || state != StateProcessing || !lease.Valid || lease.Int64 != c.lease {
		c.dead = true
	}
	return err
}

func (c *claimTx) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := c.do(func() error {
		var err error
		result, err = c.tx.ExecContext(ctx, query, args...)
		return err
	})
	return result, err
}

// ErrTransactionControl reports a handler statement that would begin, end
// or nest the claim's transaction: the claim owns it.
var ErrTransactionControl = errors.New("worker: a handler cannot control the claim's transaction")

// controls reports whether a statement begins with a transaction-control
// keyword, after any leading whitespace, comments and empty statements (a
// lone ";", which SQLite skips).
func controls(query string) bool {
	for {
		query = strings.TrimLeftFunc(query, func(r rune) bool { return unicode.IsSpace(r) || r == ';' })
		switch {
		case strings.HasPrefix(query, "--"):
			_, rest, found := strings.Cut(query, "\n")
			if !found {
				return false
			}
			query = rest
		case strings.HasPrefix(query, "/*"):
			_, rest, found := strings.Cut(query[2:], "*/")
			if !found {
				return false
			}
			query = rest
		default:
			end := strings.IndexFunc(query, func(r rune) bool { return !unicode.IsLetter(r) })
			if end < 0 {
				end = len(query)
			}
			switch strings.ToUpper(query[:end]) {
			case "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
				return true
			}
			return false
		}
	}
}

// ExecContext runs a statement in the claim's transaction.
func (t Tx) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	if controls(query) {
		return nil, ErrTransactionControl
	}
	return t.claim.exec(ctx, query, args...)
}

// QueryContext runs a query in the claim's transaction.
func (t Tx) QueryContext(ctx context.Context, query string, args ...any) (*Rows, error) {
	if controls(query) {
		return nil, ErrTransactionControl
	}
	var rows *sql.Rows
	err := t.claim.do(func() error {
		var err error
		rows, err = t.claim.tx.QueryContext(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &Rows{rows: rows, claim: t.claim}, nil
}

// QueryRowContext runs a single-row query in the claim's transaction. The
// query runs, and its failure is checked, before it returns.
func (t Tx) QueryRowContext(ctx context.Context, query string, args ...any) *Row {
	if controls(query) {
		return &Row{err: ErrTransactionControl}
	}
	var row *sql.Row
	err := t.claim.do(func() error {
		row = t.claim.tx.QueryRowContext(ctx, query, args...)
		return row.Err()
	})
	if err != nil {
		return &Row{err: err}
	}
	return &Row{row: row, claim: t.claim}
}

// Row is the result of Tx.QueryRowContext.
type Row struct {
	row   *sql.Row
	claim *claimTx
	err   error
}

// Scan copies the row's columns into dest, like sql.Row.Scan.
func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return r.claim.do(func() error { return r.row.Scan(dest...) })
}

// Err reports the row's error without scanning, like sql.Row.Err.
func (r *Row) Err() error { return r.err }

// Rows is the result of Tx.QueryContext.
type Rows struct {
	rows  *sql.Rows
	claim *claimTx
	err   error
}

// Next prepares the next row, like sql.Rows.Next. When it returns false,
// Err reports why.
func (r *Rows) Next() bool {
	next := false
	if err := r.claim.do(func() error {
		next = r.rows.Next()
		if next {
			return nil
		}
		return r.rows.Err()
	}); err != nil {
		r.err = err
	}
	return next
}

// Scan copies the current row's columns into dest, like sql.Rows.Scan.
func (r *Rows) Scan(dest ...any) error {
	return r.claim.do(func() error { return r.rows.Scan(dest...) })
}

// Err reports the error, if any, that ended the iteration.
func (r *Rows) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.rows.Err()
}

// Close closes the rows.
func (r *Rows) Close() error { return r.rows.Close() }

type Queue struct {
	database    store.Database
	writeDomain *store.WriteDomain
	clock       func() time.Time
	lease       time.Duration
	busyTimeout time.Duration
	// claimTurn is this process's turn to start an attempt on the queue's
	// write domain, shared by every Queue on that domain (claimTurnFor).
	claimTurn chan struct{}
	mu        sync.RWMutex
	schemas   map[string]schema.Schema
	// hold and minRetention are the application's retention policy for
	// finished jobs (WithRetentionHold, WithMinRetention).
	hold         func(RetainedJob) bool
	minRetention time.Duration
	// compactRowHook, test-only, runs before each row a compaction write
	// transaction erases.
	compactRowHook func()
}

// Option configures a Queue.
type Option func(*options)

type options struct {
	lease        time.Duration
	hold         func(RetainedJob) bool
	minRetention time.Duration
}

// WithLease sets how long a started attempt keeps its job from other workers
// (DefaultLease otherwise). It bounds how long a job waits after its worker
// dies before it is delivered again. New refuses a lease no longer than twice
// the store's busy timeout: between starting an attempt and taking it over
// to run the handler, a worker may wait for its write turn and then for the
// write lock, each up to the busy timeout, and a lease that ran out meanwhile
// would lose the attempt to another worker.
func WithLease(lease time.Duration) Option {
	return func(o *options) { o.lease = lease }
}

// WithRetentionHold sets the application's legal hold for finished jobs:
// Compact keeps every job for which hold returns true, with all of its
// content (#290). A hold that panics keeps the job.
func WithRetentionHold(hold func(RetainedJob) bool) Option {
	return func(o *options) { o.hold = hold }
}

// WithMinRetention sets the application's legal minimum for finished jobs:
// Compact never erases a job that finished less than minimum ago, whatever
// cutoff it is given (#290). New refuses a negative minimum.
func WithMinRetention(minimum time.Duration) Option {
	return func(o *options) { o.minRetention = minimum }
}

func New(ctx context.Context, database store.Database, clock func() time.Time, opts ...Option) (*Queue, error) {
	if database == nil {
		return nil, errors.New("worker: database is required")
	}
	if clock == nil {
		clock = time.Now
	}
	configured := options{lease: DefaultLease}
	for _, opt := range opts {
		if opt != nil {
			opt(&configured)
		}
	}
	busyTimeout, ok := store.BusyTimeoutOf(database)
	if !ok {
		busyTimeout = defaultBusyTimeout
	}
	if configured.minRetention < 0 {
		return nil, errors.New("worker: minimum retention must not be negative")
	}
	if configured.lease <= 2*busyTimeout {
		return nil, fmt.Errorf("worker: lease %v must be longer than twice the store's busy timeout (%v)", configured.lease, busyTimeout)
	}
	writeDomain, _ := store.WriteDomainOf(database)
	queue := &Queue{database: database, writeDomain: writeDomain, clock: clock, lease: configured.lease, busyTimeout: busyTimeout, claimTurn: claimTurnFor(writeDomain), schemas: map[string]schema.Schema{}, hold: configured.hold, minRetention: configured.minRetention}
	if err := migration.Retry(ctx, func() error {
		return queue.withTx(ctx, func(tx *sql.Tx) error {
			if err := createJobs(ctx, tx); err != nil {
				return err
			}
			if err := ensureColumn(ctx, tx, "deferrals", "INTEGER NOT NULL DEFAULT 0"); err != nil {
				return err
			}
			if err := ensureColumn(ctx, tx, "principal_json", "TEXT NOT NULL DEFAULT ''"); err != nil {
				return err
			}
			// enqueue_seq orders jobs that share a created_at by when they were
			// enqueued (#217). Jobs from before the column existed keep 0 and
			// fall back to job_id among themselves.
			if err := ensureColumn(ctx, tx, "enqueue_seq", "INTEGER NOT NULL DEFAULT 0"); err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS worker_jobs_enqueue_seq ON worker_jobs (enqueue_seq)`); err != nil {
				return err
			}
			// The trace context a job's run joins (#276). Jobs from before
			// the columns existed carry none.
			if err := ensureColumn(ctx, tx, "traceparent", "TEXT NOT NULL DEFAULT ''"); err != nil {
				return err
			}
			if err := ensureColumn(ctx, tx, "tracestate", "TEXT NOT NULL DEFAULT ''"); err != nil {
				return err
			}
			// The operational census (ADR 0022) reads only unfinished and
			// dead jobs through this covering partial index, so its cost
			// follows the live queue, not the completed history (#105).
			if _, err := tx.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS worker_jobs_unfinished ON worker_jobs (state, lease_until, available_at) WHERE state <> 'completed'`); err != nil {
				return err
			}
			// Tombstones, erasure counters and the finished-job index
			// Compact reads (#290).
			return migrateRetention(ctx, tx)
		})
	}); err != nil {
		return nil, fmt.Errorf("worker: schema: %w", err)
	}
	return queue, nil
}

func createJobs(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS worker_jobs (
		job_id TEXT PRIMARY KEY,
		request_key TEXT NOT NULL UNIQUE,
		kind TEXT NOT NULL,
		payload_json BLOB NOT NULL,
		payload_digest TEXT NOT NULL,
		attempt INTEGER NOT NULL DEFAULT 0,
		deferrals INTEGER NOT NULL DEFAULT 0,
		principal_json TEXT NOT NULL DEFAULT '',
		max_attempts INTEGER NOT NULL,
		state TEXT NOT NULL,
		available_at INTEGER NOT NULL,
		lease_until INTEGER,
		error_text TEXT NOT NULL DEFAULT '',
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		enqueue_seq INTEGER NOT NULL DEFAULT 0,
		traceparent TEXT NOT NULL DEFAULT '',
		tracestate TEXT NOT NULL DEFAULT ''
	)`)
	return err
}

// ensureColumn adds a column to queues created before it existed. Existing
// jobs take the column default.
func ensureColumn(ctx context.Context, tx *sql.Tx, column, definition string) error {
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(worker_jobs)`)
	if err != nil {
		return err
	}
	present := false
	for rows.Next() {
		var cid, notNull, primary int
		var name, kind string
		var fallback sql.NullString
		if err := rows.Scan(&cid, &name, &kind, &notNull, &fallback, &primary); err != nil {
			rows.Close()
			return err
		}
		present = present || name == column
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if present {
		return nil
	}
	_, err = tx.ExecContext(ctx, `ALTER TABLE worker_jobs ADD COLUMN `+column+` `+definition)
	return err
}

// RegisterKind declares the input schema for a job kind. Enqueue then rejects
// payloads that do not validate, before anything is persisted. Kinds without
// a registered schema accept any JSON payload.
func (q *Queue) RegisterKind(kind string, inputSchema []byte) error {
	if kind == "" {
		return errors.New("worker: kind is required")
	}
	parsed, err := schema.Parse(inputSchema)
	if err != nil {
		return fmt.Errorf("worker: kind %s schema: %w", kind, err)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, exists := q.schemas[kind]; exists {
		return fmt.Errorf("worker: kind %s is already registered", kind)
	}
	q.schemas[kind] = parsed
	return nil
}

func (q *Queue) Enqueue(ctx context.Context, request EnqueueRequest) (EnqueueResult, error) {
	if q.writeDomain != nil {
		claimed, ok := ctx.Value(claimedWriteDomainKey{}).(*claimedWriteDomain)
		if ok && claimed.active.Load() && store.SameWriteDomain(claimed.domain, q.writeDomain) {
			return EnqueueResult{}, ErrNestedSubmission
		}
	}
	if request.RequestKey == "" || request.Kind == "" {
		return EnqueueResult{}, errors.New("worker: request key and kind are required")
	}
	if !json.Valid(request.Payload) {
		return EnqueueResult{}, fmt.Errorf("%w: payload is not valid JSON", ErrInvalidPayload)
	}
	q.mu.RLock()
	kindSchema, registered := q.schemas[request.Kind]
	q.mu.RUnlock()
	if registered {
		if _, err := kindSchema.Normalize(request.Payload); err != nil {
			return EnqueueResult{}, fmt.Errorf("%w: %v", ErrInvalidPayload, err)
		}
	}
	if request.MaxAttempts <= 0 {
		request.MaxAttempts = 3
	}
	payloadDigest := digest(request.Payload)
	principal, err := encodePrincipal(request.Principal)
	if err != nil {
		return EnqueueResult{}, err
	}
	// The request key is unique, so it alone identifies the job. Deriving the
	// ID from the clock let equal payloads collide under a coarse clock.
	jobID := "job:" + digest([]byte(request.RequestKey))[:32]
	traceparent, tracestate := encodeTrace(request.Trace)
	var result EnqueueResult
	err = q.withTx(store.Writer(ctx), func(tx *sql.Tx) error {
		// enqueue_seq is one past the highest so far, read under this
		// transaction's write lock, so it follows commit order. It is stored
		// rather than taken from SQLite's rowid, which SQLite documents VACUUM
		// may renumber for tables without an INTEGER PRIMARY KEY. A key
		// whose job Compact erased still has its tombstone, and inserts
		// nothing (#290).
		res, err := tx.ExecContext(ctx, `INSERT INTO worker_jobs
			(job_id, request_key, kind, payload_json, payload_digest, max_attempts, principal_json, state, available_at, created_at, updated_at, enqueue_seq, traceparent, tracestate)
			SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT COALESCE(MAX(enqueue_seq), 0) + 1 FROM worker_jobs), ?, ?
			WHERE NOT EXISTS (SELECT 1 FROM worker_compacted WHERE request_digest = ?) ON CONFLICT(request_key) DO NOTHING`,
			jobID, request.RequestKey, request.Kind, []byte(request.Payload), payloadDigest, request.MaxAttempts, principal, StatePending, q.now(), q.now(), q.now(), traceparent, tracestate, digest([]byte(request.RequestKey)))
		if err != nil {
			return err
		}
		count, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			job, err := scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM worker_jobs WHERE request_key = ?`, request.RequestKey))
			if errors.Is(err, sql.ErrNoRows) {
				// The key's job was compacted: its tombstone keeps the
				// identity a duplicate must match, and nothing else.
				tombstone, identity, err := compacted(ctx, tx, request.RequestKey)
				if err != nil {
					return err
				}
				if identity != identityDigest(request.Kind, payloadDigest, principal) {
					return ErrRequestConflict
				}
				result = EnqueueResult{Job: tombstone, Accepted: false}
				return nil
			}
			if err != nil {
				return err
			}
			var existingDigest, existingPrincipal string
			if err := tx.QueryRowContext(ctx, `SELECT payload_digest, principal_json FROM worker_jobs WHERE request_key = ?`, request.RequestKey).Scan(&existingDigest, &existingPrincipal); err != nil {
				return err
			}
			if job.Kind != request.Kind || existingDigest != payloadDigest || existingPrincipal != principal {
				return ErrRequestConflict
			}
			result = EnqueueResult{Job: job, Accepted: false}
			return nil
		}
		result = EnqueueResult{Job: Job{ID: jobID, RequestKey: request.RequestKey, Kind: request.Kind, Payload: append([]byte(nil), request.Payload...), MaxAttempts: request.MaxAttempts, Principal: request.Principal, Trace: decodeTrace(traceparent, tracestate), State: StatePending}, Accepted: true}
		return nil
	})
	if err != nil {
		// A WithTx error means nothing was committed (the driver rolls back
		// any failed COMMIT). The deadline is also read from ctx: if it ran
		// out just before COMMIT, database/sql may report ErrTxDone instead.
		if errors.Is(err, store.ErrBusy) || errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			// A store too busy to take the submission before the lock wait
			// or the caller's deadline runs out is saturation: nothing was
			// committed (database/sql checks the deadline before COMMIT),
			// so the submission may be retried. A caller that canceled is
			// not saturation.
			saturated := fmt.Errorf("worker: enqueue: %w: %w", trigger.ErrSaturated, err)
			// Name the write domain the submission waited on. A store that
			// annotates its busy errors already did, even through a wrapper
			// that hides WriteDomain; otherwise it is this queue's own. A
			// handler that returns this error from inside a claim on the
			// same domain is diagnosed by ProcessOnce (#207).
			if _, named := store.ErrorWriteDomain(saturated); !named {
				saturated = store.WithWriteDomain(saturated, q.writeDomain)
			}
			return EnqueueResult{}, saturated
		}
		return EnqueueResult{}, fmt.Errorf("worker: enqueue: %w", err)
	}
	return result, nil
}

// ErrConsumerLost reports that the consumer's context ended while a claimed
// job was being handled. The claim and every handler write were rolled back,
// so the job is delivered again without consuming an attempt.
var ErrConsumerLost = errors.New("worker: consumer lost before acknowledgment")

// claimedWriteDomainKey marks the write domain held by ProcessOnce's claim.
// The value is attached only to the handler's trusted native context.
type claimedWriteDomainKey struct{}

type claimedWriteDomain struct {
	domain *store.WriteDomain
	active atomic.Bool
}

// claimTurns holds one claim turn per write domain this process has opened
// a Queue on, so every Queue on a domain shares it (#245 review). Entries are
// kept for the life of the process; a domain without identity gets a turn of
// its own.
var claimTurns struct {
	mu    sync.Mutex
	turns []claimTurn
}

type claimTurn struct {
	domain *store.WriteDomain
	turn   chan struct{}
}

func claimTurnFor(domain *store.WriteDomain) chan struct{} {
	if domain == nil {
		return make(chan struct{}, 1)
	}
	claimTurns.mu.Lock()
	defer claimTurns.mu.Unlock()
	for _, existing := range claimTurns.turns {
		if store.SameWriteDomain(existing.domain, domain) {
			return existing.turn
		}
	}
	turn := make(chan struct{}, 1)
	claimTurns.turns = append(claimTurns.turns, claimTurn{domain: domain, turn: turn})
	return turn
}

// takeClaimTurn waits for this process's turn to start an attempt on the
// queue's write domain, up to the store's busy timeout, and returns its
// release. Like the store's own write turn (#214) it is not interrupted by
// the consumer's context, and it fails as the store would, with
// store.ErrBusy naming the write domain, so a handler that runs ProcessOnce
// on the store its own claim holds fails after one busy wait instead of
// deadlocking (#207).
func (q *Queue) takeClaimTurn() (func(), error) {
	release := func() { <-q.claimTurn }
	select {
	case q.claimTurn <- struct{}{}:
		return release, nil
	default:
	}
	timer := time.NewTimer(q.busyTimeout)
	defer timer.Stop()
	select {
	case q.claimTurn <- struct{}{}:
		return release, nil
	case <-timer.C:
		return nil, store.WithWriteDomain(fmt.Errorf("worker: claim: %w: no claim turn within %v", store.ErrBusy, q.busyTimeout), q.writeDomain)
	}
}

// ProcessOnce claims and processes one job, in two write transactions.
//
// The first starts the attempt: it leases the job (DefaultLease, or
// WithLease) and counts the attempt, and commits. A worker process that dies
// after this point (SIGKILL, OOM, a crash) leaves the lease and the counted
// attempt behind, so a handler that kills its process every time still
// spends the job's attempts and dead-letters it as ClaimAbandoned instead of
// being redelivered forever (#245). The job is claimed again once the lease
// expires.
//
// Within one process, one worker at a time per write domain holds a started
// attempt: ProcessOnce takes the domain's claim turn before it starts one
// and keeps it until the attempt's outcome is committed. Another worker
// could not run its handler meanwhile anyway (the handler holds the write
// lock), and an attempt it started while waiting would be charged if the
// running handler crashed the process. A worker that cannot take the turn
// within the store's busy timeout fails with store.ErrBusy and starts
// nothing. Workers in other processes on the same database do not share the
// turn.
//
// The second takes that lease over and runs the handler. The handler's
// writes and the job's acknowledgment are in this one transaction, so a crash
// rolls back both the business write and the delivery acknowledgment (ADR
// 0006); only the started attempt survives it. A handler must not perform
// unknown external effects without an idempotency key or reconciliation path.
//
// Only the claim statement and the handler observe ctx. The claim holds
// nothing while it waits for the claim turn, for its turn in the store's
// writer queue (#214) or for the write lock, so a consumer canceled meanwhile
// is reported as ErrConsumerLost. None of these waits is interrupted by ctx
// (the transactions deliberately are not canceled with it), so that report
// can take up to the busy timeout for each. A consumer lost after the
// attempt started gives the attempt back: before the handler runs, without
// a deferral; while it runs, charging a deferral instead.
// Everything after the claim runs on a context ctx cannot cancel, so losing
// the consumer rolls the handler's transaction back synchronously before
// ProcessOnce returns instead of leaving database/sql to abort it in the
// background while the write lock is still held.
//
// A handler that panics, or calls runtime.Goexit, does not return, and
// neither does ProcessOnce: the panic reaches its caller with its original
// value; ProcessOnce does not recover it (#267). On the way out the store
// rolls the handle transaction back, the handler's writes with it, and the
// attempt fails as HandlerPanicked: the job is retried after a backoff, or
// dead once its attempts are spent. The claim turn is held until that
// accounting has committed.
//
// The handler runs inside a write transaction, which holds the store's single
// write lock until it commits: handlers of concurrent workers run one at a
// time, and every other writer waits for them under the busy timeout. Keep
// handlers short; a writer that waits longer than the busy timeout still
// fails with SQLITE_BUSY (ADR 0003).
func (q *Queue) ProcessOnce(ctx context.Context, handler Handler) (bool, error) {
	if handler == nil {
		return false, errors.New("worker: handler is required")
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrConsumerLost, err)
	}
	txCtx := context.WithoutCancel(ctx)
	release, err := q.takeClaimTurn()
	if err != nil {
		if ctx.Err() != nil {
			// The consumer was lost while this worker waited for its claim
			// turn: it claimed nothing, so the job is untouched.
			err = fmt.Errorf("%w: %w", ErrConsumerLost, errors.Join(ctx.Err(), err))
		}
		return false, fmt.Errorf("worker: process: %w", err)
	}
	defer release()
	var job Job
	var lease int64
	abandoned := false
	// The claim writes first (#176), so it takes its turn in the store's
	// writer queue (#214).
	err = q.withTx(store.Writer(txCtx), func(tx *sql.Tx) error {
		var err error
		job, lease, err = q.claim(ctx, tx)
		if errors.Is(err, ErrNotFound) {
			return err
		}
		if err != nil && ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrConsumerLost, ctx.Err())
		}
		if err != nil {
			return err
		}
		if job.Attempt <= job.MaxAttempts {
			return nil
		}
		// Every attempt this job had was started and none finished: each
		// worker that held it died (or lost its lease) before acknowledging.
		abandoned = true
		_, err = tx.ExecContext(txCtx, `UPDATE worker_jobs SET state = ?, attempt = max_attempts, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ?`, StateDead, ClaimAbandoned, q.now(), job.ID)
		return err
	})
	if errors.Is(err, ErrNotFound) {
		return false, nil
	}
	if err != nil {
		if !errors.Is(err, ErrConsumerLost) && ctx.Err() != nil {
			// The consumer was lost while this worker waited for its write
			// turn (#214): it claimed nothing, so the job is untouched.
			err = fmt.Errorf("%w: %w", ErrConsumerLost, errors.Join(ctx.Err(), err))
		}
		return false, fmt.Errorf("worker: process: %w", err)
	}
	if abandoned {
		return true, nil
	}
	return q.handle(ctx, txCtx, handler, job, lease)
}

// handle runs the handler for a started attempt, in a transaction that takes
// over the attempt's lease and commits the handler's writes with the job's
// outcome.
func (q *Queue) handle(ctx, txCtx context.Context, handler Handler, job Job, lease int64) (bool, error) {
	// held identifies this transaction's hold on the job. The fail-closed Tx
	// checks for it: once SQLite has rolled the transaction back, the job
	// shows the started attempt's lease again, not held (#180).
	held := lease + 1
	ran, lost, died := false, false, false
	// inHandler is true while the handler runs. Still true once the handle
	// transaction has unwound, it means the handler panicked or called
	// runtime.Goexit instead of returning (#267).
	inHandler := false
	var activeDomain *claimedWriteDomain
	live := liveHandler{turn: q.claimTurn, job: job.ID}
	defer func() {
		liveHandlers.Delete(live)
		if activeDomain != nil {
			activeDomain.active.Store(false)
		}
		if !inHandler {
			return
		}
		// The store rolled the handle transaction back on the way out, the
		// handler's writes with it. The attempt, counted when it started,
		// fails: the job is retried after a backoff, or dead once its
		// attempts are spent, as for any handle transaction that rolled back
		// after its handler ran (ADR 0006). The panic value is not recorded.
		// The panic, or the Goexit, then continues to the caller unchanged,
		// still holding the claim turn until this accounting has committed.
		// If it cannot commit, the attempt stays counted and the job is
		// redelivered once the start's lease expires, as after a crash.
		_ = q.chargeAttempt(txCtx, job, lease, HandlerPanicked)
	}()
	// This transaction also writes first (#176), taking the lease over, so it
	// too takes its turn in the store's writer queue (#214); the handler then
	// runs inside that turn, as it runs inside the write lock.
	err := q.withTx(store.Writer(txCtx), func(tx *sql.Tx) error {
		result, err := tx.ExecContext(txCtx, `UPDATE worker_jobs SET lease_until = ?, updated_at = ? WHERE job_id = ? AND state = ? AND lease_until = ?`, held, q.now(), job.ID, StateProcessing, lease)
		if err != nil {
			return err
		}
		if taken, err := result.RowsAffected(); err != nil {
			return err
		} else if taken == 0 {
			return errLeaseExpired
		}
		if err := ctx.Err(); err != nil {
			// The consumer was lost before the handler started: it does not
			// run on a context that is already canceled, and the attempt is
			// given back.
			return fmt.Errorf("%w: %w", ErrConsumerLost, err)
		}
		// Every statement after the claim goes through the claim's
		// fail-closed transaction: once SQLite has ended it, nothing more
		// runs, so nothing can commit outside the claim.
		claimed := &claimTx{tx: tx, jobID: job.ID, lease: held}
		ended := func(err error) error {
			if claimed.lost() {
				died = true
				return ErrClaimLost
			}
			return err
		}
		if _, err := claimed.exec(txCtx, `SAVEPOINT worker_handler`); err != nil {
			return ended(err)
		}
		handlerCtx := ctx
		if job.Trace.Valid() {
			handlerCtx = observe.WithTrace(handlerCtx, job.Trace)
		}
		if q.writeDomain != nil {
			activeDomain = &claimedWriteDomain{domain: q.writeDomain}
			activeDomain.active.Store(true)
			handlerCtx = context.WithValue(handlerCtx, claimedWriteDomainKey{}, activeDomain)
		}
		ran, inHandler = true, true
		liveHandlers.Store(live, struct{}{})
		handlerErr := handler(handlerCtx, Tx{claim: claimed}, job)
		inHandler = false
		if ctx.Err() != nil {
			// Returning an error discards the handler's writes with its
			// outcome: the delivery was never acknowledged.
			lost = true
			return fmt.Errorf("%w: %w", ErrConsumerLost, ctx.Err())
		}
		if claimed.lost() {
			return ended(ErrClaimLost)
		}
		if handlerErr == nil {
			if _, err := claimed.exec(txCtx, `RELEASE SAVEPOINT worker_handler`); err != nil {
				return ended(err)
			}
			_, err = claimed.exec(txCtx, `UPDATE worker_jobs SET state = ?, lease_until = NULL, updated_at = ? WHERE job_id = ? AND state = ?`, StateCompleted, q.now(), job.ID, StateProcessing)
			return ended(err)
		}
		if _, err := claimed.exec(txCtx, `ROLLBACK TO SAVEPOINT worker_handler`); err != nil {
			return ended(err)
		}
		if _, err := claimed.exec(txCtx, `RELEASE SAVEPOINT worker_handler`); err != nil {
			return ended(err)
		}
		// Store saturation is backpressure. Enqueue diagnoses a submission
		// to this claim's own write domain as ErrNestedSubmission before it
		// waits when the handler's context reaches it. When it did not (a
		// detached context, or a wrapper that hides WriteDomain) the
		// submission waits out the busy timeout and names the domain it
		// waited on: if that is this claim's, the claim itself held the
		// lock, and deferring would only repeat the wait (#207). A handler
		// may join that failure with saturation from another store, so the
		// job fails if any branch of the error names this claim's domain,
		// not only the first. Busy errors and deadlines from other domains
		// alone defer normally.
		if errors.Is(handlerErr, trigger.ErrSaturated) && !errors.Is(handlerErr, ErrNestedSubmission) {
			for _, domain := range store.ErrorWriteDomains(handlerErr) {
				if store.SameWriteDomain(domain, q.writeDomain) {
					handlerErr = fmt.Errorf("%w: %w", ErrNestedSubmission, handlerErr)
					break
				}
			}
		}
		if errors.Is(handlerErr, trigger.ErrSaturated) && !errors.Is(handlerErr, ErrNestedSubmission) {
			// Saturation is backpressure, not a handler failure: the job is
			// deferred without consuming an attempt, within its deferral budget.
			state, message := StatePending, ""
			if job.Deferrals+1 > MaxDeferrals {
				state, message = StateDead, DeferralExhausted
			}
			_, err = claimed.exec(txCtx, `UPDATE worker_jobs SET state = ?, attempt = attempt - 1, deferrals = deferrals + 1, available_at = ?, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ?`, state, q.now()+int64(deferralDelay(job.Deferrals+1)), message, q.now(), job.ID, StateProcessing)
			return ended(err)
		}
		message := safeMessage(handlerErr)
		state := StateDead
		available := q.now()
		if retryable(handlerErr) && job.Attempt < job.MaxAttempts {
			state = StatePending
			available += int64(time.Duration(job.Attempt) * time.Second)
		}
		_, err = claimed.exec(txCtx, `UPDATE worker_jobs SET state = ?, available_at = ?, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ?`, state, available, message, q.now(), job.ID, StateProcessing)
		return ended(err)
	})
	if activeDomain != nil {
		// The transaction has committed or rolled back; retained handler
		// contexts no longer represent a held write lock.
		activeDomain.active.Store(false)
	}
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, errLeaseExpired):
		// Another worker claimed the job after this attempt's lease ran out
		// while this worker waited for its write turn; that worker owns it now.
		return false, fmt.Errorf("worker: process: %w: the attempt's lease expired before its handler started", ErrClaimLost)
	case !ran:
		// The handler never ran (the transaction could not take the lease
		// over, or the consumer was lost first): the attempt is given back
		// and the job released at once.
		if ctx.Err() != nil && !errors.Is(err, ErrConsumerLost) {
			err = fmt.Errorf("%w: %w", ErrConsumerLost, errors.Join(ctx.Err(), err))
		}
		if releaseErr := q.release(txCtx, job, lease); releaseErr != nil {
			err = errors.Join(err, releaseErr)
		}
		return false, fmt.Errorf("worker: process: %w", err)
	case lost:
		if deferErr := q.deferLost(txCtx, job, lease); deferErr != nil {
			err = errors.Join(err, deferErr)
		}
		return false, fmt.Errorf("worker: process: %w", err)
	case died:
		// The claim's transaction ended and took the handler's writes with
		// it. The attempt was counted when it started; the job is retried
		// after a backoff, or dead-lettered once its attempts are spent.
		if chargeErr := q.chargeLostClaim(txCtx, job, lease); chargeErr != nil {
			return true, fmt.Errorf("worker: process: %w", errors.Join(err, chargeErr))
		}
		return true, nil
	default:
		// The handler ran but its transaction did not commit: the attempt
		// failed and was counted when it started.
		if chargeErr := q.chargeLostClaim(txCtx, job, lease); chargeErr != nil {
			err = errors.Join(err, chargeErr)
		}
		return false, fmt.Errorf("worker: process: %w", err)
	}
}

// claimEnded is the error recorded for an attempt whose handle transaction
// ended under its handler, or failed to commit.
const claimEnded = "claim transaction ended"

// errLeaseExpired reports that a started attempt's lease was taken by
// another worker before this worker's handler transaction could take it over.
var errLeaseExpired = errors.New("worker: lease expired")

// chargeLostClaim ends a started attempt whose handler transaction did not
// commit as a failed attempt: the job is retried after a backoff, or dead
// once its attempts are spent. The attempt was counted when it started, so a
// crash before this commits still counts it; the job is then redelivered
// once the attempt's lease expires.
func (q *Queue) chargeLostClaim(ctx context.Context, job Job, lease int64) error {
	return q.chargeAttempt(ctx, job, lease, claimEnded)
}

// chargeAttempt ends a started attempt whose handler ran and whose handle
// transaction rolled back as a failed attempt, recording reason as the
// job's error.
func (q *Queue) chargeAttempt(ctx context.Context, job Job, lease int64, reason string) error {
	return q.withTx(store.Writer(ctx), func(tx *sql.Tx) error {
		state, available := StateDead, q.now()
		if job.Attempt < job.MaxAttempts {
			state = StatePending
			available += int64(time.Duration(job.Attempt) * time.Second)
		}
		_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET state = ?, available_at = ?, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ? AND lease_until = ?`, state, available, reason, q.now(), job.ID, StateProcessing, lease)
		return err
	})
}

// deferLost gives a lost consumer's attempt back and charges the job's
// deferral budget instead, after the handler's transaction has been rolled
// back. A crash between the two leaves the attempt counted, and the job is
// redelivered once its lease expires; it is never acknowledged.
func (q *Queue) deferLost(ctx context.Context, job Job, lease int64) error {
	return q.withTx(store.Writer(ctx), func(tx *sql.Tx) error {
		state, message := StatePending, ""
		if job.Deferrals+1 > MaxDeferrals {
			state, message = StateDead, DeferralExhausted
		}
		_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET state = ?, attempt = attempt - 1, deferrals = deferrals + 1, available_at = ?, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ? AND lease_until = ?`, state, q.now()+int64(deferralDelay(job.Deferrals+1)), message, q.now(), job.ID, StateProcessing, lease)
		return err
	})
}

// release gives back a started attempt whose handler never ran, and makes
// the job available again at once. If it cannot commit, the attempt stays
// counted and the job is redelivered once its lease expires.
func (q *Queue) release(ctx context.Context, job Job, lease int64) error {
	return q.withTx(store.Writer(ctx), func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET state = ?, attempt = attempt - 1, lease_until = NULL, updated_at = ? WHERE job_id = ? AND state = ? AND lease_until = ?`, StatePending, q.now(), job.ID, StateProcessing, lease)
		return err
	})
}

// Submit implements trigger.Submitter on the durable queue: the submission
// is committed (or found already committed) before Submit returns.
func (q *Queue) Submit(ctx context.Context, submission trigger.Submission) (bool, error) {
	result, err := q.Enqueue(ctx, EnqueueRequest{RequestKey: submission.Key, Kind: submission.Kind, Payload: submission.Payload, Principal: submission.Principal, Trace: submission.Trace})
	if err != nil {
		return false, err
	}
	return result.Accepted, nil
}

var _ trigger.Submitter = (*Queue)(nil)

// encodeTrace stores a valid trace context as its canonical traceparent and
// tracestate, and anything else as empty. An invalid tracestate, or a member
// of it the redaction boundary flags, is dropped rather than discarding the
// context.
func encodeTrace(trace observe.TraceContext) (traceparent, tracestate string) {
	if !trace.TraceID.IsValid() || !trace.SpanID.IsValid() {
		return "", ""
	}
	trace.Flags &= observe.FlagSampled
	trace.State = trigger.SafeTracestate(trace.State)
	return trace.Traceparent(), trace.State
}

// decodeTrace reads a stored trace context. A value that does not parse (a
// row written by something other than encodeTrace) is ignored, never an
// error: a job is never refused for its correlation data.
func decodeTrace(traceparent, tracestate string) observe.TraceContext {
	trace, ok := observe.ExtractTrace([]string{traceparent}, []string{tracestate})
	if !ok {
		return observe.TraceContext{}
	}
	trace.State = trigger.SafeTracestate(trace.State)
	return trace
}

// encodePrincipal stores an empty principal as "" so jobs from producers that
// establish none compare equal, and sorts roles so the same principal always
// encodes the same way.
func encodePrincipal(principal trigger.Principal) (string, error) {
	if principal.ID == "" && len(principal.Roles) == 0 {
		return "", nil
	}
	roles := append([]string(nil), principal.Roles...)
	sort.Strings(roles)
	principal.Roles = slices.Compact(roles)
	data, err := json.Marshal(principal)
	if err != nil {
		return "", fmt.Errorf("worker: principal: %w", err)
	}
	return string(data), nil
}

// Settled reports whether the work submitted under a request key has
// finished or been dead-lettered. A key never submitted, or whose job's
// tombstone has expired, has nothing running and is settled.
func (q *Queue) Settled(ctx context.Context, requestKey string) (bool, error) {
	job, err := q.Get(ctx, requestKey)
	if errors.Is(err, ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	return job.State == StateCompleted || job.State == StateDead, nil
}

// Get reads the job submitted under requestKey. A job Compact erased is
// returned as its tombstone (Job.Compacted); a key never submitted, or whose
// tombstone expired, is ErrNotFound.
func (q *Queue) Get(ctx context.Context, requestKey string) (Job, error) {
	var job Job
	err := q.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		job, err = scanJob(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM worker_jobs WHERE request_key = ?`, requestKey))
		if errors.Is(err, sql.ErrNoRows) {
			job, _, err = compacted(ctx, tx, requestKey)
		}
		return err
	})
	return job, err
}

// claim leases the next available job and counts its attempt: the oldest
// created_at first, and among jobs that share one, the first enqueued
// (#217). A job whose lease expired is available again: its worker started
// an attempt and never finished it. Its first statement
// writes: a
// transaction that reads before writing cannot wait for a concurrent
// writer and fails with SQLITE_BUSY once that writer commits; one that
// writes first waits under the busy timeout (as cron's cursor writes do).
func (q *Queue) claim(ctx context.Context, tx *sql.Tx) (Job, int64, error) {
	now := q.now()
	leaseUntil := time.Unix(0, now).Add(q.lease).UnixNano()
	job, err := scanJob(tx.QueryRowContext(ctx, `UPDATE worker_jobs SET state = ?, attempt = attempt + 1, lease_until = ?, updated_at = ?
		WHERE job_id = (SELECT job_id FROM worker_jobs
			WHERE (state = ? OR (state = ? AND lease_until <= ?)) AND available_at <= ? ORDER BY created_at, enqueue_seq, job_id LIMIT 1)
		RETURNING `+jobColumns,
		StateProcessing, leaseUntil, now, StatePending, StateProcessing, now, now))
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, 0, ErrNotFound
	}
	if err != nil {
		return Job{}, 0, err
	}
	return job, leaseUntil, nil
}

// jobColumns are the columns scanJob reads, in its order.
const jobColumns = `job_id, request_key, kind, payload_json, attempt, max_attempts, deferrals, principal_json, state, error_text, traceparent, tracestate`

func scanJob(row interface{ Scan(...any) error }) (Job, error) {
	var job Job
	var payload []byte
	var principal, traceparent, tracestate string
	if err := row.Scan(&job.ID, &job.RequestKey, &job.Kind, &payload, &job.Attempt, &job.MaxAttempts, &job.Deferrals, &principal, &job.State, &job.Error, &traceparent, &tracestate); err != nil {
		return Job{}, err
	}
	job.Trace = decodeTrace(traceparent, tracestate)
	if principal != "" {
		if err := json.Unmarshal([]byte(principal), &job.Principal); err != nil {
			return Job{}, fmt.Errorf("worker: stored principal: %w", err)
		}
	}
	job.Payload = append([]byte(nil), payload...)
	return job, nil
}

func (q *Queue) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return q.database.WithTx(ctx, fn)
}
func (q *Queue) now() int64     { return q.clock().UTC().UnixNano() }
func digest(data []byte) string { sum := sha256.Sum256(data); return fmt.Sprintf("%x", sum[:]) }

// retryable reports whether a failed job gets another attempt. A nested
// submission never does, even when joined with a retryable HandlerError:
// every attempt would submit to its own claimed store again (#225).
func retryable(err error) bool {
	if errors.Is(err, ErrNestedSubmission) {
		return false
	}
	var target *HandlerError
	return errors.As(err, &target) && target.Retryable
}

// safeMessage records only framework-controlled text: an explicit
// HandlerError message, a classified error's stable code, or a generic
// diagnostic. Arbitrary error text never reaches the dead-letter record.
func safeMessage(err error) string {
	if errors.Is(err, ErrNestedSubmission) {
		return "nested submission to claimed store; use worker.Tx for atomic writes"
	}
	var target *HandlerError
	if errors.As(err, &target) && target.Message != "" {
		return target.Message
	}
	if code, _, ok := trigger.Classify(err); ok && code != "" {
		return code
	}
	return "job handler failed"
}
