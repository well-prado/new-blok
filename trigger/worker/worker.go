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
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
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
	State     string
	Error     string
}

const (
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

type Handler func(context.Context, *sql.Tx, Job) error

type Queue struct {
	database store.Database
	clock    func() time.Time
	mu       sync.RWMutex
	schemas  map[string]schema.Schema
}

func New(ctx context.Context, database store.Database, clock func() time.Time) (*Queue, error) {
	if database == nil {
		return nil, errors.New("worker: database is required")
	}
	if clock == nil {
		clock = time.Now
	}
	queue := &Queue{database: database, clock: clock, schemas: map[string]schema.Schema{}}
	if err := queue.withTx(ctx, func(tx *sql.Tx) error {
		if err := createJobs(ctx, tx); err != nil {
			return err
		}
		if err := ensureColumn(ctx, tx, "deferrals", "INTEGER NOT NULL DEFAULT 0"); err != nil {
			return err
		}
		return ensureColumn(ctx, tx, "principal_json", "TEXT NOT NULL DEFAULT ''")
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
		updated_at INTEGER NOT NULL
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
	var result EnqueueResult
	err = q.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO worker_jobs
			(job_id, request_key, kind, payload_json, payload_digest, max_attempts, principal_json, state, available_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`,
			jobID, request.RequestKey, request.Kind, []byte(request.Payload), payloadDigest, request.MaxAttempts, principal, StatePending, q.now(), q.now(), q.now())
		if err != nil {
			return err
		}
		count, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			job, err := scanJob(tx.QueryRowContext(ctx, `SELECT job_id, request_key, kind, payload_json, attempt, max_attempts, deferrals, principal_json, state, error_text FROM worker_jobs WHERE request_key = ?`, request.RequestKey))
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
		result = EnqueueResult{Job: Job{ID: jobID, RequestKey: request.RequestKey, Kind: request.Kind, Payload: append([]byte(nil), request.Payload...), MaxAttempts: request.MaxAttempts, Principal: request.Principal, State: StatePending}, Accepted: true}
		return nil
	})
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("worker: enqueue: %w", err)
	}
	return result, nil
}

// ErrConsumerLost reports that the consumer's context ended while a claimed
// job was being handled. The claim and every handler write were rolled back,
// so the job is delivered again without consuming an attempt.
var ErrConsumerLost = errors.New("worker: consumer lost before acknowledgment")

// ProcessOnce claims and processes one job. The handler and acknowledgment are
// in the same transaction, so a crash rolls back both the business write and
// the delivery acknowledgment. A handler must not perform unknown external
// effects without an idempotency key or reconciliation path.
//
// Only the handler observes ctx. The transaction runs on a context ctx cannot
// cancel, so losing the consumer rolls the claim back synchronously before
// ProcessOnce returns instead of leaving database/sql to abort it in the
// background while the write lock is still held.
func (q *Queue) ProcessOnce(ctx context.Context, handler Handler) (bool, error) {
	if handler == nil {
		return false, errors.New("worker: handler is required")
	}
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("%w: %w", ErrConsumerLost, err)
	}
	txCtx := context.WithoutCancel(ctx)
	processed := false
	var lost Job
	err := q.withTx(txCtx, func(tx *sql.Tx) error {
		job, err := q.claim(txCtx, tx)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		processed = true
		if _, err := tx.ExecContext(txCtx, `SAVEPOINT worker_handler`); err != nil {
			return err
		}
		handlerErr := handler(ctx, tx, job)
		if ctx.Err() != nil {
			// Returning an error discards the claim with the handler's
			// writes: the delivery was never acknowledged.
			lost = job
			return fmt.Errorf("%w: %w", ErrConsumerLost, ctx.Err())
		}
		if handlerErr == nil {
			if _, err := tx.ExecContext(txCtx, `RELEASE SAVEPOINT worker_handler`); err != nil {
				return err
			}
			_, err = tx.ExecContext(txCtx, `UPDATE worker_jobs SET state = ?, lease_until = NULL, updated_at = ? WHERE job_id = ? AND state = ?`, StateCompleted, q.now(), job.ID, StateProcessing)
			return err
		}
		if _, err := tx.ExecContext(txCtx, `ROLLBACK TO SAVEPOINT worker_handler`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(txCtx, `RELEASE SAVEPOINT worker_handler`); err != nil {
			return err
		}
		if errors.Is(handlerErr, trigger.ErrSaturated) {
			// Saturation is backpressure, not a handler failure: the job is
			// deferred without consuming an attempt, within its deferral budget.
			state, message := StatePending, ""
			if job.Deferrals+1 > MaxDeferrals {
				state, message = StateDead, DeferralExhausted
			}
			_, err = tx.ExecContext(txCtx, `UPDATE worker_jobs SET state = ?, attempt = attempt - 1, deferrals = deferrals + 1, available_at = ?, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ?`, state, q.now()+int64(deferralDelay(job.Deferrals+1)), message, q.now(), job.ID, StateProcessing)
			return err
		}
		message := safeMessage(handlerErr)
		state := StateDead
		available := q.now()
		if retryable(handlerErr) && job.Attempt < job.MaxAttempts {
			state = StatePending
			available += int64(time.Duration(job.Attempt) * time.Second)
		}
		_, err = tx.ExecContext(txCtx, `UPDATE worker_jobs SET state = ?, available_at = ?, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ?`, state, available, message, q.now(), job.ID, StateProcessing)
		return err
	})
	if errors.Is(err, ErrConsumerLost) && lost.ID != "" {
		if deferErr := q.deferLost(txCtx, lost); deferErr != nil {
			err = errors.Join(err, deferErr)
		}
	}
	if err != nil {
		return false, fmt.Errorf("worker: process: %w", err)
	}
	return processed, nil
}

// deferLost charges a lost consumer against the job's deferral budget after
// the claim has been rolled back. A crash between the two leaves one
// uncounted redelivery, never an acknowledged one.
func (q *Queue) deferLost(ctx context.Context, job Job) error {
	return q.withTx(ctx, func(tx *sql.Tx) error {
		state, message := StatePending, ""
		if job.Deferrals+1 > MaxDeferrals {
			state, message = StateDead, DeferralExhausted
		}
		_, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET state = ?, deferrals = deferrals + 1, available_at = ?, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ?`, state, q.now()+int64(deferralDelay(job.Deferrals+1)), message, q.now(), job.ID, StatePending)
		return err
	})
}

// Submit implements trigger.Submitter on the durable queue: the submission
// is committed (or found already committed) before Submit returns.
func (q *Queue) Submit(ctx context.Context, submission trigger.Submission) (bool, error) {
	result, err := q.Enqueue(ctx, EnqueueRequest{RequestKey: submission.Key, Kind: submission.Kind, Payload: submission.Payload, Principal: submission.Principal})
	if err != nil {
		return false, err
	}
	return result.Accepted, nil
}

var _ trigger.Submitter = (*Queue)(nil)

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

func (q *Queue) Get(ctx context.Context, requestKey string) (Job, error) {
	var job Job
	err := q.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		job, err = scanJob(tx.QueryRowContext(ctx, `SELECT job_id, request_key, kind, payload_json, attempt, max_attempts, deferrals, principal_json, state, error_text FROM worker_jobs WHERE request_key = ?`, requestKey))
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

func (q *Queue) claim(ctx context.Context, tx *sql.Tx) (Job, error) {
	var job Job
	row := tx.QueryRowContext(ctx, `SELECT job_id, request_key, kind, payload_json, attempt, max_attempts, deferrals, principal_json, state, error_text FROM worker_jobs
		WHERE (state = ? OR (state = ? AND lease_until <= ?)) AND available_at <= ? ORDER BY created_at, job_id LIMIT 1`, StatePending, StateProcessing, q.now(), q.now())
	if scanned, err := scanJob(row); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, err
	} else {
		job = scanned
	}
	leaseUntil := time.Unix(0, q.now()).Add(30 * time.Second).UnixNano()
	result, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET state = ?, attempt = attempt + 1, lease_until = ?, updated_at = ? WHERE job_id = ? AND (state = ? OR (state = ? AND lease_until <= ?))`, StateProcessing, leaseUntil, q.now(), job.ID, StatePending, StateProcessing, q.now())
	if err != nil {
		return Job{}, err
	}
	if changed, _ := result.RowsAffected(); changed != 1 {
		return Job{}, ErrNotFound
	}
	job.Attempt++
	job.State = StateProcessing
	return job, nil
}

func scanJob(row interface{ Scan(...any) error }) (Job, error) {
	var job Job
	var payload []byte
	var principal string
	if err := row.Scan(&job.ID, &job.RequestKey, &job.Kind, &payload, &job.Attempt, &job.MaxAttempts, &job.Deferrals, &principal, &job.State, &job.Error); err != nil {
		return Job{}, err
	}
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
func retryable(err error) bool {
	var target *HandlerError
	return errors.As(err, &target) && target.Retryable
}

// safeMessage records only framework-controlled text: an explicit
// HandlerError message, a classified error's stable code, or a generic
// diagnostic. Arbitrary error text never reaches the dead-letter record.
func safeMessage(err error) string {
	var target *HandlerError
	if errors.As(err, &target) && target.Message != "" {
		return target.Message
	}
	if code, _, ok := trigger.Classify(err); ok && code != "" {
		return code
	}
	return "job handler failed"
}
