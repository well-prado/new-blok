// Package worker implements the initial local durable worker trigger.
package worker

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/store"
)

var (
	ErrRequestConflict = errors.New("worker: request key conflicts with existing payload")
	ErrNotFound        = errors.New("worker: job not found")
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
	State       string
	Error       string
}

type EnqueueRequest struct {
	RequestKey  string
	Kind        string
	Payload     json.RawMessage
	MaxAttempts int
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
}

func New(ctx context.Context, database store.Database, clock func() time.Time) (*Queue, error) {
	if database == nil {
		return nil, errors.New("worker: database is required")
	}
	if clock == nil {
		clock = time.Now
	}
	queue := &Queue{database: database, clock: clock}
	if err := queue.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS worker_jobs (
			job_id TEXT PRIMARY KEY,
			request_key TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL,
			payload_json BLOB NOT NULL,
			payload_digest TEXT NOT NULL,
			attempt INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL,
			state TEXT NOT NULL,
			available_at INTEGER NOT NULL,
			lease_until INTEGER,
			error_text TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL
		)`)
		return err
	}); err != nil {
		return nil, fmt.Errorf("worker: schema: %w", err)
	}
	return queue, nil
}

func (q *Queue) Enqueue(ctx context.Context, request EnqueueRequest) (EnqueueResult, error) {
	if request.RequestKey == "" || request.Kind == "" || !json.Valid(request.Payload) {
		return EnqueueResult{}, errors.New("worker: request key, kind and valid JSON payload are required")
	}
	if request.MaxAttempts <= 0 {
		request.MaxAttempts = 3
	}
	payloadDigest := digest(request.Payload)
	jobID := fmt.Sprintf("job:%d:%s", q.clock().UnixNano(), payloadDigest[:16])
	var result EnqueueResult
	err := q.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, `INSERT INTO worker_jobs
			(job_id, request_key, kind, payload_json, payload_digest, max_attempts, state, available_at, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`,
			jobID, request.RequestKey, request.Kind, []byte(request.Payload), payloadDigest, request.MaxAttempts, StatePending, q.now(), q.now(), q.now())
		if err != nil {
			return err
		}
		count, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if count == 0 {
			job, err := scanJob(tx.QueryRowContext(ctx, `SELECT job_id, request_key, kind, payload_json, attempt, max_attempts, state, error_text FROM worker_jobs WHERE request_key = ?`, request.RequestKey))
			if err != nil {
				return err
			}
			var existingDigest string
			if err := tx.QueryRowContext(ctx, `SELECT payload_digest FROM worker_jobs WHERE request_key = ?`, request.RequestKey).Scan(&existingDigest); err != nil {
				return err
			}
			if job.Kind != request.Kind || existingDigest != payloadDigest {
				return ErrRequestConflict
			}
			result = EnqueueResult{Job: job, Accepted: false}
			return nil
		}
		result = EnqueueResult{Job: Job{ID: jobID, RequestKey: request.RequestKey, Kind: request.Kind, Payload: append([]byte(nil), request.Payload...), MaxAttempts: request.MaxAttempts, State: StatePending}, Accepted: true}
		return nil
	})
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("worker: enqueue: %w", err)
	}
	return result, nil
}

// ProcessOnce claims and processes one job. The handler and acknowledgment are
// in the same transaction, so a crash rolls back both the business write and
// the delivery acknowledgment. A handler must not perform unknown external
// effects without an idempotency key or reconciliation path.
func (q *Queue) ProcessOnce(ctx context.Context, handler Handler) (bool, error) {
	if handler == nil {
		return false, errors.New("worker: handler is required")
	}
	processed := false
	err := q.withTx(ctx, func(tx *sql.Tx) error {
		job, err := q.claim(ctx, tx)
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		processed = true
		if _, err := tx.ExecContext(ctx, `SAVEPOINT worker_handler`); err != nil {
			return err
		}
		handlerErr := handler(ctx, tx, job)
		if handlerErr == nil {
			if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT worker_handler`); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE worker_jobs SET state = ?, lease_until = NULL, updated_at = ? WHERE job_id = ? AND state = ?`, StateCompleted, q.now(), job.ID, StateProcessing)
			return err
		}
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT worker_handler`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT worker_handler`); err != nil {
			return err
		}
		message := safeMessage(handlerErr)
		state := StateDead
		available := q.now()
		if retryable(handlerErr) && job.Attempt < job.MaxAttempts {
			state = StatePending
			available += int64(time.Duration(job.Attempt) * time.Second)
		}
		_, err = tx.ExecContext(ctx, `UPDATE worker_jobs SET state = ?, available_at = ?, lease_until = NULL, error_text = ?, updated_at = ? WHERE job_id = ? AND state = ?`, state, available, message, q.now(), job.ID, StateProcessing)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return false, fmt.Errorf("worker: process: %w", err)
	}
	return processed, nil
}

func (q *Queue) Get(ctx context.Context, requestKey string) (Job, error) {
	var job Job
	err := q.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		job, err = scanJob(tx.QueryRowContext(ctx, `SELECT job_id, request_key, kind, payload_json, attempt, max_attempts, state, error_text FROM worker_jobs WHERE request_key = ?`, requestKey))
		return err
	})
	if errors.Is(err, sql.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return job, err
}

func (q *Queue) claim(ctx context.Context, tx *sql.Tx) (Job, error) {
	var job Job
	row := tx.QueryRowContext(ctx, `SELECT job_id, request_key, kind, payload_json, attempt, max_attempts, state, error_text FROM worker_jobs
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
	if err := row.Scan(&job.ID, &job.RequestKey, &job.Kind, &payload, &job.Attempt, &job.MaxAttempts, &job.State, &job.Error); err != nil {
		return Job{}, err
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
func safeMessage(err error) string {
	var target *HandlerError
	if errors.As(err, &target) && target.Message != "" {
		return target.Message
	}
	return "job handler failed"
}
