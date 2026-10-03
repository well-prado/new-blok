// Package journal persists accepted work and effect transitions.
package journal

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/store"
)

var (
	ErrRequestConflict = errors.New("journal: request key conflicts with existing input")
	ErrNotFound        = errors.New("journal: record not found")
	ErrNotDispatchable = errors.New("journal: operation is not dispatchable")
	ErrStaleAttempt    = errors.New("journal: stale attempt cannot publish")
	ErrUncertain       = errors.New("journal: operation outcome is uncertain")
)

const (
	runAccepted         = "accepted"
	runCompleted        = "completed"
	operationIntent     = "intent"
	operationDispatched = "dispatched"
	operationCommitted  = "committed"
	operationUncertain  = "uncertain"
	operationFailed     = "failed"
	attemptDispatched   = "dispatched"
	attemptFailed       = "failed"
	attemptCommitted    = "committed"
	attemptUncertain    = "uncertain"
)

// Hooks are test-only crash barriers. Production callers leave them nil.
type Hooks struct {
	BeforeCommit func(string)
	AfterCommit  func(string)
}

type Config struct {
	Clock func() time.Time
	Hooks Hooks
}

type Journal struct {
	database store.Database
	clock    func() time.Time
	hooks    Hooks
}

type AdmissionRequest struct {
	RequestKey     string
	Workflow       string
	ArtifactDigest string
	Input          json.RawMessage
}

type Admission struct {
	RunID      string
	RequestKey string
	Accepted   bool
	State      string
	ReplayOf   string
}

type Run struct {
	RunID          string
	RequestKey     string
	Workflow       string
	ArtifactDigest string
	Input          json.RawMessage
	InputDigest    string
	State          string
	ReplayOf       string
	Output         json.RawMessage
}

type OperationIdentity struct {
	RunID          string
	ArtifactDigest string
	InvocationPath string
	IterationPath  string
}

func (i OperationIdentity) Key() string {
	data, _ := json.Marshal(i)
	digest := sha256.Sum256(data)
	return "op:" + hex.EncodeToString(digest[:])
}

type EffectIntent struct {
	Identity OperationIdentity
}

type Operation struct {
	Key                  string
	Identity             OperationIdentity
	ProviderOperationKey string
	State                string
	CurrentAttemptID     string
	Result               json.RawMessage
}

type Attempt struct {
	ID                   string
	OperationKey         string
	AttemptNumber        int
	ProviderOperationKey string
	State                string
	Result               json.RawMessage
}

type EffectCommit struct {
	OperationKey string
	AttemptID    string
	Result       json.RawMessage
}

func New(ctx context.Context, database store.Database, config Config) (*Journal, error) {
	if database == nil {
		return nil, errors.New("journal: database is required")
	}
	if err := database.Integrity(ctx); err != nil {
		return nil, fmt.Errorf("journal: integrity check: %w", err)
	}
	j := &Journal{database: database, clock: config.Clock, hooks: config.Hooks}
	if j.clock == nil {
		j.clock = time.Now
	}
	if err := j.withTx(ctx, "schema", func(tx *sql.Tx) error {
		for _, statement := range schemaStatements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("schema: %w", err)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return j, nil
}

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS journal_runs (
		run_id TEXT PRIMARY KEY,
		request_key TEXT NOT NULL UNIQUE,
		workflow TEXT NOT NULL,
		artifact_digest TEXT NOT NULL,
		input_json BLOB NOT NULL,
		input_digest TEXT NOT NULL,
		state TEXT NOT NULL,
		replay_of TEXT NOT NULL DEFAULT '',
		output_json BLOB,
		created_at INTEGER NOT NULL,
		completed_at INTEGER
	)`,
	`CREATE TABLE IF NOT EXISTS journal_operations (
		operation_key TEXT PRIMARY KEY,
		run_id TEXT NOT NULL,
		artifact_digest TEXT NOT NULL,
		invocation_path TEXT NOT NULL,
		iteration_path TEXT NOT NULL,
		provider_operation_key TEXT NOT NULL,
		state TEXT NOT NULL,
		current_attempt_id TEXT NOT NULL DEFAULT '',
		result_json BLOB,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		FOREIGN KEY (run_id) REFERENCES journal_runs(run_id)
	)`,
	`CREATE TABLE IF NOT EXISTS journal_attempts (
		attempt_id TEXT PRIMARY KEY,
		operation_key TEXT NOT NULL,
		attempt_number INTEGER NOT NULL,
		provider_operation_key TEXT NOT NULL,
		state TEXT NOT NULL,
		result_json BLOB,
		error_text TEXT NOT NULL DEFAULT '',
		started_at INTEGER NOT NULL,
		finished_at INTEGER,
		UNIQUE (operation_key, attempt_number),
		FOREIGN KEY (operation_key) REFERENCES journal_operations(operation_key)
	)`,
	`CREATE INDEX IF NOT EXISTS journal_operations_run ON journal_operations(run_id)`,
	`CREATE INDEX IF NOT EXISTS journal_attempts_operation ON journal_attempts(operation_key, attempt_number)`,
	`CREATE TABLE IF NOT EXISTS journal_waits (
		wait_id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL,
		name TEXT NOT NULL,
		due_at INTEGER NOT NULL,
		state TEXT NOT NULL,
		signal_id TEXT NOT NULL DEFAULT '',
		payload_json BLOB,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		FOREIGN KEY (run_id) REFERENCES journal_runs(run_id),
		UNIQUE (run_id, name)
	)`,
	`CREATE TABLE IF NOT EXISTS journal_signals (
		run_id TEXT NOT NULL,
		signal_id TEXT NOT NULL,
		name TEXT NOT NULL,
		principal TEXT NOT NULL,
		payload_json BLOB NOT NULL,
		state TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		PRIMARY KEY (run_id, signal_id)
	)`,
	`CREATE INDEX IF NOT EXISTS journal_waits_due ON journal_waits(state, due_at)`,
	`CREATE TABLE IF NOT EXISTS journal_checkpoints (
		run_id TEXT PRIMARY KEY,
		artifact_digest TEXT NOT NULL,
		checkpoint_digest TEXT NOT NULL,
		status TEXT NOT NULL,
		state_json BLOB NOT NULL,
		updated_at INTEGER NOT NULL,
		FOREIGN KEY (run_id) REFERENCES journal_runs(run_id)
	)`,
	`CREATE TABLE IF NOT EXISTS journal_scopes (
		run_id TEXT NOT NULL,
		path TEXT NOT NULL,
		kind TEXT NOT NULL,
		parent_path TEXT NOT NULL DEFAULT '',
		state TEXT NOT NULL,
		output_json BLOB,
		error_text TEXT NOT NULL DEFAULT '',
		updated_at INTEGER NOT NULL,
		PRIMARY KEY (run_id, path),
		FOREIGN KEY (run_id) REFERENCES journal_runs(run_id)
	)`,
	`CREATE TABLE IF NOT EXISTS journal_children (
		run_id TEXT NOT NULL,
		path TEXT NOT NULL,
		child_run_id TEXT NOT NULL,
		state TEXT NOT NULL,
		result_json BLOB,
		PRIMARY KEY (run_id, path),
		FOREIGN KEY (run_id) REFERENCES journal_runs(run_id)
	)`,
	`CREATE TABLE IF NOT EXISTS journal_joins (
		run_id TEXT NOT NULL,
		path TEXT NOT NULL,
		expected INTEGER NOT NULL,
		completed INTEGER NOT NULL,
		results_json BLOB NOT NULL,
		state TEXT NOT NULL,
		PRIMARY KEY (run_id, path),
		FOREIGN KEY (run_id) REFERENCES journal_runs(run_id)
	)`,
	`CREATE TABLE IF NOT EXISTS journal_artifacts (
		digest TEXT PRIMARY KEY,
		version TEXT NOT NULL,
		manifest_json BLOB NOT NULL,
		created_at INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS journal_reconciliations (
		operation_key TEXT PRIMARY KEY,
		actor TEXT NOT NULL,
		evidence TEXT NOT NULL,
		result_json BLOB NOT NULL,
		state TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		FOREIGN KEY (operation_key) REFERENCES journal_operations(operation_key)
	)`,
	`CREATE TABLE IF NOT EXISTS journal_audit (
		audit_id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL,
		request_key TEXT NOT NULL,
		artifact_digest TEXT NOT NULL,
		state TEXT NOT NULL,
		output_json BLOB,
		created_at INTEGER NOT NULL
	)`,
}

func (j *Journal) Admit(ctx context.Context, request AdmissionRequest) (Admission, error) {
	if err := validateAdmission(request); err != nil {
		return Admission{}, err
	}
	digest := digestBytes(request.Input)
	runID, err := randomID("run")
	if err != nil {
		return Admission{}, err
	}
	admission := Admission{RunID: runID, RequestKey: request.RequestKey}
	err = j.withTx(ctx, "admission", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `INSERT INTO journal_runs
			(run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`,
			runID, request.RequestKey, request.Workflow, request.ArtifactDigest, []byte(request.Input), digest, runAccepted, j.now())
		if err != nil {
			return err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		var existing Run
		if inserted == 0 {
			existing, err = scanRun(tx.QueryRowContext(ctx, `SELECT run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, output_json FROM journal_runs WHERE request_key = ?`, request.RequestKey))
			if err != nil {
				return err
			}
			if existing.Workflow != request.Workflow || existing.ArtifactDigest != request.ArtifactDigest || existing.InputDigest != digest {
				return ErrRequestConflict
			}
			admission.RunID = existing.RunID
			admission.Accepted = false
			admission.State = existing.State
			admission.ReplayOf = existing.ReplayOf
			return nil
		}
		admission.Accepted = true
		admission.State = runAccepted
		return nil
	})
	if err != nil {
		return Admission{}, fmt.Errorf("journal: admit: %w", err)
	}
	return admission, nil
}

func (j *Journal) Replay(ctx context.Context, sourceRunID, requestKey string) (Admission, error) {
	if sourceRunID == "" || requestKey == "" {
		return Admission{}, errors.New("journal: source run and replay request key are required")
	}
	runID, err := randomID("run")
	if err != nil {
		return Admission{}, err
	}
	var admission Admission
	err = j.withTx(ctx, "replay", func(tx *sql.Tx) error {
		source, err := scanRun(tx.QueryRowContext(ctx, `SELECT run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, output_json FROM journal_runs WHERE run_id = ?`, sourceRunID))
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO journal_runs
			(run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`,
			runID, requestKey, source.Workflow, source.ArtifactDigest, []byte(source.Input), source.InputDigest, runAccepted, source.RunID, j.now())
		if err != nil {
			return err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if inserted == 0 {
			return ErrRequestConflict
		}
		admission = Admission{RunID: runID, RequestKey: requestKey, Accepted: true, State: runAccepted, ReplayOf: source.RunID}
		return nil
	})
	if err != nil {
		return Admission{}, fmt.Errorf("journal: replay: %w", err)
	}
	return admission, nil
}

func (j *Journal) BeginEffect(ctx context.Context, intent EffectIntent) (Operation, error) {
	if err := intent.Identity.validate(); err != nil {
		return Operation{}, err
	}
	key := intent.Identity.Key()
	var operation Operation
	err := j.withTx(ctx, "effect-intent", func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_operations
			(operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(operation_key) DO NOTHING`,
			key, intent.Identity.RunID, intent.Identity.ArtifactDigest, intent.Identity.InvocationPath, intent.Identity.IterationPath, key, operationIntent, j.now(), j.now()); err != nil {
			return err
		}
		var err error
		operation, err = scanOperation(tx.QueryRowContext(ctx, `SELECT operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, current_attempt_id, result_json FROM journal_operations WHERE operation_key = ?`, key))
		return err
	})
	if err != nil {
		return Operation{}, fmt.Errorf("journal: effect intent: %w", err)
	}
	return operation, nil
}

func (j *Journal) StartAttempt(ctx context.Context, operationKey string) (Attempt, error) {
	if operationKey == "" {
		return Attempt{}, errors.New("journal: operation key is required")
	}
	var attempt Attempt
	err := j.withTx(ctx, "attempt-start", func(tx *sql.Tx) error {
		operation, err := scanOperation(tx.QueryRowContext(ctx, `SELECT operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, current_attempt_id, result_json FROM journal_operations WHERE operation_key = ?`, operationKey))
		if err != nil {
			return err
		}
		if operation.State != operationIntent {
			if operation.State == operationUncertain {
				return ErrUncertain
			}
			return ErrNotDispatchable
		}
		var next int
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM journal_attempts WHERE operation_key = ?`, operationKey).Scan(&next); err != nil {
			return err
		}
		attemptID, err := randomID("attempt")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_attempts (attempt_id, operation_key, attempt_number, provider_operation_key, state, started_at) VALUES (?, ?, ?, ?, ?, ?)`, attemptID, operationKey, next, operation.ProviderOperationKey, attemptDispatched, j.now()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_operations SET state = ?, current_attempt_id = ?, updated_at = ? WHERE operation_key = ? AND state IN (?, ?)`, operationDispatched, attemptID, j.now(), operationKey, operationIntent, operationFailed); err != nil {
			return err
		}
		attempt = Attempt{ID: attemptID, OperationKey: operationKey, AttemptNumber: next, ProviderOperationKey: operation.ProviderOperationKey, State: attemptDispatched}
		return nil
	})
	if err != nil {
		return Attempt{}, fmt.Errorf("journal: start attempt: %w", err)
	}
	return attempt, nil
}

func (j *Journal) FailAttempt(ctx context.Context, operationKey, attemptID string, retryable bool, message string) error {
	if operationKey == "" || attemptID == "" {
		return errors.New("journal: operation and attempt keys are required")
	}
	return j.withTx(ctx, "attempt-fail", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE journal_attempts SET state = ?, error_text = ?, finished_at = ? WHERE attempt_id = ? AND operation_key = ? AND state = ?`, attemptFailed, message, j.now(), attemptID, operationKey, attemptDispatched)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrStaleAttempt
		}
		nextState := operationFailed
		if retryable {
			nextState = operationIntent
		}
		result, err = tx.ExecContext(ctx, `UPDATE journal_operations SET state = ?, current_attempt_id = '', updated_at = ? WHERE operation_key = ? AND current_attempt_id = ? AND state = ?`, nextState, j.now(), operationKey, attemptID, operationDispatched)
		if err != nil {
			return err
		}
		changed, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrStaleAttempt
		}
		return nil
	})
}

func (j *Journal) CommitEffect(ctx context.Context, commit EffectCommit) error {
	if commit.OperationKey == "" || commit.AttemptID == "" || !json.Valid(commit.Result) {
		return errors.New("journal: valid operation, attempt and result are required")
	}
	return j.withTx(ctx, "effect-commit", func(tx *sql.Tx) error {
		var state, current string
		if err := tx.QueryRowContext(ctx, `SELECT state, current_attempt_id FROM journal_operations WHERE operation_key = ?`, commit.OperationKey).Scan(&state, &current); err != nil {
			return err
		}
		if state == operationCommitted && current == commit.AttemptID {
			return nil
		}
		if state == operationUncertain {
			return ErrUncertain
		}
		result, err := tx.ExecContext(ctx, `UPDATE journal_attempts SET state = ?, result_json = ?, finished_at = ? WHERE attempt_id = ? AND operation_key = ? AND state = ? AND EXISTS (SELECT 1 FROM journal_operations WHERE operation_key = ? AND state = ? AND current_attempt_id = ?)`, attemptCommitted, []byte(commit.Result), j.now(), commit.AttemptID, commit.OperationKey, attemptDispatched, commit.OperationKey, operationDispatched, commit.AttemptID)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrStaleAttempt
		}
		result, err = tx.ExecContext(ctx, `UPDATE journal_operations SET state = ?, result_json = ?, updated_at = ? WHERE operation_key = ? AND state = ? AND current_attempt_id = ?`, operationCommitted, []byte(commit.Result), j.now(), commit.OperationKey, operationDispatched, commit.AttemptID)
		if err != nil {
			return err
		}
		changed, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrStaleAttempt
		}
		return nil
	})
}

func (j *Journal) MarkUncertain(ctx context.Context, operationKey, attemptID, evidence string) error {
	if operationKey == "" || attemptID == "" {
		return errors.New("journal: operation and attempt keys are required")
	}
	return j.withTx(ctx, "effect-uncertain", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE journal_attempts SET state = ?, error_text = ?, finished_at = ? WHERE attempt_id = ? AND operation_key = ? AND state = ?`, attemptUncertain, evidence, j.now(), attemptID, operationKey, attemptDispatched)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrStaleAttempt
		}
		result, err = tx.ExecContext(ctx, `UPDATE journal_operations SET state = ?, current_attempt_id = ?, updated_at = ? WHERE operation_key = ? AND state = ? AND current_attempt_id = ?`, operationUncertain, attemptID, j.now(), operationKey, operationDispatched, attemptID)
		if err != nil {
			return err
		}
		changed, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrStaleAttempt
		}
		return nil
	})
}

func (j *Journal) CompleteRun(ctx context.Context, runID string, output json.RawMessage) error {
	if runID == "" || !json.Valid(output) {
		return errors.New("journal: valid run and output are required")
	}
	return j.withTx(ctx, "run-complete", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE journal_runs SET state = ?, output_json = ?, completed_at = ? WHERE run_id = ? AND state = ?`, runCompleted, []byte(output), j.now(), runID, runAccepted)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			var state string
			if scanErr := tx.QueryRowContext(ctx, `SELECT state FROM journal_runs WHERE run_id = ?`, runID).Scan(&state); scanErr != nil {
				return scanErr
			}
			if state == runCompleted {
				return nil
			}
			return ErrStaleAttempt
		}
		return nil
	})
}

func (j *Journal) Run(ctx context.Context, runID string) (Run, error) {
	var run Run
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var err error
		run, err = scanRun(tx.QueryRowContext(ctx, `SELECT run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, output_json FROM journal_runs WHERE run_id = ?`, runID))
		return err
	})
	if err != nil {
		return Run{}, fmt.Errorf("journal: run: %w", err)
	}
	return run, nil
}

func (j *Journal) Operation(ctx context.Context, operationKey string) (Operation, error) {
	var operation Operation
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var err error
		operation, err = scanOperation(tx.QueryRowContext(ctx, `SELECT operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, current_attempt_id, result_json FROM journal_operations WHERE operation_key = ?`, operationKey))
		return err
	})
	if err != nil {
		return Operation{}, fmt.Errorf("journal: operation: %w", err)
	}
	return operation, nil
}

func (j *Journal) withTx(ctx context.Context, name string, fn func(*sql.Tx) error) error {
	err := j.database.WithTx(ctx, func(tx *sql.Tx) error {
		// Reserve the writer before reading a snapshot. SQLite cannot wait when
		// upgrading a read transaction to a writer; even an empty UPDATE takes
		// the writer reservation without changing any committed values.
		// Schema creation already starts with DDL, before this table exists.
		if name != "schema" {
			if _, err := tx.ExecContext(ctx, `UPDATE journal_runs SET run_id = run_id WHERE 0`); err != nil {
				return err
			}
		}
		if err := fn(tx); err != nil {
			return err
		}
		if j.hooks.BeforeCommit != nil {
			j.hooks.BeforeCommit(name)
		}
		return nil
	})
	if err == nil && j.hooks.AfterCommit != nil {
		j.hooks.AfterCommit(name)
	}
	return err
}

func (j *Journal) withRead(ctx context.Context, fn func(*sql.Tx) error) error {
	return j.database.WithTx(ctx, fn)
}

func (j *Journal) now() int64 { return j.clock().UTC().UnixNano() }

func validateAdmission(request AdmissionRequest) error {
	if request.RequestKey == "" || request.Workflow == "" || request.ArtifactDigest == "" || !json.Valid(request.Input) {
		return errors.New("journal: request key, workflow, artifact digest and valid JSON input are required")
	}
	return nil
}

func (i OperationIdentity) validate() error {
	if i.RunID == "" || i.ArtifactDigest == "" || i.InvocationPath == "" || i.IterationPath == "" {
		return errors.New("journal: run, artifact, invocation and iteration identity are required")
	}
	return nil
}

func digestBytes(data []byte) string {
	digest := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func randomID(prefix string) (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("journal: generate %s id: %w", prefix, err)
	}
	return prefix + ":" + hex.EncodeToString(bytes[:]), nil
}

type scanner interface{ Scan(...any) error }

func scanRun(row scanner) (Run, error) {
	var run Run
	var input, output []byte
	if err := row.Scan(&run.RunID, &run.RequestKey, &run.Workflow, &run.ArtifactDigest, &input, &run.InputDigest, &run.State, &run.ReplayOf, &output); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Run{}, ErrNotFound
		}
		return Run{}, err
	}
	run.Input = append([]byte(nil), input...)
	run.Output = append([]byte(nil), output...)
	return run, nil
}

func scanOperation(row scanner) (Operation, error) {
	var operation Operation
	var result []byte
	if err := row.Scan(&operation.Key, &operation.Identity.RunID, &operation.Identity.ArtifactDigest, &operation.Identity.InvocationPath, &operation.Identity.IterationPath, &operation.ProviderOperationKey, &operation.State, &operation.CurrentAttemptID, &result); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Operation{}, ErrNotFound
		}
		return Operation{}, err
	}
	operation.Result = append([]byte(nil), result...)
	return operation, nil
}
