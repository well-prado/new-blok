// Package journal persists accepted work and effect transitions.
package journal

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/migration"
	"github.com/well-prado/new-blok/store"
)

var (
	ErrRequestConflict  = errors.New("journal: request key conflicts with existing input")
	ErrNotFound         = errors.New("journal: record not found")
	ErrNotDispatchable  = errors.New("journal: operation is not dispatchable")
	ErrStaleAttempt     = errors.New("journal: stale attempt cannot publish")
	ErrUncertain        = errors.New("journal: operation outcome is uncertain")
	ErrRunActiveWork    = errors.New("journal: run still has active durable work")
	ErrRunNotActive     = errors.New("journal: run is not active")
	ErrObservationLimit = errors.New("journal: inspection input exceeds the hard byte limit")
	ErrRunOutputLimit   = errors.New("journal: run output exceeds the hard byte limit")
)

const (
	MaxInspectionInputBytes = 64 << 10
	MaxRunOutputBytes       = 1 << 20
	runAccepted             = "accepted"
	runCompleted            = "completed"
	runFailed               = "failed"
	runUncertain            = "uncertain"
	operationIntent         = "intent"
	operationDispatched     = "dispatched"
	operationCommitted      = "committed"
	operationUncertain      = "uncertain"
	operationFailed         = "failed"
	attemptDispatched       = "dispatched"
	attemptFailed           = "failed"
	attemptCommitted        = "committed"
	attemptUncertain        = "uncertain"
)

// Hooks are test-only crash barriers. Production callers leave them nil.
type Hooks struct {
	BeforeCommit func(string)
	AfterCommit  func(string)
}

type Config struct {
	Clock func() time.Time
	Hooks Hooks
	// Audit is the durable audit journal (ADR 0021). Reconcile and
	// DecideUpgrade require it and refuse without it. It must write to the
	// same database, so each decision and its record commit together.
	Audit *audit.Journal
	// Hold is the application's legal-hold policy for run data: Compact
	// keeps a completed run for which it returns true.
	Hold func(RetainedRun) bool
	// MinRetention is the application's legal minimum for run data: Compact
	// never removes a run completed less than this long ago, whatever cutoff
	// it is given. Negative is refused.
	MinRetention time.Duration
}

// RetainedRun identifies a completed run Compact is about to delete.
type RetainedRun struct {
	RunID, Workflow, Principal string
}

type Journal struct {
	database store.Database
	clock    func() time.Time
	hooks    Hooks
	audit    *audit.Journal
	hold     func(RetainedRun) bool
	minimum  time.Duration
}

type AdmissionRequest struct {
	RequestKey     string
	Principal      string
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
	Principal      string
	RequestKey     string
	Workflow       string
	ArtifactDigest string
	Input          json.RawMessage
	InputDigest    string
	State          string
	ReplayOf       string
	Output         json.RawMessage
	ErrorCode      string
	ErrorClass     string
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
	Input    json.RawMessage
}

type Operation struct {
	Key                  string
	Identity             OperationIdentity
	ProviderOperationKey string
	State                string
	CurrentAttemptID     string
	Input                json.RawMessage
	Result               json.RawMessage
}

type Attempt struct {
	ID                   string
	OperationKey         string
	AttemptNumber        int
	ProviderOperationKey string
	State                string
	Input                json.RawMessage
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
	if config.Audit != nil && !config.Audit.Shares(database) {
		return nil, errors.New("journal: audit must write to the journal's database")
	}
	if config.MinRetention < 0 {
		return nil, errors.New("journal: minimum retention must not be negative")
	}
	j := &Journal{database: database, clock: config.Clock, hooks: config.Hooks, audit: config.Audit, hold: config.Hold, minimum: config.MinRetention}
	if j.clock == nil {
		j.clock = time.Now
	}
	// Adding a column reads the schema first, so concurrent openers of a
	// journal that needs one race; the idempotent migration is retried
	// while it loses (#235).
	if err := migration.Retry(ctx, func() error {
		return j.withTx(ctx, "schema", func(tx *sql.Tx) error {
			return migration.Apply(ctx, tx, migration.Schema{Component: "journal", Supported: schemaVersion, Infer: inferSchemaVersion}, func(from int) error {
				return j.migrate(ctx, tx, from)
			})
		})
	}); err != nil {
		return nil, err
	}
	return j, nil
}

// schemaVersion is the highest journal schema version this binary
// understands (#291): 1 before #281, 2 with #281's erasure tables
// (journal_compacted, journal_meta, and journal_reconciliations rebuilt
// with digests and erased_at), 3 with #286's reconciliation tenant, 4 with
// #332's wait identity (journal_waits keyed by step and iteration instead
// of by name). New refuses a journal stamped with a newer one. Raise it
// with every change an older binary would misread, together with the
// migration step that makes it. It is a variable only so tests can stand in for an older
// binary.
var schemaVersion = 4

// inferSchemaVersion classifies a journal written before the stamp existed
// by its shape: 0 when it has no tables yet, 4 when its waits carry an
// iteration path (#332), 3 when its reconciliations carry a tenant (#286),
// 2 when they carry erased_at (#281), else 1.
func inferSchemaVersion(ctx context.Context, tx *sql.Tx) (int, error) {
	if found, err := migration.TableExists(ctx, tx, "journal_runs"); err != nil || !found {
		return 0, err
	}
	for _, shape := range []struct {
		table, column string
		version       int
	}{{"journal_waits", "iteration_path", 4}, {"journal_reconciliations", "tenant", 3}, {"journal_reconciliations", "erased_at", 2}} {
		found, err := migration.ColumnExists(ctx, tx, shape.table, shape.column)
		if err != nil {
			return 0, err
		}
		if found {
			return shape.version, nil
		}
	}
	return 1, nil
}

// migrate brings the journal's tables to the current shape. Every step is
// guarded by the shape it changes and runs on every open, not only when the
// stamp is older: a binary from before #291 cannot see the stamp, and a
// pre-#281 one recreates journal_audit, or a pre-#286 one inserts a
// reconciliation without a tenant, in a journal already stamped current.
// The next open repairs both. Steps from #332 on run only when the version
// found is older than their own.
func (j *Journal) migrate(ctx context.Context, tx *sql.Tx, from int) error {
	for _, statement := range schemaStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	rows, err := tx.QueryContext(ctx, `PRAGMA table_info(journal_runs)`)
	if err != nil {
		return err
	}
	hasPrincipal := false
	for rows.Next() {
		var cid, notnull, pk int
		var name, typ string
		var def any
		if err := rows.Scan(&cid, &name, &typ, &notnull, &def, &pk); err != nil {
			rows.Close()
			return err
		}
		if name == "principal" {
			hasPrincipal = true
		}
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if !hasPrincipal {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE journal_runs ADD COLUMN principal TEXT NOT NULL DEFAULT ''`); err != nil {
			return err
		}
	}
	for _, column := range []struct{ table, name, declaration string }{
		{"journal_runs", "error_code", `TEXT NOT NULL DEFAULT ''`},
		{"journal_runs", "error_class", `TEXT NOT NULL DEFAULT ''`},
		{"journal_operations", "input_json", `BLOB`},
		{"journal_attempts", "input_json", `BLOB`},
		{"journal_scopes", "input_json", `BLOB`},
		{"journal_scopes", "attempt_id", `TEXT NOT NULL DEFAULT ''`},
	} {
		if err := ensureColumn(ctx, tx, column.table, column.name, column.declaration); err != nil {
			return err
		}
	}
	if err := j.migrateErasure(ctx, tx); err != nil {
		return err
	}
	// A reconciliation records the tenant that decided it (#286).
	// The column is added once; every open then fixes the tenant of
	// any row without one, from its verified audit record or "".
	if err := ensureColumn(ctx, tx, "journal_reconciliations", "tenant", `TEXT`); err != nil {
		return err
	}
	if err := backfillReconciliationTenants(ctx, tx); err != nil {
		return err
	}
	if from < 4 {
		return migrateWaitIdentity(ctx, tx)
	}
	return nil
}

// migrateWaitIdentity rebuilds journal_waits keyed by step and iteration
// instead of UNIQUE (run_id, name) (#332). SQLite cannot drop a table
// constraint, so the table is copied whole. A wait written before #332 has
// no step identity: its paths stay NULL, which no new wait can collide
// with, and it keeps its ID, name, due time, state, signal and timestamps.
func migrateWaitIdentity(ctx context.Context, tx *sql.Tx) error {
	current, err := hasColumn(ctx, tx, "journal_waits", "iteration_path")
	if err != nil || current {
		return err
	}
	const columns = `wait_id, run_id, name, due_at, state, signal_id, payload_json, created_at, updated_at`
	for _, statement := range append([]string{
		waitsTable("journal_waits_332"),
		`INSERT INTO journal_waits_332 (` + columns + `) SELECT ` + columns + ` FROM journal_waits`,
		`DROP TABLE journal_waits`,
		`ALTER TABLE journal_waits_332 RENAME TO journal_waits`,
	}, waitIndexes...) {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("migrate wait identity: %w", err)
		}
	}
	return nil
}

func ensureColumn(ctx context.Context, tx *sql.Tx, table, column, declaration string) error {
	found, err := hasColumn(ctx, tx, table, column)
	if err != nil || found {
		return err
	}
	_, err = tx.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, declaration))
	return err
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
		completed_at INTEGER,
		principal TEXT NOT NULL DEFAULT '',
		error_code TEXT NOT NULL DEFAULT '',
		error_class TEXT NOT NULL DEFAULT ''
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
		input_json BLOB,
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
		input_json BLOB,
		error_text TEXT NOT NULL DEFAULT '',
		started_at INTEGER NOT NULL,
		finished_at INTEGER,
		UNIQUE (operation_key, attempt_number),
		FOREIGN KEY (operation_key) REFERENCES journal_operations(operation_key)
	)`,
	`CREATE INDEX IF NOT EXISTS journal_operations_run ON journal_operations(run_id)`,
	`CREATE INDEX IF NOT EXISTS journal_attempts_operation ON journal_attempts(operation_key, attempt_number)`,
	waitsTable("journal_waits"),
	waitIndexes[0],
	waitIndexes[1],
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
		input_json BLOB,
		error_text TEXT NOT NULL DEFAULT '',
		attempt_id TEXT NOT NULL DEFAULT '',
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
	// A reconciliation's evidence, provider result and actor are kept with
	// its run and erased with it, leaving the decision's identity, tenant
	// and digests (#281, #286). The table has no foreign key to journal_operations:
	// the decision outlives its compacted operation.
	reconciliationsTable("journal_reconciliations"),
	// journal_meta holds the journal's own counters: erasure_generation
	// counts compactions that erased content, purged_generation the latest
	// one whose log purge succeeded (#281).
	`CREATE TABLE IF NOT EXISTS journal_meta (name TEXT PRIMARY KEY, value INTEGER NOT NULL)`,
	// The compaction tombstone proves a run existed and ended; it holds
	// digests and timestamps only (#281).
	`CREATE TABLE IF NOT EXISTS journal_compacted (
		run_id TEXT PRIMARY KEY,
		request_digest TEXT NOT NULL,
		artifact_digest TEXT NOT NULL,
		input_digest TEXT NOT NULL,
		output_digest TEXT NOT NULL,
		state TEXT NOT NULL,
		completed_at INTEGER NOT NULL,
		compacted_at INTEGER NOT NULL
	)`,
}

// waitsTable is journal_waits since #332: a wait is identified by the step
// that waits and its iteration, as an effect is (OperationIdentity), so one
// run can wait on the same name in successive iterations or steps. Waits
// written before #332 have NULL paths (see migrateWaitIdentity).
func waitsTable(name string) string {
	return `CREATE TABLE IF NOT EXISTS ` + name + ` (
		wait_id TEXT PRIMARY KEY,
		run_id TEXT NOT NULL,
		name TEXT NOT NULL,
		due_at INTEGER NOT NULL,
		state TEXT NOT NULL,
		signal_id TEXT NOT NULL DEFAULT '',
		payload_json BLOB,
		created_at INTEGER NOT NULL,
		updated_at INTEGER NOT NULL,
		invocation_path TEXT,
		iteration_path TEXT,
		FOREIGN KEY (run_id) REFERENCES journal_runs(run_id),
		UNIQUE (run_id, invocation_path, iteration_path),
		CHECK ((invocation_path IS NULL) = (iteration_path IS NULL))
	)`
}

// waitIndexes serve the timer claim (state, due_at) and signal routing to
// a run's open wait of a name, which UNIQUE (run_id, name) served before
// #332.
var waitIndexes = []string{
	`CREATE INDEX IF NOT EXISTS journal_waits_due ON journal_waits(state, due_at)`,
	`CREATE INDEX IF NOT EXISTS journal_waits_signal ON journal_waits(run_id, name, state)`,
}

func reconciliationsTable(name string) string {
	return `CREATE TABLE IF NOT EXISTS ` + name + ` (
		operation_key TEXT PRIMARY KEY,
		run_id TEXT NOT NULL,
		actor TEXT NOT NULL,
		evidence TEXT,
		result_json BLOB,
		evidence_digest TEXT NOT NULL,
		result_digest TEXT NOT NULL,
		state TEXT NOT NULL,
		created_at INTEGER NOT NULL,
		erased_at INTEGER,
		tenant TEXT
	)`
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
			(run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, created_at, principal)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`,
			runID, request.RequestKey, request.Workflow, request.ArtifactDigest, []byte(request.Input), digest, runAccepted, j.now(), request.Principal)
		if err != nil {
			return err
		}
		inserted, err := result.RowsAffected()
		if err != nil {
			return err
		}
		var existing Run
		if inserted == 0 {
			existing, err = scanRun(tx.QueryRowContext(ctx, `SELECT run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, output_json, principal, error_code, error_class FROM journal_runs WHERE request_key = ?`, request.RequestKey))
			if err != nil {
				return err
			}
			if existing.Principal != request.Principal || existing.Workflow != request.Workflow || existing.ArtifactDigest != request.ArtifactDigest || existing.InputDigest != digest {
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
		source, err := scanRun(tx.QueryRowContext(ctx, `SELECT run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, output_json, principal, error_code, error_class FROM journal_runs WHERE run_id = ?`, sourceRunID))
		if err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `INSERT INTO journal_runs
			(run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, created_at, principal)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(request_key) DO NOTHING`,
			runID, requestKey, source.Workflow, source.ArtifactDigest, []byte(source.Input), source.InputDigest, runAccepted, source.RunID, j.now(), source.Principal)
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
	if len(intent.Input) > MaxInspectionInputBytes {
		return Operation{}, ErrObservationLimit
	}
	if len(intent.Input) > 0 && !json.Valid(intent.Input) {
		return Operation{}, errors.New("journal: effect input must be valid JSON")
	}
	key := intent.Identity.Key()
	var operation Operation
	err := j.withTx(ctx, "effect-intent", func(tx *sql.Tx) error {
		var runState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM journal_runs WHERE run_id=?`, intent.Identity.RunID).Scan(&runState); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		var exists int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_operations WHERE operation_key=?`, key).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 && runState != runAccepted {
			return ErrRunNotActive
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_operations
		(operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, input_json, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(operation_key) DO NOTHING`,
			key, intent.Identity.RunID, intent.Identity.ArtifactDigest, intent.Identity.InvocationPath, intent.Identity.IterationPath, key, operationIntent, nullableJSON(intent.Input), j.now(), j.now()); err != nil {
			return err
		}
		var err error
		operation, err = scanOperation(tx.QueryRowContext(ctx, `SELECT operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, current_attempt_id, input_json, result_json FROM journal_operations WHERE operation_key = ?`, key))
		if err == nil && len(intent.Input) > 0 && len(operation.Input) > 0 && !bytes.Equal(intent.Input, operation.Input) {
			return ErrRequestConflict
		}
		if err == nil && len(intent.Input) > 0 && len(operation.Input) == 0 && operation.State == operationIntent {
			var updated sql.Result
			updated, err = tx.ExecContext(ctx, `UPDATE journal_operations SET input_json=? WHERE operation_key=? AND input_json IS NULL AND state=? AND NOT EXISTS (SELECT 1 FROM journal_attempts WHERE operation_key=?)`, []byte(intent.Input), key, operationIntent, key)
			if err == nil {
				var changed int64
				changed, err = updated.RowsAffected()
				if err == nil && changed == 1 {
					operation.Input = append([]byte(nil), intent.Input...)
				}
			}
		}
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
		operation, err := scanOperation(tx.QueryRowContext(ctx, `SELECT operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, current_attempt_id, input_json, result_json FROM journal_operations WHERE operation_key = ?`, operationKey))
		if err != nil {
			return err
		}
		if operation.State != operationIntent {
			if operation.State == operationUncertain {
				return ErrUncertain
			}
			return ErrNotDispatchable
		}
		var runState string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM journal_runs WHERE run_id=?`, operation.Identity.RunID).Scan(&runState); err != nil {
			return err
		}
		if runState != runAccepted {
			return ErrRunNotActive
		}
		var next int
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(attempt_number), 0) + 1 FROM journal_attempts WHERE operation_key = ?`, operationKey).Scan(&next); err != nil {
			return err
		}
		attemptID, err := randomID("attempt")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_attempts (attempt_id, operation_key, attempt_number, provider_operation_key, state, input_json, started_at) VALUES (?, ?, ?, ?, ?, ?, ?)`, attemptID, operationKey, next, operation.ProviderOperationKey, attemptDispatched, nullableJSON(operation.Input), j.now()); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `UPDATE journal_operations SET state = ?, current_attempt_id = ?, updated_at = ? WHERE operation_key = ? AND state IN (?, ?)`, operationDispatched, attemptID, j.now(), operationKey, operationIntent, operationFailed); err != nil {
			return err
		}
		attempt = Attempt{ID: attemptID, OperationKey: operationKey, AttemptNumber: next, ProviderOperationKey: operation.ProviderOperationKey, State: attemptDispatched, Input: append([]byte(nil), operation.Input...)}
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
	if len(output) > MaxRunOutputBytes {
		return ErrRunOutputLimit
	}
	if runID == "" || !json.Valid(output) {
		return errors.New("journal: valid run and output are required")
	}
	return j.withTx(ctx, "run-complete", func(tx *sql.Tx) error {
		if err := requireQuiescentRun(ctx, tx, runID); err != nil {
			return err
		}
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

// FailRun records the canonical terminal workflow outcome. Attempt failures
// never imply that the whole run stopped: callers invoke this only after the
// engine has ended all active dispatches and waits.
func (j *Journal) FailRun(ctx context.Context, runID, errorCode, errorClass string) error {
	if runID == "" || !validDiagnosticLabel(errorCode) || !validDiagnosticLabel(errorClass) {
		return errors.New("journal: run and safe failure code/class are required")
	}
	return j.withTx(ctx, "run-fail", func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRowContext(ctx, `SELECT state FROM journal_runs WHERE run_id=?`, runID).Scan(&state); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if state == runFailed {
			var oldCode, oldClass string
			if err := tx.QueryRowContext(ctx, `SELECT error_code,error_class FROM journal_runs WHERE run_id=?`, runID).Scan(&oldCode, &oldClass); err != nil {
				return err
			}
			if oldCode == errorCode && oldClass == errorClass {
				return nil
			}
			return ErrStaleAttempt
		}
		if state == runUncertain {
			return ErrUncertain
		}
		if state != runAccepted {
			return ErrStaleAttempt
		}
		if err := requireQuiescentRun(ctx, tx, runID); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE journal_runs SET state=?,error_code=?,error_class=?,completed_at=? WHERE run_id=? AND state=?`, runFailed, errorCode, errorClass, j.now(), runID, runAccepted)
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
		return nil
	})
}

// MarkRunUncertain records a conservative terminal projection when the
// execution boundary reports an unknown external outcome. It intentionally
// does not rewrite operation/attempt facts or authorize a retry.
func (j *Journal) MarkRunUncertain(ctx context.Context, runID, errorCode, errorClass string) error {
	if runID == "" || !validDiagnosticLabel(errorCode) || !validDiagnosticLabel(errorClass) {
		return errors.New("journal: run and safe uncertainty code/class are required")
	}
	return j.withTx(ctx, "run-uncertain", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE journal_runs SET state=?,error_code=?,error_class=?,completed_at=? WHERE run_id=? AND state=?`, runUncertain, errorCode, errorClass, j.now(), runID, runAccepted)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed == 1 {
			return nil
		}
		var state, oldCode, oldClass string
		if err := tx.QueryRowContext(ctx, `SELECT state,error_code,error_class FROM journal_runs WHERE run_id=?`, runID).Scan(&state, &oldCode, &oldClass); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return ErrNotFound
			}
			return err
		}
		if state == runUncertain && oldCode == errorCode && oldClass == errorClass {
			return nil
		}
		return ErrStaleAttempt
	})
}

func requireQuiescentRun(ctx context.Context, tx *sql.Tx, runID string) error {
	var dispatched, waiting, uncertain, runningScopes int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_operations WHERE run_id=? AND state=?`, runID, operationDispatched).Scan(&dispatched); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_waits WHERE run_id=? AND state=?`, runID, waitWaiting).Scan(&waiting); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_operations WHERE run_id=? AND state=?`, runID, operationUncertain).Scan(&uncertain); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM journal_scopes WHERE run_id=? AND state=?`, runID, checkpointRunning).Scan(&runningScopes); err != nil {
		return err
	}
	if uncertain > 0 {
		return ErrUncertain
	}
	if dispatched > 0 || waiting > 0 || runningScopes > 0 {
		return ErrRunActiveWork
	}
	return nil
}

func validDiagnosticLabel(value string) bool {
	if value == "" || len(value) > 64 {
		return false
	}
	for _, c := range value {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' || c == '.') {
			return false
		}
	}
	return true
}

func (j *Journal) Run(ctx context.Context, runID string) (Run, error) {
	var run Run
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var err error
		run, err = scanRun(tx.QueryRowContext(ctx, `SELECT run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, replay_of, output_json, principal, error_code, error_class FROM journal_runs WHERE run_id = ?`, runID))
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
		operation, err = scanOperation(tx.QueryRowContext(ctx, `SELECT operation_key, run_id, artifact_digest, invocation_path, iteration_path, provider_operation_key, state, current_attempt_id, input_json, result_json FROM journal_operations WHERE operation_key = ?`, operationKey))
		return err
	})
	if err != nil {
		return Operation{}, fmt.Errorf("journal: operation: %w", err)
	}
	return operation, nil
}

func (j *Journal) withTx(ctx context.Context, name string, fn func(*sql.Tx) error) error {
	// Every transition writes first, so it takes its turn in the store's
	// writer queue (#214). Schema creation does not: reopening a journal
	// only reads that its tables exist, so it is not queued.
	txCtx := store.Writer(ctx)
	if name == "schema" {
		txCtx = ctx
	}
	err := j.database.WithTx(txCtx, func(tx *sql.Tx) error {
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

func nullableJSON(data json.RawMessage) any {
	if len(data) == 0 {
		return nil
	}
	return []byte(data)
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
	if err := row.Scan(&run.RunID, &run.RequestKey, &run.Workflow, &run.ArtifactDigest, &input, &run.InputDigest, &run.State, &run.ReplayOf, &output, &run.Principal, &run.ErrorCode, &run.ErrorClass); err != nil {
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
	var input, result []byte
	if err := row.Scan(&operation.Key, &operation.Identity.RunID, &operation.Identity.ArtifactDigest, &operation.Identity.InvocationPath, &operation.Identity.IterationPath, &operation.ProviderOperationKey, &operation.State, &operation.CurrentAttemptID, &input, &result); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Operation{}, ErrNotFound
		}
		return Operation{}, err
	}
	operation.Input = append([]byte(nil), input...)
	operation.Result = append([]byte(nil), result...)
	return operation, nil
}
