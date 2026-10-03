package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

const (
	checkpointRunning   = "running"
	checkpointCompleted = "completed"
	checkpointCanceled  = "canceled"
	childRunning        = "running"
	childCompleted      = "completed"
	joinRunning         = "running"
	joinCompleted       = "completed"
)

var ErrArtifactMismatch = errors.New("journal: checkpoint artifact does not match")

type Checkpoint struct {
	RunID            string
	ArtifactDigest   string
	CheckpointDigest string
	Status           string
	State            json.RawMessage
}

type ScopeRecord struct {
	RunID      string
	Path       string
	Kind       string
	ParentPath string
	State      string
	Output     json.RawMessage
	Error      string
}

type ChildRecord struct {
	RunID      string
	Path       string
	ChildRunID string
	State      string
	Result     json.RawMessage
}

type JoinRecord struct {
	RunID     string
	Path      string
	Expected  int
	Completed int
	Results   []json.RawMessage
	State     string
}

type Recovery struct {
	Checkpoint Checkpoint
	Scopes     []ScopeRecord
	Children   []ChildRecord
	Joins      []JoinRecord
}

func (j *Journal) SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error {
	if checkpoint.RunID == "" || checkpoint.ArtifactDigest == "" || checkpoint.CheckpointDigest == "" || !json.Valid(checkpoint.State) {
		return errors.New("journal: valid checkpoint identity and JSON state are required")
	}
	if checkpoint.Status == "" {
		checkpoint.Status = checkpointRunning
	}
	return j.withTx(ctx, "checkpoint", func(tx *sql.Tx) error {
		var artifact, digest string
		err := tx.QueryRowContext(ctx, `SELECT artifact_digest, checkpoint_digest FROM journal_checkpoints WHERE run_id = ?`, checkpoint.RunID).Scan(&artifact, &digest)
		if err == nil && (artifact != checkpoint.ArtifactDigest || digest != checkpoint.CheckpointDigest) {
			return ErrArtifactMismatch
		}
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO journal_checkpoints (run_id, artifact_digest, checkpoint_digest, status, state_json, updated_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(run_id) DO UPDATE SET status = excluded.status, state_json = excluded.state_json, updated_at = excluded.updated_at`, checkpoint.RunID, checkpoint.ArtifactDigest, checkpoint.CheckpointDigest, checkpoint.Status, []byte(checkpoint.State), j.now())
		return err
	})
}

// StartScope is idempotent after a crash. A completed path is returned with
// alreadyCompleted=true so recovery never dispatches its effect again.
func (j *Journal) StartScope(ctx context.Context, record ScopeRecord) (alreadyCompleted bool, err error) {
	if record.RunID == "" || record.Path == "" || record.Kind == "" {
		return false, errors.New("journal: scope run, path and kind are required")
	}
	err = j.withTx(ctx, "scope-start", func(tx *sql.Tx) error {
		var state string
		queryErr := tx.QueryRowContext(ctx, `SELECT state FROM journal_scopes WHERE run_id = ? AND path = ?`, record.RunID, record.Path).Scan(&state)
		if queryErr == nil {
			alreadyCompleted = state == checkpointCompleted
			if alreadyCompleted {
				return nil
			}
			_, queryErr = tx.ExecContext(ctx, `UPDATE journal_scopes SET state = ?, updated_at = ? WHERE run_id = ? AND path = ?`, checkpointRunning, j.now(), record.RunID, record.Path)
			return queryErr
		}
		if !errors.Is(queryErr, sql.ErrNoRows) {
			return queryErr
		}
		_, queryErr = tx.ExecContext(ctx, `INSERT INTO journal_scopes (run_id, path, kind, parent_path, state, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, record.RunID, record.Path, record.Kind, record.ParentPath, checkpointRunning, j.now())
		return queryErr
	})
	return alreadyCompleted, err
}

func (j *Journal) CompleteScope(ctx context.Context, runID, path string, output json.RawMessage) error {
	if runID == "" || path == "" || !json.Valid(output) {
		return errors.New("journal: valid scope identity and output are required")
	}
	return j.withTx(ctx, "scope-complete", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE journal_scopes SET state = ?, output_json = ?, updated_at = ? WHERE run_id = ? AND path = ? AND state != ?`, checkpointCompleted, []byte(output), j.now(), runID, path, checkpointCanceled)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrNotFound
		}
		return nil
	})
}

func (j *Journal) CancelScope(ctx context.Context, runID, path, reason string) error {
	return j.withTx(ctx, "scope-cancel", func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `UPDATE journal_scopes SET state = ?, error_text = ?, updated_at = ? WHERE run_id = ? AND path = ? AND state != ?`, checkpointCanceled, reason, j.now(), runID, path, checkpointCompleted)
		if err != nil {
			return err
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if changed != 1 {
			return ErrNotFound
		}
		return nil
	})
}

func (j *Journal) RecordChild(ctx context.Context, record ChildRecord) error {
	if record.RunID == "" || record.Path == "" || record.ChildRunID == "" {
		return errors.New("journal: child run, path and child identity are required")
	}
	if record.State == "" {
		record.State = childRunning
	}
	return j.withTx(ctx, "child", func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO journal_children (run_id, path, child_run_id, state, result_json) VALUES (?, ?, ?, ?, ?) ON CONFLICT(run_id, path) DO UPDATE SET state = excluded.state, result_json = excluded.result_json`, record.RunID, record.Path, record.ChildRunID, record.State, []byte(record.Result))
		return err
	})
}

func (j *Journal) RecordJoin(ctx context.Context, record JoinRecord) error {
	if record.RunID == "" || record.Path == "" || record.Expected < 1 || record.Completed < 0 || record.Completed > record.Expected {
		return errors.New("journal: invalid join record")
	}
	encoded, err := json.Marshal(record.Results)
	if err != nil {
		return err
	}
	if record.State == "" {
		record.State = joinRunning
	}
	if record.Completed == record.Expected {
		record.State = joinCompleted
	}
	return j.withTx(ctx, "join", func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO journal_joins (run_id, path, expected, completed, results_json, state) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(run_id, path) DO UPDATE SET completed = excluded.completed, results_json = excluded.results_json, state = excluded.state`, record.RunID, record.Path, record.Expected, record.Completed, encoded, record.State)
		return err
	})
}

func (j *Journal) Recover(ctx context.Context, runID, artifactDigest, checkpointDigest string) (Recovery, error) {
	if runID == "" || artifactDigest == "" || checkpointDigest == "" {
		return Recovery{}, errors.New("journal: recovery identity is required")
	}
	var recovery Recovery
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var state []byte
		if err := tx.QueryRowContext(ctx, `SELECT artifact_digest, checkpoint_digest, status, state_json FROM journal_checkpoints WHERE run_id = ?`, runID).Scan(&recovery.Checkpoint.ArtifactDigest, &recovery.Checkpoint.CheckpointDigest, &recovery.Checkpoint.Status, &state); err != nil {
			return err
		}
		recovery.Checkpoint.RunID = runID
		recovery.Checkpoint.State = append([]byte(nil), state...)
		if recovery.Checkpoint.ArtifactDigest != artifactDigest || recovery.Checkpoint.CheckpointDigest != checkpointDigest {
			return ErrArtifactMismatch
		}
		var registeredDigest string
		if err := tx.QueryRowContext(ctx, `SELECT digest FROM journal_artifacts WHERE digest = ?`, artifactDigest).Scan(&registeredDigest); errors.Is(err, sql.ErrNoRows) {
			return ErrArtifactMissing
		} else if err != nil {
			return err
		}
		rows, err := tx.QueryContext(ctx, `SELECT run_id, path, kind, parent_path, state, output_json, error_text FROM journal_scopes WHERE run_id = ? ORDER BY path`, runID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var record ScopeRecord
			var output []byte
			if err := rows.Scan(&record.RunID, &record.Path, &record.Kind, &record.ParentPath, &record.State, &output, &record.Error); err != nil {
				return err
			}
			record.Output = append([]byte(nil), output...)
			recovery.Scopes = append(recovery.Scopes, record)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		childRows, err := tx.QueryContext(ctx, `SELECT run_id, path, child_run_id, state, result_json FROM journal_children WHERE run_id = ? ORDER BY path`, runID)
		if err != nil {
			return err
		}
		defer childRows.Close()
		for childRows.Next() {
			var record ChildRecord
			var result []byte
			if err := childRows.Scan(&record.RunID, &record.Path, &record.ChildRunID, &record.State, &result); err != nil {
				return err
			}
			record.Result = append([]byte(nil), result...)
			recovery.Children = append(recovery.Children, record)
		}
		if err := childRows.Err(); err != nil {
			return err
		}
		joinRows, err := tx.QueryContext(ctx, `SELECT run_id, path, expected, completed, results_json, state FROM journal_joins WHERE run_id = ? ORDER BY path`, runID)
		if err != nil {
			return err
		}
		defer joinRows.Close()
		for joinRows.Next() {
			var record JoinRecord
			var encoded []byte
			if err := joinRows.Scan(&record.RunID, &record.Path, &record.Expected, &record.Completed, &encoded, &record.State); err != nil {
				return err
			}
			if err := json.Unmarshal(encoded, &record.Results); err != nil {
				return err
			}
			recovery.Joins = append(recovery.Joins, record)
		}
		return joinRows.Err()
	})
	if err != nil {
		return Recovery{}, fmt.Errorf("journal: recover: %w", err)
	}
	sort.Slice(recovery.Scopes, func(i, k int) bool { return recovery.Scopes[i].Path < recovery.Scopes[k].Path })
	return recovery, nil
}
