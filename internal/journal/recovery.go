package journal

import (
	"bytes"
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

// Refusals of the recovery records (#334). A recovery record is a run's
// receipt: once written it does not change, except to move forward to a
// terminal state (a scope's completed or canceled, a join's or a child's
// completed). Each refusal below leaves the stored record exactly as it
// was.
var (
	// ErrRecordFinal: the record has reached a terminal state and the
	// write would change it: a completed scope given another output or by
	// another attempt, a canceled scope started again, or a completed join
	// or child written differently. Repeating the write that completed it,
	// byte for byte (for a scope, from the attempt that completed it),
	// succeeds instead.
	ErrRecordFinal = errors.New("journal: recovery record is final")
	// ErrRecordConflict: the write contradicts what the record fixed when it
	// was created (a join's expected count, a child's run id) or does not
	// move it forward (a join's completed count going down, or staying the
	// same with other results; a running child written as running with
	// another result).
	ErrRecordConflict = errors.New("journal: recovery record conflicts with the recorded one")
	// ErrChildRunNotFound: a new child record names a run the journal does
	// not hold.
	ErrChildRunNotFound = errors.New("journal: child run does not exist")
)

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
	Input      json.RawMessage
	Output     json.RawMessage
	Error      string
}

// ScopeAttempt is what StartScope hands the caller. AttemptID fences the
// scope: only the attempt that started it last may complete it. A completed
// scope has no attempt to run; AlreadyCompleted is true and AttemptID empty.
type ScopeAttempt struct {
	AttemptID        string
	AlreadyCompleted bool
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

// runStateAndArtifact reads a run's state and admitted artifact,
// ErrNotFound when the journal does not hold it.
func runStateAndArtifact(ctx context.Context, tx *sql.Tx, runID string) (state, artifact string, err error) {
	err = tx.QueryRowContext(ctx, `SELECT state, artifact_digest FROM journal_runs WHERE run_id = ?`, runID).Scan(&state, &artifact)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", ErrNotFound
	}
	return state, artifact, err
}

// SaveCheckpoint stores the run's checkpoint. The run must be accepted
// (ErrRunNotActive otherwise, ErrNotFound when unknown), and the checkpoint
// must name the artifact the run was admitted under (ErrArtifactMismatch),
// as must every later checkpoint of the run.
func (j *Journal) SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error {
	if checkpoint.RunID == "" || checkpoint.ArtifactDigest == "" || checkpoint.CheckpointDigest == "" || !json.Valid(checkpoint.State) {
		return errors.New("journal: valid checkpoint identity and JSON state are required")
	}
	if checkpoint.Status == "" {
		checkpoint.Status = checkpointRunning
	}
	return j.withTx(ctx, "checkpoint", func(tx *sql.Tx) error {
		state, admitted, err := runStateAndArtifact(ctx, tx, checkpoint.RunID)
		if err != nil {
			return err
		}
		if admitted != checkpoint.ArtifactDigest {
			return ErrArtifactMismatch
		}
		if state != runAccepted {
			return ErrRunNotActive
		}
		var artifact, digest string
		err = tx.QueryRowContext(ctx, `SELECT artifact_digest, checkpoint_digest FROM journal_checkpoints WHERE run_id = ?`, checkpoint.RunID).Scan(&artifact, &digest)
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
// AlreadyCompleted so recovery never dispatches its effect again, and a
// canceled one is refused with ErrRecordFinal: it stays canceled. Otherwise
// it starts a new attempt, which fences out every earlier one: CompleteScope
// accepts only the AttemptID returned here last. The run must be accepted
// (ErrRunNotActive otherwise, ErrNotFound when unknown).
func (j *Journal) StartScope(ctx context.Context, record ScopeRecord) (ScopeAttempt, error) {
	if record.RunID == "" || record.Path == "" || record.Kind == "" {
		return ScopeAttempt{}, errors.New("journal: scope run, path and kind are required")
	}
	if len(record.Input) > MaxInspectionInputBytes {
		return ScopeAttempt{}, ErrObservationLimit
	}
	if len(record.Input) > 0 && !json.Valid(record.Input) {
		return ScopeAttempt{}, errors.New("journal: scope input must be valid JSON")
	}
	attemptID, err := randomID("scope-attempt")
	if err != nil {
		return ScopeAttempt{}, err
	}
	var started ScopeAttempt
	err = j.withTx(ctx, "scope-start", func(tx *sql.Tx) error {
		run, _, err := runStateAndArtifact(ctx, tx, record.RunID)
		if err != nil {
			return err
		}
		if run != runAccepted {
			return ErrRunNotActive
		}
		var state string
		var input []byte
		queryErr := tx.QueryRowContext(ctx, `SELECT state,input_json FROM journal_scopes WHERE run_id = ? AND path = ?`, record.RunID, record.Path).Scan(&state, &input)
		if queryErr == nil {
			if len(record.Input) > 0 && len(input) > 0 && !bytes.Equal(record.Input, input) {
				return ErrRequestConflict
			}
			if state == checkpointCompleted {
				started.AlreadyCompleted = true
				return nil
			}
			if state == checkpointCanceled {
				return ErrRecordFinal
			}
			// Existing rows may predate input capture. Never backfill an unknown
			// historical input from a later recovery request.
			_, queryErr = tx.ExecContext(ctx, `UPDATE journal_scopes SET state = ?, attempt_id = ?, updated_at = ? WHERE run_id = ? AND path = ?`, checkpointRunning, attemptID, j.now(), record.RunID, record.Path)
			started.AttemptID = attemptID
			return queryErr
		}
		if !errors.Is(queryErr, sql.ErrNoRows) {
			return queryErr
		}
		_, queryErr = tx.ExecContext(ctx, `INSERT INTO journal_scopes (run_id, path, kind, parent_path, state, input_json, attempt_id, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, record.RunID, record.Path, record.Kind, record.ParentPath, checkpointRunning, nullableJSON(record.Input), attemptID, j.now())
		started.AttemptID = attemptID
		return queryErr
	})
	if err != nil {
		return ScopeAttempt{}, err
	}
	return started, nil
}

// CompleteScope commits a scope's output, once. attemptID is the one
// StartScope returned last for the scope; an earlier attempt, or none, is
// refused with ErrStaleAttempt. A completed scope is refused with
// ErrRecordFinal unless this is the completing attempt repeating the same
// output byte for byte, which succeeds and changes nothing. A canceled or
// unknown scope is ErrNotFound. ErrStaleAttempt, ErrRecordFinal and, for a
// canceled scope, ErrNotFound all mean the caller lost the scope: a
// superseded attempt sees ErrStaleAttempt while the scope runs and
// ErrRecordFinal once another attempt completed it.
func (j *Journal) CompleteScope(ctx context.Context, runID, path, attemptID string, output json.RawMessage) error {
	if runID == "" || path == "" || !json.Valid(output) {
		return errors.New("journal: valid scope identity and output are required")
	}
	if attemptID == "" {
		return ErrStaleAttempt
	}
	return j.withTx(ctx, "scope-complete", func(tx *sql.Tx) error {
		var state, current string
		var stored []byte
		err := tx.QueryRowContext(ctx, `SELECT state, attempt_id, output_json FROM journal_scopes WHERE run_id = ? AND path = ?`, runID, path).Scan(&state, &current, &stored)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		switch {
		case state == checkpointCompleted:
			if current == attemptID && bytes.Equal(stored, output) {
				return nil
			}
			return ErrRecordFinal
		case state != checkpointRunning:
			return ErrNotFound
		case current != attemptID:
			return ErrStaleAttempt
		}
		result, err := tx.ExecContext(ctx, `UPDATE journal_scopes SET state = ?, output_json = ?, updated_at = ? WHERE run_id = ? AND path = ? AND state = ? AND attempt_id = ?`, checkpointCompleted, []byte(output), j.now(), runID, path, checkpointRunning, attemptID)
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

// RecordChild records the child run a parent step started, and later its
// result. The child run id is fixed when the record is created and must name
// a run the journal holds (ErrChildRunNotFound); another id is
// ErrRecordConflict. A record moves from running to completed only: a
// completed record is ErrRecordFinal unless the write repeats it byte for
// byte. The parent run must exist (ErrNotFound).
func (j *Journal) RecordChild(ctx context.Context, record ChildRecord) error {
	if record.RunID == "" || record.Path == "" || record.ChildRunID == "" {
		return errors.New("journal: child run, path and child identity are required")
	}
	if record.State == "" {
		record.State = childRunning
	}
	if record.State != childRunning && record.State != childCompleted {
		return errors.New("journal: child state must be running or completed")
	}
	return j.withTx(ctx, "child", func(tx *sql.Tx) error {
		if _, _, err := runStateAndArtifact(ctx, tx, record.RunID); err != nil {
			return err
		}
		var childRunID, state string
		var result []byte
		err := tx.QueryRowContext(ctx, `SELECT child_run_id, state, result_json FROM journal_children WHERE run_id = ? AND path = ?`, record.RunID, record.Path).Scan(&childRunID, &state, &result)
		if errors.Is(err, sql.ErrNoRows) {
			if _, _, err := runStateAndArtifact(ctx, tx, record.ChildRunID); errors.Is(err, ErrNotFound) {
				return ErrChildRunNotFound
			} else if err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO journal_children (run_id, path, child_run_id, state, result_json) VALUES (?, ?, ?, ?, ?)`, record.RunID, record.Path, record.ChildRunID, record.State, []byte(record.Result))
			return err
		}
		if err != nil {
			return err
		}
		if childRunID != record.ChildRunID {
			return ErrRecordConflict
		}
		if state == record.State && bytes.Equal(result, record.Result) {
			return nil
		}
		if state == childCompleted {
			return ErrRecordFinal
		}
		if record.State != childCompleted {
			return ErrRecordConflict
		}
		updated, err := tx.ExecContext(ctx, `UPDATE journal_children SET state = ?, result_json = ? WHERE run_id = ? AND path = ? AND state = ? AND child_run_id = ?`, childCompleted, []byte(record.Result), record.RunID, record.Path, childRunning, record.ChildRunID)
		return requireOneRow(updated, err)
	})
}

// RecordJoin records how many of a join's branches have come back. It only
// moves forward: Expected is fixed when the record is created, Completed
// never decreases, and the same count with other results is
// ErrRecordConflict. The join completes when Completed reaches Expected;
// a completed join is ErrRecordFinal unless the write repeats it byte for
// byte. State is derived from the counts. The run must exist (ErrNotFound).
func (j *Journal) RecordJoin(ctx context.Context, record JoinRecord) error {
	if record.RunID == "" || record.Path == "" || record.Expected < 1 || record.Completed < 0 || record.Completed > record.Expected {
		return errors.New("journal: invalid join record")
	}
	encoded, err := json.Marshal(record.Results)
	if err != nil {
		return err
	}
	state := joinRunning
	if record.Completed == record.Expected {
		state = joinCompleted
	}
	if record.State != "" && record.State != state {
		return errors.New("journal: invalid join record")
	}
	return j.withTx(ctx, "join", func(tx *sql.Tx) error {
		if _, _, err := runStateAndArtifact(ctx, tx, record.RunID); err != nil {
			return err
		}
		var expected, completed int
		var results []byte
		var stored string
		err := tx.QueryRowContext(ctx, `SELECT expected, completed, results_json, state FROM journal_joins WHERE run_id = ? AND path = ?`, record.RunID, record.Path).Scan(&expected, &completed, &results, &stored)
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, `INSERT INTO journal_joins (run_id, path, expected, completed, results_json, state) VALUES (?, ?, ?, ?, ?, ?)`, record.RunID, record.Path, record.Expected, record.Completed, encoded, state)
			return err
		}
		if err != nil {
			return err
		}
		if expected == record.Expected && completed == record.Completed && bytes.Equal(results, encoded) {
			return nil
		}
		if stored == joinCompleted {
			return ErrRecordFinal
		}
		if expected != record.Expected || record.Completed <= completed {
			return ErrRecordConflict
		}
		result, err := tx.ExecContext(ctx, `UPDATE journal_joins SET completed = ?, results_json = ?, state = ? WHERE run_id = ? AND path = ? AND state = ? AND expected = ? AND completed < ?`, record.Completed, encoded, state, record.RunID, record.Path, joinRunning, record.Expected, record.Completed)
		return requireOneRow(result, err)
	})
}

// requireOneRow is a fenced UPDATE's outcome: it must have moved exactly
// the one record its checks read in the same writer transaction, or the
// write conflicts.
func requireOneRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return ErrRecordConflict
	}
	return nil
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
		// A checkpoint written before #334 may name another artifact than
		// the one its run was admitted under; it is not this run's to resume.
		// Such a run fails closed: it is recovered under neither artifact,
		// and SaveCheckpoint cannot replace the row.
		if _, admitted, err := runStateAndArtifact(ctx, tx, runID); err != nil {
			return err
		} else if admitted != artifactDigest {
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
