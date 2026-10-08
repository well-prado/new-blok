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
	// ErrRecordConflict: the write contradicts what the record holds: a
	// join's expected count, a filled join slot given another value, a
	// child's run id, or a row the pre-#334 upsert left in a shape the write
	// cannot extend. Branches
	// filling different slots of one join never get it, whatever their order
	// or handle: each write merges into the stored slots in the writer
	// transaction. Two writers filling the same slot with different values
	// do: the first wins and the other gets ErrRecordConflict, the same
	// error a stale or wrong writer gets for a slot already filled. The
	// engine either serialises a join's fan-in or, on ErrRecordConflict,
	// re-reads the join and decides; retrying the same write cannot succeed.
	ErrRecordConflict = errors.New("journal: recovery record conflicts with the recorded one")
	// ErrChildRunNotFound: a new child record names a run the journal does
	// not hold.
	ErrChildRunNotFound = errors.New("journal: child run does not exist")
	// ErrChildPrincipalMismatch: a new child record names a run admitted
	// for another principal than its parent (#372). A child's result is
	// read by its parent, so binding another principal's run would hand
	// the parent that principal's output. Runs admitted with no principal
	// (the system principal) bind only to each other.
	ErrChildPrincipalMismatch = errors.New("journal: child run belongs to another principal than its parent")
	// ErrChildCycle: a new child record names its own parent run, or a run
	// the parent descends from through recorded children (#372): the
	// parent would wait for itself.
	ErrChildCycle = errors.New("journal: child run is its parent or one of the parent's ancestors")
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
// a run the journal holds (ErrChildRunNotFound), admitted for the parent's
// principal (ErrChildPrincipalMismatch), that is neither the parent nor one
// of its ancestors through recorded children (ErrChildCycle); another id is
// ErrRecordConflict. A record moves from running to completed only: a
// completed record is ErrRecordFinal unless the write repeats it byte for
// byte. A running record has no result, and a completed one has a JSON
// result: a write that breaks either is an invalid record, whether it would
// create the record or update it. The parent run must exist (ErrNotFound).
//
// The child's binding (its existence, principal and ancestry) is checked
// when the record is created, inside the writer transaction, and not on
// later writes: a run's principal never changes and neither does the
// record's child run id, so a record that passed once stays valid, and a
// child run compacted afterwards does not strand its parent's record.
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
	if record.State == childRunning && len(record.Result) > 0 {
		return errors.New("journal: a running child has no result")
	}
	if record.State == childCompleted && !json.Valid(record.Result) {
		return errors.New("journal: a completed child's result must be valid JSON")
	}
	return j.withTx(ctx, "child", func(tx *sql.Tx) error {
		if _, _, err := runStateAndArtifact(ctx, tx, record.RunID); err != nil {
			return err
		}
		var childRunID, state string
		var result []byte
		err := tx.QueryRowContext(ctx, `SELECT child_run_id, state, result_json FROM journal_children WHERE run_id = ? AND path = ?`, record.RunID, record.Path).Scan(&childRunID, &state, &result)
		if errors.Is(err, sql.ErrNoRows) {
			if err := childBindable(ctx, tx, record.RunID, record.ChildRunID); err != nil {
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

// childBindable reports whether childRunID may be recorded as a child of
// parentRunID (#372): the child run exists (ErrChildRunNotFound), was
// admitted for the parent's principal (ErrChildPrincipalMismatch), and is
// not the parent or an ancestor of it through recorded children
// (ErrChildCycle). It runs in the writer transaction that inserts the
// record, so no concurrent write can close a cycle between the check and
// the insert.
func childBindable(ctx context.Context, tx *sql.Tx, parentRunID, childRunID string) error {
	var parentPrincipal, childPrincipal string
	if err := tx.QueryRowContext(ctx, `SELECT principal FROM journal_runs WHERE run_id = ?`, parentRunID).Scan(&parentPrincipal); err != nil {
		return err
	}
	err := tx.QueryRowContext(ctx, `SELECT principal FROM journal_runs WHERE run_id = ?`, childRunID).Scan(&childPrincipal)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrChildRunNotFound
	}
	if err != nil {
		return err
	}
	if childPrincipal != parentPrincipal {
		return ErrChildPrincipalMismatch
	}
	// The child is the parent or an ancestor of it exactly when the parent
	// is the child or one of its descendants. Walk down from the child,
	// itself included, through its recorded children: each step reads
	// journal_children by run_id, its primary key's prefix, so the walk
	// costs the child's own subtree (nothing yet, for a run just
	// started), not the table. UNION (not UNION ALL) visits each run once,
	// so the walk ends even on a cycle a pre-#372 journal may hold.
	var found int
	err = tx.QueryRowContext(ctx, `WITH RECURSIVE descendants(run_id) AS (
			SELECT ?
			UNION
			SELECT c.child_run_id FROM journal_children c JOIN descendants d ON c.run_id = d.run_id
		)
		SELECT 1 FROM descendants WHERE run_id = ? LIMIT 1`, childRunID, parentRunID).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return ErrChildCycle
}

// RecordJoin records which of a join's branches have come back. Results
// are positional: one slot per expected branch, in branch order, JSON null
// (or empty) while that branch has not come back, and Completed is the
// number of filled slots. No results at all is Expected empty slots. A
// branch whose own result is JSON null must therefore be recorded wrapped,
// or it reads as not yet back.
//
// Expected is fixed when the record is created. A write merges into the
// stored slots inside the writer transaction: it may fill empty slots, and
// an empty slot in the write leaves the stored one as it is, so branches
// that fill different slots never conflict, in any order or from any
// handle. A write that would change a filled slot, or names another
// Expected, is ErrRecordConflict (ErrRecordFinal once every slot is filled:
// a completed join is final). A write that brings nothing new, such as an
// identical retry, succeeds and changes nothing. State is derived from the
// slots; a caller State that disagrees, results of another length than
// Expected, or a Completed that is not the number of filled slots is an
// invalid record. The run must exist (ErrNotFound).
//
// Slots are compared in their compact JSON form, the form json.Marshal
// stores (spacing dropped; <, > and & escaped), so resending a slot spelled
// differently is the same slot. Nothing else is normalised: reordered
// object keys or 1.0 for 1 are another value.
//
// A row the pre-#334 upsert left in another shape is never changed. One
// whose results are not one slot per branch, or whose completed count
// disagrees with its slots, cannot be read as slots: every write but an
// identical one is ErrRecordConflict. One in a state other than running or
// completed refuses a write that would fill a slot (ErrRecordConflict), and
// a write that brings nothing new succeeds and changes nothing, as for any
// join. A row stored with no results (results_json null) is read as its
// empty slots.
func (j *Journal) RecordJoin(ctx context.Context, record JoinRecord) error {
	if record.RunID == "" || record.Path == "" || record.Expected < 1 {
		return errors.New("journal: invalid join record")
	}
	incoming, err := joinSlots(record.Results, record.Expected)
	if err != nil {
		return err
	}
	if filledSlots(incoming) != record.Completed {
		return errors.New("journal: invalid join record: completed is not the number of filled slots")
	}
	state := joinRunning
	if record.Completed == record.Expected {
		state = joinCompleted
	}
	if record.State != "" && record.State != state {
		return errors.New("journal: invalid join record: state contradicts the slots")
	}
	encoded, err := json.Marshal(incoming)
	if err != nil {
		return err
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
		if expected == record.Expected && completed == record.Completed && stored == state && bytes.Equal(results, encoded) {
			return nil
		}
		final := ErrRecordConflict
		if stored == joinCompleted {
			final = ErrRecordFinal
		}
		if expected != record.Expected {
			return final
		}
		var raw []json.RawMessage
		if json.Unmarshal(results, &raw) != nil {
			return ErrRecordConflict
		}
		// Normalised like a write: a row the pre-#334 upsert stored with nil
		// results ('null') reads as its empty slots.
		current, err := joinSlots(raw, expected)
		if err != nil || filledSlots(current) != completed {
			return ErrRecordConflict
		}
		merged := make([]json.RawMessage, expected)
		added := 0
		for i := range merged {
			merged[i] = current[i]
			switch {
			case !slotFilled(incoming[i]):
			case !slotFilled(current[i]):
				merged[i] = incoming[i]
				added++
			case !bytes.Equal(current[i], incoming[i]):
				return final
			}
		}
		if added == 0 {
			return nil
		}
		next := completed + added
		nextState := joinRunning
		if next == expected {
			nextState = joinCompleted
		}
		mergedJSON, err := json.Marshal(merged)
		if err != nil {
			return err
		}
		// The fence also refuses a row the pre-#334 upsert left in a state
		// other than running: it changes no row.
		updated, err := tx.ExecContext(ctx, `UPDATE journal_joins SET completed = ?, results_json = ?, state = ? WHERE run_id = ? AND path = ? AND state = ? AND expected = ? AND completed = ?`, next, mergedJSON, nextState, record.RunID, record.Path, joinRunning, expected, completed)
		return requireOneRow(updated, err)
	})
}

// joinSlots normalises a join's results to exactly expected slots, an
// empty slot as JSON null; no results at all is expected empty slots.
func joinSlots(results []json.RawMessage, expected int) ([]json.RawMessage, error) {
	if len(results) == 0 {
		results = make([]json.RawMessage, expected)
	}
	if len(results) != expected {
		return nil, errors.New("journal: invalid join record: results must have one slot per expected branch")
	}
	slots := make([]json.RawMessage, expected)
	for i, slot := range results {
		trimmed := bytes.TrimSpace(slot)
		if !slotFilled(trimmed) {
			slots[i] = json.RawMessage("null")
			continue
		}
		if !json.Valid(trimmed) {
			return nil, errors.New("journal: invalid join record: a result is not valid JSON")
		}
		// The compact form is what json.Marshal stores, so it is also what
		// a slot is compared in.
		compact, err := json.Marshal(json.RawMessage(trimmed))
		if err != nil {
			return nil, err
		}
		slots[i] = compact
	}
	return slots, nil
}

func slotFilled(slot json.RawMessage) bool {
	trimmed := bytes.TrimSpace(slot)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

func filledSlots(slots []json.RawMessage) int {
	filled := 0
	for _, slot := range slots {
		if slotFilled(slot) {
			filled++
		}
	}
	return filled
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
