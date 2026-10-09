package journal

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/internal/engine"
)

// ErrReservedSignal: a signal addressed to a parent's child wait
// (engine.ChildWaitPrefix). Only the child run's end fires that wait.
var ErrReservedSignal = errors.New("journal: signal name is reserved for child runs")

// childRequestPrefix begins a child run's request key:
// "blok-child/<depth>/<parent run id>/<digest of the scope path>". A run's
// depth and parent are read from it, with no schema change and no walk.
const childRequestPrefix = "blok-child/"

// ChildWorkflow is how a run journal admits a child of a workflow: the
// artifact it runs and its input decoder, as the runner registers them.
type ChildWorkflow struct {
	ArtifactDigest string
	DecodeInput    func(json.RawMessage) (any, error)
}

// WithChildren sets how this run's child steps resolve their workflows
// (StartChild). Without it a child step is refused (child_workflow_unknown).
func (r *RunJournal) WithChildren(resolve func(workflow string) (ChildWorkflow, bool)) *RunJournal {
	r.mu.Lock()
	r.children = resolve
	r.mu.Unlock()
	return r
}

var _ engine.ChildJournal = (*RunJournal)(nil)

// childIdentity derives a child's run id and request key from its parent
// run and its step's scope path: a replay of the step derives the same
// child, so it can never admit a second one.
func childIdentity(parentRunID, scopePath string, depth int) (runID, requestKey string) {
	sum := sha256.Sum256([]byte("blok-child/v1\x00" + parentRunID + "\x00" + scopePath))
	return "run:" + hex.EncodeToString(sum[:16]), childRequestPrefix + strconv.Itoa(depth) + "/" + parentRunID + "/" + hex.EncodeToString(sum[16:])
}

// childLineage reads a run's child depth and parent from its request key:
// depth 0 and no parent for a run not started as a child.
func childLineage(requestKey string) (depth int, parent string) {
	if !strings.HasPrefix(requestKey, childRequestPrefix) {
		return 0, ""
	}
	parts := strings.Split(strings.TrimPrefix(requestKey, childRequestPrefix), "/")
	if len(parts) != 3 {
		return 0, ""
	}
	depth, err := strconv.Atoi(parts[0])
	if err != nil || depth < 1 {
		return 0, ""
	}
	return depth, parts[1]
}

// childUnavailable is the one refusal for a child that cannot be bound,
// whatever the reason: a run already holding the child's id that another
// principal admitted is reported exactly as one that does not exist, so a
// workflow learns nothing about another principal's runs (#397).
func childUnavailable() error {
	return &engine.ChildError{Code: "child_unavailable", Class: "conflict", Err: errors.New("the child run is not available")}
}

// StartChild starts the child of a child step (engine.ChildJournal), in
// one transaction under the run lease: the child run (derived id, the
// parent's principal, Input as its admitted input and, through its
// workflow's decoder, its engine input), its record (journal_children),
// the step's scope (decision: the child's id) and the parent's wait. A
// replay finds the scope and returns its child. The child is always a run
// this transaction admits, so it has no children and binding it cannot
// close a cycle: no walk is needed.
func (r *RunJournal) StartChild(ctx context.Context, request engine.ChildRequest) (engine.ChildStart, error) {
	if err := r.checkScope(request.Scope); err != nil || request.Scope.Kind != "child" || request.Workflow == "" || !json.Valid(request.Input) {
		return engine.ChildStart{}, errors.Join(ErrRequestConflict, err)
	}
	operation, err := r.operation(request.Step)
	if err != nil {
		return engine.ChildStart{}, err
	}
	r.mu.Lock()
	resolve := r.children
	r.mu.Unlock()
	attemptID, err := randomID("scope-attempt")
	if err != nil {
		return engine.ChildStart{}, err
	}
	var start engine.ChildStart
	err = r.settle(ctx, "child-start", true, func(tx *sql.Tx) error {
		var kind, parent, state string
		var input []byte
		queryErr := tx.QueryRowContext(ctx, `SELECT kind, parent_path, state, input_json FROM journal_scopes WHERE run_id = ? AND path = ?`, r.runID, request.Scope.Path).Scan(&kind, &parent, &state, &input)
		if queryErr == nil {
			var decision struct {
				Child string `json:"child"`
			}
			if kind != request.Scope.Kind || parent != request.Scope.ParentPath || json.Unmarshal(input, &decision) != nil || decision.Child == "" {
				return fmt.Errorf("%w: scope %s does not record a child", ErrRequestConflict, request.Scope.Path)
			}
			start = engine.ChildStart{RunID: decision.Child, Entry: engine.ScopeEntry{Scope: request.Scope, Decision: append(json.RawMessage(nil), input...)}}
			switch state {
			case checkpointCompleted:
				start.Entry.Completed = true
				return nil
			case checkpointRunning:
				start.Entry.AttemptID = attemptID
				_, err := tx.ExecContext(ctx, `UPDATE journal_scopes SET attempt_id = ?, updated_at = ? WHERE run_id = ? AND path = ?`, attemptID, r.journal.now(), r.runID, request.Scope.Path)
				return err
			default:
				return fmt.Errorf("%w: scope %s is %s", ErrRecordFinal, request.Scope.Path, state)
			}
		}
		if !errors.Is(queryErr, sql.ErrNoRows) {
			return queryErr
		}
		var principal, requestKey string
		if err := tx.QueryRowContext(ctx, `SELECT principal, request_key FROM journal_runs WHERE run_id = ?`, r.runID).Scan(&principal, &requestKey); err != nil {
			return err
		}
		depth, _ := childLineage(requestKey)
		if depth+1 > r.journal.depth {
			return &engine.ChildError{Code: "child_depth_exceeded", Class: "admission", Err: fmt.Errorf("a child of a run at depth %d would nest deeper than %d", depth, r.journal.depth)}
		}
		var workflow ChildWorkflow
		ok := false
		if resolve != nil {
			workflow, ok = resolve(request.Workflow)
		}
		if !ok || workflow.ArtifactDigest == "" || workflow.DecodeInput == nil {
			return &engine.ChildError{Code: "child_workflow_unknown", Class: "configuration", Err: fmt.Errorf("workflow %q is not registered", request.Workflow)}
		}
		decoded, err := decodeChildInput(workflow, request.Input)
		if err != nil {
			return &engine.ChildError{Code: "child_input_invalid", Class: "validation", Err: err}
		}
		engineInput, err := engine.InputDigest(decoded)
		if err != nil {
			return &engine.ChildError{Code: "child_input_invalid", Class: "validation", Err: err}
		}
		childRunID, childKey := childIdentity(r.runID, request.Scope.Path, depth+1)
		result, err := tx.ExecContext(ctx, `INSERT INTO journal_runs
			(run_id, request_key, workflow, artifact_digest, input_json, input_digest, state, created_at, principal, engine_input_digest)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING`,
			childRunID, childKey, request.Workflow, workflow.ArtifactDigest, []byte(request.Input), digestBytes(request.Input), runAccepted, r.journal.now(), principal, engineInput)
		if err != nil {
			return err
		}
		if inserted, err := result.RowsAffected(); err != nil {
			return err
		} else if inserted == 0 {
			// A run already holds the child's id or request key, and this
			// step never recorded it: it is not this step's child. Whose
			// it is, and whether it exists at all, stays unsaid (#397).
			return childUnavailable()
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_children (run_id, path, child_run_id, state, result_json) VALUES (?, ?, ?, ?, NULL)`, r.runID, request.Scope.Path, childRunID, childRunning); err != nil {
			return err
		}
		decision, _ := json.Marshal(map[string]string{"child": childRunID})
		if err := r.journal.insertScope(ctx, tx, ScopeRecord{RunID: r.runID, Path: request.Scope.Path, Kind: request.Scope.Kind, ParentPath: request.Scope.ParentPath, Input: decision}, attemptID); err != nil {
			return err
		}
		if _, err := r.journal.scheduleWait(ctx, tx, WaitRequest{RunID: r.runID, WaitID: waitID(operation, request.Step), Name: engine.ChildWaitPrefix + childRunID, InvocationPath: operation.InvocationPath, IterationPath: operation.IterationPath, DueAt: neverDue}); err != nil {
			return err
		}
		start = engine.ChildStart{RunID: childRunID, Entry: engine.ScopeEntry{Scope: request.Scope, AttemptID: attemptID, Decision: decision}}
		return nil
	})
	if err != nil {
		return engine.ChildStart{}, err
	}
	return start, nil
}

// decodeChildInput runs a child workflow's input decoder, turning a panic
// into an error.
func decodeChildInput(workflow ChildWorkflow, raw json.RawMessage) (decoded any, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("input decoder panicked: %v", p)
		}
	}()
	return workflow.DecodeInput(raw)
}

// settleParent records, in the transaction that ends a child run, how it
// ended: the parent's record of the child completes with the outcome, and
// the parent's wait for it fires with the outcome as payload, so the
// parent is listed for resumption. A run that is not a child, or whose
// parent's record has already settled, changes nothing.
func (j *Journal) settleParent(ctx context.Context, tx *sql.Tx, runID string, outcome engine.ChildOutcome) error {
	var requestKey string
	if err := tx.QueryRowContext(ctx, `SELECT request_key FROM journal_runs WHERE run_id = ?`, runID).Scan(&requestKey); err != nil {
		return err
	}
	_, parent := childLineage(requestKey)
	if parent == "" {
		return nil
	}
	payload, err := json.Marshal(outcome)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `UPDATE journal_children SET state = ?, result_json = ? WHERE run_id = ? AND child_run_id = ? AND state = ?`, childCompleted, payload, parent, runID, childRunning)
	if err != nil {
		return err
	}
	if settled, err := result.RowsAffected(); err != nil || settled == 0 {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE journal_waits SET state = ?, signal_id = ?, payload_json = ?, fired_at = ?, updated_at = ? WHERE run_id = ? AND name = ? AND state = ?`,
		waitFired, "child:"+runID, payload, j.now(), j.now(), parent, engine.ChildWaitPrefix+runID, waitWaiting)
	return err
}

// ChildrenToStart lists the children runID started that no holder has
// leased yet, for its runner to start at once rather than after a lease's
// grace (InterruptedRuns takes them then, should that runner have died).
func (j *Journal) ChildrenToStart(ctx context.Context, runID string) ([]string, error) {
	var children []string
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		var err error
		children, err = queryStrings(ctx, tx, `SELECT c.child_run_id FROM journal_children c JOIN journal_runs r ON r.run_id = c.child_run_id WHERE c.run_id = ? AND c.state = ? AND r.state = ? AND r.leased_at IS NULL ORDER BY c.path`, runID, childRunning, runAccepted)
		return err
	})
	return children, err
}

// IsChildRun reports whether run was started as another run's child.
func IsChildRun(run Run) bool {
	_, parent := childLineage(run.RequestKey)
	return parent != ""
}
