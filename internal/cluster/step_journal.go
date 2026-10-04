package cluster

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/store/distributed"
)

type stepRecord struct {
	Identity       engine.StepIdentity `json:"identity"`
	State          string              `json:"state"`
	AttemptNumber  int                 `json:"attemptNumber"`
	CurrentAttempt string              `json:"currentAttempt"`
	Effects        []string            `json:"effects,omitempty"`
	Output         json.RawMessage     `json:"output,omitempty"`
}

type runStepJournal struct {
	runtime *Runtime
	owner   distributed.Owner
	record  RunRecord
}

func (j *runStepJournal) VerifyRun(ctx context.Context, runID, artifact, inputDigest string) error {
	if runID != j.record.RunID || artifact != j.record.ArtifactDigest || inputDigest != j.record.InputDigest {
		return ErrRequestConflict
	}
	record, _, err := j.runtime.readRun(ctx, j.owner.Partition, runID)
	if err != nil {
		return err
	}
	if record.RunID == "" || record.ArtifactDigest != artifact || record.InputDigest != inputDigest || record.OwnerID != j.owner.ID || record.Fence != j.owner.Token || record.State != "running" {
		return distributed.ErrOwnershipLost
	}
	return nil
}

func (j *runStepJournal) Load(ctx context.Context, identity engine.StepIdentity) (json.RawMessage, bool, error) {
	if !sameStepIdentity(identity, j.record, identity.StepID) {
		return nil, false, ErrRequestConflict
	}
	stateID := stepStateID(identity.OperationKey)
	data, revision, err := j.runtime.store.ReadState(ctx, j.owner.Partition, stateID)
	if err != nil {
		return nil, false, err
	}
	if revision == 0 {
		return nil, false, nil
	}
	var persisted stepRecord
	if err := json.Unmarshal(data, &persisted); err != nil {
		return nil, false, fmt.Errorf("cluster: decode step checkpoint: %w", err)
	}
	if persisted.Identity != identity {
		return nil, false, ErrRequestConflict
	}
	switch persisted.State {
	case "committed":
		return append(json.RawMessage(nil), persisted.Output...), true, nil
	case "uncertain":
		return nil, false, ErrEffectUncertain
	case "dispatched":
		if len(persisted.Effects) > 0 {
			persisted.State = "uncertain"
			encoded, _ := json.Marshal(persisted)
			_, markErr := j.runtime.store.CommitFencedState(ctx, j.owner, stateID, revision, stepTransition("uncertain", identity.OperationKey, persisted.CurrentAttempt), "step.uncertain", encoded, encoded)
			if markErr != nil && !errors.Is(markErr, distributed.ErrAlreadyWritten) {
				return nil, false, markErr
			}
			return nil, false, ErrEffectUncertain
		}
		return nil, false, nil // Pure calls are safe to execute again.
	case "retryable":
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("cluster: invalid step checkpoint state %q", persisted.State)
	}
}

func (j *runStepJournal) Begin(ctx context.Context, identity engine.StepIdentity, input json.RawMessage, effects []string) (engine.StepAttempt, error) {
	if !sameStepIdentity(identity, j.record, identity.StepID) || digest(input) != identity.InputDigest {
		return engine.StepAttempt{}, ErrRequestConflict
	}
	stateID := stepStateID(identity.OperationKey)
	current, revision, err := j.runtime.store.ReadState(ctx, j.owner.Partition, stateID)
	if err != nil {
		return engine.StepAttempt{}, err
	}
	record := stepRecord{Identity: identity, State: "dispatched", AttemptNumber: 1, Effects: append([]string(nil), effects...)}
	if revision != 0 {
		var old stepRecord
		if err := json.Unmarshal(current, &old); err != nil {
			return engine.StepAttempt{}, err
		}
		if old.Identity != identity {
			return engine.StepAttempt{}, ErrRequestConflict
		}
		if old.State == "committed" {
			return engine.StepAttempt{}, errors.New("cluster: committed step must be loaded, not dispatched")
		}
		if old.State == "uncertain" || old.State == "dispatched" && len(old.Effects) > 0 {
			return engine.StepAttempt{}, ErrEffectUncertain
		}
		record.AttemptNumber = old.AttemptNumber + 1
	}
	attemptID, err := randomAttemptID()
	if err != nil {
		return engine.StepAttempt{}, err
	}
	record.CurrentAttempt = attemptID
	encoded, _ := json.Marshal(record)
	transitionID := stepTransition("dispatch", identity.OperationKey, attemptID)
	if _, err := j.runtime.store.CommitFencedState(ctx, j.owner, stateID, revision, transitionID, "step.dispatched", encoded, encoded); err != nil {
		return engine.StepAttempt{}, err
	}
	return engine.StepAttempt{Identity: identity, AttemptID: attemptID}, nil
}

func (j *runStepJournal) Complete(ctx context.Context, attempt engine.StepAttempt, output json.RawMessage) error {
	return j.transition(ctx, attempt, "committed", "step.committed", output)
}

func (j *runStepJournal) Await(ctx context.Context, identity engine.WaitIdentity) (engine.WaitResult, bool, error) {
	if !sameStepIdentity(identity.Step, j.record, identity.Step.StepID) || identity.Name == "" || identity.TimeoutMillis < 0 {
		return engine.WaitResult{}, false, ErrRequestConflict
	}
	waitID := WaitIDFor(j.record.RunID, identity.Step.StepID)
	var dueAt time.Time
	if identity.TimeoutMillis > 0 {
		dueAt = time.Now().UTC().Add(time.Duration(identity.TimeoutMillis) * time.Millisecond)
	}
	wait, err := j.runtime.scheduleWait(ctx, j.owner, j.record.RunID, waitID, identity.Name, dueAt, identity.Step, identity.TimeoutMillis)
	if err != nil {
		return engine.WaitResult{}, false, err
	}
	switch wait.State {
	case "waiting":
		return engine.WaitResult{}, false, nil
	case "signaled":
		return engine.WaitResult{SignalID: wait.SignalID, Payload: append(json.RawMessage(nil), wait.Payload...)}, true, nil
	case "timed_out":
		return engine.WaitResult{TimedOut: true}, true, nil
	default:
		return engine.WaitResult{}, false, fmt.Errorf("cluster: invalid durable wait state %q", wait.State)
	}
}

func (j *runStepJournal) Fail(ctx context.Context, attempt engine.StepAttempt, effects []string, _ error) error {
	state := "retryable"
	kind := "step.retryable"
	if len(effects) > 0 {
		state, kind = "uncertain", "step.uncertain"
	}
	return j.transition(ctx, attempt, state, kind, nil)
}

func (j *runStepJournal) transition(ctx context.Context, attempt engine.StepAttempt, state, kind string, output json.RawMessage) error {
	if attempt.AttemptID == "" || !sameStepIdentity(attempt.Identity, j.record, attempt.Identity.StepID) {
		return ErrRequestConflict
	}
	stateID := stepStateID(attempt.Identity.OperationKey)
	data, revision, err := j.runtime.store.ReadState(ctx, j.owner.Partition, stateID)
	if err != nil {
		return err
	}
	if revision == 0 {
		return errors.New("cluster: step state disappeared before transition")
	}
	var current stepRecord
	if err := json.Unmarshal(data, &current); err != nil {
		return err
	}
	if current.Identity != attempt.Identity || current.CurrentAttempt != attempt.AttemptID {
		return errors.New("cluster: stale step attempt cannot publish")
	}
	if current.State == "committed" {
		if state == "committed" && string(current.Output) == string(output) {
			return nil
		}
		return errors.New("cluster: committed step cannot transition")
	}
	current.State, current.Output = state, append(json.RawMessage(nil), output...)
	encoded, _ := json.Marshal(current)
	id := stepTransition(state, attempt.Identity.OperationKey, attempt.AttemptID)
	if _, err := j.runtime.store.CommitFencedState(ctx, j.owner, stateID, revision, id, kind, encoded, encoded); err != nil && !errors.Is(err, distributed.ErrAlreadyWritten) {
		return err
	}
	return nil
}

func sameStepIdentity(identity engine.StepIdentity, run RunRecord, step string) bool {
	return identity.RunID == run.RunID && identity.ArtifactDigest == run.ArtifactDigest && identity.StepID == step && identity.InputDigest != "" && identity.OperationKey != ""
}

func stepStateID(operationKey string) string {
	return "step-" + operationKey[len("op:"):]
}

func stepTransition(kind, operationKey, attempt string) string {
	encoded, _ := json.Marshal(struct {
		Domain       string `json:"domain"`
		Kind         string `json:"kind"`
		OperationKey string `json:"operationKey"`
		AttemptID    string `json:"attemptId"`
	}{"blok.step-transition.v1", kind, operationKey, attempt})
	hash := sha256.Sum256(encoded)
	return kind + "-" + hex.EncodeToString(hash[:])
}

func randomAttemptID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}
