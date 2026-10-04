package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/store/distributed"
)

var (
	ErrWaitNotFound       = errors.New("cluster: wait not found")
	ErrWaitClosed         = errors.New("cluster: wait is already closed")
	ErrUnauthorizedSignal = errors.New("cluster: signal is unauthorized")
)

type WaitRecord struct {
	WaitID         string          `json:"waitId"`
	RunID          string          `json:"runId"`
	Tenant         string          `json:"tenant"`
	Name           string          `json:"name"`
	DueAt          time.Time       `json:"dueAt,omitempty"`
	State          string          `json:"state"`
	ArtifactDigest string          `json:"artifactDigest,omitempty"`
	StepID         string          `json:"stepId,omitempty"`
	OperationKey   string          `json:"operationKey,omitempty"`
	TimeoutMillis  int64           `json:"timeoutMillis,omitempty"`
	SignalID       string          `json:"signalId,omitempty"`
	Principal      string          `json:"principal,omitempty"`
	Payload        json.RawMessage `json:"payload,omitempty"`
}

type SignalResult struct {
	Accepted  bool `json:"accepted"`
	Duplicate bool `json:"duplicate"`
	Late      bool `json:"late"`
}

func (r *Runtime) GetWait(ctx context.Context, tenant, waitID string) (WaitRecord, error) {
	if tenant == "" || waitID == "" {
		return WaitRecord{}, ErrInvalid
	}
	data, revision, err := r.store.ReadState(ctx, r.Partition(tenant), waitStateID(tenant, waitID))
	if err != nil {
		return WaitRecord{}, err
	}
	if revision == 0 {
		return WaitRecord{}, ErrWaitNotFound
	}
	var record WaitRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return WaitRecord{}, err
	}
	if record.Tenant != tenant {
		return WaitRecord{}, ErrWaitNotFound
	}
	return record, nil
}

// ScheduleWait stores a timer under the same partition fence as the run.
func (r *Runtime) ScheduleWait(ctx context.Context, owner distributed.Owner, runID, waitID, name string, dueAt time.Time) (WaitRecord, error) {
	return r.scheduleWait(ctx, owner, runID, waitID, name, dueAt, engine.StepIdentity{}, 0)
}

func (r *Runtime) scheduleWait(ctx context.Context, owner distributed.Owner, runID, waitID, name string, dueAt time.Time, identity engine.StepIdentity, timeoutMillis int64) (WaitRecord, error) {
	if runID == "" || waitID == "" || name == "" || owner.Partition == "" || (identity.StepID == "" && dueAt.IsZero()) {
		return WaitRecord{}, ErrInvalid
	}
	run, _, err := r.readRun(ctx, owner.Partition, runID)
	if err != nil {
		return WaitRecord{}, fmt.Errorf("%w: read run before scheduling wait: %w", ErrUnavailable, err)
	}
	if run.RunID == "" || run.Tenant == "" || r.Partition(run.Tenant) != owner.Partition || run.State != "running" || run.Fence != owner.Token || run.OwnerID != owner.ID {
		return WaitRecord{}, distributed.ErrOwnershipLost
	}
	stateID := waitStateID(run.Tenant, waitID)
	if data, revision, err := r.store.ReadState(ctx, owner.Partition, stateID); err != nil {
		return WaitRecord{}, err
	} else if revision != 0 {
		var existing WaitRecord
		if err := json.Unmarshal(data, &existing); err != nil {
			return WaitRecord{}, err
		}
		identityMismatch := identity.StepID != "" && (existing.ArtifactDigest != identity.ArtifactDigest || existing.StepID != identity.StepID || existing.OperationKey != identity.OperationKey || existing.TimeoutMillis != timeoutMillis)
		dueMismatch := identity.StepID == "" && !existing.DueAt.Equal(dueAt.UTC())
		if existing.RunID != runID || existing.Name != name || identityMismatch || dueMismatch {
			return WaitRecord{}, ErrRequestConflict
		}
		return existing, nil
	}
	record := WaitRecord{WaitID: waitID, RunID: runID, Tenant: run.Tenant, Name: name, State: "waiting", ArtifactDigest: identity.ArtifactDigest, StepID: identity.StepID, OperationKey: identity.OperationKey, TimeoutMillis: timeoutMillis}
	if !dueAt.IsZero() {
		record.DueAt = dueAt.UTC()
	}
	encoded, _ := json.Marshal(record)
	var timer *distributed.TimerIndexMutation
	if !record.DueAt.IsZero() {
		timer = &distributed.TimerIndexMutation{StateID: stateID, DueAt: record.DueAt}
	}
	if _, err := r.store.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{{StateID: stateID, ExpectedRevision: 0, State: encoded}}, timer, waitTransition("scheduled", run.Tenant+"\x00"+waitID), "wait.scheduled", encoded); err != nil {
		return WaitRecord{}, err
	}
	return record, nil
}

// DeliverSignal uses the current owner fence and a compare-and-swap against
// the wait state. A signal and timer race therefore have one ordered winner.
func (r *Runtime) DeliverSignal(ctx context.Context, tenant, waitID, signalID, principal string, payload json.RawMessage, authorized bool) (SignalResult, error) {
	if !authorized {
		return SignalResult{}, ErrUnauthorizedSignal
	}
	if tenant == "" || waitID == "" || signalID == "" || len(signalID) > 180 || principal == "" || !json.Valid(payload) || len(payload) > MaxInputBytes {
		return SignalResult{}, ErrInvalid
	}
	partition := r.Partition(tenant)
	owner, err := r.store.CurrentOwner(ctx, partition)
	if err != nil {
		return SignalResult{}, fmt.Errorf("%w: no current partition owner: %v", ErrUnavailable, err)
	}
	stateID := waitStateID(tenant, waitID)
	data, revision, err := r.store.ReadState(ctx, partition, stateID)
	if err != nil {
		return SignalResult{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	if revision == 0 {
		return SignalResult{}, ErrWaitNotFound
	}
	var wait WaitRecord
	if err := json.Unmarshal(data, &wait); err != nil {
		return SignalResult{}, err
	}
	if wait.Tenant != tenant {
		return SignalResult{}, ErrWaitNotFound
	}
	if wait.State == "signaled" && wait.SignalID == signalID {
		if wait.Principal != principal || string(wait.Payload) != string(payload) {
			return SignalResult{}, ErrRequestConflict
		}
		return SignalResult{Accepted: true, Duplicate: true}, nil
	}
	if wait.State != "waiting" {
		if err := r.commitLateSignal(ctx, owner, tenant, waitID, signalID, principal, payload); err != nil {
			return SignalResult{}, err
		}
		return SignalResult{Late: true}, nil
	}
	wait.State, wait.SignalID, wait.Principal = "signaled", signalID, principal
	wait.Payload = append(json.RawMessage(nil), payload...)
	encoded, _ := json.Marshal(wait)
	eventID := waitTransition("signal", tenant+"\x00"+waitID+"\x00"+signalID)
	var commitErr error
	var timerIndex *distributed.TimerIndexMutation
	if !wait.DueAt.IsZero() {
		timerIndex = &distributed.TimerIndexMutation{StateID: stateID, DueAt: wait.DueAt, Delete: true}
	}
	if wait.StepID != "" {
		run, runRevision, readErr := r.readRun(ctx, partition, wait.RunID)
		if readErr != nil {
			return SignalResult{}, fmt.Errorf("%w: read suspended run for signal: %v", ErrUnavailable, readErr)
		}
		if run.State == "waiting" || run.State == "running" {
			run.State, run.OwnerID, run.Fence = "accepted", owner.ID, owner.Token
			encodedRun, _ := json.Marshal(run)
			_, commitErr = r.store.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{
				{StateID: stateID, ExpectedRevision: revision, State: encoded},
				{StateID: run.RunID, ExpectedRevision: runRevision, State: encodedRun},
			}, timerIndex, eventID, "wait.signaled", encoded)
		} else {
			_, commitErr = r.store.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{{StateID: stateID, ExpectedRevision: revision, State: encoded}}, timerIndex, eventID, "wait.signaled", encoded)
		}
	} else {
		_, commitErr = r.store.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{{StateID: stateID, ExpectedRevision: revision, State: encoded}}, nil, eventID, "wait.signaled", encoded)
	}
	if commitErr != nil {
		if errors.Is(commitErr, distributed.ErrStateConflict) || errors.Is(commitErr, distributed.ErrAlreadyWritten) {
			latestData, latestRevision, readErr := r.store.ReadState(ctx, partition, stateID)
			if readErr != nil {
				return SignalResult{}, fmt.Errorf("%w: reconcile signal race: %v", ErrUnavailable, readErr)
			}
			var latest WaitRecord
			if latestRevision > 0 && json.Unmarshal(latestData, &latest) == nil && latest.State == "signaled" && latest.SignalID == signalID && latest.Principal == principal && string(latest.Payload) == string(payload) {
				return SignalResult{Accepted: true, Duplicate: true}, nil
			}
			if err := r.commitLateSignal(ctx, owner, tenant, waitID, signalID, principal, payload); err != nil {
				return SignalResult{}, err
			}
			return SignalResult{Late: true}, nil
		}
		return SignalResult{}, commitErr
	}
	return SignalResult{Accepted: true}, nil
}

func (r *Runtime) commitLateSignal(ctx context.Context, owner distributed.Owner, tenant, waitID, signalID, principal string, payload json.RawMessage) error {
	data, _ := json.Marshal(struct {
		Tenant    string          `json:"tenant"`
		WaitID    string          `json:"waitId"`
		SignalID  string          `json:"signalId"`
		Principal string          `json:"principal"`
		Payload   json.RawMessage `json:"payload"`
	}{tenant, waitID, signalID, principal, payload})
	id := waitTransition("late-signal", tenant+"\x00"+waitID+"\x00"+signalID)
	err := r.store.Commit(ctx, owner, id, "wait.signal_late", data)
	if !errors.Is(err, distributed.ErrAlreadyWritten) {
		return err
	}
	committed, readErr := r.store.Read(ctx, owner.Partition, id)
	if readErr != nil {
		return readErr
	}
	var event struct {
		ID      string          `json:"id"`
		Kind    string          `json:"kind"`
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(committed, &event) == nil && event.ID == id && event.Kind == "wait.signal_late" && string(event.Payload) == string(data) {
		return nil
	}
	return ErrRequestConflict
}

// FireDueWaits commits timer claims with the current owner fence. A bounded
// limit prevents a timer backlog from starving ordinary run admission.
func (r *Runtime) FireDueWaits(ctx context.Context, owner distributed.Owner, now time.Time, limit int) ([]WaitRecord, error) {
	if limit < 1 {
		return nil, ErrInvalid
	}
	timers, err := r.store.ListDueTimers(ctx, owner.Partition, now, limit)
	if err != nil {
		return nil, err
	}
	fired := make([]WaitRecord, 0, len(timers))
	for _, timer := range timers {
		data, revision, err := r.store.ReadState(ctx, owner.Partition, timer.StateID)
		if err != nil {
			return nil, err
		}
		var current WaitRecord
		if revision == 0 || json.Unmarshal(data, &current) != nil || current.State != "waiting" || current.DueAt.IsZero() || current.DueAt.After(now) {
			continue
		}
		if current.Tenant == "" || r.Partition(current.Tenant) != owner.Partition || waitStateID(current.Tenant, current.WaitID) != timer.StateID {
			return fired, ErrRequestConflict
		}
		current.State = "timed_out"
		encoded, _ := json.Marshal(current)
		eventID := waitTransition("timer", current.Tenant+"\x00"+current.WaitID)
		var commitErr error
		timerMutation := &distributed.TimerIndexMutation{StateID: timer.StateID, DueAt: current.DueAt, Delete: true}
		if current.StepID != "" {
			run, runRevision, readErr := r.readRun(ctx, owner.Partition, current.RunID)
			if readErr != nil {
				return fired, readErr
			}
			if run.State == "waiting" || run.State == "running" {
				run.State, run.OwnerID, run.Fence = "accepted", owner.ID, owner.Token
				encodedRun, _ := json.Marshal(run)
				_, commitErr = r.store.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{
					{StateID: timer.StateID, ExpectedRevision: revision, State: encoded},
					{StateID: run.RunID, ExpectedRevision: runRevision, State: encodedRun},
				}, timerMutation, eventID, "wait.timed_out", encoded)
			} else if run.State == "accepted" {
				_, commitErr = r.store.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{{StateID: timer.StateID, ExpectedRevision: revision, State: encoded}}, timerMutation, eventID, "wait.timed_out", encoded)
			} else {
				continue
			}
		} else {
			_, commitErr = r.store.CommitFencedWaitStates(ctx, owner, []distributed.StateMutation{{StateID: timer.StateID, ExpectedRevision: revision, State: encoded}}, timerMutation, eventID, "wait.timed_out", encoded)
		}
		if commitErr != nil {
			if errors.Is(commitErr, distributed.ErrStateConflict) || errors.Is(commitErr, distributed.ErrAlreadyWritten) {
				continue
			}
			return fired, commitErr
		}
		fired = append(fired, current)
	}
	return fired, nil
}

func waitStateID(tenant, waitID string) string {
	hash := sha256.Sum256([]byte(tenant + "\x00" + waitID))
	return "wait-" + hex.EncodeToString(hash[:])
}

func waitTransition(kind, identity string) string {
	hash := sha256.Sum256([]byte(kind + "\x00" + identity))
	return kind + "-" + hex.EncodeToString(hash[:16])
}

// WaitIDFor returns the stable external signal address for a durable wait step.
// Callers must still authenticate and authorize every signal independently.
func WaitIDFor(runID, stepID string) string {
	hash := sha256.Sum256([]byte(runID + "\x00" + stepID))
	return "wait-" + hex.EncodeToString(hash[:])
}
