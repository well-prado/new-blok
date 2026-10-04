package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/well-prado/new-blok/store/distributed"
)

var (
	ErrWaitNotFound       = errors.New("cluster: wait not found")
	ErrWaitClosed         = errors.New("cluster: wait is already closed")
	ErrUnauthorizedSignal = errors.New("cluster: signal is unauthorized")
)

type WaitRecord struct {
	WaitID    string          `json:"waitId"`
	RunID     string          `json:"runId"`
	Tenant    string          `json:"tenant"`
	Name      string          `json:"name"`
	DueAt     time.Time       `json:"dueAt"`
	State     string          `json:"state"`
	SignalID  string          `json:"signalId,omitempty"`
	Principal string          `json:"principal,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
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
	if runID == "" || waitID == "" || name == "" || dueAt.IsZero() || owner.Partition == "" {
		return WaitRecord{}, ErrInvalid
	}
	run, _, err := r.readRun(ctx, owner.Partition, runID)
	if err != nil || run.RunID == "" || run.Tenant == "" || r.Partition(run.Tenant) != owner.Partition || run.State != "running" || run.Fence != owner.Token || run.OwnerID != owner.ID {
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
		if existing.RunID != runID || existing.Name != name || !existing.DueAt.Equal(dueAt.UTC()) {
			return WaitRecord{}, ErrRequestConflict
		}
		return existing, nil
	}
	record := WaitRecord{WaitID: waitID, RunID: runID, Tenant: run.Tenant, Name: name, DueAt: dueAt.UTC(), State: "waiting"}
	encoded, _ := json.Marshal(record)
	if _, err := r.store.CommitFencedState(ctx, owner, stateID, 0, waitTransition("scheduled", run.Tenant+"\x00"+waitID), "wait.scheduled", encoded, encoded); err != nil {
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
	if _, err := r.store.CommitFencedState(ctx, owner, stateID, revision, waitTransition("signal", tenant+"\x00"+waitID+"\x00"+signalID), "wait.signaled", encoded, encoded); err != nil {
		if errors.Is(err, distributed.ErrStateConflict) || errors.Is(err, distributed.ErrAlreadyWritten) {
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
		return SignalResult{}, err
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
	events, readErr := r.store.ListEvents(ctx, owner.Partition)
	if readErr != nil {
		return readErr
	}
	for _, event := range events {
		if event.ID == id && event.Kind == "wait.signal_late" && string(event.Payload) == string(data) {
			return nil
		}
	}
	return ErrRequestConflict
}

// FireDueWaits commits timer claims with the current owner fence. A bounded
// limit prevents a timer backlog from starving ordinary run admission.
func (r *Runtime) FireDueWaits(ctx context.Context, owner distributed.Owner, now time.Time, limit int) ([]WaitRecord, error) {
	if limit < 1 {
		return nil, ErrInvalid
	}
	events, err := r.store.ListEvents(ctx, owner.Partition)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	fired := make([]WaitRecord, 0, limit)
	for _, event := range events {
		if event.Kind != "wait.scheduled" {
			continue
		}
		var scheduled WaitRecord
		if err := json.Unmarshal(event.Payload, &scheduled); err != nil {
			return nil, err
		}
		if scheduled.Tenant == "" || r.Partition(scheduled.Tenant) != owner.Partition {
			return nil, ErrRequestConflict
		}
		waitKey := scheduled.Tenant + "\x00" + scheduled.WaitID
		if seen[waitKey] {
			continue
		}
		seen[waitKey] = true
		data, revision, err := r.store.ReadState(ctx, owner.Partition, waitStateID(scheduled.Tenant, scheduled.WaitID))
		if err != nil {
			return nil, err
		}
		var current WaitRecord
		if revision == 0 || json.Unmarshal(data, &current) != nil || current.State != "waiting" || current.DueAt.After(now) {
			continue
		}
		current.State = "timed_out"
		encoded, _ := json.Marshal(current)
		if _, err := r.store.CommitFencedState(ctx, owner, waitStateID(current.Tenant, current.WaitID), revision, waitTransition("timer", current.Tenant+"\x00"+current.WaitID), "wait.timed_out", encoded, encoded); err != nil {
			if errors.Is(err, distributed.ErrStateConflict) || errors.Is(err, distributed.ErrAlreadyWritten) {
				continue
			}
			return fired, err
		}
		fired = append(fired, current)
		if len(fired) == limit {
			break
		}
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
