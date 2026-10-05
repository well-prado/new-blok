// Package distributed is an executable ownership and fencing spike backed by
// etcd's linearizable transactions. It is deliberately not wired into app or
// the runtime; it records the smallest boundary needed to test the decision.
package distributed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	ErrOwnershipLost  = errors.New("distributed store: partition ownership lost")
	ErrAlreadyWritten = errors.New("distributed store: event already committed")
	ErrIncarnation    = errors.New("distributed store: cluster incarnation mismatch")
	ErrStateConflict  = errors.New("distributed store: state revision changed")
	ErrAdmissionFull  = errors.New("distributed store: bounded admission capacity is full")
	ErrConfigConflict = errors.New("distributed store: immutable cluster setting conflicts with existing configuration")
	// ErrAdmissionConflict reports that the request's admission identity is
	// already committed with a different payload. It is definite, not retryable.
	ErrAdmissionConflict = errors.New("distributed store: request identity conflicts with committed admission")
	// ErrAdmissionSlotTaken reports that a slot chosen from an earlier read
	// was consumed by a concurrent admission. It says nothing about whether
	// other slots remain free; callers re-read capacity before rejecting.
	ErrAdmissionSlotTaken = errors.New("distributed store: chosen admission slot was taken concurrently")
)

const MaxPayloadBytes = 512 << 10

type Client interface {
	Grant(context.Context, int64) (*clientv3.LeaseGrantResponse, error)
	Revoke(context.Context, clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
	KeepAliveOnce(context.Context, clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error)
	Txn(context.Context) clientv3.Txn
	Get(context.Context, string, ...clientv3.OpOption) (*clientv3.GetResponse, error)
}

type Store struct {
	client      Client
	incarnation string
}

func New(ctx context.Context, client Client, incarnation string) (*Store, error) {
	if client == nil || !validName(incarnation) {
		return nil, errors.New("distributed store: etcd client and explicit cluster incarnation are required")
	}
	key := incarnationKey()
	response, err := client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, incarnation)).Commit()
	if err != nil {
		return nil, fmt.Errorf("distributed store: establish cluster incarnation: %w", err)
	}
	if !response.Succeeded {
		current, err := client.Get(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("distributed store: read cluster incarnation: %w", err)
		}
		if len(current.Kvs) == 0 || string(current.Kvs[0].Value) != incarnation {
			return nil, ErrIncarnation
		}
	}
	return &Store{client: client, incarnation: incarnation}, nil
}

// RotateIncarnation is an offline restore barrier. Recovery tooling must call
// it with a fresh value before routing any worker to a restored cluster. The
// incarnation lives outside the partition data namespace so snapshot restore
// cannot silently reuse a pre-restore fencing token.
func (s *Store) RotateIncarnation(ctx context.Context, previous, next string) error {
	if !validName(previous) || !validName(next) || previous == next || s.incarnation != previous {
		return ErrIncarnation
	}
	response, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", previous)).
		Then(clientv3.OpPut(incarnationKey(), next)).Commit()
	if err != nil {
		return fmt.Errorf("distributed store: rotate restored-cluster incarnation: %w", err)
	}
	if !response.Succeeded {
		return ErrIncarnation
	}
	s.incarnation = next
	return nil
}

// Owner is a lease on one stable partition. Token is the etcd revision at
// which the owner key was created; it is monotonic for the lifetime of a
// cluster and is compared by every state commit.
type Owner struct {
	Partition   string
	ID          string
	Token       int64
	Incarnation string
	LeaseID     clientv3.LeaseID
}

type event struct {
	Partition   string          `json:"partition"`
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Fence       int64           `json:"fence"`
	Incarnation string          `json:"incarnation"`
	Payload     json.RawMessage `json:"payload"`
}

// StateMutation identifies one projection update guarded by the same owner
// fence and event identity as the rest of a distributed transition.
type StateMutation struct {
	StateID          string
	ExpectedRevision int64
	State            []byte
}

type TimerIndexMutation struct {
	StateID string
	DueAt   time.Time
	Delete  bool
}

type DueTimer struct {
	StateID string
	DueAt   time.Time
}

// CommittedEvent is a linearizably read immutable transition record.
type CommittedEvent struct {
	Partition   string          `json:"partition"`
	ID          string          `json:"id"`
	Kind        string          `json:"kind"`
	Fence       int64           `json:"fence"`
	Incarnation string          `json:"incarnation"`
	Payload     json.RawMessage `json:"payload"`
	Revision    int64           `json:"revision"`
}

func (s *Store) Acquire(ctx context.Context, partition, ownerID string, ttl time.Duration) (Owner, error) {
	if !validName(partition) || !validName(ownerID) || ttl < time.Second || ttl > 24*time.Hour {
		return Owner{}, errors.New("distributed store: valid partition, owner, and TTL (1s..24h) are required")
	}
	lease, err := s.client.Grant(ctx, int64((ttl+time.Second-1)/time.Second))
	if err != nil {
		return Owner{}, fmt.Errorf("distributed store: grant lease: %w", err)
	}
	key := ownerKey(s.incarnation, partition)
	response, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation),
			clientv3.Compare(clientv3.Version(key), "=", 0),
		).
		Then(clientv3.OpPut(key, ownerID, clientv3.WithLease(lease.ID))).Commit()
	if err != nil {
		s.cleanupLease(lease.ID)
		return Owner{}, fmt.Errorf("distributed store: acquire partition: %w", err)
	}
	if !response.Succeeded {
		s.cleanupLease(lease.ID)
		current, err := s.client.Get(ctx, incarnationKey())
		if err != nil {
			return Owner{}, fmt.Errorf("distributed store: verify incarnation after rejected acquisition: %w", err)
		}
		if len(current.Kvs) == 0 || string(current.Kvs[0].Value) != s.incarnation {
			return Owner{}, ErrIncarnation
		}
		return Owner{}, ErrOwnershipLost
	}
	return Owner{Partition: partition, ID: ownerID, Token: response.Header.Revision, Incarnation: s.incarnation, LeaseID: lease.ID}, nil
}

// CurrentOwner reads the authoritative live partition lease for fenced ingress
// transitions such as signal delivery. The subsequent write must still compare
// this fence; the read alone is never authority.
func (s *Store) CurrentOwner(ctx context.Context, partition string) (Owner, error) {
	if !validName(partition) {
		return Owner{}, errors.New("distributed store: valid partition is required")
	}
	transaction, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(clientv3.OpGet(ownerKey(s.incarnation, partition))).Commit()
	if err != nil {
		return Owner{}, fmt.Errorf("distributed store: read current owner: %w", err)
	}
	if !transaction.Succeeded {
		return Owner{}, ErrIncarnation
	}
	if len(transaction.Responses) == 0 || len(transaction.Responses[0].GetResponseRange().Kvs) == 0 {
		return Owner{}, ErrOwnershipLost
	}
	entry := transaction.Responses[0].GetResponseRange().Kvs[0]
	owner := Owner{Partition: partition, ID: string(entry.Value), Token: entry.CreateRevision, Incarnation: s.incarnation, LeaseID: clientv3.LeaseID(entry.Lease)}
	if !owner.valid() {
		return Owner{}, ErrOwnershipLost
	}
	return owner, nil
}

// Renew extends a live lease. An expired lease cannot be revived; the caller
// must acquire again and receive a new fencing token.
func (s *Store) Renew(ctx context.Context, owner Owner) error {
	if !owner.valid() {
		return ErrOwnershipLost
	}
	response, err := s.client.KeepAliveOnce(ctx, owner.LeaseID)
	// A live etcd lease is not proof that this owner is authoritative: a
	// restored cluster can retain the lease while its incarnation is rotated.
	// Renew only reports ownership while the same linearizable fence still
	// holds, matching Commit's authority check.
	check, checkErr := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", owner.Incarnation),
			clientv3.Compare(clientv3.CreateRevision(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.Token),
			clientv3.Compare(clientv3.Value(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.ID),
		).
		Then(clientv3.OpGet(ownerKey(owner.Incarnation, owner.Partition))).Commit()
	if checkErr != nil {
		if err != nil {
			return fmt.Errorf("distributed store: renew lease: %v; verify ownership: %w", err, checkErr)
		}
		return fmt.Errorf("distributed store: verify renewed ownership: %w", checkErr)
	}
	if !check.Succeeded {
		return ErrOwnershipLost
	}
	if err != nil {
		return fmt.Errorf("distributed store: renew lease: %w", err)
	}
	if response == nil || response.TTL <= 0 {
		return ErrOwnershipLost
	}
	return nil
}

// Release relinquishes only the exact live owner fence and then revokes its
// lease. A stale process cannot delete a successor's owner key.
func (s *Store) Release(ctx context.Context, owner Owner) error {
	if !owner.valid() {
		return ErrOwnershipLost
	}
	response, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", owner.Incarnation),
			clientv3.Compare(clientv3.CreateRevision(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.Token),
			clientv3.Compare(clientv3.Value(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.ID),
		).
		Then(clientv3.OpDelete(ownerKey(owner.Incarnation, owner.Partition))).Commit()
	if err != nil {
		return fmt.Errorf("distributed store: release partition: %w", err)
	}
	if !response.Succeeded {
		return ErrOwnershipLost
	}
	if _, err := s.client.Revoke(ctx, owner.LeaseID); err != nil {
		return fmt.Errorf("distributed store: owner released but lease cleanup failed: %w", err)
	}
	return nil
}

// Commit atomically compares the current partition owner and writes the event
// to etcd. The transaction is the authority: a process pause, lease expiry or
// takeover that ordered before this transaction makes the compare fail.
func (s *Store) Commit(ctx context.Context, owner Owner, id, kind string, payload []byte) error {
	if !owner.valid() || !validName(id) || !validName(kind) || len(payload) > MaxPayloadBytes || !json.Valid(payload) {
		return errors.New("distributed store: valid owner, event identity, JSON payload and payload bound are required")
	}
	key := eventKey(owner.Partition, id)
	encoded, err := json.Marshal(event{Partition: owner.Partition, ID: id, Kind: kind, Fence: owner.Token, Incarnation: owner.Incarnation, Payload: payload})
	if err != nil {
		return fmt.Errorf("distributed store: encode event: %w", err)
	}
	response, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", owner.Incarnation),
			clientv3.Compare(clientv3.CreateRevision(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.Token),
			clientv3.Compare(clientv3.Value(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.ID),
			clientv3.Compare(clientv3.Version(key), "=", 0),
		).
		Then(clientv3.OpPut(key, string(encoded))).Commit()
	if err != nil {
		return fmt.Errorf("distributed store: commit (outcome must be reconciled by event ID): %w", err)
	}
	if response.Succeeded {
		return nil
	}
	current, err := s.client.Get(ctx, ownerKey(owner.Incarnation, owner.Partition))
	if err != nil {
		return fmt.Errorf("distributed store: inspect rejected commit: %w", err)
	}
	incarnation, incErr := s.client.Get(ctx, incarnationKey())
	if incErr != nil {
		return fmt.Errorf("distributed store: inspect rejected commit incarnation: %w", incErr)
	}
	if len(incarnation.Kvs) == 0 || string(incarnation.Kvs[0].Value) != owner.Incarnation || len(current.Kvs) == 0 || current.Kvs[0].CreateRevision != owner.Token || string(current.Kvs[0].Value) != owner.ID {
		return ErrOwnershipLost
	}
	return ErrAlreadyWritten
}

// FreeAdmissionSlots reports bounded queue slots that are currently unused.
// The subsequent CommitAdmission compares each selected slot in the same
// transaction as the run record, so concurrent ingress cannot over-admit.
func (s *Store) FreeAdmissionSlots(ctx context.Context, partition, tenant string, partitionLimit, tenantLimit int) ([]string, []string, error) {
	if !validName(partition) || !validName(tenant) || partitionLimit < 1 || tenantLimit < 1 || tenantLimit > partitionLimit {
		return nil, nil, errors.New("distributed store: valid partition/tenant and bounded admission limits are required")
	}
	response, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(clientv3.OpGet(globalSlotsPrefix(partition), clientv3.WithPrefix()), clientv3.OpGet(tenantSlotsPrefix(partition, tenant), clientv3.WithPrefix())).Commit()
	if err != nil {
		return nil, nil, fmt.Errorf("distributed store: inspect admission capacity: %w", err)
	}
	if !response.Succeeded {
		return nil, nil, ErrIncarnation
	}
	usedGlobal := map[string]bool{}
	usedTenant := map[string]bool{}
	if len(response.Responses) > 0 {
		for _, entry := range response.Responses[0].GetResponseRange().Kvs {
			usedGlobal[string(entry.Key)] = true
		}
	}
	if len(response.Responses) > 1 {
		for _, entry := range response.Responses[1].GetResponseRange().Kvs {
			usedTenant[string(entry.Key)] = true
		}
	}
	global := make([]string, 0, partitionLimit)
	for slot := 0; slot < partitionLimit; slot++ {
		id := fmt.Sprintf("%d", slot)
		if !usedGlobal[globalSlotKey(partition, id)] {
			global = append(global, id)
		}
	}
	tenantSlots := make([]string, 0, tenantLimit)
	for slot := 0; slot < tenantLimit; slot++ {
		id := fmt.Sprintf("%d", slot)
		if !usedTenant[tenantSlotKey(partition, tenant, id)] {
			tenantSlots = append(tenantSlots, id)
		}
	}
	return global, tenantSlots, nil
}

// CommitAdmission durably accepts a run and consumes one global and one
// tenant slot atomically. It is idempotent by event ID; the caller reconciles
// ErrAlreadyWritten by reading and comparing the event's full payload, and
// treats ErrAdmissionSlotTaken as contention, not as exhausted capacity.
func (s *Store) CommitAdmission(ctx context.Context, partition, tenant, globalSlot, tenantSlot, runID string, state, payload []byte) error {
	if !validName(partition) || !validName(tenant) || !validName(globalSlot) || !validName(tenantSlot) || !validName(runID) || len(state) > MaxPayloadBytes || len(payload) > MaxPayloadBytes || !json.Valid(state) || !json.Valid(payload) {
		return errors.New("distributed store: valid admission identity, JSON values and payload bounds are required")
	}
	transitionID := "accepted-" + runID
	eventPath := eventKey(partition, transitionID)
	statePath := projectionKey(partition, runID)
	globalPath := globalSlotKey(partition, globalSlot)
	tenantPath := tenantSlotKey(partition, tenant, tenantSlot)
	encodedEvent, err := json.Marshal(event{Partition: partition, ID: transitionID, Kind: "run.accepted", Incarnation: s.incarnation, Payload: payload})
	if err != nil {
		return fmt.Errorf("distributed store: encode admission: %w", err)
	}
	response, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation),
			clientv3.Compare(clientv3.Version(eventPath), "=", 0),
			clientv3.Compare(clientv3.Version(statePath), "=", 0),
			clientv3.Compare(clientv3.Version(globalPath), "=", 0),
			clientv3.Compare(clientv3.Version(tenantPath), "=", 0),
		).
		Then(
			clientv3.OpPut(eventPath, string(encodedEvent)),
			clientv3.OpPut(statePath, string(state)),
			clientv3.OpPut(globalPath, runID),
			clientv3.OpPut(tenantPath, runID),
		).Commit()
	if err != nil {
		return fmt.Errorf("distributed store: admission commit (outcome must be reconciled by run ID): %w", err)
	}
	if response.Succeeded {
		return nil
	}
	current, err := s.client.Get(ctx, eventPath)
	if err != nil {
		return fmt.Errorf("distributed store: reconcile admission: %w", err)
	}
	if len(current.Kvs) > 0 {
		if string(current.Kvs[0].Value) == string(encodedEvent) {
			return ErrAlreadyWritten
		}
		return ErrAdmissionConflict
	}
	return ErrAdmissionSlotTaken
}

// ReleaseAdmissionSlots frees the run's capacity in the same fenced
// transition that publishes its terminal state.
func (s *Store) ReleaseAdmissionSlots(ctx context.Context, owner Owner, runID, tenant, globalSlot, tenantSlot string, expectedRevision int64, eventID, kind string, state, payload []byte) (int64, error) {
	if !owner.valid() || !validName(runID) || !validName(tenant) || !validName(globalSlot) || !validName(tenantSlot) || expectedRevision < 1 || !validName(eventID) || !validName(kind) || !json.Valid(state) || !json.Valid(payload) || len(state) > MaxPayloadBytes || len(payload) > MaxPayloadBytes {
		return 0, errors.New("distributed store: valid fenced admission release is required")
	}
	statePath := projectionKey(owner.Partition, runID)
	eventPath := eventKey(owner.Partition, eventID)
	globalPath := globalSlotKey(owner.Partition, globalSlot)
	tenantPath := tenantSlotKey(owner.Partition, tenant, tenantSlot)
	encodedEvent, err := json.Marshal(event{Partition: owner.Partition, ID: eventID, Kind: kind, Fence: owner.Token, Incarnation: owner.Incarnation, Payload: payload})
	if err != nil {
		return 0, err
	}
	response, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", owner.Incarnation),
			clientv3.Compare(clientv3.CreateRevision(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.Token),
			clientv3.Compare(clientv3.Value(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.ID),
			clientv3.Compare(clientv3.ModRevision(statePath), "=", expectedRevision),
			clientv3.Compare(clientv3.Value(globalPath), "=", runID),
			clientv3.Compare(clientv3.Value(tenantPath), "=", runID),
			clientv3.Compare(clientv3.Version(eventPath), "=", 0),
		).
		Then(clientv3.OpPut(statePath, string(state)), clientv3.OpPut(eventPath, string(encodedEvent)), clientv3.OpDelete(globalPath), clientv3.OpDelete(tenantPath)).Commit()
	if err != nil {
		return 0, fmt.Errorf("distributed store: release admission (outcome must be reconciled by event ID): %w", err)
	}
	if response.Succeeded {
		return response.Header.Revision, nil
	}
	return 0, s.classifyRejectedCommit(ctx, owner, eventPath, encodedEvent)
}

// CommitFencedState atomically appends a transition and replaces its compact
// state projection. expectedRevision is zero for create, otherwise the exact
// etcd ModRevision returned by ReadState. This is the run/queue transaction
// primitive: ownership, incarnation, prior state and stable transition ID are
// all compared in the same transaction as both writes.
func (s *Store) CommitFencedState(ctx context.Context, owner Owner, stateID string, expectedRevision int64, eventID, kind string, state, payload []byte) (int64, error) {
	if !owner.valid() || !validName(stateID) || expectedRevision < 0 || !validName(eventID) || !validName(kind) || len(state) > MaxPayloadBytes || len(payload) > MaxPayloadBytes || !json.Valid(state) || !json.Valid(payload) {
		return 0, errors.New("distributed store: valid owner, state/transition identity, JSON values and payload bounds are required")
	}
	stateKey := projectionKey(owner.Partition, stateID)
	transitionKey := eventKey(owner.Partition, eventID)
	encodedEvent, err := json.Marshal(event{Partition: owner.Partition, ID: eventID, Kind: kind, Fence: owner.Token, Incarnation: owner.Incarnation, Payload: payload})
	if err != nil {
		return 0, fmt.Errorf("distributed store: encode state transition: %w", err)
	}
	stateCompare := clientv3.Compare(clientv3.Version(stateKey), "=", 0)
	if expectedRevision > 0 {
		stateCompare = clientv3.Compare(clientv3.ModRevision(stateKey), "=", expectedRevision)
	}
	response, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", owner.Incarnation),
			clientv3.Compare(clientv3.CreateRevision(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.Token),
			clientv3.Compare(clientv3.Value(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.ID),
			stateCompare,
			clientv3.Compare(clientv3.Version(transitionKey), "=", 0),
		).
		Then(clientv3.OpPut(stateKey, string(state)), clientv3.OpPut(transitionKey, string(encodedEvent))).Commit()
	if err != nil {
		return 0, fmt.Errorf("distributed store: fenced state commit (outcome must be reconciled by transition ID): %w", err)
	}
	if response.Succeeded {
		return response.Header.Revision, nil
	}
	return 0, s.classifyRejectedCommit(ctx, owner, transitionKey, encodedEvent)
}

// CommitFencedStates atomically commits one event and several state
// projections. It is used when a timer or signal must both resolve a wait and
// make its suspended run runnable; neither projection may become visible
// without the other under the current partition fence.
func (s *Store) CommitFencedStates(ctx context.Context, owner Owner, mutations []StateMutation, eventID, kind string, payload []byte) (int64, error) {
	return s.commitFencedStates(ctx, owner, mutations, nil, eventID, kind, payload)
}

// CommitFencedWaitStates atomically updates wait/run projections and their
// bounded due-timer index entry. A nil timer mutation is used for an
// indefinite signal wait; Delete removes a previously scheduled timer.
func (s *Store) CommitFencedWaitStates(ctx context.Context, owner Owner, mutations []StateMutation, timer *TimerIndexMutation, eventID, kind string, payload []byte) (int64, error) {
	return s.commitFencedStates(ctx, owner, mutations, timer, eventID, kind, payload)
}

func (s *Store) commitFencedStates(ctx context.Context, owner Owner, mutations []StateMutation, timer *TimerIndexMutation, eventID, kind string, payload []byte) (int64, error) {
	if !owner.valid() || len(mutations) < 1 || len(mutations) > 8 || !validName(eventID) || !validName(kind) || len(payload) > MaxPayloadBytes || !json.Valid(payload) {
		return 0, errors.New("distributed store: bounded fenced state mutations, event identity and JSON payload are required")
	}
	conditions := []clientv3.Cmp{
		clientv3.Compare(clientv3.Value(incarnationKey()), "=", owner.Incarnation),
		clientv3.Compare(clientv3.CreateRevision(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.Token),
		clientv3.Compare(clientv3.Value(ownerKey(owner.Incarnation, owner.Partition)), "=", owner.ID),
	}
	operations := make([]clientv3.Op, 0, len(mutations)+1)
	seen := make(map[string]bool, len(mutations))
	for _, mutation := range mutations {
		if !validName(mutation.StateID) || mutation.ExpectedRevision < 0 || len(mutation.State) > MaxPayloadBytes || !json.Valid(mutation.State) || seen[mutation.StateID] {
			return 0, errors.New("distributed store: each fenced state mutation requires a unique identity, expected revision and bounded JSON")
		}
		seen[mutation.StateID] = true
		statePath := projectionKey(owner.Partition, mutation.StateID)
		if mutation.ExpectedRevision == 0 {
			conditions = append(conditions, clientv3.Compare(clientv3.Version(statePath), "=", 0))
		} else {
			conditions = append(conditions, clientv3.Compare(clientv3.ModRevision(statePath), "=", mutation.ExpectedRevision))
		}
		operations = append(operations, clientv3.OpPut(statePath, string(mutation.State)))
	}
	if timer != nil {
		if !validName(timer.StateID) || timer.DueAt.IsZero() {
			return 0, errors.New("distributed store: timer index mutation requires a state identity and due time")
		}
		indexKey := timerIndexKey(owner.Partition, timer.DueAt, timer.StateID)
		if timer.Delete {
			operations = append(operations, clientv3.OpDelete(indexKey))
		} else {
			conditions = append(conditions, clientv3.Compare(clientv3.Version(indexKey), "=", 0))
			operations = append(operations, clientv3.OpPut(indexKey, timer.StateID))
		}
	}
	eventPath := eventKey(owner.Partition, eventID)
	encodedEvent, err := json.Marshal(event{Partition: owner.Partition, ID: eventID, Kind: kind, Fence: owner.Token, Incarnation: owner.Incarnation, Payload: payload})
	if err != nil {
		return 0, fmt.Errorf("distributed store: encode state transition: %w", err)
	}
	conditions = append(conditions, clientv3.Compare(clientv3.Version(eventPath), "=", 0))
	operations = append(operations, clientv3.OpPut(eventPath, string(encodedEvent)))
	response, err := s.client.Txn(ctx).If(conditions...).Then(operations...).Commit()
	if err != nil {
		return 0, fmt.Errorf("distributed store: fenced multi-state commit (outcome must be reconciled by transition ID): %w", err)
	}
	if response.Succeeded {
		return response.Header.Revision, nil
	}
	return 0, s.classifyRejectedCommit(ctx, owner, eventPath, encodedEvent)
}

// ListActiveRunIDs reads the admission-slot index, whose cardinality is
// bounded by the configured partition capacity. Terminal transitions remove
// their slot in the same transaction as publishing terminal state.
func (s *Store) ListActiveRunIDs(ctx context.Context, partition string, limit int) ([]string, error) {
	if !validName(partition) || limit < 1 || limit > 4096 {
		return nil, errors.New("distributed store: active-run query requires a valid partition and bounded limit")
	}
	response, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(clientv3.OpGet(globalSlotsPrefix(partition), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(int64(limit+1)))).Commit()
	if err != nil {
		return nil, fmt.Errorf("distributed store: list bounded active runs: %w", err)
	}
	if !response.Succeeded {
		return nil, ErrIncarnation
	}
	if len(response.Responses) == 0 {
		return []string{}, nil
	}
	entries := response.Responses[0].GetResponseRange().Kvs
	if len(entries) > limit {
		return nil, ErrAdmissionFull
	}
	runs := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !validName(string(entry.Value)) {
			return nil, errors.New("distributed store: active-run slot contains an invalid run ID")
		}
		runs = append(runs, string(entry.Value))
	}
	return runs, nil
}

// ListDueTimers reads only the ordered timer-index prefix through now and
// returns at most limit entries; later due timers remain for the next poll.
func (s *Store) ListDueTimers(ctx context.Context, partition string, now time.Time, limit int) ([]DueTimer, error) {
	if !validName(partition) || now.IsZero() || limit < 1 || limit > 4096 {
		return nil, errors.New("distributed store: due-timer query requires a valid partition, time and bounded limit")
	}
	start := timerIndexPrefix(partition)
	end := start + fmt.Sprintf("%020d/\xff", now.UTC().UnixNano())
	response, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(clientv3.OpGet(start, clientv3.WithRange(end), clientv3.WithSort(clientv3.SortByKey, clientv3.SortAscend), clientv3.WithLimit(int64(limit)))).Commit()
	if err != nil {
		return nil, fmt.Errorf("distributed store: list bounded due timers: %w", err)
	}
	if !response.Succeeded {
		return nil, ErrIncarnation
	}
	if len(response.Responses) == 0 {
		return []DueTimer{}, nil
	}
	result := make([]DueTimer, 0, len(response.Responses[0].GetResponseRange().Kvs))
	for _, entry := range response.Responses[0].GetResponseRange().Kvs {
		var due int64
		if _, err := fmt.Sscanf(strings.TrimPrefix(string(entry.Key), start), "%d/", &due); err != nil || !validName(string(entry.Value)) {
			return nil, errors.New("distributed store: malformed timer index entry")
		}
		result = append(result, DueTimer{StateID: string(entry.Value), DueAt: time.Unix(0, due).UTC()})
	}
	return result, nil
}

func (s *Store) classifyRejectedCommit(ctx context.Context, owner Owner, transitionKey string, intended []byte) error {
	current, err := s.client.Get(ctx, ownerKey(owner.Incarnation, owner.Partition))
	if err != nil {
		return fmt.Errorf("distributed store: inspect rejected state commit: %w", err)
	}
	incarnation, err := s.client.Get(ctx, incarnationKey())
	if err != nil {
		return fmt.Errorf("distributed store: inspect state commit incarnation: %w", err)
	}
	if len(incarnation.Kvs) == 0 || string(incarnation.Kvs[0].Value) != owner.Incarnation || len(current.Kvs) == 0 || current.Kvs[0].CreateRevision != owner.Token || string(current.Kvs[0].Value) != owner.ID {
		return ErrOwnershipLost
	}
	committed, err := s.client.Get(ctx, transitionKey)
	if err != nil {
		return fmt.Errorf("distributed store: reconcile state transition: %w", err)
	}
	if len(committed.Kvs) > 0 {
		if string(committed.Kvs[0].Value) == string(intended) {
			return ErrAlreadyWritten
		}
		return errors.New("distributed store: transition ID conflicts with committed payload")
	}
	return ErrStateConflict
}

// ReadState reads a partition's compact projection with a linearizable read
// guarded by the store's current cluster incarnation.
func (s *Store) ReadState(ctx context.Context, partition, stateID string) ([]byte, int64, error) {
	if !validName(partition) || !validName(stateID) {
		return nil, 0, errors.New("distributed store: valid partition and state ID are required")
	}
	transaction, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(clientv3.OpGet(projectionKey(partition, stateID))).Commit()
	if err != nil {
		return nil, 0, fmt.Errorf("distributed store: linearizable state read: %w", err)
	}
	if !transaction.Succeeded {
		return nil, 0, ErrIncarnation
	}
	if len(transaction.Responses) == 0 || len(transaction.Responses[0].GetResponseRange().Kvs) == 0 {
		return nil, 0, nil
	}
	entry := transaction.Responses[0].GetResponseRange().Kvs[0]
	return append([]byte(nil), entry.Value...), entry.ModRevision, nil
}

// EnsureSetting establishes immutable, incarnation-scoped routing or capacity
// configuration. Replicas with a different partition map or admission limit
// fail closed rather than silently creating incompatible queues.
func (s *Store) EnsureSetting(ctx context.Context, name string, value []byte) error {
	if !validName(name) || len(value) == 0 || len(value) > MaxPayloadBytes || !json.Valid(value) {
		return errors.New("distributed store: valid setting name and JSON value are required")
	}
	key := "/blok/v1/incarnations/" + url.PathEscape(s.incarnation) + "/settings/" + url.PathEscape(name)
	response, err := s.client.Txn(ctx).
		If(
			clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation),
			clientv3.Compare(clientv3.Version(key), "=", 0),
		).
		Then(clientv3.OpPut(key, string(value))).Commit()
	if err != nil {
		return fmt.Errorf("distributed store: establish immutable setting: %w", err)
	}
	if response.Succeeded {
		return nil
	}
	current, err := s.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("distributed store: read immutable setting: %w", err)
	}
	if len(current.Kvs) == 0 || string(current.Kvs[0].Value) != string(value) {
		return ErrConfigConflict
	}
	return nil
}

// ListEvents returns immutable events in commit-revision order for recovery.
func (s *Store) ListEvents(ctx context.Context, partition string) ([]CommittedEvent, error) {
	if !validName(partition) {
		return nil, errors.New("distributed store: valid partition is required")
	}
	transaction, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(clientv3.OpGet(eventPrefix(partition), clientv3.WithPrefix(), clientv3.WithSort(clientv3.SortByModRevision, clientv3.SortAscend))).Commit()
	if err != nil {
		return nil, fmt.Errorf("distributed store: list partition events: %w", err)
	}
	if !transaction.Succeeded {
		return nil, ErrIncarnation
	}
	if len(transaction.Responses) == 0 {
		return []CommittedEvent{}, nil
	}
	entries := transaction.Responses[0].GetResponseRange().Kvs
	result := make([]CommittedEvent, 0, len(entries))
	for _, entry := range entries {
		var record event
		if err := json.Unmarshal(entry.Value, &record); err != nil {
			return nil, fmt.Errorf("distributed store: decode event at revision %d: %w", entry.ModRevision, err)
		}
		result = append(result, CommittedEvent{Partition: record.Partition, ID: record.ID, Kind: record.Kind, Fence: record.Fence, Incarnation: record.Incarnation, Payload: append(json.RawMessage(nil), record.Payload...), Revision: entry.ModRevision})
	}
	return result, nil
}

// Read returns a committed event using etcd's default linearizable read.
func (s *Store) Read(ctx context.Context, partition, id string) ([]byte, error) {
	if !validName(partition) || !validName(id) {
		return nil, errors.New("distributed store: valid partition and event ID are required")
	}
	transaction, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Value(incarnationKey()), "=", s.incarnation)).
		Then(clientv3.OpGet(eventKey(partition, id))).Commit()
	if err != nil {
		return nil, fmt.Errorf("distributed store: linearizable read: %w", err)
	}
	if !transaction.Succeeded {
		return nil, ErrIncarnation
	}
	if len(transaction.Responses) == 0 || len(transaction.Responses[0].GetResponseRange().Kvs) == 0 {
		return nil, nil
	}
	return append([]byte(nil), transaction.Responses[0].GetResponseRange().Kvs[0].Value...), nil
}

func (s *Store) cleanupLease(id clientv3.LeaseID) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = s.client.Revoke(ctx, id)
}

func (o Owner) valid() bool {
	return validName(o.Partition) && validName(o.ID) && validName(o.Incarnation) && o.Token > 0 && o.LeaseID != 0
}

func validName(value string) bool {
	return value != "" && len(value) <= 180 && strings.TrimSpace(value) == value && !strings.ContainsRune(value, '\x00')
}

func incarnationKey() string { return "/blok/v1/cluster-incarnation" }

func ownerKey(incarnation, partition string) string {
	return "/blok/v1/incarnations/" + url.PathEscape(incarnation) + "/partitions/" + url.PathEscape(partition) + "/owner"
}

func eventKey(partition, id string) string {
	return eventPrefix(partition) + url.PathEscape(id)
}

func eventPrefix(partition string) string {
	return "/blok/v1/partitions/" + url.PathEscape(partition) + "/events/"
}

func projectionKey(partition, id string) string {
	return "/blok/v1/partitions/" + url.PathEscape(partition) + "/state/" + url.PathEscape(id)
}

func globalSlotsPrefix(partition string) string {
	return "/blok/v1/partitions/" + url.PathEscape(partition) + "/admission-slots/global/"
}

func timerIndexPrefix(partition string) string {
	return "/blok/v1/partitions/" + url.PathEscape(partition) + "/timers/"
}

func timerIndexKey(partition string, dueAt time.Time, stateID string) string {
	return timerIndexPrefix(partition) + fmt.Sprintf("%020d/", dueAt.UTC().UnixNano()) + url.PathEscape(stateID)
}

func globalSlotKey(partition, slot string) string {
	return globalSlotsPrefix(partition) + url.PathEscape(slot)
}

func tenantSlotsPrefix(partition, tenant string) string {
	return "/blok/v1/partitions/" + url.PathEscape(partition) + "/admission-slots/tenants/" + url.PathEscape(tenant) + "/"
}

func tenantSlotKey(partition, tenant, slot string) string {
	return tenantSlotsPrefix(partition, tenant) + url.PathEscape(slot)
}
