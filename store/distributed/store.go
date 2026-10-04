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

// Renew extends a live lease. An expired lease cannot be revived; the caller
// must acquire again and receive a new fencing token.
func (s *Store) Renew(ctx context.Context, owner Owner) error {
	if !owner.valid() {
		return ErrOwnershipLost
	}
	response, err := s.client.KeepAliveOnce(ctx, owner.LeaseID)
	if err != nil {
		return fmt.Errorf("distributed store: renew lease: %w", err)
	}
	if response == nil || response.TTL <= 0 {
		return ErrOwnershipLost
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
	return "/blok/v1/partitions/" + url.PathEscape(partition) + "/events/" + url.PathEscape(id)
}
