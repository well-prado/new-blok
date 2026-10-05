package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Default etcd transport bounds. They are the published defaults of etcd
// v3.6 (--max-request-bytes) and of clientv3.Config.MaxCallSendMsgSize; a
// deployment that changes either must pass its values with WithRequestLimits.
const (
	DefaultServerMaxRequestBytes  = 3 << 19 // 1.5 MiB
	DefaultClientMaxCallSendBytes = 2 << 20 // 2 MiB
)

// MinRequestBytes is the smallest effective transport bound the store
// accepts: two records at MaxPayloadBytes plus their keys, compares and
// event envelopes. It guarantees that every two-record transaction (admission,
// claim, suspension, finish) fits whenever each record fits, so only a
// transaction that carries three or more records can exceed the transport.
const MinRequestBytes = 2*MaxPayloadBytes + 64<<10

// Upper bounds, in bytes, on the protobuf framing etcd adds around one
// request op or compare (field tags, length varints, int64 targets) and
// around the whole transaction (TxnRequest and the server's raft header).
const (
	opFramingBytes  = 16
	cmpFramingBytes = 32
	txnFramingBytes = 256
)

// RequestLimits are the etcd transport bounds a transaction must respect.
type RequestLimits struct {
	// ServerMaxRequestBytes is the etcd members' --max-request-bytes.
	ServerMaxRequestBytes int
	// ClientMaxCallSendBytes is the etcd client's MaxCallSendMsgSize.
	ClientMaxCallSendBytes int
}

func (l RequestLimits) effective() int {
	return min(l.ServerMaxRequestBytes, l.ClientMaxCallSendBytes)
}

// Option configures a Store at construction.
type Option func(*Store)

// WithRequestLimits declares the etcd server and client transport bounds the
// store's client actually runs with. Without it the etcd defaults apply.
func WithRequestLimits(limits RequestLimits) Option {
	return func(s *Store) { s.limits = limits }
}

// RecordTooLargeError reports which part of a transaction exceeded its bound.
// It is decided before any transaction is sent, or mapped from etcd's own
// size rejection, so nothing was written and retrying the same records can
// never succeed. It matches ErrRecordTooLarge with errors.Is.
type RecordTooLargeError struct {
	// Record is "state" (StateID names it), "payload" (an event payload or
	// setting value) or "request" (the whole transaction).
	Record  string
	StateID string
	Size    int
	Limit   int
	// Transport is etcd's or gRPC's size rejection when the store's own
	// estimate let the transaction through; nil otherwise.
	Transport error
}

func (e *RecordTooLargeError) Error() string {
	subject := e.Record
	if e.StateID != "" {
		subject += " " + e.StateID
	}
	if e.Transport != nil {
		return fmt.Sprintf("%v: %s rejected by the etcd transport (estimated %d of %d bytes): %v", ErrRecordTooLarge, subject, e.Size, e.Limit, e.Transport)
	}
	return fmt.Sprintf("%v: %s is %d > %d bytes", ErrRecordTooLarge, subject, e.Size, e.Limit)
}

func (e *RecordTooLargeError) Unwrap() error { return ErrRecordTooLarge }

// CheckRecord reports an encoded record over MaxPayloadBytes. Callers pass
// the bytes exactly as they will be persisted, so HTML escaping and envelope
// fields are counted; runtimes use it to check a record's worst-case future
// shape before admitting it.
func CheckRecord(record []byte) error {
	return checkPayload(record)
}

func checkPayload(payload []byte) error {
	if len(payload) > MaxPayloadBytes {
		return &RecordTooLargeError{Record: "payload", Size: len(payload), Limit: MaxPayloadBytes}
	}
	return nil
}

func checkState(stateID string, state []byte) error {
	if len(state) > MaxPayloadBytes {
		return &RecordTooLargeError{Record: "state", StateID: stateID, Size: len(state), Limit: MaxPayloadBytes}
	}
	return nil
}

// requestBytes is an upper bound on the encoded size of a transaction.
func requestBytes(conditions []clientv3.Cmp, operations []clientv3.Op) int {
	size := txnFramingBytes
	for index := range conditions {
		size += len(conditions[index].Key) + len(conditions[index].RangeEnd) + len(conditions[index].ValueBytes()) + cmpFramingBytes
	}
	for _, operation := range operations {
		size += len(operation.KeyBytes()) + len(operation.RangeBytes()) + len(operation.ValueBytes()) + opFramingBytes
	}
	return size
}

// commitTxn sends a record-carrying transaction only if it fits the etcd
// transport, and reports etcd's or gRPC's own size rejection the same way, so
// an over-bound transaction is never mistaken for an outage.
func (s *Store) commitTxn(ctx context.Context, conditions []clientv3.Cmp, operations []clientv3.Op) (*clientv3.TxnResponse, error) {
	size, limit := requestBytes(conditions, operations), s.limits.effective()
	if size > limit {
		return nil, &RecordTooLargeError{Record: "request", Size: size, Limit: limit}
	}
	response, err := s.client.Txn(ctx).If(conditions...).Then(operations...).Commit()
	if err != nil && transportTooLarge(err) {
		return nil, &RecordTooLargeError{Record: "request", Size: size, Limit: limit, Transport: err}
	}
	return response, err
}

// transportTooLarge recognizes etcd's --max-request-bytes rejection and gRPC's
// message-size limits. etcd's NOSPACE alarm is also ResourceExhausted, but it
// is an operational condition, not a property of the request, and stays an
// ordinary error.
func transportTooLarge(err error) bool {
	if errors.Is(err, rpctypes.ErrRequestTooLarge) || errors.Is(err, rpctypes.ErrGRPCRequestTooLarge) {
		return true
	}
	if errors.Is(err, rpctypes.ErrNoSpace) || errors.Is(err, rpctypes.ErrGRPCNoSpace) {
		return false
	}
	grpcStatus, ok := status.FromError(err)
	return ok && grpcStatus.Code() == codes.ResourceExhausted && strings.Contains(grpcStatus.Message(), "larger than max")
}
