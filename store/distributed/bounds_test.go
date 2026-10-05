package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestOversizeRecordsAreRejectedAsTooLarge pins that every write primitive
// reports an over-bound record as ErrRecordTooLarge before contacting etcd, so
// callers can classify it as a definite request error instead of an outage
// (#254). The store has no client: reaching the transaction would panic.
func TestOversizeRecordsAreRejectedAsTooLarge(t *testing.T) {
	store := &Store{incarnation: "bounds-incarnation", limits: RequestLimits{ServerMaxRequestBytes: DefaultServerMaxRequestBytes, ClientMaxCallSendBytes: DefaultClientMaxCallSendBytes}}
	owner := Owner{Partition: "p-0000", ID: "owner", Incarnation: "bounds-incarnation", Token: 1, LeaseID: 1}
	small := []byte(`{}`)
	// A JSON string exactly one byte over the bound.
	big := []byte(`"` + strings.Repeat("a", MaxPayloadBytes-1) + `"`)
	ctx := context.Background()
	checks := map[string]func() error{
		"Commit": func() error { return store.Commit(ctx, owner, "event", "kind", big) },
		"CommitAdmission state": func() error {
			return store.CommitAdmission(ctx, "p-0000", "tenant", "0", "0", "run", big, small)
		},
		"CommitAdmission payload": func() error {
			return store.CommitAdmission(ctx, "p-0000", "tenant", "0", "0", "run", small, big)
		},
		"ReleaseAdmissionSlots": func() error {
			_, err := store.ReleaseAdmissionSlots(ctx, owner, "run", "tenant", "0", "0", 1, "event", "kind", big, small)
			return err
		},
		"CommitFencedState": func() error {
			_, err := store.CommitFencedState(ctx, owner, "state", 0, "event", "kind", small, big)
			return err
		},
		"CommitFencedWaitStates state": func() error {
			_, err := store.CommitFencedWaitStates(ctx, owner, []StateMutation{{StateID: "a", State: small}, {StateID: "b", State: big}}, &TimerIndexMutation{StateID: "b", DueAt: time.Now()}, "event", "kind", small)
			return err
		},
		"CommitFencedStates payload": func() error {
			_, err := store.CommitFencedStates(ctx, owner, []StateMutation{{StateID: "a", State: small}}, "event", "kind", big)
			return err
		},
		"EnsureSetting": func() error { return store.EnsureSetting(ctx, "setting", big) },
	}
	for name, check := range checks {
		if err := check(); !errors.Is(err, ErrRecordTooLarge) {
			t.Errorf("%s with a %d-byte record: err=%v, want ErrRecordTooLarge", name, len(big), err)
		}
	}
	if err := CheckRecord(big[:MaxPayloadBytes]); err != nil {
		t.Fatalf("a record of exactly MaxPayloadBytes is within the bound: %v", err)
	}
	// The concrete error names the over-bound record, so a runtime can tell
	// a caller's record from one it grew itself.
	_, err := store.CommitFencedWaitStates(ctx, owner, []StateMutation{{StateID: "wait", State: small}, {StateID: "run", State: big}}, nil, "event", "kind", small)
	var tooLarge *RecordTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Record != "state" || tooLarge.StateID != "run" || tooLarge.Size != len(big) || tooLarge.Limit != MaxPayloadBytes {
		t.Fatalf("over-bound second mutation: err=%v detail=%+v, want state \"run\" %d > %d", err, tooLarge, len(big), MaxPayloadBytes)
	}
}

// TestTransactionOverTransportBoundIsTooLarge pins the request-level bound:
// three records each exactly at MaxPayloadBytes fit their own bound but not
// etcd's default 1.5 MiB request, and the store says so before sending.
func TestTransactionOverTransportBoundIsTooLarge(t *testing.T) {
	store := &Store{incarnation: "bounds-incarnation", limits: RequestLimits{ServerMaxRequestBytes: DefaultServerMaxRequestBytes, ClientMaxCallSendBytes: DefaultClientMaxCallSendBytes}}
	owner := Owner{Partition: "p-0000", ID: "owner", Incarnation: "bounds-incarnation", Token: 1, LeaseID: 1}
	exact := []byte(`"` + strings.Repeat("a", MaxPayloadBytes-2) + `"`)
	_, err := store.CommitFencedStates(context.Background(), owner, []StateMutation{{StateID: "a", State: exact}, {StateID: "b", State: exact}}, "event", "kind", exact)
	var tooLarge *RecordTooLargeError
	if !errors.As(err, &tooLarge) || tooLarge.Record != "request" || tooLarge.Limit != DefaultServerMaxRequestBytes || tooLarge.Size <= 3*MaxPayloadBytes || tooLarge.Transport != nil {
		t.Fatalf("three records at the bound: err=%v detail=%+v, want a pre-send request rejection over %d bytes", err, tooLarge, DefaultServerMaxRequestBytes)
	}
	if limits := (RequestLimits{ServerMaxRequestBytes: 8 << 20, ClientMaxCallSendBytes: DefaultClientMaxCallSendBytes}); limits.effective() != DefaultClientMaxCallSendBytes {
		t.Fatalf("effective bound=%d, want the smaller client send limit", limits.effective())
	}
}

type unusedClient struct{ Client }

func TestNewRejectsTransportBoundsBelowTwoRecords(t *testing.T) {
	_, err := New(context.Background(), unusedClient{}, "bounds-incarnation", WithRequestLimits(RequestLimits{ServerMaxRequestBytes: MinRequestBytes - 1, ClientMaxCallSendBytes: DefaultClientMaxCallSendBytes}))
	if err == nil || !strings.Contains(err.Error(), "request limits") {
		t.Fatalf("New with --max-request-bytes below MinRequestBytes: err=%v, want a configuration rejection before any network call", err)
	}
}

func TestTransportTooLargeRecognizesOnlySizeRejections(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"etcd request too large":      {rpctypes.ErrRequestTooLarge, true},
		"etcd grpc request too large": {rpctypes.ErrGRPCRequestTooLarge, true},
		"client send limit":           {status.Error(codes.ResourceExhausted, "grpc: trying to send message larger than max (2097200 vs. 2097152)"), true},
		"server receive limit":        {fmt.Errorf("wrapped: %w", status.Error(codes.ResourceExhausted, "grpc: received message larger than max (2200000 vs. 2097152)")), true},
		"nospace alarm":               {rpctypes.ErrNoSpace, false},
		"grpc nospace alarm":          {rpctypes.ErrGRPCNoSpace, false},
		"unavailable":                 {status.Error(codes.Unavailable, "connection refused"), false},
		"deadline":                    {context.DeadlineExceeded, false},
	}
	for name, test := range cases {
		if got := transportTooLarge(test.err); got != test.want {
			t.Errorf("%s: transportTooLarge(%v)=%v, want %v", name, test.err, got, test.want)
		}
	}
}
