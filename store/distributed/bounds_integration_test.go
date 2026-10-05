package distributed

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestTransportBoundAgainstRealEtcd checks the request-level bound (#254)
// against a real three-voter etcd running its default --max-request-bytes
// and a client with its default send limit:
//   - transactions of three records at MaxPayloadBytes are refused before
//     they are sent, as ErrRecordTooLarge;
//   - the largest transaction the default bound lets through is accepted
//     by etcd, so the store never sends what etcd would refuse; and
//   - when the store is told limits larger than the real ones, etcd's own
//     "request is too large" and gRPC's send-limit rejection are still
//     reported as ErrRecordTooLarge, never as an ordinary failure.
func TestTransportBoundAgainstRealEtcd(t *testing.T) {
	store, client := integrationStore(t, "BLOK_DISTRIBUTED_ENDPOINTS")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	partition := fmt.Sprintf("bounds-%d", time.Now().UnixNano())
	owner, err := store.Acquire(ctx, partition, "bounds-owner", 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Release(context.Background(), owner) })
	record := func(n int) []byte { return []byte(`"` + strings.Repeat("a", n-2) + `"`) }
	exact := record(MaxPayloadBytes)
	mutations := func(name string, count int, state []byte) []StateMutation {
		result := make([]StateMutation, count)
		for index := range result {
			result[index] = StateMutation{StateID: fmt.Sprintf("%s-%d", name, index), State: state}
		}
		return result
	}
	requestRejection := func(t *testing.T, label string, err error, wantTransport bool) {
		t.Helper()
		var tooLarge *RecordTooLargeError
		if !errors.Is(err, ErrRecordTooLarge) || !errors.As(err, &tooLarge) || tooLarge.Record != "request" || (tooLarge.Transport != nil) != wantTransport {
			t.Fatalf("%s: err=%v detail=%+v, want a request-level ErrRecordTooLarge (transport=%v)", label, err, tooLarge, wantTransport)
		}
	}

	for _, count := range []int{2, 3, 8} {
		_, err := store.CommitFencedStates(ctx, owner, mutations(fmt.Sprintf("default-%d", count), count, exact), fmt.Sprintf("default-%d", count), "bounds", exact)
		requestRejection(t, fmt.Sprintf("default limits, %d states + payload at the bound", count), err, false)
	}

	// The largest two-state transaction the default bound admits must
	// commit on real etcd.
	committed := false
	for size := MaxPayloadBytes; size > MaxPayloadBytes-8192; size -= 16 {
		id := fmt.Sprintf("largest-%d", size)
		_, err := store.CommitFencedStates(ctx, owner, mutations(id, 2, record(size)), id, "bounds", exact)
		if errors.Is(err, ErrRecordTooLarge) {
			requestRejection(t, fmt.Sprintf("states of %d", size), err, false)
			continue
		}
		if err != nil {
			t.Fatalf("largest transaction the default bound admits (states of %d bytes) was refused by etcd: %v", size, err)
		}
		t.Logf("largest admitted: two states of %d bytes + payload of %d", size, len(exact))
		committed = true
		break
	}
	if !committed {
		t.Fatal("no two-state transaction near the bound was admitted")
	}

	// Told limits above the real ones, the store sends; etcd and gRPC refuse
	// and the store still reports a definite size rejection.
	inflated, err := New(ctx, client, store.incarnation, WithRequestLimits(RequestLimits{ServerMaxRequestBytes: 64 << 20, ClientMaxCallSendBytes: 64 << 20}))
	if err != nil {
		t.Fatal(err)
	}
	_, err = inflated.CommitFencedStates(ctx, owner, mutations("server", 2, exact), "server", "bounds", exact)
	requestRejection(t, "~1.5 MiB over etcd --max-request-bytes", err, true)
	t.Logf("server-side rejection: %v", err)
	_, err = inflated.CommitFencedStates(ctx, owner, mutations("client", 4, exact), "client", "bounds", exact)
	requestRejection(t, "~2.5 MiB over the client send limit", err, true)
	t.Logf("client-side rejection: %v", err)
}
