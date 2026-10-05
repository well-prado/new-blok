package distributed

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestOversizeRecordsAreRejectedAsTooLarge pins that every write primitive
// reports an over-bound record as ErrRecordTooLarge before contacting etcd, so
// callers can classify it as a definite request error instead of an outage
// (#254). The store has no client: reaching the transaction would panic.
func TestOversizeRecordsAreRejectedAsTooLarge(t *testing.T) {
	store := &Store{incarnation: "bounds-incarnation"}
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
	if err := recordBounds(big[:MaxPayloadBytes]); err != nil {
		t.Fatalf("a record of exactly MaxPayloadBytes is within the bound: %v", err)
	}
}
