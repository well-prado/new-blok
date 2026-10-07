package journal

import (
	"errors"
	"reflect"
	"testing"

	"github.com/well-prado/new-blok/contract/signal"
)

// TestTargetedSignalReachesOnlyItsWait: a signal addressed to one wait, by
// wait ID or by step and iteration, goes to that wait even when an older
// wait of its name is open, is late once that wait has closed, and is
// refused with nothing stored when the run has no such wait (yet, or of
// another name, or when the ID and paths name different waits). Its retry
// once the wait exists is delivered.
func TestTargetedSignalReachesOnlyItsWait(t *testing.T) {
	r := newSignalRig(t, ticking(fixtureBase))
	r.wait("approve[0]", "approve", "0")
	r.wait("approve[1]", "approve", "1")
	send := func(signalID, name string, target WaitTarget) (SignalResult, error) {
		return r.journal.SignalWait(r.ctx, signal.Envelope{RunID: r.run, SignalID: signalID, Name: name, Principal: "operator", Payload: []byte(`{}`)}, target, true)
	}
	if result, err := send("to-1", "approval", WaitTarget{WaitID: "approve[1]"}); err != nil || result != delivered {
		t.Fatalf("to approve[1] by ID=%+v err=%v", result, err)
	}
	if result, err := send("to-0", "approval", WaitTarget{InvocationPath: "approve", IterationPath: "0"}); err != nil || result != delivered {
		t.Fatalf("to approve[0] by step and iteration=%+v err=%v", result, err)
	}
	if result, err := send("to-1-again", "approval", WaitTarget{WaitID: "approve[1]", InvocationPath: "approve", IterationPath: "1"}); err != nil || result != late {
		t.Fatalf("to the closed approve[1]=%+v err=%v; want late", result, err)
	}
	for _, refused := range []struct {
		label, name string
		target      WaitTarget
	}{
		{"a wait not yet scheduled", "approval", WaitTarget{WaitID: "approve[2]"}},
		{"a wait of another name", "review", WaitTarget{WaitID: "approve[0]"}},
		{"an ID and paths of different waits", "approval", WaitTarget{WaitID: "approve[0]", InvocationPath: "approve", IterationPath: "1"}},
	} {
		if result, err := send("to-2", refused.name, refused.target); !errors.Is(err, ErrNotFound) {
			t.Fatalf("%s=%+v err=%v; want ErrNotFound", refused.label, result, err)
		}
	}
	if _, err := send("half", "approval", WaitTarget{InvocationPath: "approve"}); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatalf("a target with a step and no iteration: err=%v; want refused as invalid", err)
	}
	r.wait("approve[2]", "approve", "2")
	r.wait("approve[3]", "approve", "3")
	if result, err := send("to-2", "approval", WaitTarget{WaitID: "approve[2]"}); err != nil || result != delivered {
		t.Fatalf("retry once approve[2] exists=%+v err=%v", result, err)
	}
	want := []string{"approve[0]|fired|to-0", "approve[1]|fired|to-1", "approve[2]|fired|to-2", "approve[3]|waiting|", "to-1|stored", "to-0|stored", "to-1-again|late", "to-2|stored"}
	if got := r.rows(); !reflect.DeepEqual(got, want) {
		t.Fatalf("rows:\n got %q\nwant %q", got, want)
	}
}
