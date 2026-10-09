package flow_test

import (
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/flow"
)

// TestWaitLowersToADurableWait (#333 slice 5): flow.Wait lowers to a wait
// instruction with its signal name and timeout, in a control program (its
// result is the signal as a JSON value, read later as "$step.<id>"); a
// wait with no name or a negative timeout is refused when it is recorded.
func TestWaitLowersToADurableWait(t *testing.T) {
	n := newConformanceNodes(t)
	lowered, err := flow.MustDefine(conformanceSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		reserved := flow.Call(b, "reserve", n.reserve, in)
		flow.Wait(b, "approval", "approval", 90*time.Second)
		return reserved
	}).Lower()
	if err != nil {
		t.Fatal(err)
	}
	wait := lowered.Instructions[1]
	if lowered.Format != contract.ControlFormat || wait.Kind != "wait" || wait.ID != "approval" || wait.Wait == nil || *wait.Wait != (contract.WaitInstruction{Name: "approval", TimeoutMillis: 90000}) {
		t.Fatalf("lowered %+v (wait %+v)", lowered, wait.Wait)
	}
	for name, build := range map[string]func(*flow.Builder, flow.Ref[object]) flow.Ref[object]{
		"no name":          func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] { flow.Wait(b, "w", "", 0); return in },
		"negative timeout": func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] { flow.Wait(b, "w", "x", -time.Second); return in },
	} {
		if _, err := flow.Define(conformanceSpec, build); err == nil || !strings.Contains(err.Error(), "flow: wait") {
			t.Fatalf("%s: Define err=%v", name, err)
		}
	}
}
