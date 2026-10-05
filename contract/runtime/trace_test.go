package runtime

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"google.golang.org/protobuf/proto"
)

const sampleTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

func tracedCall(traceparent, tracestate string) Call {
	return Call{CallID: "trace", AttemptID: "attempt-trace", Node: "fixture/echo", NodeVersion: "1.0.0", Generation: 1, Deadline: time.Now().Add(time.Minute), Input: []byte(`{}`), Traceparent: traceparent, Tracestate: tracestate}
}

func TestCallTraceContextIsCanonicalAndBounded(t *testing.T) {
	cases := []struct {
		name, traceparent, tracestate string
		ok                            bool
	}{
		{"absent", "", "", true},
		{"canonical", sampleTraceparent, "vendor=opaque", true},
		{"tracestate at the bound", sampleTraceparent, strings.Repeat("v", observe.MaxTracestateBytes), true},
		{"tracestate over the bound", sampleTraceparent, strings.Repeat("v", observe.MaxTracestateBytes+1), false},
		{"tracestate without traceparent", "", "vendor=opaque", false},
		{"non-canonical upper case", strings.ToUpper(sampleTraceparent), "", false},
		{"future version is not sent", "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x", "", false},
		{"zero trace id", "00-00000000000000000000000000000000-00f067aa0ba902b7-01", "", false},
		{"control character in tracestate", sampleTraceparent, "k=v\r", false},
	}
	for _, tc := range cases {
		err := tracedCall(tc.traceparent, tc.tracestate).Validate(DefaultLimits(), 1)
		if (err == nil) != tc.ok {
			t.Fatalf("%s: err=%v, want ok=%v", tc.name, err, tc.ok)
		}
		if !tc.ok && !errors.Is(err, observe.ErrInvalidTraceContext) {
			t.Fatalf("%s: err=%v does not name the trace context", tc.name, err)
		}
	}
}

func TestCallTraceContextRoundTripsTheWire(t *testing.T) {
	call := tracedCall(sampleTraceparent, "vendor=opaque")
	encoded, err := proto.Marshal(CallWire(call))
	if err != nil {
		t.Fatal(err)
	}
	var decoded wire.Call
	if err := proto.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	back := CallFromWire(&decoded)
	if back.Traceparent != call.Traceparent || back.Tracestate != call.Tracestate {
		t.Fatalf("round trip lost trace context: %+v", back)
	}
	// The trace fields count toward the complete frame bound.
	if EncodedCallBytes(call) <= EncodedCallBytes(tracedCall("", "")) {
		t.Fatal("trace context is not counted in the encoded call size")
	}
}
