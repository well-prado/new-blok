package engine

import (
	"encoding/json"
	"runtime"
	"strings"
	"testing"
)

func TestMarshalObservationBoundsLargePayloadBeforeSerialization(t *testing.T) {
	large := make([]byte, 32<<20)
	for index := range large {
		large[index] = 'x'
	}
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	captured := marshalObservation(large)
	runtime.ReadMemStats(&after)
	if string(captured) != `{"$truncated":true}` {
		t.Fatalf("large binary payload was not replaced by bounded marker: len=%d payload=%q", len(captured), captured)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 256<<10 {
		t.Fatalf("capture allocated %d bytes for oversized payload; expected bounded pre-serialization rejection", allocated)
	} else {
		t.Logf("32 MiB observer payload allocated %d bytes during bounded capture", allocated)
	}
}

type marshalerProbe struct {
	Value string `json:"value"`
	calls int
}

func (p *marshalerProbe) MarshalJSON() ([]byte, error) {
	p.calls++
	return json.Marshal(strings.Repeat("x", 1<<20))
}

func TestMarshalObservationDoesNotInvokeCustomMarshaler(t *testing.T) {
	probe := &marshalerProbe{Value: "safe"}
	captured := marshalObservation(probe)
	if probe.calls != 0 {
		t.Fatalf("observation invoked caller-controlled MarshalJSON %d times", probe.calls)
	}
	if len(captured) > 128 || !json.Valid(captured) {
		t.Fatalf("custom marshal output escaped structural capture bound: len=%d payload=%q", len(captured), captured)
	}
}
