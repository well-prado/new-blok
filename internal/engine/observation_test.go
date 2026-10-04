package engine

import (
	"encoding/json"
	"encoding/json/jsontext"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestMarshalObservationPreservesJSONShapeOrExplicitlyTruncates(t *testing.T) {
	type embedded struct {
		ID string `json:"id"`
	}
	values := []any{
		struct {
			First  string `json:"x"`
			Second string `json:"x"`
		}{First: "a", Second: "b"},
		struct {
			First  string `json:"x,omitempty"`
			Second string `json:"x"`
		}{Second: "b"},
		observationPrivateAppender("synthetic-private-value"),
		observationPrivateJSONTo("synthetic-private-value"),
		map[observationPrivateText]int{"synthetic-key": 1},
		struct {
			Value int `json:"'a,b'"`
		}{Value: 1},
		[2]byte{1, 2},
		[]byte{1, 2},
		[]observationNamedByte{1, 2},
		observationPrivateText("synthetic-private-value"),
		struct {
			Nil   []string `json:"nil"`
			Empty []string `json:"empty,omitempty"`
		}{Empty: []string{}},
		struct{ embedded }{embedded: embedded{ID: "123"}},
		struct {
			Count int `json:"count,string"`
		}{Count: 42},
		struct {
			Count int `json:"count,omitzero"`
		}{},
	}
	for _, value := range values {
		captured := marshalObservation(value)
		if string(captured) == `{"$truncated":true}` {
			continue
		}
		want, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var actual, expected any
		if err := json.Unmarshal(captured, &actual); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(want, &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(actual, expected) {
			t.Fatalf("%T: observed %s, canonical %s", value, captured, want)
		}
	}
}

func TestMarshalObservationRejectsInvalidStandardTime(t *testing.T) {
	for _, value := range []time.Time{
		time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 1, 0, 0, 0, 0, time.FixedZone("invalid", 24*60*60)),
	} {
		if _, err := json.Marshal(value); err == nil {
			t.Fatal("fixture unexpectedly encodes as valid JSON time")
		}
		if captured := marshalObservation(value); string(captured) != `{"$truncated":true}` {
			t.Fatalf("invalid time silently represented: %s", captured)
		}
	}
}

type observationNamedByte byte
type observationPrivateText string

func (observationPrivateText) MarshalText() ([]byte, error) { return []byte("redacted"), nil }

type observationPrivateAppender string

func (observationPrivateAppender) AppendText(b []byte) ([]byte, error) {
	return append(b, "redacted"...), nil
}

type observationPrivateJSONTo string

func (observationPrivateJSONTo) MarshalJSONTo(encoder *jsontext.Encoder) error {
	return encoder.WriteToken(jsontext.String("redacted"))
}

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
