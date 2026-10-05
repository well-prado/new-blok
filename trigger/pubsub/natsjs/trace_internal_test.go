package natsjs

import (
	"slices"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// headerMsg is a JetStream message that only has headers.
type headerMsg struct {
	jetstream.Msg
	header nats.Header
}

func (m headerMsg) Headers() nats.Header { return m.header }

// TestTraceHeadersReportsEveryValueAndSpelling: NATS header names are case
// sensitive, so a message can carry traceparent under two spellings; both
// are reported, which the consumer then ignores as a duplicate (#276).
func TestTraceHeadersReportsEveryValueAndSpelling(t *testing.T) {
	const value = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	cases := []struct {
		name                    string
		header                  nats.Header
		traceparent, tracestate []string
	}{
		{"none", nats.Header{"Nats-Msg-Id": {"1"}}, nil, nil},
		{"one of each", nats.Header{"traceparent": {value}, "tracestate": {"a=1"}}, []string{value}, []string{"a=1"}},
		{"two values", nats.Header{"traceparent": {value, value}}, []string{value, value}, nil},
		{"two spellings", nats.Header{"traceparent": {value}, "Traceparent": {value}, "TRACESTATE": {"a=1"}}, []string{value, value}, []string{"a=1"}},
	}
	for _, tc := range cases {
		traceparent, tracestate := (&message{msg: headerMsg{header: tc.header}}).TraceHeaders()
		if !slices.Equal(traceparent, tc.traceparent) || !slices.Equal(tracestate, tc.tracestate) {
			t.Fatalf("%s: traceparent %q tracestate %q", tc.name, traceparent, tracestate)
		}
	}
}
