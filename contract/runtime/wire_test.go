package runtime

import (
	"bytes"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"github.com/well-prado/new-blok/contract/schema"
	"google.golang.org/protobuf/proto"
	"testing"
	"time"
)

func TestWirePreservesMissingNullAndExactInt64(t *testing.T) {
	s, err := schema.Parse([]byte(`{"type":"object","properties":{"n":{"type":"integer","wire":"int64-string"},"optional":{"type":"string","nullable":true}},"required":["n"]}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range []string{`{"n":"9223372036854775807"}`, `{"n":"-9223372036854775808","optional":null}`} {
		normalized, err := s.Normalize([]byte(payload))
		if err != nil {
			t.Fatal(err)
		}
		c := Call{CallID: "c", AttemptID: "a", Node: "math", NodeVersion: "1.0.0", Generation: 1, Deadline: time.Now().Add(time.Minute), Input: normalized}
		raw, err := proto.Marshal(CallWire(c))
		if err != nil {
			t.Fatal(err)
		}
		var decoded wire.Call
		if err := proto.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		got := CallFromWire(&decoded)
		if !bytes.Equal(got.Input, normalized) {
			t.Fatalf("wire changed presence/int64: %s", got.Input)
		}
		if err := ValidatePayload(got.Input, s); err != nil {
			t.Fatal(err)
		}
	}
	if err := ValidatePayload([]byte(`{"n":"9223372036854775808"}`), s); err == nil {
		t.Fatal("overflow accepted")
	}
	if err := ValidatePayload([]byte(`{"n":"1"} {}`), s); err == nil {
		t.Fatal("trailing input accepted")
	}
}

func TestDirectionContractHasNoOrchestrationRPC(t *testing.T) {
	d := wire.File_runtime_proto.Services().ByName("Worker")
	if d.Methods().Len() != 2 || !d.Methods().ByName("Connect").IsStreamingClient() || !d.Methods().ByName("Connect").IsStreamingServer() {
		t.Fatal("unexpected service direction")
	}
}
