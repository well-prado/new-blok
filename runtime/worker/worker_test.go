package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/well-prado/new-blok/contract/schema"
	"strings"
	"testing"
)

func TestEffectTransportFailurePreservesKnownCauseBeforeContextTimer(t *testing.T) {
	for _, cause := range []error{context.DeadlineExceeded, context.Canceled} {
		ctx := context.Background() // Its timer has not observed the deadline.
		failure := transportFailure(ctx, fmt.Errorf("synthetic-sensitive-detail: %w", cause))
		if ctx.Err() != nil || !errors.Is(failure, cause) || !failure.Uncertain || failure.Class != "uncertain" {
			t.Fatalf("lost known cause: %v", failure)
		}
		if strings.Contains(failure.Error(), "synthetic-sensitive-detail") {
			t.Fatal("transport detail leaked")
		}
	}
	failure := transportFailure(context.Background(), errors.New("synthetic-sensitive-detail"))
	if failure.Err != nil || errors.Is(failure, context.DeadlineExceeded) || strings.Contains(failure.Error(), "synthetic-sensitive-detail") {
		t.Fatal("unrelated transport failure was reclassified or leaked")
	}
}

func TestNativeJSONConvertsInt64InsideNestedUnion(t *testing.T) {
	s, err := schema.Parse([]byte(`{"anyOf":[{"type":"object","properties":{"amount":{"type":"integer","wire":"int64-string"}},"required":["amount"]},{"type":"object","properties":{"text":{"type":"string"}},"required":["text"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"-9223372036854775808", "9223372036854775807"} {
		wire, err := s.Normalize([]byte(`{"amount":"` + value + `"}`))
		if err != nil {
			t.Fatal(err)
		}
		raw, err := nativeJSON(s, wire)
		if err != nil {
			t.Fatal(err)
		}
		var out struct {
			Amount int64 `json:"amount"`
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("typed union decode: %s %v", raw, err)
		}
		if string(raw) != `{"amount":`+value+`}` {
			t.Fatalf("lost exact integer: %s", raw)
		}
	}
}
