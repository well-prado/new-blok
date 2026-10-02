package node

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
)

var quoteSchemas = []byte(`{"type":"object"}`)

type quoteInput struct {
	SKU      string
	Quantity int
}
type quoteOutput struct{ TotalCents int64 }

func quoteNode(t *testing.T, handler Handler[quoteInput, quoteOutput], options ...Option) Definition[quoteInput, quoteOutput] {
	t.Helper()
	options = append([]Option{Description("calculates a quote"), Schemas(quoteSchemas, quoteSchemas)}, options...)
	return MustDefine("shop/calculate-quote", "1.0.0", handler, options...)
}

func TestDirectInvocationAndInjectedDependency(t *testing.T) {
	prices := map[string]int64{"coffee": 1500}
	n := quoteNode(t, func(ctx context.Context, in quoteInput) (quoteOutput, error) {
		if err := ctx.Err(); err != nil {
			return quoteOutput{}, err
		}
		price, ok := prices[in.SKU]
		if !ok {
			return quoteOutput{}, &DomainError{Code: "unknown_sku", Class: "validation", Err: errors.New("unknown sku")}
		}
		return quoteOutput{TotalCents: price * int64(in.Quantity)}, nil
	}, Pure())
	got, err := n.Invoke(context.Background(), quoteInput{SKU: "coffee", Quantity: 2})
	if err != nil || got.TotalCents != 3000 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if len(n.Descriptor().InputSchema) == 0 || len(n.Descriptor().OutputSchema) == 0 {
		t.Fatal("descriptor schemas missing")
	}
}

func TestRegistrationDoesNotCallBusinessHandler(t *testing.T) {
	calls := 0
	n := quoteNode(t, func(context.Context, quoteInput) (quoteOutput, error) { calls++; return quoteOutput{}, nil })
	registry := NewRegistry()
	if err := registry.Register(n.Any()); err != nil {
		t.Fatal(err)
	}
	if calls != 0 {
		t.Fatalf("registration called handler %d times", calls)
	}
}

func TestDuplicateAndIncompatibleDescriptors(t *testing.T) {
	first := quoteNode(t, func(context.Context, quoteInput) (quoteOutput, error) { return quoteOutput{}, nil })
	second := quoteNode(t, func(context.Context, quoteInput) (quoteOutput, error) { return quoteOutput{}, nil })
	registry := NewRegistry()
	if err := registry.Register(first.Any()); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(second.Any()); err == nil || err.(*Error).Code != "duplicate_node" {
		t.Fatalf("got %v", err)
	}
	third := quoteNode(t, func(context.Context, quoteInput) (quoteOutput, error) { return quoteOutput{}, nil }, Schemas([]byte(`{"type":"string"}`), quoteSchemas))
	if err := registry.Register(third.Any()); err == nil || err.(*Error).Code != "incompatible_schema_descriptor" {
		t.Fatalf("got %v", err)
	}
}

func TestCancellationAndPanicClassification(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	n := quoteNode(t, func(context.Context, quoteInput) (quoteOutput, error) {
		t.Fatal("canceled handler ran")
		return quoteOutput{}, nil
	})
	if _, err := n.Invoke(ctx, quoteInput{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	panics := quoteNode(t, func(context.Context, quoteInput) (quoteOutput, error) { panic("boom") })
	if _, err := panics.Invoke(context.Background(), quoteInput{}); err == nil || err.(*Error).Code != "node_panic" {
		t.Fatalf("got %v", err)
	}
}

func TestDomainErrorClassification(t *testing.T) {
	n := quoteNode(t, func(context.Context, quoteInput) (quoteOutput, error) {
		return quoteOutput{}, &DomainError{Code: "payment_timeout", Class: "transient", Retryable: true}
	})
	_, err := n.Invoke(context.Background(), quoteInput{})
	domain, ok := AsDomainError(err)
	if !ok || !domain.IsRetryable() || domain.Class != "transient" {
		t.Fatalf("domain=%+v ok=%v", domain, ok)
	}
}

func TestSyntheticFixtureDeclaresExpectedCounts(t *testing.T) {
	type fixtureCase struct {
		Name                string `json:"name"`
		ExpectedOutputCount int    `json:"expectedOutputCount"`
		ExpectedErrorCount  int    `json:"expectedErrorCount"`
		ExpectedEffectCount int    `json:"expectedEffectCount"`
	}
	var fixture struct {
		Cases []fixtureCase `json:"cases"`
	}
	data, err := os.ReadFile("testdata/fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) < 4 {
		t.Fatalf("fixture cases=%d, want at least 4", len(fixture.Cases))
	}
	for _, testCase := range fixture.Cases {
		if testCase.Name == "" || testCase.ExpectedOutputCount < 0 || testCase.ExpectedErrorCount < 0 || testCase.ExpectedEffectCount < 0 {
			t.Fatalf("invalid fixture case %+v", testCase)
		}
	}
}
