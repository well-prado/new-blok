package flowtest

import (
	"context"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/node"
)

type catalog struct{}

func (catalog) PriceCents(context.Context, string) (int64, error) { return 1500, nil }

func TestRunUsesRealEngineAndInspectsResolvedStep(t *testing.T) {
	definition, err := quote.NewNode(catalog{})
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "calculate", Kind: "call", Node: "quote"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
	result := Run(context.Background(), program, map[string]node.Any{"quote": definition.Any()}, quote.Input{SKU: "coffee", Quantity: 2}, Options{})
	if !result.OK() || result.Response() != int64(3000) {
		t.Fatalf("ok=%v response=%v err=%v", result.OK(), result.Response(), result.Err())
	}
	step, ok := result.Step("calculate")
	if !ok || !step.Executed || step.Attempt != 1 {
		t.Fatalf("step=%+v ok=%v", step, ok)
	}
	if _, ok := result.State("calculate"); !ok {
		t.Fatal("missing committed state")
	}
}

func TestMocksAreOutputValidatedAndFakeClockDoesNotSleep(t *testing.T) {
	definition, err := quote.NewNode(catalog{})
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{{Index: 0, ID: "calculate", Kind: "call", Node: "quote"}}}
	result := Run(context.Background(), program, map[string]node.Any{"quote": definition.Any()}, quote.Input{}, Options{Mocks: map[string]Mock{"quote": {Base: definition.Any(), Output: quote.Output{TotalCents: 3000}}}})
	if !result.OK() {
		t.Fatal(result.Err())
	}
	bad := Run(context.Background(), program, map[string]node.Any{"quote": definition.Any()}, quote.Input{}, Options{Mocks: map[string]Mock{"quote": {Base: definition.Any(), Output: struct{ Wrong string }{Wrong: "x"}}}})
	if bad.OK() || bad.Err() == nil {
		t.Fatal("invalid mock output accepted")
	}
	clock := NewFakeClock(time.Unix(0, 0))
	if err := clock.Sleep(context.Background(), time.Second); err != nil || !clock.Now().Equal(time.Unix(1, 0)) {
		t.Fatalf("clock=%v err=%v", clock.Now(), err)
	}
}
