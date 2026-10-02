package engine_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

func quoteProgram() contract.InternalProgram {
	return contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
}

type catalog struct{}

func (catalog) PriceCents(context.Context, string) (int64, error) { return 1500, nil }

func TestEngineRunsRealQuoteNodeAndCommitsDeclaredOutput(t *testing.T) {
	definition, err := quote.NewNode(catalog{})
	if err != nil {
		t.Fatal(err)
	}
	runner := engine.New(map[string]node.Any{"shop/calculate-quote": definition.Any()})
	result, err := runner.Run(context.Background(), quoteProgram(), quote.Input{SKU: "coffee", Quantity: 2})
	if err != nil || result.Output != int64(3000) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(result.State) != 1 || len(result.Steps) != 2 || !result.Steps[0].Executed {
		t.Fatalf("result=%+v", result)
	}
}

func TestEnginePublishesNoOutputOnErrorPanicInvalidOutputOrCancellation(t *testing.T) {
	inputSchema := []byte(`{"type":"object"}`)
	outputSchema := []byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`)
	makeNode := func(handler node.Handler[quote.Input, map[string]any]) node.Any {
		return node.MustDefine("test/node", "1.0.0", handler, node.Description("engine test"), node.Schemas(inputSchema, outputSchema)).Any()
	}
	program := quoteProgram()
	program.Instructions[0].Node = "test/node"
	for name, definition := range map[string]node.Any{
		"error": makeNode(func(context.Context, quote.Input) (map[string]any, error) { return nil, errors.New("failed") }),
		"panic": makeNode(func(context.Context, quote.Input) (map[string]any, error) { panic("boom") }),
		"invalid": makeNode(func(context.Context, quote.Input) (map[string]any, error) {
			return map[string]any{"value": "wrong"}, nil
		}),
	} {
		t.Run(name, func(t *testing.T) {
			result, err := engine.New(map[string]node.Any{"test/node": definition}).Run(context.Background(), program, quote.Input{SKU: "coffee", Quantity: 2})
			if err == nil || len(result.State) != 0 || result.Output != nil {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			if !strings.Contains(err.Error(), map[string]string{"error": "node_error", "panic": "node_panic", "invalid": "invalid_output"}[name]) {
				t.Fatalf("err=%v", err)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, err := engine.New(map[string]node.Any{"test/node": makeNode(func(context.Context, quote.Input) (map[string]any, error) { return map[string]any{"value": 1}, nil })}).Run(ctx, program, quote.Input{})
	if err == nil || !strings.Contains(err.Error(), "canceled") || len(result.State) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
