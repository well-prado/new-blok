package quote

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
)

type catalog map[string]int64

func (c catalog) PriceCents(_ context.Context, sku string) (int64, error) {
	price, ok := c[sku]
	if !ok {
		return 0, &node.DomainError{Code: "unknown_sku", Class: "validation", Err: fmt.Errorf("sku %q is not available", sku)}
	}
	return price, nil
}

var quoteInputSchema = []byte(`{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`)

func quoteProgram(definition node.Definition[Input, Output]) (flow.Definition[Input, int64], error) {
	return flow.Define(flow.Spec{Name: "quote", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[Input]) flow.Ref[int64] {
		quote := flow.Call(builder, "calculate", definition, input)
		return flow.Select[Output, int64](quote, "totalCents")
	})
}

func NewApplication() (*app.Application, *blokhttp.Server, error) {
	definition, err := NewNode(catalog{"coffee": 1500})
	if err != nil {
		return nil, nil, err
	}
	workflow, err := quoteProgram(definition)
	if err != nil {
		return nil, nil, err
	}
	program, err := workflow.Lower()
	if err != nil {
		return nil, nil, err
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "shop/quote"}}, Routes: []app.Route{{Method: "POST", Path: "/quotes", Workflow: "shop/quote"}}})
	if err != nil {
		return nil, nil, err
	}
	handler, err := blokhttp.New(application, []blokhttp.Endpoint{{Method: "POST", Path: "/quotes", InputSchema: quoteInputSchema, Handle: func(ctx context.Context, input blokhttp.Input) (any, error) {
		var request Input
		if err := json.Unmarshal(input.Body, &request); err != nil {
			return nil, err
		}
		result, err := engine.New(map[string]node.Any{"shop/calculate-quote": definition.Any()}).Run(ctx, program, request)
		if err != nil {
			return nil, err
		}
		return map[string]any{"totalCents": result.Output}, nil
	}}})
	if err != nil {
		return nil, nil, err
	}
	return application, handler, nil
}
