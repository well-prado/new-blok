// Package quote is the trigger-independent domain workflow shared by the
// selection binaries. It imports no trigger, listener or store.
package quote

import (
	"context"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type Input struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type Output struct {
	TotalCents int64 `json:"totalCents"`
}

var InputSchema = []byte(`{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`)

var outputSchema = []byte(`{"type":"object","properties":{"totalCents":{"type":"integer"}},"required":["totalCents"]}`)

// Runner executes the quote workflow through the production engine.
type Runner struct {
	engine  *engine.Engine
	program contract.InternalProgram
}

func New() (*Runner, error) {
	calculate, err := node.Define("selection/calculate-quote", "1.0.0", func(_ context.Context, in Input) (Output, error) {
		return Output{TotalCents: int64(in.Quantity) * 1500}, nil
	}, node.Description("Prices a synthetic quote"), node.Schemas(InputSchema, outputSchema), node.Pure())
	if err != nil {
		return nil, err
	}
	workflow, err := flow.Define(flow.Spec{Name: "selection/quote", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[Input]) flow.Ref[Output] {
		return flow.Call(b, "calculate", calculate, in)
	})
	if err != nil {
		return nil, err
	}
	program, err := workflow.Lower()
	if err != nil {
		return nil, err
	}
	return &Runner{engine: engine.New(map[string]node.Any{"selection/calculate-quote": calculate.Any()}), program: program}, nil
}

func (r *Runner) Run(ctx context.Context, in Input) (any, error) {
	result, err := r.engine.Run(ctx, r.program, in)
	if err != nil {
		return nil, err
	}
	return result.Output, nil
}
