package quote

import (
	"context"

	"github.com/well-prado/new-blok/node"
)

type Input struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}
type Output struct {
	SKU        string `json:"sku"`
	Quantity   int    `json:"quantity"`
	TotalCents int64  `json:"totalCents"`
	Currency   string `json:"currency"`
}
type Catalog interface {
	PriceCents(context.Context, string) (int64, error)
}

var quoteOutputSchema = []byte(`{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"},"totalCents":{"type":"integer"},"currency":{"type":"string"}},"required":["sku","quantity","totalCents","currency"]}`)

func NewNode(catalog Catalog) (node.Definition[Input, Output], error) {
	return node.Define("shop/calculate-quote", "1.0.0", func(ctx context.Context, input Input) (Output, error) {
		if input.Quantity < 1 || input.Quantity > 100 {
			return Output{}, &node.DomainError{Code: "invalid_quantity", Class: "validation"}
		}
		price, err := catalog.PriceCents(ctx, input.SKU)
		if err != nil {
			return Output{}, err
		}
		return Output{SKU: input.SKU, Quantity: input.Quantity, TotalCents: price * int64(input.Quantity), Currency: "USD"}, nil
	}, node.Description("Calculates a quote using an injected catalog"), node.Schemas([]byte(`{"type":"object"}`), quoteOutputSchema))
}
