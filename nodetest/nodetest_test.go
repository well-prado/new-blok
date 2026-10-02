package nodetest

import (
	"context"
	"testing"

	"github.com/well-prado/new-blok/examples/quote"
)

type catalog struct{}

func (catalog) PriceCents(context.Context, string) (int64, error) { return 1500, nil }

func TestRunUsesDirectProductionNodeBoundary(t *testing.T) {
	definition, err := quote.NewNode(catalog{})
	if err != nil {
		t.Fatal(err)
	}
	output, err := Run(context.Background(), definition, quote.Input{SKU: "coffee", Quantity: 2})
	if err != nil || output.TotalCents != 3000 {
		t.Fatalf("output=%+v err=%v", output, err)
	}
}
