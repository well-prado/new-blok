package quote

import (
	"context"
	"testing"
)

type fakeCatalog struct{ calls int }

func (f *fakeCatalog) PriceCents(context.Context, string) (int64, error) { f.calls++; return 1500, nil }

func TestQuoteNodeUsesInjectedCatalog(t *testing.T) {
	catalog := &fakeCatalog{}
	n, err := NewNode(catalog)
	if err != nil {
		t.Fatal(err)
	}
	got, err := n.Invoke(context.Background(), Input{SKU: "coffee", Quantity: 2})
	if err != nil || got.TotalCents != 3000 || catalog.calls != 1 {
		t.Fatalf("got=%+v err=%v calls=%d", got, err, catalog.calls)
	}
}
