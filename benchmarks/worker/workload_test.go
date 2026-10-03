package workerbench

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/well-prado/new-blok/internal/engine"
)

func TestNativeOrderCompleteWorkflowAndProviderEffects(t *testing.T) {
	p := NewProvider()
	defer p.Close()
	w, err := NewWorkflow(NativeNodes(p.Server.URL))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		result, err := w.Run(context.Background(), Order{OrderID: "same", SKU: "coffee", Quantity: 2, Mode: "ok"})
		if err != nil {
			t.Fatal(err)
		}
		want := Receipt{OrderID: "same", TotalCents: 3000, ReceiptID: "receipt-same", Status: "paid"}
		if result.Output != want || len(result.Steps) != 4 {
			t.Fatalf("result: %+v", result)
		}
	}
	requests, effects := p.Counts()
	if requests != 2 || effects != 1 {
		t.Fatalf("dedup: %d %d", requests, effects)
	}
}

func TestNativeOrderFailureFixtures(t *testing.T) {
	for _, test := range []struct {
		name, sku, mode, code string
		effects               int
	}{
		{"unknown", "unknown", "ok", "unknown_sku", 0},
		{"reject", "coffee", "reject", "payment_declined", 0},
		{"transient", "coffee", "transient", "provider_unavailable", 0},
		{"uncertain", "coffee", "uncertain", "provider_uncertain", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := NewProvider()
			defer p.Close()
			w, err := NewWorkflow(NativeNodes(p.Server.URL))
			if err != nil {
				t.Fatal(err)
			}
			result, err := w.Run(context.Background(), Order{OrderID: test.name, SKU: test.sku, Quantity: 2, Mode: test.mode})
			var failure *engine.Error
			if !errors.As(err, &failure) || failure.Code != test.code || result.Output != nil {
				t.Fatalf("result=%+v error=%v", result, err)
			}
			_, effects := p.Counts()
			if effects != test.effects {
				t.Fatalf("effects=%d want=%d", effects, test.effects)
			}
		})
	}
}

func TestNativeOrderCancellationHasNoPublishedReceipt(t *testing.T) {
	p := NewProvider()
	defer p.Close()
	w, err := NewWorkflow(NativeNodes(p.Server.URL))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	result, err := w.Run(ctx, Order{OrderID: "cancel", SKU: "coffee", Quantity: 2, Mode: "delay"})
	if !errors.Is(err, context.DeadlineExceeded) || result.Output != nil || result.State["pay"] != nil {
		t.Fatalf("cancel: %+v %v", result, err)
	}
	_, effects := p.Counts()
	if effects != 0 {
		t.Fatalf("canceled effect: %d", effects)
	}
}
