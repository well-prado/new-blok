package workerbench

import (
	"context"
	"errors"
	"net/http"
	"strings"
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

// charge posts one priced order to the provider in the given mode and reports
// the HTTP status (or the transport error) on the returned channel.
func charge(ctx context.Context, p *Provider, id, mode string) <-chan error {
	out := make(chan error, 1)
	go func() {
		body := `{"orderId":"` + id + `","totalCents":3000,"mode":"` + mode + `"}`
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.Server.URL+"/charge", strings.NewReader(body))
		if err != nil {
			out <- err
			return
		}
		req.Header.Set("Idempotency-Key", id)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			out <- err
			return
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			err = errors.New(resp.Status)
		}
		out <- err
	}()
	return out
}

func waitFor(t *testing.T, c <-chan string, what string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(10 * time.Second):
		t.Fatal(what)
	}
}

func TestProviderBarrierHoldsUntilReleased(t *testing.T) {
	for _, test := range []struct {
		mode             string
		effectsWhileHeld int
	}{{"delay", 0}, {"late", 1}} {
		t.Run(test.mode, func(t *testing.T) {
			p := NewProvider()
			defer p.Close()
			result := charge(context.Background(), p, test.mode, test.mode)
			if test.mode == "delay" {
				waitFor(t, p.Held(), "request not parked")
			} else {
				waitFor(t, p.Committed(), "effect not committed")
			}
			select {
			case err := <-result:
				t.Fatalf("answered while parked: %v", err)
			case <-time.After(250 * time.Millisecond):
			}
			if requests, effects := p.Counts(); requests != 1 || effects != test.effectsWhileHeld {
				t.Fatalf("while parked: %d/%d", requests, effects)
			}
			p.Release()
			p.Release()
			if err := <-result; err != nil {
				t.Fatal(err)
			}
			if requests, effects := p.Counts(); requests != 1 || effects != 1 {
				t.Fatalf("after release: %d/%d", requests, effects)
			}
		})
	}
}

func TestProviderBarrierAbandonedWhenCallerLeaves(t *testing.T) {
	for _, mode := range []string{"delay", "late"} {
		t.Run(mode, func(t *testing.T) {
			p := NewProvider()
			defer p.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := charge(ctx, p, mode, mode)
			if mode == "delay" {
				waitFor(t, p.Held(), "request not parked")
			} else {
				waitFor(t, p.Committed(), "effect not committed")
			}
			cancel()
			if err := <-result; err == nil {
				t.Fatal("canceled call succeeded")
			}
			waitFor(t, p.Abandoned(), "parked request not abandoned")
			want := 0
			if mode == "late" {
				want = 1
			}
			// Releasing afterwards must not revive the abandoned request.
			p.Release()
			time.Sleep(50 * time.Millisecond)
			if requests, effects := p.Counts(); requests != 1 || effects != want {
				t.Fatalf("after abandon: %d/%d", requests, effects)
			}
		})
	}
}

func TestProviderCloseUnblocksParkedRequest(t *testing.T) {
	p := NewProvider()
	result := charge(context.Background(), p, "close", "delay")
	waitFor(t, p.Held(), "request not parked")
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung on a parked request")
	}
	<-result
}
