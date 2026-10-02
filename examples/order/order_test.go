package order

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/store/sqlite"
)

func TestOrderCommitAndOutboxAreAtomicAndDuplicateDeliveryIsIdempotent(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(context.Background(), Request{RequestKey: "req-1", SKU: "coffee", Quantity: 2}); err != nil {
		t.Fatal(err)
	}
	processed, err := service.ProcessOnce(context.Background())
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	if _, err := service.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := service.Get(context.Background(), "req-1")
	if err != nil || result.TotalCents != 3000 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	sent := 0
	for {
		processed, err := service.DispatchOne(context.Background(), func(_ context.Context, event Event) error {
			sent++
			if event.ID != "event:req-1" {
				t.Fatalf("event=%+v", event)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		if !processed {
			break
		}
	}
	if sent != 1 {
		t.Fatalf("sent=%d, want one outbox event", sent)
	}
	processed, err = service.DispatchOne(context.Background(), func(context.Context, Event) error { sent++; return nil })
	if err != nil || processed {
		t.Fatalf("second dispatch processed=%v err=%v", processed, err)
	}
}

func TestInvalidOrderIsDeadLetteredWithoutBusinessRows(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(context.Background(), Request{RequestKey: "bad", SKU: "coffee", Quantity: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Get(context.Background(), "bad"); err == nil {
		t.Fatal("invalid order was stored")
	}
}

func TestOutboxProviderFailureKeepsPendingEventAndRetriesStableID(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "orders.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	service, err := New(context.Background(), database, map[string]int64{"coffee": 1500}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Enqueue(context.Background(), Request{RequestKey: "req-timeout", SKU: "coffee", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var eventIDs []string
	processed, err := service.DispatchOne(context.Background(), func(_ context.Context, event Event) error {
		eventIDs = append(eventIDs, event.ID)
		return errors.New("provider timeout")
	})
	if !processed || err == nil {
		t.Fatalf("failed dispatch processed=%v err=%v", processed, err)
	}
	processed, err = service.DispatchOne(context.Background(), func(_ context.Context, event Event) error {
		eventIDs = append(eventIDs, event.ID)
		return nil
	})
	if !processed || err != nil {
		t.Fatalf("retry dispatch processed=%v err=%v", processed, err)
	}
	if len(eventIDs) != 2 || eventIDs[0] != eventIDs[1] {
		t.Fatalf("event IDs=%v, want stable provider identity across retry", eventIDs)
	}
	processed, err = service.DispatchOne(context.Background(), func(context.Context, Event) error {
		t.Fatal("sent outbox event was dispatched again")
		return nil
	})
	if processed || err != nil {
		t.Fatalf("post-commit dispatch processed=%v err=%v", processed, err)
	}
}
