package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/examples/recipes/shop"
	"github.com/well-prado/new-blok/store/sqlite"
)

func TestExternalModuleComposesHTTPWorkflowAndDurablePublisher(t *testing.T) {
	ctx := context.Background()
	const expectedAccepted = 1
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "app.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	sinkDatabase, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "sink.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sinkDatabase.Close()
	sink, err := shop.NewSyntheticSink(ctx, sinkDatabase)
	if err != nil {
		t.Fatal(err)
	}
	application, err := shop.New(ctx, shop.Config{
		Database: database, Tokens: map[string]string{
			"alice": "external-alice-token-0001",
			"bob":   "external-bob-token-00002",
		},
		WebhookKey: []byte("external-webhook-secret-0001"), Publisher: sink,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(ctx)
	server := httptest.NewServer(application.Handler)
	defer server.Close()
	request, err := http.NewRequest(http.MethodPost, server.URL+"/records", strings.NewReader(`{"id":"external-record","value":"external"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer external-alice-token-0001")
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("external composition create status=%d", response.StatusCode)
	}
	processed, err := application.DrainOutbox(ctx)
	if err != nil || !processed {
		t.Fatalf("external composition publisher processed=%v err=%v", processed, err)
	}
	var accepted int
	var payload []byte
	if err := sinkDatabase.WithTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRow(`SELECT COUNT(*) FROM synthetic_sink_events`).Scan(&accepted); err != nil {
			return err
		}
		return tx.QueryRow(`SELECT payload_json FROM synthetic_sink_events LIMIT 1`).Scan(&payload)
	}); err != nil {
		t.Fatal(err)
	}
	if accepted != expectedAccepted {
		t.Fatalf("synthetic receiver accepted %d events, want %d", accepted, expectedAccepted)
	}
	var event shop.RecordEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "shop.record.changed" || event.Record.Owner != "alice" {
		t.Fatalf("external workflow event=%+v", event)
	}
}
