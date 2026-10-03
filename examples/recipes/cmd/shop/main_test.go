package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/examples/recipes/shop"
	"github.com/well-prado/new-blok/store/sqlite"
)

const (
	aliceToken = "alice-example-token-0001"
	bobToken   = "bob-example-token-00002"
	webhookKey = "synthetic-webhook-key-00000001"
)

func TestFreshMigrationReplayStatusAndTeardownCommands(t *testing.T) {
	databasePath := filepath.Join(t.TempDir(), "new", "shop.db")
	t.Setenv("SHOP_DB_PATH", databasePath)
	for range 2 {
		if err := run([]string{"migrate-up"}); err != nil {
			t.Fatalf("migrate-up: %v", err)
		}
	}
	if err := run([]string{"migrate-status"}); err != nil {
		t.Fatalf("migrate-status: %v", err)
	}
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	var version, count int
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT MAX(version), COUNT(*) FROM shop_schema_migrations`).Scan(&version, &count)
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	if version != 3 || count != 3 {
		t.Fatalf("migration version=%d rows=%d", version, count)
	}
	if err := run([]string{"teardown"}); err != nil {
		t.Fatalf("teardown: %v", err)
	}
	if err := run([]string{"migrate-up"}); err != nil {
		t.Fatalf("fresh migration after teardown: %v", err)
	}
}

func TestServePollIterationPublishesCommittedOutboxEvent(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "serve.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	sinkDatabase, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "receiver.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer sinkDatabase.Close()
	sink, err := shop.NewSyntheticSink(ctx, sinkDatabase)
	if err != nil {
		t.Fatal(err)
	}
	application, err := shop.New(ctx, shop.Config{
		Database: database, Tokens: map[string]string{"alice": aliceToken, "bob": bobToken},
		WebhookKey: []byte(webhookKey), Publisher: sink,
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
	request, err := http.NewRequest(http.MethodPost, server.URL+"/records", strings.NewReader(`{"id":"serve-outbox","value":"published"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+aliceToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(response.Body)
		t.Fatalf("create status=%d body=%s", response.StatusCode, body)
	}
	_ = response.Body.Close()
	processed, err := processAvailable(ctx, application)
	if err != nil || !processed {
		t.Fatalf("serve poll iteration processed=%v err=%v", processed, err)
	}
	var event shop.RecordEvent
	if err := sinkDatabase.WithTx(ctx, func(tx *sql.Tx) error {
		var payload []byte
		if err := tx.QueryRow(`SELECT payload_json FROM synthetic_sink_events LIMIT 1`).Scan(&payload); err != nil {
			return err
		}
		return json.Unmarshal(payload, &event)
	}); err != nil {
		t.Fatal(err)
	}
	if event.Type != "shop.record.changed" || event.Record.ID != "serve-outbox" {
		t.Fatalf("serve published event=%+v", event)
	}
}
