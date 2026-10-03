package sse_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/sse"
	"github.com/well-prado/new-blok/trigger/worker"
)

// TestBusyStoreStartAnswersSaturated: an SSE start whose durable submission
// cannot get the store's write lock is answered 503 saturated with
// Retry-After, opens no stream that work stands behind, and commits
// nothing (#184).
func TestBusyStoreStartAnswersSaturated(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "busy.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := worker.New(ctx, database, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := queue.RegisterKind("order.build", []byte(orderSchema)); err != nil {
		t.Fatal(err)
	}
	hub, err := sse.NewHub(sse.HubConfig{})
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	server, err := sse.New(application, hub, []sse.Endpoint{{Name: "orders", Path: "/orders", Kind: "order.build", Submit: queue, Tracker: queue, Authenticate: authenticate, InputSchema: []byte(orderSchema)}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stop, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(stop)
		_ = application.Shutdown(stop)
	})
	listener := httptest.NewServer(server)
	defer listener.Close()
	holding, release, held := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		held <- database.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `UPDATE worker_jobs SET updated_at = updated_at`); err != nil {
				return err
			}
			close(holding)
			<-release
			return nil
		})
	}()
	<-holding
	var released sync.Once
	t.Cleanup(func() { released.Do(func() { close(release) }) })
	request, err := http.NewRequest(http.MethodPost, listener.URL+"/orders", strings.NewReader(`{"item":"book"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer alice")
	request.Header.Set("Idempotency-Key", "busy")
	response, err := listener.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	var answer map[string]any
	_ = json.NewDecoder(response.Body).Decode(&answer)
	response.Body.Close()
	released.Do(func() { close(release) })
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || answer["error"] != "saturated" || response.Header.Get("Retry-After") != "1" {
		t.Fatalf("a busy store answered %d %v Retry-After=%q; want 503 saturated", response.StatusCode, answer, response.Header.Get("Retry-After"))
	}
	if settled, err := queue.Settled(ctx, sse.SubmissionKey("orders", trigger.Principal{ID: "alice"}, "busy")); err != nil || !settled {
		t.Fatalf("a saturated start left work behind: settled=%v err=%v", settled, err)
	}
}
