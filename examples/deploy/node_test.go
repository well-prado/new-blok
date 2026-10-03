package deploy

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/deployment"
)

func TestNodeDeploymentConfigurationFailsClosed(t *testing.T) {
	c := deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 2, DrainTimeout: time.Second}
	t.Setenv("BLOK_WORKER_TOKEN", "")
	if _, err := NewNodeDeployment(c, t.TempDir()); !errors.Is(err, deployment.ErrNotReady) {
		t.Fatalf("missing secret: %v", err)
	}
	t.Setenv("BLOK_WORKER_TOKEN", "synthetic-deployment-token-000000001")
	for _, address := range []string{"0.0.0.0:9001", "127.0.0.1:0", "localhost:9001", "127.0.0.1:65536", "[::1]:9001", "127.0.0.2:9001"} {
		t.Setenv("BLOK_WORKER_ADDRESS", address)
		if _, err := NewNodeDeployment(c, t.TempDir()); err == nil || !strings.Contains(err.Error(), "literal loopback") {
			t.Fatalf("invalid worker address %s: %v", address, err)
		}
	}
	t.Setenv("BLOK_WORKER_ADDRESS", "127.0.0.1:9001")
	if _, err := NewNodeDeployment(c, t.TempDir()); err == nil {
		t.Fatal("missing worker files accepted")
	}
	c.MaxAdmission = 33
	if _, err := NewNodeDeployment(c, t.TempDir()); !errors.Is(err, deployment.ErrInvalid) {
		t.Fatalf("unbounded admission: %v", err)
	}
}

func TestActualNodeDeploymentReadinessAndWorkflow(t *testing.T) {
	root := os.Getenv("BLOK_NODE_INTEGRATION_ROOT")
	if root == "" {
		t.Skip("requires built Node worker")
	}
	t.Setenv("BLOK_WORKER_TOKEN", "synthetic-deployment-token-000000001")
	t.Setenv("BLOK_WORKER_ADDRESS", "127.0.0.1:9001")
	c := deployment.Config{ListenerAddress: "127.0.0.1:0", MaxAdmission: 2, DrainTimeout: time.Second}
	d, err := NewNodeDeployment(c, root)
	if err != nil {
		t.Fatal(err)
	}
	// Exercise startup/readiness with the actual child process via Run, then stop
	// through context cancellation. Port-zero avoids host listener collisions.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- d.Run(ctx, nil) }()
	for deadline := time.Now().Add(5 * time.Second); ; {
		response := httptest.NewRecorder()
		d.ServeHTTP(response, httptest.NewRequest("GET", "/readyz", nil))
		if response.Code == http.StatusOK {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("startup: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatal("worker not ready")
		}
		time.Sleep(20 * time.Millisecond)
	}
	response := httptest.NewRecorder()
	d.ServeHTTP(response, httptest.NewRequest("POST", "/quotes", strings.NewReader(`{"sku":"coffee","quantity":2,"delayMs":0}`)))
	if response.Code != 200 || strings.TrimSpace(response.Body.String()) != `{"totalCents":3000}` {
		t.Fatalf("typed workflow: %d %s", response.Code, response.Body.String())
	}
	// An external file mutation after startup must withdraw readiness. Use a
	// synthetic scratch file added under the inventory rather than changing code.
	path := filepath.Join(root, "runtime/nodejs/dist/deployment-synthetic-fault")
	if err := os.WriteFile(path, []byte("synthetic artifact mutation"), 0600); err != nil {
		t.Fatal(err)
	}
	defer os.Remove(path)
	response = httptest.NewRecorder()
	d.ServeHTTP(response, httptest.NewRequest("GET", "/readyz", nil))
	if response.Code != 503 {
		t.Fatalf("changed artifact readiness: %d", response.Code)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
