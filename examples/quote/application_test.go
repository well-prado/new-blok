package quote

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestQuoteApplicationUsesRealListenerAndEngine(t *testing.T) {
	application, handler, err := NewApplication()
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	listener := httptest.NewServer(handler)
	defer listener.Close()
	response, err := http.Post(listener.URL+"/quotes", "application/json", strings.NewReader(`{"sku":"coffee","quantity":2}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var body map[string]int64
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || body["totalCents"] != 3000 {
		t.Fatalf("status=%d body=%v", response.StatusCode, body)
	}
}

func TestQuoteApplicationReportsCorrelatedValidationFailure(t *testing.T) {
	application, handler, err := NewApplication()
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/quotes", strings.NewReader(`{"sku":"tea","quantity":2}`))
	handler.ServeHTTP(recorder, request)
	var body map[string]string
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if recorder.Code != http.StatusBadRequest || body["error"] != "unknown_sku" || body["requestId"] == "" {
		t.Fatalf("status=%d body=%v", recorder.Code, body)
	}
}

func TestQuoteApplicationServesActualTCPListener(t *testing.T) {
	application, handler, err := NewApplication()
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	defer server.Close()
	go func() { _ = server.Serve(listener) }()
	response, err := http.Post("http://"+listener.Addr().String()+"/quotes", "application/json", strings.NewReader(`{"sku":"coffee","quantity":2}`))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", response.StatusCode)
	}
}
