package provider

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func manifest() Manifest {
	return Manifest{Capabilities: []string{"payment:charge"}, SecretRefs: []string{"PAYMENT_TOKEN"}, MaxRequestBytes: 1024}
}

func TestHTTPProviderPassesIdempotencyAndClassifiesResponses(t *testing.T) {
	var gotKey string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	p := HTTP{Manifest: manifest()}
	resp, err := p.Execute(context.Background(), Request{Method: http.MethodPost, URL: server.URL, Body: []byte(`{}`), IdempotencyKey: "op-1"})
	if err != nil || resp.Status != 200 || gotKey != "op-1" {
		t.Fatalf("response=%+v err=%v key=%q", resp, err, gotKey)
	}
}

func TestHTTPProviderClassifiesBusinessAndTransientFailures(t *testing.T) {
	for _, tc := range []struct {
		status int
		class  ErrorClass
	}{{400, Business}, {500, Uncertain}, {429, Transient}, {408, Uncertain}} {
		t.Run(http.StatusText(tc.status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(tc.status) }))
			defer server.Close()
			p := HTTP{Manifest: manifest()}
			_, err := p.Execute(context.Background(), Request{Method: http.MethodGet, URL: server.URL})
			if !IsClass(err, tc.class) {
				t.Fatalf("got %v want %s", err, tc.class)
			}
		})
	}
}

func TestEffectRequiresInjectedProviderAndBoundsSecrets(t *testing.T) {
	if _, err := (Effect{Manifest: manifest()}).Execute(context.Background(), Request{}); err == nil {
		t.Fatal("missing provider accepted")
	}
	m := manifest()
	m.MaxRequestBytes = 1
	p := HTTP{Manifest: m}
	_, err := p.Execute(context.Background(), Request{Method: http.MethodPost, URL: "http://example.invalid", Body: []byte("too long")})
	if !IsClass(err, Invalid) {
		t.Fatalf("bound error: %v", err)
	}
	if strings.Contains(RedactedError(&Error{Class: Business, Code: "failed", Err: errors.New("PAYMENT_TOKEN=secret")}), "secret") {
		t.Fatal("credential leaked")
	}
}

func TestCanceledExternalRequestIsUncertain(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	p := HTTP{Manifest: manifest()}
	_, err := p.Execute(ctx, Request{Method: http.MethodPost, URL: server.URL, IdempotencyKey: "op-2"})
	if !IsClass(err, Uncertain) {
		t.Fatalf("got %v want uncertain", err)
	}
}

func TestEndpointSchemaBindingIsOwnedAndRequiredBeforeDispatch(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte(`{"value":null}`))
	}))
	defer server.Close()
	p, err := NewEndpoint[GenerateInput, GenerateOutput](server.URL, nil, HTTP{Manifest: manifest()})
	if err != nil {
		t.Fatal(err)
	}
	input := GenerateInput{Key: "schema-binding", Prompt: "synthetic"}
	raw := []byte(`{"type":"object","properties":{"value":{"type":"string","nullable":true}},"required":["value"],"additionalProperties":false}`)
	bound, err := p.WithOutputSchema(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[0] = 'x' // The bound schema owns its parsed data.
	if _, err := p.WithOutputSchema(raw); !IsClass(err, Invalid) {
		t.Fatalf("invalid schema accepted: %v", err)
	}
	if _, err := p.Execute(context.Background(), input); !IsClass(err, Invalid) || requests.Load() != 0 {
		t.Fatalf("unbound endpoint dispatched: requests=%d error=%v", requests.Load(), err)
	}
	out, err := bound.Execute(context.Background(), input)
	if err != nil || string(out.Value) != "null" || requests.Load() != 1 {
		t.Fatalf("bound nullable response: output=%s requests=%d error=%v", out.Value, requests.Load(), err)
	}
}
