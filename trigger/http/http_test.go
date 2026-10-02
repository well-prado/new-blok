package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
)

func readyApp(t *testing.T) *app.Application {
	t.Helper()
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	return application
}

func TestHTTPQuoteBindingUsesAdmissionAndMapsParametersBodyAndPrincipal(t *testing.T) {
	application := readyApp(t)
	server, err := New(application, []Endpoint{{Method: "POST", Path: "/quotes/:sku", InputSchema: []byte(`{"type":"object","properties":{"quantity":{"type":"integer"}},"required":["quantity"]}`), Authenticate: func(request *http.Request) (Principal, error) {
		return Principal{ID: request.Header.Get("Authorization")}, nil
	}, Handle: func(_ context.Context, input Input) (any, error) {
		var body struct {
			Quantity int `json:"quantity"`
		}
		if err := json.Unmarshal(input.Body, &body); err != nil {
			return nil, err
		}
		return map[string]any{"sku": input.Params["sku"], "quantity": body.Quantity, "principal": input.Principal.ID}, nil
	}}})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/quotes/coffee?source=test", strings.NewReader(`{"quantity":2}`))
	request.Header.Set("Authorization", "user-1")
	server.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"principal":"user-1"`) || !strings.Contains(recorder.Body.String(), `"sku":"coffee"`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestHTTPRejectsWrongTypesOversizedAndHidesInternalDetails(t *testing.T) {
	application := readyApp(t)
	server, err := New(application, []Endpoint{{Method: "POST", Path: "/quote", MaxBodyBytes: 20, InputSchema: []byte(`{"type":"object","properties":{"quantity":{"type":"integer"}},"required":["quantity"]}`), Handle: func(context.Context, Input) (any, error) { return nil, &internalError{} }}})
	if err != nil {
		t.Fatal(err)
	}
	wrong := httptest.NewRecorder()
	server.ServeHTTP(wrong, httptest.NewRequest(http.MethodPost, "/quote", strings.NewReader(`{"quantity":"two"}`)))
	if wrong.Code != http.StatusBadRequest {
		t.Fatalf("wrong type status=%d", wrong.Code)
	}
	large := httptest.NewRecorder()
	server.ServeHTTP(large, httptest.NewRequest(http.MethodPost, "/quote", strings.NewReader(`{"quantity":123456789}`)))
	if large.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("large status=%d", large.Code)
	}
	server2, err := New(application, []Endpoint{{Method: "POST", Path: "/internal", Handle: func(context.Context, Input) (any, error) { return nil, &internalError{} }}})
	if err != nil {
		t.Fatal(err)
	}
	internal := httptest.NewRecorder()
	server2.ServeHTTP(internal, httptest.NewRequest(http.MethodPost, "/internal", nil))
	if internal.Code != http.StatusInternalServerError || strings.Contains(internal.Body.String(), "secret") {
		t.Fatalf("internal=%d body=%s", internal.Code, internal.Body.String())
	}
}

func TestHTTPPreflightConflictsAndTimeoutCancellation(t *testing.T) {
	application := readyApp(t)
	if _, err := New(application, []Endpoint{{Method: "GET", Path: "/x/:id", Handle: func(context.Context, Input) (any, error) { return nil, nil }}, {Method: "GET", Path: "/x/:name", Handle: func(context.Context, Input) (any, error) { return nil, nil }}}); err == nil {
		t.Fatal("ambiguous routes accepted")
	}
	server, err := New(application, []Endpoint{{Method: "GET", Path: "/slow", Timeout: time.Millisecond, Handle: func(ctx context.Context, _ Input) (any, error) { <-ctx.Done(); return nil, ctx.Err() }}})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/slow", nil))
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("timeout status=%d", recorder.Code)
	}
}

type internalError struct{}

func (*internalError) Error() string { return "secret implementation detail" }
