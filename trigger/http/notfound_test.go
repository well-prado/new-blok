package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
)

// The tests in this file cover #306: a domain error of class "not_found" is
// answered 404 with its code, and a record that is missing and one that
// belongs to another principal get byte-identical answers. The class is
// spelled as the wire string, not the constant, so the tests compile and fail
// on a tree without the mapping.

func startedApp(t *testing.T) *app.Application {
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

func serveError(t *testing.T, failure error) *httptest.ResponseRecorder {
	t.Helper()
	server, err := blokhttp.New(startedApp(t), []blokhttp.Endpoint{{Method: "GET", Path: "/records/:id", Handle: func(context.Context, blokhttp.Input) (any, error) {
		return nil, failure
	}}})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	server.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/records/r1", nil))
	return recorder
}

// TestHTTPMapsErrorClassesToStatus pins the class-to-status table, including
// the classes that already existed, so adding not_found moves nothing else.
// A not-found answer is a definite 4xx: never a 5xx a client or proxy would
// retry, and never a Retry-After.
func TestHTTPMapsErrorClassesToStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
		body   string
	}{
		{"not found", &node.DomainError{Code: "not_found", Class: "not_found", Err: errors.New("SYNTHETIC-SECRET row 42")}, http.StatusNotFound, "not_found"},
		{"not found with an entity code", fmt.Errorf("wrapped: %w", &node.DomainError{Code: "order_not_found", Class: "not_found"}), http.StatusNotFound, "order_not_found"},
		{"validation", &node.DomainError{Code: "invalid_quantity", Class: "validation"}, http.StatusBadRequest, "invalid_quantity"},
		{"another class", &node.DomainError{Code: "order_closed", Class: "state"}, http.StatusInternalServerError, "internal error"},
		{"not found with an unstable code", &node.DomainError{Code: "Not Found!", Class: "not_found"}, http.StatusInternalServerError, "internal error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := serveError(t, tc.err)
			var body struct {
				Error     string `json:"error"`
				RequestID string `json:"requestId"`
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if recorder.Code != tc.status || body.Error != tc.body || body.RequestID == "" {
				t.Fatalf("status=%d body=%s; want %d %q", recorder.Code, recorder.Body, tc.status, tc.body)
			}
			if strings.Contains(recorder.Body.String(), "SYNTHETIC") {
				t.Fatalf("error text reached the client: %s", recorder.Body)
			}
			if tc.status == http.StatusNotFound && (recorder.Header().Get("Retry-After") != "" || recorder.Code >= 500) {
				t.Fatalf("a not-found answer invites a retry: status=%d Retry-After=%q", recorder.Code, recorder.Header().Get("Retry-After"))
			}
		})
	}
}

type recordInput struct {
	ID        string `json:"id"`
	Principal string `json:"principal"`
}

type record struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
}

// owners is the synthetic table: record id to owning principal.
var owners = map[string]string{"r-alice": "alice"}

// getRecord fails a missing record and another principal's record with the
// same code and class; only the private cause differs, and it must not
// reach the caller.
func getRecord(t *testing.T) node.Definition[recordInput, record] {
	t.Helper()
	definition, err := node.Define("records/get", "1.0.0", func(_ context.Context, input recordInput) (record, error) {
		owner, ok := owners[input.ID]
		switch {
		case !ok:
			return record{}, &node.DomainError{Code: "not_found", Class: "not_found", Err: fmt.Errorf("SYNTHETIC: no row with id %q", input.ID)}
		case owner != input.Principal:
			return record{}, &node.DomainError{Code: "not_found", Class: "not_found", Err: fmt.Errorf("SYNTHETIC: row %q belongs to %s", input.ID, owner)}
		}
		return record{ID: input.ID, Owner: owner}, nil
	}, node.Description("Reads a record its principal owns"), node.Pure(), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object","properties":{"id":{"type":"string"},"owner":{"type":"string"}},"required":["id","owner"]}`)))
	if err != nil {
		t.Fatal(err)
	}
	return definition
}

var requestID = regexp.MustCompile(`"requestId":"[^"]*"`)

// TestHTTPHiddenAndMissingRecordsAreIndistinguishable runs a real workflow
// through the engine: bob asking for alice's record and for a record that
// does not exist gets the same status, headers and body, apart from the
// request id.
func TestHTTPHiddenAndMissingRecordsAreIndistinguishable(t *testing.T) {
	definition := getRecord(t)
	workflow, err := flow.Define(flow.Spec{Name: "records-get", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[recordInput]) flow.Ref[record] {
		return flow.Call(builder, "get", definition, input)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := workflow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	application := startedApp(t)
	runner := execution.NewRunner(application, map[string]node.Any{"records/get": definition.Any()})
	server, err := blokhttp.New(application, []blokhttp.Endpoint{{
		Method: "GET", Path: "/records/:id",
		Authenticate: func(request *http.Request) (blokhttp.Principal, error) {
			return blokhttp.Principal{ID: request.Header.Get("Authorization")}, nil
		},
		Handle: func(ctx context.Context, input blokhttp.Input) (any, error) {
			result, err := runner.Run(ctx, program, recordInput{ID: input.Params["id"], Principal: input.Principal.ID}, inspection.Invocation{})
			if err != nil {
				return nil, err
			}
			return result.Output, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	get := func(principal, id string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/records/"+id, nil)
		request.Header.Set("Authorization", principal)
		server.ServeHTTP(recorder, request)
		return recorder
	}
	if own := get("alice", "r-alice"); own.Code != http.StatusOK || !strings.Contains(own.Body.String(), `"owner":"alice"`) {
		t.Fatalf("owner read: status=%d body=%s", own.Code, own.Body)
	}
	hidden, missing := get("bob", "r-alice"), get("bob", "r-missing")
	if hidden.Code != http.StatusNotFound {
		t.Fatalf("another principal's record: status=%d body=%s; want 404", hidden.Code, hidden.Body)
	}
	hiddenBody := requestID.ReplaceAllString(hidden.Body.String(), `"requestId":"-"`)
	missingBody := requestID.ReplaceAllString(missing.Body.String(), `"requestId":"-"`)
	if hidden.Code != missing.Code || hiddenBody != missingBody || !reflect.DeepEqual(hidden.Header(), missing.Header()) {
		t.Fatalf("distinguishable:\nhidden  %d %v %s\nmissing %d %v %s", hidden.Code, hidden.Header(), hidden.Body, missing.Code, missing.Header(), missing.Body)
	}
	if hiddenBody != `{"error":"not_found","requestId":"-"}`+"\n" {
		t.Fatalf("not-found body %q", hiddenBody)
	}
	for _, leak := range []string{"SYNTHETIC", "alice", "r-alice", "r-missing", "belongs"} {
		if strings.Contains(hidden.Body.String(), leak) || strings.Contains(missing.Body.String(), leak) {
			t.Fatalf("answer reveals %q: %s / %s", leak, hidden.Body, missing.Body)
		}
	}
}
