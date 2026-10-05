package trigger_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
	"github.com/well-prado/new-blok/trigger/testdata/selection/quote"
	"github.com/well-prado/new-blok/trigger/worker"
)

type bindingFixture struct {
	Workflow struct {
		Name        string          `json:"name"`
		InputSchema json.RawMessage `json:"inputSchema"`
	} `json:"workflow"`
	Bindings []struct {
		ID             string          `json:"id"`
		Kind           trigger.Kind    `json:"kind"`
		Workflow       string          `json:"workflow"`
		Path           string          `json:"path"`
		Authenticated  bool            `json:"authenticated"`
		ProtocolSchema json.RawMessage `json:"protocolSchema"`
		Mapping        []string        `json:"mapping"`
		InputSchema    json.RawMessage `json:"inputSchema"`
	} `json:"bindings"`
	RejectedBindings []struct {
		Name    string `json:"name"`
		Code    string `json:"code"`
		Binding struct {
			ID          string          `json:"id"`
			Kind        trigger.Kind    `json:"kind"`
			Workflow    string          `json:"workflow"`
			InputSchema json.RawMessage `json:"inputSchema"`
		} `json:"binding"`
	} `json:"rejectedBindings"`
	Deliveries []struct {
		Binding    string          `json:"binding"`
		Payload    json.RawMessage `json:"payload"`
		Credential string          `json:"credential"`
		Expect     string          `json:"expect"`
		Code       string          `json:"code"`
		Principal  string          `json:"principal"`
	} `json:"deliveries"`
	Expected struct {
		Output     int   `json:"output"`
		Errors     int   `json:"errors"`
		Effects    int   `json:"effects"`
		TotalCents int64 `json:"totalCents"`
	} `json:"expected"`
}

// TestOneWorkflowServesMultipleCompatibleBindings binds one domain workflow to
// two HTTP routes with different schemas and authentication and to a durable
// worker kind, rejects incompatible bindings at startup, and drives every
// binding through its real protocol.
func TestOneWorkflowServesMultipleCompatibleBindings(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "testdata", "conformance", "multi-binding.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture bindingFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	var bindings []trigger.Binding
	for _, b := range fixture.Bindings {
		bindings = append(bindings, trigger.Binding{ID: b.ID, Kind: b.Kind, Workflow: b.Workflow, InputSchema: b.InputSchema})
	}
	if err := trigger.CheckBindings(fixture.Workflow.Name, fixture.Workflow.InputSchema, bindings); err != nil {
		t.Fatalf("compatible bindings rejected: %v", err)
	}
	for _, rejected := range fixture.RejectedBindings {
		candidate := append(append([]trigger.Binding(nil), bindings...), trigger.Binding{ID: rejected.Binding.ID, Kind: rejected.Binding.Kind, Workflow: rejected.Binding.Workflow, InputSchema: rejected.Binding.InputSchema})
		err := trigger.CheckBindings(fixture.Workflow.Name, fixture.Workflow.InputSchema, candidate)
		var diagnostic *trigger.Error
		if !errors.As(err, &diagnostic) || diagnostic.Code != rejected.Code {
			t.Fatalf("%s: err=%v, want %s", rejected.Name, err, rejected.Code)
		}
	}

	runner, err := quote.New()
	if err != nil {
		t.Fatal(err)
	}
	workflowSchema, err := schema.Parse(fixture.Workflow.InputSchema)
	if err != nil {
		t.Fatal(err)
	}
	mappings := map[string][]string{}
	for _, b := range fixture.Bindings {
		mappings[b.ID] = b.Mapping
	}
	// run applies the binding's mapping, then admits the value only if the
	// workflow schema accepts it. Nothing relies on a lenient decoder.
	var effects atomic.Int64
	run := func(ctx context.Context, binding string, body []byte) (any, error) {
		mapped, err := applyMapping(body, mappings[binding])
		if err != nil {
			return nil, err
		}
		if _, err := workflowSchema.Normalize(mapped); err != nil {
			return nil, fmt.Errorf("binding %s handed the workflow an invalid value: %w", binding, err)
		}
		var input quote.Input
		if err := json.Unmarshal(mapped, &input); err != nil {
			return nil, err
		}
		output, err := runner.Run(ctx, input)
		if err == nil {
			effects.Add(1)
		}
		return output, err
	}

	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer application.Shutdown(context.Background())
	var principalsMu sync.Mutex
	principals := map[string]string{}
	principal := func(binding string) string {
		principalsMu.Lock()
		defer principalsMu.Unlock()
		return principals[binding]
	}
	var endpoints []blokhttp.Endpoint
	for _, b := range fixture.Bindings {
		if b.Kind != trigger.HTTP {
			continue
		}
		binding := b
		endpoint := blokhttp.Endpoint{Method: "POST", Path: binding.Path, InputSchema: binding.ProtocolSchema, Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
			principalsMu.Lock()
			principals[binding.ID] = in.Principal.ID
			principalsMu.Unlock()
			return run(ctx, binding.ID, in.Body)
		}}
		if binding.Authenticated {
			endpoint.Authenticate = func(request *http.Request) (blokhttp.Principal, error) {
				if request.Header.Get("Authorization") != "Bearer partner-token" {
					return blokhttp.Principal{}, errors.New("unauthenticated")
				}
				return blokhttp.Principal{ID: "partner-1"}, nil
			}
		}
		endpoints = append(endpoints, endpoint)
	}
	handler, err := blokhttp.New(application, endpoints)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()

	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	queue, err := worker.New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	paths := map[string]string{}
	for _, b := range fixture.Bindings {
		paths[b.ID] = b.Path
		if b.Kind == trigger.Worker {
			if err := queue.RegisterKind(b.ID, b.ProtocolSchema); err != nil {
				t.Fatal(err)
			}
		}
	}

	output, failures := 0, 0
	for index, delivery := range fixture.Deliveries {
		var status, code string
		var totalCents int64
		if path := paths[delivery.Binding]; path != "" {
			request, _ := http.NewRequest(http.MethodPost, server.URL+path, bytes.NewReader(delivery.Payload))
			if delivery.Credential != "" {
				request.Header.Set("Authorization", "Bearer "+delivery.Credential)
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			_ = json.NewDecoder(response.Body).Decode(&body)
			response.Body.Close()
			switch response.StatusCode {
			case http.StatusOK:
				status = "completed"
				totalCents = int64(body["totalCents"].(float64))
			case http.StatusUnauthorized:
				status, code = "rejected", "unauthorized"
			case http.StatusBadRequest:
				status, code = "rejected", "invalid_input"
			default:
				t.Fatalf("delivery %d: status %d", index, response.StatusCode)
			}
		} else {
			key := "delivery-" + strings.Repeat("x", index+1)
			if _, err := queue.Enqueue(context.Background(), worker.EnqueueRequest{RequestKey: key, Kind: delivery.Binding, Payload: delivery.Payload}); err != nil {
				t.Fatal(err)
			}
			var workerOutput any
			if _, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, _ worker.Tx, job worker.Job) error {
				workerOutput, err = run(ctx, job.Kind, job.Payload)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			job, err := queue.Get(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			if job.State == worker.StateCompleted {
				status = "completed"
				encoded, err := json.Marshal(workerOutput)
				if err != nil {
					t.Fatal(err)
				}
				var result quote.Output
				if err := json.Unmarshal(encoded, &result); err != nil {
					t.Fatal(err)
				}
				totalCents = result.TotalCents
			}
		}
		if status != delivery.Expect || code != delivery.Code {
			t.Fatalf("delivery %d via %s: %s/%s, want %s/%s", index, delivery.Binding, status, code, delivery.Expect, delivery.Code)
		}
		if status == "completed" {
			output++
			if totalCents != fixture.Expected.TotalCents {
				t.Fatalf("delivery %d via %s: totalCents=%d", index, delivery.Binding, totalCents)
			}
			if delivery.Principal != "" && principal(delivery.Binding) != delivery.Principal {
				t.Fatalf("delivery %d principal=%q, want %q", index, principal(delivery.Binding), delivery.Principal)
			}
		} else {
			failures++
		}
	}
	if output != fixture.Expected.Output || failures != fixture.Expected.Errors || int(effects.Load()) != fixture.Expected.Effects {
		t.Fatalf("output=%d errors=%d effects=%d, want %+v", output, failures, effects.Load(), fixture.Expected)
	}
	if got := principal("http-public"); got != "" {
		t.Fatalf("unauthenticated binding observed principal %q", got)
	}
}

// applyMapping is the test's binding mapper. The only operation the fixture
// uses is "drop:<field>", which removes a protocol-only field.
func applyMapping(body []byte, mapping []string) ([]byte, error) {
	if len(mapping) == 0 {
		return body, nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil, err
	}
	for _, operation := range mapping {
		field, ok := strings.CutPrefix(operation, "drop:")
		if !ok {
			return nil, fmt.Errorf("unsupported mapping %q", operation)
		}
		delete(fields, field)
	}
	return json.Marshal(fields)
}
