package parity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/internal/compile"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/migration"
	"github.com/well-prado/new-blok/node"
)

type migratedWorkflowCase struct {
	ID       string          `json:"id"`
	Source   json.RawMessage `json:"source"`
	Expected struct {
		Supported        bool            `json:"supported"`
		Output           json.RawMessage `json:"output"`
		OldOutput        json.RawMessage `json:"oldOutput"`
		Code             string          `json:"code"`
		Path             string          `json:"path"`
		ProviderCalls    int             `json:"providerCalls"`
		CommittedEffects int             `json:"committedEffects"`
	} `json:"expected"`
}

// TestConvertedWorkflowExecutesLikePublishedRunner executes the same Blok v2
// JSON source twice: as given, through the published 2.5.0 Configuration and
// Runner, and after migration.Convert plus the canonical compiler, through the
// native engine. Supported sources must produce the predeclared business output
// and provider ledger on both sides. A source the old runner executes but the
// converter does not support must be refused with an actionable diagnostic,
// never rewritten into a different workflow.
func TestConvertedWorkflowExecutesLikePublishedRunner(t *testing.T) {
	requireOldEngine(t)
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", "contracts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		MigratedWorkflows struct {
			Request map[string]any         `json:"request"`
			Cases   []migratedWorkflowCase `json:"cases"`
		} `json:"migratedWorkflows"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.MigratedWorkflows.Cases) == 0 || len(fixture.MigratedWorkflows.Request) == 0 {
		t.Fatal("contracts.json must predeclare migrated workflow cases")
	}
	request := fixture.MigratedWorkflows.Request
	for _, tc := range fixture.MigratedWorkflows.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			oldProvider := newProvider(t)
			old := runOldJSONWorkflow(t, oldProvider.URL, tc.Source, request)
			oldCalls, oldEffects := oldProvider.ledger.snapshot()
			wantOld := tc.Expected.Output
			if !tc.Expected.Supported {
				wantOld = tc.Expected.OldOutput
			}
			if !old.OK || !equalJSON(old.Response, wantOld) || oldCalls != tc.Expected.ProviderCalls || oldEffects != tc.Expected.CommittedEffects {
				t.Fatalf("published runner on the source workflow: ok=%t output=%s error=%+v calls=%d effects=%d; want output=%s calls=%d effects=%d", old.OK, old.Response, old.Error, oldCalls, oldEffects, wantOld, tc.Expected.ProviderCalls, tc.Expected.CommittedEffects)
			}

			newProvider := newProvider(t)
			nodes, inventory := migratedNativeNodes(t, newProvider.URL)
			document, err := migration.Convert(tc.Source, inventory)
			if !tc.Expected.Supported {
				var diagnostic migration.Diagnostic
				if err == nil || !errors.As(err, &diagnostic) || diagnostic.Code != tc.Expected.Code || diagnostic.Path != tc.Expected.Path || diagnostic.Remediation == "" {
					t.Fatalf("Convert() = %v; want actionable %s at %s for a source the old runner executes", err, tc.Expected.Code, tc.Expected.Path)
				}
				if len(document.Workflow.Instructions) != 0 {
					t.Fatal("Convert() returned a document alongside its refusal")
				}
				t.Logf("E20-T01 migration refusal case=%s old_output=%s diagnostic=%s", tc.ID, old.Response, diagnostic.Error())
				return
			}
			if err != nil {
				t.Fatalf("Convert() = %v", err)
			}
			compiled, err := compile.Compile(document)
			if err != nil {
				t.Fatalf("compile converted document: %v", err)
			}
			// The converted workflow's input is the value the old `@trigger`
			// root resolved to: the same request envelope the old harness built.
			envelope := map[string]any{"body": request, "headers": map[string]any{}, "query": map[string]any{}, "params": map[string]any{}, "method": "POST", "url": "/parity/json"}
			result, err := engine.New(nodes).Run(context.Background(), compiled.Program, envelope)
			if err != nil {
				t.Fatalf("native engine on converted workflow: %v", err)
			}
			newOutput, err := json.Marshal(result.Output)
			if err != nil {
				t.Fatal(err)
			}
			newCalls, newEffects := newProvider.ledger.snapshot()
			if !equalJSON(newOutput, tc.Expected.Output) || !equalJSON(newOutput, old.Response) || newCalls != oldCalls || newEffects != oldEffects {
				t.Fatalf("converted workflow output=%s calls=%d effects=%d; old output=%s calls=%d effects=%d; predeclared %s", newOutput, newCalls, newEffects, old.Response, oldCalls, oldEffects, tc.Expected.Output)
			}
			t.Logf("E20-T01 migrated execution case=%s workflow=%s old=%s new=%s calls=%d effects=%d", tc.ID, document.Workflow.ID, old.Response, newOutput, newCalls, newEffects)
		})
	}
}

func runOldJSONWorkflow(t *testing.T, providerURL string, source json.RawMessage, payload map[string]any) runResult {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"mode": "json-workflow", "workflow": source, "payload": payload})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "node", "run.mjs")
	command.Dir = "old-engine"
	command.Env = append(os.Environ(), "BLOK_PARITY_PROVIDER_URL="+providerURL)
	command.Stdin = bytes.NewReader(encoded)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil {
		t.Fatalf("published runner JSON workflow failed: %v\nstderr: %s\nstdout: %s", err, stderr.String(), stdout.String())
	}
	var result runResult
	if err := json.Unmarshal(bytes.TrimSpace(stdout.Bytes()), &result); err != nil {
		t.Fatalf("decode published runner output %q: %v", stdout.String(), err)
	}
	return result
}

// migratedNativeNodes returns native nodes matching the old JSON harness's
// node names and the converter inventory that pins their schemas.
func migratedNativeNodes(t *testing.T, providerURL string) (map[string]node.Any, map[string]contract.NodeDescriptor) {
	t.Helper()
	client := persistentHTTPClient()
	quoteInput, quoteResponse := schemasForOperation("quote")
	var quoteBody map[string]any
	var response struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(quoteResponse, &response); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(response.Properties["body"], &quoteBody); err != nil {
		t.Fatal(err)
	}
	quoteBodySchema, err := json.Marshal(quoteBody)
	if err != nil {
		t.Fatal(err)
	}
	var quoteInputSchema map[string]any
	if err := json.Unmarshal(quoteInput, &quoteInputSchema); err != nil {
		t.Fatal(err)
	}
	object := map[string]any{"type": "object"}
	envelopeSchema, err := json.Marshal(map[string]any{
		"type":       "object",
		"properties": map[string]any{"body": quoteInputSchema, "headers": object, "query": object, "params": object, "method": map[string]any{"type": "string"}, "url": map[string]any{"type": "string"}},
		"required":   []string{"body", "headers", "query", "params", "method", "url"},
	})
	if err != nil {
		t.Fatal(err)
	}
	priceOutput := []byte(`{"type":"object","properties":{"sku":{"type":"string"},"totalCents":{"type":"integer"},"display":{"type":"string"}},"required":["sku","totalCents","display"]}`)
	quote := func(ctx context.Context, body map[string]any) (map[string]any, error) {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/quote", bytes.NewReader(encoded))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Content-Type", "application/json")
		key, _ := body["requestKey"].(string)
		request.Header.Set("idempotency-key", key)
		response, err := client.Do(request)
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		var decoded map[string]any
		if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
			return nil, err
		}
		if response.StatusCode >= http.StatusBadRequest {
			code, _ := decoded["code"].(string)
			return nil, &node.DomainError{Code: code, Class: "provider", Err: fmt.Errorf("synthetic provider status %d", response.StatusCode)}
		}
		return map[string]any{"status": response.StatusCode, "body": decoded}, nil
	}
	catalogRequest, err := node.Define[map[string]any, map[string]any]("catalog-request", "1.0.0", func(ctx context.Context, envelope map[string]any) (map[string]any, error) {
		body, _ := envelope["body"].(map[string]any)
		return quote(ctx, body)
	}, node.Description("Quotes the SKU in a request envelope's body"), node.Schemas(envelopeSchema, quoteResponse), node.Effects("network"))
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := node.Define[map[string]any, map[string]any]("catalog", "1.0.0", quote, node.Description("Quotes a SKU"), node.Schemas(quoteInput, quoteResponse), node.Effects("network"))
	if err != nil {
		t.Fatal(err)
	}
	price, err := node.Define[map[string]any, map[string]any]("price", "1.0.0", func(_ context.Context, quote map[string]any) (map[string]any, error) {
		return map[string]any{"sku": quote["sku"], "totalCents": quote["totalCents"], "display": fmt.Sprintf("%v %v", quote["totalCents"], quote["currency"])}, nil
	}, node.Description("Formats a provider quote for display"), node.Schemas(quoteBodySchema, priceOutput), node.Pure())
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[string]node.Any{"catalog": catalog.Any(), "catalog-request": catalogRequest.Any(), "price": price.Any()}
	inventory := map[string]contract.NodeDescriptor{}
	for id, definition := range nodes {
		descriptor := definition.Descriptor()
		inventory[id] = contract.NodeDescriptor{ID: id, Version: descriptor.Version, Digest: runtimecontract.CanonicalDigest(descriptor.InputSchema), InputSchema: descriptor.InputSchema, OutputSchema: descriptor.OutputSchema}
	}
	return nodes, inventory
}
