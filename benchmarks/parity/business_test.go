package parity

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/internal/compile"
	"github.com/well-prado/new-blok/node"
)

// The business-logic workloads compute every business value in workflow
// nodes: the provider only returns catalog unit prices and records effects.
// These native nodes mirror parity-business-* in old-engine/run.mjs; the two
// implementations are written independently against the same rules:
//
//	subtotal = unit × quantity
//	discount = ⌊subtotal / 10⌋ when quantity ≥ 10, else 0
//	tax      = ⌊((subtotal − discount) × 825 + 5000) / 10000⌋   (8.25 %, half up)
//	total    = subtotal − discount + tax
//
// and quantity must be an integer in [1, 100] (invalid_quantity).

const (
	businessMaxQuantity    = 100
	businessBulkQuantity   = 10
	businessTaxBasisPoints = 825
)

type businessPricing struct {
	Subtotal, Discount, Tax, Total int64
}

func priceBusinessLine(unit, quantity int64) (businessPricing, error) {
	if quantity < 1 || quantity > businessMaxQuantity {
		return businessPricing{}, &node.DomainError{Code: "invalid_quantity", Class: "validation", Err: errors.New("quantity must be between 1 and 100")}
	}
	subtotal := unit * quantity
	var discount int64
	if quantity >= businessBulkQuantity {
		discount = subtotal / 10
	}
	taxable := subtotal - discount
	tax := (taxable*businessTaxBasisPoints + 5000) / 10000
	return businessPricing{Subtotal: subtotal, Discount: discount, Tax: tax, Total: taxable + tax}, nil
}

func businessInt(value any) (int64, bool) {
	switch typed := value.(type) {
	case int:
		return int64(typed), true
	case int64:
		return typed, true
	case float64:
		if typed != math.Trunc(typed) || math.Abs(typed) > 1<<53 {
			return 0, false
		}
		return int64(typed), true
	case json.Number:
		parsed, err := typed.Int64()
		return parsed, err == nil
	default:
		return 0, false
	}
}

func businessSchema(fields map[string]string) []byte {
	properties := map[string]any{}
	required := make([]string, 0, len(fields))
	for name, kind := range fields {
		properties[name] = map[string]any{"type": kind}
		required = append(required, name)
	}
	encoded, err := json.Marshal(map[string]any{"type": "object", "properties": properties, "required": required})
	if err != nil {
		panic(err)
	}
	return encoded
}

func businessFields(base map[string]string, extra ...string) map[string]string {
	fields := maps.Clone(base)
	for index := 0; index+1 < len(extra); index += 2 {
		fields[extra[index]] = extra[index+1]
	}
	return fields
}

// businessNativePrograms returns the native business nodes and the quote and
// order programs, compiled by the canonical document compiler with each step
// reading the previous step's whole output (flow.Lower is avoided: #244).
func businessNativePrograms(providerURL string) (map[string]node.Any, contract.InternalProgram, contract.InternalProgram, error) {
	client := persistentHTTPClient()
	line := map[string]string{"requestKey": "string", "sku": "string", "quantity": "integer"}
	looked := businessFields(line, "unitCents", "integer")
	priced := businessFields(looked, "subtotalCents", "integer", "discountCents", "integer", "taxCents", "integer", "totalCents", "integer")
	reserved := businessFields(looked, "reservationId", "string")
	pricedOrder := businessFields(priced, "reservationId", "string")
	committed := businessFields(pricedOrder, "orderId", "string", "status", "string")

	call := func(ctx context.Context, operation, key string, body map[string]any) (map[string]any, error) {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		request, err := http.NewRequestWithContext(ctx, http.MethodPost, providerURL+"/"+operation, bytes.NewReader(encoded))
		if err != nil {
			return nil, err
		}
		request.Header.Set("Content-Type", "application/json")
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
			if code == "" {
				code = "provider_error"
			}
			return nil, &node.DomainError{Code: code, Class: "provider", Err: fmt.Errorf("synthetic provider status %d", response.StatusCode)}
		}
		return decoded, nil
	}
	quantityOf := func(input map[string]any) (int64, error) {
		quantity, ok := businessInt(input["quantity"])
		if !ok {
			return 0, &node.DomainError{Code: "invalid_quantity", Class: "validation", Err: errors.New("quantity must be an integer")}
		}
		return quantity, nil
	}
	withPricing := func(input map[string]any) (map[string]any, error) {
		quantity, err := quantityOf(input)
		if err != nil {
			return nil, err
		}
		unit, ok := businessInt(input["unitCents"])
		if !ok {
			return nil, fmt.Errorf("unitCents is not an integer")
		}
		pricing, err := priceBusinessLine(unit, quantity)
		if err != nil {
			return nil, err
		}
		output := maps.Clone(input)
		output["subtotalCents"], output["discountCents"], output["taxCents"], output["totalCents"] = pricing.Subtotal, pricing.Discount, pricing.Tax, pricing.Total
		return output, nil
	}
	type spec struct {
		name, description string
		in, out           map[string]string
		handler           func(context.Context, map[string]any) (map[string]any, error)
		effects           bool
	}
	specs := []spec{
		{name: "parity-business-quote-lookup", description: "Reads the SKU's unit price from the provider catalog", in: line, out: looked, effects: true, handler: func(ctx context.Context, input map[string]any) (map[string]any, error) {
			key, _ := input["requestKey"].(string)
			entry, err := call(ctx, "catalog", key+"-catalog", map[string]any{"sku": input["sku"]})
			if err != nil {
				return nil, err
			}
			output := maps.Clone(input)
			output["unitCents"] = entry["unitCents"]
			return output, nil
		}},
		{name: "parity-business-quote-price", description: "Computes subtotal, bulk discount, tax and total", in: looked, out: priced, handler: func(_ context.Context, input map[string]any) (map[string]any, error) {
			return withPricing(input)
		}},
		{name: "parity-business-order-validate", description: "Rejects an order line before any provider call", in: line, out: line, handler: func(_ context.Context, input map[string]any) (map[string]any, error) {
			quantity, err := quantityOf(input)
			if err != nil {
				return nil, err
			}
			if quantity < 1 || quantity > businessMaxQuantity {
				return nil, &node.DomainError{Code: "invalid_quantity", Class: "validation", Err: errors.New("quantity must be between 1 and 100")}
			}
			return maps.Clone(input), nil
		}},
		{name: "parity-business-order-reserve", description: "Reserves stock and reads the reserved unit price", in: line, out: reserved, effects: true, handler: func(ctx context.Context, input map[string]any) (map[string]any, error) {
			key, _ := input["requestKey"].(string)
			reservation, err := call(ctx, "order-reserve", key+"-reserve", map[string]any{"sku": input["sku"], "quantity": input["quantity"]})
			if err != nil {
				return nil, err
			}
			output := maps.Clone(input)
			output["unitCents"], output["reservationId"] = reservation["unitCents"], reservation["reservationId"]
			return output, nil
		}},
		{name: "parity-business-order-price", description: "Computes the order's line totals", in: reserved, out: pricedOrder, handler: func(_ context.Context, input map[string]any) (map[string]any, error) {
			return withPricing(input)
		}},
		{name: "parity-business-order-commit", description: "Commits the reservation at the node-computed total", in: pricedOrder, out: committed, effects: true, handler: func(ctx context.Context, input map[string]any) (map[string]any, error) {
			key, _ := input["requestKey"].(string)
			result, err := call(ctx, "order-commit", key+"-commit", map[string]any{"reservationId": input["reservationId"], "totalCents": input["totalCents"]})
			if err != nil {
				return nil, err
			}
			output := maps.Clone(input)
			output["orderId"], output["status"] = result["orderId"], result["status"]
			return output, nil
		}},
	}
	nodes := map[string]node.Any{}
	descriptors := map[string]contract.NodeDescriptor{}
	for _, item := range specs {
		options := []node.Option{node.Description(item.description), node.Schemas(businessSchema(item.in), businessSchema(item.out))}
		if item.effects {
			options = append(options, node.Effects("network"))
		} else {
			options = append(options, node.Pure())
		}
		definition, err := node.Define[map[string]any, map[string]any](item.name, "1.0.0", item.handler, options...)
		if err != nil {
			return nil, contract.InternalProgram{}, contract.InternalProgram{}, err
		}
		nodes[item.name] = definition.Any()
		descriptor := definition.Descriptor()
		descriptors[item.name] = contract.NodeDescriptor{ID: item.name, Version: descriptor.Version, Digest: runtimecontract.CanonicalDigest(descriptor.InputSchema), InputSchema: descriptor.InputSchema, OutputSchema: descriptor.OutputSchema}
	}
	chain := func(id string, steps ...[2]string) (contract.InternalProgram, error) {
		first, last := descriptors[steps[0][1]], descriptors[steps[len(steps)-1][1]]
		document := contract.Document{Version: contract.CurrentVersion, Workflow: contract.Workflow{
			ID: id, Name: id, Version: "1.0.0", Digest: runtimecontract.CanonicalDigest([]byte("issue108-" + id + "-v1")),
			InputSchema: first.InputSchema, OutputSchema: last.OutputSchema,
		}}
		for index, step := range steps {
			instruction := contract.Instruction{ID: step[0], Kind: "call", Node: step[1]}
			if index > 0 {
				instruction.References = []contract.Reference{{Step: steps[index-1][0]}}
			}
			document.Workflow.Instructions = append(document.Workflow.Instructions, instruction)
			document.Nodes = append(document.Nodes, descriptors[step[1]])
		}
		document.Workflow.Instructions = append(document.Workflow.Instructions, contract.Instruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: steps[len(steps)-1][0]}}})
		compiled, err := compile.Compile(document)
		return compiled.Program, err
	}
	quote, err := chain("parity-business-quote", [2]string{"lookup", "parity-business-quote-lookup"}, [2]string{"price", "parity-business-quote-price"})
	if err != nil {
		return nil, contract.InternalProgram{}, contract.InternalProgram{}, err
	}
	order, err := chain("parity-business-order", [2]string{"validate", "parity-business-order-validate"}, [2]string{"reserve", "parity-business-order-reserve"}, [2]string{"price", "parity-business-order-price"}, [2]string{"commit", "parity-business-order-commit"})
	if err != nil {
		return nil, contract.InternalProgram{}, contract.InternalProgram{}, err
	}
	return nodes, quote, order, nil
}

type businessWorkload struct {
	ID       string         `json:"id"`
	Route    string         `json:"route"`
	Request  map[string]any `json:"request"`
	Expected struct {
		Output           json.RawMessage `json:"output"`
		ErrorCode        string          `json:"errorCode"`
		ProviderCalls    int             `json:"providerCalls"`
		CommittedEffects int             `json:"committedEffects"`
		CommittedTotal   *int64          `json:"committedTotalCents"`
	} `json:"expected"`
}

// TestBusinessLogicWorkflowsMatch runs quote and order workflows whose
// business result is computed by workflow nodes on each engine (catalog
// lookup -> pricing; validate -> reserve -> pricing -> commit), with data
// carried between steps, through the long-lived old Runner app and the native
// engine app. Every output, error code, provider call count, effect count
// and committed total is predeclared in contracts.json.
func TestBusinessLogicWorkflowsMatch(t *testing.T) {
	requireOldEngine(t)
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "parity", "contracts.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		BusinessWorkloads struct {
			Class     string             `json:"class"`
			Workloads []businessWorkload `json:"workloads"`
		} `json:"businessWorkloads"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.BusinessWorkloads.Class != "business-logic" || len(fixture.BusinessWorkloads.Workloads) == 0 {
		t.Fatal("contracts.json must predeclare businessWorkloads of class business-logic")
	}
	for _, side := range []struct {
		name  string
		start func(*testing.T, string) (*persistentAppProcess, time.Duration)
	}{
		{name: "published-blok-2.5.0", start: startOldPersistentApp},
		{name: "native-engine", start: startNewPersistentApp},
	} {
		t.Run(side.name, func(t *testing.T) {
			provider := newProvider(t)
			app, _ := side.start(t, provider.URL)
			client := persistentHTTPClient()
			for _, tc := range fixture.BusinessWorkloads.Workloads {
				beforeCalls, beforeEffects := provider.ledger.snapshot()
				output, code, err := postBusiness(client, app.url+tc.Route, tc.Request)
				if err != nil {
					t.Fatalf("%s %s: %v", side.name, tc.ID, err)
				}
				calls, effects := provider.ledger.snapshot()
				calls, effects = calls-beforeCalls, effects-beforeEffects
				if tc.Expected.ErrorCode != "" {
					if code != tc.Expected.ErrorCode || output != nil {
						t.Errorf("%s %s: error code=%q output=%s; want error %q", side.name, tc.ID, code, output, tc.Expected.ErrorCode)
					}
				} else if code != "" || !equalJSON(output, tc.Expected.Output) {
					t.Errorf("%s %s: output=%s code=%q; want %s", side.name, tc.ID, output, code, tc.Expected.Output)
				}
				if calls != tc.Expected.ProviderCalls || effects != tc.Expected.CommittedEffects {
					t.Errorf("%s %s: provider calls=%d effects=%d; want %d/%d", side.name, tc.ID, calls, effects, tc.Expected.ProviderCalls, tc.Expected.CommittedEffects)
				}
				if tc.Expected.CommittedTotal != nil {
					key, _ := tc.Request["requestKey"].(string)
					provider.ledger.mu.Lock()
					recorded, _ := businessInt(provider.ledger.committedTotals[key+"-commit"])
					provider.ledger.mu.Unlock()
					if recorded != *tc.Expected.CommittedTotal {
						t.Errorf("%s %s: provider recorded committed total %d, want %d", side.name, tc.ID, recorded, *tc.Expected.CommittedTotal)
					}
				}
				t.Logf("E20-T01 business engine=%s case=%s output=%s code=%q calls=%d effects=%d", side.name, tc.ID, output, code, calls, effects)
			}
		})
	}
}

// postBusiness returns the business output, or the error code of a failed run.
func postBusiness(client *http.Client, endpoint string, request map[string]any) (json.RawMessage, string, error) {
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, "", err
	}
	response, err := client.Post(endpoint, "application/json", bytes.NewReader(encoded))
	if err != nil {
		return nil, "", err
	}
	defer response.Body.Close()
	var envelope struct {
		OK       bool            `json:"ok"`
		Response json.RawMessage `json:"response"`
		Code     *string         `json:"code"`
		Error    *string         `json:"error"`
	}
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		return nil, "", fmt.Errorf("status %d: %w", response.StatusCode, err)
	}
	if envelope.OK && response.StatusCode == http.StatusOK {
		return envelope.Response, "", nil
	}
	if envelope.Code == nil || *envelope.Code == "" {
		message := ""
		if envelope.Error != nil {
			message = *envelope.Error
		}
		return nil, "", fmt.Errorf("status %d failed run carries no error code: %s", response.StatusCode, message)
	}
	return nil, *envelope.Code, nil
}

func TestPriceBusinessLineRules(t *testing.T) {
	for _, test := range []struct {
		unit, quantity int64
		want           businessPricing
		wantErr        bool
	}{
		{unit: 1500, quantity: 2, want: businessPricing{Subtotal: 3000, Discount: 0, Tax: 248, Total: 3248}},
		{unit: 900, quantity: 9, want: businessPricing{Subtotal: 8100, Discount: 0, Tax: 668, Total: 8768}},
		{unit: 900, quantity: 10, want: businessPricing{Subtotal: 9000, Discount: 900, Tax: 668, Total: 8768}},
		{unit: 900, quantity: 12, want: businessPricing{Subtotal: 10800, Discount: 1080, Tax: 802, Total: 10522}},
		{unit: 1500, quantity: 0, wantErr: true},
		{unit: 1500, quantity: 101, wantErr: true},
	} {
		got, err := priceBusinessLine(test.unit, test.quantity)
		if (err != nil) != test.wantErr || got != test.want {
			t.Errorf("priceBusinessLine(%d, %d) = %+v, %v; want %+v err=%t", test.unit, test.quantity, got, err, test.want, test.wantErr)
		}
	}
}
