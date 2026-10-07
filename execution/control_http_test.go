package execution_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
)

type quoteInput struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type quote struct {
	TotalCents int    `json:"totalCents"`
	Lane       string `json:"lane,omitempty"`
}

// TestBranchingWorkflowOverHTTP is the `blok new` starter's composition
// (internal/scaffold appSource) with a branching workflow: the application,
// the lowered flow program, execution.Runner and the HTTP trigger, as an
// application wires them. Large orders take the review lane (#333).
func TestBranchingWorkflowOverHTTP(t *testing.T) {
	schemas := node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))
	calculate := node.MustDefine("app/calculate-quote", "1.0.0", func(_ context.Context, in quoteInput) (quote, error) {
		return quote{TotalCents: 1500 * in.Quantity}, nil
	}, node.Description("prices an order"), schemas)
	review := node.MustDefine("app/review", "1.0.0", func(_ context.Context, in quote) (quote, error) {
		return quote{TotalCents: in.TotalCents, Lane: "review"}, nil
	}, node.Description("sends a large order to review"), schemas)
	express := node.MustDefine("app/express", "1.0.0", func(_ context.Context, in quote) (quote, error) {
		return quote{TotalCents: in.TotalCents, Lane: "express"}, nil
	}, node.Description("ships a small order"), schemas)
	workflow, err := flow.Define(flow.Spec{Name: "app/quote", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[quoteInput]) flow.Ref[quote] {
		priced := flow.Call(b, "calculate", calculate, in)
		large := flow.Compare(b, "large", "gte", flow.Select[quote, int](priced, "totalCents"), flow.Lit(10000))
		return flow.If(b, "route", large,
			func(arm *flow.ArmBuilder) flow.Ref[quote] { return flow.ArmCall(arm, "review", review, priced) },
			func(arm *flow.ArmBuilder) flow.Ref[quote] { return flow.ArmCall(arm, "express", express, priced) })
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := workflow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{
		Workflows: []app.Workflow{{Name: "app/quote"}},
		Routes:    []app.Route{{Method: http.MethodPost, Path: "/quotes", Workflow: "app/quote"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{"app/calculate-quote": calculate.Any(), "app/review": review.Any(), "app/express": express.Any()})
	handler, err := blokhttp.New(application, []blokhttp.Endpoint{{
		Method: http.MethodPost, Path: "/quotes", InputSchema: []byte(`{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1}},"required":["sku","quantity"]}`),
		Handle: func(ctx context.Context, input blokhttp.Input) (any, error) {
			var request quoteInput
			if err := json.Unmarshal(input.Body, &request); err != nil {
				return nil, err
			}
			result, err := runner.Run(ctx, program, request, inspection.Invocation{})
			if err != nil {
				return nil, err
			}
			return result.Output, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	for _, test := range []struct{ body, want string }{
		{`{"sku":"coffee","quantity":2}`, `{"totalCents":3000,"lane":"express"}`},
		{`{"sku":"coffee","quantity":7}`, `{"totalCents":10500,"lane":"review"}`},
	} {
		response, err := http.Post(server.URL+"/quotes", "application/json", strings.NewReader(test.body))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != test.want {
			t.Fatalf("%s: status=%d body=%s; want 200 %s", test.body, response.StatusCode, body, test.want)
		}
	}
}

type orderInput struct {
	Lines []quoteInput `json:"lines"`
}

// TestLoopWorkflowOverHTTP is the starter's composition with a loop: the
// workflow prices every line of the posted order, four at a time, reading
// the lines straight from the workflow input (#333 slice 1b).
func TestLoopWorkflowOverHTTP(t *testing.T) {
	schemas := node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))
	calculate := node.MustDefine("app/calculate-quote", "1.0.0", func(_ context.Context, in quoteInput) (quote, error) {
		return quote{TotalCents: 1500 * in.Quantity}, nil
	}, node.Description("prices an order line"), schemas)
	workflow, err := flow.Define(flow.Spec{Name: "app/order", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[orderInput]) flow.Ref[[]quote] {
		return flow.Each(b, "lines", flow.Select[orderInput, []quoteInput](in, "lines"), 4, func(arm *flow.ArmBuilder, line flow.Ref[quoteInput]) flow.Ref[quote] {
			return flow.ArmCall(arm, "price", calculate, line)
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := workflow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{
		Workflows: []app.Workflow{{Name: "app/order"}},
		Routes:    []app.Route{{Method: http.MethodPost, Path: "/orders", Workflow: "app/order"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{"app/calculate-quote": calculate.Any()})
	handler, err := blokhttp.New(application, []blokhttp.Endpoint{{
		Method: http.MethodPost, Path: "/orders", InputSchema: []byte(`{"type":"object","properties":{"lines":{"type":"array","items":{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"}},"required":["sku","quantity"]}}},"required":["lines"]}`),
		Handle: func(ctx context.Context, input blokhttp.Input) (any, error) {
			var request orderInput
			if err := json.Unmarshal(input.Body, &request); err != nil {
				return nil, err
			}
			result, err := runner.Run(ctx, program, request, inspection.Invocation{})
			if err != nil {
				return nil, err
			}
			return result.Output, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = application.Shutdown(context.Background()) })
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	for _, test := range []struct{ body, want string }{
		{`{"lines":[{"sku":"coffee","quantity":2},{"sku":"tea","quantity":1},{"sku":"cake","quantity":3}]}`, `[{"totalCents":3000},{"totalCents":1500},{"totalCents":4500}]`},
		{`{"lines":[]}`, `[]`},
	} {
		response, err := http.Post(server.URL+"/orders", "application/json", strings.NewReader(test.body))
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK || strings.TrimSpace(string(body)) != test.want {
			t.Fatalf("%s: status=%d body=%s; want 200 %s", test.body, response.StatusCode, body, test.want)
		}
	}
}
