package otel_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/otel"
)

// Synthetic sentinels: none of them may ever reach a collector.
const (
	principalSentinel = "principal-synthetic-sentinel"
	payloadSentinel   = "synthetic-payload-sentinel"
	cardTokenSentinel = "synthetic-card-token-sentinel"
)

type orderInput struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Note     string `json:"note,omitempty"`
}

type orderOutput struct {
	TotalCents int64  `json:"totalCents"`
	Receipt    string `json:"receipt"`
}

var (
	orderInputSchema  = []byte(`{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100},"note":{"type":"string"}},"required":["sku","quantity"]}`)
	orderOutputSchema = []byte(`{"type":"object","properties":{"totalCents":{"type":"integer"},"receipt":{"type":"string"}},"required":["totalCents","receipt"]}`)
)

// harness is one application composed with the exporter: a validate step,
// an effectful charge step and the workflow output.
type harness struct {
	collector *collector
	exporter  *otel.Exporter
	app       *app.Application
	runner    *execution.Runner
	program   contract.InternalProgram
	effects   atomic.Int64
	charges   atomic.Int64
}

type harnessOptions struct {
	ratio    float64
	signals  signals
	tune     func(*otel.Config)
	observer func(*otel.Exporter) inspection.Observer
}

func newHarness(t *testing.T, options harnessOptions) *harness {
	t.Helper()
	h := &harness{collector: newCollector(t)}
	if options.signals == (signals{}) {
		options.signals = allSignals
	}
	config := h.collector.exporters(t, options.signals)
	config.TenantLabels = []string{"tenant-a"}
	config.LogAttributes = []string{"sku"}
	if options.tune != nil {
		options.tune(&config)
	}
	exporter, err := otel.New(config)
	if err != nil {
		t.Fatal(err)
	}
	h.exporter = exporter
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = exporter.Shutdown(ctx)
	})
	validate, charge, program := orderDefinitions(t, h)
	h.program = program
	var observer inspection.Observer = exporter
	if options.observer != nil {
		observer = options.observer(exporter)
	}
	h.app, err = app.New(app.Config{Workflows: []app.Workflow{{Name: "shop/order"}}, Inspection: observer, Trace: observe.TracePolicy{Ratio: options.ratio},
		Dependencies: []app.Dependency{{Name: "telemetry", Start: func(context.Context) error { return nil }, Close: exporter.Shutdown}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := h.app.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	h.runner = execution.NewRunner(h.app, map[string]node.Any{"shop/validate": validate.Any(), "shop/charge": charge.Any()})
	return h
}

func (h *harness) run(t *testing.T, runID, tenant string, input orderInput) (execution.Result, error) {
	t.Helper()
	return h.runner.Run(context.Background(), h.program, input, inspection.Invocation{RunID: runID, Principal: principalSentinel, Tenant: tenant})
}

func (h *harness) flush(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.exporter.Flush(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

// orderDefinitions defines the synthetic order workflow: validate, then an
// effectful charge whose effects and dispatches h counts.
func orderDefinitions(t *testing.T, h *harness) (node.Definition[orderInput, orderInput], node.Definition[orderInput, orderOutput], contract.InternalProgram) {
	t.Helper()
	validate := node.MustDefine("shop/validate", "1.0.0", func(_ context.Context, in orderInput) (orderInput, error) { return in, nil },
		node.Description("Synthetic order validation"), node.Schemas(orderInputSchema, orderInputSchema), node.Pure())
	charge := node.MustDefine("shop/charge", "1.0.0", func(ctx context.Context, in orderInput) (orderOutput, error) {
		h.charges.Add(1)
		node.Logger(ctx).Info("charging", "sku", in.SKU, "card_token", cardTokenSentinel)
		switch in.SKU {
		case "declined":
			return orderOutput{}, &node.DomainError{Code: "card_declined", Class: "business"}
		case "uncertain":
			h.effects.Add(1)
			return orderOutput{}, &node.DomainError{Code: "charge_unconfirmed", Class: "uncertain", Uncertain: true}
		}
		h.effects.Add(1)
		return orderOutput{TotalCents: int64(in.Quantity) * 1500, Receipt: "receipt-" + in.SKU}, nil
	}, node.Description("Synthetic effectful charge"), node.Schemas(orderInputSchema, orderOutputSchema), node.Effects("payments:charge"))
	definition, err := flow.Define(flow.Spec{Name: "shop/order", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[orderInput]) flow.Ref[orderOutput] {
		return flow.Call(b, "charge", charge, flow.Call(b, "validate", validate, in))
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := definition.Lower()
	if err != nil {
		t.Fatal(err)
	}
	return validate, charge, program
}
