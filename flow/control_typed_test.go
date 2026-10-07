package flow_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

// Review R round 1 on #379: every other control test passes map[string]any
// between nodes. Real nodes are typed Go functions, and a Ref[T] must reach
// them as a T whichever way a construct produced it.

type quantityIn struct {
	Quantity int `json:"quantity"`
}

type orderOut struct {
	Coupon   *string     `json:"coupon"`
	Tags     []string    `json:"tags"`
	Limit    *int        `json:"limit"`
	Shipping *quantityIn `json:"shipping"`
	Count    int         `json:"count"`
}

type typedNodes struct {
	order    node.Definition[orderOut, orderOut]
	coupon   node.Definition[string, string]
	double   node.Definition[int, int]
	quantity node.Definition[quantityIn, int]
}

func newTypedNodes(t *testing.T) typedNodes {
	t.Helper()
	any := []byte(`{"type":"object"}`)
	return typedNodes{
		// order passes the order through; its optional fields are typed
		// nil pointers, a nil slice, or set.
		order: node.MustDefine("order", "1.0.0", func(_ context.Context, in orderOut) (orderOut, error) { return in, nil },
			node.Description("order"), node.Schemas(any, any)),
		coupon: node.MustDefine("coupon", "1.0.0", func(_ context.Context, in string) (string, error) { return "applied:" + in, nil },
			node.Description("coupon"), node.Schemas([]byte(`{"type":"string"}`), []byte(`{"type":"string"}`))),
		double: node.MustDefine("double", "1.0.0", func(_ context.Context, in int) (int, error) { return in * 2, nil },
			node.Description("double"), node.Schemas([]byte(`{"type":"integer"}`), []byte(`{"type":"integer"}`))),
		quantity: node.MustDefine("quantity", "1.0.0", func(_ context.Context, in quantityIn) (int, error) { return in.Quantity, nil },
			node.Description("quantity"), node.Schemas([]byte(`{"type":"object","properties":{"quantity":{"type":"integer"}}}`), []byte(`{"type":"integer"}`))),
	}
}

func runTyped[O any](t *testing.T, n typedNodes, definition flow.Definition[orderOut, O], input orderOut) (any, error) {
	t.Helper()
	program, err := definition.Lower()
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: controlSpec.Name}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	registry := map[string]node.Any{"order": n.order.Any(), "coupon": n.coupon.Any(), "double": n.double.Any(), "quantity": n.quantity.Any()}
	result, err := execution.NewRunner(application, registry).Run(context.Background(), program, input, inspection.Invocation{})
	return result.Output, err
}

// A typed nil (*string, []string, *int, *struct) is absent: its JSON form
// is null, so Default falls back.
func TestDefaultFallsBackOnTypedNil(t *testing.T) {
	n := newTypedNodes(t)
	coupon := "SAVE10"
	limit := 7
	couponFlow := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[string] {
		order := flow.Call(b, "order", n.order, in)
		return flow.Call(b, "apply", n.coupon, flow.Default(b, "pick", flow.Select[orderOut, string](order, "coupon"), flow.Lit("FALLBACK")))
	})
	limitFlow := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[int] {
		order := flow.Call(b, "order", n.order, in)
		return flow.Call(b, "apply", n.double, flow.Default(b, "pick", flow.Select[orderOut, int](order, "limit"), flow.Lit(5)))
	})
	shippingFlow := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[int] {
		order := flow.Call(b, "order", n.order, in)
		return flow.Call(b, "apply", n.quantity, flow.Default(b, "pick", flow.Select[orderOut, quantityIn](order, "shipping"), flow.Lit(quantityIn{Quantity: 3})))
	})
	tagsFlow := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[[]string] {
		order := flow.Call(b, "order", n.order, in)
		return flow.Default(b, "pick", flow.Select[orderOut, []string](order, "tags"), flow.Lit([]string{"none"}))
	})
	for name, tc := range map[string]struct {
		run  func() (any, error)
		want any
	}{
		"nil *string falls back":    {func() (any, error) { return runTyped(t, n, couponFlow, orderOut{}) }, "applied:FALLBACK"},
		"set *string is kept":       {func() (any, error) { return runTyped(t, n, couponFlow, orderOut{Coupon: &coupon}) }, "applied:SAVE10"},
		"nil *int falls back":       {func() (any, error) { return runTyped(t, n, limitFlow, orderOut{}) }, 10},
		"set *int is kept":          {func() (any, error) { return runTyped(t, n, limitFlow, orderOut{Limit: &limit}) }, 14},
		"nil *struct falls back":    {func() (any, error) { return runTyped(t, n, shippingFlow, orderOut{}) }, 3},
		"set *struct reaches a T":   {func() (any, error) { return runTyped(t, n, shippingFlow, orderOut{Shipping: &quantityIn{Quantity: 9}}) }, 9},
		"nil []string falls back":   {func() (any, error) { return runTyped(t, n, tagsFlow, orderOut{}) }, []any{"none"}},
		"empty []string is a value": {func() (any, error) { return runTyped(t, n, tagsFlow, orderOut{Tags: []string{}}) }, []string{}},
	} {
		output, err := tc.run()
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !reflect.DeepEqual(output, tc.want) {
			t.Errorf("%s: output=%#v want %#v", name, output, tc.want)
		}
	}
}

// A literal reaches a typed node as the node's own input type: an int
// literal is an int, a struct literal a struct.
func TestLiteralOperandsReachTypedNodes(t *testing.T) {
	n := newTypedNodes(t)
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[int] {
		order := flow.Call(b, "order", n.order, in)
		big := flow.Compare(b, "big", "gt", flow.Select[orderOut, int](order, "count"), flow.Lit(10))
		return flow.If(b, "route", big,
			func(arm *flow.ArmBuilder) flow.Ref[int] {
				return flow.ArmCall(arm, "many", n.quantity, flow.Default(arm.Builder(), "fixed", flow.Select[orderOut, quantityIn](order, "shipping"), flow.Lit(quantityIn{Quantity: 100})))
			},
			func(arm *flow.ArmBuilder) flow.Ref[int] {
				return flow.ArmCall(arm, "few", n.double, flow.Default(arm.Builder(), "one", flow.Select[orderOut, int](order, "limit"), flow.Lit(1)))
			})
	})
	for count, want := range map[int]int{50: 100, 2: 2} {
		output, err := runTyped(t, n, definition, orderOut{Count: count})
		if err != nil || output != want {
			t.Errorf("count %d: output=%#v err=%v; want %d", count, output, err, want)
		}
	}
}
