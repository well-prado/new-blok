package flow_test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
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

// Review R round 2 on #379: a call reads the value its reference names the
// same way in a control program as in a call-only one. Only a value a
// control construct produced is converted to the node's type, and never
// into a value the source did not hold.

type pShip struct {
	Quantity int `json:"quantity"`
}

type pOther struct {
	Zip string `json:"zip"`
}

// secret redacts itself when encoded; read through a pointer it is not a
// string, whatever its encoding says.
type secret string

func (secret) MarshalJSON() ([]byte, error) { return []byte(`"***"`), nil }

type sourceOut struct {
	Coupon *string `json:"coupon"`
	Ship   *pShip  `json:"ship"`
	Other  pOther  `json:"other"`
	Secret *secret `json:"secret"`
	Limit  *int    `json:"limit"`
}

func TestControlProgramsReadReferencesLikeCallOnlyPrograms(t *testing.T) {
	object := []byte(`{"type":"object"}`)
	hidden, limit := secret("token"), 7
	source := node.MustDefine("source", "1.0.0", func(context.Context, sourceOut) (sourceOut, error) {
		return sourceOut{Other: pOther{Zip: "12345"}, Secret: &hidden, Limit: &limit}, nil
	}, node.Description("source"), node.Schemas(object, object))
	var received []any
	text := node.MustDefine("text", "1.0.0", func(_ context.Context, in string) (string, error) { received = append(received, in); return in, nil },
		node.Description("text"), node.Schemas([]byte(`{"type":"string"}`), []byte(`{"type":"string"}`)))
	ship := node.MustDefine("ship", "1.0.0", func(_ context.Context, in pShip) (int, error) {
		received = append(received, in)
		return in.Quantity, nil
	},
		node.Description("ship"), node.Schemas([]byte(`{"type":"object","properties":{"quantity":{"type":"integer"}},"required":["quantity"]}`), []byte(`{"type":"integer"}`)))
	number := node.MustDefine("number", "1.0.0", func(_ context.Context, in int) (int, error) { received = append(received, in); return in, nil },
		node.Description("number"), node.Schemas([]byte(`{"type":"integer"}`), []byte(`{"type":"integer"}`)))
	registry := map[string]node.Any{"source": source.Any(), "text": text.Any(), "ship": ship.Any(), "number": number.Any()}
	run := func(control bool, field string, target string) (string, []any) {
		received = nil
		definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[sourceOut]) flow.Ref[sourceOut] {
			read := flow.Call(b, "source", source, in)
			if control {
				flow.Compare(b, "unrelated", "eq", flow.Lit(1), flow.Lit(1))
			}
			switch target {
			case "text":
				flow.Call(b, "use", text, flow.Select[sourceOut, string](read, field))
			case "number":
				flow.Call(b, "use", number, flow.Select[sourceOut, int](read, field))
			default:
				flow.Call(b, "use", ship, flow.Select[sourceOut, pShip](read, field))
			}
			return read
		})
		program, err := definition.Lower()
		if err != nil {
			t.Fatal(err)
		}
		application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: controlSpec.Name}}})
		if err != nil {
			t.Fatal(err)
		}
		if err := application.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		_, err = execution.NewRunner(application, registry).Run(context.Background(), program, sourceOut{}, inspection.Invocation{})
		return fmt.Sprint(err), received
	}
	for _, tc := range []struct{ field, target, want string }{
		{"coupon", "text", "null_not_allowed"},
		{"ship", "ship", "null_not_allowed"},
		{"other", "ship", "unknown_field at $.zip"},
		{"secret", "text", "input_type_mismatch"},
		// A node's own *int output is not converted: only a construct's.
		{"limit", "number", "input_type_mismatch: expected int, got *int"},
	} {
		callOnly, callOnlyReceived := run(false, tc.field, tc.target)
		withControl, controlReceived := run(true, tc.field, tc.target)
		if !strings.Contains(callOnly, tc.want) {
			t.Fatalf("%s → %s call-only: %s; want %s", tc.field, tc.target, callOnly, tc.want)
		}
		if withControl != callOnly || !reflect.DeepEqual(controlReceived, callOnlyReceived) {
			t.Errorf("%s → %s: call-only %q received %v; with a control instruction %q received %v", tc.field, tc.target, callOnly, callOnlyReceived, withControl, controlReceived)
		}
	}
}

// A construct's value is converted only when it has an exact reading as
// the node's type: a JSON value with a field the type lacks, or null for a
// type that cannot be nil, reaches the node unconverted and is refused with
// the node's own input_type_mismatch, naming the type.
func TestConstructValuesAreNeverInventedForTypedNodes(t *testing.T) {
	n := newTypedNodes(t)
	permissive := node.MustDefine("quantity", "1.0.0", func(_ context.Context, in quantityIn) (int, error) { return in.Quantity, nil },
		node.Description("quantity"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"integer"}`)))
	n.quantity = permissive
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[int] {
		order := flow.Call(b, "order", n.order, in)
		return flow.Call(b, "apply", n.quantity, flow.Default(b, "pick", flow.Select[orderOut, quantityIn](order, "shipping"), flow.Ref[quantityIn](flow.Lit(map[string]any{"quantity": 3, "zip": "12345"}))))
	})
	_, err := runTyped(t, n, definition, orderOut{})
	if err == nil || !strings.Contains(err.Error(), "input_type_mismatch: expected flow_test.quantityIn, got map[string]interface {}") {
		t.Fatalf("err=%v; want the node's input_type_mismatch naming its type", err)
	}
	nullable := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[int] {
		order := flow.Call(b, "order", n.order, in)
		return flow.Call(b, "apply", n.quantity, flow.Default(b, "pick", flow.Select[orderOut, quantityIn](order, "shipping"), flow.Ref[quantityIn](flow.Lit[any](nil))))
	})
	if _, err := runTyped(t, n, nullable, orderOut{}); err == nil || !strings.Contains(err.Error(), "input_type_mismatch: expected flow_test.quantityIn, got <nil>") {
		t.Fatalf("null into a struct: err=%v; want input_type_mismatch, not a zero struct", err)
	}
}

// Review R round 3 on #379: the node's input schema checks a construct's
// value as resolved, before it is converted to the node's type. A literal
// missing a required field must be refused, not filled in with the field's
// Go zero value by the conversion and then accepted.

type zipIn struct {
	Quantity int    `json:"quantity"`
	Zip      string `json:"zip"`
}

func TestSchemaChecksConstructValueBeforeConversion(t *testing.T) {
	n := newTypedNodes(t)
	ran := false
	zip := node.MustDefine("zip", "1.0.0", func(_ context.Context, in zipIn) (int, error) { ran = true; return in.Quantity, nil },
		node.Description("zip"), node.Schemas([]byte(`{"type":"object","properties":{"quantity":{"type":"integer"},"zip":{"type":"string"}},"required":["quantity","zip"]}`), []byte(`{"type":"integer"}`)))
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[orderOut]) flow.Ref[int] {
		order := flow.Call(b, "order", n.order, in)
		return flow.Call(b, "apply", zip, flow.Default(b, "pick", flow.Select[orderOut, zipIn](order, "shipping"), flow.Ref[zipIn](flow.Lit(map[string]any{"quantity": 3}))))
	})
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
	registry := map[string]node.Any{"order": n.order.Any(), "zip": zip.Any()}
	result, err := execution.NewRunner(application, registry).Run(context.Background(), program, orderOut{}, inspection.Invocation{})
	if err == nil || !strings.HasPrefix(err.Error(), "invalid_input: ") || !strings.Contains(err.Error(), "zip") {
		t.Fatalf("err=%v output=%#v; want invalid_input naming the missing zip", err, result.Output)
	}
	t.Logf("refused: %v", err)
	if ran {
		t.Fatal("the node ran on a value its schema refuses")
	}
	for _, step := range result.Steps {
		if step.ID == "apply" && step.Executed {
			t.Fatalf("step apply executed: %+v", step)
		}
	}
}
