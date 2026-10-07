package flow_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

// #333 slice 1: control flow lowers and runs through the public runner.
// Every test here authors the workflow with the public flow API, lowers it
// with Definition.Lower and runs it with execution.Runner, as an
// application does.

var controlSpec = flow.Spec{Name: "control", Version: "1.0.0", Durability: flow.Memory}

type controlNodes struct {
	price, vip, standard, charge, release node.Definition[object, object]
	released                              *atomic.Int32
}

func newControlNodes(t *testing.T) controlNodes {
	t.Helper()
	define := func(name string, run func(object) (object, error)) node.Definition[object, object] {
		return node.MustDefine(name, "1.0.0", func(_ context.Context, in object) (object, error) { return run(in) },
			node.Description(name), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)))
	}
	released := &atomic.Int32{}
	return controlNodes{
		// price totals the order; it passes a coupon through when the
		// order has one, so a default instruction sees both shapes.
		price: define("price", func(in object) (object, error) {
			out := object{"sku": in["sku"], "total": quantity(in) * 3}
			if coupon, ok := in["coupon"]; ok {
				out["coupon"] = coupon
			}
			return out, nil
		}),
		vip:      define("vip", func(in object) (object, error) { return object{"lane": "vip", "total": in["total"]}, nil }),
		standard: define("standard", func(in object) (object, error) { return object{"lane": "standard", "total": in["total"]}, nil }),
		charge: define("charge", func(in object) (object, error) {
			if in["sku"] == "broken" {
				return nil, errors.New("card declined")
			}
			return object{"charged": in["sku"]}, nil
		}),
		release:  define("release", func(object) (object, error) { released.Add(1); return object{"released": true}, nil }),
		released: released,
	}
}

func quantity(in object) int {
	switch value := in["quantity"].(type) {
	case float64:
		return int(value)
	case int:
		return value
	}
	return 0
}

func (n controlNodes) registry() map[string]node.Any {
	return map[string]node.Any{"price": n.price.Any(), "vip": n.vip.Any(), "standard": n.standard.Any(), "charge": n.charge.Any(), "release": n.release.Any()}
}

// runControl runs a lowered program through the public runner of a started
// application and returns the run's result and error.
func runControl(t *testing.T, n controlNodes, definition flow.Definition[object, object], input object) (execution.Result, error) {
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
	return execution.NewRunner(application, n.registry()).Run(context.Background(), program, input, inspection.Invocation{})
}

func stepIDs(result execution.Result) []string {
	ids := make([]string, 0, len(result.Steps))
	for _, step := range result.Steps {
		ids = append(ids, step.ID)
	}
	return ids
}

func TestIfRunsTheSelectedArmThroughThePublicRunner(t *testing.T) {
	n := newControlNodes(t)
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		priced := flow.Call(b, "price", n.price, in)
		big := flow.Compare(b, "big", "gt", flow.Select[object, int](priced, "total"), flow.Lit(100))
		return flow.If(b, "route", big,
			func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "vip", n.vip, priced) },
			func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "standard", n.standard, priced) })
	})
	for _, tc := range []struct {
		quantity int
		want     object
		steps    []string
	}{
		{50, object{"lane": "vip", "total": 150}, []string{"price", "big", "vip", "route", "output"}},
		{2, object{"lane": "standard", "total": 6}, []string{"price", "big", "standard", "route", "output"}},
	} {
		result, err := runControl(t, n, definition, object{"sku": "coffee", "quantity": tc.quantity})
		if err != nil {
			t.Fatalf("quantity %d: %v", tc.quantity, err)
		}
		if !reflect.DeepEqual(result.Output, tc.want) {
			t.Errorf("quantity %d: output=%#v want %#v", tc.quantity, result.Output, tc.want)
		}
		if got := stepIDs(result); !reflect.DeepEqual(got, tc.steps) {
			t.Errorf("quantity %d: steps=%v want %v", tc.quantity, got, tc.steps)
		}
		if result.State["big"] != (tc.quantity == 50) {
			t.Errorf("quantity %d: compare state=%v", tc.quantity, result.State["big"])
		}
	}
}

func TestChooseRunsTheMatchingCaseOrTheDefault(t *testing.T) {
	n := newControlNodes(t)
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		priced := flow.Call(b, "price", n.price, in)
		return flow.Choose(b, "lane", flow.Select[object, string](priced, "sku"), map[string]func(*flow.ArmBuilder) flow.Ref[object]{
			"coffee": func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "coffee-lane", n.vip, priced) },
			"tea":    func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "tea-lane", n.standard, priced) },
		}, func(arm *flow.ArmBuilder) flow.Ref[object] {
			return flow.ArmCall(arm, "other-lane", n.standard, priced)
		})
	})
	for sku, ran := range map[string]string{"coffee": "coffee-lane", "tea": "tea-lane", "juice": "other-lane"} {
		result, err := runControl(t, n, definition, object{"sku": sku, "quantity": 1})
		if err != nil {
			t.Fatalf("%s: %v", sku, err)
		}
		if got, want := stepIDs(result), []string{"price", ran, "lane", "output"}; !reflect.DeepEqual(got, want) {
			t.Errorf("%s: steps=%v want %v", sku, got, want)
		}
	}
}

func TestDefaultFallsBackOnlyWhenTheValueIsAbsent(t *testing.T) {
	n := newControlNodes(t)
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		priced := flow.Call(b, "price", n.price, in)
		return flow.Default(b, "coupon", flow.Select[object, object](priced, "coupon"), flow.Lit(object{"percent": 0}))
	})
	for _, tc := range []struct {
		input object
		want  any
	}{
		{object{"sku": "tea", "quantity": 1}, map[string]any{"percent": float64(0)}},
		{object{"sku": "tea", "quantity": 1, "coupon": nil}, map[string]any{"percent": float64(0)}},
		{object{"sku": "tea", "quantity": 1, "coupon": object{"percent": 10}}, map[string]any{"percent": float64(10)}},
	} {
		result, err := runControl(t, n, definition, tc.input)
		if err != nil {
			t.Fatalf("%v: %v", tc.input, err)
		}
		if got := canonical(t, result.Output); got != canonical(t, tc.want) {
			t.Errorf("%v: output=%s want %s", tc.input, got, canonical(t, tc.want))
		}
	}
}

func TestTryFinallyRunsFinallyOnSuccessAndOnFailure(t *testing.T) {
	n := newControlNodes(t)
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		return flow.TryFinally(b, "payment",
			func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "charge", n.charge, in) },
			func(arm *flow.ArmBuilder) { flow.ArmCall(arm, "release", n.release, in) })
	})
	result, err := runControl(t, n, definition, object{"sku": "coffee", "quantity": 1})
	if err != nil {
		t.Fatalf("success: %v", err)
	}
	if !reflect.DeepEqual(result.Output, object{"charged": "coffee"}) || n.released.Load() != 1 {
		t.Fatalf("success: output=%#v released=%d", result.Output, n.released.Load())
	}
	if got, want := stepIDs(result), []string{"charge", "release", "payment", "output"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("success: steps=%v want %v", got, want)
	}

	result, err = runControl(t, n, definition, object{"sku": "broken", "quantity": 1})
	var classified interface{ ErrorCode() string }
	if err == nil || !errors.As(err, &classified) || classified.ErrorCode() != "node_error" || !strings.Contains(err.Error(), "card declined") {
		t.Fatalf("failure: err=%v; want the try arm's node_error", err)
	}
	if n.released.Load() != 2 {
		t.Fatalf("failure: finally ran %d times in total, want 2", n.released.Load())
	}
	if got, want := stepIDs(result), []string{"charge", "release", "payment"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("failure: steps=%v want %v", got, want)
	}
	if result.Steps[0].Error == nil || result.Steps[1].Error != nil || !result.Steps[1].Executed || result.Steps[2].Error == nil {
		t.Fatalf("failure: steps=%+v; want charge failed, release ran, payment failed", result.Steps)
	}
}

// Invalid shapes are refused at Lower with a diagnostic naming the step,
// the construct and what to do instead.
func TestLowerExplainsControlFlowItCannotRun(t *testing.T) {
	n := newControlNodes(t)
	vip := func(arm *flow.ArmBuilder, in flow.Ref[object]) flow.Ref[object] {
		return flow.ArmCall(arm, "vip", n.vip, in)
	}
	standard := func(arm *flow.ArmBuilder, in flow.Ref[object]) flow.Ref[object] {
		return flow.ArmCall(arm, "standard", n.standard, in)
	}
	cases := map[string]struct {
		define func() (flow.Definition[object, object], error)
		want   string
	}{
		"arm step read after the construct": {func() (flow.Definition[object, object], error) {
			return flow.Define(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				priced := flow.Call(b, "price", n.price, in)
				flow.If(b, "route", flow.Compare(b, "big", "gt", flow.Select[object, int](priced, "total"), flow.Lit(100)),
					func(arm *flow.ArmBuilder) flow.Ref[object] { return vip(arm, priced) },
					func(arm *flow.ArmBuilder) flow.Ref[object] { return standard(arm, priced) })
				return flow.Call(b, "after", n.standard, selectStep("vip"))
			})
		}, `flow: call "after": input "$step.vip" names a step inside arm "then" of if "route", which is not visible here: read the construct's result instead`},
		"step of one arm read in the other": {func() (flow.Definition[object, object], error) {
			return flow.Define(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				priced := flow.Call(b, "price", n.price, in)
				return flow.If(b, "route", flow.Compare(b, "big", "gt", flow.Select[object, int](priced, "total"), flow.Lit(100)),
					func(arm *flow.ArmBuilder) flow.Ref[object] { return vip(arm, priced) },
					func(arm *flow.ArmBuilder) flow.Ref[object] { return standard(arm, selectStep("vip")) })
			})
		}, `flow: call "standard": input "$step.vip" names a step inside arm "then" of if "route", which is not visible here: read the construct's result instead`},
		"unknown comparison operator": {func() (flow.Definition[object, object], error) {
			return flow.Define(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				priced := flow.Call(b, "price", n.price, in)
				return flow.If(b, "route", flow.Compare(b, "big", "greater", flow.Select[object, int](priced, "total"), flow.Lit(100)),
					func(arm *flow.ArmBuilder) flow.Ref[object] { return vip(arm, priced) },
					func(arm *flow.ArmBuilder) flow.Ref[object] { return standard(arm, priced) })
			})
		}, `flow: compare "big": operator "greater" is not one of eq, ne, gt, gte, lt, lte`},
		"condition read before it is computed": {func() (flow.Definition[object, object], error) {
			return flow.Define(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
				priced := flow.Call(b, "price", n.price, in)
				return flow.If(b, "route", selectOp("later"),
					func(arm *flow.ArmBuilder) flow.Ref[object] { return vip(arm, priced) },
					func(arm *flow.ArmBuilder) flow.Ref[object] { return standard(arm, priced) })
			})
		}, `flow: if "route": condition "$op.later" does not reference an earlier operation or construct`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			definition, err := tc.define()
			if err != nil {
				t.Fatalf("Define: %v", err)
			}
			program, err := definition.Lower()
			if err == nil || err.Error() != tc.want {
				t.Fatalf("Lower err=%v program=%+v; want %q", err, program.Instructions, tc.want)
			}
		})
	}
}

// A step recorded on the enclosing builder while an arm is being built
// would silently run outside the arm, before the construct. The builder
// refuses it instead.
func TestStepRecordedOnTheEnclosingBuilderInsideAnArmIsRefused(t *testing.T) {
	n := newControlNodes(t)
	_, err := flow.Define(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		return flow.TryFinally(b, "payment",
			func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.Call(b, "charge", n.charge, in) },
			func(arm *flow.ArmBuilder) { flow.ArmCall(arm, "release", n.release, in) })
	})
	want := `flow: step "charge" was recorded on an enclosing builder while one of its arms was being built; record it on the arm (ArmCall, or the arm's Builder)`
	if err == nil || err.Error() != want {
		t.Fatalf("Define err=%v; want %q", err, want)
	}
}

// selectStep and selectOp are references a workflow can only obtain by
// leaking them from another construct; they reach the cases Lower must
// explain.
func selectStep(id string) flow.Ref[object] {
	var leaked flow.Ref[object]
	flow.MustDefine(flow.Spec{Name: "leak", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		leaked = flow.Call(b, id, node.MustDefine("leak", "1.0.0", func(context.Context, object) (object, error) { return nil, nil }, node.Description("leak"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))), in)
		return leaked
	})
	return leaked
}

func selectOp(id string) flow.Ref[bool] {
	var leaked flow.Ref[bool]
	flow.MustDefine(flow.Spec{Name: "leak", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		leaked = flow.Compare(b, id, "eq", in, in)
		return in
	})
	return leaked
}

func canonical(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
