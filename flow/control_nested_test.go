package flow_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/flow"
)

// Nested constructs run through the public runner, each arm reading values
// from the scopes around it, and every step reports the invocation and
// iteration path later durable slices key their records by (ADR 0031).
func TestNestedControlFlowRunsAndReportsPaths(t *testing.T) {
	n := newControlNodes(t)
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		return flow.TryFinally(b, "payment",
			func(arm *flow.ArmBuilder) flow.Ref[object] {
				priced := flow.ArmCall(arm, "price", n.price, in)
				big := flow.Compare(arm.Builder(), "big", "gte", flow.Select[object, int](priced, "total"), flow.Lit(30))
				return flow.If(arm.Builder(), "route", big,
					func(then *flow.ArmBuilder) flow.Ref[object] {
						return flow.Choose(then.Builder(), "lane", flow.Select[object, string](priced, "sku"), map[string]func(*flow.ArmBuilder) flow.Ref[object]{
							"coffee": func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "coffee-vip", n.vip, priced) },
						}, func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "other-vip", n.vip, priced) })
					},
					func(otherwise *flow.ArmBuilder) flow.Ref[object] {
						return flow.ArmCall(otherwise, "standard", n.standard, priced)
					})
			},
			func(arm *flow.ArmBuilder) { flow.ArmCall(arm, "release", n.release, in) })
	})
	for _, tc := range []struct {
		input object
		lane  string
		paths []string
	}{
		{object{"sku": "coffee", "quantity": 20}, "vip", []string{
			"payment/try/price", "payment/try/big", "payment/try/route/then/lane/case-0/coffee-vip", "payment/try/route/then/lane",
			"payment/try/route", "payment/finally/release", "payment", "output",
		}},
		{object{"sku": "tea", "quantity": 20}, "vip", []string{
			"payment/try/price", "payment/try/big", "payment/try/route/then/lane/default/other-vip", "payment/try/route/then/lane",
			"payment/try/route", "payment/finally/release", "payment", "output",
		}},
		{object{"sku": "coffee", "quantity": 1}, "standard", []string{
			"payment/try/price", "payment/try/big", "payment/try/route/else/standard", "payment/try/route", "payment/finally/release", "payment", "output",
		}},
	} {
		result, err := runControl(t, n, definition, tc.input)
		if err != nil {
			t.Fatalf("%v: %v", tc.input, err)
		}
		if lane := result.Output.(object)["lane"]; lane != tc.lane {
			t.Errorf("%v: lane=%v want %s", tc.input, lane, tc.lane)
		}
		var paths []string
		for _, step := range result.Steps {
			paths = append(paths, step.InvocationPath)
			if step.IterationPath != "root" {
				t.Errorf("%v: step %s iteration path %q, want root", tc.input, step.ID, step.IterationPath)
			}
		}
		if !reflect.DeepEqual(paths, tc.paths) {
			t.Errorf("%v: paths=%v\n want %v", tc.input, paths, tc.paths)
		}
		if _, leaked := result.State["price"]; leaked {
			t.Errorf("%v: an arm's step leaked into the workflow scope: %v", tc.input, result.State)
		}
	}
}

// The control instruction encoding is the program format ADR 0031 records.
func TestControlProgramEncoding(t *testing.T) {
	n := newControlNodes(t)
	lowered, err := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
		priced := flow.Call(b, "price", n.price, in)
		big := flow.Compare(b, "big", "gt", flow.Select[object, int](priced, "total"), flow.Lit(100))
		return flow.If(b, "route", big,
			func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "vip", n.vip, priced) },
			func(arm *flow.ArmBuilder) flow.Ref[object] {
				return flow.Default(arm.Builder(), "kept", priced, priced)
			})
	}).Lower()
	if err != nil {
		t.Fatal(err)
	}
	if lowered.Format != contract.ControlFormat {
		t.Fatalf("format=%d want %d", lowered.Format, contract.ControlFormat)
	}
	encoded, err := json.Marshal(lowered)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"workflowId":"control","version":"1.0.0","digest":"","format":2,"instructions":[` +
		`{"index":0,"id":"price","kind":"call","node":"price","output":null},` +
		`{"index":1,"id":"big","kind":"compare","output":null,"control":{"operator":"gt","operands":[{"reference":{"step":"price","path":["total"]}},{"literal":100}]}},` +
		`{"index":2,"id":"route","kind":"if","output":null,"control":{"operands":[{"reference":{"step":"big"}}],"arms":[` +
		`{"name":"then","instructions":[{"index":0,"id":"vip","kind":"call","node":"vip","references":[{"step":"price"}],"output":null}],"output":{"reference":{"step":"vip"}}},` +
		`{"name":"else","instructions":[{"index":0,"id":"kept","kind":"default","output":null,"control":{"operands":[{"reference":{"step":"price"}},{"reference":{"step":"price"}}]}}],"output":{"reference":{"step":"kept"}}}]}},` +
		`{"index":3,"id":"output","kind":"output","references":[{"step":"route"}],"output":null}]}`
	if string(encoded) != want {
		t.Fatalf("encoding:\n got %s\nwant %s", encoded, want)
	}
}
