package flowtest_test

import (
	"context"
	"testing"

	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/flowtest"
	"github.com/well-prado/new-blok/node"
)

type order struct {
	Total int `json:"total"`
}

type lane struct {
	Name string `json:"name"`
}

// flowtest runs a lowered branching workflow through the production engine,
// with a mock in the selected arm (#333).
func TestFlowtestRunsBranchesWithMocks(t *testing.T) {
	schemas := node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))
	vip := node.MustDefine("vip", "1.0.0", func(context.Context, order) (lane, error) { return lane{Name: "vip"}, nil }, node.Description("vip lane"), schemas)
	standard := node.MustDefine("standard", "1.0.0", func(context.Context, order) (lane, error) { return lane{Name: "standard"}, nil }, node.Description("standard lane"), schemas)
	measure := node.MustDefine("measure", "1.0.0", func(_ context.Context, in order) (order, error) { return in, nil }, node.Description("reads the order"), schemas)
	program, err := flow.MustDefine(flow.Spec{Name: "lanes", Version: "1.0.0"}, func(b *flow.Builder, in flow.Ref[order]) flow.Ref[lane] {
		big := flow.Compare(b, "big", "gt", flow.Select[order, int](flow.Call(b, "measure", measure, in), "total"), flow.Lit(100))
		return flow.If(b, "route", big,
			func(arm *flow.ArmBuilder) flow.Ref[lane] { return flow.ArmCall(arm, "vip", vip, in) },
			func(arm *flow.ArmBuilder) flow.Ref[lane] { return flow.ArmCall(arm, "standard", standard, in) })
	}).Lower()
	if err != nil {
		t.Fatal(err)
	}
	nodes := map[string]node.Any{"measure": measure.Any(), "vip": vip.Any(), "standard": standard.Any()}
	mocked := flowtest.Run(context.Background(), program, nodes, order{Total: 500}, flowtest.Options{Mocks: map[string]flowtest.Mock{"vip": {Base: vip.Any(), Output: lane{Name: "mocked-vip"}}}})
	if !mocked.OK() || mocked.Response().(lane).Name != "mocked-vip" {
		t.Fatalf("vip: ok=%v err=%v response=%#v", mocked.OK(), mocked.Err(), mocked.Response())
	}
	if _, ran := mocked.Step("standard"); ran {
		t.Fatal("vip: the standard arm ran")
	}
	small := flowtest.Run(context.Background(), program, nodes, order{Total: 5}, flowtest.Options{})
	if !small.OK() || small.Response().(lane).Name != "standard" {
		t.Fatalf("standard: ok=%v err=%v response=%#v", small.OK(), small.Err(), small.Response())
	}
}
