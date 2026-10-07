package flow_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

// Each inside If inside Each, and Parallel inside Each, run through the
// public runner; every execution reports the invocation path and the
// iteration path (each[i], joined with "/" when nested) durable slices key
// their records by (ADR 0028).
func TestNestedLoopsAndParallelReportIterationPaths(t *testing.T) {
	n := newLoopNodes()
	pack := node.MustDefine("pack", "1.0.0", func(_ context.Context, lines []object) (object, error) {
		return object{"packed": len(lines)}, nil
	}, node.Description("packs priced lines"), node.Schemas([]byte(`{"type":"array","items":{"type":"object","properties":{"sku":{"type":"string"},"total":{"type":"integer"}}}}`), []byte(`{"type":"object"}`)))
	n.extra = map[string]node.Any{"pack": pack.Any()}
	definition := flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[[]object] {
		return flow.Each(b, "orders", flow.Select[object, []object](in, "orders"), 2, func(arm *flow.ArmBuilder, order flow.Ref[object]) flow.Ref[object] {
			rush := flow.Compare(arm.Builder(), "rush", "eq", flow.Select[object, bool](order, "rush"), flow.Lit(true))
			return flow.If(arm.Builder(), "route", rush,
				func(then *flow.ArmBuilder) flow.Ref[object] {
					priced := flow.Each(then.Builder(), "lines", flow.Select[object, []object](order, "lines"), 2, func(body *flow.ArmBuilder, line flow.Ref[object]) flow.Ref[object] {
						return flow.ArmCall(body, "line", n.line, line)
					})
					return flow.ArmCall(then, "pack", pack, priced)
				},
				func(otherwise *flow.ArmBuilder) flow.Ref[object] {
					var audited flow.Ref[object]
					flow.Parallel(otherwise.Builder(), "checks",
						func(check *flow.ArmBuilder) { audited = flow.ArmCall(check, "audit-a", n.verdict, order) },
						func(check *flow.ArmBuilder) { flow.ArmCall(check, "audit-b", n.verdict, order) })
					return flow.ArmCall(otherwise, "approve", n.verdict, audited)
				})
		})
	})
	result, err := runLoop(t, n, definition, object{"orders": []any{
		object{"id": "o-1", "rush": true, "lines": lines("tea", "cake")},
		object{"id": "o-2", "rush": false},
	}})
	if err != nil {
		t.Fatal(err)
	}
	want := []any{object{"packed": 2}, object{"verdict": object{"verdict": object{"id": "o-2", "rush": false}}}}
	if canonical(t, result.Output) != canonical(t, want) {
		t.Fatalf("output=%s want %s", canonical(t, result.Output), canonical(t, want))
	}
	var got []string
	for _, step := range result.Steps {
		got = append(got, step.InvocationPath+" @ "+step.IterationPath)
	}
	sort.Strings(got)
	expected := []string{
		"orders @ root",
		"orders/body/route @ orders[0]",
		"orders/body/route @ orders[1]",
		"orders/body/route/else/approve @ orders[1]",
		"orders/body/route/else/checks @ orders[1]",
		"orders/body/route/else/checks/0/audit-a @ orders[1]",
		"orders/body/route/else/checks/1/audit-b @ orders[1]",
		"orders/body/route/then/lines @ orders[0]",
		"orders/body/route/then/lines/body/line @ orders[0]/lines[0]",
		"orders/body/route/then/lines/body/line @ orders[0]/lines[1]",
		"orders/body/route/then/pack @ orders[0]",
		"orders/body/rush @ orders[0]",
		"orders/body/rush @ orders[1]",
		"output @ root",
	}
	if strings.Join(got, "\n") != strings.Join(expected, "\n") {
		t.Fatalf("paths:\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(expected, "\n"))
	}
}
