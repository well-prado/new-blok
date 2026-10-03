package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/well-prado/new-blok/contract/capacity"
	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store"
)

// TestWorkflowToolHidesSaturationAfterAChildEffect: a catalog workflow tool
// whose first child charges and whose second finds the store busy. The
// charge happened, so the caller must not be told to retry: the error does
// not read as saturation, nor as a busy store a caller may retry, and names
// the step that committed (#190).
func TestWorkflowToolHidesSaturationAfterAChildEffect(t *testing.T) {
	registry := node.NewRegistry()
	var charges atomic.Int32
	charge := node.MustDefine("native/charge", "1.0.0", func(_ context.Context, in value) (value, error) {
		charges.Add(1)
		return in, nil
	}, node.Description("charge"), node.Schemas(valueSchema, valueSchema), node.Effects("db:charge"))
	record := node.MustDefine("native/record", "1.0.0", func(context.Context, value) (value, error) {
		return value{}, fmt.Errorf("record: %w", store.ErrBusy)
	}, node.Description("record"), node.Schemas(valueSchema, valueSchema), node.Effects("db:record"))
	for _, n := range []node.Any{charge.Any(), record.Any()} {
		if err := registry.Register(n); err != nil {
			t.Fatal(err)
		}
	}
	catalog := NewCatalog(registry, nil)
	if err := RegisterNode(catalog, charge, manifest("charge"), tool.Resources{}, metadata()); err != nil {
		t.Fatal(err)
	}
	if err := RegisterNode(catalog, record, manifest("record"), tool.Resources{}, metadata()); err != nil {
		t.Fatal(err)
	}
	pay := flow.MustDefine(flow.Spec{Name: "workflow/pay", Version: "1.0.0", Durability: flow.Memory}, func(b *flow.Builder, in flow.Ref[value]) flow.Ref[value] {
		return flow.Call(b, "record", record, flow.Call(b, "charge", charge, in))
	})
	if err := RegisterWorkflow(catalog, pay, valueSchema, valueSchema, manifest("charge"), metadata()); err != nil {
		t.Fatal(err)
	}
	_, err := catalog.Invoke(context.Background(), principal("charge", "record"), "workflow/pay", "1.0.0", []byte(`{"value":1}`), budget())
	if err == nil || errors.Is(err, capacity.ErrSaturated) || errors.Is(err, store.ErrBusy) || charges.Load() != 1 || !strings.Contains(err.Error(), `after step "charge" committed its effects`) {
		t.Fatalf("err=%v charges=%d saturated=%v; want a failure after the charge that is not saturation", err, charges.Load(), errors.Is(err, capacity.ErrSaturated))
	}
}
