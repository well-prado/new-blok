package fixture

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/well-prado/new-blok/app"
	contract "github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/generate"
	"github.com/well-prado/new-blok/node"
)

// TestBindingsAreCurrent: bindings_gen.go is exactly what the generator
// produces from types.go, so the workflow test below exercises generated
// code, not hand-written accessors.
func TestBindingsAreCurrent(t *testing.T) {
	source, err := os.ReadFile("types.go")
	if err != nil {
		t.Fatal(err)
	}
	want, err := generate.Source(source, generate.Options{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile("bindings_gen.go")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("bindings_gen.go is stale; regenerate it from types.go:\n%s", want)
	}
}

// TestGeneratedAccessorsResolveAtRunTime: a workflow composed only through
// generated accessors reads every encoded field of a real node's output.
// Accessors that selected the Go field name failed here with
// invalid_output_reference (#240).
func TestGeneratedAccessorsResolveAtRunTime(t *testing.T) {
	ctx := context.Background()
	schema := []byte(`{"type":"object"}`)
	price, err := node.Define("fixture/price", "1.0.0", func(_ context.Context, line Line) (Line, error) {
		line.TotalCents = 1500
		line.Total = 3000
		return line, nil
	}, node.Description("prices a line"), node.Schemas(schema, schema))
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "fixture"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{"fixture/price": price.Any()})
	run := func(t *testing.T, lower func() (contract.InternalProgram, error)) any {
		t.Helper()
		program, err := lower()
		if err != nil {
			t.Fatal(err)
		}
		result, err := runner.Run(ctx, program, Line{SKU: "coffee"}, inspection.Invocation{})
		if err != nil {
			t.Fatalf("run: %v", err)
		}
		return result.Output
	}
	spec := flow.Spec{Name: "fixture", Version: "1.0.0"}
	t.Run("tagged", func(t *testing.T) {
		definition, err := flow.Define(spec, func(b *flow.Builder, in flow.Ref[Line]) flow.Ref[string] {
			return LineFields(flow.Call(b, "price", price, in)).SKU()
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := run(t, definition.Lower); got != "coffee" {
			t.Fatalf("SKU()=%#v; want coffee", got)
		}
	})
	t.Run("untagged", func(t *testing.T) {
		definition, err := flow.Define(spec, func(b *flow.Builder, in flow.Ref[Line]) flow.Ref[int64] {
			return LineFields(flow.Call(b, "price", price, in)).TotalCents()
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := run(t, definition.Lower); got != int64(1500) {
			t.Fatalf("TotalCents()=%#v; want 1500", got)
		}
	})
	t.Run("renamed-optional", func(t *testing.T) {
		definition, err := flow.Define(spec, func(b *flow.Builder, in flow.Ref[Line]) flow.Ref[int64] {
			return LineFields(flow.Call(b, "price", price, in)).Total()
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := run(t, definition.Lower); got != int64(3000) {
			t.Fatalf("Total()=%#v; want 3000", got)
		}
	})
}
