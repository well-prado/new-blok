package flow

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/well-prado/new-blok/node"
)

var controlSchemas = []byte(`{"type":"object"}`)

func controlNode(t *testing.T) node.Definition[quoteInput, quoteOutput] {
	t.Helper()
	return node.MustDefine("shop/control-quote", "1.0.0", func(context.Context, quoteInput) (quoteOutput, error) {
		t.Fatal("control builder executed node")
		return quoteOutput{}, nil
	}, node.Description("control fixture"), node.Schemas(controlSchemas, controlSchemas), node.Pure())
}

func TestControlFlowRecordsScopedJoinsAndBounds(t *testing.T) {
	n := controlNode(t)
	definition := MustDefine(Spec{Name: "shop/control", Version: "1.0.0", Durability: Memory}, func(builder *Builder, input Ref[quoteInput]) Ref[quoteOutput] {
		condition := Ref[bool]{expression: expression{Kind: "reference", Source: "$condition"}}
		selected := If(builder, "choose-lane", condition,
			func(arm *ArmBuilder) Ref[quoteOutput] { return ArmCall(arm, "premium", n, input) },
			func(arm *ArmBuilder) Ref[quoteOutput] { return ArmCall(arm, "standard", n, input) },
		)
		items := Ref[[]quoteInput]{expression: expression{Kind: "reference", Source: "$items"}}
		Each(builder, "line-items", items, 4, func(arm *ArmBuilder, item Ref[quoteInput]) Ref[quoteOutput] {
			return ArmCall(arm, "line", n, item)
		})
		Parallel(builder, "audit", func(arm *ArmBuilder) { ArmCall(arm, "audit-one", n, input) }, func(arm *ArmBuilder) { ArmCall(arm, "audit-two", n, input) })
		return selected
	}).Program()
	if len(definition.Instructions) != 8 {
		t.Fatalf("instructions=%d, want 8: %+v", len(definition.Instructions), definition.Instructions)
	}
	if definition.Instructions[2].Kind != "if" || definition.Instructions[4].Kind != "each" || definition.Instructions[7].Kind != "parallel" {
		t.Fatalf("instructions=%+v", definition.Instructions)
	}
	if definition.Instructions[4].Data["concurrency"] != 4 || definition.Instructions[4].Data["preserveOrder"] != true {
		t.Fatalf("each data=%+v", definition.Instructions[4].Data)
	}
}

func TestDuplicateIDsAcrossArmsAndUnboundedEachAreRejected(t *testing.T) {
	n := controlNode(t)
	assertPanics(t, func() {
		MustDefine(Spec{Name: "shop/duplicate", Version: "1.0.0", Durability: Memory}, func(builder *Builder, input Ref[quoteInput]) Ref[quoteOutput] {
			condition := Ref[bool]{expression: expression{Source: "$condition"}}
			return If(builder, "route", condition,
				func(arm *ArmBuilder) Ref[quoteOutput] { return ArmCall(arm, "same", n, input) },
				func(arm *ArmBuilder) Ref[quoteOutput] { return ArmCall(arm, "same", n, input) },
			)
		})
	})
	assertPanics(t, func() {
		Each(&Builder{program: Program{}, ids: make(map[string]struct{})}, "unbounded", Ref[[]quoteInput]{}, 0, func(*ArmBuilder, Ref[quoteInput]) Ref[quoteOutput] { return Ref[quoteOutput]{} })
	})
}

func TestControlFixtureDeclaresExpectedCounts(t *testing.T) {
	var fixture struct {
		Cases []struct {
			Name                string `json:"name"`
			ExpectedOutputCount int    `json:"expectedOutputCount"`
			ExpectedErrorCount  int    `json:"expectedErrorCount"`
			ExpectedEffectCount int    `json:"expectedEffectCount"`
		} `json:"cases"`
	}
	data, err := os.ReadFile("testdata/control/fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Cases) < 2 {
		t.Fatalf("cases=%d, want at least 2", len(fixture.Cases))
	}
}

func assertPanics(t *testing.T, action func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	action()
}
