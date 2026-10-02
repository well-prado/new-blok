package flow

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"github.com/well-prado/new-blok/node"
)

type quoteInput struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

type quoteOutput struct {
	TotalCents int64 `json:"totalCents"`
}

func testQuoteNode(t *testing.T) node.Definition[quoteInput, quoteOutput] {
	t.Helper()
	return node.MustDefine("shop/calculate-quote", "1.0.0", func(context.Context, quoteInput) (quoteOutput, error) {
		panic("must not execute while building")
	}, node.Description("calculates a quote"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)), node.Pure())
}

func buildQuote(t *testing.T) Definition[quoteInput, quoteOutput] {
	t.Helper()
	quote := testQuoteNode(t)
	return MustDefine(Spec{Name: "shop/quote", Version: "1.0.0", Durability: Memory}, func(builder *Builder, input Ref[quoteInput]) Ref[quoteOutput] {
		return Call(builder, "calculate", quote, input)
	})
}

func TestBuildRecordsWholeValueProgramWithoutExecutingNode(t *testing.T) {
	program := buildQuote(t).Program()
	if len(program.Instructions) != 1 || program.Output != "$step.calculate" {
		t.Fatalf("program=%+v", program)
	}
	instruction := program.Instructions[0]
	if instruction.Input != "$input" || instruction.Node.Name != "shop/calculate-quote" {
		t.Fatalf("instruction=%+v", instruction)
	}
}

func TestRepeatedDefinitionProducesEquivalentPrograms(t *testing.T) {
	first, second := buildQuote(t).Program(), buildQuote(t).Program()
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
}

func TestLiteralIsRecordedStructurally(t *testing.T) {
	quote := testQuoteNode(t)
	program := MustDefine(Spec{Name: "shop/quote", Version: "1.0.0", Durability: Memory}, func(builder *Builder, _ Ref[quoteInput]) Ref[quoteOutput] {
		return Call(builder, "calculate", quote, Lit(quoteInput{SKU: "coffee", Quantity: 2}))
	}).Program()
	instruction := program.Instructions[0]
	if instruction.Input != "$literal" || string(instruction.Literal) != `{"sku":"coffee","quantity":2}` {
		t.Fatalf("instruction=%+v", instruction)
	}
}

func TestProgramRoundTripMatchesFixture(t *testing.T) {
	program := buildQuote(t).Program()
	wantBytes, err := os.ReadFile("testdata/authoring/quote-program.json")
	if err != nil {
		t.Fatal(err)
	}
	var want Program
	if err := json.Unmarshal(wantBytes, &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(program, want) {
		t.Fatalf("program=%+v want=%+v", program, want)
	}
}

func TestMustDefineMakesStartupFailureExplicit(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustDefine did not panic")
		}
	}()
	MustDefine[quoteInput, quoteOutput](Spec{}, func(*Builder, Ref[quoteInput]) Ref[quoteOutput] {
		return Ref[quoteOutput]{}
	})
}
