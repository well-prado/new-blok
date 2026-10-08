package node

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
)

type convertShip struct {
	Quantity int `json:"quantity"`
}

type convertSecret string

func (convertSecret) MarshalJSON() ([]byte, error) { return []byte(`"***"`), nil }

func convertNode[I any]() Any {
	return MustDefine("convert", "1.0.0", func(context.Context, I) (I, error) { var zero I; return zero, nil },
		Description("convert"), Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))).Any()
}

// ConvertInput reads a value as the node's input type only when it has an
// exact reading as one, and otherwise hands it back unchanged.
func TestConvertInputNeverInventsAValue(t *testing.T) {
	ship, quantity := convertShip{Quantity: 2}, 7
	hidden := convertSecret("token")
	var nilShip *convertShip
	cases := []struct {
		name  string
		node  Any
		value any
		want  any
	}{
		{"same type", convertNode[convertShip](), ship, ship},
		{"pointer dereferenced", convertNode[convertShip](), &ship, ship},
		{"pointer to int", convertNode[int](), &quantity, 7},
		{"json number into int", convertNode[int](), json.Number("5"), 5},
		{"json object into struct", convertNode[convertShip](), map[string]any{"quantity": json.Number("3")}, convertShip{Quantity: 3}},
		{"unknown field refused", convertNode[convertShip](), map[string]any{"quantity": json.Number("3"), "zip": "1"}, map[string]any{"quantity": json.Number("3"), "zip": "1"}},
		{"null into a struct refused", convertNode[convertShip](), nil, nil},
		{"nil pointer into a struct refused", convertNode[convertShip](), nilShip, nilShip},
		{"null into a pointer", convertNode[*convertShip](), nil, (*convertShip)(nil)},
		{"null into a slice", convertNode[[]convertShip](), nil, []convertShip(nil)},
		{"results element by element", convertNode[[]convertShip](), []any{ship, &ship, map[string]any{"quantity": json.Number("1")}}, []convertShip{ship, ship, {Quantity: 1}}},
		{"one bad element refuses all", convertNode[[]convertShip](), []any{ship, "x"}, []any{ship, "x"}},
		{"self-encoding value not re-read", convertNode[string](), &hidden, &hidden},
		{"other struct refused", convertNode[convertShip](), struct{ Zip string }{"1"}, struct{ Zip string }{"1"}},
		{"interface input untouched", convertNode[any](), map[string]any{"a": 1}, map[string]any{"a": 1}},
	}
	for _, tc := range cases {
		if got := tc.node.ConvertInput(tc.value); !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%s: got %#v want %#v", tc.name, got, tc.want)
		}
	}
}
