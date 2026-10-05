package node_test

import (
	"context"
	"encoding/json"
	"os"
	"strconv"
	"testing"

	"github.com/well-prado/new-blok/node"
)

func TestDecodeOutputCheckpointFixturesPreserveNumbersAndRejectInvalidValues(t *testing.T) {
	data, err := os.ReadFile("../testdata/engine/checkpoint-output-fixtures.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []struct {
		Name     string `json:"name"`
		Target   string `json:"target"`
		JSON     string `json:"json"`
		Valid    bool   `json:"valid"`
		Expected string `json:"expected"`
	}
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	uintNode := node.MustDefine("fixture/uint", "1.0.0", func(context.Context, struct{}) (uint64, error) { return 0, nil }, node.Description("checkpoint decode fixture"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"integer"}`))).Any()
	interfaceNode := node.MustDefine("fixture/interface", "1.0.0", func(context.Context, struct{}) (any, error) { return nil, nil }, node.Description("checkpoint decode fixture"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))).Any()
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			definition := uintNode
			if fixture.Target == "interface" {
				definition = interfaceNode
			}
			value, err := definition.DecodeOutput([]byte(fixture.JSON))
			if fixture.Valid != (err == nil) {
				t.Fatalf("decode error=%v, valid=%v", err, fixture.Valid)
			}
			if err != nil {
				return
			}
			var got string
			switch typed := value.(type) {
			case uint64:
				got = strconv.FormatUint(typed, 10)
			case json.Number:
				got = typed.String()
			default:
				t.Fatalf("decoded type %T", value)
			}
			if got != fixture.Expected {
				t.Fatalf("decoded number=%s, expected %s", got, fixture.Expected)
			}
		})
	}
}
