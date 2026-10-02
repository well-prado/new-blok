// Package catalog contains small, pure, schema-described nodes. It owns no
// providers and never invokes another node; workflows compose catalog nodes.
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"strings"

	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/node"
)

const (
	MaxCollectionItems = 1024
	MaxTemplateBytes   = 1 << 16
)

type Registry struct{ nodes *node.Registry }

func NewRegistry() *Registry { return &Registry{nodes: node.NewRegistry()} }

// Register accepts both built-in and application-defined nodes through the
// same registry boundary. The registry stores descriptors, not compositions.
func (r *Registry) Register(def node.Any) error {
	if r == nil {
		return fmt.Errorf("catalog: nil registry")
	}
	return r.nodes.Register(def)
}

func (r *Registry) Lookup(name, version string) (node.Any, bool) {
	if r == nil {
		return node.Any{}, false
	}
	return r.nodes.Lookup(name, version)
}

type ValidateInput struct {
	Value  json.RawMessage `json:"value"`
	Schema json.RawMessage `json:"schema"`
}

type ValidateOutput struct {
	Value json.RawMessage `json:"value"`
}

func Validate() node.Definition[ValidateInput, ValidateOutput] {
	return node.MustDefine("catalog/validate", "1.0.0", func(_ context.Context, input ValidateInput) (ValidateOutput, error) {
		s, err := schema.Parse(input.Schema)
		if err != nil {
			return ValidateOutput{}, err
		}
		normalized, err := s.Normalize(input.Value)
		if err != nil {
			return ValidateOutput{}, err
		}
		return ValidateOutput{Value: append(json.RawMessage(nil), normalized...)}, nil
	}, node.Description("Validates and normalizes a bounded portable value"), node.Schemas(validateInputSchema, validateOutputSchema), node.Pure())
}

type MapInput struct {
	Value  json.RawMessage   `json:"value"`
	Fields map[string]string `json:"fields"`
}

type MapOutput struct {
	Value json.RawMessage `json:"value"`
}

func Map() node.Definition[MapInput, MapOutput] {
	return node.MustDefine("catalog/map", "1.0.0", func(_ context.Context, input MapInput) (MapOutput, error) {
		var value any
		dec := json.NewDecoder(strings.NewReader(string(input.Value)))
		dec.UseNumber()
		if err := dec.Decode(&value); err != nil {
			return MapOutput{}, fmt.Errorf("catalog/map: invalid value: %w", err)
		}
		if len(input.Fields) > MaxCollectionItems {
			return MapOutput{}, fmt.Errorf("catalog/map: collection_limit: fields exceed %d", MaxCollectionItems)
		}
		out := make(map[string]any, len(input.Fields))
		for destination, source := range input.Fields {
			selected, err := selectPath(value, source)
			if err != nil {
				return MapOutput{}, err
			}
			out[destination] = selected
		}
		encoded, err := json.Marshal(out)
		if err != nil {
			return MapOutput{}, err
		}
		return MapOutput{Value: encoded}, nil
	}, node.Description("Selects bounded fields without evaluating arbitrary expressions"), node.Schemas(mapInputSchema, validateOutputSchema), node.Pure())
}

type TemplateInput struct {
	Template string            `json:"template"`
	Values   map[string]string `json:"values"`
}

type TemplateOutput struct {
	Value string `json:"value"`
}

var templateVariable = regexp.MustCompile(`\{\{([a-zA-Z0-9_.\[\]-]+)\}\}`)

func Template() node.Definition[TemplateInput, TemplateOutput] {
	return node.MustDefine("catalog/template", "1.0.0", func(_ context.Context, input TemplateInput) (TemplateOutput, error) {
		if len(input.Template) > MaxTemplateBytes {
			return TemplateOutput{}, fmt.Errorf("catalog/template: template_limit: template exceeds %d bytes", MaxTemplateBytes)
		}
		if len(input.Values) > MaxCollectionItems {
			return TemplateOutput{}, fmt.Errorf("catalog/template: collection_limit: values exceed %d", MaxCollectionItems)
		}
		missing := ""
		result := templateVariable.ReplaceAllStringFunc(input.Template, func(match string) string {
			key := strings.TrimSuffix(strings.TrimPrefix(match, "{{"), "}}")
			value, ok := input.Values[key]
			if !ok {
				missing = key
				return match
			}
			return value
		})
		if missing != "" {
			return TemplateOutput{}, fmt.Errorf("catalog/template: missing_field: values.%s is required", missing)
		}
		if len(result) > MaxTemplateBytes {
			return TemplateOutput{}, fmt.Errorf("catalog/template: template_limit: rendered output exceeds %d bytes", MaxTemplateBytes)
		}
		return TemplateOutput{Value: result}, nil
	}, node.Description("Renders bounded named values without arbitrary evaluation"), node.Schemas(templateInputSchema, templateOutputSchema), node.Pure())
}

type IntegerInput struct {
	Left  int64 `json:"left"`
	Right int64 `json:"right"`
}
type IntegerOutput struct {
	Value int64 `json:"value"`
}

func AddInteger() node.Definition[IntegerInput, IntegerOutput] {
	return node.MustDefine("catalog/add-integer", "1.0.0", func(_ context.Context, input IntegerInput) (IntegerOutput, error) {
		return addInteger(input, "catalog/add-integer")
	}, node.Description("Adds signed int64 values with overflow detection"), node.Schemas(integerInputSchema, integerOutputSchema), node.Pure())
}

func AddMoney() node.Definition[IntegerInput, IntegerOutput] {
	return node.MustDefine("catalog/add-money-cents", "1.0.0", func(_ context.Context, input IntegerInput) (IntegerOutput, error) {
		return addInteger(input, "catalog/add-money-cents")
	}, node.Description("Adds exact minor-unit money values with overflow detection"), node.Schemas(integerInputSchema, integerOutputSchema), node.Pure())
}

func addInteger(input IntegerInput, name string) (IntegerOutput, error) {
	if (input.Right > 0 && input.Left > math.MaxInt64-input.Right) || (input.Right < 0 && input.Left < math.MinInt64-input.Right) {
		return IntegerOutput{}, fmt.Errorf("%s: integer_overflow: int64 addition overflow", name)
	}
	return IntegerOutput{Value: input.Left + input.Right}, nil
}

func selectPath(value any, path string) (any, error) {
	if path == "" || path == "$" {
		return value, nil
	}
	path = strings.TrimPrefix(path, "$")
	path = strings.TrimPrefix(path, ".")
	for _, part := range strings.FieldsFunc(path, func(r rune) bool { return r == '.' || r == '[' || r == ']' }) {
		switch current := value.(type) {
		case map[string]any:
			next, ok := current[part]
			if !ok {
				return nil, fmt.Errorf("catalog/map: missing_field: value.%s is required", path)
			}
			value = next
		case []any:
			var index int
			if _, err := fmt.Sscanf(part, "%d", &index); err != nil || index < 0 || index >= len(current) {
				return nil, fmt.Errorf("catalog/map: field_path: %s is out of bounds", path)
			}
			value = current[index]
		default:
			return nil, fmt.Errorf("catalog/map: field_path: %s cannot be selected", path)
		}
	}
	return value, nil
}

var validateInputSchema = []byte(`{"type":"object","properties":{"value":{"anyOf":[{"type":"object","additionalProperties":true},{"type":"array","items":{"type":"string"}},{"type":"string"},{"type":"integer"},{"type":"number"},{"type":"boolean"},{"type":"null"}]},"schema":{"type":"object","additionalProperties":true}},"required":["value","schema"],"additionalProperties":false}`)
var validateOutputSchema = []byte(`{"type":"object","properties":{"value":{"anyOf":[{"type":"object","additionalProperties":true},{"type":"array","items":{"type":"string"}},{"type":"string"},{"type":"integer"},{"type":"number"},{"type":"boolean"},{"type":"null"}]}},"required":["value"],"additionalProperties":false}`)
var mapInputSchema = []byte(`{"type":"object","properties":{"value":{"anyOf":[{"type":"object","additionalProperties":true},{"type":"array","items":{"type":"string"}}]},"fields":{"type":"object","additionalProperties":true}},"required":["value","fields"],"additionalProperties":false}`)
var templateInputSchema = []byte(`{"type":"object","properties":{"template":{"type":"string"},"values":{"type":"object","additionalProperties":true}},"required":["template","values"],"additionalProperties":false}`)
var templateOutputSchema = []byte(`{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}`)
var integerInputSchema = []byte(`{"type":"object","properties":{"left":{"type":"integer"},"right":{"type":"integer"}},"required":["left","right"],"additionalProperties":false}`)
var integerOutputSchema = []byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"],"additionalProperties":false}`)
