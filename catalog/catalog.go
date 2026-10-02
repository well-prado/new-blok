// Package catalog contains small schema-described pure helpers. They operate
// on values and never invoke nodes or evaluate source expressions.
package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"

	"github.com/well-prado/new-blok/contract/schema"
)

const (
	MaxCollectionItems = 1024
	MaxTemplateBytes   = 64 << 10
)

var (
	ErrOverflow = errors.New("catalog: integer overflow")
	ErrBounds   = errors.New("catalog: bound exceeded")
)

type Descriptor struct {
	Name          string
	Version       string
	Description   string
	InputSchema   json.RawMessage
	OutputSchema  json.RawMessage
	Effects       []string
	Deterministic bool
}

type Node struct {
	Descriptor Descriptor
	Execute    func(any) (any, error)
}

type Registry struct{ nodes map[string]Node }

func NewRegistry() *Registry { return &Registry{nodes: map[string]Node{}} }

func (r *Registry) Register(n Node) error {
	if r == nil {
		return errors.New("catalog: nil registry")
	}
	if err := ValidateDescriptor(n.Descriptor); err != nil {
		return err
	}
	if n.Execute == nil {
		return errors.New("catalog: node executor is required")
	}
	key := n.Descriptor.Name + "@" + n.Descriptor.Version
	if _, ok := r.nodes[key]; ok {
		return fmt.Errorf("catalog: duplicate node %s", key)
	}
	r.nodes[key] = n
	return nil
}

func (r *Registry) Lookup(name, version string) (Node, bool) {
	if r == nil {
		return Node{}, false
	}
	n, ok := r.nodes[name+"@"+version]
	return n, ok
}

func ValidateDescriptor(d Descriptor) error {
	if !regexp.MustCompile(`^[a-z][a-z0-9_/-]{0,127}$`).MatchString(d.Name) || strings.Contains(d.Name, "..") {
		return errors.New("catalog: invalid node identity")
	}
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(d.Version) {
		return errors.New("catalog: invalid node version")
	}
	if strings.TrimSpace(d.Description) == "" {
		return errors.New("catalog: description is required")
	}
	if _, err := schema.Parse(d.InputSchema); err != nil {
		return fmt.Errorf("catalog: input schema: %w", err)
	}
	if _, err := schema.Parse(d.OutputSchema); err != nil {
		return fmt.Errorf("catalog: output schema: %w", err)
	}
	if len(d.Effects) > 0 && d.Deterministic {
		return errors.New("catalog: effectful node cannot be deterministic")
	}
	return nil
}

func Validate(data []byte, contract schema.Schema) ([]byte, error) { return contract.Normalize(data) }

// SelectMap selects declared paths only. Missing fields are reported with a
// field path; it cannot call another node or execute arbitrary expressions.
func SelectMap(input []byte, fields map[string]string) ([]byte, error) {
	if len(fields) > MaxCollectionItems {
		return nil, ErrBounds
	}
	var value map[string]any
	if err := json.Unmarshal(input, &value); err != nil {
		return nil, fmt.Errorf("catalog: input: %w", err)
	}
	out := make(map[string]any, len(fields))
	for output, path := range fields {
		v, ok := lookup(value, path)
		if !ok {
			return nil, fmt.Errorf("catalog: missing field %s", path)
		}
		out[output] = v
	}
	return json.Marshal(out)
}

func lookup(value any, path string) (any, bool) {
	if path == "" || strings.HasPrefix(path, ".") || strings.Contains(path, "[") {
		return nil, false
	}
	var current any = value
	for _, part := range strings.Split(path, ".") {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

var placeholder = regexp.MustCompile(`\{\{([a-zA-Z0-9_.-]{1,128})\}\}`)

func Template(template string, input []byte, defaults map[string]string) (string, error) {
	if len([]byte(template)) > MaxTemplateBytes {
		return "", ErrBounds
	}
	var value map[string]any
	if err := json.Unmarshal(input, &value); err != nil {
		return "", fmt.Errorf("catalog: input: %w", err)
	}
	var firstErr error
	out := placeholder.ReplaceAllStringFunc(template, func(match string) string {
		path := placeholder.FindStringSubmatch(match)[1]
		if found, ok := lookup(value, path); ok {
			return format(found)
		}
		if fallback, ok := defaults[path]; ok {
			return fallback
		}
		firstErr = fmt.Errorf("catalog: missing template field %s", path)
		return match
	})
	if firstErr != nil {
		return "", firstErr
	}
	if len([]byte(out)) > MaxTemplateBytes {
		return "", ErrBounds
	}
	return out, nil
}

func format(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case json.Number:
		return x.String()
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(x)
	case nil:
		return "null"
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

func AddInt64(a, b int64) (int64, error) {
	if (b > 0 && a > math.MaxInt64-b) || (b < 0 && a < math.MinInt64-b) {
		return 0, ErrOverflow
	}
	return a + b, nil
}
func MultiplyInt64(a, b int64) (int64, error) {
	if a == 0 || b == 0 {
		return 0, nil
	}
	if a == -1 && b == math.MinInt64 || b == -1 && a == math.MinInt64 {
		return 0, ErrOverflow
	}
	if a > 0 {
		if b > 0 && a > math.MaxInt64/b || b < 0 && b < math.MinInt64/a {
			return 0, ErrOverflow
		}
	} else {
		if b > 0 && a < math.MinInt64/b || b < 0 && a < math.MaxInt64/b {
			return 0, ErrOverflow
		}
	}
	return a * b, nil
}
