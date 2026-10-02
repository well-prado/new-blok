// Package node defines typed native Go nodes and explicit application
// registration. A node performs one operation; workflows own composition.
package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"

	"github.com/well-prado/new-blok/contract/schema"
)

type Handler[I, O any] func(context.Context, I) (O, error)

type Descriptor struct {
	Name          string          `json:"name"`
	Version       string          `json:"version"`
	Description   string          `json:"description"`
	InputSchema   json.RawMessage `json:"inputSchema"`
	OutputSchema  json.RawMessage `json:"outputSchema"`
	Effects       []string        `json:"effects,omitempty"`
	Deterministic bool            `json:"deterministic"`
}

type Error struct{ Code, Message string }

func (e *Error) Error() string { return e.Code + ": " + e.Message }

type DomainError struct {
	Code      string
	Class     string
	Err       error
	Retryable bool
	Uncertain bool
}

func (e *DomainError) Error() string {
	if e.Err == nil {
		return e.Code + ": " + e.Class
	}
	return e.Code + ": " + e.Err.Error()
}
func (e *DomainError) Unwrap() error     { return e.Err }
func (e *DomainError) IsUncertain() bool { return e.Uncertain }
func (e *DomainError) IsRetryable() bool { return e.Retryable }

type Option func(*config)
type config struct {
	description               string
	inputSchema, outputSchema json.RawMessage
	effects                   []string
	deterministic             bool
}

func Description(value string) Option { return func(c *config) { c.description = value } }
func Schemas(input, output []byte) Option {
	return func(c *config) {
		c.inputSchema = append([]byte(nil), input...)
		c.outputSchema = append([]byte(nil), output...)
	}
}
func Pure() Option { return func(c *config) { c.deterministic = true; c.effects = nil } }
func Effects(values ...string) Option {
	return func(c *config) { c.effects = append([]string(nil), values...); c.deterministic = false }
}

type Definition[I, O any] struct{ any Any }

type Any struct {
	descriptor Descriptor
	inputType  reflect.Type
	outputType reflect.Type
	invoke     func(context.Context, any) (any, error)
}

func (n Any) Descriptor() Descriptor { return n.descriptor }

func Define[I, O any](name, version string, handler Handler[I, O], options ...Option) (Definition[I, O], error) {
	var c config
	for _, option := range options {
		option(&c)
	}
	d := Descriptor{Name: name, Version: version, Description: c.description, InputSchema: c.inputSchema, OutputSchema: c.outputSchema, Effects: append([]string(nil), c.effects...), Deterministic: c.deterministic}
	if err := validateDescriptor(d); err != nil {
		return Definition[I, O]{}, err
	}
	if handler == nil {
		return Definition[I, O]{}, &Error{Code: "missing_handler", Message: "node handler is required"}
	}
	var in I
	var out O
	a := Any{descriptor: d, inputType: reflect.TypeOf(in), outputType: reflect.TypeOf(out)}
	a.invoke = func(ctx context.Context, value any) (result any, err error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		input, ok := value.(I)
		if !ok {
			return nil, &Error{Code: "input_type_mismatch", Message: fmt.Sprintf("expected %T, got %T", in, value)}
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				err = &Error{Code: "node_panic", Message: fmt.Sprint(recovered)}
				result = nil
			}
		}()
		return handler(ctx, input)
	}
	return Definition[I, O]{any: a}, nil
}

func MustDefine[I, O any](name, version string, handler Handler[I, O], options ...Option) Definition[I, O] {
	definition, err := Define(name, version, handler, options...)
	if err != nil {
		panic(err)
	}
	return definition
}

func (n Definition[I, O]) Descriptor() Descriptor { return n.any.descriptor }
func (n Definition[I, O]) Any() Any               { return n.any }
func (n Definition[I, O]) Invoke(ctx context.Context, input I) (O, error) {
	var zero O
	value, err := n.any.invoke(ctx, input)
	if err != nil {
		return zero, err
	}
	output, ok := value.(O)
	if !ok {
		return zero, &Error{Code: "output_type_mismatch", Message: fmt.Sprintf("handler returned %T, descriptor requires %T", value, zero)}
	}
	return output, nil
}

func (n Any) Invoke(ctx context.Context, input any) (any, error) {
	if n.invoke == nil {
		return nil, &Error{Code: "invalid_node", Message: "node has no invocation boundary"}
	}
	return n.invoke(ctx, input)
}

func (n Any) Mock(output any, returned error) (Any, error) {
	if returned == nil {
		if err := validateRuntimeSchema(n.descriptor.OutputSchema, output); err != nil {
			return Any{}, &Error{Code: "invalid_mock_output", Message: err.Error()}
		}
	}
	copy := n
	copy.invoke = func(ctx context.Context, _ any) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return output, returned
	}
	return copy, nil
}

func validateRuntimeSchema(raw []byte, output any) error {
	parsed, err := schema.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Type == "object" && len(parsed.Properties) == 0 && len(parsed.Required) == 0 {
		return nil
	}
	data, err := json.Marshal(output)
	if err != nil {
		return err
	}
	_, err = parsed.Normalize(data)
	return err
}

type Registry struct{ nodes map[string]Any }

func NewRegistry() *Registry { return &Registry{nodes: map[string]Any{}} }
func (r *Registry) Register(node Any) error {
	if r == nil {
		return &Error{Code: "nil_registry", Message: "registry is nil"}
	}
	if err := validateDescriptor(node.descriptor); err != nil {
		return err
	}
	key := node.descriptor.Name + "@" + node.descriptor.Version
	if previous, exists := r.nodes[key]; exists {
		if string(previous.descriptor.InputSchema) != string(node.descriptor.InputSchema) || string(previous.descriptor.OutputSchema) != string(node.descriptor.OutputSchema) {
			return &Error{Code: "incompatible_schema_descriptor", Message: "node identity/version has incompatible schemas"}
		}
		return &Error{Code: "duplicate_node", Message: "node identity/version is already registered"}
	}
	r.nodes[key] = node
	return nil
}
func (r *Registry) Lookup(name, version string) (Any, bool) {
	if r == nil {
		return Any{}, false
	}
	value, ok := r.nodes[name+"@"+version]
	return value, ok
}

func validateDescriptor(d Descriptor) error {
	if !regexp.MustCompile(`^[a-z][a-z0-9_/-]{0,127}$`).MatchString(d.Name) || strings.Contains(d.Name, "..") {
		return &Error{Code: "invalid_node_identity", Message: "node name must be a stable namespace, not a path"}
	}
	if !regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(d.Version) {
		return &Error{Code: "invalid_node_version", Message: "node version must be major.minor.patch"}
	}
	if strings.TrimSpace(d.Description) == "" {
		return &Error{Code: "missing_node_metadata", Message: "node description is required"}
	}
	if _, err := schema.Parse(d.InputSchema); err != nil {
		return &Error{Code: "invalid_input_schema", Message: err.Error()}
	}
	if _, err := schema.Parse(d.OutputSchema); err != nil {
		return &Error{Code: "invalid_output_schema", Message: err.Error()}
	}
	return nil
}

func AsDomainError(err error) (*DomainError, bool) {
	var target *DomainError
	return target, errors.As(err, &target)
}
