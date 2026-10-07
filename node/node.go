// Package node defines typed native Go nodes and explicit application
// registration. A node performs one operation; workflows own composition.
package node

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	"github.com/well-prado/new-blok/contract/schema"
)

type Handler[I, O any] func(context.Context, I) (O, error)

type Descriptor struct {
	Name                 string          `json:"name"`
	Version              string          `json:"version"`
	Description          string          `json:"description"`
	InputSchema          json.RawMessage `json:"inputSchema"`
	OutputSchema         json.RawMessage `json:"outputSchema"`
	Effects              []string        `json:"effects,omitempty"`
	RequiredCapabilities []string        `json:"requiredCapabilities,omitempty"`
	Deterministic        bool            `json:"deterministic"`
}

type Error struct{ Code, Message string }

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// ClassNotFound is the DomainError class for a record the caller cannot
// see: one that does not exist, or one that exists but the caller may not
// read. Return the same error (code and class) in both cases, so that no
// trigger can tell them apart: HTTP answers 404 and gRPC NotFound with the
// error's code, and only that code reaches the caller. It is terminal: the
// framework never retries it (#306, ADR 0005). trigger.ClassNotFound is the
// same value on the adapter side.
const ClassNotFound = "not_found"

// DomainError is a node failure with a stable public Code and Class. Triggers
// map the class to their protocol (validation, not_found, ...) and expose
// only the code; Err stays private.
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
func (e *DomainError) Unwrap() error      { return e.Err }
func (e *DomainError) IsUncertain() bool  { return e.Uncertain }
func (e *DomainError) IsRetryable() bool  { return e.Retryable }
func (e *DomainError) ErrorCode() string  { return e.Code }
func (e *DomainError) ErrorClass() string { return e.Class }

type Option func(*config)
type config struct {
	description               string
	inputSchema, outputSchema json.RawMessage
	effects                   []string
	requiredCapabilities      []string
	remote                    bool
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

// RequiredCapabilities records the trusted boundary's actual dispatch needs.
func RequiredCapabilities(values ...string) Option {
	return func(c *config) { c.requiredCapabilities = append([]string(nil), values...) }
}

// RemoteBoundary marks a foreign handler. Remote token-using agent tools fail
// closed until an adapter can actually enforce their reserved token budget.
func RemoteBoundary() Option { return func(c *config) { c.remote = true } }

type Definition[I, O any] struct{ any Any }

type Any struct {
	descriptor Descriptor
	inputType  reflect.Type
	outputType reflect.Type
	invoke     func(context.Context, any) (any, error)
	remote     bool
}

func (n Any) Descriptor() Descriptor { return cloneDescriptor(n.descriptor) }
func (n Any) IsRemote() bool         { return n.remote }
func cloneDescriptor(d Descriptor) Descriptor {
	d.InputSchema = append(json.RawMessage(nil), d.InputSchema...)
	d.OutputSchema = append(json.RawMessage(nil), d.OutputSchema...)
	d.Effects = append([]string(nil), d.Effects...)
	d.RequiredCapabilities = append([]string(nil), d.RequiredCapabilities...)
	return d
}

func Define[I, O any](name, version string, handler Handler[I, O], options ...Option) (Definition[I, O], error) {
	var c config
	for _, option := range options {
		option(&c)
	}
	d := Descriptor{Name: name, Version: version, Description: c.description, InputSchema: c.inputSchema, OutputSchema: c.outputSchema, Effects: append([]string(nil), c.effects...), RequiredCapabilities: append([]string(nil), c.requiredCapabilities...), Deterministic: c.deterministic}
	if err := validateDescriptor(d); err != nil {
		return Definition[I, O]{}, err
	}
	if handler == nil {
		return Definition[I, O]{}, &Error{Code: "missing_handler", Message: "node handler is required"}
	}
	var in I
	var out O
	a := Any{descriptor: d, inputType: reflect.TypeOf(in), outputType: reflect.TypeOf(out), remote: c.remote}
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

func (n Definition[I, O]) Descriptor() Descriptor { return n.any.Descriptor() }
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

// DecodeOutput restores a JSON-persisted node result to the node's declared
// Go output type. Durable execution adapters use this when resuming a
// completed step; it does not invoke the node or establish trust in the data.
func (n Any) DecodeOutput(data []byte) (any, error) {
	if n.outputType == nil {
		var value any
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			if err == nil {
				return nil, errors.New("node: persisted output contains multiple JSON values")
			}
			return nil, err
		}
		return value, nil
	}
	value := reflect.New(n.outputType)
	if err := json.Unmarshal(data, value.Interface()); err != nil {
		return nil, err
	}
	return value.Elem().Interface(), nil
}

// ConvertInput returns value as the node's declared Go input type when it
// has an exact reading as that type, and value unchanged otherwise, so that
// Invoke refuses it with input_type_mismatch as it refuses any other value
// of the wrong type. The engine calls it only for a value a control
// construct produced (ADR 0028): a JSON literal, an each's results, a value
// a construct handed on. It never invents a value the source does not hold:
//
//   - a value of the input type, or any value for an interface input type,
//     is returned as is;
//   - a non-nil pointer to the input type is dereferenced;
//   - nil (or a nil pointer) becomes the zero value only of a type that can
//     itself be nil (pointer, slice, map, interface);
//   - a JSON value (nil, bool, string, json.Number, float64, map[string]any,
//     []any of JSON values) is decoded into the type, refusing a field the
//     type does not declare;
//   - a slice or array converts element by element into a slice type.
//
// Values are never re-encoded through their own MarshalJSON, so a type that
// encodes itself as something else is never read as that.
func (n Any) ConvertInput(value any) any {
	if n.inputType == nil || n.inputType.Kind() == reflect.Interface {
		return value
	}
	if converted, ok := convertInput(value, n.inputType); ok {
		return converted.Interface()
	}
	return value
}

func convertInput(value any, target reflect.Type) (reflect.Value, bool) {
	if value == nil {
		return nilOf(target)
	}
	current := reflect.ValueOf(value)
	switch {
	case current.Type() == target:
		return current, true
	case current.Kind() == reflect.Pointer && current.IsNil():
		return nilOf(target)
	case current.Kind() == reflect.Pointer && current.Type().Elem() == target:
		return current.Elem(), true
	case jsonValue(value):
		data, err := json.Marshal(value)
		if err != nil {
			return reflect.Value{}, false
		}
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		converted := reflect.New(target)
		if err := decoder.Decode(converted.Interface()); err != nil {
			return reflect.Value{}, false
		}
		return converted.Elem(), true
	case (current.Kind() == reflect.Slice || current.Kind() == reflect.Array) && target.Kind() == reflect.Slice:
		if current.Kind() == reflect.Slice && current.IsNil() {
			return reflect.Zero(target), true
		}
		converted := reflect.MakeSlice(target, current.Len(), current.Len())
		for index := 0; index < current.Len(); index++ {
			element, ok := convertInput(current.Index(index).Interface(), target.Elem())
			if !ok {
				return reflect.Value{}, false
			}
			converted.Index(index).Set(element)
		}
		return converted, true
	}
	return reflect.Value{}, false
}

// nilOf is target's zero value when null has a reading as target.
func nilOf(target reflect.Type) (reflect.Value, bool) {
	switch target.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
		return reflect.Zero(target), true
	}
	return reflect.Value{}, false
}

// jsonValue reports whether value is made only of the values JSON decodes
// into an interface: nil, bool, string, json.Number, float64,
// map[string]any and []any of them.
func jsonValue(value any) bool {
	switch typed := value.(type) {
	case nil, bool, string, json.Number, float64:
		return true
	case map[string]any:
		for _, member := range typed {
			if !jsonValue(member) {
				return false
			}
		}
		return true
	case []any:
		for _, element := range typed {
			if !jsonValue(element) {
				return false
			}
		}
		return true
	}
	return false
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

// ValidateDescriptor applies the same declaration rules at registry and foreign
// catalog boundaries. Canonical hashing alone is not descriptor validation.
func ValidateDescriptor(d Descriptor) error { return validateDescriptor(d) }

// The descriptor grammars are compiled once: Define runs on every invoke of a
// catalog workflow tool, and recompiling them was about a quarter of its CPU
// (#319). They are not contract.IDPattern: node names allow "/" and run to 128
// bytes, document ids do neither.
var (
	capabilityGrammar = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$`)
	nameGrammar       = regexp.MustCompile(`^[a-z][a-z0-9_/-]{0,127}$`)
	versionGrammar    = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)
)

func validateDescriptor(d Descriptor) error {
	if len(d.RequiredCapabilities) > 128 {
		return &Error{Code: "invalid_capability", Message: "too many required capabilities"}
	}
	seen := map[string]bool{}
	for _, cap := range d.RequiredCapabilities {
		if seen[cap] || !capabilityGrammar.MatchString(cap) || strings.Contains(cap, "orchestrate") {
			return &Error{Code: "invalid_capability", Message: "invalid required capability"}
		}
		seen[cap] = true
	}
	if !nameGrammar.MatchString(d.Name) || strings.Contains(d.Name, "..") {
		return &Error{Code: "invalid_node_identity", Message: "node name must be a stable namespace, not a path"}
	}
	if !versionGrammar.MatchString(d.Version) {
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
