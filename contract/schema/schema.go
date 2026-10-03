// Package schema validates and normalizes the bounded portable value subset.
package schema

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const MaxPayloadBytes = 1 << 20
const MaxDepth = 64

type Error struct{ Code, Path, Message string }

func (e *Error) Error() string {
	if e.Path == "" {
		return e.Code + ": " + e.Message
	}
	return e.Code + " at " + e.Path + ": " + e.Message
}

type Schema struct {
	Type                 string            `json:"type"`
	Properties           map[string]Schema `json:"properties,omitempty"`
	Required             []string          `json:"required,omitempty"`
	Items                *Schema           `json:"items,omitempty"`
	AnyOf                []Schema          `json:"anyOf,omitempty"`
	Nullable             bool              `json:"nullable,omitempty"`
	AdditionalProperties *bool             `json:"additionalProperties,omitempty"`
	Default              json.RawMessage   `json:"default,omitempty"`
	Minimum              *int64            `json:"minimum,omitempty"`
	Maximum              *int64            `json:"maximum,omitempty"`
	Wire                 string            `json:"wire,omitempty"`
	Format               string            `json:"format,omitempty"`
}

func Parse(data []byte) (Schema, error) {
	if len(data) == 0 || len(data) > MaxPayloadBytes {
		return Schema{}, &Error{Code: "schema_too_large", Message: "schema must be between 1 byte and 1 MiB"}
	}
	var s Schema
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return Schema{}, &Error{Code: "invalid_schema", Message: err.Error()}
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return Schema{}, &Error{Code: "trailing_data", Message: "schema has trailing JSON values"}
	}
	if err := s.Validate(); err != nil {
		return Schema{}, err
	}
	return s, nil
}

func (s Schema) Validate() error {
	return s.validate("$", 0)
}

func (s Schema) validate(path string, depth int) error {
	if depth > MaxDepth {
		return &Error{Code: "depth_exceeded", Path: path, Message: "schema exceeds maximum depth"}
	}
	valid := map[string]bool{"string": true, "boolean": true, "integer": true, "number": true, "object": true, "array": true, "null": true}
	if len(s.AnyOf) == 0 && !valid[s.Type] {
		return &Error{Code: "unsupported_schema", Path: path, Message: "schema type is not supported"}
	}
	if s.Minimum != nil && s.Maximum != nil && *s.Minimum > *s.Maximum {
		return &Error{Code: "invalid_range", Path: path, Message: "minimum exceeds maximum"}
	}
	if s.Type == "integer" && s.Wire != "" && s.Wire != "int64-string" {
		return &Error{Code: "unsupported_schema", Path: path, Message: "integer wire encoding is unsupported"}
	}
	if s.Type == "number" && s.Wire != "" && s.Wire != "decimal-string" {
		return &Error{Code: "unsupported_schema", Path: path, Message: "number wire encoding is unsupported"}
	}
	if s.Type == "object" {
		for name, child := range s.Properties {
			if err := child.validate(path+"."+name, depth+1); err != nil {
				return err
			}
		}
		for _, name := range s.Required {
			if _, ok := s.Properties[name]; !ok {
				return &Error{Code: "invalid_schema", Path: path + ".required", Message: "required field is not declared"}
			}
		}
	}
	if s.Type == "array" {
		if s.Items == nil {
			return &Error{Code: "invalid_schema", Path: path + ".items", Message: "array schema requires items"}
		}
		if err := s.Items.validate(path+"[]", depth+1); err != nil {
			return err
		}
	}
	for i, child := range s.AnyOf {
		if err := child.validate(fmt.Sprintf("%s.anyOf[%d]", path, i), depth+1); err != nil {
			return err
		}
	}
	if len(s.Default) > 0 {
		if _, err := s.normalize(s.Default, path, depth); err != nil {
			return &Error{Code: "invalid_default", Path: path + ".default", Message: err.Error()}
		}
	}
	return nil
}

func (s Schema) ValidateValue(data []byte) error {
	if len(data) > MaxPayloadBytes {
		return &Error{Code: "payload_too_large", Message: "value exceeds 1 MiB"}
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return &Error{Code: "invalid_value", Message: err.Error()}
	}
	if err := checkDepth(v, 0, "$"); err != nil {
		return err
	}
	_, err := s.normalizeValue(v, "$")
	return err
}

// NormalizeValue applies the same normalized contract to an already-decoded
// native value. Wire adapters can compare this result with Normalize's JSON
// result without using floating-point conversion for int64 values.
func (s Schema) NormalizeValue(value any) (any, error) {
	if err := checkDepth(value, 0, "$"); err != nil {
		return nil, err
	}
	return s.normalizeValue(value, "$")
}

func (s Schema) Normalize(data []byte) ([]byte, error) {
	if len(data) > MaxPayloadBytes {
		return nil, &Error{Code: "payload_too_large", Message: "value exceeds 1 MiB"}
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, &Error{Code: "invalid_value", Message: err.Error()}
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		return nil, &Error{Code: "trailing_data", Message: "value has trailing JSON values"}
	}
	if err := checkDepth(v, 0, "$"); err != nil {
		return nil, err
	}
	out, err := s.normalizeValue(v, "$")
	if err != nil {
		return nil, err
	}
	return json.Marshal(out)
}

func (s Schema) normalize(raw json.RawMessage, path string, depth int) (any, error) {
	var v any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return s.normalizeValueAt(v, path, depth)
}
func (s Schema) normalizeValue(v any, path string) (any, error) {
	return s.normalizeValueAt(v, path, 0)
}
func (s Schema) normalizeValueAt(v any, path string, depth int) (any, error) {
	if depth > MaxDepth {
		return nil, &Error{Code: "depth_exceeded", Path: path, Message: "value exceeds maximum depth"}
	}
	if v == nil && s.Nullable {
		return nil, nil
	}
	if len(s.AnyOf) > 0 {
		var match any
		matches := 0
		for _, candidate := range s.AnyOf {
			got, err := candidate.normalizeValueAt(v, path, depth+1)
			if err == nil {
				match = got
				matches++
			}
		}
		if matches != 1 {
			return nil, &Error{Code: "compatibility_unproven", Path: path, Message: "value does not match exactly one union branch"}
		}
		return match, nil
	}
	if v == nil {
		if s.Type == "null" {
			return nil, nil
		}
		return nil, &Error{Code: "null_not_allowed", Path: path, Message: "null is not allowed"}
	}
	switch s.Type {
	case "object":
		obj, ok := v.(map[string]any)
		if !ok {
			return nil, typeError(path, "object")
		}
		out := map[string]any{}
		for name, value := range obj {
			child, exists := s.Properties[name]
			if !exists {
				if s.AdditionalProperties == nil || !*s.AdditionalProperties {
					return nil, &Error{Code: "unknown_field", Path: path + "." + name, Message: "field is not declared"}
				}
				out[name] = value
				continue
			}
			normalized, err := child.normalizeValueAt(value, path+"."+name, depth+1)
			if err != nil {
				return nil, err
			}
			out[name] = normalized
		}
		for _, name := range s.Required {
			if _, exists := obj[name]; !exists {
				child := s.Properties[name]
				if len(child.Default) > 0 {
					normalized, err := child.normalize(child.Default, path+"."+name, depth+1)
					if err != nil {
						return nil, err
					}
					out[name] = normalized
				} else {
					return nil, &Error{Code: "missing_required", Path: path + "." + name, Message: "required field is absent"}
				}
			}
		}
		for name, child := range s.Properties {
			if _, exists := obj[name]; !exists {
				if len(child.Default) > 0 {
					normalized, err := child.normalize(child.Default, path+"."+name, depth+1)
					if err != nil {
						return nil, err
					}
					out[name] = normalized
				}
			}
		}
		return out, nil
	case "array":
		arr, ok := v.([]any)
		if !ok {
			return nil, typeError(path, "array")
		}
		out := make([]any, len(arr))
		for i, item := range arr {
			normalized, err := s.Items.normalizeValueAt(item, fmt.Sprintf("%s[%d]", path, i), depth+1)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	case "string":
		value, ok := v.(string)
		if !ok {
			return nil, typeError(path, "string")
		}
		if err := validateString(value, s.Format); err != nil {
			return nil, &Error{Code: "invalid_format", Path: path, Message: err.Error()}
		}
		return value, nil
	case "boolean":
		if _, ok := v.(bool); !ok {
			return nil, typeError(path, "boolean")
		}
		return v, nil
	case "integer":
		return normalizeInteger(v, s, path)
	case "number":
		return normalizeNumber(v, s, path)
	case "null":
		return nil, typeError(path, "null")
	default:
		return nil, &Error{Code: "unsupported_schema", Path: path, Message: "schema type is not supported"}
	}
}

func normalizeInteger(v any, s Schema, path string) (any, error) {
	var n int64
	switch value := v.(type) {
	case string:
		if s.Wire != "int64-string" {
			return nil, typeError(path, "integer")
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, &Error{Code: "integer_range", Path: path, Message: "int64 string is out of range"}
		}
		n = parsed
	case int:
		n = int64(value)
	case int8:
		n = int64(value)
	case int16:
		n = int64(value)
	case int32:
		n = int64(value)
	case int64:
		n = value
	case json.Number:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return nil, &Error{Code: "integer_range", Path: path, Message: "integer is out of int64 range"}
		}
		n = parsed
	default:
		return nil, typeError(path, "integer")
	}
	if s.Minimum != nil && n < *s.Minimum || s.Maximum != nil && n > *s.Maximum {
		return nil, &Error{Code: "integer_range", Path: path, Message: "integer is outside the declared range"}
	}
	if s.Wire == "int64-string" {
		return strconv.FormatInt(n, 10), nil
	}
	return json.Number(strconv.FormatInt(n, 10)), nil
}
func normalizeNumber(v any, s Schema, path string) (any, error) {
	if s.Wire == "decimal-string" {
		text, ok := v.(string)
		if !ok || !regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`).MatchString(text) {
			return nil, &Error{Code: "decimal_invalid", Path: path, Message: "decimal must be a canonical decimal string"}
		}
		return text, nil
	}
	n, ok := v.(json.Number)
	if !ok {
		return nil, typeError(path, "number")
	}
	if _, err := strconv.ParseFloat(string(n), 64); err != nil {
		return nil, &Error{Code: "number_invalid", Path: path, Message: "number is invalid"}
	}
	return n, nil
}
func validateString(v, format string) error {
	switch format {
	case "date-time":
		if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
			return err
		}
	case "byte":
		if _, err := base64.StdEncoding.DecodeString(v); err != nil {
			return err
		}
	case "blob-ref":
		if !strings.HasPrefix(v, "blob://") {
			return fmt.Errorf("blob reference must use blob://")
		}
	}
	return nil
}
func typeError(path, want string) error {
	return &Error{Code: "type_mismatch", Path: path, Message: "expected " + want}
}
func checkDepth(v any, depth int, path string) error {
	if depth > MaxDepth {
		return &Error{Code: "depth_exceeded", Path: path, Message: "value exceeds maximum depth"}
	}
	switch x := v.(type) {
	case map[string]any:
		for name, child := range x {
			if err := checkDepth(child, depth+1, path+"."+name); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range x {
			if err := checkDepth(child, depth+1, fmt.Sprintf("%s[%d]", path, i)); err != nil {
				return err
			}
		}
	}
	return nil
}

func Compatible(from, to Schema) error {
	if err := from.Validate(); err != nil {
		return err
	}
	if err := to.Validate(); err != nil {
		return err
	}
	if from.Type != to.Type || from.Wire != to.Wire {
		return &Error{Code: "compatibility_unproven", Message: "schema type or wire representation changed"}
	}
	if from.Type == "object" {
		for name := range to.Properties {
			if _, ok := from.Properties[name]; !ok {
				return &Error{Code: "compatibility_unproven", Path: "." + name, Message: "new object field has no proven source value"}
			}
		}
	}
	if from.Type == "integer" && from.Minimum != nil && to.Minimum != nil && *from.Minimum < *to.Minimum {
		return &Error{Code: "compatibility_unproven", Message: "integer range narrowed without migration evidence"}
	}
	return nil
}
