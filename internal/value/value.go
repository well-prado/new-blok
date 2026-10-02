// Package value owns native value isolation and portable normalization boundaries.
package value

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/well-prado/new-blok/contract/schema"
)

type visit struct {
	typ reflect.Type
	ptr uintptr
}

func Clone[T any](input T) (T, error) {
	cloned, err := clone(reflect.ValueOf(input), map[visit]bool{})
	if err != nil {
		var zero T
		return zero, err
	}
	if !cloned.IsValid() {
		var zero T
		return zero, nil
	}
	output, ok := cloned.Interface().(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("value_type_mismatch: cloned value has type %s", cloned.Type())
	}
	return output, nil
}

func clone(input reflect.Value, seen map[visit]bool) (reflect.Value, error) {
	if !input.IsValid() {
		return reflect.Value{}, nil
	}
	switch input.Kind() {
	case reflect.Interface:
		if input.IsNil() {
			return reflect.Zero(input.Type()), nil
		}
		inner, err := clone(input.Elem(), seen)
		if err != nil {
			return reflect.Value{}, err
		}
		output := reflect.New(input.Type()).Elem()
		output.Set(inner)
		return output, nil
	case reflect.Pointer:
		if input.IsNil() {
			return reflect.Zero(input.Type()), nil
		}
		key := visit{typ: input.Type(), ptr: input.Pointer()}
		if seen[key] {
			return reflect.Value{}, fmt.Errorf("cyclic_value: pointer graph contains a cycle")
		}
		seen[key] = true
		defer delete(seen, key)
		output := reflect.New(input.Type().Elem())
		inner, err := clone(input.Elem(), seen)
		if err != nil {
			return reflect.Value{}, err
		}
		output.Elem().Set(inner)
		return output, nil
	case reflect.Map:
		if input.IsNil() {
			return reflect.Zero(input.Type()), nil
		}
		key := visit{typ: input.Type(), ptr: input.Pointer()}
		if seen[key] {
			return reflect.Value{}, fmt.Errorf("cyclic_value: map graph contains a cycle")
		}
		seen[key] = true
		defer delete(seen, key)
		output := reflect.MakeMapWithSize(input.Type(), input.Len())
		for _, mapKey := range input.MapKeys() {
			clonedKey, err := clone(mapKey, seen)
			if err != nil {
				return reflect.Value{}, err
			}
			clonedValue, err := clone(input.MapIndex(mapKey), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			output.SetMapIndex(clonedKey, clonedValue)
		}
		return output, nil
	case reflect.Slice:
		if input.IsNil() {
			return reflect.Zero(input.Type()), nil
		}
		output := reflect.MakeSlice(input.Type(), input.Len(), input.Len())
		for index := 0; index < input.Len(); index++ {
			cloned, err := clone(input.Index(index), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			output.Index(index).Set(cloned)
		}
		return output, nil
	case reflect.Array:
		output := reflect.New(input.Type()).Elem()
		for index := 0; index < input.Len(); index++ {
			cloned, err := clone(input.Index(index), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			output.Index(index).Set(cloned)
		}
		return output, nil
	case reflect.Struct:
		output := reflect.New(input.Type()).Elem()
		for index := 0; index < input.NumField(); index++ {
			if !output.Field(index).CanSet() {
				return reflect.Value{}, fmt.Errorf("unsupported_unexported_field: %s.%s", input.Type(), input.Type().Field(index).Name)
			}
			cloned, err := clone(input.Field(index), seen)
			if err != nil {
				return reflect.Value{}, err
			}
			output.Field(index).Set(cloned)
		}
		return output, nil
	default:
		return input, nil
	}
}

func Normalize[T any](contract schema.Schema, input T) (T, error) {
	data, err := json.Marshal(input)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("encode_native_value: %w", err)
	}
	normalized, err := contract.Normalize(data)
	if err != nil {
		var zero T
		return zero, err
	}
	var wireValue any
	if err := json.Unmarshal(normalized, &wireValue); err != nil {
		var zero T
		return zero, fmt.Errorf("decode_normalized_value: %w", err)
	}
	nativeValue, err := decodeNative(contract, wireValue)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("decode_normalized_value: %w", err)
	}
	nativeData, err := json.Marshal(nativeValue)
	if err != nil {
		var zero T
		return zero, fmt.Errorf("encode_native_value: %w", err)
	}
	var output T
	if err := json.Unmarshal(nativeData, &output); err != nil {
		var zero T
		return zero, fmt.Errorf("decode_normalized_value: %w", err)
	}
	return output, nil
}

func decodeNative(contract schema.Schema, value any) (any, error) {
	if value == nil {
		return nil, nil
	}
	if len(contract.AnyOf) > 0 {
		for _, candidate := range contract.AnyOf {
			if decoded, err := decodeNative(candidate, value); err == nil {
				return decoded, nil
			}
		}
		return nil, fmt.Errorf("no native union branch matched")
	}
	switch contract.Type {
	case "integer":
		if contract.Wire == "int64-string" {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected int64 string")
			}
			parsed, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, err
			}
			return parsed, nil
		}
	case "number":
		if contract.Wire == "decimal-string" {
			text, ok := value.(string)
			if !ok {
				return nil, fmt.Errorf("expected decimal string")
			}
			return text, nil
		}
	case "object":
		object, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected object")
		}
		for name, child := range contract.Properties {
			if present, exists := object[name]; exists {
				decoded, err := decodeNative(child, present)
				if err != nil {
					return nil, err
				}
				object[name] = decoded
			}
		}
		return object, nil
	case "array":
		items, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("expected array")
		}
		for index, item := range items {
			decoded, err := decodeNative(*contract.Items, item)
			if err != nil {
				return nil, err
			}
			items[index] = decoded
		}
		return items, nil
	}
	return value, nil
}

type BlobRef struct {
	Digest    string    `json:"digest"`
	SizeBytes int64     `json:"sizeBytes"`
	MediaType string    `json:"mediaType"`
	ExpiresAt time.Time `json:"expiresAt"`
}

type BlobPolicy struct {
	MaxBytes      int64
	RequireExpiry bool
}

func (b BlobRef) Validate(policy BlobPolicy, now time.Time) error {
	if !strings.HasPrefix(b.Digest, "sha256:") || len(b.Digest) != len("sha256:")+64 {
		return fmt.Errorf("invalid_blob_digest: digest must be a sha256 reference")
	}
	if b.SizeBytes < 0 || (policy.MaxBytes > 0 && b.SizeBytes > policy.MaxBytes) {
		return fmt.Errorf("blob_size_exceeded: blob size is outside the allowed limit")
	}
	if strings.TrimSpace(b.MediaType) == "" {
		return fmt.Errorf("missing_blob_media_type: media type is required")
	}
	if policy.RequireExpiry && (b.ExpiresAt.IsZero() || !b.ExpiresAt.After(now)) {
		return fmt.Errorf("expired_blob_reference: blob reference must expire in the future")
	}
	return nil
}
