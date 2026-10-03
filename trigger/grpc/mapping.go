package grpc

import (
	"math"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/well-prado/new-blok/contract/schema"
)

// The mapping between protobuf messages and domain values is protojson
// with proto field names: int64 kinds carry the int64-string wire form,
// bytes are base64 strings, enums are their names. Kinds without a
// portable domain form (oneofs, maps, uint64/fixed64, groups and the
// google.protobuf well-known types) are refused at startup rather than
// guessed.

func mappingError(path, code, reason string) error {
	return &MappingError{Path: path, Code: code, Reason: reason}
}

func isOpen(s schema.Schema) bool { return s.AdditionalProperties != nil && *s.AdditionalProperties }

// requestMaps proves that a request message maps to values of the binding's
// input schema shape: every field has a property of its kind and every
// property a field. Ranges and required fields are enforced per call.
func requestMaps(message protoreflect.MessageDescriptor, s schema.Schema, path string) error {
	if err := supported(message, path); err != nil {
		return err
	}
	if s.Type != "object" || len(s.AnyOf) > 0 {
		return mappingError(path, "kind_mismatch", "a message maps to an object schema")
	}
	fields := message.Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		name := string(field.Name())
		property, ok := s.Properties[name]
		if !ok {
			if isOpen(s) {
				// Passed through unchecked by the schema, but still a kind
				// with a portable domain form.
				if err := representable(field, path+"."+name, map[protoreflect.FullName]bool{}); err != nil {
					return err
				}
				continue
			}
			return mappingError(path+"."+name, "unmapped_field", "the request field has no property in the input schema")
		}
		if err := fieldMaps(field, property, path+"."+name, requestMaps); err != nil {
			return err
		}
	}
	for name := range s.Properties {
		if fields.ByName(protoreflect.Name(name)) == nil {
			return mappingError(path+"."+name, "missing_field", "the input schema property has no request field")
		}
	}
	return nil
}

// responseMaps proves that every value of the workflow's output schema can
// be encoded in the response message: a closed object whose properties are
// all fields of a kind and range that can hold them.
func responseMaps(message protoreflect.MessageDescriptor, s schema.Schema, path string) error {
	if err := supported(message, path); err != nil {
		return err
	}
	if s.Type != "object" || len(s.AnyOf) > 0 {
		return mappingError(path, "kind_mismatch", "a message maps to an object schema")
	}
	if isOpen(s) {
		return mappingError(path, "open_output", "an open output object may hold properties the response cannot carry")
	}
	fields := message.Fields()
	for name, property := range s.Properties {
		field := fields.ByName(protoreflect.Name(name))
		if field == nil {
			return mappingError(path+"."+name, "unmapped_property", "the output property has no response field")
		}
		if err := fieldMaps(field, property, path+"."+name, responseMaps); err != nil {
			return err
		}
		if err := fitsRange(field, property, path+"."+name); err != nil {
			return err
		}
	}
	return nil
}

// supported refuses message shapes without a portable domain form.
func supported(message protoreflect.MessageDescriptor, path string) error {
	if strings.HasPrefix(string(message.FullName()), "google.protobuf.") {
		return mappingError(path, "well_known_unsupported", "google.protobuf well-known types are not mapped")
	}
	oneofs := message.Oneofs()
	for i := 0; i < oneofs.Len(); i++ {
		if !oneofs.Get(i).IsSynthetic() {
			return mappingError(path+"."+string(oneofs.Get(i).Name()), "oneof_unsupported", "oneofs are not mapped")
		}
	}
	return nil
}

// representable refuses a field without a portable domain form, whatever
// schema receives it: maps, uint64/fixed64, groups, oneofs and well-known
// types, at any depth. seen guards recursive messages.
func representable(field protoreflect.FieldDescriptor, path string, seen map[protoreflect.FullName]bool) error {
	switch {
	case field.IsMap():
		return mappingError(path, "map_unsupported", "map fields are not mapped")
	case field.Kind() == protoreflect.Uint64Kind, field.Kind() == protoreflect.Fixed64Kind:
		return mappingError(path, "uint64_unsupported", "uint64 values beyond int64 have no portable domain form")
	case field.Kind() == protoreflect.GroupKind:
		return mappingError(path, "group_unsupported", "groups are not mapped")
	case field.Kind() != protoreflect.MessageKind:
		return nil
	}
	message := field.Message()
	if err := supported(message, path); err != nil {
		return err
	}
	if seen[message.FullName()] {
		return nil
	}
	seen[message.FullName()] = true
	fields := message.Fields()
	for i := 0; i < fields.Len(); i++ {
		if err := representable(fields.Get(i), path+"."+string(fields.Get(i).Name()), seen); err != nil {
			return err
		}
	}
	return nil
}

// fieldMaps checks one field against a property; nested messages recurse
// with the direction's own rule.
func fieldMaps(field protoreflect.FieldDescriptor, s schema.Schema, path string, nested func(protoreflect.MessageDescriptor, schema.Schema, string) error) error {
	if field.IsMap() {
		return mappingError(path, "map_unsupported", "map fields are not mapped")
	}
	if field.IsList() {
		if s.Type != "array" || s.Items == nil {
			return mappingError(path, "kind_mismatch", "a repeated field maps to an array schema with items")
		}
		s = *s.Items
	}
	if len(s.AnyOf) > 0 {
		return mappingError(path, "kind_mismatch", "anyOf is not mapped")
	}
	kind := field.Kind()
	want := func(ok bool, expected string) error {
		if ok {
			return nil
		}
		return mappingError(path, "kind_mismatch", "a "+kind.String()+" field maps to "+expected)
	}
	switch kind {
	case protoreflect.MessageKind:
		if s.Type != "object" {
			return want(false, "an object schema")
		}
		return nested(field.Message(), s, path)
	case protoreflect.GroupKind:
		return mappingError(path, "group_unsupported", "groups are not mapped")
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return mappingError(path, "uint64_unsupported", "uint64 values beyond int64 have no portable domain form")
	case protoreflect.StringKind:
		return want(s.Type == "string" && s.Format != "byte", "a string schema")
	case protoreflect.EnumKind:
		return want(s.Type == "string" && s.Format == "", "a string schema of enum names")
	case protoreflect.BytesKind:
		return want(s.Type == "string" && s.Format == "byte", `a string schema with format "byte" (base64)`)
	case protoreflect.BoolKind:
		return want(s.Type == "boolean", "a boolean schema")
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return want(s.Type == "integer" && s.Wire == "", "an integer schema in JSON number form")
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return want(s.Type == "integer" && s.Wire == "int64-string", `an integer schema with wire "int64-string"`)
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return want(s.Type == "number" && s.Wire == "", "a number schema")
	}
	return mappingError(path, "kind_unsupported", "the field kind is not mapped")
}

// fitsRange proves an output integer fits its 32-bit field.
func fitsRange(field protoreflect.FieldDescriptor, s schema.Schema, path string) error {
	if field.IsList() && s.Items != nil {
		s = *s.Items
	}
	low, high := int64(math.MinInt32), int64(math.MaxInt32)
	switch field.Kind() {
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		low, high = 0, math.MaxUint32
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
	default:
		return nil
	}
	if s.Minimum == nil || s.Maximum == nil || *s.Minimum < low || *s.Maximum > high {
		return mappingError(path, "range_unproven", "an output integer for a 32-bit field needs bounds within its range")
	}
	return nil
}
