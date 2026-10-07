package engine

import (
	"bytes"
	"encoding"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/well-prado/new-blok/internal/jsontag"
)

// Field references select encoding/json object keys (#241, ADR 0001).
//
// A value reaches a reference either still typed, as a node returned it, or
// after a JSON boundary (a checkpoint, a foreign runtime, a child workflow)
// as decoded maps. Both must resolve a path the same way, so the key set of
// an object is the one encoding/json emits for it: promoted fields of
// embedded structs flattened, json:"-" fields absent, a tagged field only
// under its tag name, empty omitempty and zero omitzero fields absent,
// ,string fields as their quoted text, and values with their own encoder
// (MarshalJSON, MarshalJSONTo, MarshalText, AppendText) as written.
//
// Work is bounded by the selected member, never by its siblings. A struct
// resolves through a per-type key index, built once per type, for json tags
// in plain form (internal/jsontag): there the key is the tag's name part and
// each option's effect is known. A struct with any other tag (quoted parts,
// malformed options such as ",omitempty ", which encoding/json still
// applies) resolves against its whole encoding instead. omitempty and
// omitzero are decided from the selected field alone; a ,string scalar is
// the only member encoded. Map keys are named without
// encoding values. Only a container whose own type has a custom encoder
// (MarshalJSON, MarshalJSONTo, MarshalText or AppendText) is encoded whole,
// because its keys exist nowhere else.
//
// Supported and tested: Go 1.27's default, v2-backed encoding/json. The
// GOEXPERIMENT=nojsonv2 (v1) implementation is untested: this repository
// does not build in that mode (observation.go imports encoding/json/v2).
//
// A selected field is handed on as its Go value, so native nodes receive
// their declared input types whether the output was a T or a *T. The
// exception is a ,string field, whose member is its quoted text.

var (
	jsonNumberType = reflect.TypeFor[json.Number]()
	stringType     = reflect.TypeFor[string]()
	// customEncoders are the method sets encoding/json calls instead of
	// encoding a value's own fields or kind. Go 1.27's default (v2-backed)
	// encoding/json honours all four, by value or pointer receiver.
	customEncoders = []reflect.Type{
		reflect.TypeFor[json.Marshaler](),
		reflect.TypeFor[jsonv2.MarshalerTo](),
		reflect.TypeFor[encoding.TextMarshaler](),
		reflect.TypeFor[encoding.TextAppender](),
	}
)

// encodesItself reports whether typ has one of customEncoders' methods.
func encodesItself(typ reflect.Type) bool {
	for _, encoder := range customEncoders {
		if typ.Implements(encoder) {
			return true
		}
	}
	return false
}

// field selects the member name of the JSON object current encodes to.
func field(current reflect.Value, name string) (reflect.Value, error) {
	for current.Kind() == reflect.Interface || current.Kind() == reflect.Pointer {
		if current.IsNil() {
			return reflect.Value{}, nullError(name)
		}
		if current.Kind() == reflect.Pointer && customEncoding(current) {
			break
		}
		current = current.Elem()
	}
	if customEncoding(current) {
		return fieldFromJSON(current, name)
	}
	switch current.Kind() {
	case reflect.Map:
		return mapMember(current, name)
	case reflect.Struct:
		return structMember(current, name)
	}
	return reflect.Value{}, kindError(current, name)
}

func nullError(name string) error { return absentError(fmt.Sprintf("cannot read %q from null", name)) }

func missingError(name string) error { return absentError(fmt.Sprintf("field %q is missing", name)) }

// absentError is a reference path that reaches no value: a missing field,
// or a field read from null. A default instruction falls back on it.
type absentError string

func (e absentError) Error() string { return string(e) }

func unencodableError(name string, err error) error {
	return fmt.Errorf("cannot read %q: value has no JSON encoding: %w", name, err)
}

// kindError reports why a value that is not a JSON object has no member.
func kindError(current reflect.Value, name string) error {
	switch current.Kind() {
	case reflect.Invalid:
		return nullError(name)
	case reflect.Bool:
		return fmt.Errorf("cannot read %q from a JSON boolean", name)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64:
		return fmt.Errorf("cannot read %q from a JSON number", name)
	case reflect.String:
		if current.Type() == jsonNumberType {
			return fmt.Errorf("cannot read %q from a JSON number", name)
		}
		return fmt.Errorf("cannot read %q from a JSON string", name)
	case reflect.Slice:
		if current.IsNil() {
			return nullError(name)
		}
		if current.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("cannot read %q from a JSON array", name)
		}
		if !encodesItself(reflect.PointerTo(current.Type().Elem())) {
			return fmt.Errorf("cannot read %q from a JSON string", name)
		}
		// Bytes with their own encoders: ask encoding/json. The value is the
		// one being descended into, so this is bounded by the selection.
		encoded, err := encodeInPlace(current)
		if err != nil {
			return unencodableError(name, err)
		}
		return fmt.Errorf("cannot read %q from a JSON %s", name, jsonKind(encoded))
	case reflect.Array:
		if current.Type().Elem().Kind() != reflect.Uint8 {
			return fmt.Errorf("cannot read %q from a JSON array", name)
		}
		// Byte arrays encode as arrays or strings depending on the
		// encoding/json implementation; ask it. The value is the one being
		// descended into, so this is bounded by the selection itself.
		encoded, err := encodeInPlace(current)
		if err != nil {
			return unencodableError(name, err)
		}
		return fmt.Errorf("cannot read %q from a JSON %s", name, jsonKind(encoded))
	}
	return unencodableError(name, fmt.Errorf("unsupported type %s", current.Type()))
}

// mapMember selects a map entry by its encoded key without encoding values.
func mapMember(current reflect.Value, name string) (reflect.Value, error) {
	if current.IsNil() {
		return reflect.Value{}, nullError(name)
	}
	key := current.Type().Key()
	textual := encodesItself(reflect.PointerTo(key))
	var found reflect.Value
	switch {
	case !textual && key.Kind() == reflect.String:
		found = current.MapIndex(reflect.ValueOf(name).Convert(key))
	case !textual && isInteger(key.Kind()):
		if keyValue, ok := integerKey(key, name); ok {
			found = current.MapIndex(keyValue)
		}
	default:
		// Text-marshaled and other keys: encode each key alone, as
		// encoding/json names it, never the values.
		matches := 0
		iterator := current.MapRange()
		for iterator.Next() {
			encodedName, err := mapKeyName(key, iterator.Key())
			if err != nil {
				return reflect.Value{}, unencodableError(name, err)
			}
			if encodedName == name {
				matches++
				found = iterator.Value()
			}
		}
		if matches > 1 {
			// Distinct keys sharing one name: a decoder keeps the last in
			// encoding order, which only the full encoding establishes.
			return fieldFromJSON(current, name)
		}
	}
	if !found.IsValid() {
		return reflect.Value{}, missingError(name)
	}
	return found, nil
}

func isInteger(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return true
	}
	return false
}

// integerKey parses name as the canonical decimal encoding/json writes for
// an integer key; "07" names no key.
func integerKey(key reflect.Type, name string) (reflect.Value, bool) {
	switch key.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		parsed, err := strconv.ParseInt(name, 10, key.Bits())
		if err != nil || strconv.FormatInt(parsed, 10) != name {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(parsed).Convert(key), true
	default:
		parsed, err := strconv.ParseUint(name, 10, key.Bits())
		if err != nil || strconv.FormatUint(parsed, 10) != name {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(parsed).Convert(key), true
	}
}

// mapKeyName is the object name encoding/json writes for one map key.
func mapKeyName(keyType reflect.Type, key reflect.Value) (string, error) {
	single := reflect.MakeMapWithSize(reflect.MapOf(keyType, reflect.TypeFor[bool]()), 1)
	single.SetMapIndex(key, reflect.ValueOf(true))
	encoded, err := json.Marshal(single.Interface())
	if err != nil {
		return "", err
	}
	var names map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &names); err != nil {
		return "", err
	}
	for encodedName := range names {
		return encodedName, nil
	}
	return "", fmt.Errorf("map key has no encoded name")
}

// structMember selects the field encoding/json writes under name.
func structMember(current reflect.Value, name string) (reflect.Value, error) {
	plan := structPlanFor(current.Type())
	if plan.unsupported {
		return fieldFromJSON(current, name)
	}
	selected, ok := plan.keys[name]
	if !ok {
		return reflect.Value{}, missingError(name)
	}
	value := current
	for _, position := range selected.index {
		if value.Kind() == reflect.Pointer {
			if value.IsNil() {
				// encoding/json omits fields promoted through a nil
				// embedded pointer.
				return reflect.Value{}, missingError(name)
			}
			value = value.Elem()
		}
		value = value.Field(position)
	}
	if !value.CanInterface() {
		return fieldFromJSON(current, name)
	}
	if selected.omitEmpty && emptyValue(value) || selected.omitZero && zeroValue(value) {
		return reflect.Value{}, missingError(name)
	}
	if selected.probe == nil {
		return value, nil
	}
	// ,string re-encodes a scalar as text; encoding/json decides whether it
	// applies and what the text is. The member is a scalar, so this is
	// bounded by the selection.
	quoted, err := encodeProbe(selected.probe, value)
	if err != nil {
		// NaN or ±Inf: no JSON form exists to disagree with.
		return value, nil
	}
	if bytes.Equal(quoted, []byte("{}")) {
		return reflect.Value{}, missingError(name)
	}
	// Both probes carry the same key, so equal encodings mean the option
	// changed nothing and the Go value is the member.
	if plain, err := encodeProbe(selected.plainProbe, value); err == nil && bytes.Equal(plain, quoted) {
		return value, nil
	}
	return decodeMember(probeMember(quoted), name)
}

// quotable reports the kinds encoding/json's ,string option can apply to:
// booleans, numbers and strings, directly or through an unnamed pointer.
func quotable(typ reflect.Type) bool {
	if typ.Name() == "" && typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	switch typ.Kind() {
	case reflect.Bool, reflect.String, reflect.Float32, reflect.Float64:
		return true
	}
	return isInteger(typ.Kind())
}

var isZeroerType = reflect.TypeFor[interface{ IsZero() bool }]()

// zeroValue is encoding/json's omitzero test: a type's own IsZero method
// when it has one, otherwise the Go zero value.
func zeroValue(value reflect.Value) bool {
	typ := value.Type()
	switch {
	case typ.Kind() == reflect.Interface && typ.Implements(isZeroerType):
		return value.IsNil() || value.Elem().Kind() == reflect.Pointer && value.Elem().IsNil() || value.Interface().(interface{ IsZero() bool }).IsZero()
	case typ.Kind() == reflect.Pointer && typ.Implements(isZeroerType):
		return value.IsNil() || value.Interface().(interface{ IsZero() bool }).IsZero()
	case typ.Implements(isZeroerType):
		return value.Interface().(interface{ IsZero() bool }).IsZero()
	case reflect.PointerTo(typ).Implements(isZeroerType):
		if !value.CanAddr() {
			boxed := reflect.New(typ).Elem()
			boxed.Set(value)
			value = boxed
		}
		return value.Addr().Interface().(interface{ IsZero() bool }).IsZero()
	}
	return value.IsZero()
}

// encodeProbe encodes value as the single field of probe, where it sits:
// addressable values reach pointer-receiver methods.
func encodeProbe(probeStruct reflect.Type, value reflect.Value) ([]byte, error) {
	probe := reflect.New(probeStruct)
	probe.Elem().Field(0).Set(value)
	if value.CanAddr() {
		return json.Marshal(probe.Interface())
	}
	return json.Marshal(probe.Elem().Interface())
}

// probeMember returns the member of a one-key object encoding/json wrote.
func probeMember(encoded []byte) []byte {
	var members map[string]json.RawMessage
	if json.Unmarshal(encoded, &members) != nil {
		return nil
	}
	for _, member := range members {
		return member
	}
	return nil
}

func decodeMember(member []byte, name string) (reflect.Value, error) {
	decoder := json.NewDecoder(bytes.NewReader(member))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return reflect.Value{}, fmt.Errorf("cannot read %q: %w", name, err)
	}
	return reflect.ValueOf(&decoded).Elem(), nil
}

// emptyValue is encoding/json's omitempty test: false, 0, a nil pointer or
// interface, or an empty array, slice, map or string. Go 1.27's v2-backed
// implementation keeps this definition for encoding/json
// (OmitEmptyWithLegacySemantics).
func emptyValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	}
	return false
}

// fieldFromJSON resolves name against the whole encoding of current. It is
// reserved for containers whose keys only their own marshaler knows.
func fieldFromJSON(current reflect.Value, name string) (reflect.Value, error) {
	encoded, err := encodeInPlace(current)
	if err != nil {
		return reflect.Value{}, unencodableError(name, err)
	}
	if kind := jsonKind(encoded); kind != "object" {
		if kind == "null" {
			return reflect.Value{}, nullError(name)
		}
		return reflect.Value{}, fmt.Errorf("cannot read %q from a JSON %s", name, kind)
	}
	// Decoding as any JSON consumer would keeps decoded-map semantics,
	// including the last duplicate name winning.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return reflect.Value{}, fmt.Errorf("cannot read %q: %w", name, err)
	}
	member, ok := members[name]
	if !ok {
		return reflect.Value{}, missingError(name)
	}
	return decodeMember(member, name)
}

// encodeInPlace encodes value as encoding/json does where it sits:
// addressable values may use pointer-receiver marshalers.
func encodeInPlace(value reflect.Value) ([]byte, error) {
	if value.CanAddr() && value.Addr().CanInterface() {
		return json.Marshal(value.Addr().Interface())
	}
	if !value.CanInterface() {
		return nil, fmt.Errorf("unexported value")
	}
	return json.Marshal(value.Interface())
}

func jsonKind(encoded []byte) string {
	trimmed := bytes.TrimLeft(encoded, " \t\r\n")
	if len(trimmed) == 0 {
		return "value"
	}
	switch trimmed[0] {
	case '{':
		return "object"
	case '[':
		return "array"
	case '"':
		return "string"
	case 't', 'f':
		return "boolean"
	case 'n':
		return "null"
	default:
		return "number"
	}
}

// customEncoding reports whether encoding/json would call one of the
// customEncoders' methods for value where it sits: pointer-receiver methods
// only apply to addressable values.
func customEncoding(value reflect.Value) bool {
	typ := value.Type()
	if encodesItself(typ) {
		return true
	}
	return typ.Kind() != reflect.Pointer && value.CanAddr() && encodesItself(reflect.PointerTo(typ))
}

type structKey struct {
	index []int
	// For a ,string field: one-field structs with the field's type and its
	// json tag, with and without the string option, built with the plan.
	probe      reflect.Type
	plainProbe reflect.Type
	omitEmpty  bool
	omitZero   bool
	quoted     bool
	field      reflect.StructField
}

type structPlan struct {
	keys map[string]structKey
	// unsupported marks a struct with a json tag outside jsontag's plain
	// form, or using embed or format, which this index does not model; such
	// structs resolve against their whole encoding.
	unsupported bool
}

var structPlans sync.Map // reflect.Type -> structPlan

func probeType(field reflect.StructField, jsonTag string) reflect.Type {
	return reflect.StructOf([]reflect.StructField{{Name: field.Name, Type: field.Type, Tag: reflect.StructTag("json:" + strconv.Quote(jsonTag))}})
}

// structPlanFor maps each JSON key of a struct type to the field
// encoding/json selects for it, following Go's embedding rules: json:"-"
// fields are skipped, untagged embedded structs are flattened, and among
// fields sharing a key the shallowest wins, a tagged one breaking a tie,
// while a remaining tie drops the key.
func structPlanFor(typ reflect.Type) structPlan {
	if cached, ok := structPlans.Load(typ); ok {
		return cached.(structPlan)
	}
	type candidate struct {
		key    structKey
		tagged bool
	}
	type level struct {
		typ   reflect.Type
		index []int
	}
	plan := structPlan{}
	best := map[string][]candidate{}
	visited := map[reflect.Type]bool{}
	next := []level{{typ: typ}}
	for len(next) > 0 && !plan.unsupported {
		current := next
		next = nil
		count := map[reflect.Type]int{}
		for _, entry := range current {
			count[entry.typ]++
		}
		for _, entry := range current {
			if visited[entry.typ] {
				continue
			}
			visited[entry.typ] = true
			for position := 0; position < entry.typ.NumField(); position++ {
				structField := entry.typ.Field(position)
				fieldType := structField.Type
				if structField.Anonymous {
					if fieldType.Kind() == reflect.Pointer {
						fieldType = fieldType.Elem()
					}
					if !structField.IsExported() && fieldType.Kind() != reflect.Struct {
						continue
					}
				} else if !structField.IsExported() {
					continue
				}
				tag, tagged := structField.Tag.Lookup("json")
				if tag == "-" {
					continue
				}
				options, plain := jsontag.Parse(tag)
				if !plain || options.Embed || options.Format {
					plan.unsupported = true
					break
				}
				omitEmpty, omitZero, quoted := options.OmitEmpty, options.OmitZero, options.String
				key := ""
				if tagged {
					key = options.Name
				}
				index := append(append([]int(nil), entry.index...), position)
				if key != "" || !structField.Anonymous || fieldType.Kind() != reflect.Struct {
					named := key != ""
					if key == "" {
						key = structField.Name
					}
					leaf := structField
					leaf.Anonymous = false
					selected := structKey{index: index, field: leaf, omitEmpty: omitEmpty, omitZero: omitZero, quoted: quoted}
					if quoted && quotable(leaf.Type) {
						selected.probe = probeType(leaf, tag)
						selected.plainProbe = probeType(leaf, withoutStringOption(tag))
					}
					best[key] = append(best[key], candidate{key: selected, tagged: named})
					if count[entry.typ] > 1 {
						// The same embedded type reached twice at one depth
						// is ambiguous for every field it carries.
						best[key] = append(best[key], candidate{key: selected, tagged: named})
					}
					continue
				}
				next = append(next, level{typ: fieldType, index: index})
			}
		}
	}
	if !plan.unsupported {
		plan.keys = make(map[string]structKey, len(best))
		for key, candidates := range best {
			dominant := candidates[0]
			tied := false
			for _, other := range candidates[1:] {
				switch {
				case len(other.key.index) < len(dominant.key.index) || len(other.key.index) == len(dominant.key.index) && other.tagged && !dominant.tagged:
					dominant, tied = other, false
				case len(other.key.index) == len(dominant.key.index) && other.tagged == dominant.tagged:
					tied = true
				}
			}
			if !tied {
				plan.keys[key] = dominant.key
			}
		}
	}
	cached, _ := structPlans.LoadOrStore(typ, plan)
	return cached.(structPlan)
}

func withoutStringOption(tag string) string {
	name, options, _ := strings.Cut(tag, ",")
	kept := []string{name}
	for _, option := range strings.Split(options, ",") {
		if option != "string" && option != "" {
			kept = append(kept, option)
		}
	}
	return strings.Join(kept, ",")
}
