package engine

import (
	"bytes"
	"encoding"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// Field references select encoding/json object keys (#241, ADR 0001).
//
// A value reaches a reference either still typed, as a node returned it, or
// after a JSON boundary (a checkpoint, a foreign runtime, a child workflow)
// as decoded maps. Both must resolve a path the same way, so the key set of
// an object is the one encoding/json emits for it: promoted fields of
// embedded structs flattened, json:"-" fields absent, a tagged field only
// under its tag name, empty omitempty and zero omitzero fields absent,
// ,string fields as their quoted text, and custom MarshalJSON/MarshalText
// output as written.
//
// Work is bounded by the selected member, never by its siblings. A struct
// resolves through a per-type key index, built once per type. Each tag's
// key name is delegated to encoding/json through a one-field struct with
// the same json tag, so unusual tags follow the linked implementation (Go
// 1.27's default and its GOEXPERIMENT=nojsonv2 v1 implementation name them
// differently). omitempty and omitzero are decided from the selected field
// alone; a ,string scalar is the only member encoded. Map keys are named
// without encoding values. Only a container whose own type has a custom
// marshaler is encoded whole, because its keys exist nowhere else.
//
// A selected field is handed on as its Go value, so native nodes receive
// their declared input types whether the output was a T or a *T. The
// exception is a ,string field, whose member is its quoted text.

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
	jsonNumberType    = reflect.TypeFor[json.Number]()
	stringType        = reflect.TypeFor[string]()
)

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

func nullError(name string) error { return fmt.Errorf("cannot read %q from null", name) }

func missingError(name string) error { return fmt.Errorf("field %q is missing", name) }

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
		if current.Type().Elem().Kind() == reflect.Uint8 {
			pointer := reflect.PointerTo(current.Type().Elem())
			if !pointer.Implements(jsonMarshalerType) && !pointer.Implements(textMarshalerType) {
				return fmt.Errorf("cannot read %q from a JSON string", name)
			}
		}
		return fmt.Errorf("cannot read %q from a JSON array", name)
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
	textual := key.Implements(textMarshalerType) || reflect.PointerTo(key).Implements(textMarshalerType) ||
		key.Implements(jsonMarshalerType) || reflect.PointerTo(key).Implements(jsonMarshalerType)
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
	if !selected.quoted || !quotable(selected.field.Type) {
		return value, nil
	}
	// ,string re-encodes a scalar as text; encoding/json decides whether it
	// applies and what the text is. The member is a scalar, so this is
	// bounded by the selection.
	member, present, err := encodeMember(selected.probe, value)
	if err != nil {
		// NaN or ±Inf: no JSON form exists to disagree with.
		return value, nil
	}
	if !present {
		return reflect.Value{}, missingError(name)
	}
	plain, _, err := encodeMember(probeType(selected.field, selected.plainTag), value)
	if err != nil || !bytes.Equal(plain, member) {
		return decodeMember(member, name)
	}
	return value, nil
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

// encodeMember encodes value as the single field of probe, where it sits:
// addressable values reach pointer-receiver methods.
func encodeMember(probeStruct reflect.Type, value reflect.Value) ([]byte, bool, error) {
	probe := reflect.New(probeStruct)
	probe.Elem().Field(0).Set(value)
	var encoded []byte
	var err error
	if value.CanAddr() {
		encoded, err = json.Marshal(probe.Interface())
	} else {
		encoded, err = json.Marshal(probe.Elem().Interface())
	}
	if err != nil {
		return nil, false, err
	}
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return nil, false, err
	}
	if len(members) == 0 {
		return nil, false, nil
	}
	for _, member := range members {
		return member, true, nil
	}
	return nil, false, nil
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

// customEncoding reports whether encoding/json would call a MarshalJSON or
// MarshalText method for value where it sits.
func customEncoding(value reflect.Value) bool {
	typ := value.Type()
	if typ.Implements(jsonMarshalerType) || typ.Implements(textMarshalerType) {
		return true
	}
	if typ.Kind() != reflect.Pointer && value.CanAddr() {
		pointer := reflect.PointerTo(typ)
		return pointer.Implements(jsonMarshalerType) || pointer.Implements(textMarshalerType)
	}
	return false
}

type structKey struct {
	index     []int
	probe     reflect.Type // for ,string: one-field struct with the field's type and json tag
	plainTag  string       // the json tag without the string option
	omitEmpty bool
	omitZero  bool
	quoted    bool
	field     reflect.StructField
}

type structPlan struct {
	keys map[string]structKey
	// unsupported marks tags using options this index does not model
	// (encoding/json/v2 options such as inline or format); such structs
	// resolve against their whole encoding.
	unsupported bool
}

var (
	structPlans sync.Map // reflect.Type -> structPlan
	tagNames    sync.Map // json tag -> tagName
)

type tagName struct {
	name    string
	present bool // false: encoding/json writes no key for this tag
	err     error
}

const probeFieldName = "BlokReferenceProbe"

// keyForTag asks encoding/json which key a tag names, so unusual tags follow
// the linked implementation. An empty name means the tag names no key and
// the Go field name applies.
func keyForTag(tag string) tagName {
	if cached, ok := tagNames.Load(tag); ok {
		return cached.(tagName)
	}
	name := func(fieldName string) (string, bool, error) {
		probe := reflect.StructOf([]reflect.StructField{{Name: fieldName, Type: stringType, Tag: reflect.StructTag("json:" + strconv.Quote(tag))}})
		value := reflect.New(probe).Elem()
		value.Field(0).SetString("x")
		encoded, err := json.Marshal(value.Interface())
		if err != nil {
			return "", false, err
		}
		var members map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &members); err != nil {
			return "", false, err
		}
		for key := range members {
			return key, true, nil
		}
		return "", false, nil
	}
	result := tagName{}
	first, present, err := name(probeFieldName)
	switch {
	case err != nil:
		result.err = err
	case !present:
	case first != probeFieldName:
		result = tagName{name: first, present: true}
	default:
		// Either the tag names no key, or it literally names the probe.
		second, _, err := name(probeFieldName + "B")
		if err != nil {
			result.err = err
		} else if second == probeFieldName {
			result = tagName{name: probeFieldName, present: true}
		} else {
			result = tagName{present: true}
		}
	}
	cached, _ := tagNames.LoadOrStore(tag, result)
	return cached.(tagName)
}

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
				_, options, _ := strings.Cut(tag, ",")
				omitEmpty, omitZero, quoted, known := tagOptions(options)
				if !known {
					plan.unsupported = true
					break
				}
				key := ""
				if tagged && tag != "" {
					derived := keyForTag(tag)
					if derived.err != nil {
						plan.unsupported = true
						break
					}
					if !derived.present {
						continue
					}
					key = derived.name
				}
				index := append(append([]int(nil), entry.index...), position)
				if key != "" || !structField.Anonymous || fieldType.Kind() != reflect.Struct {
					named := key != ""
					if key == "" {
						key = structField.Name
					}
					plainTag := tag
					if quoted {
						plainTag = withoutStringOption(tag)
					}
					leaf := structField
					leaf.Anonymous = false
					selected := structKey{index: index, field: leaf, plainTag: plainTag, omitEmpty: omitEmpty, omitZero: omitZero, quoted: quoted}
					if quoted {
						selected.probe = probeType(leaf, tag)
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

// tagOptions recognizes the options whose effect this index models; any
// other option makes the struct resolve against its whole encoding.
func tagOptions(options string) (omitEmpty, omitZero, quoted, known bool) {
	if options == "" {
		return false, false, false, true
	}
	for _, option := range strings.Split(options, ",") {
		switch option {
		case "omitempty":
			omitEmpty = true
		case "omitzero":
			omitZero = true
		case "string":
			quoted = true
		case "":
		default:
			return false, false, false, false
		}
	}
	return omitEmpty, omitZero, quoted, true
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
