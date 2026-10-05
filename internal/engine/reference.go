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
// an object is exactly what encoding/json emits for it: promoted fields of
// embedded structs flattened, json:"-" fields absent, a tagged field only
// under its tag name, omitempty/omitzero fields absent when empty, ,string
// fields as their quoted text, and custom MarshalJSON/MarshalText output as
// written.
//
// Rather than re-implement those rules (which already differ between Go
// releases for unusual tags and text-marshaled map keys), a struct or any
// custom-encoded value is resolved against its actual encoding/json
// encoding. The Go-typed field is returned when its own encoding is the
// selected member byte for byte, so native nodes keep receiving their
// declared types; otherwise the decoded member is returned. Either way the
// resolved value encodes to exactly the member's JSON.

var (
	jsonMarshalerType = reflect.TypeFor[json.Marshaler]()
	textMarshalerType = reflect.TypeFor[encoding.TextMarshaler]()
)

// field selects the member name of the JSON object current encodes to.
func field(current reflect.Value, name string) (reflect.Value, error) {
	for current.Kind() == reflect.Interface || current.Kind() == reflect.Pointer {
		if current.IsNil() {
			return reflect.Value{}, fmt.Errorf("cannot read %q from null", name)
		}
		if current.Kind() == reflect.Pointer && customEncoding(current) {
			break
		}
		current = current.Elem()
	}
	if current.Kind() == reflect.Map && plainStringKeys(current.Type()) && !customEncoding(current) {
		if current.IsNil() {
			return reflect.Value{}, fmt.Errorf("cannot read %q from null", name)
		}
		found := current.MapIndex(reflect.ValueOf(name).Convert(current.Type().Key()))
		if !found.IsValid() {
			return reflect.Value{}, fmt.Errorf("field %q is missing", name)
		}
		return found, nil
	}
	return fieldFromJSON(current, name)
}

// fieldFromJSON resolves name against the encoding/json encoding of current.
func fieldFromJSON(current reflect.Value, name string) (reflect.Value, error) {
	encoded, err := encodeInPlace(current)
	if err != nil {
		return reflect.Value{}, fmt.Errorf("cannot read %q: value has no JSON encoding: %w", name, err)
	}
	if kind := jsonKind(encoded); kind != "object" {
		if kind == "null" {
			return reflect.Value{}, fmt.Errorf("cannot read %q from null", name)
		}
		return reflect.Value{}, fmt.Errorf("cannot read %q from a JSON %s", name, kind)
	}
	// Decoding the object exactly as any JSON consumer would keeps the
	// decoded-map semantics, including the last duplicate name winning.
	var members map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &members); err != nil {
		return reflect.Value{}, fmt.Errorf("cannot read %q: %w", name, err)
	}
	member, ok := members[name]
	if !ok {
		return reflect.Value{}, fmt.Errorf("field %q is missing", name)
	}
	// The typed value is compared as it will be handed on: a copy, which no
	// longer reaches pointer-receiver marshalers its place in a pointer
	// output did.
	if typed, ok := typedMember(current, name); ok {
		if same, err := json.Marshal(typed.Interface()); err == nil && bytes.Equal(same, member) {
			return typed, nil
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(member))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return reflect.Value{}, fmt.Errorf("cannot read %q: %w", name, err)
	}
	return reflect.ValueOf(&decoded).Elem(), nil
}

// typedMember finds the Go value encoding/json would place under name, if
// the container's own fields decide its keys.
func typedMember(container reflect.Value, name string) (reflect.Value, bool) {
	if customEncoding(container) {
		return reflect.Value{}, false
	}
	switch container.Kind() {
	case reflect.Struct:
		index, ok := structKeys(container.Type())[name]
		if !ok {
			return reflect.Value{}, false
		}
		current := container
		for _, position := range index {
			if current.Kind() == reflect.Pointer {
				if current.IsNil() {
					return reflect.Value{}, false
				}
				current = current.Elem()
			}
			current = current.Field(position)
		}
		return current, current.CanInterface()
	case reflect.Map:
		key := container.Type().Key()
		var keyValue reflect.Value
		switch key.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			parsed, err := strconv.ParseInt(name, 10, key.Bits())
			if err != nil || strconv.FormatInt(parsed, 10) != name {
				return reflect.Value{}, false
			}
			keyValue = reflect.ValueOf(parsed).Convert(key)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			parsed, err := strconv.ParseUint(name, 10, key.Bits())
			if err != nil || strconv.FormatUint(parsed, 10) != name {
				return reflect.Value{}, false
			}
			keyValue = reflect.ValueOf(parsed).Convert(key)
		default:
			return reflect.Value{}, false
		}
		found := container.MapIndex(keyValue)
		return found, found.IsValid() && found.CanInterface()
	}
	return reflect.Value{}, false
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

// plainStringKeys reports whether every key of a map of this type encodes
// as its own string, so the key set can be read without encoding.
func plainStringKeys(typ reflect.Type) bool {
	key := typ.Key()
	if key.Kind() != reflect.String {
		return false
	}
	return !key.Implements(textMarshalerType) && !reflect.PointerTo(key).Implements(textMarshalerType) &&
		!key.Implements(jsonMarshalerType) && !reflect.PointerTo(key).Implements(jsonMarshalerType)
}

var structKeyCache sync.Map // reflect.Type -> map[string][]int

// structKeys maps each JSON key of a struct type to the index path of the
// field encoding/json selects for it, following Go's embedding rules:
// json:"-" fields are skipped, untagged embedded structs are flattened, and
// among fields sharing a key the shallowest wins, a tagged one breaking a
// tie, while a remaining tie drops the key. The result only chooses which
// typed value to return; field verifies it against the actual encoding.
func structKeys(typ reflect.Type) map[string][]int {
	if cached, ok := structKeyCache.Load(typ); ok {
		return cached.(map[string][]int)
	}
	type candidate struct {
		index  []int
		tagged bool
	}
	type level struct {
		typ   reflect.Type
		index []int
	}
	best := map[string][]candidate{}
	visited := map[reflect.Type]bool{}
	next := []level{{typ: typ}}
	for len(next) > 0 {
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
				tag := structField.Tag.Get("json")
				if tag == "-" {
					continue
				}
				key, _, _ := strings.Cut(tag, ",")
				index := append(append([]int(nil), entry.index...), position)
				if key != "" || !structField.Anonymous || fieldType.Kind() != reflect.Struct {
					tagged := key != ""
					if key == "" {
						key = structField.Name
					}
					best[key] = append(best[key], candidate{index: index, tagged: tagged})
					if count[entry.typ] > 1 {
						// The same embedded type reached twice at one depth
						// is ambiguous for every field it carries.
						best[key] = append(best[key], candidate{index: index, tagged: tagged})
					}
					continue
				}
				next = append(next, level{typ: fieldType, index: index})
			}
		}
	}
	keys := make(map[string][]int, len(best))
	for key, candidates := range best {
		dominant := candidates[0]
		tied := false
		for _, other := range candidates[1:] {
			switch {
			case len(other.index) < len(dominant.index) || len(other.index) == len(dominant.index) && other.tagged && !dominant.tagged:
				dominant, tied = other, false
			case len(other.index) == len(dominant.index) && other.tagged == dominant.tagged:
				tied = true
			}
		}
		if !tied {
			keys[key] = dominant.index
		}
	}
	cached, _ := structKeyCache.LoadOrStore(typ, keys)
	return cached.(map[string][]int)
}
