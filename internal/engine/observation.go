package engine

import (
	"encoding"
	"encoding/base64"
	"encoding/json"
	jsonv2 "encoding/json/v2"
	"reflect"
	"strings"
	"time"
	"unicode"
)

// Observation snapshots deliberately trade fidelity for a hard memory bound.
// Payloads larger than 32 KiB, values deeper than 32 levels, and values with
// more than 8192 visited items become the fixed truncation marker. The walker
// copies only JSON-compatible data and never invokes user MarshalJSON methods.
const (
	maxObservedPayloadBytes = 32 << 10
	maxObservationNodes     = 8192
	maxObservationDepth     = 32
)

var (
	observationTimeType          = reflect.TypeOf(time.Time{})
	observationNumberType        = reflect.TypeOf(json.Number(""))
	observationRawType           = reflect.TypeOf(json.RawMessage(nil))
	observationMarshalerType     = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	observationTextMarshalerType = reflect.TypeOf((*encoding.TextMarshaler)(nil)).Elem()
	observationTextAppenderType  = reflect.TypeOf((*encoding.TextAppender)(nil)).Elem()
	observationJSONToType        = reflect.TypeOf((*jsonv2.MarshalerTo)(nil)).Elem()
	observationByteType          = reflect.TypeOf(byte(0))
)

type observationBudget struct {
	remaining int
	nodes     int
}

func (b *observationBudget) charge(size int) bool {
	if size < 0 || size > b.remaining {
		return false
	}
	b.remaining -= size
	return true
}

func (b *observationBudget) capture(value reflect.Value, depth int) (any, bool) {
	if depth > maxObservationDepth || b.nodes <= 0 {
		return nil, false
	}
	b.nodes--
	if !value.IsValid() {
		return nil, b.charge(4)
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return nil, b.charge(4)
		}
		value = value.Elem()
		if depth >= maxObservationDepth {
			return nil, false
		}
		depth++
	}

	if value.Type() == observationTimeType {
		// This concrete standard-library type has a bounded representation.
		// Its strict encoder rejects dates/offsets that Format would accept.
		text, err := value.Interface().(time.Time).MarshalText()
		if err != nil {
			return nil, false
		}
		return b.captureString(string(text))
	}
	if value.Type() == observationNumberType {
		text := value.Interface().(json.Number).String()
		if len(text) == 0 || len(text) > b.remaining/6 || !json.Valid([]byte(text)) {
			return nil, false
		}
		if !b.charge(len(text)*6 + 2) {
			return nil, false
		}
		return json.Number(text), true
	}
	if value.Type() == observationRawType {
		if value.Len() > b.remaining || !json.Valid(value.Bytes()) {
			return nil, false
		}
		if !b.charge(value.Len() + 2) {
			return nil, false
		}
		return append(json.RawMessage(nil), value.Bytes()...), true
	}
	// Custom JSON representations cannot be reconstructed without invoking
	// application code. Mark them explicitly instead of inventing a shape.
	if observationCustomRepresentation(value.Type()) {
		return nil, false
	}

	switch value.Kind() {
	case reflect.Bool:
		if !b.charge(5) {
			return nil, false
		}
		return value.Bool(), true
	case reflect.String:
		return b.captureString(value.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if !b.charge(24) {
			return nil, false
		}
		return value.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		if !b.charge(24) {
			return nil, false
		}
		return value.Uint(), true
	case reflect.Float32, reflect.Float64:
		if !b.charge(32) {
			return nil, false
		}
		return value.Float(), true
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return nil, b.charge(4)
		}
		if value.Kind() == reflect.Slice && value.Type().Elem().Kind() == reflect.Uint8 {
			if value.Type().Elem() != observationByteType {
				return nil, false
			}
			if value.Len() > b.remaining/2 {
				return nil, false
			}
			encodedSize := base64.StdEncoding.EncodedLen(value.Len())
			if !b.charge(encodedSize + 2) {
				return nil, false
			}
			encoded := make([]byte, encodedSize)
			binary := make([]byte, value.Len())
			reflect.Copy(reflect.ValueOf(binary), value)
			base64.StdEncoding.Encode(encoded, binary)
			return string(encoded), true
		}
		if value.Len() > b.nodes || !b.charge(2) {
			return nil, false
		}
		items := make([]any, 0, value.Len())
		for index := 0; index < value.Len(); index++ {
			item, ok := b.capture(value.Index(index), depth+1)
			if !ok {
				return nil, false
			}
			items = append(items, item)
		}
		return items, true
	case reflect.Map:
		if value.IsNil() {
			return nil, b.charge(4)
		}
		if value.Type().Key().Kind() != reflect.String || observationCustomRepresentation(value.Type().Key()) || value.Len() > b.nodes/2 || !b.charge(2) {
			return nil, false
		}
		items := make(map[string]any, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key := iterator.Key().String()
			if _, ok := b.captureString(key); !ok {
				return nil, false
			}
			item, ok := b.capture(iterator.Value(), depth+1)
			if !ok {
				return nil, false
			}
			items[key] = item
		}
		return items, true
	case reflect.Struct:
		fields := make(map[string]any)
		seenNames := make(map[string]struct{})
		if !b.charge(2) {
			return nil, false
		}
		typ := value.Type()
		for index := 0; index < value.NumField(); index++ {
			if b.nodes <= 0 {
				return nil, false
			}
			b.nodes--
			field := typ.Field(index)
			// encoding/json promotes anonymous fields and resolves collisions.
			// This bounded walker does not implement that selection algorithm.
			if field.Anonymous {
				return nil, false
			}
			if field.PkgPath != "" {
				continue
			}
			tag := field.Tag.Get("json")
			if strings.HasPrefix(tag, "'") {
				return nil, false
			}
			name, options, _ := strings.Cut(tag, ",")
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			if len(name) > b.remaining {
				return nil, false
			}
			for _, char := range name {
				if !unicode.IsLetter(char) && !unicode.IsDigit(char) && !strings.ContainsRune("!#$%&()*+-./:;<=>?@[]^_{|}~ ", char) {
					return nil, false
				}
			}
			optionSet := "," + options + ","
			if strings.Contains(optionSet, ",string,") || strings.Contains(optionSet, ",omitzero,") {
				return nil, false
			}
			// JSON selects fields before omitting empty values. An ambiguous
			// name must not expose the last field or an otherwise omitted one.
			if _, duplicate := seenNames[name]; duplicate {
				return nil, false
			}
			if _, ok := b.captureString(name); !ok {
				return nil, false
			}
			seenNames[name] = struct{}{}
			if strings.Contains(optionSet, ",omitempty,") && observationEmpty(value.Field(index)) {
				continue
			}
			item, ok := b.capture(value.Field(index), depth+1)
			if !ok {
				return nil, false
			}
			fields[name] = item
		}
		return fields, true
	default:
		return nil, false
	}
}

func observationCustomRepresentation(typ reflect.Type) bool {
	for _, custom := range []reflect.Type{observationMarshalerType, observationTextMarshalerType, observationTextAppenderType, observationJSONToType} {
		if typ.Implements(custom) || reflect.PointerTo(typ).Implements(custom) {
			return true
		}
	}
	return false
}

func observationEmpty(value reflect.Value) bool {
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

func (b *observationBudget) captureString(value string) (any, bool) {
	if len(value) > (b.remaining-2)/6 || !b.charge(len(value)*6+2) {
		return nil, false
	}
	return value, true
}
