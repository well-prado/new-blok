package jsontag

import (
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
)

// encodeOne encodes a one-field struct whose field F carries tag.
func encodeOne(t *testing.T, tag string, value int64) map[string]any {
	t.Helper()
	typ := reflect.StructOf([]reflect.StructField{{Name: "F", Type: reflect.TypeFor[int64](), Tag: reflect.StructTag("json:" + strconv.Quote(tag))}})
	instance := reflect.New(typ).Elem()
	instance.Field(0).SetInt(value)
	encoded, err := json.Marshal(instance.Interface())
	if err != nil {
		t.Fatalf("tag %q: %v", tag, err)
	}
	var members map[string]any
	if err := json.Unmarshal(encoded, &members); err != nil {
		t.Fatal(err)
	}
	return members
}

// TestPlainTagsPredictEncodingJSON: for every tag Parse accepts, the key,
// omitempty/omitzero and ,string effects it reports are what encoding/json
// does, checked by encoding a zero and a non-zero value.
func TestPlainTagsPredictEncodingJSON(t *testing.T) {
	for _, tag := range []string{
		"", "id", "-,", "id,omitempty", "id,omitzero", "id,string", ",omitempty", "id,omitempty,string",
		"id,required", "id,case:ignore", "id,omitEmpty", "id,", "id,,omitempty", "x y", "é", "a.b",
	} {
		options, ok := Parse(tag)
		if !ok {
			t.Fatalf("tag %q: not plain", tag)
		}
		key := options.Name
		if key == "" {
			key = "F"
		}
		for _, value := range []int64{0, 5} {
			members := encodeOne(t, tag, value)
			got, present := members[key]
			wantPresent := !(value == 0 && (options.OmitEmpty || options.OmitZero))
			if present != wantPresent || len(members) != map[bool]int{true: 1, false: 0}[wantPresent] {
				t.Fatalf("tag %q value %d: encoded %v; predicted key %q present=%v", tag, value, members, key, wantPresent)
			}
			if !present {
				continue
			}
			if _, quoted := got.(string); quoted != options.String {
				t.Fatalf("tag %q value %d: encoded %v; predicted string=%v", tag, value, members, options.String)
			}
		}
	}
}

// TestNonPlainTagsAreRejected: tags whose effect encoding/json decides by
// its own parsing (malformed options it still applies, quoted parts).
func TestNonPlainTagsAreRejected(t *testing.T) {
	for _, tag := range []string{
		"id,omitempty ", "id,omitempty;", "id,omitempty:x", "id, omitempty", "n,string ", "'a,string'",
		"n,'string'", "it's", `a"b`, `a\b`, "id,format:", "\xff",
	} {
		if options, ok := Parse(tag); ok {
			t.Fatalf("tag %q accepted as plain: %+v", tag, options)
		}
	}
	if options, ok := Parse("id,embed"); !ok || !options.Embed {
		t.Fatalf("embed: %+v %v", options, ok)
	}
	if options, ok := Parse("id,format:units"); !ok || !options.Format {
		t.Fatalf("format: %+v %v", options, ok)
	}
}
