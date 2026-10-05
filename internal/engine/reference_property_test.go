package engine

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// The property test builds random struct shapes with reflect.StructOf and
// checks every candidate key against encoding/json itself: resolving a key
// on the typed value (as T and as *T) must give the same outcome as
// resolving it on the value's json.Marshal → json.Unmarshal form. Shapes
// collide names on purpose (tagged versus untagged, equal depths, embedded
// values and pointers, nil embedded pointers, omitempty/omitzero/string,
// "-", ignored options) so the embedding and dominance rules are exercised.

var (
	propertyGoNames  = []string{"A", "B", "C", "D", "E"}
	propertyTagNames = []string{"A", "B", "a", "b", "-"}
	propertyOptions  = []string{"", ",omitempty", ",omitzero", ",string", ",required"}
)

type propertyShapes struct {
	random *rand.Rand
}

func (p *propertyShapes) leafType() reflect.Type {
	switch p.random.IntN(3) {
	case 0:
		return reflect.TypeFor[int]()
	case 1:
		return reflect.TypeFor[string]()
	default:
		return reflect.TypeFor[*int]()
	}
}

func (p *propertyShapes) structType(depth int) reflect.Type {
	count := 1 + p.random.IntN(4)
	names := p.random.Perm(len(propertyGoNames))[:count]
	fields := make([]reflect.StructField, 0, count)
	for _, nameIndex := range names {
		field := reflect.StructField{Name: propertyGoNames[nameIndex]}
		switch kind := p.random.IntN(10); {
		case depth < 3 && kind < 3:
			// Embedded struct, by value or pointer; sometimes tagged, which
			// stops it being flattened.
			inner := p.structType(depth + 1)
			field.Name = fmt.Sprintf("E%d%s", depth, field.Name)
			if p.random.IntN(2) == 0 {
				inner = reflect.PointerTo(inner)
			}
			field.Type = inner
			field.Anonymous = true
			if p.random.IntN(4) == 0 {
				field.Tag = reflect.StructTag(`json:"` + propertyTagNames[p.random.IntN(len(propertyTagNames)-1)] + `"`)
			}
		case depth < 3 && kind < 4:
			field.Type = p.structType(depth + 1)
		default:
			field.Type = p.leafType()
		}
		if field.Tag == "" && p.random.IntN(3) > 0 {
			tag := ""
			if p.random.IntN(4) > 0 {
				tag = propertyTagNames[p.random.IntN(len(propertyTagNames))]
			}
			if tag != "-" {
				tag += propertyOptions[p.random.IntN(len(propertyOptions))]
			} else if p.random.IntN(2) == 0 {
				tag = "-,"
			}
			field.Tag = reflect.StructTag("json:" + strconv.Quote(tag))
		}
		fields = append(fields, field)
	}
	return reflect.StructOf(fields)
}

// fill sets every reachable field to a random value, leaving some empty
// and some embedded pointers nil.
func (p *propertyShapes) fill(value reflect.Value) {
	switch value.Kind() {
	case reflect.Int:
		value.SetInt(int64(p.random.IntN(3)))
	case reflect.String:
		if p.random.IntN(2) == 0 {
			value.SetString("x")
		}
	case reflect.Pointer:
		if p.random.IntN(3) > 0 {
			value.Set(reflect.New(value.Type().Elem()))
			p.fill(value.Elem())
		}
	case reflect.Struct:
		for index := range value.NumField() {
			p.fill(value.Field(index))
		}
	}
}

type propertyOutcome struct {
	value string
	err   string
}

func resolveOutcome(source any, name string) propertyOutcome {
	resolved, err := field(reflect.ValueOf(&source).Elem(), name)
	if err != nil {
		return propertyOutcome{err: err.Error()}
	}
	encoded, err := json.Marshal(resolved.Interface())
	if err != nil {
		return propertyOutcome{err: "unencodable result: " + err.Error()}
	}
	// Canonical form: a struct encodes its fields in declaration order, a
	// decoded map in sorted key order.
	var canonical any
	if err := json.Unmarshal(encoded, &canonical); err != nil {
		return propertyOutcome{err: "undecodable result: " + err.Error()}
	}
	encoded, _ = json.Marshal(canonical)
	return propertyOutcome{value: string(encoded)}
}

func TestReferenceKeysMatchEncodingJSONOnRandomShapes(t *testing.T) {
	const shapes = 1500
	p := &propertyShapes{random: rand.New(rand.NewPCG(241, 2026))}
	names := append(append([]string(nil), propertyGoNames...), propertyTagNames...)
	checked := 0
	for shape := range shapes {
		typ := p.structType(0)
		for range 2 {
			pointer := reflect.New(typ)
			p.fill(pointer.Elem())
			for _, source := range []any{pointer.Elem().Interface(), pointer.Interface()} {
				encoded, err := json.Marshal(source)
				if err != nil {
					t.Fatalf("shape %d %s: %v", shape, typ, err)
				}
				var decoded any
				if err := json.Unmarshal(encoded, &decoded); err != nil {
					t.Fatal(err)
				}
				keys := append([]string(nil), names...)
				for key := range decoded.(map[string]any) {
					keys = append(keys, key)
				}
				for _, key := range keys {
					typed, fromJSON := resolveOutcome(source, key), resolveOutcome(decoded, key)
					if typed != fromJSON {
						t.Fatalf("shape %d %s (%T)\nencoded %s\nkey %q: typed %+v, decoded %+v", shape, typ, source, encoded, key, typed, fromJSON)
					}
					checked++
				}
			}
		}
	}
	t.Logf("%d shapes, %d key resolutions compared", shapes, checked)
}

// hiddenBase is unexported: encoding/json still promotes its exported
// fields. (Such values never reach the engine's state, which refuses to
// commit unexported fields, but the resolver must still name them.)
type hiddenBase struct {
	ID string `json:"id"`
}

type exposesHidden struct {
	hiddenBase
	Name string `json:"name"`
}

func TestUnexportedEmbeddedStructPromotesItsFields(t *testing.T) {
	for _, source := range []any{exposesHidden{hiddenBase{ID: "b-1"}, "n"}, &exposesHidden{hiddenBase{ID: "b-1"}, "n"}} {
		encoded, _ := json.Marshal(source)
		var decoded any
		_ = json.Unmarshal(encoded, &decoded)
		for _, key := range []string{"id", "name", "hiddenBase", "ID"} {
			if typed, fromJSON := resolveOutcome(source, key), resolveOutcome(decoded, key); typed != fromJSON {
				t.Fatalf("%T key %q: typed %+v, decoded %+v", source, key, typed, fromJSON)
			}
		}
	}
}

// recursiveLevel embeds itself, so its key index must stop at types it has
// already visited.
type recursiveLevel struct {
	*recursiveLevel
	V int
}

func TestRecursiveEmbeddingTerminates(t *testing.T) {
	done := make(chan propertyOutcome, 1)
	go func() {
		done <- resolveOutcome(recursiveLevel{recursiveLevel: &recursiveLevel{V: 2}, V: 1}, "V")
	}()
	select {
	case got := <-done:
		if got != (propertyOutcome{value: "1"}) {
			t.Fatalf("got %+v; want V=1, the shallowest", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("resolving a key of a self-embedding type did not terminate")
	}
}
