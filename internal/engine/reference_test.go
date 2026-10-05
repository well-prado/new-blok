package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

// Source shapes for testdata/references/json-keys.json (#241). Each one
// exercises a way encoding/json chooses, hides, flattens or re-encodes an
// object key.

// RefBase is exported: an embedded field of an unexported struct type is an
// unexported field, and the engine refuses to commit those (value.Clone).
type RefBase struct {
	ID   string `json:"id"`
	Note string
}

type refTagged struct {
	SKU        string `json:"sku"`
	TotalCents int64
}

type refEmbedded struct {
	RefBase
	Name string `json:"name"`
}

type RefPtrEmbedded struct {
	*RefBase
	Name string `json:"name"`
}

type RefTaggedEmbedded struct {
	RefBase `json:"base"`
	Name    string
}

type RefShadowed struct {
	RefBase
	ID string `json:"id"`
}

// RefOtherNote carries an untagged key that RefBase also promotes, at the
// same depth. (Tagged duplicates would be equally ambiguous; go vet rejects
// those outright.)
type RefOtherNote struct {
	Note string
}

type RefAmbiguous struct {
	RefBase
	RefOtherNote
}

type RefTaggedKey struct {
	Key string `json:"Key"`
}

type RefPlainKey struct {
	Key string
}

type RefTagDominates struct {
	RefTaggedKey
	RefPlainKey
}

type Label string

type RefEmbeddedLabel struct {
	Label
}

type refHidden struct {
	Secret string `json:"-"`
	Dash   string `json:"-,"`
}

type refOmitEmpty struct {
	Note string `json:"note,omitempty"`
}

type refOmitZero struct {
	At time.Duration `json:"at,omitzero"`
}

type refStringOption struct {
	N int64  `json:"n,string"`
	S string `json:"s,string"`
	P *int   `json:"p,string"`
}

type refMoney struct {
	Cents int64
}

func (m refMoney) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"amount": fmt.Sprintf("%d.%02d", m.Cents/100, m.Cents%100)})
}

type refRaw struct {
	Extra json.RawMessage `json:"extra"`
	Bytes []byte          `json:"bytes"`
}

type refBoxed struct {
	Data any `json:"data"`
}

type refCode string

// refEncodesTo, refAppendsText and refPointerAppender use the encoders Go
// 1.27's encoding/json added: MarshalJSONTo and AppendText.
type refEncodesTo struct{ Secret int }

func (refEncodesTo) MarshalJSONTo(encoder *jsontext.Encoder) error {
	return encoder.WriteToken(jsontext.String("viaTo"))
}

type refAppendsText struct{ Secret int }

func (refAppendsText) AppendText(text []byte) ([]byte, error) { return append(text, "app"...), nil }

type refPointerAppender struct{ Secret int }

func (*refPointerAppender) AppendText(text []byte) ([]byte, error) {
	return append(text, "papp"...), nil
}

type refHoldsPointerAppender struct {
	P refPointerAppender `json:"p"`
}

// refAppendKey is a map key whose AppendText upper-cases it.
type refAppendKey string

func (k refAppendKey) AppendText(text []byte) ([]byte, error) {
	return append(text, strings.ToUpper(string(k))...), nil
}

// refIgnoredOption carries a tag option encoding/json ignores.
type refIgnoredOption struct {
	ID string `json:"id,required"`
}

// refUpperKey is a string-kind map key with its own text form; encoding/json
// writes the text form, not the string.
type refUpperKey string

func (k refUpperKey) MarshalText() ([]byte, error) { return []byte(strings.ToUpper(string(k))), nil }

// refPointerMarshaler encodes through a pointer receiver, which encoding/json
// uses only where the value is addressable (inside a pointer output).
type refPointerMarshaler struct{ A int }

func (*refPointerMarshaler) MarshalJSON() ([]byte, error) { return []byte(`{"ptr":true}`), nil }

type refHoldsPointerMarshaler struct {
	In refPointerMarshaler `json:"in"`
}

// refDuplicateNames writes one name twice; a JSON consumer keeps the last.
type refDuplicateNames struct{}

func (refDuplicateNames) MarshalJSON() ([]byte, error) { return []byte(`{"a":1,"a":2}`), nil }

// malformedTag builds a one-field struct whose json tag go vet would reject
// in source: Go 1.27's encoding/json still applies the leading identifier
// of a malformed option and parses quoted parts its own way.
func malformedTag(field, tag string, value any) any {
	typ := reflect.StructOf([]reflect.StructField{{Name: field, Type: reflect.TypeOf(value), Tag: reflect.StructTag("json:" + strconv.Quote(tag))}})
	instance := reflect.New(typ).Elem()
	instance.Field(0).Set(reflect.ValueOf(value))
	return instance.Interface()
}

func referenceSources() map[string]any {
	base := RefBase{ID: "b-1", Note: "base note"}
	return map[string]any{
		"tagged":             refTagged{SKU: "coffee", TotalCents: 3000},
		"embedded":           refEmbedded{RefBase: base, Name: "n"},
		"embeddedPointer":    RefPtrEmbedded{RefBase: &RefBase{ID: "b-1"}, Name: "n"},
		"embeddedNilPointer": RefPtrEmbedded{Name: "n"},
		"embeddedTagged":     RefTaggedEmbedded{RefBase: base, Name: "n"},
		"shadowed":           RefShadowed{RefBase: base, ID: "outer"},
		"ambiguous":          RefAmbiguous{RefBase: base, RefOtherNote: RefOtherNote{Note: "other"}},
		"tagDominates":       RefTagDominates{RefTaggedKey{Key: "tagged"}, RefPlainKey{Key: "plain"}},
		"embeddedLabel":      RefEmbeddedLabel{Label: "gold"},
		"hidden":             refHidden{Secret: "s3cr3t", Dash: "dash"},
		"omitEmpty":          refOmitEmpty{},
		"omitEmptySet":       refOmitEmpty{Note: "kept"},
		"omitZero":           refOmitZero{},
		"stringOption":       refStringOption{N: 5, S: "hi"},
		"money":              refMoney{Cents: 1500},
		"raw":                refRaw{Extra: json.RawMessage(`{"k":"v"}`), Bytes: []byte("hi")},
		"boxed":              refBoxed{Data: refEmbedded{RefBase: base}},
		"pointerRoot":        &refEmbedded{RefBase: base, Name: "n"},
		"namedKeyMap":        map[refCode]int{"a": 1},
		"intKeyMap":          map[int]string{7: "seven"},
		"nilMap":             map[string]any(nil),
		"textKeyMap":         map[refUpperKey]int{"x": 1},
		"addressable":        &refHoldsPointerMarshaler{},
		"notAddressable":     refHoldsPointerMarshaler{},
		"duplicateNames":     refDuplicateNames{},
		"orderValue":         refOuter{Order: refOrder{ID: "o-1", Price: refCents{Cents: 1500}}},
		"orderPointer":       &refOuter{Order: refOrder{ID: "o-1", Price: refCents{Cents: 1500}}},
		"twice":              RefTwice{RefLeft{RefShared{V: RefShapeA{V: 1}}}, RefRight{RefShared{V: RefShapeA{V: 2}}}},
		"untaggedFirst":      RefUntaggedFirst{RefPlainShape{Key: RefShapeB{V: 1}}, RefTaggedShape{Key: RefShapeA{V: 1}}},
		"tieThenTagged":      RefTieThenTagged{RefPlainShape{Key: RefShapeB{V: 1}}, RefPlainShapeC{Key: RefShapeB{V: 1}}, RefTaggedShape{Key: RefShapeA{V: 1}}},
		"recursive":          RefRecursive{RefRecursive: &RefRecursive{V: 2}, V: 1},
		"encodesTo":          refEncodesTo{Secret: 1},
		"appendsText":        refAppendsText{Secret: 1},
		"appendKeyMap":       map[refAppendKey]int{"x": 1},
		"appenderInPointer":  &refHoldsPointerAppender{},
		"appenderInValue":    refHoldsPointerAppender{},
		"ignoredOption":      refIgnoredOption{ID: "b-1"},
		"omitEmptySpace":     malformedTag("ID", "id,omitempty ", ""),
		"omitEmptySemicolon": malformedTag("ID", "id,omitempty;", ""),
		"omitEmptyColon":     malformedTag("ID", "id,omitempty:x", ""),
		"spaceBeforeOption":  malformedTag("ID", "id, omitempty", ""),
		"stringSpace":        malformedTag("N", "n,string ", int64(5)),
		"quotedNameString":   malformedTag("N", "'a,string'", int64(5)),
	}
}

type referenceFixture struct {
	ID       string          `json:"id"`
	Kind     string          `json:"kind"`
	Source   string          `json:"source"`
	Path     []string        `json:"path"`
	Expected json.RawMessage `json:"expected"`
}

type referenceOutcome struct {
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
	// Found distinguishes a resolved null from no resolution.
	Found bool `json:"-"`
}

func (o referenceOutcome) String() string {
	if o.Found {
		return "value " + string(o.Value)
	}
	return "error " + o.Error
}

func expectedOutcome(t *testing.T, raw json.RawMessage) referenceOutcome {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	if value, ok := fields["value"]; ok {
		var compact bytes.Buffer
		if err := json.Compact(&compact, value); err != nil {
			t.Fatal(err)
		}
		return referenceOutcome{Value: compact.Bytes(), Found: true}
	}
	var message string
	if err := json.Unmarshal(fields["error"], &message); err != nil || message == "" {
		t.Fatalf("fixture expectation needs a value or an error: %s", raw)
	}
	return referenceOutcome{Error: message}
}

// resolveThroughEngine commits source as the output of a real node and
// resolves the workflow output reference against it, the same path every
// trigger takes. A panic is reported as an outcome, not a crash.
func resolveThroughEngine(t *testing.T, source any, path []string) (outcome referenceOutcome) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			outcome = referenceOutcome{Error: fmt.Sprintf("panic: %v", recovered)}
		}
	}()
	schema := []byte(`{"type":"object"}`)
	definition := node.MustDefine("test/source", "1.0.0", func(context.Context, any) (any, error) {
		return source, nil
	}, node.Description("reference source"), node.Schemas(schema, schema))
	program := contract.InternalProgram{WorkflowID: "references", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "source", Kind: "call", Node: "test/source"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "source", Path: path}}},
	}}
	result, err := engine.New(map[string]node.Any{"test/source": definition.Any()}).Run(context.Background(), program, map[string]any{})
	if err != nil {
		var classified *engine.Error
		if !errors.As(err, &classified) || classified.Code != "invalid_output_reference" || classified.Err == nil {
			t.Fatalf("unexpected failure class: %v", err)
		}
		return referenceOutcome{Error: classified.Err.Error()}
	}
	encoded, err := json.Marshal(result.Output)
	if err != nil {
		return referenceOutcome{Error: "resolved value does not encode: " + err.Error()}
	}
	return referenceOutcome{Value: encoded, Found: true}
}

// decoded is source as it arrives after any JSON boundary: a checkpoint, a
// foreign runtime, a child workflow.
func decoded(t *testing.T, source any) any {
	t.Helper()
	encoded, err := json.Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	var value any
	if err := json.Unmarshal(encoded, &value); err != nil {
		t.Fatal(err)
	}
	return value
}

// TestReferencesResolveTypedAndDecodedValuesIdentically (#241): a reference
// selects encoding/json keys, so a typed node output and its JSON-decoded
// form resolve every path to the same outcome.
func TestReferencesResolveTypedAndDecodedValuesIdentically(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "references", "json-keys.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document struct {
		Cases []referenceFixture `json:"cases"`
	}
	if err := json.Unmarshal(data, &document); err != nil {
		t.Fatal(err)
	}
	sources := referenceSources()
	used := map[string]bool{}
	kinds := map[string]int{}
	for _, tc := range document.Cases {
		t.Run(tc.ID, func(t *testing.T) {
			source, ok := sources[tc.Source]
			if !ok {
				t.Fatalf("fixture names unknown source %q", tc.Source)
			}
			used[tc.Source] = true
			kinds[tc.Kind]++
			want := expectedOutcome(t, tc.Expected)
			if (tc.Kind == "valid") != want.Found {
				t.Fatalf("kind %q disagrees with expectation %s", tc.Kind, want)
			}
			typed := resolveThroughEngine(t, source, tc.Path)
			fromJSON := resolveThroughEngine(t, decoded(t, source), tc.Path)
			if typed.String() != fromJSON.String() {
				t.Errorf("typed and decoded values resolve differently:\n typed:   %s\n decoded: %s", typed, fromJSON)
			}
			if typed.String() != want.String() {
				t.Errorf("typed value: got %s, want %s", typed, want)
			}
			if fromJSON.String() != want.String() {
				t.Errorf("decoded value: got %s, want %s", fromJSON, want)
			}
		})
	}
	for name := range sources {
		if !used[name] {
			t.Errorf("source %q has no fixture case", name)
		}
	}
	if kinds["valid"] == 0 || kinds["rejected"] == 0 {
		t.Fatalf("fixture needs valid and rejected cases: %v", kinds)
	}
}

// RefShapeA and RefShapeB encode identically, so only the Go type a
// reference hands on shows which field encoding/json's rules selected.
type RefShapeA struct {
	V int `json:"v"`
}

type RefShapeB struct {
	V int `json:"v"`
}

type RefDeepData struct {
	Data RefShapeA `json:"data"`
}

// RefShadowStruct: the shallower "data" shadows the promoted one.
type RefShadowStruct struct {
	RefDeepData
	Data RefShapeB `json:"data"`
}

type RefTaggedShape struct {
	Key RefShapeA `json:"Key"`
}

type RefPlainShape struct {
	Key RefShapeB
}

// RefTagDominantStruct: at equal depth the tagged "Key" wins.
type RefTagDominantStruct struct {
	RefTaggedShape
	RefPlainShape
}

type RefShared struct {
	V RefShapeA
}

type RefLeft struct{ RefShared }

type RefRight struct{ RefShared }

// RefTwice reaches RefShared twice at one depth, so its "V" is ambiguous.
type RefTwice struct {
	RefLeft
	RefRight
}

// refCents encodes through a pointer receiver, like many money types.
type refCents struct {
	Cents int64
}

func (c *refCents) MarshalJSON() ([]byte, error) {
	return json.Marshal(fmt.Sprintf("%d.%02d", c.Cents/100, c.Cents%100))
}

type refOrder struct {
	ID    string
	Price refCents
}

type refOuter struct {
	Order refOrder `json:"order"`
}

type RefPlainShapeC struct {
	Key RefShapeB
}

// RefUntaggedFirst lists the untagged "Key" before the tagged one.
type RefUntaggedFirst struct {
	RefPlainShape
	RefTaggedShape
}

// RefTieThenTagged: two untagged "Key"s tie, then a tagged one breaks it.
type RefTieThenTagged struct {
	RefPlainShape
	RefPlainShapeC
	RefTaggedShape
}

// RefRecursive embeds itself; key indexing must terminate.
type RefRecursive struct {
	*RefRecursive
	V int
}

// TestTypedReferencesKeepGoTypes: native nodes assert their declared input
// type, so a selected field is handed on as its Go value, the same for a T
// and a *T output. Only a ,string member and members inside a container
// with its own marshaler arrive decoded.
func TestTypedReferencesKeepGoTypes(t *testing.T) {
	sources := referenceSources()
	sources["shadowStruct"] = RefShadowStruct{RefDeepData: RefDeepData{Data: RefShapeA{V: 1}}, Data: RefShapeB{V: 1}}
	sources["tagDominantStruct"] = RefTagDominantStruct{RefTaggedShape{Key: RefShapeA{V: 1}}, RefPlainShape{Key: RefShapeB{V: 1}}}
	for _, tc := range []struct {
		source string
		path   []string
		want   any
	}{
		{"tagged", []string{"TotalCents"}, int64(3000)},
		{"embedded", []string{"id"}, "b-1"},
		{"embeddedTagged", []string{"base"}, RefBase{ID: "b-1", Note: "base note"}},
		{"boxed", []string{"data"}, refEmbedded{RefBase: RefBase{ID: "b-1", Note: "base note"}}},
		{"pointerRoot", []string{"RefBase"}, nil},
		{"intKeyMap", []string{"7"}, "seven"},
		{"namedKeyMap", []string{"a"}, 1},
		{"stringOption", []string{"n"}, "5"},
		{"money", []string{"amount"}, "15.00"},
		{"notAddressable", []string{"in"}, refPointerMarshaler{}},
		{"addressable", []string{"in"}, refPointerMarshaler{}},
		{"orderValue", []string{"order"}, refOrder{ID: "o-1", Price: refCents{Cents: 1500}}},
		{"orderPointer", []string{"order"}, refOrder{ID: "o-1", Price: refCents{Cents: 1500}}},
		{"orderValue", []string{"order", "Price"}, refCents{Cents: 1500}},
		{"orderPointer", []string{"order", "Price"}, refCents{Cents: 1500}},
		{"shadowStruct", []string{"data"}, RefShapeB{V: 1}},
		{"tagDominantStruct", []string{"Key"}, RefShapeA{V: 1}},
		{"twice", []string{"V"}, nil},
		{"untaggedFirst", []string{"Key"}, RefShapeA{V: 1}},
		{"tieThenTagged", []string{"Key"}, RefShapeA{V: 1}},
		{"recursive", []string{"V"}, 1},
	} {
		t.Run(tc.source+"."+strings.Join(tc.path, "."), func(t *testing.T) {
			got := resolveValue(t, sources[tc.source], tc.path)
			if tc.want == nil {
				if got.err == nil {
					t.Fatalf("resolved %#v (%T); want a missing field", got.value, got.value)
				}
				return
			}
			if got.err != nil || !reflect.DeepEqual(got.value, tc.want) {
				t.Fatalf("got %#v (%T) err=%v; want %#v (%T)", got.value, got.value, got.err, tc.want, tc.want)
			}
		})
	}
}

// RefWrapper promotes RefTaggedEmbedded's "base" object to its own level.
type RefWrapper struct {
	RefTaggedEmbedded
}

// TestCallInputReferenceDeliversTheDeclaredType: a selected struct reaches
// a native node as the node's own input type, whether the source node
// returned a value or a pointer.
func TestCallInputReferenceDeliversTheDeclaredType(t *testing.T) {
	schema := []byte(`{"type":"object"}`)
	order := refOuter{Order: refOrder{ID: "o-1", Price: refCents{Cents: 1500}}}
	readBase := node.MustDefine("test/consume", "1.0.0", func(_ context.Context, base RefBase) (string, error) {
		return base.ID, nil
	}, node.Description("reads a base"), node.Schemas(schema, []byte(`{"type":"string"}`)))
	readOrder := node.MustDefine("test/consume", "1.0.0", func(_ context.Context, order refOrder) (string, error) {
		return order.ID, nil
	}, node.Description("reads an order"), node.Schemas(schema, []byte(`{"type":"string"}`)))
	readPrice := node.MustDefine("test/consume", "1.0.0", func(_ context.Context, price refCents) (string, error) {
		return fmt.Sprint(price.Cents), nil
	}, node.Description("reads a price"), node.Schemas(schema, []byte(`{"type":"string"}`)))
	for _, tc := range []struct {
		name    string
		source  any
		path    []string
		consume node.Any
		want    string
	}{
		{"promoted struct", RefWrapper{RefTaggedEmbedded{RefBase: RefBase{ID: "b-1"}, Name: "n"}}, []string{"base"}, readBase.Any(), "b-1"},
		{"struct from value output", order, []string{"order"}, readOrder.Any(), "o-1"},
		{"struct from pointer output", &order, []string{"order"}, readOrder.Any(), "o-1"},
		{"pointer-marshaled field from value output", order, []string{"order", "Price"}, readPrice.Any(), "1500"},
		{"pointer-marshaled field from pointer output", &order, []string{"order", "Price"}, readPrice.Any(), "1500"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := node.MustDefine("test/source", "1.0.0", func(context.Context, any) (any, error) {
				return tc.source, nil
			}, node.Description("reference source"), node.Schemas(schema, schema))
			program := contract.InternalProgram{WorkflowID: "references", Instructions: []contract.InternalInstruction{
				{Index: 0, ID: "source", Kind: "call", Node: "test/source"},
				{Index: 1, ID: "consume", Kind: "call", Node: "test/consume", References: []contract.Reference{{Step: "source", Path: tc.path}}},
				{Index: 2, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "consume"}}},
			}}
			result, err := engine.New(map[string]node.Any{"test/source": source.Any(), "test/consume": tc.consume}).Run(context.Background(), program, map[string]any{})
			if err != nil || result.Output != tc.want {
				t.Fatalf("output=%#v err=%v", result.Output, err)
			}
		})
	}
}

// TestUnencodableSiblingsDoNotBlockReferences: only the selected member is
// examined, so a sibling encoding/json cannot encode (NaN, ±Inf, a map with
// bool keys, a func) does not stop a reference beside it. Descending into
// such a value still fails.
func TestUnencodableSiblingsDoNotBlockReferences(t *testing.T) {
	type withSibling struct {
		Ratio float64        `json:"ratio"`
		Flags map[bool]int   `json:"flags"`
		Hook  func()         `json:"hook"`
		Quiet float64        `json:"quiet,omitempty"`
		Text  float64        `json:"text,string"`
		Next  map[string]any `json:"next"`
		ID    string         `json:"id"`
	}
	for _, ratio := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		source := withSibling{Ratio: ratio, Flags: map[bool]int{true: 1}, Hook: func() {}, Text: ratio, ID: "b-1"}
		if got := resolveValue(t, source, []string{"id"}); got.err != nil || got.value != "b-1" {
			t.Fatalf("ratio %v: value=%#v err=%v", ratio, got.value, got.err)
		}
		if got := resolveValue(t, source, []string{"ratio"}); got.err != nil || fmt.Sprint(got.value) != fmt.Sprint(ratio) {
			t.Fatalf("unencodable field: value=%#v err=%v", got.value, got.err)
		}
		if got := resolveValue(t, source, []string{"flags"}); got.err != nil || !reflect.DeepEqual(got.value, map[bool]int{true: 1}) {
			t.Fatalf("unencodable map field: value=%#v err=%v", got.value, got.err)
		}
		if got := resolveValue(t, source, []string{"flags", "true"}); got.err == nil || !strings.Contains(got.err.Error(), "value has no JSON encoding") {
			t.Fatalf("descended into a bool-keyed map: value=%#v err=%v", got.value, got.err)
		}
		if got := resolveValue(t, source, []string{"quiet"}); got.err == nil {
			t.Fatalf("empty omitempty field resolved: %#v", got.value)
		}
		if got := resolveValue(t, source, []string{"text"}); got.err != nil || fmt.Sprint(got.value) != fmt.Sprint(ratio) {
			t.Fatalf("unencodable ,string field: value=%#v err=%v", got.value, got.err)
		}
		if got := resolveValue(t, source, []string{"ratio", "x"}); got.err == nil || !strings.Contains(got.err.Error(), `cannot read "x" from a JSON number`) {
			t.Fatalf("descended into a number: value=%#v err=%v", got.value, got.err)
		}
		if got := resolveValue(t, source, []string{"hook", "x"}); got.err == nil || !strings.Contains(got.err.Error(), "value has no JSON encoding") {
			t.Fatalf("descended into a func: value=%#v err=%v", got.value, got.err)
		}
	}
}

type resolved struct {
	value any
	err   error
}

func resolveValue(t *testing.T, source any, path []string) resolved {
	t.Helper()
	schema := []byte(`{"type":"object"}`)
	definition := node.MustDefine("test/source", "1.0.0", func(context.Context, any) (any, error) {
		return source, nil
	}, node.Description("reference source"), node.Schemas(schema, schema))
	program := contract.InternalProgram{WorkflowID: "references", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "source", Kind: "call", Node: "test/source"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "source", Path: path}}},
	}}
	result, err := engine.New(map[string]node.Any{"test/source": definition.Any()}).Run(context.Background(), program, map[string]any{})
	return resolved{value: result.Output, err: err}
}
