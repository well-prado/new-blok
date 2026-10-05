package engine_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
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

// TestTypedReferencesKeepGoTypes: native nodes assert their declared input
// type, so a typed output resolves to the Go value whenever that value's own
// encoding is the selected member. Only members whose JSON differs from the
// Go value's (,string, custom marshalers) arrive decoded.
func TestTypedReferencesKeepGoTypes(t *testing.T) {
	sources := referenceSources()
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
		{"addressable", []string{"in"}, map[string]any{"ptr": true}},
	} {
		t.Run(tc.source+"."+strings.Join(tc.path, "."), func(t *testing.T) {
			got := resolveValue(t, sources[tc.source], tc.path)
			if tc.want == nil {
				if got.err == nil {
					t.Fatalf("resolved %#v; want a missing field", got.value)
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

// TestCallInputReferenceDeliversTheDeclaredType: a promoted struct selected
// by its JSON key reaches a native node as the node's own input type.
func TestCallInputReferenceDeliversTheDeclaredType(t *testing.T) {
	schema := []byte(`{"type":"object"}`)
	source := node.MustDefine("test/source", "1.0.0", func(context.Context, any) (RefWrapper, error) {
		return RefWrapper{RefTaggedEmbedded{RefBase: RefBase{ID: "b-1"}, Name: "n"}}, nil
	}, node.Description("reference source"), node.Schemas(schema, schema))
	consume := node.MustDefine("test/consume", "1.0.0", func(_ context.Context, base RefBase) (string, error) {
		return base.ID, nil
	}, node.Description("reads a base"), node.Schemas(schema, []byte(`{"type":"string"}`)))
	program := contract.InternalProgram{WorkflowID: "references", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "source", Kind: "call", Node: "test/source"},
		{Index: 1, ID: "consume", Kind: "call", Node: "test/consume", References: []contract.Reference{{Step: "source", Path: []string{"base"}}}},
		{Index: 2, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "consume"}}},
	}}
	result, err := engine.New(map[string]node.Any{"test/source": source.Any(), "test/consume": consume.Any()}).Run(context.Background(), program, map[string]any{})
	if err != nil || result.Output != "b-1" {
		t.Fatalf("output=%#v err=%v", result.Output, err)
	}
}

// TestReferenceIntoValueWithoutJSONEncodingFails: a value encoding/json
// cannot encode has no keys to select; resolution fails instead of reading
// Go fields the JSON form would never carry.
func TestReferenceIntoValueWithoutJSONEncodingFails(t *testing.T) {
	got := resolveValue(t, struct {
		Hook  func() `json:"hook"`
		Count int    `json:"count"`
	}{Count: 1}, []string{"count"})
	if got.err == nil || !strings.Contains(got.err.Error(), "value has no JSON encoding") {
		t.Fatalf("value=%#v err=%v", got.value, got.err)
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
