package generate

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

const orderSource = `package order

type Address struct {
	City string
}

type Order struct {
	ID      string
	Address *Address
	Items   []string
}
`

func TestSourceGeneratesDeterministicFieldRefsAndArgs(t *testing.T) {
	first, err := Source([]byte(orderSource), Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Source([]byte(orderSource), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("generation is not byte-stable")
	}
	text := string(first)
	for _, expected := range []string{"Code generated", "type OrderRef struct", "func (r OrderRef) ID() flow.Ref[string]", "func (r OrderRef) Address() flow.Ref[*Address]", "type OrderArgs struct"} {
		if !strings.Contains(text, expected) {
			t.Fatalf("generated source missing %q:\n%s", expected, text)
		}
	}
}

func TestSourceDoesNotExecuteInitAndRejectsUnsupportedTypes(t *testing.T) {
	source := []byte("package bomb\nfunc init() { panic(\"init bomb\") }\ntype Bomb struct { Handler func() }\n")
	if _, err := Source(source, Options{}); err == nil || !strings.Contains(err.Error(), "unsupported type") {
		t.Fatalf("got %v", err)
	}
}

// TestAccessorsSelectTheJSONKey: an accessor selects the key encoding/json
// writes, which is what a workflow reference resolves: the tag's name, or the
// exact Go field name without one. A field tagged "-" has no accessor (#240).
func TestAccessorsSelectTheJSONKey(t *testing.T) {
	source := []byte("package shop\n\ntype Line struct {\n" +
		"\tSKU string `json:\"sku\"`\n" +
		"\tTotalCents int64\n" +
		"\tTotal int64 `json:\"total_cents,omitempty\"`\n" +
		"\tNote string `json:\",omitempty\"`\n" +
		"\tSecret string `json:\"-\"`\n" +
		"\tDash string `json:\"-,\"`\n" +
		"}\n")
	generated, err := Source(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	for accessor, path := range map[string]string{"SKU": "sku", "TotalCents": "TotalCents", "Total": "total_cents", "Note": "Note", "Dash": "-"} {
		want := "func (r LineRef) " + accessor + "() "
		index := strings.Index(text, want)
		if index < 0 {
			t.Fatalf("no %s accessor:\n%s", accessor, text)
		}
		line := text[index : index+strings.IndexByte(text[index:], '\n')]
		if !strings.Contains(line, `(r.value, "`+path+`")`) {
			t.Fatalf("%s selects %q; want %q", accessor, line, path)
		}
	}
	if strings.Contains(text, "func (r LineRef) Secret()") {
		t.Fatalf("a field tagged \"-\" got an accessor:\n%s", text)
	}
}

// TestDottedKeyIsRejected: a reference path splits on dots, so a json key
// with one cannot be selected; the generator says so instead of emitting an
// accessor that fails at run time.
func TestDottedKeyIsRejected(t *testing.T) {
	source := []byte("package shop\n\ntype Line struct {\n\tAB string `json:\"a.b\"`\n}\n")
	if _, err := Source(source, Options{}); err == nil || !strings.Contains(err.Error(), `Line.AB: json key "a.b" contains '.'`) {
		t.Fatalf("err=%v; want the dotted key rejected", err)
	}
}

// TestCustomMarshalersGetNoFieldAccessors: a type with its own MarshalJSON
// or MarshalText encodes to whatever that method returns, so its Go fields
// are not reference keys; an accessor for one would fail at run time (#241).
func TestCustomMarshalersGetNoFieldAccessors(t *testing.T) {
	source := []byte("package shop\n\n" +
		"type Money struct{ Cents int64 }\n\n" +
		"func (m Money) MarshalJSON() ([]byte, error) { return nil, nil }\n\n" +
		"type Cents struct{ Value int64 }\n\n" +
		"func (c *Cents) MarshalJSON() ([]byte, error) { return nil, nil }\n\n" +
		"type Code struct{ Text string }\n\n" +
		"func (c Code) MarshalText() ([]byte, error) { return nil, nil }\n\n" +
		"type NotJSON struct{ Value int64 }\n\n" +
		"func (NotJSON) MarshalJSON() string { return \"\" }\n\n" +
		"type Line struct {\n\tPrice Money `json:\"price\"`\n}\n")
	generated, err := Source(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	for _, absent := range []string{"MoneyRef", "CentsRef", "CodeRef"} {
		if strings.Contains(text, absent) {
			t.Fatalf("custom-marshaled type got %s:\n%s", absent, text)
		}
	}
	for _, present := range []string{"func (r NotJSONRef) Value() flow.Ref[int64]", "func (r LineRef) Price() flow.Ref[Money]"} {
		if !strings.Contains(text, present) {
			t.Fatalf("missing %q:\n%s", present, text)
		}
	}
}

// TestStringOptionFieldsGetNoAccessor: a reference resolves a ",string"
// field to its quoted text, so a flow.Ref of the field's Go type would be
// wrong (#241).
func TestStringOptionFieldsGetNoAccessor(t *testing.T) {
	source := []byte("package shop\n\ntype Line struct {\n" +
		"\tCount int64 `json:\"count,string\"`\n" +
		"\tNote string `json:\"note,omitempty,string\"`\n" +
		"\tTotal int64 `json:\"total\"`\n" +
		"}\n")
	generated, err := Source(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	for _, absent := range []string{"func (r LineRef) Count()", "func (r LineRef) Note()"} {
		if strings.Contains(text, absent) {
			t.Fatalf("a ,string field got an accessor:\n%s", text)
		}
	}
	if !strings.Contains(text, `func (r LineRef) Total() flow.Ref[int64] { return flow.Select[Line, int64](r.value, "total") }`) {
		t.Fatalf("missing Total accessor:\n%s", text)
	}
}

// TestNewerEncodersGetNoFieldAccessors: Go 1.27's encoding/json also calls
// AppendText and MarshalJSONTo, by value or pointer receiver (#241).
func TestNewerEncodersGetNoFieldAccessors(t *testing.T) {
	source := []byte("package shop\n\n" +
		"type Appends struct{ Secret int64 }\n\n" +
		"func (Appends) AppendText(b []byte) ([]byte, error) { return b, nil }\n\n" +
		"type PointerAppends struct{ Secret int64 }\n\n" +
		"func (*PointerAppends) AppendText(b []byte) ([]byte, error) { return b, nil }\n\n" +
		"// Encoder stands in for jsontext.Encoder: the generator type-checks\n" +
		"// one file without imports.\n" +
		"type Encoder struct{}\n\n" +
		"type EncodesTo struct{ Secret int64 }\n\n" +
		"func (EncodesTo) MarshalJSONTo(*Encoder) error { return nil }\n\n" +
		"type Plain struct{ Secret int64 }\n")
	generated, err := Source(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	for _, absent := range []string{"AppendsRef", "PointerAppendsRef", "EncodesToRef"} {
		if strings.Contains(text, absent) {
			t.Fatalf("type with its own encoder got %s:\n%s", absent, text)
		}
	}
	if !strings.Contains(text, "func (r PlainRef) Secret() flow.Ref[int64]") {
		t.Fatalf("missing Plain accessor:\n%s", text)
	}
}

// TestSharedKeyGetsOneAccessor: when fields share a key, encoding/json
// writes the tagged one and drops a tie; the generator emits exactly that
// field's accessor, or none (#241).
func TestSharedKeyGetsOneAccessor(t *testing.T) {
	source := []byte("package shop\n\ntype Line struct {\n" +
		"\tName string\n" +
		"\tAlias int64 `json:\"Name\"`\n" +
		"\tLeft int64 `json:\"side\"`\n" +
		"\tRight int64 `json:\"side\"`\n" +
		"\tCount int64 `json:\"n,string\"`\n" +
		"\tTotal int64 `json:\"n\"`\n" +
		"}\n")
	generated, err := Source(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	if !strings.Contains(text, `func (r LineRef) Alias() flow.Ref[int64] { return flow.Select[Line, int64](r.value, "Name") }`) {
		t.Fatalf("missing the dominant Name accessor:\n%s", text)
	}
	for _, absent := range []string{"func (r LineRef) Name()", "func (r LineRef) Left()", "func (r LineRef) Right()", "func (r LineRef) Count()", "func (r LineRef) Total()"} {
		if strings.Contains(text, absent) {
			t.Fatalf("unexpected %s:\n%s", absent, text)
		}
	}
}

// TestTaggedEmbeddedFieldTakesPartInDominance: a tagged embedded field is
// nested under its tag name at the top level, so it ties with a tagged
// field of the same key and encoding/json writes neither; an untagged
// embedded non-struct is written under its type name (#241).
func TestTaggedEmbeddedFieldTakesPartInDominance(t *testing.T) {
	source := []byte("package shop\n\n" +
		"type Inner struct{ Z int64 }\n\n" +
		"type Label string\n\n" +
		"type Line struct {\n" +
		"\tInner `json:\"x\"`\n" +
		"\tX int64 `json:\"x\"`\n" +
		"\tLabel\n" +
		"\tName string `json:\"Label\"`\n" +
		"\tY int64\n" +
		"}\n")
	generated, err := Source(source, Options{})
	if err != nil {
		t.Fatal(err)
	}
	text := string(generated)
	if strings.Contains(text, "func (r LineRef) X()") {
		t.Fatalf("X ties with the tagged embedded Inner and is never written:\n%s", text)
	}
	for _, present := range []string{
		`func (r LineRef) Name() flow.Ref[string] { return flow.Select[Line, string](r.value, "Label") }`,
		`func (r LineRef) Y() flow.Ref[int64] { return flow.Select[Line, int64](r.value, "Y") }`,
	} {
		if !strings.Contains(text, present) {
			t.Fatalf("missing %q:\n%s", present, text)
		}
	}
}

// TestNonPlainTagsGetNoAccessors: encoding/json applies a malformed option's
// leading identifier (",string " still quotes) and parses quoted parts its
// own way, so a type with such a tag gets no accessors at all (#241).
func TestNonPlainTagsGetNoAccessors(t *testing.T) {
	for _, tag := range []string{`n,string `, `'a,string'`, `n,omitempty;`} {
		source := []byte("package shop\n\ntype Line struct {\n\tCount int64 `json:" + strconv.Quote(tag) + "`\n\tTotal int64\n}\n")
		generated, err := Source(source, Options{})
		if err != nil {
			t.Fatalf("tag %q: %v", tag, err)
		}
		if strings.Contains(string(generated), "LineRef") {
			t.Fatalf("tag %q: generated accessors:\n%s", tag, generated)
		}
	}
}
