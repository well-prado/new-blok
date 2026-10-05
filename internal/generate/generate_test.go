package generate

import (
	"bytes"
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
