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
