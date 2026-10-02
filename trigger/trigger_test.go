package trigger

import (
	"errors"
	"testing"
)

const workflowInput = `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100},"note":{"type":"string"},"addr":{"type":"object","properties":{"zip":{"type":"string"}},"required":["zip"]},"tags":{"type":"array","items":{"type":"string"}}},"required":["sku","quantity"]}`

func TestCheckBindingsProvesEveryProducedValueIsAccepted(t *testing.T) {
	for _, tc := range []struct {
		name, schema, code string
	}{
		{"identical required fields", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`, ""},
		{"optional workflow fields omitted", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":2,"maximum":10}},"required":["sku","quantity"]}`, ""},
		{"nested and array fields that match", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100},"addr":{"type":"object","properties":{"zip":{"type":"string"}},"required":["zip"]},"tags":{"type":"array","items":{"type":"string"}}},"required":["sku","quantity"]}`, ""},
		{"anyOf branches each accepted", `{"anyOf":[{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":5}},"required":["sku","quantity"]},{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":50,"maximum":60}},"required":["sku","quantity"]}]}`, ""},
		{"wrong field type", `{"type":"object","properties":{"sku":{"type":"integer"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"wrong nested type", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100},"addr":{"type":"object","properties":{"zip":{"type":"integer"}},"required":["zip"]}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"wider integer bounds", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100000}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"unbounded integer", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer"}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"field the closed workflow rejects", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100},"partnerRef":{"type":"string"}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"open binding into closed workflow", `{"type":"object","additionalProperties":true,"properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"required field made optional", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku"]}`, "incompatible_binding"},
		{"nullable into non-nullable", `{"type":"object","properties":{"sku":{"type":"string","nullable":true},"quantity":{"type":"integer","minimum":1,"maximum":100}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"array item type changed", `{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":100},"tags":{"type":"array","items":{"type":"integer"}}},"required":["sku","quantity"]}`, "incompatible_binding"},
		{"one anyOf branch rejected", `{"anyOf":[{"type":"object","properties":{"sku":{"type":"string"},"quantity":{"type":"integer","minimum":1,"maximum":5}},"required":["sku","quantity"]},{"type":"string"}]}`, "incompatible_binding"},
		{"missing schema", ``, "invalid_binding"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckBindings("shop/quote", []byte(workflowInput), []Binding{{ID: "b", Kind: HTTP, Workflow: "shop/quote", InputSchema: []byte(tc.schema)}})
			if tc.code == "" {
				if err != nil {
					t.Fatalf("compatible binding rejected: %v", err)
				}
				return
			}
			var diagnostic *Error
			if !errors.As(err, &diagnostic) || diagnostic.Code != tc.code {
				t.Fatalf("err=%v, want %s", err, tc.code)
			}
		})
	}
}

func TestDeclarationsAreRestrictedPerKind(t *testing.T) {
	for _, tc := range []struct {
		name  string
		d     Declaration
		valid bool
	}{
		{"http memory caller", Declaration{HTTP, "a", Memory, CancelWork, Caller}, true},
		{"http durable detach", Declaration{HTTP, "a", Durable, StopWaiting, Caller}, true},
		{"worker durable redeliver producer", Declaration{Worker, "a", Durable, Redeliver, TrustedProducer}, true},
		{"pubsub with caller principal", Declaration{PubSub, "a", Durable, Redeliver, Caller}, true},
		{"http without caller authentication", Declaration{HTTP, "a", Memory, CancelWork, TrustedProducer}, false},
		{"mcp without caller authentication", Declaration{MCP, "a", Memory, CancelWork, TrustedProducer}, false},
		{"worker claiming a caller", Declaration{Worker, "a", Durable, Redeliver, Caller}, false},
		{"memory work that survives disconnect", Declaration{HTTP, "a", Memory, StopWaiting, Caller}, false},
		{"durable work canceled by disconnect", Declaration{SSE, "a", Durable, CancelWork, Caller}, false},
		{"memory webhook", Declaration{Webhook, "a", Memory, CancelWork, Caller}, false},
		{"unknown kind", Declaration{"pigeon", "a", Memory, CancelWork, Caller}, false},
		{"unnamed adapter", Declaration{HTTP, " ", Memory, CancelWork, Caller}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.d.Validate(); (err == nil) != tc.valid {
				t.Fatalf("Validate()=%v, valid=%v", err, tc.valid)
			}
		})
	}
	for _, kind := range Kinds() {
		if len(allowed[kind]) == 0 || len(authentication[kind]) == 0 {
			t.Fatalf("%s has no permitted declaration", kind)
		}
	}
}

type codedError struct{ code, class string }

func (e codedError) Error() string      { return e.code }
func (e codedError) ErrorCode() string  { return e.code }
func (e codedError) ErrorClass() string { return e.class }

func TestClassifyReportsOnlyStableCodes(t *testing.T) {
	if code, class, ok := Classify(codedError{"out_of_stock", "validation"}); !ok || code != "out_of_stock" || class != "validation" {
		t.Fatalf("stable code dropped: %q %q %v", code, class, ok)
	}
	for _, bad := range []string{"", "Out Of Stock", "user alice@example.com not found", "dsn=postgres://u:p@h/db", "x" + string(make([]byte, 80))} {
		if _, _, ok := Classify(codedError{bad, "validation"}); ok {
			t.Fatalf("unstable code %q was reported", bad)
		}
	}
	if _, _, ok := Classify(errors.New("plain")); ok {
		t.Fatal("unclassified error was reported")
	}
}
