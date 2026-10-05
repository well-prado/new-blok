package otel_test

import (
	"encoding/base64"
	"testing"

	"github.com/well-prado/new-blok/observe/otel"
	"github.com/well-prado/new-blok/observe/redact"
)

// TestAllowlistedLogAttributesStayOpaque is ADR 0021's telemetry rule: an
// application that allowlists a log attribute selects it for export, but a
// sensitive key's value and a credential-shaped value (an encoded one
// included) are still exported only as the redaction marker.
func TestAllowlistedLogAttributesStayOpaque(t *testing.T) {
	h := newHarness(t, harnessOptions{ratio: 1, tune: func(c *otel.Config) { c.LogAttributes = []string{"sku", "card_token"} }})
	encodedSKU := base64.StdEncoding.EncodeToString([]byte("password=SYNTHETIC-otel-0001"))
	if _, err := h.run(t, "run-opaque-encoded", "tenant-a", orderInput{SKU: encodedSKU, Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := h.run(t, "run-opaque-plain", "tenant-a", orderInput{SKU: "coffee", Quantity: 1}); err != nil {
		t.Fatal(err)
	}
	h.flush(t)
	for _, sentinel := range []string{cardTokenSentinel, encodedSKU, "SYNTHETIC-otel-0001"} {
		if h.collector.contains(sentinel) {
			t.Fatalf("%q reached the collector", sentinel)
		}
	}
	attrs := exportedStrings(h.collector)
	tokens, skus := attrs["card_token"], attrs["sku"]
	if len(tokens) != 2 || tokens[0] != redact.Marker || tokens[1] != redact.Marker {
		t.Fatalf("card_token exported as %q, want two markers", tokens)
	}
	markers, plain := 0, 0
	for _, value := range skus {
		switch value {
		case redact.Marker:
			markers++
		case "coffee":
			plain++
		}
	}
	if markers != 1 || plain != 1 {
		t.Fatalf("sku exported as %q, want one marker and one coffee", skus)
	}
}
