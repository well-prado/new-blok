package otel_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/observe/otel"
)

// exportedStrings returns every string attribute value on exported spans and
// log records, keyed by attribute name.
func exportedStrings(c *collector) map[string][]string {
	spans, _, logs := c.snapshot()
	out := map[string][]string{}
	add := func(values []*commonpb.KeyValue) {
		for _, kv := range values {
			if v, ok := kv.Value.GetValue().(*commonpb.AnyValue_StringValue); ok {
				out[kv.Key] = append(out[kv.Key], v.StringValue)
			}
		}
	}
	for _, span := range spans {
		add(span.Attributes)
	}
	for _, record := range logs {
		add(record.Attributes)
	}
	return out
}

// TestSpanAndLogTenantsFollowTheAllowlist drives 50 distinct user-shaped
// tenants. By default spans and logs carry the allowlisted tenant or
// "other", exactly like metrics; only an explicit RawTenants opt-in exports
// the raw values, and even then never as metric labels.
func TestSpanAndLogTenantsFollowTheAllowlist(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprintf("RawTenants=%v", raw), func(t *testing.T) {
			h := newHarness(t, harnessOptions{ratio: 1, tune: func(c *otel.Config) { c.RawTenants = raw }})
			for i := 0; i < 50; i++ {
				if _, err := h.run(t, fmt.Sprintf("run-tenant-%d", i), fmt.Sprintf("user-%d", i), orderInput{SKU: "coffee", Quantity: 1}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := h.run(t, "run-tenant-listed", "tenant-a", orderInput{SKU: "coffee", Quantity: 1}); err != nil {
				t.Fatal(err)
			}
			h.flush(t)
			tenants := map[string]bool{}
			for _, value := range exportedStrings(h.collector)[otel.AttrTenant] {
				tenants[value] = true
			}
			if raw {
				if len(tenants) != 51 || !tenants["user-7"] || !tenants["tenant-a"] {
					t.Fatalf("RawTenants exported %d distinct tenants, want all 51", len(tenants))
				}
			} else if len(tenants) != 2 || !tenants["other"] || !tenants["tenant-a"] {
				t.Fatalf("spans/logs exported tenants %v, want only tenant-a and other", tenants)
			}
			if !raw && h.collector.contains("user-7") {
				t.Fatal("a raw user tenant reached the collector without the opt-in")
			}
			for _, point := range h.collector.latest(otel.MetricRuns) {
				if tenant := point.attrs[otel.AttrTenant]; tenant != "tenant-a" && tenant != "other" {
					t.Fatalf("metric tenant label %q escaped the allowlist", tenant)
				}
			}
		})
	}
}

// TestExportedIdentitiesAreBoundedShapes runs a hand-built program whose
// step id is 120 characters under a 200-character run id. Every string
// exported on spans and logs must be a valid label (at most 64 bytes);
// identifiers that are not become "h:" plus 16 hex digits of their SHA-256.
func TestExportedIdentitiesAreBoundedShapes(t *testing.T) {
	h := newHarness(t, harnessOptions{ratio: 1})
	longStep := strings.Repeat("s", 120)
	longRun := "run-" + strings.Repeat("r", 196)
	program := contract.InternalProgram{WorkflowID: "shop/order", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: longStep, Kind: "call", Node: "shop/charge"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: longStep}}},
	}}
	if _, err := h.runner.Run(context.Background(), program, orderInput{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: longRun, Principal: principalSentinel, Tenant: "tenant-a"}); err != nil {
		t.Fatal(err)
	}
	h.flush(t)
	values := exportedStrings(h.collector)
	if len(values[otel.AttrAttemptID]) == 0 || len(values[otel.AttrRunID]) == 0 {
		t.Fatalf("no ids exported: %v", values)
	}
	for key, list := range values {
		for _, value := range list {
			if !observe.ValidLabel(value) {
				t.Fatalf("%s exported %d-byte value %q", key, len(value), value)
			}
		}
	}
	sum := sha256.Sum256([]byte(longRun))
	if want := "h:" + hex.EncodeToString(sum[:8]); values[otel.AttrRunID][0] != want {
		t.Fatalf("run id %q, want %q", values[otel.AttrRunID][0], want)
	}
	// The long step's attempt id embeds the 120-character step id, so it is
	// exported as a digest; the short output step's id stays readable.
	digests := 0
	for _, value := range values[otel.AttrAttemptID] {
		if strings.HasPrefix(value, "h:") {
			if len(value) != 18 {
				t.Fatalf("attempt id digest %q is not h: plus 16 hex digits", value)
			}
			digests++
		}
	}
	if digests == 0 {
		t.Fatalf("the long step's attempt id was not bounded: %v", values[otel.AttrAttemptID])
	}
	if h.collector.contains(strings.Repeat("s", 65)) || h.collector.contains(strings.Repeat("r", 65)) {
		t.Fatal("an over-long identifier reached the collector")
	}
}
