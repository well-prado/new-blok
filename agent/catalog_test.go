package agent

import (
	"context"
	"errors"
	"testing"
	"time"
)

var schemaObject = []byte(`{"type":"object"}`)

func safeTool(name string, m Manifest) Tool {
	return Tool{Name: name, Version: "1.0.0", Description: "test", InputSchema: schemaObject, OutputSchema: schemaObject, Manifest: m, Invoke: func(context.Context, []byte) ([]byte, error) { return []byte(`{"ok":true}`), nil }}
}
func budget() Budget {
	return Budget{MaxDepth: 4, MaxInputBytes: 1024, MaxOutputBytes: 1024, MaxTokens: 100, Deadline: time.Now().Add(time.Minute)}
}

func TestCatalogFailsClosedAndOmitsUnauthorizedTools(t *testing.T) {
	c := NewCatalog()
	good := safeTool("native/read", Manifest{Version: 1, Compatibility: "agent-compatible", Capabilities: []string{"read"}})
	if err := c.Register(good); err != nil {
		t.Fatal(err)
	}
	if err := c.Register(safeTool("legacy/tool", Manifest{Version: 1, Compatibility: "trusted-legacy"})); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("legacy registration: %v", err)
	}
	if got := c.List(Principal{Capabilities: []string{"write"}, MaxDepth: 4}); len(got) != 0 {
		t.Fatalf("unauthorized listing: %+v", got)
	}
	if got := c.List(Principal{Capabilities: []string{"read"}, MaxDepth: 4}); len(got) != 1 || got[0].CapabilityDigest == "" {
		t.Fatalf("authorized listing: %+v", got)
	}
}

func TestInvocationUsesAdmissionBudgetAndNormalizedCopy(t *testing.T) {
	c := NewCatalog()
	if err := c.Register(safeTool("native/read", Manifest{Version: 1, Compatibility: "agent-compatible", Capabilities: []string{"read"}})); err != nil {
		t.Fatal(err)
	}
	out, err := c.Invoke(context.Background(), Principal{Capabilities: []string{"read"}, MaxDepth: 4}, "native/read", "1.0.0", []byte(`{}`), budget())
	if err != nil || string(out) != `{"ok":true}` {
		t.Fatalf("invoke: %s %v", out, err)
	}
	b := budget()
	b.MaxInputBytes = 1
	if _, err := c.Invoke(context.Background(), Principal{Capabilities: []string{"read"}, MaxDepth: 4}, "native/read", "1.0.0", []byte(`{}`), b); !errors.Is(err, ErrBudget) {
		t.Fatalf("input budget: %v", err)
	}
	if _, err := c.Invoke(context.Background(), Principal{Capabilities: []string{"read"}, MaxDepth: 4}, "native/read", "1.0.0", []byte(`{}`), budget()); err != nil {
		t.Fatal(err)
	}
}

func TestComposedToolCannotWidenChildAuthority(t *testing.T) {
	parent := Manifest{Version: 1, Compatibility: "agent-compatible", Capabilities: []string{"read"}, Effects: []string{"db:read"}}
	child := safeTool("child/write", Manifest{Version: 1, Compatibility: "agent-compatible", Capabilities: []string{"write"}, Effects: []string{"db:write"}})
	composed, err := Compose("workflow/read", "1.0.0", []Tool{child}, parent)
	if err != nil {
		t.Fatal(err)
	}
	if !contains(composed.Manifest.Effects, "db:write") || !contains(composed.Manifest.Effects, "db:read") {
		t.Fatalf("child effects not aggregated: %+v", composed.Manifest)
	}
	if !contains(composed.Manifest.Capabilities, "read") || contains(composed.Manifest.Capabilities, "write") {
		t.Fatalf("child capability widened: %+v", composed.Manifest)
	}
}

func TestModelLikeManifestAndSecretMetadataCannotEstablishTrust(t *testing.T) {
	c := NewCatalog()
	bad := safeTool("model/claimed", Manifest{Version: 1, Compatibility: "agent-compatible", SecretRefs: []string{"token"}, Deterministic: true})
	if err := c.Register(bad); !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("trusted model metadata accepted: %v", err)
	}
}
