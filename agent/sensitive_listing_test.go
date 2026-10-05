package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/tool"
	"github.com/well-prado/new-blok/node"
)

// TestCatalogListingsAreOpaqueByDefault is ADR 0021's catalog rule: a tool
// reaches a secret only through opaque reference names, and a listing whose
// model-visible text carries an actual credential value (plainly or encoded)
// is refused at registration, before any principal can list it. Prose that
// only mentions a password, a bearer or a token limit is admitted.
func TestCatalogListingsAreOpaqueByDefault(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("api_key=SYNTHETIC-catalog-0001"))
	cases := []struct {
		name, description string
		schema            []byte
		metadata          tool.Metadata
		admitted          bool
	}{
		{name: "prose-mentions-password", description: "Changes the account password: the new password must differ from the old one", schema: valueSchema, metadata: metadata(), admitted: true},
		{name: "prose-mentions-token-limit", description: "Summarizes text; max_tokens: 256 by default", schema: []byte(`{"type":"object","properties":{"value":{"type":"integer"},"max_tokens":{"type":"integer","default":256},"mode":{"type":"string","default":"token: optional"}},"required":["value"]}`), metadata: metadata(), admitted: true},
		{name: "prose-mentions-bearer", description: "Sends the request as the bearer of the caller's session", schema: valueSchema, metadata: metadata(), admitted: true},
		{name: "provider-token-in-description", description: "uses ghp_SYNTHETICabcdefghijklmnop0123", schema: valueSchema, metadata: metadata()},
		{name: "secret-names-only", description: "charges a card via the payments provider", schema: []byte(`{"type":"object","properties":{"value":{"type":"integer"},"password":{"type":"string"}},"required":["value"]}`), metadata: metadata(), admitted: true},
		{name: "description-assignment", description: "call with api_key=SYNTHETIC-catalog-0002", schema: valueSchema, metadata: metadata()},
		{name: "schema-default-bearer", description: "reads", schema: []byte(`{"type":"object","properties":{"value":{"type":"integer"},"auth":{"type":"string","default":"Bearer SYNTHETIC-catalog-0003"}},"required":["value"]}`), metadata: metadata()},
		{name: "schema-default-encoded", description: "reads", schema: []byte(`{"type":"object","properties":{"value":{"type":"integer"},"mode":{"type":"string","default":"` + encoded + `"}},"required":["value"]}`), metadata: metadata()},
		{name: "description-encoded", description: "uses " + encoded, schema: valueSchema, metadata: metadata()},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			r := node.NewRegistry()
			calls := 0
			def := node.MustDefine("native/"+item.name, "1.0.0", func(context.Context, value) (value, error) { calls++; return value{}, nil },
				node.Description(item.description), node.Schemas(item.schema, valueSchema), node.Effects("db:read"))
			if err := r.Register(def.Any()); err != nil {
				t.Fatal(err)
			}
			c := NewCatalog(r, nil)
			m := manifest("read")
			m.SecretRefs = []string{"payments/api-key"}
			err := RegisterNode(c, def, m, tool.Resources{}, item.metadata)
			listed := c.List(principal("read"))
			if item.admitted {
				if err != nil || len(listed) != 1 {
					t.Fatalf("admitted=%v listed=%d", err, len(listed))
				}
				raw, _ := json.Marshal(listed)
				if strings.Contains(string(raw), "SYNTHETIC") || strings.Contains(string(raw), "payments/api-key") {
					t.Fatalf("listing exposes a secret or its reference: %s", raw)
				}
				return
			}
			if !errors.Is(err, ErrSensitiveListing) || len(listed) != 0 || calls != 0 {
				t.Fatalf("err=%v listed=%d calls=%d; want ErrSensitiveListing, nothing listed, no dispatch", err, len(listed), calls)
			}
		})
	}
}
