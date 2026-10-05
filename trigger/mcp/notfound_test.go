package mcp_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/trigger"
	tmcp "github.com/well-prado/new-blok/trigger/mcp"
)

// TestNotFoundToolErrorCarriesOnlyTheCode pins how MCP expresses the
// not-found class (#306). MCP reports a tool's own failure as a result with
// isError, not as a JSON-RPC error (those are for protocol failures, and the
// spec's -32002 "resource not found" is for resources/read, not tools/call).
// So a not-found tool call is an isError result carrying {"code":<code>}, and
// a missing record and another principal's record get the same result.
func TestNotFoundToolErrorCarriesOnlyTheCode(t *testing.T) {
	owners := map[string]string{"r-alice": "alice"}
	r := newRig(t, func(c *tmcp.Config) {
		c.Expose = []string{"demo/record@1.0.0"}
		c.Catalog.(*fakeCatalog).tools["demo/record@1.0.0"] = fakeTool{
			tool:       tmcp.Tool{Name: "demo/record", Version: "1.0.0", Description: "Synthetic record read", InputSchema: objectSchema(`"id":{"type":"string"}`, "id")},
			capability: "echo",
			run: func(_ context.Context, call tmcp.Call) (json.RawMessage, error) {
				var in struct{ ID string }
				if err := json.Unmarshal(call.Input, &in); err != nil {
					return nil, err
				}
				if _, ok := owners[in.ID]; !ok {
					return nil, &node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: fmt.Errorf("%s: no row %q", secretMarker, in.ID)}
				}
				// The fake catalog has no record owners of its own: every
				// existing record is alice's, and bob is the caller.
				return nil, &node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: errors.New(secretMarker + ": belongs to alice")}
			},
		}
	})
	session := r.connect("bob")
	answer := func(id string) string {
		result, err := call(session, "demo.record_v1.0.0", map[string]string{"id": id}, nil)
		if err != nil {
			t.Fatalf("%s: %v", id, err)
		}
		if !result.IsError || toolCode(result) != "not_found" {
			t.Fatalf("%s: result=%+v; want an isError result with code not_found", id, result)
		}
		data, err := json.Marshal(result)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	hidden, missing := answer("r-alice"), answer("r-missing")
	if hidden != missing {
		t.Fatalf("distinguishable:\nhidden  %s\nmissing %s", hidden, missing)
	}
	if strings.Contains(r.wire.String(), secretMarker) || strings.Contains(r.wire.String(), "belongs") {
		t.Fatal("a private cause reached the wire")
	}
}
