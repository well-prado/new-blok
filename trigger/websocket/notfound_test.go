package websocket_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/trigger"
	blokws "github.com/well-prado/new-blok/trigger/websocket"
)

// TestNotFoundReplyCarriesOnlyTheCode pins how WebSocket expresses the
// not-found class (#306). The reply protocol has no status, so a not-found
// message is answered like any classified failure: {"id","error":<code>}. A
// missing record and another principal's record get byte-identical frames.
// An OnConnect that answers not-found closes the socket 1008 with the code,
// as for any classified refusal: the protocol has no not-found close code
// and the adapter does not invent one in the private 4000-4999 range.
func TestNotFoundReplyCarriesOnlyTheCode(t *testing.T) {
	f := loadFixture(t)
	owners := map[string]string{"coffee": "user-alice"}
	e := newEnv(t, f, func(endpoint *blokws.Endpoint) {
		endpoint.OnMessage = func(_ context.Context, m blokws.Message) (json.RawMessage, error) {
			var input struct {
				SKU string `json:"sku"`
			}
			_ = json.Unmarshal(m.Input, &input)
			owner, ok := owners[input.SKU]
			switch {
			case !ok:
				return nil, &node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: fmt.Errorf("synthetic-secret-detail: no row %q", input.SKU)}
			case owner != m.Connection.Principal.ID:
				return nil, &node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: fmt.Errorf("synthetic-secret-detail: %q belongs to %s", input.SKU, owner)}
			}
			return json.RawMessage(`{"owner":"` + owner + `"}`), nil
		}
		endpoint.OnConnect = func(_ context.Context, c blokws.Connection) error {
			if c.Principal.ID == "user-gone" {
				return &node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: errors.New("synthetic-secret-detail: no such room")}
			}
			return nil
		}
	})
	conn, _, err := e.dial(t, "user-bob", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	raw := func(frame string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			t.Fatal(err)
		}
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	hidden := raw(`{"id":"r1","input":{"sku":"coffee","quantity":1}}`)
	missing := raw(`{"id":"r1","input":{"sku":"tea","quantity":1}}`)
	if hidden != `{"id":"r1","error":"not_found"}` || hidden != missing {
		t.Fatalf("hidden=%s missing=%s; want both {\"id\":\"r1\",\"error\":\"not_found\"}", hidden, missing)
	}
	gone, _, err := e.dial(t, "user-gone", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer gone.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = gone.Read(ctx)
	var closeErr websocket.CloseError
	if !errors.As(err, &closeErr) || closeErr.Code != websocket.StatusPolicyViolation || closeErr.Reason != "not_found" || strings.Contains(closeErr.Reason, "synthetic") {
		t.Fatalf("not-found connect closed with %v, want 1008 not_found", err)
	}
}
