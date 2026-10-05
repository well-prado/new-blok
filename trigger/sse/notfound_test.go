package sse_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/sse"
)

// TestNotFoundFailureCarriesOnlyTheCode pins how SSE expresses the not-found
// class (#306). The start is a durable submission answered before the
// workflow runs, so its status cannot carry the workflow's outcome; the
// outcome arrives as the final "failed" event, which carries only the code. A
// missing record and another principal's record publish the same event.
func TestNotFoundFailureCarriesOnlyTheCode(t *testing.T) {
	hidden := sse.Failure(fmt.Errorf("run: %w", &node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: errors.New("synthetic-secret-detail: belongs to alice")}))
	missing := sse.Failure(&node.DomainError{Code: "not_found", Class: trigger.ClassNotFound, Err: errors.New("synthetic-secret-detail: no row")})
	if hidden.Type != "failed" || string(hidden.Data) != `{"code":"not_found"}` || hidden.Type != missing.Type || !bytes.Equal(hidden.Data, missing.Data) {
		t.Fatalf("hidden=%s %s missing=%s %s; want both failed {\"code\":\"not_found\"}", hidden.Type, hidden.Data, missing.Type, missing.Data)
	}
}
