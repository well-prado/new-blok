package worker

import (
	"bytes"
	"context"
	"encoding/base64"
	"log/slog"
	"strings"
	"testing"

	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/redact"
)

// TestWorkerLogsUseTheSharedRedactionBoundary: a worker log frame reaches the
// node logger only through observe/redact (ADR 0021): a sensitive key, a
// credential-shaped attribute value and an encoded credential in the message
// are all redacted before any application handler sees them.
func TestWorkerLogsUseTheSharedRedactionBoundary(t *testing.T) {
	var out bytes.Buffer
	ctx := node.WithLogger(context.Background(), slog.New(slog.NewJSONHandler(&out, nil)))
	encoded := base64.StdEncoding.EncodeToString([]byte("password=SYNTHETIC-worker-0001"))
	emitWorkerLog(ctx, contract.Log{Level: "INFO", Message: "forwarding " + encoded, Attrs: []byte(`{"client.secret":"SYNTHETIC-worker-0002","note":"Bearer SYNTHETIC-worker-0003","sku":"coffee"}`)})
	emitWorkerLog(ctx, contract.Log{Level: "INFO", Message: "quote calculated", Attrs: []byte(`{"sku":"tea"}`)})
	logged := out.String()
	if strings.Contains(logged, "SYNTHETIC") || strings.Contains(logged, encoded) {
		t.Fatalf("worker log leaked: %s", logged)
	}
	for _, want := range []string{redact.MessageMarker, `"client.secret":"` + redact.Marker + `"`, `"note":"` + redact.Marker + `"`, `"sku":"coffee"`, "quote calculated", `"sku":"tea"`} {
		if !strings.Contains(logged, want) {
			t.Fatalf("missing %q in %s", want, logged)
		}
	}
}
