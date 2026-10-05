package otel_test

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/trigger"
	blokhttp "github.com/well-prado/new-blok/trigger/http"
)

// TestInboundTracestateCredentialsNeverReachTheCollector (#285 review F1):
// a caller's tracestate becomes the exported spans' trace_state, so a
// credential in it would reach the collector. Only its clean members may.
func TestInboundTracestateCredentialsNeverReachTheCollector(t *testing.T) {
	const traceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	encoded := strings.TrimRight(base64.StdEncoding.EncodeToString([]byte("password=SYNTHETIC-b64-0003")), "=")
	h := newHarness(t, harnessOptions{ratio: 1})
	server, err := blokhttp.New(h.app, []blokhttp.Endpoint{{Method: "POST", Path: "/orders", Trace: trigger.TraceIngress{Extract: true, Sampling: observe.HonorInboundSampling},
		Handle: func(ctx context.Context, in blokhttp.Input) (any, error) {
			var order orderInput
			if err := json.Unmarshal(in.Body, &order); err != nil {
				return nil, err
			}
			result, err := h.runner.Run(ctx, h.program, order, inspection.Invocation{RunID: "run-inbound", Principal: principalSentinel, Tenant: "tenant-a"})
			return result.Output, err
		}}})
	if err != nil {
		t.Fatal(err)
	}
	listener := httptest.NewServer(server)
	defer listener.Close()
	request, err := http.NewRequest(http.MethodPost, listener.URL+"/orders", strings.NewReader(`{"sku":"coffee","quantity":2}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("traceparent", "00-"+traceID+"-00f067aa0ba902b7-01")
	request.Header.Set("tracestate", "token=SYNTHETIC-ts-0001,pw=password:hunter2,vendor=ok,enc="+encoded)
	response, err := listener.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("%d %s", response.StatusCode, body)
	}
	h.flush(t)
	for _, secret := range []string{"SYNTHETIC-ts-0001", "hunter2", encoded} {
		if h.collector.contains(secret) {
			t.Errorf("the collector received %q", secret)
		}
	}
	spans, _, _ := h.collector.snapshot()
	joined := 0
	for _, span := range spans {
		if hex.EncodeToString(span.TraceId) != traceID {
			continue
		}
		joined++
		if span.TraceState != "vendor=ok" {
			t.Errorf("span %s exported trace_state %q, want vendor=ok", span.Name, span.TraceState)
		}
	}
	if joined == 0 {
		t.Fatalf("no exported span joined the inbound trace (%d spans)", len(spans))
	}
}
