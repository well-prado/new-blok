package grpc_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
	tgrpc "github.com/well-prado/new-blok/trigger/grpc"
	"github.com/well-prado/new-blok/trigger/grpc/internal/orderpb"
)

// TestNotFoundIsIndistinguishable covers #306 over a real gRPC connection: a
// record that does not exist and one that belongs to another principal fail
// with the same domain error and different private causes. Both calls get
// NotFound, and the same status bytes (code, message, details), headers and
// trailers. NotFound is not a code gRPC retry policies or clients treat as
// transient. The class is the wire string, so this compiles on a tree without
// the mapping and fails there.
func TestNotFoundIsIndistinguishable(t *testing.T) {
	assertNotFoundIndistinguishable(t, func(_ context.Context, call tgrpc.Call) (json.RawMessage, error) {
		var order struct {
			SKU string `json:"sku"`
		}
		if err := json.Unmarshal(call.Input, &order); err != nil {
			return nil, err
		}
		if err := lookupOwned(order.SKU, call.Principal.ID); err != nil {
			return nil, err
		}
		return echo(context.Background(), call)
	})
}

// TestNotFoundThroughTheEngineIsIndistinguishable is the same acceptance with
// the failure produced by a real node in a workflow run by the engine, so the
// adapter sees the engine's classified error, not a hand-built DomainError.
func TestNotFoundThroughTheEngineIsIndistinguishable(t *testing.T) {
	type lookup struct {
		SKU       string `json:"sku"`
		Principal string `json:"principal"`
	}
	type owned struct {
		SKU string `json:"sku"`
	}
	definition, err := node.Define("orders/owned", "1.0.0", func(_ context.Context, input lookup) (owned, error) {
		if err := lookupOwned(input.SKU, input.Principal); err != nil {
			return owned{}, err
		}
		return owned{SKU: input.SKU}, nil
	}, node.Description("Reads an order its principal owns"), node.Pure(), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object","properties":{"sku":{"type":"string"}},"required":["sku"]}`)))
	if err != nil {
		t.Fatal(err)
	}
	workflow, err := flow.Define(flow.Spec{Name: "orders-owned", Version: "1.0.0"}, func(builder *flow.Builder, input flow.Ref[lookup]) flow.Ref[owned] {
		return flow.Call(builder, "owned", definition, input)
	})
	if err != nil {
		t.Fatal(err)
	}
	program, err := workflow.Lower()
	if err != nil {
		t.Fatal(err)
	}
	application := started(t)
	runner := execution.NewRunner(application, map[string]node.Any{"orders/owned": definition.Any()})
	assertNotFoundIndistinguishableOn(t, application, func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
		var order struct {
			SKU string `json:"sku"`
		}
		if err := json.Unmarshal(call.Input, &order); err != nil {
			return nil, err
		}
		if _, err := runner.Run(ctx, program, lookup{SKU: order.SKU, Principal: call.Principal.ID}, inspection.Invocation{}); err != nil {
			return nil, err
		}
		return echo(ctx, call)
	})
}

// lookupOwned is the synthetic table: "coffee" belongs to alice. A missing
// order and another principal's order fail with the same code and class and
// different private causes.
func lookupOwned(sku, principal string) error {
	owner, ok := map[string]string{"coffee": "alice"}[sku]
	if !ok {
		return &node.DomainError{Code: "not_found", Class: "not_found", Err: fmt.Errorf("synthetic-secret-detail: no row %q", sku)}
	}
	if owner != principal {
		return &node.DomainError{Code: "not_found", Class: "not_found", Err: fmt.Errorf("synthetic-secret-detail: %q belongs to %s", sku, owner)}
	}
	return nil
}

func assertNotFoundIndistinguishable(t *testing.T, handle func(context.Context, tgrpc.Call) (json.RawMessage, error)) {
	t.Helper()
	assertNotFoundIndistinguishableOn(t, started(t), handle)
}

func assertNotFoundIndistinguishableOn(t *testing.T, application *app.Application, handle func(context.Context, tgrpc.Call) (json.RawMessage, error)) {
	t.Helper()
	f := loadCases(t)
	adapter, err := tgrpc.New(application, principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: notGuest,
		Handle: handle,
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	type answer struct {
		status            []byte
		code              codes.Code
		header, trailer   metadata.MD
		message, reasonOf string
	}
	invoke := func(principal, sku string) answer {
		request, err := encodeOrder(json.RawMessage(`{"sku":"` + sku + `","quantity":2}`))
		if err != nil {
			t.Fatal(err)
		}
		var header, trailer metadata.MD
		err = conn.Invoke(as(context.Background(), principal), placeMethod, request, &orderpb.Placed{}, grpc.ForceCodec(rawCodec{}), grpc.Header(&header), grpc.Trailer(&trailer))
		st := status.Convert(err)
		data, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(st.Proto())
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return answer{status: data, code: st.Code(), header: volatile(header), trailer: volatile(trailer), message: st.Message(), reasonOf: reason(err)}
	}
	if own := invoke("alice", "coffee"); own.code != codes.OK {
		t.Fatalf("owner read: %v %q", own.code, own.message)
	}
	hidden, missing := invoke("bob", "coffee"), invoke("bob", "tea")
	if hidden.code != codes.NotFound || hidden.message != "not_found" || hidden.reasonOf != "not_found" {
		t.Fatalf("another principal's record: %v %q (reason %q); want NotFound not_found", hidden.code, hidden.message, hidden.reasonOf)
	}
	if !bytes.Equal(hidden.status, missing.status) || !reflect.DeepEqual(hidden.header, missing.header) || !reflect.DeepEqual(hidden.trailer, missing.trailer) {
		t.Fatalf("distinguishable:\nhidden  %+v\nmissing %+v", hidden, missing)
	}
	if bytes.Contains(hidden.status, []byte("synthetic")) || strings.Contains(fmt.Sprint(hidden.trailer), "synthetic") {
		t.Fatalf("private cause reached the client: %+v", hidden)
	}
	// Retry policies (gRFC A6) retry only the codes they list; the
	// conventional transient set is Unavailable, ResourceExhausted, Aborted
	// and DeadlineExceeded. NotFound is in none of them.
	for _, transient := range []codes.Code{codes.Unavailable, codes.ResourceExhausted, codes.Aborted, codes.DeadlineExceeded, codes.Internal} {
		if hidden.code == transient {
			t.Fatalf("not-found answered as %v", transient)
		}
	}
}

// volatile drops the transport's own per-response metadata, which is not
// chosen by the adapter.
func volatile(md metadata.MD) metadata.MD {
	out := metadata.MD{}
	for key, values := range md {
		if key == "date" {
			continue
		}
		out[key] = values
	}
	return out
}
