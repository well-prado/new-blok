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
	f := loadCases(t)
	owners := map[string]string{"coffee": "alice"}
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: notGuest,
		Handle: func(_ context.Context, call tgrpc.Call) (json.RawMessage, error) {
			var order struct {
				SKU string `json:"sku"`
			}
			if err := json.Unmarshal(call.Input, &order); err != nil {
				return nil, err
			}
			owner, ok := owners[order.SKU]
			if !ok {
				return nil, &node.DomainError{Code: "not_found", Class: "not_found", Err: fmt.Errorf("synthetic-secret-detail: no row %q", order.SKU)}
			}
			if owner != call.Principal.ID {
				return nil, &node.DomainError{Code: "not_found", Class: "not_found", Err: fmt.Errorf("synthetic-secret-detail: %q belongs to %s", order.SKU, owner)}
			}
			return echo(context.Background(), call)
		},
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
