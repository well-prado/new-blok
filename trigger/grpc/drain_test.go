package grpc_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/well-prado/new-blok/internal/drainprobe"
	tgrpc "github.com/well-prado/new-blok/trigger/grpc"
	"github.com/well-prado/new-blok/trigger/grpc/internal/orderpb"
)

// TestDrainTimeoutCancelsACall: when the drain times out under a call, its
// handler is canceled and stops before the application closes its
// dependencies, and the caller is told the service is unavailable (#177).
func TestDrainTimeoutCancelsACall(t *testing.T) {
	f := loadCases(t)
	probe := &drainprobe.Probe{}
	application := probe.Start(t, 50*time.Millisecond)
	work := drainprobe.NewHeld(t, probe)
	adapter, err := tgrpc.New(application, principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
		Timeout: 30 * time.Second,
		Handle: func(ctx context.Context, _ tgrpc.Call) (json.RawMessage, error) {
			return nil, work.Run(ctx)
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	answered := make(chan error, 1)
	go func() {
		sku, quantity := "tea", int32(1)
		_, err := orderpb.NewOrdersClient(conn).Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &sku, Quantity: &quantity})
		answered <- err
	}()
	drainprobe.Abort(t, application, probe, work)
	if err := <-answered; status.Code(err) != codes.Unavailable || reason(err) != "unavailable" {
		t.Fatalf("call answered %v; want Unavailable unavailable", err)
	}
}
