package grpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/conformance"
	"github.com/well-prado/new-blok/trigger"
	tgrpc "github.com/well-prado/new-blok/trigger/grpc"
	"github.com/well-prado/new-blok/trigger/grpc/internal/orderpb"
)

const placeMethod = "/newblok.test.orders.v1.Orders/Place"

var placeDescriptor = orderpb.File_orders_proto.Services().ByName("Orders").Methods().ByName("Place")

// bearer authenticates the "authorization: Bearer <credential>" metadata.
func bearer(authenticate func(string) (trigger.Principal, error)) tgrpc.Authenticator {
	return func(ctx context.Context) (trigger.Principal, error) {
		values := metadata.ValueFromIncomingContext(ctx, "authorization")
		if len(values) != 1 {
			return trigger.Principal{}, errors.New("no credential")
		}
		credential, ok := strings.CutPrefix(values[0], "Bearer ")
		if !ok || credential == "" {
			return trigger.Principal{}, errors.New("no credential")
		}
		return authenticate(credential)
	}
}

// rawCodec sends a request's exact bytes, so a client can put on the wire
// what a generated message cannot hold (a wrong-typed field), and decodes
// responses as protobuf.
type rawCodec struct{}

type rawRequest []byte

func (rawCodec) Name() string { return "proto" }
func (rawCodec) Marshal(v any) ([]byte, error) {
	if raw, ok := v.(rawRequest); ok {
		return raw, nil
	}
	return proto.Marshal(v.(proto.Message))
}
func (rawCodec) Unmarshal(data []byte, v any) error { return proto.Unmarshal(data, v.(proto.Message)) }

// encodeOrder encodes a delivered JSON payload as an Order. A value the
// field cannot hold, or a field the Order does not have (number 99), is sent
// anyway as a length-delimited field, the way a client with a different
// proto would send it; every other field keeps its own wire type.
func encodeOrder(payload json.RawMessage) (rawRequest, error) {
	if len(payload) == 0 {
		return rawRequest{}, nil
	}
	var order orderpb.Order
	if err := protojson.Unmarshal(payload, &order); err == nil {
		data, err := proto.Marshal(&order)
		return rawRequest(data), err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		return nil, err
	}
	numbers := map[string]protowire.Number{"sku": 1, "quantity": 2, "principal": 3}
	var data []byte
	for name, value := range fields {
		number, ok := numbers[name]
		if !ok {
			number = 99
		}
		var integer int64
		if name == "quantity" && json.Unmarshal(value, &integer) == nil {
			data = protowire.AppendTag(data, number, protowire.VarintType)
			data = protowire.AppendVarint(data, uint64(integer))
			continue
		}
		var text string
		if json.Unmarshal(value, &text) != nil {
			text = string(value)
		}
		data = protowire.AppendTag(data, number, protowire.BytesType)
		data = protowire.AppendString(data, text)
	}
	return rawRequest(data), nil
}

// grpcDriver serves the real adapter on a real listener and calls it with
// a real gRPC client. The driver never calls the workflow itself.
type grpcDriver struct {
	env      conformance.TriggerEnv
	app      *app.Application
	adapter  *tgrpc.Server
	server   *grpc.Server
	conn     *grpc.ClientConn
	mu       sync.Mutex
	address  string
	serveErr chan error
}

func (*grpcDriver) Declaration() trigger.Declaration { return tgrpc.Declaration }

func (d *grpcDriver) Open(_ context.Context, env conformance.TriggerEnv) error {
	d.env = env
	application, err := app.New(app.Config{})
	if err != nil {
		return err
	}
	output := `{"type":"object","properties":{"echo":` + string(env.InputSchema) + `}}`
	adapter, err := tgrpc.New(application, bearer(env.Authenticate), []tgrpc.Binding{{
		Method: placeDescriptor, Workflow: "place", WorkflowInput: env.InputSchema,
		InputSchema: env.InputSchema, OutputSchema: []byte(output), Authorize: tgrpc.AllowAuthenticated,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			return env.Workflow(ctx, conformance.Call{Input: call.Input, Principal: call.Principal})
		},
	}})
	if err != nil {
		return err
	}
	d.app, d.adapter = application, adapter
	return nil
}

func (d *grpcDriver) Start(ctx context.Context) error {
	if err := d.app.Start(ctx); err != nil {
		return err
	}
	server, err := d.adapter.NewServer()
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	d.mu.Lock()
	d.server, d.conn, d.address = server, conn, listener.Addr().String()
	d.serveErr = make(chan error, 1)
	d.mu.Unlock()
	go func() { d.serveErr <- server.Serve(listener) }()
	return nil
}

func (d *grpcDriver) Endpoint() string { d.mu.Lock(); defer d.mu.Unlock(); return d.address }

// reason reads the stable code a status carries.
func reason(err error) string {
	st := status.Convert(err)
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.Reason
		}
	}
	return ""
}

func (d *grpcDriver) Deliver(ctx context.Context, delivery conformance.Delivery) (conformance.Outcome, error) {
	if d.Endpoint() == "" {
		return conformance.Outcome{Kind: conformance.Rejected, Code: "unavailable"}, nil
	}
	request, err := encodeOrder(delivery.Payload)
	if err != nil {
		return conformance.Outcome{}, err
	}
	callCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if delivery.Credential != "" {
		callCtx = metadata.AppendToOutgoingContext(callCtx, "authorization", "Bearer "+delivery.Credential)
	}
	if delivery.Disconnect != nil {
		go func() {
			select {
			case <-delivery.Disconnect:
				cancel()
			case <-callCtx.Done():
			}
		}()
	}
	var placed orderpb.Placed
	err = d.conn.Invoke(callCtx, placeMethod, request, &placed, grpc.ForceCodec(rawCodec{}))
	if err == nil {
		output, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(&placed)
		if err != nil {
			return conformance.Outcome{}, err
		}
		return conformance.Outcome{Kind: conformance.Completed, Output: output}, nil
	}
	st := status.Convert(err)
	if st.Code() == codes.Canceled && delivery.Disconnect != nil {
		return conformance.Outcome{Kind: conformance.Disconnected}, nil
	}
	if reason(err) == "" {
		return conformance.Outcome{}, err
	}
	return conformance.Outcome{Kind: conformance.Rejected, Code: reason(err), Message: st.Message()}, nil
}

func (d *grpcDriver) Stop(ctx context.Context) error {
	d.mu.Lock()
	server, conn := d.server, d.conn
	d.address = ""
	d.mu.Unlock()
	shutdownErr := d.app.Shutdown(ctx)
	server.GracefulStop()
	closeErr := conn.Close()
	if err := <-d.serveErr; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		return errors.Join(shutdownErr, closeErr, err)
	}
	return errors.Join(shutdownErr, closeErr)
}

func TestGRPCAdapterPassesTriggerConformance(t *testing.T) {
	corpus, err := conformance.LoadTriggerCorpus()
	if err != nil {
		t.Fatal(err)
	}
	report, err := conformance.RunTrigger(context.Background(), &grpcDriver{}, corpus, conformance.TriggerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ran := 0
	for _, result := range report.Results {
		if result.Applicable {
			ran++
		} else {
			t.Logf("not applicable: %s (%s)", result.CaseID, result.Reason)
		}
	}
	if ran != 12 {
		t.Fatalf("ran=%d report=%+v", ran, report)
	}
}
