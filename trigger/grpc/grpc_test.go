package grpc_test

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/trigger"
	tgrpc "github.com/well-prado/new-blok/trigger/grpc"
	"github.com/well-prado/new-blok/trigger/grpc/internal/orderpb"
)

var orders = orderpb.File_orders_proto.Services().ByName("Orders")

// principals: a bearer token is the principal id; "guest" may call only
// methods its authorization admits.
func principals(ctx context.Context) (trigger.Principal, error) {
	return bearer(func(credential string) (trigger.Principal, error) {
		if credential == "forged" {
			return trigger.Principal{}, errors.New("forged")
		}
		return trigger.Principal{ID: credential}, nil
	})(ctx)
}

func notGuest(principal trigger.Principal, _ string) error {
	if principal.ID == "guest" {
		return errors.New("forbidden")
	}
	return nil
}

type casesFixture struct {
	Order   json.RawMessage `json:"order"`
	Placed  json.RawMessage `json:"placed"`
	Startup []struct {
		Name     string          `json:"name"`
		Method   string          `json:"method"`
		Input    json.RawMessage `json:"input"`
		Output   json.RawMessage `json:"output"`
		Workflow json.RawMessage `json:"workflow"`
		Expect   string          `json:"expect"`
	} `json:"startup"`
	Calls []struct {
		Name       string          `json:"name"`
		Credential string          `json:"credential"`
		Request    json.RawMessage `json:"request"`
		Raw        string          `json:"raw"`
		Behavior   string          `json:"behavior"`
		Code       string          `json:"code"`
		Reason     string          `json:"reason"`
	} `json:"calls"`
	Expected struct {
		StartupAccepted int `json:"startupAccepted"`
		StartupRefused  int `json:"startupRefused"`
		CallsOK         int `json:"callsOK"`
		CallsRefused    int `json:"callsRefused"`
		Dispatches      int `json:"dispatches"`
		Effects         int `json:"effects"`
	} `json:"expected"`
}

func loadCases(t *testing.T) casesFixture {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", "grpc", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f casesFixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

// canonical spells a status code the way the gRPC specification does
// (PERMISSION_DENIED), as the fixture does.
func canonical(code codes.Code) string {
	var out strings.Builder
	for i, r := range code.String() {
		if i > 0 && r >= 'A' && r <= 'Z' && code != codes.OK {
			out.WriteByte('_')
		}
		out.WriteRune(r)
	}
	return strings.ToUpper(out.String())
}

func started(t *testing.T) *app.Application {
	t.Helper()
	application, err := app.New(app.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return application
}

func echo(_ context.Context, call tgrpc.Call) (json.RawMessage, error) {
	return json.RawMessage(`{"echo":` + string(call.Input) + `}`), nil
}

// serve runs an adapter on its own loopback listener and returns a client
// connection to it.
func serve(t *testing.T, adapter *tgrpc.Server, options ...grpc.ServerOption) (*grpc.Server, *grpc.ClientConn) {
	t.Helper()
	server, err := adapter.NewServer(options...)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close(); server.Stop() })
	return server, conn
}

func as(ctx context.Context, credential string) context.Context {
	if credential == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+credential)
}

// TestFixture runs the predeclared startup and call cases: which mappings
// are proven or refused (and with which code), and how each call is
// answered, with the counts of dispatches and effects.
func TestFixture(t *testing.T) {
	f := loadCases(t)
	accepted, refused := 0, 0
	for _, tc := range f.Startup {
		var method protoreflect.MethodDescriptor = orders.Methods().ByName(protoreflect.Name(tc.Method))
		if tc.Method == "worker" {
			method = wire.File_runtime_proto.Services().ByName("Worker").Methods().ByName("Invoke")
		}
		workflow := tc.Workflow
		if workflow == nil {
			workflow = tc.Input
		}
		_, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{Method: method, Workflow: "w", WorkflowInput: workflow, InputSchema: tc.Input, OutputSchema: tc.Output, Handle: echo, Authorize: tgrpc.AllowAuthenticated}})
		var mapping *tgrpc.MappingError
		switch {
		case tc.Expect == "" && err == nil:
			accepted++
		case tc.Expect != "" && errors.As(err, &mapping) && mapping.Code == tc.Expect:
			refused++
		default:
			t.Fatalf("%s: %v, want %q", tc.Name, err, tc.Expect)
		}
	}
	var dispatches, effects atomic.Int64
	var behavior atomic.Value
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: notGuest,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			dispatches.Add(1)
			switch b := behavior.Load().(string); {
			case b == "echo":
				effects.Add(1)
				return echo(ctx, call)
			case strings.HasPrefix(b, "domain:"):
				parts := strings.Split(b, ":")
				return nil, &node.DomainError{Code: parts[1], Class: parts[2], Err: errors.New("synthetic-secret-detail")}
			case b == "internal":
				return nil, errors.New("synthetic-secret-detail: dsn=postgres://synthetic")
			case b == "saturated":
				return nil, trigger.ErrSaturated
			case strings.HasPrefix(b, "output:"):
				return json.RawMessage(strings.TrimPrefix(b, "output:")), nil
			}
			return nil, errors.New("unknown behavior")
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	ok, failed := 0, 0
	for _, tc := range f.Calls {
		behavior.Store(tc.Behavior)
		var request rawRequest
		if tc.Raw != "" {
			request, _ = hex.DecodeString(tc.Raw)
		} else if request, err = encodeOrder(tc.Request); err != nil {
			t.Fatal(err)
		}
		var placed orderpb.Placed
		err := conn.Invoke(as(context.Background(), tc.Credential), placeMethod, request, &placed, grpc.ForceCodec(rawCodec{}))
		st := status.Convert(err)
		if canonical(st.Code()) != tc.Code || reason(err) != tc.Reason {
			t.Fatalf("%s: %v (reason %q), want %s %q", tc.Name, err, reason(err), tc.Code, tc.Reason)
		}
		if strings.Contains(st.Message(), "synthetic") || (err != nil && st.Message() != tc.Reason) {
			t.Fatalf("%s: status message %q is not the stable code", tc.Name, st.Message())
		}
		if err == nil {
			ok++
		} else {
			failed++
		}
	}
	e := f.Expected
	if accepted != e.StartupAccepted || refused != e.StartupRefused || ok != e.CallsOK || failed != e.CallsRefused || int(dispatches.Load()) != e.Dispatches || int(effects.Load()) != e.Effects {
		t.Fatalf("startup %d/%d, calls %d/%d, dispatches %d, effects %d; want %+v", accepted, refused, ok, failed, dispatches.Load(), effects.Load(), e)
	}
}

// TestGeneratedClientRoundTripsEveryKind calls through the generated client
// stub: every supported field kind reaches the workflow in its domain form
// and comes back intact.
func TestGeneratedClientRoundTripsEveryKind(t *testing.T) {
	f := loadCases(t)
	var kinds json.RawMessage
	for _, tc := range f.Startup {
		if tc.Method == "Inspect" && tc.Expect == "" {
			kinds = tc.Input
		}
	}
	var seen json.RawMessage
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Inspect"), Workflow: "inspect", WorkflowInput: kinds, InputSchema: kinds, OutputSchema: kinds, Authorize: tgrpc.AllowAuthenticated,
		Handle: func(_ context.Context, call tgrpc.Call) (json.RawMessage, error) {
			seen = call.Input
			return call.Input, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	sku, quantity := "tea", int32(3)
	sent := &orderpb.Kinds{
		Name: "n", Active: true, Small: -7, Large: 9007199254740993, Count: 4000000000, Ratio: 0.25,
		Blob: []byte{0, 1, 2, 255}, Color: orderpb.Color_COLOR_BLUE, Order: &orderpb.Order{Sku: &sku, Quantity: &quantity},
		Tags: []string{"a", "b"}, Lines: []*orderpb.Order{{Sku: &sku, Quantity: &quantity}}, Counts: []int32{-1, 0, 2147483647},
	}
	got, err := orderpb.NewOrdersClient(conn).Inspect(as(context.Background(), "alice"), sent)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(sent, got) {
		t.Fatalf("round trip changed the message:\n sent %v\n got  %v", sent, got)
	}
	var domain map[string]any
	if err := json.Unmarshal(seen, &domain); err != nil {
		t.Fatal(err)
	}
	if domain["large"] != "9007199254740993" || domain["color"] != "COLOR_BLUE" || domain["blob"] != "AAEC/w==" || domain["note"] != nil || domain["count"] != float64(4000000000) {
		t.Fatalf("domain input %s", seen)
	}
}

// TestDeadlinesAndCancellation: the shorter of the client's deadline and
// the binding's timeout bounds a call, a client that cancels cancels the
// workflow, and a workflow that ignores its context and returns late still
// fails the call. Each case is told apart by what the workflow observed and
// by the status the server itself answered.
func TestDeadlinesAndCancellation(t *testing.T) {
	f := loadCases(t)
	sku, quantity := "tea", int32(1)
	order := &orderpb.Order{Sku: &sku, Quantity: &quantity}
	for _, tc := range []struct {
		name     string
		binding  time.Duration
		client   time.Duration
		cancel   time.Duration
		ignore   bool
		lag      time.Duration
		code     codes.Code
		observed error
	}{
		{"client deadline shorter", 5 * time.Second, 50 * time.Millisecond, 0, false, 0, codes.DeadlineExceeded, nil},
		{"binding timeout shorter", 100 * time.Millisecond, 5 * time.Second, 0, false, 0, codes.DeadlineExceeded, context.DeadlineExceeded},
		{"client cancels", 5 * time.Second, 5 * time.Second, 50 * time.Millisecond, false, 0, codes.Canceled, context.Canceled},
		{"workflow ignores its deadline", 100 * time.Millisecond, 5 * time.Second, 0, true, 0, codes.DeadlineExceeded, nil},
		{"client cancels before its deadline, workflow returns late", 5 * time.Second, 300 * time.Millisecond, 50 * time.Millisecond, false, 400 * time.Millisecond, codes.Canceled, context.Canceled},
	} {
		observed := make(chan error, 1)
		adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
			Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
			Timeout: tc.binding,
			Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
				if tc.ignore {
					time.Sleep(300 * time.Millisecond)
					observed <- nil
					return echo(ctx, call)
				}
				<-ctx.Done()
				observed <- ctx.Err()
				time.Sleep(tc.lag)
				return nil, ctx.Err()
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		answered := make(chan codes.Code, 1)
		_, conn := serve(t, adapter, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			response, err := handler(ctx, req)
			answered <- status.Code(err)
			return response, err
		}))
		ctx, cancel := context.WithTimeout(as(context.Background(), "alice"), tc.client)
		if tc.cancel > 0 {
			time.AfterFunc(tc.cancel, cancel)
		}
		_, err = orderpb.NewOrdersClient(conn).Place(ctx, order)
		cancel()
		if tc.code != codes.Canceled && status.Code(err) != tc.code {
			t.Fatalf("%s: the client got %v", tc.name, err)
		}
		select {
		case seen := <-observed:
			if tc.observed != nil && !errors.Is(seen, tc.observed) {
				t.Fatalf("%s: the workflow observed %v, want %v", tc.name, seen, tc.observed)
			}
			if !tc.ignore && seen == nil {
				t.Fatalf("%s: the workflow was not canceled", tc.name)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: the workflow kept running", tc.name)
		}
		server := <-answered
		// The client's reset at its own deadline can reach the server
		// before the server's slightly later deadline; it then reads as a
		// cancel, indistinguishable on the wire (#230, statusFor).
		if tc.name == "client deadline shorter" && server == codes.Canceled {
			continue
		}
		if server != tc.code {
			t.Fatalf("%s: the server answered %v, want %v", tc.name, server, tc.code)
		}
	}
}

// TestCapacityAndSizeBounds: calls beyond the binding's concurrency are
// refused rather than queued, a request larger than the binding allows is
// refused, a response the binding may not send is not sent, and a draining
// application refuses new calls.
func TestCapacityAndSizeBounds(t *testing.T) {
	f := loadCases(t)
	release := make(chan struct{})
	running := make(chan struct{}, 4)
	application := started(t)
	adapter, err := tgrpc.New(application, principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
		MaxConcurrency: 2, MaxRequestBytes: 64, MaxResponseBytes: 64,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			if strings.Contains(string(call.Input), "slow") {
				running <- struct{}{}
				<-release
			}
			if strings.Contains(string(call.Input), "big") {
				return json.RawMessage(`{"echo":{"sku":"` + strings.Repeat("x", 100) + `","quantity":1}}`), nil
			}
			return echo(ctx, call)
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	// As on a shared server, the transport admits larger messages than this
	// binding does, so the binding's own bounds decide.
	_, conn := serve(t, adapter, grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20))
	client := orderpb.NewOrdersClient(conn)
	place := func(sku string) error {
		quantity := int32(1)
		_, err := client.Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &sku, Quantity: &quantity})
		return err
	}
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _ = place("slow") }()
		<-running
	}
	if err := place("tea"); status.Code(err) != codes.ResourceExhausted || reason(err) != "saturated" {
		t.Fatalf("a third concurrent call: %v", err)
	}
	close(release)
	wg.Wait()
	if err := place("tea"); err != nil {
		t.Fatalf("after the slots freed: %v", err)
	}
	if err := place(strings.Repeat("x", 100)); status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" {
		t.Fatalf("an oversized request: %v", err)
	}
	if err := place("big"); status.Code(err) != codes.ResourceExhausted || reason(err) != "response_too_large" {
		t.Fatalf("an oversized response: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := application.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := place("tea"); status.Code(err) != codes.Unavailable || reason(err) != "unavailable" {
		t.Fatalf("while draining: %v", err)
	}
}

// TestServerMessageLimit: the independent server refuses a message larger
// than its bound before the adapter sees it.
func TestServerMessageLimit(t *testing.T) {
	f := loadCases(t)
	var calls atomic.Int64
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
		MaxRequestBytes: 1024, MaxResponseBytes: 1024,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			calls.Add(1)
			return echo(ctx, call)
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	sku, quantity := strings.Repeat("x", 4096), int32(1)
	_, err = orderpb.NewOrdersClient(conn).Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &sku, Quantity: &quantity})
	// The transport refuses it: no adapter reason, no call.
	if status.Code(err) != codes.ResourceExhausted || reason(err) != "" || calls.Load() != 0 {
		t.Fatalf("a message over the server bound: %v (reason %q), %d calls", err, reason(err), calls.Load())
	}
}

// TestShutdown: GracefulStop lets an in-flight call finish; Stop cancels it.
func TestShutdown(t *testing.T) {
	f := loadCases(t)
	for _, graceful := range []bool{true, false} {
		entered := make(chan struct{})
		finished := make(chan error, 1)
		adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
			Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
			Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
				close(entered)
				select {
				case <-time.After(200 * time.Millisecond):
					finished <- nil
					return echo(ctx, call)
				case <-ctx.Done():
					finished <- ctx.Err()
					return nil, ctx.Err()
				}
			},
		}})
		if err != nil {
			t.Fatal(err)
		}
		server, conn := serve(t, adapter)
		sku, quantity := "tea", int32(1)
		done := make(chan error, 1)
		go func() {
			_, err := orderpb.NewOrdersClient(conn).Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &sku, Quantity: &quantity})
			done <- err
		}()
		<-entered
		if graceful {
			server.GracefulStop()
		} else {
			server.Stop()
		}
		workflow, call := <-finished, <-done
		if graceful && (workflow != nil || call != nil) {
			t.Fatalf("graceful stop interrupted the call: workflow %v, call %v", workflow, call)
		}
		if !graceful && (workflow == nil || call == nil) {
			t.Fatalf("stop did not cancel the call: workflow %v, call %v", workflow, call)
		}
	}
}

// certificate makes a self-signed certificate for 127.0.0.1.
func certificate(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pool
}

// workerStub is the worker runtime service a shared server also carries.
type workerStub struct{ wire.UnimplementedWorkerServer }

// TestListeners: the bindings serve on an independent TLS listener, or on
// a TLS listener shared with the worker runtime service, where both answer,
// the application's interceptors apply, and a service name already present
// is refused instead of colliding.
func TestListeners(t *testing.T) {
	f := loadCases(t)
	cert, pool := certificate(t)
	serverTLS := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13})
	clientTLS := credentials.NewTLS(&tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS13})
	newAdapter := func() *tgrpc.Server {
		adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Handle: echo, Authorize: tgrpc.AllowAuthenticated}})
		if err != nil {
			t.Fatal(err)
		}
		return adapter
	}
	dial := func(server *grpc.Server) *grpc.ClientConn {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		go func() { _ = server.Serve(listener) }()
		conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(clientTLS))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close(); server.Stop() })
		return conn
	}
	sku, quantity := "tea", int32(1)
	order := &orderpb.Order{Sku: &sku, Quantity: &quantity}

	independent, err := newAdapter().NewServer(grpc.Creds(serverTLS))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := orderpb.NewOrdersClient(dial(independent)).Place(as(context.Background(), "alice"), order); err != nil {
		t.Fatalf("independent TLS listener: %v", err)
	}

	var intercepted atomic.Int64
	shared := grpc.NewServer(grpc.Creds(serverTLS), grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == placeMethod {
			intercepted.Add(1)
		}
		return handler(ctx, req)
	}))
	wire.RegisterWorkerServer(shared, workerStub{})
	adapter := newAdapter()
	if err := adapter.Register(shared); err != nil {
		t.Fatal(err)
	}
	if err := newAdapter().Register(shared); !errors.Is(err, tgrpc.ErrServiceConflict) {
		t.Fatalf("a second registration of the same service: %v", err)
	}
	conn := dial(shared)
	if _, err := orderpb.NewOrdersClient(conn).Place(as(context.Background(), "alice"), order); err != nil || intercepted.Load() != 1 {
		t.Fatalf("application service on the shared listener: %v, intercepted %d", err, intercepted.Load())
	}
	if _, err := wire.NewWorkerClient(conn).Invoke(context.Background(), &wire.Call{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("worker service on the shared listener: %v", err)
	}
	insecureConn, err := grpc.NewClient(strings.TrimPrefix(conn.Target(), "passthrough:///"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer insecureConn.Close()
	if _, err := orderpb.NewOrdersClient(insecureConn).Place(as(context.Background(), "alice"), order); status.Code(err) != codes.Unavailable {
		t.Fatalf("a plaintext client on the TLS listener: %v", err)
	}
	if fmt.Sprint(adapter.ServiceNames()) != "[newblok.test.orders.v1.Orders]" {
		t.Fatalf("services %v", adapter.ServiceNames())
	}
}

// TestCallsDoNotLeak: many calls, including refused and cancelled ones,
// leave no goroutine behind once the server stops.
func TestCallsDoNotLeak(t *testing.T) {
	f := loadCases(t)
	before := runtime.NumGoroutine()
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: notGuest,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			if strings.Contains(string(call.Input), "slow") {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			return echo(ctx, call)
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	server, err := adapter.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	client := orderpb.NewOrdersClient(conn)
	quantity := int32(1)
	for i := 0; i < 300; i++ {
		sku, credential := "tea", "alice"
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		switch i % 4 {
		case 1:
			credential = "guest"
		case 2:
			sku = "slow"
			ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
		case 3:
			quantity = 1000
		}
		_, _ = client.Place(as(ctx, credential), &orderpb.Order{Sku: &sku, Quantity: &quantity})
		cancel()
		quantity = 1
	}
	conn.Close()
	server.GracefulStop()
	deadline := time.Now().Add(5 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Fatalf("goroutines %d before, %d after 300 calls", before, after)
	}
}

// TestDuplicateMethodRefused: one method cannot be bound twice.
func TestDuplicateMethodRefused(t *testing.T) {
	f := loadCases(t)
	binding := tgrpc.Binding{Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Handle: echo, Authorize: tgrpc.AllowAuthenticated}
	_, err := tgrpc.New(started(t), principals, []tgrpc.Binding{binding, binding})
	var mapping *tgrpc.MappingError
	if !errors.As(err, &mapping) || mapping.Code != "duplicate_method" {
		t.Fatalf("a method bound twice: %v", err)
	}
}

// TestDecodingIsBoundedBeforeAllocation: a request of many tiny nested
// messages is refused from its wire bytes, before it is decoded into a
// large structure; a request of repeated fields is bounded by its wire size,
// not by its decoded size.
func TestDecodingIsBoundedBeforeAllocation(t *testing.T) {
	f := loadCases(t)
	var kinds json.RawMessage
	for _, tc := range f.Startup {
		if tc.Method == "Inspect" && tc.Expect == "" {
			kinds = tc.Input
		}
	}
	var calls atomic.Int64
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{
		{Method: orders.Methods().ByName("Inspect"), Workflow: "inspect", WorkflowInput: kinds, InputSchema: kinds, OutputSchema: kinds, Authorize: tgrpc.AllowAuthenticated,
			MaxRequestBytes: tgrpc.MaxMessageBytesLimit, Handle: func(_ context.Context, call tgrpc.Call) (json.RawMessage, error) {
				calls.Add(1)
				return call.Input, nil
			}},
		{Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
			MaxRequestBytes: 64, Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
				calls.Add(1)
				return echo(ctx, call)
			}},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	// Field 11 (repeated Order lines), each an empty message: two bytes on
	// the wire, a whole message once decoded.
	flood := bytes.Repeat([]byte{0x5a, 0x00}, (tgrpc.MaxMessageBytesLimit-64)/2)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	err = conn.Invoke(as(context.Background(), "alice"), "/newblok.test.orders.v1.Orders/Inspect", rawRequest(flood), &orderpb.Kinds{}, grpc.ForceCodec(rawCodec{}))
	runtime.ReadMemStats(&after)
	if status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" || calls.Load() != 0 {
		t.Fatalf("a flood of empty messages: %v", err)
	}
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > 32<<20 {
		t.Fatalf("refusing %d bytes allocated %d bytes", len(flood), allocated)
	}
	// Repeated sku fields: the last one wins once decoded, but the wire
	// size is what is bounded.
	var repeated []byte
	for len(repeated) < 4096 {
		repeated = protowire.AppendTag(repeated, 1, protowire.BytesType)
		repeated = protowire.AppendString(repeated, "tea")
	}
	repeated = protowire.AppendTag(repeated, 2, protowire.VarintType)
	repeated = protowire.AppendVarint(repeated, 1)
	err = conn.Invoke(as(context.Background(), "alice"), placeMethod, rawRequest(repeated), &orderpb.Placed{}, grpc.ForceCodec(rawCodec{}))
	if status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" || calls.Load() != 0 {
		t.Fatalf("repeated fields over the wire bound: %v", err)
	}
}

// TestJSONValueLimit: a message whose JSON form exceeds the domain value
// limit is refused as too large, not as invalid, in either direction.
func TestJSONValueLimit(t *testing.T) {
	f := loadCases(t)
	var kinds json.RawMessage
	for _, tc := range f.Startup {
		if tc.Method == "Inspect" && tc.Expect == "" {
			kinds = tc.Input
		}
	}
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Inspect"), Workflow: "inspect", WorkflowInput: kinds, InputSchema: kinds, OutputSchema: kinds, Authorize: tgrpc.AllowAuthenticated,
		MaxRequestBytes: tgrpc.MaxMessageBytesLimit, MaxResponseBytes: tgrpc.MaxMessageBytesLimit,
		Handle: func(_ context.Context, call tgrpc.Call) (json.RawMessage, error) {
			if strings.Contains(string(call.Input), `"name":"grow"`) {
				return json.RawMessage(`{"name":"` + strings.Repeat("x", tgrpc.MaxMessageBytesLimit) + `"}`), nil
			}
			return call.Input, nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	client := orderpb.NewOrdersClient(conn)
	_, err = client.Inspect(as(context.Background(), "alice"), &orderpb.Kinds{Blob: bytes.Repeat([]byte{7}, 800<<10)})
	if status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" {
		t.Fatalf("a request whose JSON form is too large: %v", err)
	}
	// The estimate is a lower bound: control characters are escaped six
	// bytes each, so only the complete JSON form shows this one too large.
	_, err = client.Inspect(as(context.Background(), "alice"), &orderpb.Kinds{Name: strings.Repeat("\x01", 200<<10)})
	if status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" {
		t.Fatalf("a request whose escaped JSON form is too large: %v", err)
	}
	_, err = client.Inspect(as(context.Background(), "alice"), &orderpb.Kinds{Name: "grow"})
	if status.Code(err) != codes.ResourceExhausted || reason(err) != "response_too_large" {
		t.Fatalf("an output whose JSON form is too large: %v", err)
	}
}

// TestPanicFailsTheCallNotTheServer: a panicking workflow on a server shared
// with the worker runtime fails its call; the server keeps serving.
func TestPanicFailsTheCallNotTheServer(t *testing.T) {
	f := loadCases(t)
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			if strings.Contains(string(call.Input), "boom") {
				panic("synthetic workflow panic")
			}
			return echo(ctx, call)
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	shared := grpc.NewServer()
	wire.RegisterWorkerServer(shared, workerStub{})
	if err := adapter.Register(shared); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = shared.Serve(listener) }()
	defer shared.Stop()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := orderpb.NewOrdersClient(conn)
	boom, ok, quantity := "boom", "tea", int32(1)
	if _, err := client.Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &boom, Quantity: &quantity}); status.Code(err) != codes.Internal || reason(err) != "internal" || strings.Contains(status.Convert(err).Message(), "panic") {
		t.Fatalf("a panicking workflow: %v", err)
	}
	if _, err := client.Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &ok, Quantity: &quantity}); err != nil {
		t.Fatalf("after a panic: %v", err)
	}
	if _, err := wire.NewWorkerClient(conn).Invoke(context.Background(), &wire.Call{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("the worker service after a panic: %v", err)
	}
}

// TestInterceptorMayReplaceTheRequest: an application interceptor's request
// is the one the workflow receives.
func TestInterceptorMayReplaceTheRequest(t *testing.T) {
	f := loadCases(t)
	var seen atomic.Value
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			seen.Store(string(call.Input))
			return echo(ctx, call)
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter, grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		replaced := proto.Clone(req.(proto.Message))
		message := replaced.ProtoReflect()
		message.Set(message.Descriptor().Fields().ByName("sku"), protoreflect.ValueOfString("replaced"))
		return handler(ctx, replaced)
	}))
	sku, quantity := "tea", int32(1)
	if _, err := orderpb.NewOrdersClient(conn).Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &sku, Quantity: &quantity}); err != nil {
		t.Fatal(err)
	}
	if got, _ := seen.Load().(string); !strings.Contains(got, `"replaced"`) {
		t.Fatalf("the workflow received %s", got)
	}
}

// TestScanCountsPackedElements: a packed repeated field is one tag on the
// wire but many elements once decoded; each element counts.
func TestScanCountsPackedElements(t *testing.T) {
	f := loadCases(t)
	var kinds json.RawMessage
	for _, tc := range f.Startup {
		if tc.Method == "Inspect" && tc.Expect == "" {
			kinds = tc.Input
		}
	}
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Inspect"), Workflow: "inspect", WorkflowInput: kinds, InputSchema: kinds, OutputSchema: kinds, Authorize: tgrpc.AllowAuthenticated,
		MaxElements: 100, Handle: func(_ context.Context, call tgrpc.Call) (json.RawMessage, error) { return call.Input, nil },
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	client := orderpb.NewOrdersClient(conn)
	if _, err := client.Inspect(as(context.Background(), "alice"), &orderpb.Kinds{Counts: make([]int32, 50)}); err != nil {
		t.Fatalf("50 packed elements: %v", err)
	}
	if _, err := client.Inspect(as(context.Background(), "alice"), &orderpb.Kinds{Counts: make([]int32, 500)}); status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" {
		t.Fatalf("500 packed elements against a budget of 100: %v", err)
	}
}

var (
	openObject = json.RawMessage(`{"type":"object","additionalProperties":true}`)
	nameOnly   = json.RawMessage(`{"type":"object","properties":{"name":{"type":"string"}}}`)
)

// TestDefaultsCannotExpandTheValue: implicit-presence fields are written
// with their defaults, so a few wire bytes per element can mean hundreds of
// JSON bytes; a request whose JSON form would exceed the value limit is
// refused before that form is built.
func TestDefaultsCannotExpandTheValue(t *testing.T) {
	var calls atomic.Int64
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Fill"), Workflow: "fill", WorkflowInput: openObject, InputSchema: openObject, OutputSchema: nameOnly, Authorize: tgrpc.AllowAuthenticated,
		MaxElements: tgrpc.MaxElementsLimit, MaxRequestBytes: tgrpc.MaxMessageBytesLimit,
		Handle: func(context.Context, tgrpc.Call) (json.RawMessage, error) {
			calls.Add(1)
			return json.RawMessage(`{"name":"ok"}`), nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	client := orderpb.NewOrdersClient(conn)
	if _, err := client.Fill(as(context.Background(), "alice"), &orderpb.Rows{Rows: make([]*orderpb.Wide, 100)}); err != nil {
		t.Fatalf("100 empty rows: %v", err)
	}
	// 99 999 empty rows: two wire bytes each, about 300 JSON bytes each.
	flood := bytes.Repeat([]byte{0x0a, 0x00}, tgrpc.MaxElementsLimit/2-1)
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	err = conn.Invoke(as(context.Background(), "alice"), "/newblok.test.orders.v1.Orders/Fill", rawRequest(flood), &orderpb.Node{}, grpc.ForceCodec(rawCodec{}))
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	t.Logf("refusing %d wire bytes allocated %d bytes", len(flood), allocated)
	if status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" || calls.Load() != 1 {
		t.Fatalf("rows whose defaults exceed the value limit: %v", err)
	}
	if allocated > expansionBound {
		t.Fatalf("refusing the rows allocated %d bytes: the JSON form was built", allocated)
	}
}

// TestNestingBound: nesting up to 32 levels is accepted and one more is
// refused from its wire bytes.
func TestNestingBound(t *testing.T) {
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Walk"), Workflow: "walk", WorkflowInput: openObject, InputSchema: openObject, OutputSchema: nameOnly, Authorize: tgrpc.AllowAuthenticated,
		Handle: func(context.Context, tgrpc.Call) (json.RawMessage, error) {
			return json.RawMessage(`{"name":"ok"}`), nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	client := orderpb.NewOrdersClient(conn)
	nested := func(depth int) *orderpb.Node {
		node := &orderpb.Node{Name: "leaf"}
		for i := 0; i < depth; i++ {
			node = &orderpb.Node{Name: "n", Child: node}
		}
		return node
	}
	if _, err := client.Walk(as(context.Background(), "alice"), nested(tgrpc.MaxNesting)); err != nil {
		t.Fatalf("nesting %d: %v", tgrpc.MaxNesting, err)
	}
	if _, err := client.Walk(as(context.Background(), "alice"), nested(tgrpc.MaxNesting+1)); status.Code(err) != codes.ResourceExhausted || reason(err) != "too_large" {
		t.Fatalf("nesting %d: %v", tgrpc.MaxNesting+1, err)
	}
}

// jsonCodec encodes messages as protobuf JSON, under the "json" content
// subtype.
type jsonCodec struct{}

func (jsonCodec) Name() string                  { return "json" }
func (jsonCodec) Marshal(v any) ([]byte, error) { return protojson.Marshal(v.(proto.Message)) }
func (jsonCodec) Unmarshal(data []byte, v any) error {
	return protojson.Unmarshal(data, v.(proto.Message))
}

func init() { encoding.RegisterCodec(jsonCodec{}) }

// TestOnlyProtobufEncoding: a call in another encoding is refused, not
// misread.
func TestOnlyProtobufEncoding(t *testing.T) {
	f := loadCases(t)
	var calls atomic.Int64
	adapter, err := tgrpc.New(started(t), principals, []tgrpc.Binding{{
		Method: orders.Methods().ByName("Place"), Workflow: "place", WorkflowInput: f.Order, InputSchema: f.Order, OutputSchema: f.Placed, Authorize: tgrpc.AllowAuthenticated,
		Handle: func(ctx context.Context, call tgrpc.Call) (json.RawMessage, error) {
			calls.Add(1)
			return echo(ctx, call)
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, conn := serve(t, adapter)
	sku, quantity := "tea", int32(1)
	_, err = orderpb.NewOrdersClient(conn).Place(as(context.Background(), "alice"), &orderpb.Order{Sku: &sku, Quantity: &quantity}, grpc.CallContentSubtype("json"))
	if status.Code(err) != codes.InvalidArgument || reason(err) != "unsupported_encoding" || calls.Load() != 0 {
		t.Fatalf("a JSON-encoded call: %v", err)
	}
}
