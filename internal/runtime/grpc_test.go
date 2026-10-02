package runtime

import (
	"context"
	"errors"
	"fmt"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type testWorker struct{ wire.UnimplementedWorkerServer }

func (testWorker) Connect(stream wire.Worker_ConnectServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	if len(md.Get("authorization")) != 1 || md.Get("authorization")[0] != "Bearer synthetic-test-token" || len(md.Get("x-blok-principal")) != 1 || md.Get("x-blok-principal")[0] != "app-1" {
		return status.Error(codes.Unauthenticated, "denied")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h := contract.HelloFromWire(first.GetHello())
	if err := h.Validate(); err != nil {
		return status.Error(codes.InvalidArgument, "invalid hello")
	}
	peer := hello()
	ready, err := contract.Negotiate(h, peer)
	if err != nil {
		return status.Error(codes.FailedPrecondition, "identity mismatch")
	}
	_ = ready
	if err := stream.Send(&wire.Frame{Body: &wire.Frame_Ready{Ready: &wire.Ready{Contract: contract.HelloWire(peer)}}}); err != nil {
		return err
	}
	var mu sync.Mutex
	active := map[string]context.CancelFunc{}
	sendMu := sync.Mutex{}
	defer func() {
		mu.Lock()
		defer mu.Unlock()
		for _, cancel := range active {
			cancel()
		}
	}()
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch v := frame.Body.(type) {
		case *wire.Frame_Call:
			c := v.Call
			if c.Principal != "app-1" {
				return status.Error(codes.PermissionDenied, "principal mismatch")
			}
			ctx, cancel := context.WithDeadline(stream.Context(), time.Unix(0, c.DeadlineUnixNanos))
			mu.Lock()
			active[c.CallId] = cancel
			mu.Unlock()
			go func() {
				defer cancel()
				defer func() { mu.Lock(); delete(active, c.CallId); mu.Unlock() }()
				if c.Node == "effect" {
					_ = os.WriteFile(os.Getenv("WORKER_EFFECT_MARK"), []byte(c.IdempotencyKey), 0600)
					<-ctx.Done()
					return
				}
				if c.Node == "slow" {
					select {
					case <-time.After(time.Second):
					case <-ctx.Done():
						return
					}
				}
				if c.Node == "late" {
					time.Sleep(50 * time.Millisecond)
				}
				sendMu.Lock()
				defer sendMu.Unlock()
				_ = stream.Send(&wire.Frame{Body: &wire.Frame_Result{Result: &wire.Result{CallId: c.CallId, AttemptId: c.AttemptId, Generation: c.Generation, Output: c.Input}}})
			}()
		case *wire.Frame_Cancel:
			mu.Lock()
			cancel := active[v.Cancel.CallId]
			mu.Unlock()
			if cancel != nil {
				cancel()
			}
		case *wire.Frame_Drain:
			return nil
		default:
			return status.Error(codes.InvalidArgument, "wrong direction")
		}
	}
}
func TestWorkerSubprocess(t *testing.T) {
	if os.Getenv("WORKER_CHILD") != "1" {
		return
	}
	listener, err := net.Listen("tcp", os.Getenv("WORKER_TEST_ADDRESS"))
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.MaxRecvMsgSize(contract.MaxFrameBytes))
	wire.RegisterWorkerServer(server, testWorker{})
	if err := server.Serve(listener); err != nil {
		t.Fatal(err)
	}
}
func freeAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	return address
}
func factory(t *testing.T, address string) ProcessFactory {
	t.Helper()
	return ProcessFactory{Command: os.Args[0], Args: []string{"-test.run=^TestWorkerSubprocess$"}, Address: address, Token: "synthetic-test-token", Principal: "app-1", Env: []string{"WORKER_CHILD=1", "WORKER_TEST_ADDRESS=" + address, "WORKER_EFFECT_MARK=" + filepath.Join(t.TempDir(), "effect")}, StartupTimeout: time.Second * 3}
}

func TestPersistentSubprocessConcurrentCallsCancellationAndDeath(t *testing.T) {
	f := factory(t, freeAddress(t))
	s, err := New(Config{Hello: hello(), Factory: f, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	p := s.conn.(*processConnection)
	pid := p.PID()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	}()
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := call(fmt.Sprintf("concurrent-%d", i))
			if _, err := s.Call(context.Background(), c); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if p.PID() != pid {
		t.Fatal("worker replaced per call")
	}
	ctx, cancel := context.WithCancel(context.Background())
	slow := call("cancel")
	slow.Node = "slow"
	done := make(chan error, 1)
	go func() { _, err := s.Call(ctx, slow); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Millisecond)
	late := call("late")
	late.Node = "late"
	_, err = s.Call(ctx, late)
	cancel()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}
	time.Sleep(70 * time.Millisecond)
	if _, err := s.Call(context.Background(), call("after-late")); err != nil {
		t.Fatalf("late result poisoned stream: %v", err)
	}
	effect := call("effect")
	effect.Node = "effect"
	effect.IdempotencyKey = "operation-1"
	go func() { _, err := s.Call(context.Background(), effect); done <- err }()
	mark := filepath.Join(filepath.Dir(f.Env[2][len("WORKER_EFFECT_MARK="):]), "effect")
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Stat(mark); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("effect never dispatched")
		}
		time.Sleep(time.Millisecond)
	}
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, contract.ErrUncertain) {
		t.Fatalf("worker died after effect: %v", err)
	}
	if _, err := s.Call(context.Background(), effect); !errors.Is(err, ErrCallActive) {
		t.Fatalf("uncertain call silently retried: %v", err)
	}
}
func TestStartupIdentityAuthenticationTimeoutAndSpoofing(t *testing.T) {
	for _, scenario := range []string{"token", "catalog", "generation"} {
		t.Run(scenario, func(t *testing.T) {
			f := factory(t, freeAddress(t))
			f.StartupTimeout = 250 * time.Millisecond
			h := hello()
			switch scenario {
			case "token":
				f.Token = "wrong"
			case "catalog":
				h.CatalogDigest = h.ArtifactDigest
			case "generation":
				h.Generation = 2
			}
			_, _, err := f.Connect(context.Background(), h)
			if err == nil {
				t.Fatal("incompatible startup accepted")
			}
		})
	}
	if _, _, err := (GRPCFactory{Address: "192.0.2.1:5000", Token: "x", Principal: "p"}).Connect(context.Background(), hello()); err == nil {
		t.Fatal("insecure remote allowed")
	}
}

func TestClientCannotSpoofAuthenticatedPrincipal(t *testing.T) {
	f := factory(t, freeAddress(t))
	conn, _, err := f.Connect(context.Background(), hello())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	c := call("spoof")
	c.Principal = "other"
	if _, err := conn.Call(context.Background(), c); !errors.Is(err, contract.ErrCapabilityDenied) {
		t.Fatalf("spoof accepted: %v", err)
	}
}
