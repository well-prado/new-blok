package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func recordingConnection(t *testing.T) (*grpcConnection, <-chan string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan string, 256)
	server := grpc.NewServer()
	wire.RegisterWorkerServer(server, testWorker{onCall: func(id string) { received <- id }})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, _, err := (GRPCFactory{Address: listener.Addr().String(), Token: "synthetic-test-token", Principal: "app-1"}).Connect(ctx, hello())
	if err != nil {
		t.Fatal(err)
	}
	c := conn.(*grpcConnection)
	t.Cleanup(c.fail)
	return c, received
}

type delayedLogWorker struct {
	wire.UnimplementedWorkerServer
	canceledCall  chan struct{}
	healthyCall   chan struct{}
	releaseFrames chan struct{}
	sendErrors    chan error
}

func (w *delayedLogWorker) Connect(stream wire.Worker_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	h := contract.HelloFromWire(first.GetHello())
	peer := hello()
	if _, err := contract.Negotiate(h, peer); err != nil {
		return err
	}
	if err := stream.Send(&wire.Frame{Body: &wire.Frame_Ready{Ready: &wire.Ready{Contract: contract.HelloWire(peer)}}}); err != nil {
		return err
	}
	var canceled *wire.Call
	var sendMu sync.Mutex
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch body := frame.Body.(type) {
		case *wire.Frame_Call:
			if body.Call.Node == "cancel-me" {
				canceled = body.Call
				close(w.canceledCall)
				continue
			}
			if body.Call.Node != "healthy" || canceled == nil {
				return status.Error(codes.InvalidArgument, "unexpected test call")
			}
			close(w.healthyCall)
			call := body.Call
			go func() {
				<-w.releaseFrames
				send := func(frame *wire.Frame) error {
					sendMu.Lock()
					defer sendMu.Unlock()
					return stream.Send(frame)
				}
				late := &wire.Log{CallId: canceled.CallId, AttemptId: canceled.AttemptId, Generation: canceled.Generation, Level: "INFO", Message: "late-after-cancel"}
				if err := send(&wire.Frame{Body: &wire.Frame_Log{Log: late}}); err != nil {
					w.sendErrors <- err
					return
				}
				current := &wire.Log{CallId: call.CallId, AttemptId: call.AttemptId, Generation: call.Generation, Level: "INFO", Message: "healthy-log"}
				if err := send(&wire.Frame{Body: &wire.Frame_Log{Log: current}}); err != nil {
					w.sendErrors <- err
					return
				}
				w.sendErrors <- send(&wire.Frame{Body: &wire.Frame_Result{Result: &wire.Result{CallId: call.CallId, AttemptId: call.AttemptId, Generation: call.Generation, Output: call.Input}}})
			}()
		case *wire.Frame_Cancel:
			if canceled == nil || body.Cancel.CallId != canceled.CallId || body.Cancel.AttemptId != canceled.AttemptId {
				return status.Error(codes.InvalidArgument, "unexpected cancellation")
			}
			// Keep the log queued until the healthy sibling is active, then send
			// it before that sibling's own log and result on the same stream.
		case *wire.Frame_Drain:
			return nil
		default:
			return status.Error(codes.InvalidArgument, "unexpected frame")
		}
	}
}

func TestLateCanceledLogDoesNotPoisonHealthySiblingOrBlockResultReader(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	worker := &delayedLogWorker{canceledCall: make(chan struct{}), healthyCall: make(chan struct{}), releaseFrames: make(chan struct{}), sendErrors: make(chan error, 1)}
	server := grpc.NewServer()
	wire.RegisterWorkerServer(server, worker)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connectCtx, stopConnect := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopConnect()
	conn, _, err := (GRPCFactory{Address: listener.Addr().String(), Token: "synthetic-test-token", Principal: "app-1"}).Connect(connectCtx, hello())
	if err != nil {
		t.Fatal(err)
	}
	connection := conn.(*grpcConnection)
	t.Cleanup(connection.fail)

	late := call("cancelled-log")
	late.Node = "cancel-me"
	late.Deadline = time.Now().Add(100 * time.Millisecond)
	var lateCallback atomic.Int32
	late.OnLog = func(contract.Log) { lateCallback.Add(1) }
	if _, err := conn.Call(context.Background(), late); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled invocation: %v", err)
	}
	select {
	case <-worker.canceledCall:
	case <-time.After(time.Second):
		t.Fatal("worker did not receive canceled invocation")
	}

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	defer close(releaseCallback)
	healthy := call("healthy-sibling")
	healthy.Node = "healthy"
	healthy.Deadline = time.Now().Add(2 * time.Second)
	healthy.OnLog = func(entry contract.Log) {
		if entry.Message == "healthy-log" {
			close(callbackStarted)
			<-releaseCallback
		}
	}
	done := make(chan error, 1)
	go func() {
		result, err := conn.Call(context.Background(), healthy)
		if err == nil && (result.CallID != healthy.CallID || string(result.Output) != string(healthy.Input)) {
			err = errors.New("healthy sibling received the wrong result")
		}
		done <- err
	}()
	select {
	case <-worker.healthyCall:
	case <-time.After(time.Second):
		t.Fatal("healthy sibling did not reach worker")
	}
	close(worker.releaseFrames)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("healthy sibling was poisoned by late log: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous log callback blocked the shared result reader")
	}
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("healthy log callback was not dispatched")
	}
	select {
	case err := <-worker.sendErrors:
		if err != nil {
			t.Fatalf("test worker send: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("test worker did not send sibling frames")
	}
	if got := lateCallback.Load(); got != 0 {
		t.Fatalf("late canceled log delivered to callback %d times", got)
	}
}

func TestAlreadyCanceledCallsNeverReachGRPCHandler(t *testing.T) {
	c, received := recordingConnection(t)
	s, err := New(Config{Hello: hello(), Factory: connectionFactory{c}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for i := 0; i < 128; i++ {
		if _, err := c.Call(ctx, call(fmt.Sprintf("direct-canceled-%d", i))); !errors.Is(err, context.Canceled) {
			t.Fatalf("direct canceled call: %v", err)
		}
		if _, err := s.Call(ctx, call(fmt.Sprintf("supervised-canceled-%d", i))); !errors.Is(err, context.Canceled) {
			t.Fatalf("supervised canceled call: %v", err)
		}
	}
	if len(s.seen) != 0 || len(s.attempts) != 0 || len(s.active) != 0 || len(c.pending) != 0 {
		t.Fatal("already canceled calls consumed admission identities or capacity")
	}
	// A completed live call is an ordered wire barrier: every earlier frame
	// would have been observed by the server before this call.
	live, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if _, err := s.Call(live, call("barrier")); err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-received:
		if id != "barrier" {
			t.Fatalf("canceled handler invoked: %s", id)
		}
	default:
		t.Fatal("live barrier handler was not invoked")
	}
}

type connectionFactory struct{ Connection }

func (f connectionFactory) Connect(context.Context, contract.Hello) (Connection, contract.Ready, error) {
	return f.Connection, makeReady(hello()), nil
}

func TestQueuedCanceledFrameNeverReachesGRPCHandler(t *testing.T) {
	live, received := recordingConnection(t)
	// A dedicated queue makes cancellation-before-writer-dispatch deterministic.
	// The real authenticated gRPC stream and its reader remain in use.
	c := &grpcConnection{client: live.client, stream: live.stream, cancel: live.cancel, pending: map[string]chan outcome{}, send: make(chan queuedFrame, 2), done: make(chan struct{})}
	t.Cleanup(c.fail)
	ctx, cancel := context.WithCancel(context.Background())
	ack := make(chan error, 1)
	request := call("queued-canceled")
	request.Principal = "app-1"
	c.send <- queuedFrame{ctx: ctx, ack: ack, frame: &wire.Frame{Body: &wire.Frame_Call{Call: contract.CallWire(request)}}}
	cancel()
	go c.writeLoop()
	select {
	case err := <-ack:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("queued cancellation acknowledgment: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not acknowledge canceled queued frame")
	}
	barrier := call("queue-barrier")
	barrier.Principal = "app-1"
	c.send <- queuedFrame{frame: &wire.Frame{Body: &wire.Frame_Call{Call: contract.CallWire(barrier)}}}
	select {
	case id := <-received:
		if id != barrier.CallID {
			t.Fatalf("queued canceled handler invoked: %s", id)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not dispatch live barrier")
	}
}

// Model a flow-controlled Send that cannot acknowledge drain until the
// connection lifetime is canceled. All other traffic uses the real stream.
type stalledDrainStream struct {
	wire.Worker_ConnectClient
	entered chan struct{}
	exited  chan struct{}
}

func (s *stalledDrainStream) Send(frame *wire.Frame) error {
	if frame.GetDrain() == nil {
		return s.Worker_ConnectClient.Send(frame)
	}
	close(s.entered)
	<-s.Context().Done()
	close(s.exited)
	return s.Context().Err()
}

func TestShutdownStalledDrainReapsRealProcess(t *testing.T) {
	for _, mode := range []string{"shutdown-deadline", "background-cleanup", "active-timeout"} {
		t.Run(mode, func(t *testing.T) {
			f := factory(t, freeAddress(t))
			f.Env = append(f.Env, "WORKER_IGNORE_TERM=1")
			s, err := New(Config{Hello: hello(), Factory: f})
			if err != nil {
				t.Fatal(err)
			}
			startup, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			if err := s.Start(startup); err != nil {
				t.Fatal(err)
			}
			p := s.conn.(*processConnection)
			t.Cleanup(func() { _ = p.cmd.Process.Kill(); <-p.exited })
			original := p.Connection.(*grpcConnection)
			stalled := &stalledDrainStream{Worker_ConnectClient: original.stream, entered: make(chan struct{}), exited: make(chan struct{})}
			// Install a separate writer before any traffic, without racing a
			// mutation of the already running stream's writer or reader.
			transport := &grpcConnection{client: original.client, stream: stalled, cancel: original.cancel, pending: map[string]chan outcome{}, send: make(chan queuedFrame, 2), done: make(chan struct{})}
			t.Cleanup(transport.fail)
			go transport.writeLoop()
			p.Connection = transport
			if mode == "active-timeout" {
				// Keep a supervisor call active until force-close releases it.
				blocking := &closeReleasedConnection{Connection: p, released: original.done, started: make(chan struct{})}
				s.conn = blocking
				result := make(chan error, 1)
				go func() { _, err := s.Call(startup, call("active")); result <- err }()
				<-blocking.started
				t.Cleanup(func() {
					select {
					case <-result:
					case <-time.After(3 * time.Second):
						t.Error("active call leaked")
					}
				})
			}
			shutdown := context.Background()
			if mode != "background-cleanup" {
				var stop context.CancelFunc
				shutdown, stop = context.WithTimeout(shutdown, 100*time.Millisecond)
				defer stop()
			}
			done := make(chan error, 1)
			go func() { done <- s.Shutdown(shutdown) }()
			if mode != "active-timeout" {
				select {
				case <-stalled.entered:
				case <-time.After(3 * time.Second):
					t.Fatal("drain never reached stalled transport")
				}
			}
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("shutdown: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("shutdown blocked behind transport drain")
			}
			select {
			case <-p.exited:
			default:
				t.Fatal("shutdown returned before process reap")
			}
			if mode != "active-timeout" {
				select {
				case <-stalled.exited:
				case <-time.After(time.Second):
					t.Fatal("stalled writer was not released by force-close")
				}
			}
		})
	}
}

func TestExplicitShutdownBudgetAllowsRealActiveCallToFinish(t *testing.T) {
	f := factory(t, freeAddress(t))
	marker := filepath.Join(t.TempDir(), "drain-started")
	f.Env = append(f.Env, "WORKER_EFFECT_MARK="+marker)
	s, err := New(Config{Hello: hello(), Factory: f})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	p := s.conn.(*processConnection)
	t.Cleanup(func() { _ = p.cmd.Process.Kill(); <-p.exited })
	request := call("explicit-drain-budget")
	request.Node = "drain-budget"
	result := make(chan outcome, 1)
	go func() {
		value, err := s.Call(ctx, request)
		result <- outcome{result: value, err: err}
	}()
	for {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("worker did not start the bounded call")
		}
		time.Sleep(time.Millisecond)
	}
	shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := s.Shutdown(shutdown); err != nil {
		t.Fatalf("explicit three-second budget was shortened: %v", err)
	}
	got := <-result
	if got.err != nil || string(got.result.Output) != string(request.Input) {
		t.Fatalf("accepted call did not finish within configured drain: %+v %v", got.result, got.err)
	}
	select {
	case <-p.exited:
	default:
		t.Fatal("shutdown returned before worker reap")
	}
}

func TestBackgroundShutdownBoundsActiveEffectAndReapsRealProcess(t *testing.T) {
	f := factory(t, freeAddress(t))
	marker := filepath.Join(t.TempDir(), "synthetic-effect")
	f.Env = append(f.Env, "WORKER_IGNORE_TERM=1", "WORKER_EFFECT_MARK="+marker)
	s, err := New(Config{Hello: hello(), Factory: f})
	if err != nil {
		t.Fatal(err)
	}
	startup, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	if err := s.Start(startup); err != nil {
		t.Fatal(err)
	}
	p := s.conn.(*processConnection)
	t.Cleanup(func() { _ = p.cmd.Process.Kill(); <-p.exited })
	callCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	request := call("background-active-effect")
	request.Node = "effect"
	request.IdempotencyKey = "synthetic-operation"
	callDone := make(chan outcome, 1)
	go func() {
		result, err := s.Call(callCtx, request)
		callDone <- outcome{result: result, err: err}
	}()
	// The child writes the effect marker before blocking on its long call
	// deadline. Shutdown must abort this real in-flight RPC, not an idle worker.
	for deadline := time.Now().Add(3 * time.Second); ; {
		if _, err := os.Stat(marker); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("real worker never reached the effect handler")
		}
		time.Sleep(time.Millisecond)
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.Shutdown(context.Background()) }()
	select {
	case err := <-shutdownDone:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("background active drain: %v", err)
		}
	case <-time.After(cleanupTimeout + time.Second):
		t.Fatal("background shutdown waited indefinitely for the active effect")
	}
	select {
	case <-p.exited:
	default:
		t.Fatal("shutdown returned before the child was reaped")
	}
	select {
	case got := <-callDone:
		if !errors.Is(got.err, contract.ErrUncertain) || len(got.result.Output) != 0 {
			t.Fatalf("dispatched effect must remain uncertain: %+v %v", got.result, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("active call goroutine leaked after force-close")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != request.IdempotencyKey {
		t.Fatalf("shutdown must not claim the dispatched effect was undone: %q %v", data, err)
	}
	if _, err := s.Call(context.Background(), call("after-background-shutdown")); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("stopped worker admitted another call: %v", err)
	}
}

type closeReleasedConnection struct {
	Connection
	released <-chan struct{}
	started  chan struct{}
}

func (c *closeReleasedConnection) Call(ctx context.Context, _ contract.Call) (contract.Result, error) {
	close(c.started)
	select {
	case <-c.released:
		return contract.Result{}, contract.ErrUncertain
	case <-ctx.Done():
		return contract.Result{}, ctx.Err()
	}
}
