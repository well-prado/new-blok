package runtime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"google.golang.org/grpc"
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
