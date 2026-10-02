package runtime

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"net"
	"sync"
)

// GRPCFactory reuses one authenticated stream. Plaintext bearer transport is
// allowed only on loopback; a remote address requires verified TLS.
type GRPCFactory struct {
	Address, Token, Principal string
	Capabilities              []contract.Capability
	TLS                       *tls.Config
}
type outcome struct {
	result contract.Result
	err    error
}
type queuedFrame struct {
	frame *wire.Frame
	ack   chan error
}
type grpcConnection struct {
	client    *grpc.ClientConn
	stream    wire.Worker_ConnectClient
	cancel    context.CancelFunc
	mu        sync.Mutex
	pending   map[string]chan outcome
	send      chan queuedFrame
	done      chan struct{}
	once      sync.Once
	ready     contract.Ready
	principal string
	caps      []contract.Capability
}

func (f GRPCFactory) Connect(ctx context.Context, h contract.Hello) (Connection, contract.Ready, error) {
	if f.Token == "" || f.Principal == "" {
		return nil, contract.Ready{}, errors.New("worker authentication configuration required")
	}
	if err := h.Validate(); err != nil {
		return nil, contract.Ready{}, err
	}
	if !capSubset(f.Capabilities, h.Capabilities) || !capSubset(h.Capabilities, f.Capabilities) {
		return nil, contract.Ready{}, contract.ErrCapabilityDenied
	}
	var transport credentials.TransportCredentials
	if f.TLS != nil {
		if f.TLS.InsecureSkipVerify {
			return nil, contract.Ready{}, errors.New("worker TLS verification required")
		}
		transport = credentials.NewTLS(f.TLS.Clone())
	} else {
		host, _, err := net.SplitHostPort(f.Address)
		if err != nil || net.ParseIP(host) == nil || !net.ParseIP(host).IsLoopback() {
			return nil, contract.Ready{}, errors.New("remote worker requires TLS")
		}
		transport = insecure.NewCredentials()
	}
	client, err := grpc.NewClient(f.Address, grpc.WithTransportCredentials(transport), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(h.Limits.MaxFrameBytes), grpc.MaxCallSendMsgSize(h.Limits.MaxFrameBytes)))
	if err != nil {
		return nil, contract.Ready{}, errors.New("worker channel creation failed")
	}
	life, cancel := context.WithCancel(context.Background())
	life = metadata.NewOutgoingContext(life, metadata.Pairs("authorization", "Bearer "+f.Token, "x-blok-principal", f.Principal))
	conn := &grpcConnection{client: client, cancel: cancel, pending: map[string]chan outcome{}, send: make(chan queuedFrame, h.Limits.MaxConcurrentCalls+2), done: make(chan struct{}), principal: f.Principal, caps: append([]contract.Capability(nil), f.Capabilities...)}
	handshake := make(chan error, 1)
	go func() {
		stream, err := wire.NewWorkerClient(client).Connect(life)
		if err != nil {
			handshake <- errors.New("worker connect failed")
			return
		}
		conn.stream = stream
		if err := stream.Send(&wire.Frame{Body: &wire.Frame_Hello{Hello: contract.HelloWire(h)}}); err != nil {
			handshake <- errors.New("worker hello send failed")
			return
		}
		frame, err := stream.Recv()
		if err != nil {
			handshake <- errors.New("worker negotiation failed")
			return
		}
		if frame.GetReady() == nil {
			handshake <- contract.ErrIncompatibleProtocol
			return
		}
		peer := contract.HelloFromWire(frame.GetReady().Contract)
		ready, err := contract.Negotiate(h, peer)
		if err == nil {
			conn.ready = ready
		}
		handshake <- err
	}()
	select {
	case err := <-handshake:
		if err != nil {
			cancel()
			client.Close()
			return nil, contract.Ready{}, err
		}
	case <-ctx.Done():
		cancel()
		client.Close()
		return nil, contract.Ready{}, ctx.Err()
	}
	go conn.writeLoop()
	go conn.readLoop()
	return conn, conn.ready, nil
}
func capSubset(a, b []contract.Capability) bool {
	for _, v := range a {
		found := false
		for _, x := range b {
			if v == x {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func (c *grpcConnection) fail() {
	c.once.Do(func() {
		c.cancel()
		_ = c.client.Close()
		close(c.done)
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, ch := range c.pending {
			select {
			case ch <- outcome{err: contract.ErrUncertain}:
			default:
			}
		}
	})
}
func (c *grpcConnection) writeLoop() {
	for {
		select {
		case q := <-c.send:
			err := c.stream.Send(q.frame)
			if q.ack != nil {
				q.ack <- err
			}
			if err != nil {
				c.fail()
				return
			}
		case <-c.done:
			return
		}
	}
}
func (c *grpcConnection) readLoop() {
	for {
		frame, err := c.stream.Recv()
		if err != nil {
			c.fail()
			return
		}
		raw := frame.GetResult()
		if raw == nil {
			c.fail()
			return
		}
		r := contract.ResultFromWire(raw)
		c.mu.Lock()
		ch := c.pending[r.CallID]
		c.mu.Unlock()
		if ch != nil {
			select {
			case ch <- outcome{result: r}:
			default:
				c.fail()
				return
			}
		}
	}
}
func (c *grpcConnection) Call(ctx context.Context, call contract.Call) (contract.Result, error) {
	if err := call.Validate(c.ready.Limits, c.ready.Generation); err != nil {
		return contract.Result{}, err
	}
	if call.Principal != "" && call.Principal != c.principal {
		return contract.Result{}, contract.ErrCapabilityDenied
	}
	if !capSubset(call.Capabilities, c.caps) {
		return contract.Result{}, contract.ErrCapabilityDenied
	}
	call.Principal = c.principal
	ch := make(chan outcome, 1)
	c.mu.Lock()
	if len(c.pending) >= c.ready.Limits.MaxConcurrentCalls {
		c.mu.Unlock()
		return contract.Result{}, ErrCapacity
	}
	if c.pending[call.CallID] != nil {
		c.mu.Unlock()
		return contract.Result{}, ErrCallActive
	}
	c.pending[call.CallID] = ch
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, call.CallID); c.mu.Unlock() }()
	callCtx, cancel := context.WithDeadline(ctx, call.Deadline)
	defer cancel()
	ack := make(chan error, 1)
	q := queuedFrame{frame: &wire.Frame{Body: &wire.Frame_Call{Call: contract.CallWire(call)}}, ack: ack}
	select {
	case c.send <- q:
	case <-c.done:
		return contract.Result{}, contract.ErrUncertain
	case <-callCtx.Done():
		return contract.Result{}, callCtx.Err()
	}
	select {
	case err := <-ack:
		if err != nil {
			return contract.Result{}, contract.ErrUncertain
		}
	case <-c.done:
		return contract.Result{}, contract.ErrUncertain
	case <-callCtx.Done():
		c.sendCancel(call)
		return contract.Result{}, callCtx.Err()
	}
	select {
	case got := <-ch:
		if got.err != nil {
			return contract.Result{}, got.err
		}
		if got.result.Generation != call.Generation || got.result.AttemptID != call.AttemptID {
			return contract.Result{}, ErrLateResult
		}
		if callCtx.Err() != nil {
			return contract.Result{}, callCtx.Err()
		}
		return got.result, nil
	case <-c.done:
		return contract.Result{}, contract.ErrUncertain
	case <-callCtx.Done():
		c.sendCancel(call)
		return contract.Result{}, callCtx.Err()
	}
}
func (c *grpcConnection) sendCancel(call contract.Call) {
	q := queuedFrame{frame: &wire.Frame{Body: &wire.Frame_Cancel{Cancel: &wire.Cancel{CallId: call.CallID, AttemptId: call.AttemptID, Generation: call.Generation}}}}
	select {
	case c.send <- q:
	default:
		c.fail()
	}
}
func (c *grpcConnection) Close(ctx context.Context) error {
	ack := make(chan error, 1)
	select {
	case <-c.done:
		return nil
	case c.send <- queuedFrame{frame: &wire.Frame{Body: &wire.Frame_Drain{Drain: &wire.Drain{}}}, ack: ack}:
	case <-ctx.Done():
		c.fail()
		return ctx.Err()
	}
	select {
	case <-ack:
	case <-ctx.Done():
		c.fail()
		return ctx.Err()
	case <-c.done:
	}
	c.fail()
	return nil
}

// NodeError exposes classifications without reflecting provider messages.
func NodeError(r contract.Result) error {
	if r.Error == nil {
		return nil
	}
	return fmt.Errorf("worker %s: %s", r.Error.Class, r.Error.Code)
}
