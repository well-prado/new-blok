package runtime

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"unicode/utf8"

	contract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/contract/runtime/wire"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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
	ctx   context.Context
}
type logRoute struct {
	attempt    string
	generation uint64
	callback   func(contract.Log)
	count      int
	bytes      int
}
type logDelivery struct {
	callback func(contract.Log)
	entry    contract.Log
}

const maxQueuedLogCallbacks = 256

type grpcConnection struct {
	client    *grpc.ClientConn
	stream    wire.Worker_ConnectClient
	cancel    context.CancelFunc
	mu        sync.Mutex
	pending   map[string]chan outcome
	loggers   map[string]logRoute
	logQueue  chan logDelivery
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
	conn := &grpcConnection{client: client, cancel: cancel, pending: map[string]chan outcome{}, loggers: map[string]logRoute{}, logQueue: make(chan logDelivery, maxQueuedLogCallbacks), send: make(chan queuedFrame, h.Limits.MaxConcurrentCalls+2), done: make(chan struct{}), principal: f.Principal, caps: append([]contract.Capability(nil), f.Capabilities...)}
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
	go conn.logLoop()
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
			// Cancellation while queued must not dispatch a node effect. Control
			// frames have no call context and still need to reach the worker.
			if q.ctx != nil && q.ctx.Err() != nil {
				if q.ack != nil {
					q.ack <- q.ctx.Err()
				}
				continue
			}
			select {
			case <-c.done:
				return
			default:
			}
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
		switch body := frame.GetBody().(type) {
		case *wire.Frame_Log:
			if body.Log == nil || body.Log.CallId == "" || body.Log.AttemptId == "" {
				c.fail()
				return
			}
			if body.Log.Generation != c.ready.Generation {
				c.fail()
				return
			}
			if !utf8.ValidString(body.Log.Level) || !utf8.ValidString(body.Log.Message) || body.Log.Level != "DEBUG" && body.Log.Level != "INFO" && body.Log.Level != "WARN" && body.Log.Level != "ERROR" || len(body.Log.Message) > contract.MaxLogMessageBytes || len(body.Log.AttrsJson) > contract.MaxLogAttrsBytes || !validLogAttrs(body.Log.AttrsJson) {
				c.fail()
				return
			}
			c.mu.Lock()
			route, ok := c.loggers[body.Log.CallId]
			if !ok || route.attempt != body.Log.AttemptId || route.generation != body.Log.Generation {
				c.mu.Unlock()
				// A late log for a completed/canceled attempt is stale optional
				// data. Never poison the shared stream or attribute it to a reused ID.
				continue
			}
			if route.count <= contract.MaxCallLogs {
				route.count++
			}
			if route.bytes <= contract.MaxCallLogBytes {
				route.bytes += len(body.Log.Message) + len(body.Log.AttrsJson)
				if route.bytes > contract.MaxCallLogBytes {
					route.bytes = contract.MaxCallLogBytes + 1
				}
			}
			dropped := route.count > contract.MaxCallLogs || route.bytes > contract.MaxCallLogBytes
			c.loggers[body.Log.CallId] = route
			callback := route.callback
			c.mu.Unlock()
			if callback != nil && !dropped {
				entry := contract.Log{Level: body.Log.Level, Message: body.Log.Message, Attrs: append([]byte(nil), body.Log.AttrsJson...)}
				select {
				case c.logQueue <- logDelivery{callback: callback, entry: entry}:
				default: // Best-effort logs never backpressure the result stream.
				}
			}
		case *wire.Frame_Result:
			if body.Result == nil {
				c.fail()
				return
			}
			r := contract.ResultFromWire(body.Result)
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
		default:
			c.fail()
			return
		}
	}
}

// logLoop isolates application logging from gRPC result progress. The queue is
// bounded and best-effort; callbacks should return promptly, since a blocked
// callback can delay later logs (but never call results) until the queue fills.
func (c *grpcConnection) logLoop() {
	for {
		select {
		case delivery := <-c.logQueue:
			func() {
				defer func() { _ = recover() }()
				delivery.callback(delivery.entry)
			}()
		case <-c.done:
			return
		}
	}
}

func validLogAttrs(raw []byte) bool {
	if len(raw) == 0 {
		return true
	}
	var attrs map[string]json.RawMessage
	return json.Unmarshal(raw, &attrs) == nil && attrs != nil
}

func (c *grpcConnection) Call(ctx context.Context, call contract.Call) (contract.Result, error) {
	callCtx, cancel := context.WithDeadline(ctx, call.Deadline)
	defer cancel()
	if err := callCtx.Err(); err != nil {
		return contract.Result{}, err
	}
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
	if err := callCtx.Err(); err != nil {
		c.mu.Unlock()
		return contract.Result{}, err
	}
	if len(c.pending) >= c.ready.Limits.MaxConcurrentCalls {
		c.mu.Unlock()
		return contract.Result{}, ErrCapacity
	}
	if c.pending[call.CallID] != nil {
		c.mu.Unlock()
		return contract.Result{}, ErrCallActive
	}
	c.pending[call.CallID] = ch
	c.loggers[call.CallID] = logRoute{attempt: call.AttemptID, generation: call.Generation, callback: call.OnLog}
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, call.CallID); delete(c.loggers, call.CallID); c.mu.Unlock() }()
	ack := make(chan error, 1)
	q := queuedFrame{frame: &wire.Frame{Body: &wire.Frame_Call{Call: contract.CallWire(call)}}, ack: ack, ctx: callCtx}
	if err := callCtx.Err(); err != nil {
		return contract.Result{}, err
	}
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
			if callCtx.Err() != nil {
				return contract.Result{}, callCtx.Err()
			}
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
	if err := ctx.Err(); err != nil {
		c.fail()
		return err
	}
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
