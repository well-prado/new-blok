package runtime

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	contract "github.com/well-prado/new-blok/contract/runtime"
)

const (
	artifact = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	catalog  = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

func hello() contract.Hello {
	return contract.Hello{Protocol: contract.ProtocolName, Major: contract.ProtocolMajor, Minor: contract.ProtocolMinor, ArtifactDigest: artifact, CatalogDigest: catalog, Generation: 1, Limits: contract.DefaultLimits()}
}

type fakeFactory struct {
	conn  *fakeConnection
	ready contract.Ready
	err   error
}

func (f fakeFactory) Connect(context.Context, contract.Hello) (Connection, contract.Ready, error) {
	return f.conn, f.ready, f.err
}

type fakeConnection struct {
	mu      sync.Mutex
	calls   int
	block   <-chan struct{}
	started chan struct{}
	closed  bool
}

func (f *fakeConnection) Call(ctx context.Context, call contract.Call) (contract.Result, error) {
	f.mu.Lock()
	f.calls++
	f.mu.Unlock()
	if f.started != nil {
		select {
		case <-f.started:
		default:
			close(f.started)
		}
	}
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
			return contract.Result{}, ctx.Err()
		}
	}
	return contract.Result{CallID: call.CallID, AttemptID: call.AttemptID, Generation: call.Generation, Output: []byte(`{"ok":true}`)}, nil
}
func (f *fakeConnection) Close(context.Context) error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

func makeReady(h contract.Hello) contract.Ready {
	return contract.Ready{Protocol: h.Protocol, Major: h.Major, Minor: h.Minor, ArtifactDigest: h.ArtifactDigest, CatalogDigest: h.CatalogDigest, Generation: h.Generation, Limits: h.Limits}
}
func call(id string) contract.Call {
	return contract.Call{CallID: id, AttemptID: "attempt-" + id, Generation: 1, Node: "shop/quote", NodeVersion: "1.0.0", Deadline: time.Now().Add(time.Minute), Input: []byte(`{}`)}
}

func TestStartNegotiatesBeforeReadyAndCallUsesPersistentConnection(t *testing.T) {
	h := hello()
	conn := &fakeConnection{}
	s, err := New(Config{Hello: h, Factory: fakeFactory{conn: conn, ready: makeReady(h)}, Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Ready(); !ok {
		t.Fatal("supervisor did not become ready")
	}
	if _, err := s.Call(context.Background(), call("one")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Call(context.Background(), call("two")); err != nil {
		t.Fatal(err)
	}
	conn.mu.Lock()
	got := conn.calls
	conn.mu.Unlock()
	if got != 2 {
		t.Fatalf("got %d calls, want 2 on one connection", got)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupRejectsGenerationMismatchBeforeTraffic(t *testing.T) {
	h := hello()
	conn := &fakeConnection{}
	ready := makeReady(h)
	ready.Generation = 2
	s, err := New(Config{Hello: h, Factory: fakeFactory{conn: conn, ready: ready}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); !errors.Is(err, contract.ErrGenerationMismatch) {
		t.Fatalf("got %v, want generation mismatch", err)
	}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if conn.calls != 0 || !conn.closed {
		t.Fatalf("mismatched connection was used: calls=%d closed=%v", conn.calls, conn.closed)
	}
}

func TestCapacityAndDuplicateCallIdentityAreBounded(t *testing.T) {
	h := hello()
	release := make(chan struct{})
	conn := &fakeConnection{block: release}
	s, err := New(Config{Hello: h, Factory: fakeFactory{conn: conn, ready: makeReady(h)}, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { _, err := s.Call(context.Background(), call("one")); first <- err }()
	time.Sleep(10 * time.Millisecond)
	if _, err := s.Call(context.Background(), call("two")); !errors.Is(err, ErrCapacity) {
		t.Fatalf("got %v, want capacity", err)
	}
	if _, err := s.Call(context.Background(), call("one")); !errors.Is(err, ErrCallActive) {
		t.Fatalf("got %v, want duplicate", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := s.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownStopsNewCallsAndWaitsForActiveCall(t *testing.T) {
	h := hello()
	release := make(chan struct{})
	started := make(chan struct{})
	conn := &fakeConnection{block: release, started: started}
	s, err := New(Config{Hello: h, Factory: fakeFactory{conn: conn, ready: makeReady(h)}, Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	callDone := make(chan error, 1)
	go func() { _, err := s.Call(context.Background(), call("one")); callDone <- err }()
	<-started
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- s.Shutdown(context.Background()) }()
	time.Sleep(10 * time.Millisecond)
	if _, err := s.Call(context.Background(), call("two")); !errors.Is(err, ErrDraining) {
		t.Fatalf("got %v, want draining", err)
	}
	close(release)
	if err := <-callDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
}
