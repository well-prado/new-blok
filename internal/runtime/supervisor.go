// Package runtime supervises one selected persistent worker connection. It
// owns admission, generation checks, cancellation and draining; it does not
// interpret workflows or retry uncertain effects.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	contract "github.com/well-prado/new-blok/contract/runtime"
)

var (
	ErrNotStarted = errors.New("worker supervisor: not started")
	ErrDraining   = errors.New("worker supervisor: draining")
	ErrCallActive = errors.New("worker supervisor: duplicate call identity")
	ErrLateResult = errors.New("worker supervisor: stale result discarded")
	ErrCapacity   = errors.New("worker supervisor: capacity exhausted")
)

// Connection is implemented by a gRPC adapter. A connection is already
// authenticated and negotiated when it is returned by Factory.Connect.
type Connection interface {
	Call(context.Context, contract.Call) (contract.Result, error)
	Close(context.Context) error
}

// Cleanup has a finite budget even when the lifecycle caller has no deadline.
const cleanupTimeout = time.Second

func closeConnection(ctx context.Context, conn Connection) error {
	cleanup, cancel := context.WithTimeout(ctx, cleanupTimeout)
	defer cancel()
	return conn.Close(cleanup)
}

type Factory interface {
	Connect(context.Context, contract.Hello) (Connection, contract.Ready, error)
}

type Config struct {
	Hello    contract.Hello
	Factory  Factory
	Capacity int
}

type Supervisor struct {
	config   Config
	mu       sync.Mutex
	conn     Connection
	ready    contract.Ready
	state    state
	active   map[string]struct{}
	seen     map[string]struct{}
	attempts map[string]struct{}
	sem      chan struct{}
	done     chan struct{}
}

type state uint8

const (
	stateNew state = iota
	stateStarting
	stateReady
	stateDraining
	stateStopped
)

func New(config Config) (*Supervisor, error) {
	if config.Factory == nil {
		return nil, errors.New("worker supervisor: factory is required")
	}
	if err := config.Hello.Validate(); err != nil {
		return nil, err
	}
	if config.Capacity <= 0 {
		config.Capacity = config.Hello.Limits.MaxConcurrentCalls
	}
	if config.Capacity <= 0 || config.Capacity > config.Hello.Limits.MaxConcurrentCalls {
		return nil, fmt.Errorf("%w: %d", ErrCapacity, config.Capacity)
	}
	return &Supervisor{config: config, state: stateNew, active: map[string]struct{}{}, seen: map[string]struct{}{}, attempts: map[string]struct{}{}, sem: make(chan struct{}, config.Capacity), done: make(chan struct{})}, nil
}

func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.state != stateNew {
		s.mu.Unlock()
		return errors.New("worker supervisor: invalid lifecycle state")
	}
	s.state = stateStarting
	s.mu.Unlock()
	conn, ready, err := s.config.Factory.Connect(ctx, s.config.Hello)
	if err != nil {
		s.finish()
		return fmt.Errorf("worker supervisor: startup failed: %w", err)
	}
	if conn == nil {
		s.finish()
		return errors.New("worker supervisor: factory returned nil connection")
	}
	if ready.Generation != s.config.Hello.Generation || ready.ArtifactDigest != s.config.Hello.ArtifactDigest || ready.CatalogDigest != s.config.Hello.CatalogDigest {
		_ = closeConnection(ctx, conn)
		s.finish()
		return contract.ErrGenerationMismatch
	}
	peer := s.config.Hello
	peer.Protocol, peer.Major, peer.Minor = ready.Protocol, ready.Major, ready.Minor
	peer.Capabilities, peer.Limits = ready.Capabilities, ready.Limits
	if _, err := contract.Negotiate(s.config.Hello, peer); err != nil {
		_ = closeConnection(ctx, conn)
		s.finish()
		return err
	}
	s.mu.Lock()
	s.conn, s.ready, s.state = conn, ready, stateReady
	s.mu.Unlock()
	return nil
}

func (s *Supervisor) Call(ctx context.Context, call contract.Call) (contract.Result, error) {
	if err := ctx.Err(); err != nil {
		return contract.Result{}, err
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return contract.Result{}, err
	}
	if s.state != stateReady {
		err := s.lifecycleError()
		s.mu.Unlock()
		return contract.Result{}, err
	}
	conn, generation := s.conn, s.ready.Generation
	_, seenCall := s.seen[call.CallID]
	_, seenAttempt := s.attempts[call.AttemptID]
	if seenCall || seenAttempt {
		s.mu.Unlock()
		return contract.Result{}, ErrCallActive
	}
	if len(s.seen) >= 100000 {
		s.mu.Unlock()
		return contract.Result{}, ErrCapacity
	}
	if err := call.Validate(s.ready.Limits, generation); err != nil {
		s.mu.Unlock()
		return contract.Result{}, err
	}
	select {
	case s.sem <- struct{}{}:
	default:
		s.mu.Unlock()
		return contract.Result{}, ErrCapacity
	}
	s.active[call.CallID] = struct{}{}
	s.seen[call.CallID] = struct{}{}
	s.attempts[call.AttemptID] = struct{}{}
	s.mu.Unlock()
	defer func() { <-s.sem; s.mu.Lock(); delete(s.active, call.CallID); s.signalDone(); s.mu.Unlock() }()
	result, err := conn.Call(ctx, call)
	if err != nil {
		return contract.Result{}, err
	}
	if err := ctx.Err(); err != nil {
		return contract.Result{}, err
	}
	if result.CallID != call.CallID || result.AttemptID != call.AttemptID || result.Generation != generation {
		return contract.Result{}, ErrLateResult
	}
	return result, nil
}

func (s *Supervisor) Shutdown(ctx context.Context) error {
	// Respect the application's explicit drain deadline. Only an unbounded
	// context needs a default budget so Background cannot wait forever.
	if _, bounded := ctx.Deadline(); !bounded {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cleanupTimeout)
		defer cancel()
	}
	s.mu.Lock()
	if s.state == stateStopped {
		s.mu.Unlock()
		return nil
	}
	if s.state != stateReady {
		s.mu.Unlock()
		return errors.New("worker supervisor: invalid lifecycle state")
	}
	s.state = stateDraining
	done := make(chan struct{})
	s.done = done
	if len(s.active) == 0 {
		close(done)
	}
	conn := s.conn
	s.mu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		_ = closeConnection(ctx, conn)
		s.finish()
		return ctx.Err()
	}
	if err := closeConnection(ctx, conn); err != nil {
		s.finish()
		return err
	}
	s.finish()
	return nil
}

func (s *Supervisor) finish() { s.mu.Lock(); s.state = stateStopped; s.signalDone(); s.mu.Unlock() }
func (s *Supervisor) lifecycleError() error {
	if s.state == stateDraining {
		return ErrDraining
	}
	return ErrNotStarted
}
func (s *Supervisor) signalDone() {
	if s.state == stateDraining && len(s.active) == 0 && s.done != nil {
		select {
		case <-s.done:
		default:
			close(s.done)
		}
	}
}

func (s *Supervisor) Ready() (contract.Ready, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ready, s.state == stateReady
}
