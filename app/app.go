// Package app owns explicit application composition and lifecycle.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
)

var (
	ErrNotReady     = errors.New("application_not_ready")
	ErrDraining     = errors.New("application_draining")
	ErrDrainTimeout = errors.New("application_drain_timeout")
)

type Dependency struct {
	Name  string
	Start func(context.Context) error
	Close func(context.Context) error
}

type Route struct {
	Method   string
	Path     string
	Workflow string
}

type Workflow struct{ Name string }

type Config struct {
	Dependencies []Dependency
	Routes       []Route
	Workflows    []Workflow
	// DrainTimeout bounds how long Shutdown waits for admitted work.
	DrainTimeout time.Duration
	Inspection   inspection.Observer
	// RunOutcomes optionally persists terminal outcomes for trusted invocation
	// IDs supplied to execution.Runner. It is an application-owned port; the
	// engine remains independent of durable stores and absent ports promise no
	// durable run outcome.
	RunOutcomes RunOutcomePort
	// AbortGrace is how long Shutdown waits, after DrainTimeout, for work it
	// has canceled to stop before it closes the dependencies anyway; zero
	// means DefaultAbortGrace. Shutdown's own ctx bounds both waits.
	AbortGrace time.Duration
}

// RunOutcomePort persists a terminal workflow result at the trusted
// application boundary. Implementations must not infer workflow failure from
// an individual failed attempt.
type RunOutcomePort interface {
	CompleteRun(context.Context, inspection.Invocation, json.RawMessage) error
	FailRun(context.Context, inspection.Invocation, string, string) error
	CancelRun(context.Context, inspection.Invocation, string) error
	MarkRunUncertain(context.Context, inspection.Invocation, string, string) error
}

// DefaultAbortGrace is the AbortGrace when none is configured.
const DefaultAbortGrace = time.Second

type State string

const (
	NewState      State = "new"
	StartingState State = "starting"
	ReadyState    State = "ready"
	DrainingState State = "draining"
	StoppedState  State = "stopped"
)

type Application struct {
	config      Config
	mu          sync.Mutex
	state       State
	active      int
	initialized []Dependency
	changed     chan struct{}
	// abort is canceled, with ErrDrainTimeout as its cause, when a
	// shutdown's drain times out; every lease's Context is abort.
	abort     context.Context
	abortWork context.CancelCauseFunc
}

func New(config Config) (*Application, error) {
	if config.DrainTimeout <= 0 {
		config.DrainTimeout = 5 * time.Second
	}
	if config.AbortGrace <= 0 {
		config.AbortGrace = DefaultAbortGrace
	}
	seen := map[string]bool{}
	for _, workflow := range config.Workflows {
		if workflow.Name == "" || seen[workflow.Name] {
			return nil, fmt.Errorf("duplicate_workflow: %s", workflow.Name)
		}
		seen[workflow.Name] = true
	}
	routes := map[string]bool{}
	for _, route := range config.Routes {
		key := route.Method + " " + route.Path
		if route.Method == "" || route.Path == "" || route.Workflow == "" {
			return nil, fmt.Errorf("invalid_route: %s", key)
		}
		if routes[key] {
			return nil, fmt.Errorf("duplicate_route: %s", key)
		}
		if !seen[route.Workflow] {
			return nil, fmt.Errorf("missing_workflow: %s", route.Workflow)
		}
		routes[key] = true
	}
	dependencies := map[string]bool{}
	for _, dependency := range config.Dependencies {
		if dependency.Name == "" || dependency.Start == nil || dependency.Close == nil {
			return nil, fmt.Errorf("invalid_dependency: %s", dependency.Name)
		}
		if dependencies[dependency.Name] {
			return nil, fmt.Errorf("duplicate_dependency: %s", dependency.Name)
		}
		dependencies[dependency.Name] = true
	}
	abort, abortWork := context.WithCancelCause(context.Background())
	return &Application{config: config, state: NewState, changed: make(chan struct{}, 1), abort: abort, abortWork: abortWork}, nil
}

func (a *Application) Start(ctx context.Context) error {
	a.mu.Lock()
	if a.state != NewState {
		a.mu.Unlock()
		return fmt.Errorf("invalid_lifecycle_state: %s", a.state)
	}
	a.state = StartingState
	a.mu.Unlock()
	for _, dependency := range a.config.Dependencies {
		if err := dependency.Start(ctx); err != nil {
			a.closeInitialized(ctx)
			a.mu.Lock()
			a.state = StoppedState
			a.mu.Unlock()
			return fmt.Errorf("startup_failed:%s: %w", dependency.Name, err)
		}
		a.mu.Lock()
		a.initialized = append(a.initialized, dependency)
		a.mu.Unlock()
	}
	a.mu.Lock()
	a.state = ReadyState
	a.mu.Unlock()
	return nil
}

func (a *Application) State() State { a.mu.Lock(); defer a.mu.Unlock(); return a.state }
func (a *Application) Ready() bool  { return a.State() == ReadyState }

// InspectionObserver exposes the configured read-only observer to the public
// execution composition package; adapters still provide trusted per-run identity.
func (a *Application) InspectionObserver() inspection.Observer {
	if a == nil {
		return nil
	}
	return a.config.Inspection
}

// AbortGrace returns the configured bound for canceled work to release its
// leases before Shutdown closes dependencies.
func (a *Application) AbortGrace() time.Duration { return a.config.AbortGrace }

// RunOutcomePort exposes the optional terminal-outcome writer to the public
// execution composition package without importing a concrete journal/store.
func (a *Application) RunOutcomePort() RunOutcomePort {
	if a == nil {
		return nil
	}
	return a.config.RunOutcomes
}

type Lease struct {
	app  *Application
	once sync.Once
}

// Context is canceled, with ErrDrainTimeout as its cause, when Shutdown's
// drain times out: the work the lease admitted must stop then, before the
// application closes its dependencies. Adapters derive the work's context
// from it and answer work it canceled as unavailable.
func (l *Lease) Context() context.Context {
	if l == nil || l.app == nil {
		return context.Background()
	}
	return l.app.abort
}

// Bind returns a context derived from parent that is also canceled, with
// ErrDrainTimeout as its cause, when the lease's work is aborted. The
// returned function releases the binding.
func (l *Lease) Bind(parent context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(parent)
	if l.Context().Err() != nil {
		// Already aborted: the bound context is canceled before it is
		// returned, not later on AfterFunc's goroutine.
		cancel(ErrDrainTimeout)
	}
	stop := context.AfterFunc(l.Context(), func() { cancel(ErrDrainTimeout) })
	return ctx, func() {
		stop()
		cancel(context.Canceled)
	}
}

// Aborted reports whether ctx was canceled because a drain timed out.
func Aborted(ctx context.Context) bool {
	return errors.Is(context.Cause(ctx), ErrDrainTimeout)
}

func (l *Lease) Release() {
	if l == nil || l.app == nil {
		return
	}
	l.once.Do(func() {
		l.app.mu.Lock()
		if l.app.active > 0 {
			l.app.active--
		}
		l.app.signal()
		l.app.mu.Unlock()
	})
}

func (a *Application) Begin() (*Lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.state != ReadyState {
		if a.state == DrainingState {
			return nil, ErrDraining
		}
		return nil, ErrNotReady
	}
	a.active++
	return &Lease{app: a}, nil
}

func (a *Application) Shutdown(ctx context.Context) error {
	a.mu.Lock()
	if a.state == StoppedState {
		a.mu.Unlock()
		return nil
	}
	if a.state != ReadyState && a.state != StartingState {
		a.mu.Unlock()
		return fmt.Errorf("invalid_lifecycle_state: %s", a.state)
	}
	a.state = DrainingState
	a.signal()
	a.mu.Unlock()
	deadline := a.config.DrainTimeout
	if deadline <= 0 {
		deadline = 5 * time.Second
	}
	drainCtx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	for {
		a.mu.Lock()
		active := a.active
		a.mu.Unlock()
		if active == 0 {
			break
		}
		select {
		case <-drainCtx.Done():
			// Admitted work outlived the drain: cancel it, give it a
			// bounded grace to stop, then close the dependencies anyway.
			a.abortWork(ErrDrainTimeout)
			a.awaitIdle(ctx, a.config.AbortGrace)
			a.closeInitialized(ctx)
			a.mu.Lock()
			a.state = StoppedState
			a.mu.Unlock()
			return ErrDrainTimeout
		case <-a.changed:
		}
	}
	closeErr := a.closeInitialized(ctx)
	a.mu.Lock()
	a.state = StoppedState
	a.mu.Unlock()
	return closeErr
}

func (a *Application) Run(ctx context.Context, signals <-chan os.Signal) error {
	if err := a.Start(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return a.Shutdown(context.Background())
	case <-signals:
		return a.Shutdown(context.Background())
	}
}

// awaitIdle waits up to grace, and no longer than ctx allows, for every
// lease to be released.
func (a *Application) awaitIdle(ctx context.Context, grace time.Duration) {
	deadline := time.NewTimer(grace)
	defer deadline.Stop()
	for {
		a.mu.Lock()
		active := a.active
		a.mu.Unlock()
		if active == 0 {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			return
		case <-a.changed:
		}
	}
}

func (a *Application) closeInitialized(ctx context.Context) error {
	a.mu.Lock()
	dependencies := append([]Dependency(nil), a.initialized...)
	a.initialized = nil
	a.mu.Unlock()
	var first error
	for index := len(dependencies) - 1; index >= 0; index-- {
		if err := dependencies[index].Close(ctx); err != nil && first == nil {
			first = fmt.Errorf("shutdown_failed:%s: %w", dependencies[index].Name, err)
		}
	}
	return first
}

func (a *Application) signal() {
	select {
	case a.changed <- struct{}{}:
	default:
	}
}
