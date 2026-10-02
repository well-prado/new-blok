// Package app owns explicit application composition and lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
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
	DrainTimeout time.Duration
}

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
}

func New(config Config) (*Application, error) {
	if config.DrainTimeout <= 0 {
		config.DrainTimeout = 5 * time.Second
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
	return &Application{config: config, state: NewState, changed: make(chan struct{}, 1)}, nil
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

type Lease struct {
	app  *Application
	once sync.Once
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
