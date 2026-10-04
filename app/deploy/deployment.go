// Package deploy provides optional HTTP deployment endpoints and signal drain.
package deploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/well-prado/new-blok/app"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/well-prado/new-blok/contract/deployment"
)

// DeploymentChecks must verify compatibility, not merely process presence.
// Store checks include retained artifact/checkpoint compatibility; worker checks
// include authenticated negotiation and catalog compatibility. Probe errors are
// deliberately excluded from responses because adapters may contain credentials.
// Checks must honor context cancellation and be safe for concurrent calls.
type DeploymentChecks struct {
	Artifact func(context.Context) error
	Store    func(context.Context) error
	Worker   func(context.Context) error
	Secret   func(string) bool
}

type Deployment struct {
	application *app.Application
	config      deployment.Config
	checks      DeploymentChecks
	limiter     *deployment.Limiter
	handler     http.Handler
	rejected    atomic.Uint64
	unrequested unrequestedConns
}

// unrequestedConns tracks accepted connections that have not yet delivered a
// request (http.StateNew). They hold no admitted work, yet http.Server.Shutdown
// keeps them open for five seconds, so a client's spare or speculative
// connection would otherwise hold the drain until DrainTimeout. Once admission
// is closed they are closed instead. The mutex is shared with the server's
// synchronous ConnState hook: a connection is either still StateNew while
// drain holds the lock, or it has become StateActive first and its request
// meets the closed limiter. Admitted work is never cut short.
type unrequestedConns struct {
	mu       sync.Mutex
	draining bool
	conns    map[net.Conn]struct{}
}

func (u *unrequestedConns) track(c net.Conn, state http.ConnState) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if state != http.StateNew {
		delete(u.conns, c)
		return
	}
	if u.draining {
		_ = c.Close()
		return
	}
	if u.conns == nil {
		u.conns = make(map[net.Conn]struct{})
	}
	u.conns[c] = struct{}{}
}

// drain must run after the limiter stops admitting work; use closeAdmission.
func (u *unrequestedConns) drain() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.draining = true
	for c := range u.conns {
		_ = c.Close()
	}
	clear(u.conns)
}

// closeAdmission stops admitting work, then closes request-less connections.
// The order is the safety argument in unrequestedConns.
func (d *Deployment) closeAdmission() {
	d.limiter.BeginDrain()
	d.unrequested.drain()
}

func NewDeployment(a *app.Application, c deployment.Config, checks DeploymentChecks, handler http.Handler) (*Deployment, error) {
	if a == nil || handler == nil || checks.Artifact == nil {
		return nil, deployment.ErrInvalid
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	c.RequiredSecrets = append([]string(nil), c.RequiredSecrets...)
	if checks.Secret == nil {
		checks.Secret = func(ref string) bool { v, ok := os.LookupEnv(ref); return ok && v != "" }
	}
	l, _ := deployment.NewLimiter(c.MaxAdmission)
	return &Deployment{application: a, config: c, checks: checks, limiter: l, handler: handler}, nil
}

func (d *Deployment) status(ctx context.Context) deployment.Status {
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	deps := deployment.Dependencies{Secrets: make(map[string]bool)}
	// Dependency handles are published when Start transitions to Ready. Avoid
	// probing partially initialized resources while startup is still running.
	initialized := d.application.Ready()
	deps.Artifact = initialized && d.checks.Artifact(ctx) == nil
	if initialized && d.config.StoreRequired && d.checks.Store != nil {
		deps.Store = d.checks.Store(ctx) == nil
	}
	if initialized && d.config.WorkerRequired && d.checks.Worker != nil {
		deps.Worker = d.checks.Worker(ctx) == nil
	}
	for _, ref := range d.config.RequiredSecrets {
		deps.Secrets[ref] = d.checks.Secret(ref)
	}
	active, draining := d.limiter.Snapshot()
	return deployment.StatusFor(d.config, deps, active, draining)
}

// ServeHTTP reserves operational paths. Probes bypass admission so saturation
// remains observable. There is no waiting admission queue: excess work gets 503.
func (d *Deployment) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" || r.URL.Path == "/metrics" {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		if r.URL.Path == "/healthz" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"health":true}`))
			return
		}
		s := d.status(r.Context())
		if r.URL.Path == "/metrics" {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			ready, draining := 0, 0
			if s.Ready {
				ready = 1
			}
			if s.Draining {
				draining = 1
			}
			_, _ = fmt.Fprintf(w, "blok_ready %d\nblok_active %d\nblok_draining %d\nblok_admission_rejected_total %d\n", ready, s.Active, draining, d.rejected.Load())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !s.Ready {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(s)
		return
	}
	release, err := d.limiter.Admit()
	if err != nil {
		d.reject(w)
		return
	}
	defer release()
	lease, err := d.application.Begin()
	if err != nil {
		d.reject(w)
		return
	}
	defer lease.Release()
	ctx, unbind := lease.Bind(r.Context())
	defer unbind()
	r = r.WithContext(ctx)
	if !d.status(ctx).Ready {
		d.reject(w)
		return
	}
	d.handler.ServeHTTP(w, r)
}

func (d *Deployment) reject(w http.ResponseWriter) {
	d.rejected.Add(1)
	w.Header().Set("Retry-After", "1")
	http.Error(w, "deployment unavailable", http.StatusServiceUnavailable)
}

// Run owns the configured listener and signal drain. DrainTimeout bounds the
// HTTP drain. If it expires, Run cancels request contexts, closes connections,
// then gives canceled handlers the application's AbortGrace to release their
// leases before dependencies close. Dependencies must honor the supplied close
// context.
func (d *Deployment) Run(ctx context.Context, signals <-chan os.Signal) error {
	if err := d.application.Start(ctx); err != nil {
		return err
	}
	cleanup := func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), d.config.DrainTimeout)
		defer cancel()
		_ = d.application.Shutdown(closeCtx)
	}
	// The startup probe judges the dependencies, not the shutdown signal: a
	// cancellation that arrives now is handled by the drain below, so it must
	// not fail the probe and turn a clean stop into ErrNotReady.
	if !d.status(context.WithoutCancel(ctx)).Ready {
		cleanup()
		return deployment.ErrNotReady
	}
	listener, err := net.Listen("tcp", d.config.ListenerAddress)
	if err != nil {
		cleanup()
		return errors.New("deployment: listener unavailable")
	}
	workCtx, cancelWork := context.WithCancelCause(context.Background())
	defer cancelWork(context.Canceled)
	// HTTP/1 only: no TLS, so no HTTP/2, whose connections never report
	// StateActive and would look request-less to unrequestedConns.
	server := &http.Server{Handler: d, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10, BaseContext: func(net.Listener) context.Context { return workCtx }, ConnState: d.unrequested.track}
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	var serveErr error
	select {
	case <-ctx.Done():
	case <-signals:
	case serveErr = <-served:
	}
	d.closeAdmission()
	drainCtx, cancel := context.WithTimeout(context.Background(), d.config.DrainTimeout)
	defer cancel()
	shutdownErr := server.Shutdown(drainCtx)
	if shutdownErr != nil {
		cancelWork(app.ErrDrainTimeout)
		_ = server.Close()
	}
	var closeErr error
	if shutdownErr != nil {
		// The listener's drain deadline has already expired. Keep the post-cancel
		// wait bounded by the application's configured abort grace instead of
		// passing an expired context that would close dependencies immediately.
		closeCtx, cancelClose := context.WithTimeout(context.Background(), d.application.AbortGrace())
		closeErr = d.application.Shutdown(closeCtx)
		cancelClose()
	} else {
		closeErr = d.application.Shutdown(drainCtx)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.New("deployment: listener failed")
	}
	if shutdownErr != nil {
		return app.ErrDrainTimeout
	}
	return closeErr
}
