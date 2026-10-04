// Package drainprobe tests that an application's drain timeout stops the
// work its adapters admitted before the application closes its
// dependencies (#177). It is used only by tests.
package drainprobe

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
)

// Probe is the application's one dependency. It records whether any work
// admitted under the application was still running when the application
// closed it.
type Probe struct {
	working         atomic.Int32
	closed          atomic.Bool
	closedUnderWork atomic.Bool
}

// Config is an application configuration whose only dependency is the
// probe, with the given drain timeout and an abort grace long enough that a
// test notices work that ignores the abort.
func (p *Probe) Config(drain time.Duration) app.Config {
	return app.Config{DrainTimeout: drain, AbortGrace: 5 * time.Second, Dependencies: []app.Dependency{{
		Name:  "store",
		Start: func(context.Context) error { return nil },
		Close: func(context.Context) error {
			if p.working.Load() > 0 {
				p.closedUnderWork.Store(true)
			}
			p.closed.Store(true)
			return nil
		},
	}}}
}

// Start returns a started application configured by Config.
func (p *Probe) Start(t testing.TB, drain time.Duration) *app.Application {
	t.Helper()
	application, err := app.New(p.Config(drain))
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return application
}

// Closed reports whether the dependency was closed.
func (p *Probe) Closed() bool { return p.closed.Load() }

// ClosedUnderWork reports whether it was closed while held work ran.
func (p *Probe) ClosedUnderWork() bool { return p.closedUnderWork.Load() }

// Held is store work that runs until its context ends or the test gives up
// on it. It counts as running work for the probe while it blocks.
type Held struct {
	probe    *Probe
	entered  chan struct{}
	giveUp   chan struct{}
	once     sync.Once
	canceled atomic.Bool
}

// NewHeld returns held work for the probe; the test gives up on it at
// cleanup at the latest.
func NewHeld(t testing.TB, probe *Probe) *Held {
	h := &Held{probe: probe, entered: make(chan struct{}, 1), giveUp: make(chan struct{})}
	t.Cleanup(h.Release)
	return h
}

// Run blocks until ctx ends, returning its error, or the test releases it.
func (h *Held) Run(ctx context.Context) error {
	h.probe.working.Add(1)
	defer h.probe.working.Add(-1)
	select {
	case h.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		h.canceled.Store(true)
		return ctx.Err()
	case <-h.giveUp:
		return nil
	}
}

// Release gives up on the work: Run returns nil.
func (h *Held) Release() { h.once.Do(func() { close(h.giveUp) }) }

// Canceled reports whether the work's context ended while it ran.
func (h *Held) Canceled() bool { return h.canceled.Load() }

// Wait waits for the work to start.
func (h *Held) Wait(t testing.TB) {
	t.Helper()
	select {
	case <-h.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("the held work never started")
	}
}

// Shutdown shuts the application down in the background.
func Shutdown(application *app.Application) <-chan error {
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done <- application.Shutdown(ctx)
	}()
	return done
}

// Abort shuts the application down while held work runs, and checks that
// the drain timed out, the work was canceled and stopped before the store
// closed, and Shutdown did not wait out the abort grace.
func Abort(t testing.TB, application *app.Application, probe *Probe, work *Held) {
	t.Helper()
	work.Wait(t)
	began := time.Now()
	if err := <-Shutdown(application); !errors.Is(err, app.ErrDrainTimeout) {
		t.Fatalf("Shutdown: %v, want the drain timeout", err)
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("Shutdown took %s: the admitted work outlived the drain", elapsed)
	}
	if !work.Canceled() || probe.ClosedUnderWork() {
		t.Fatalf("canceled=%v closedUnderWork=%v; want the work canceled and stopped before the store closed", work.Canceled(), probe.ClosedUnderWork())
	}
}
