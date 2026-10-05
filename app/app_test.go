package app

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestApplicationRejectsDuplicateAndMissingComposition(t *testing.T) {
	if _, err := New(Config{Workflows: []Workflow{{Name: "quote"}, {Name: "quote"}}}); err == nil {
		t.Fatal("duplicate workflow accepted")
	}
	if _, err := New(Config{Workflows: []Workflow{{Name: "quote"}}, Routes: []Route{{Method: "GET", Path: "/quote", Workflow: "missing"}}}); err == nil {
		t.Fatal("missing route workflow accepted")
	}
	if _, err := New(Config{Workflows: []Workflow{{Name: "quote"}}, Routes: []Route{{Method: "GET", Path: "/quote", Workflow: "quote"}, {Method: "GET", Path: "/quote", Workflow: "quote"}}}); err == nil {
		t.Fatal("duplicate route accepted")
	}
}

func TestPartialStartupClosesInitializedDependenciesInReverseOrder(t *testing.T) {
	var events []string
	add := func(event string) { events = append(events, event) }
	application, err := New(Config{Dependencies: []Dependency{
		{Name: "first", Start: func(context.Context) error { add("start:first"); return nil }, Close: func(context.Context) error { add("close:first"); return nil }},
		{Name: "second", Start: func(context.Context) error { add("start:second"); return errors.New("boom") }, Close: func(context.Context) error { add("close:second"); return nil }},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err == nil {
		t.Fatal("startup succeeded")
	}
	if !reflect.DeepEqual(events, []string{"start:first", "start:second", "close:first"}) {
		t.Fatalf("events=%v", events)
	}
	if application.Ready() || application.State() != StoppedState {
		t.Fatalf("state=%s", application.State())
	}
}

func TestShutdownDrainsAndStopsAdmission(t *testing.T) {
	closed := false
	application, err := New(Config{DrainTimeout: time.Second, Dependencies: []Dependency{{Name: "worker", Start: func(context.Context) error { return nil }, Close: func(context.Context) error { closed = true; return nil }}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := application.Begin()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- application.Shutdown(context.Background()) }()
	select {
	case <-done:
		t.Fatal("shutdown did not drain")
	case <-time.After(10 * time.Millisecond):
	}
	if _, err := application.Begin(); !errors.Is(err, ErrDraining) {
		t.Fatalf("begin during drain=%v", err)
	}
	lease.Release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !closed || application.State() != StoppedState {
		t.Fatalf("closed=%v state=%s", closed, application.State())
	}
}

func TestShutdownTimeoutStillClosesAndSignalsRun(t *testing.T) {
	application, err := New(Config{DrainTimeout: 10 * time.Millisecond, Dependencies: []Dependency{{Name: "worker", Start: func(context.Context) error { return nil }, Close: func(context.Context) error { return nil }}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, _ := application.Begin()
	if err := application.Shutdown(context.Background()); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("got %v", err)
	}
	lease.Release()
	application2, _ := New(Config{Dependencies: []Dependency{{Name: "worker", Start: func(context.Context) error { return nil }, Close: func(context.Context) error { return nil }}}})
	signals := make(chan os.Signal, 1)
	done := make(chan error, 1)
	go func() { done <- application2.Run(context.Background(), signals) }()
	for !application2.Ready() {
		time.Sleep(time.Millisecond)
	}
	signals <- os.Interrupt
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

// TestDrainTimeoutCancelsAdmittedWorkBeforeClosing: work a lease admitted
// that outlives the drain is canceled with ErrDrainTimeout as its cause,
// and the application closes its dependencies only after that work has
// stopped (#177).
func TestDrainTimeoutCancelsAdmittedWorkBeforeClosing(t *testing.T) {
	var mu sync.Mutex
	var order []string
	note := func(event string) { mu.Lock(); order = append(order, event); mu.Unlock() }
	application, err := New(Config{DrainTimeout: 100 * time.Millisecond, AbortGrace: 5 * time.Second, Dependencies: []Dependency{{
		Name:  "store",
		Start: func(context.Context) error { return nil },
		Close: func(context.Context) error { note("store closed"); return nil },
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := application.Begin()
	if err != nil {
		t.Fatal(err)
	}
	stopped := make(chan bool, 1)
	go func() {
		<-lease.Context().Done()
		stopped <- Aborted(lease.Context())
		note("work stopped")
		lease.Release()
	}()
	if err := application.Shutdown(context.Background()); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("shutdown returned %v; want ErrDrainTimeout", err)
	}
	if !<-stopped {
		t.Fatal("the work was not canceled by the drain timeout")
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(order, ", ") != "work stopped, store closed" {
		t.Fatalf("order: %v; the store closed under running work", order)
	}
}

// TestAbortGraceBoundsStubbornWork: work that ignores the cancellation
// cannot hold the application open past DrainTimeout plus AbortGrace.
func TestAbortGraceBoundsStubbornWork(t *testing.T) {
	application, err := New(Config{DrainTimeout: 50 * time.Millisecond, AbortGrace: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := application.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	begin := time.Now()
	if err := application.Shutdown(context.Background()); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("shutdown returned %v", err)
	}
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("shutdown waited %v for work that ignores cancellation", elapsed)
	}
	if application.State() != StoppedState {
		t.Fatalf("state %s", application.State())
	}
}

// TestBindAfterAbortIsAlreadyCanceled: work bound after the drain timed out
// starts canceled, not some time later (#177).
func TestBindAfterAbortIsAlreadyCanceled(t *testing.T) {
	application, err := New(Config{DrainTimeout: 10 * time.Millisecond, AbortGrace: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := application.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if err := application.Shutdown(context.Background()); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("shutdown: %v", err)
	}
	for range 1000 {
		ctx, unbind := lease.Bind(context.Background())
		if !Aborted(ctx) {
			unbind()
			t.Fatal("a context bound after the abort was not yet canceled")
		}
		unbind()
	}
}

// TestShutdownDeadlineBoundsTheAbortGrace: the caller's deadline bounds the
// grace too; Shutdown never outlives its own ctx by the grace (#177).
func TestShutdownDeadlineBoundsTheAbortGrace(t *testing.T) {
	application, err := New(Config{DrainTimeout: 5 * time.Second, AbortGrace: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	lease, err := application.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	begin := time.Now()
	if err := application.Shutdown(ctx); !errors.Is(err, ErrDrainTimeout) {
		t.Fatalf("shutdown returned %v", err)
	}
	if elapsed := time.Since(begin); elapsed > time.Second {
		t.Fatalf("shutdown took %v with a 100ms deadline", elapsed)
	}
}
