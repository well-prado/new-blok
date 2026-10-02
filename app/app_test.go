package app

import (
	"context"
	"errors"
	"os"
	"reflect"
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
