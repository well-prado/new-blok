package journal

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
)

// The journal's transactions run only framework code, except for its two
// test-only commit hooks and the caller's RetainedArtifacts visitor. A panic
// in either must not leave its transaction open (#267): the store rolls back
// every transaction whose callback does not return.

const journalPanicBusyTimeout = 2 * time.Second

type journalPanic struct{ name string }

func recoverFrom(fn func()) (recovered any) {
	defer func() { recovered = recover() }()
	fn()
	return nil
}

// TestPanickingTransitionReleasesTheWriteLock: a transition whose commit
// hook panics, after its writes and before COMMIT, must give the write lock
// back before the panic reaches the caller, with its own value. Before #267
// every later journal write on the file, from any handle, failed busy.
func TestPanickingTransitionReleasesTheWriteLock(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "journal.db")
	backend := sqlite.Backend{BusyTimeout: journalPanicBusyTimeout}
	database, err := backend.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	value := &journalPanic{name: "admission hook bug"}
	var armed atomic.Bool
	journal, err := New(ctx, database, Config{Hooks: Hooks{BeforeCommit: func(name string) {
		if name == "admission" && armed.Load() {
			panic(value)
		}
	}}})
	if err != nil {
		t.Fatal(err)
	}
	request := AdmissionRequest{RequestKey: "order-1", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee"}`)}
	armed.Store(true)
	if recovered := recoverFrom(func() { _, _ = journal.Admit(ctx, request) }); recovered != value {
		t.Fatalf("the caller recovered %#v; want the hook's own panic value", recovered)
	}
	armed.Store(false)
	otherDatabase, err := backend.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer otherDatabase.Close()
	other, err := New(ctx, otherDatabase, Config{})
	if err != nil {
		t.Fatal(err)
	}
	second := request
	second.RequestKey = "order-2"
	begin := time.Now()
	if _, err := other.Admit(ctx, second); err != nil || time.Since(begin) > journalPanicBusyTimeout/4 {
		t.Fatalf("an admission from another handle after the panic took %v (err %v); want it within %v", time.Since(begin), err, journalPanicBusyTimeout/4)
	}
	// The panicked admission was rolled back: the same request is new.
	if admission, err := journal.Admit(ctx, request); err != nil || !admission.Accepted {
		t.Fatalf("readmitting the panicked request: accepted=%v err=%v; want it accepted as new", admission.Accepted, err)
	}
}

// TestPanickingInventoryVisitorReleasesItsConnection: RetainedArtifacts
// runs its caller's visitor inside a read transaction. A visitor that panics
// must not keep that transaction: before #267 each one kept a pooled
// connection checked out, and once the pool (8 connections) was spent every
// journal call waited for a connection forever. The panicking calls use a
// context that is never canceled, as callers that recover panics typically
// do; database/sql would otherwise roll the transaction back on cancel.
func TestPanickingInventoryVisitorReleasesItsConnection(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{BusyTimeout: journalPanicBusyTimeout}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	journal, err := New(ctx, database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	request := AdmissionRequest{RequestKey: "order-1", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee"}`)}
	if _, err := journal.Admit(ctx, request); err != nil {
		t.Fatal(err)
	}
	value := &journalPanic{name: "visitor bug"}
	for call := range 16 {
		// A leaked connection shows as a call that never starts: bound each
		// one from outside, without a context that would roll back on cancel.
		recovered := make(chan any, 1)
		go func() {
			recovered <- recoverFrom(func() {
				_ = journal.RetainedArtifacts(ctx, func(RetainedArtifact) error { panic(value) })
			})
		}()
		select {
		case got := <-recovered:
			if got != value {
				t.Fatalf("the caller recovered %#v; want the visitor's own panic value", got)
			}
		case <-time.After(journalPanicBusyTimeout):
			t.Fatalf("RetainedArtifacts call %d did not finish within %v: the connections of the earlier panicking visitors are still checked out", call+1, journalPanicBusyTimeout)
		}
	}
	bounded, cancel := context.WithTimeout(ctx, journalPanicBusyTimeout)
	defer cancel()
	second := request
	second.RequestKey = "order-2"
	begin := time.Now()
	if _, err := journal.Admit(bounded, second); err != nil || time.Since(begin) > journalPanicBusyTimeout/4 {
		t.Fatalf("an admission after 16 panicking visitors took %v (err %v); want it within %v", time.Since(begin), err, journalPanicBusyTimeout/4)
	}
}
