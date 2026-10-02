package engine

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func limits() AdmissionLimits {
	return AdmissionLimits{Workers: 1, QueueCapacity: 2, PerTenantActive: 1, PerTenantQueued: 2, MaxAttempts: 3, MaxChildDepth: 4}
}

func TestSchedulerSaturatesAndShutsDownIdempotently(t *testing.T) {
	scheduler, err := NewScheduler(limits())
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	first, err := scheduler.Submit(context.Background(), Request{Tenant: "a", Attempt: 1, Run: func(context.Context) (any, error) {
		close(started)
		<-release
		return "first", nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if _, err := scheduler.Submit(context.Background(), Request{Tenant: "a", Attempt: 1, Run: func(context.Context) (any, error) { return "second", nil }}); err != nil {
		t.Fatal(err)
	}
	if _, err := scheduler.Submit(context.Background(), Request{Tenant: "a", Attempt: 1, Run: func(context.Context) (any, error) { return "third", nil }}); !errors.Is(err, ErrSaturated) {
		t.Fatalf("got %v", err)
	}
	close(release)
	if result := <-first; result.Err != nil || result.Value != "first" {
		t.Fatalf("result=%+v", result)
	}
	scheduler.Shutdown()
	scheduler.Shutdown()
	if _, err := scheduler.Submit(context.Background(), Request{Tenant: "a", Attempt: 1, Run: func(context.Context) (any, error) { return nil, nil }}); !errors.Is(err, ErrClosed) {
		t.Fatalf("got %v", err)
	}
}

func TestSchedulerFairnessPreventsNoisyTenantStarvation(t *testing.T) {
	scheduler, err := NewScheduler(AdmissionLimits{Workers: 1, QueueCapacity: 8, PerTenantActive: 1, PerTenantQueued: 8, MaxAttempts: 1, MaxChildDepth: 0})
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Shutdown()
	started := make(chan struct{})
	release := make(chan struct{})
	_, err = scheduler.Submit(context.Background(), Request{Tenant: "a", Attempt: 1, Run: func(context.Context) (any, error) { close(started); <-release; return "a0", nil }})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	var orderMu sync.Mutex
	order := []string{}
	add := func(tenant, id string) <-chan AdmissionResult {
		result, submitErr := scheduler.Submit(context.Background(), Request{Tenant: tenant, Attempt: 1, Run: func(context.Context) (any, error) {
			orderMu.Lock()
			order = append(order, id)
			orderMu.Unlock()
			return id, nil
		}})
		if submitErr != nil {
			t.Fatal(submitErr)
		}
		return result
	}
	a1 := add("a", "a1")
	b0 := add("b", "b0")
	close(release)
	<-a1
	if result := <-b0; result.Err != nil {
		t.Fatal(result.Err)
	}
	orderMu.Lock()
	defer orderMu.Unlock()
	if len(order) != 2 || order[0] != "b0" {
		t.Fatalf("order=%v", order)
	}
}

func TestSchedulerCancellationRejectsQueuedAndLateResults(t *testing.T) {
	scheduler, err := NewScheduler(limits())
	if err != nil {
		t.Fatal(err)
	}
	defer scheduler.Shutdown()
	started := make(chan struct{})
	release := make(chan struct{})
	_, err = scheduler.Submit(context.Background(), Request{Tenant: "a", Attempt: 1, Run: func(context.Context) (any, error) { close(started); <-release; return "block", nil }})
	if err != nil {
		t.Fatal(err)
	}
	<-started
	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	queued, err := scheduler.Submit(queuedCtx, Request{Tenant: "b", Attempt: 1, Run: func(context.Context) (any, error) { t.Fatal("canceled queued request ran"); return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	cancelQueued()
	close(release)
	if result := <-queued; !errors.Is(result.Err, context.Canceled) {
		t.Fatalf("queued result=%+v", result)
	}
	lateCtx, cancelLate := context.WithCancel(context.Background())
	late, err := scheduler.Submit(lateCtx, Request{Tenant: "b", Attempt: 1, Run: func(context.Context) (any, error) { time.Sleep(time.Millisecond); return "late", nil }})
	if err != nil {
		t.Fatal(err)
	}
	cancelLate()
	if result := <-late; !errors.Is(result.Err, context.Canceled) || result.Value != nil {
		t.Fatalf("late result=%+v", result)
	}
}
