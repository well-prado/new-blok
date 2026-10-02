package engine

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestControlBranchesAndEachPreserveOrder(t *testing.T) {
	var selected atomic.Int32
	plan := ControlPlan{Steps: []ControlStep{
		{ID: "route", Kind: ControlBranch, Branch: &BranchPlan{
			When: func(context.Context) (bool, error) { return false, nil },
			Then: []Action{{ID: "then", Run: func(context.Context) (any, error) { selected.Add(1); return "then", nil }}},
			Else: []Action{{ID: "else", Run: func(context.Context) (any, error) { selected.Add(10); return "else", nil }}},
		}},
		{ID: "items", Kind: ControlEach, Each: &EachPlan{Items: []any{0, 1, 2, 3}, Concurrency: 4, Run: func(ctx context.Context, item any, index int) (any, error) {
			time.Sleep(time.Duration(3-index) * time.Millisecond)
			return fmt.Sprintf("%d", item), nil
		}}},
	}}
	result, err := New(nil).RunControl(context.Background(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Load() != 10 {
		t.Fatalf("selected=%d", selected.Load())
	}
	got := result.State["items"].([]any)
	for index, want := range []string{"0", "1", "2", "3"} {
		if got[index] != want {
			t.Fatalf("ordered results=%v", got)
		}
	}
}

func TestControlParallelFailsFastAndCancelsSiblings(t *testing.T) {
	var canceled atomic.Bool
	plan := ControlPlan{Steps: []ControlStep{{ID: "parallel", Kind: ControlParallel, Parallel: &ParallelPlan{Actions: []Action{
		{ID: "fail", Run: func(context.Context) (any, error) { return nil, errors.New("business failure") }},
		{ID: "slow", Run: func(ctx context.Context) (any, error) { <-ctx.Done(); canceled.Store(true); return nil, ctx.Err() }},
	}}}}}
	_, err := New(nil).RunControl(context.Background(), plan)
	if err == nil || !stringsContains(err.Error(), "node_error") || !canceled.Load() {
		t.Fatalf("err=%v canceled=%v", err, canceled.Load())
	}
}

func TestControlTryCatchDoesNotCatchCancellationAndRunsFinallyOnBusinessError(t *testing.T) {
	var finalized atomic.Int32
	plan := ControlPlan{Steps: []ControlStep{{ID: "saga", Kind: ControlTry, Try: &TryPlan{
		Try:     []Action{{ID: "business", Run: func(context.Context) (any, error) { return nil, errors.New("business") }}},
		Catch:   func(context.Context, error) (any, error) { return "recovered", nil },
		Finally: []Action{{ID: "finally", Run: func(context.Context) (any, error) { finalized.Add(1); return nil, nil }}},
	}}}}
	result, err := New(nil).RunControl(context.Background(), plan)
	if err != nil || result.State["saga"] != "recovered" || finalized.Load() != 1 {
		t.Fatalf("result=%+v err=%v finalized=%d", result, err, finalized.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	plan.Steps[0].Try.Try[0].Run = func(ctx context.Context) (any, error) { return nil, ctx.Err() }
	_, err = New(nil).RunControl(ctx, plan)
	if err == nil || !stringsContains(err.Error(), "canceled") || finalized.Load() != 1 {
		t.Fatalf("cancellation err=%v finalized=%d", err, finalized.Load())
	}
}

func TestControlChildDepthIsBounded(t *testing.T) {
	plan := ControlPlan{MaxDepth: 1, Steps: []ControlStep{{ID: "child", Kind: ControlChild, Child: &ChildPlan{Run: func(context.Context, int) (any, error) { return "child", nil }}}}}
	if _, err := New(nil).RunControl(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
	plan.Steps[0].Child.Run = func(context.Context, int) (any, error) {
		return nil, &Error{Code: "child_depth_exceeded", Class: "admission"}
	}
	if _, err := New(nil).RunControl(context.Background(), plan); err == nil || !stringsContains(err.Error(), "child_depth_exceeded") {
		t.Fatalf("got %v", err)
	}
}

func stringsContains(value, part string) bool {
	for index := 0; index+len(part) <= len(value); index++ {
		if value[index:index+len(part)] == part {
			return true
		}
	}
	return false
}
