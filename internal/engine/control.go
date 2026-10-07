package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

type Task func(context.Context) (any, error)
type ItemTask func(context.Context, any, int) (any, error)
type Condition func(context.Context) (bool, error)

type Action struct {
	ID  string
	Run Task
}

type BranchPlan struct {
	When Condition
	Then []Action
	Else []Action
}

type EachPlan struct {
	Items       []any
	Concurrency int
	Run         ItemTask
}

type ParallelPlan struct {
	Actions []Action
}

type TryPlan struct {
	Try     []Action
	Catch   func(context.Context, error) (any, error)
	Finally []Action
}

type ChildPlan struct {
	Run func(context.Context, int) (any, error)
}

type ControlKind string

const (
	ControlAction   ControlKind = "action"
	ControlBranch   ControlKind = "branch"
	ControlEach     ControlKind = "each"
	ControlParallel ControlKind = "parallel"
	ControlTry      ControlKind = "try"
	ControlChild    ControlKind = "child"
)

type ControlStep struct {
	ID       string
	Kind     ControlKind
	Action   Task
	Branch   *BranchPlan
	Each     *EachPlan
	Parallel *ParallelPlan
	Try      *TryPlan
	Child    *ChildPlan
}

type ControlPlan struct {
	Steps    []ControlStep
	MaxSteps int
	MaxDepth int
}

type ControlResult struct {
	State map[string]any
}

func (e *Engine) RunControl(ctx context.Context, plan ControlPlan) (ControlResult, error) {
	if plan.MaxSteps == 0 {
		plan.MaxSteps = e.maxSteps
	}
	if plan.MaxDepth == 0 {
		plan.MaxDepth = 64
	}
	if len(plan.Steps) > plan.MaxSteps {
		return ControlResult{}, &Error{Code: "step_budget_exceeded", Class: "admission"}
	}
	result := ControlResult{State: map[string]any{}}
	for _, step := range plan.Steps {
		if err := ctx.Err(); err != nil {
			return result, &Error{Code: "canceled", Class: "cancellation", Step: step.ID, Err: err}
		}
		output, err := runControlStep(ctx, step, 0, plan.MaxDepth)
		if err != nil {
			return result, err
		}
		if output != nil {
			result.State[step.ID] = output
		}
	}
	return result, nil
}

func runControlStep(ctx context.Context, step ControlStep, depth, maxDepth int) (any, error) {
	if depth > maxDepth {
		return nil, &Error{Code: "child_depth_exceeded", Class: "admission", Step: step.ID}
	}
	switch step.Kind {
	case ControlAction:
		if step.Action == nil {
			return nil, fmt.Errorf("action %s is missing", step.ID)
		}
		return step.Action(ctx)
	case ControlBranch:
		if step.Branch == nil || step.Branch.When == nil {
			return nil, fmt.Errorf("branch %s is missing condition", step.ID)
		}
		selected, err := step.Branch.When(ctx)
		if err != nil {
			return nil, err
		}
		if selected {
			return runActions(ctx, step.Branch.Then)
		}
		return runActions(ctx, step.Branch.Else)
	case ControlEach:
		return runEach(ctx, step.ID, step.Each)
	case ControlParallel:
		return runParallel(ctx, step.ID, step.Parallel)
	case ControlTry:
		return runTry(ctx, step.ID, step.Try)
	case ControlChild:
		if step.Child == nil || step.Child.Run == nil {
			return nil, fmt.Errorf("child %s is missing", step.ID)
		}
		if depth >= maxDepth {
			return nil, &Error{Code: "child_depth_exceeded", Class: "admission", Step: step.ID}
		}
		return step.Child.Run(ctx, depth+1)
	default:
		return nil, &Error{Code: "unsupported_control", Class: "configuration", Step: step.ID}
	}
}

func runActions(ctx context.Context, actions []Action) (any, error) {
	var output any
	for _, action := range actions {
		if err := ctx.Err(); err != nil {
			return nil, &Error{Code: "canceled", Class: "cancellation", Step: action.ID, Err: err}
		}
		if action.Run == nil {
			return nil, fmt.Errorf("action %s is missing", action.ID)
		}
		var err error
		output, err = action.Run(ctx)
		if err != nil {
			return nil, classify(action.ID, err)
		}
	}
	return output, nil
}

func runEach(ctx context.Context, id string, plan *EachPlan) (any, error) {
	if plan == nil || plan.Run == nil {
		return nil, fmt.Errorf("each %s is missing body", id)
	}
	if plan.Concurrency < 1 || plan.Concurrency > 1024 {
		return nil, &Error{Code: "concurrency_limit", Class: "admission", Step: id}
	}
	if len(plan.Items) == 0 {
		return []any{}, nil
	}
	workers := plan.Concurrency
	if workers > len(plan.Items) {
		workers = len(plan.Items)
	}
	derived, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]any, len(plan.Items))
	jobs := make(chan int)
	var first atomic.Pointer[Error]
	var group sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for index := range jobs {
				if derived.Err() != nil {
					continue
				}
				output, err := plan.Run(derived, plan.Items[index], index)
				if err != nil {
					wrapped := classify(id, err).(*Error)
					if first.CompareAndSwap(nil, wrapped) {
						cancel()
					}
					continue
				}
				results[index] = output
			}
		}()
	}
	for index := range plan.Items {
		if derived.Err() != nil {
			break
		}
		jobs <- index
	}
	close(jobs)
	group.Wait()
	if err := first.Load(); err != nil {
		return nil, err
	}
	return results, nil
}

func runParallel(ctx context.Context, id string, plan *ParallelPlan) (any, error) {
	if plan == nil || len(plan.Actions) == 0 {
		return nil, fmt.Errorf("parallel %s is empty", id)
	}
	derived, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]any, len(plan.Actions))
	var first atomic.Pointer[Error]
	var group sync.WaitGroup
	for index, action := range plan.Actions {
		group.Add(1)
		go func(index int, action Action) {
			defer group.Done()
			if action.Run == nil {
				first.CompareAndSwap(nil, &Error{Code: "missing_action", Class: "configuration", Step: action.ID})
				cancel()
				return
			}
			output, err := action.Run(derived)
			if err != nil {
				if first.CompareAndSwap(nil, classify(action.ID, err).(*Error)) {
					cancel()
				}
				return
			}
			results[index] = output
		}(index, action)
	}
	group.Wait()
	if err := first.Load(); err != nil {
		return nil, err
	}
	return results, nil
}

func runTry(ctx context.Context, id string, plan *TryPlan) (any, error) {
	if plan == nil {
		return nil, fmt.Errorf("try %s is missing", id)
	}
	output, err := runActions(ctx, plan.Try)
	if err != nil && ctx.Err() == nil && plan.Catch != nil {
		output, err = plan.Catch(ctx, err)
	}
	// Only the caller's cancellation of the run skips finally. When a
	// construct around this one canceled it because a sibling failed
	// (fail-fast), finally still runs, canceled only if the run is.
	run, ok := ctx.Value(runContextKey{}).(context.Context)
	if !ok {
		run = ctx
	}
	if run.Err() == nil {
		finallyCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		stop := context.AfterFunc(run, cancel)
		finallyOutput, finallyErr := runActions(finallyCtx, plan.Finally)
		stop()
		cancel()
		if finallyErr != nil {
			return nil, finallyErr
		} else if finallyOutput != nil && err == nil {
			output = finallyOutput
		}
	}
	if err != nil {
		return nil, classify(id, err)
	}
	return output, nil
}
