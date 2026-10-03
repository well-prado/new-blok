// Package execution is the public application composition for compiled runs.
package execution

import (
	"context"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type Runner struct {
	application *app.Application
	engine      *engine.Engine
	inspecting  bool
}
type Result struct {
	Output any
	State  map[string]any
	Steps  []StepResult
}
type StepResult struct {
	ID                    string
	Executed              bool
	Input                 any
	Attempt               int
	Output                any
	Error                 error
	StartedAt, FinishedAt time.Time
}

// NewRunner composes an application-owned observer without making app itself
// depend on the interpreter (trigger adapters depend on app).
func NewRunner(application *app.Application, nodes map[string]node.Any) *Runner {
	var observer inspection.Observer
	if application != nil {
		observer = application.InspectionObserver()
	}
	interpreter := engine.New(nodes)
	if observer != nil {
		interpreter = interpreter.WithObserver(observer)
	}
	return &Runner{application: application, engine: interpreter, inspecting: observer != nil}
}

func (r *Runner) Run(ctx context.Context, program contract.InternalProgram, input any, identity inspection.Invocation) (Result, error) {
	if r == nil || r.application == nil {
		return Result{}, app.ErrNotReady
	}
	lease, err := r.application.Begin()
	if err != nil {
		return Result{}, err
	}
	defer lease.Release()
	var result engine.Result
	if r.inspecting {
		result, err = r.engine.RunObserved(ctx, program, input, identity)
	} else {
		result, err = r.engine.Run(ctx, program, input)
	}
	out := Result{Output: result.Output, State: result.State, Steps: make([]StepResult, len(result.Steps))}
	for i, s := range result.Steps {
		out.Steps[i] = StepResult{ID: s.ID, Executed: s.Executed, Input: s.Input, Attempt: s.Attempt, Output: s.Output, Error: s.Error, StartedAt: s.StartedAt, FinishedAt: s.FinishedAt}
	}
	return out, err
}
