// Package execution is the public application composition for compiled runs.
package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	outcomes    app.RunOutcomePort
}

// OutcomeUncertainError means execution may have committed effects but its
// terminal journal transition could not be confirmed. Callers must reconcile
// RunID, not blindly retry the workflow.
type OutcomeUncertainError struct {
	RunID          string
	ExecutionError error
	PersistenceErr error
}

func (e *OutcomeUncertainError) Error() string {
	return fmt.Sprintf("execution: terminal outcome is uncertain; reconcile run %s", e.RunID)
}

func (e *OutcomeUncertainError) Unwrap() []error {
	causes := make([]error, 0, 2)
	if e.ExecutionError != nil {
		causes = append(causes, e.ExecutionError)
	}
	if e.PersistenceErr != nil {
		causes = append(causes, e.PersistenceErr)
	}
	return causes
}

func (*OutcomeUncertainError) IsUncertain() bool { return true }

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

const terminalOutcomeWriteTimeout = 5 * time.Second

// NewRunner composes an application-owned observer without making app itself
// depend on the interpreter (trigger adapters depend on app).
func NewRunner(application *app.Application, nodes map[string]node.Any) *Runner {
	var observer inspection.Observer
	var outcomes app.RunOutcomePort
	if application != nil {
		observer = application.InspectionObserver()
		outcomes = application.RunOutcomePort()
	}
	interpreter := engine.New(nodes)
	if observer != nil {
		interpreter = interpreter.WithObserver(observer)
	}
	return &Runner{application: application, engine: interpreter, inspecting: observer != nil, outcomes: outcomes}
}

func (r *Runner) Run(ctx context.Context, program contract.InternalProgram, input any, identity inspection.Invocation) (Result, error) {
	if r == nil || r.application == nil {
		return Result{}, app.ErrNotReady
	}
	if r.outcomes != nil && (identity.RunID == "" || identity.Principal == "") {
		return Result{}, errors.New("execution: trusted run and principal are required when terminal outcomes are configured")
	}
	lease, err := r.application.Begin()
	if err != nil {
		return Result{}, err
	}
	defer lease.Release()
	runCtx, unbind := lease.Bind(ctx)
	defer unbind()
	var result engine.Result
	if r.inspecting {
		result, err = r.engine.RunObserved(runCtx, program, input, identity)
	} else {
		result, err = r.engine.Run(runCtx, program, input)
	}
	if r.outcomes != nil {
		writeTimeout := terminalOutcomeWriteTimeout
		if grace := r.application.AbortGrace(); grace < writeTimeout {
			writeTimeout = grace
		}
		outcomeCtx, cancelOutcome := context.WithTimeout(lease.Context(), writeTimeout)
		outcomeErr := r.recordOutcome(outcomeCtx, identity, result, err)
		cancelOutcome()
		if outcomeErr != nil {
			err = &OutcomeUncertainError{RunID: identity.RunID, ExecutionError: err, PersistenceErr: outcomeErr}
		}
	}
	out := Result{Output: result.Output, State: result.State, Steps: make([]StepResult, len(result.Steps))}
	for i, s := range result.Steps {
		out.Steps[i] = StepResult{ID: s.ID, Executed: s.Executed, Input: s.Input, Attempt: s.Attempt, Output: s.Output, Error: s.Error, StartedAt: s.StartedAt, FinishedAt: s.FinishedAt}
	}
	return out, err
}

func (r *Runner) recordOutcome(ctx context.Context, identity inspection.Invocation, result engine.Result, runErr error) error {
	if runErr == nil {
		output, err := json.Marshal(result.Output)
		if err != nil {
			return fmt.Errorf("encode completed output: %w", err)
		}
		return r.outcomes.CompleteRun(ctx, identity, output)
	}
	var classified interface {
		ErrorCode() string
		ErrorClass() string
		IsUncertain() bool
	}
	if errors.As(runErr, &classified) {
		if classified.IsUncertain() {
			return r.outcomes.MarkRunUncertain(ctx, identity, classified.ErrorCode(), classified.ErrorClass())
		}
	}
	if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
		return r.outcomes.CancelRun(ctx, identity, "execution canceled")
	}
	code, class := "execution_failed", "workflow"
	if errors.As(runErr, &classified) {
		code, class = classified.ErrorCode(), classified.ErrorClass()
	}
	return r.outcomes.FailRun(ctx, identity, code, class)
}
