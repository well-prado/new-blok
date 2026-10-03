// Package engine executes compiled native programs in bounded memory mode.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/internal/value"
	"github.com/well-prado/new-blok/node"
)

type Error struct {
	Code  string
	Class string
	Step  string
	Err   error
}

func (e *Error) Error() string {
	if e.Err == nil {
		return e.Code + ": " + e.Class
	}
	return e.Code + ": " + e.Err.Error()
}
func (e *Error) Unwrap() error { return e.Err }

// ErrorCode and ErrorClass expose the stable classification to adapters
// without requiring them to import the engine.
func (e *Error) ErrorCode() string  { return e.Code }
func (e *Error) ErrorClass() string { return e.Class }

type Result struct {
	Output any
	State  map[string]any
	Steps  []StepResult
}

type StepResult struct {
	ID         string
	Executed   bool
	Input      any
	Attempt    int
	Output     any
	Error      error
	StartedAt  time.Time
	FinishedAt time.Time
}

type Engine struct {
	nodes    map[string]node.Any
	maxSteps int
	observer inspection.Observer
}

func New(nodes map[string]node.Any) *Engine {
	registered := make(map[string]node.Any, len(nodes))
	for name, definition := range nodes {
		registered[name] = definition
	}
	return &Engine{nodes: registered, maxSteps: 10000}
}

func (e *Engine) WithMaxSteps(max int) *Engine {
	copy := *e
	if max > 0 {
		copy.maxSteps = max
	}
	return &copy
}

func (e *Engine) WithObserver(observer inspection.Observer) *Engine {
	copy := *e
	copy.observer = observer
	return &copy
}

func (e *Engine) Run(ctx context.Context, program contract.InternalProgram, input any) (Result, error) {
	return e.RunObserved(ctx, program, input, inspection.Invocation{})
}

// RunObserved executes the same production interpreter as Run and emits
// read-only events tied to trusted invocation metadata.
func (e *Engine) RunObserved(ctx context.Context, program contract.InternalProgram, input any, invocation inspection.Invocation) (result Result, runErr error) {
	if e == nil {
		return Result{}, &Error{Code: "nil_engine", Class: "configuration"}
	}
	if len(program.Instructions) > e.maxSteps {
		return Result{}, &Error{Code: "step_budget_exceeded", Class: "admission"}
	}
	emit := func(event inspection.Event) {
		if e.observer == nil || invocation.RunID == "" {
			return
		}
		event.RunID, event.Principal, event.Workflow = invocation.RunID, invocation.Principal, program.WorkflowID
		event.ParentRun, event.ParentStep = invocation.ParentRun, invocation.ParentStep
		event.At = time.Now().UTC()
		e.observer.Observe(event)
	}
	inputJSON, _ := json.Marshal(input)
	emit(inspection.Event{Kind: inspection.RunStarted, Input: inputJSON})
	defer func() {
		terminal := inspection.Event{Kind: inspection.RunCompleted}
		if runErr != nil {
			terminal.Kind = inspection.RunFailed
			var classified *Error
			if errors.As(runErr, &classified) {
				terminal.ErrorCode, terminal.ErrorClass = classified.Code, classified.Class
				if strings.EqualFold(classified.Class, string(inspection.StatusUncertain)) {
					terminal.Kind = inspection.RunUncertain
				}
			}
			if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
				terminal.Kind = inspection.RunCanceled
			}
		} else {
			terminal.Output, _ = json.Marshal(result.Output)
		}
		emit(terminal)
	}()
	state := make(map[string]any)
	result = Result{State: state}
	appendStep := func(step StepResult) {
		result.Steps = append(result.Steps, step)
		event := inspection.Event{Kind: inspection.StepCompleted, StepID: step.ID, Attempt: step.Attempt, Input: marshalObservation(step.Input), Output: marshalObservation(step.Output)}
		if step.Error != nil {
			event.Kind = inspection.StepFailed
			var classified *Error
			if errors.As(step.Error, &classified) {
				event.ErrorCode, event.ErrorClass = classified.Code, classified.Class
				if strings.EqualFold(classified.Class, string(inspection.StatusUncertain)) {
					event.Kind = inspection.StepUncertain
				}
			}
			if errors.Is(step.Error, context.Canceled) || errors.Is(step.Error, context.DeadlineExceeded) {
				event.Kind = inspection.StepCanceled
			}
		}
		event.At = step.FinishedAt
		emit(event)
	}
	for _, instruction := range program.Instructions {
		if err := ctx.Err(); err != nil {
			return result, &Error{Code: "canceled", Class: "cancellation", Step: instruction.ID, Err: err}
		}
		step := StepResult{ID: instruction.ID}
		switch instruction.Kind {
		case "call":
			definition, ok := e.nodes[instruction.Node]
			if !ok {
				step.Error = &Error{Code: "unknown_node", Class: "configuration", Step: instruction.ID}
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return result, step.Error
			}
			callInput, err := resolveCallInput(state, instruction, input)
			if err != nil {
				step.Error = &Error{Code: "invalid_input_reference", Class: "validation", Step: instruction.ID, Err: err}
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return result, step.Error
			}
			step.Input = callInput
			step.Attempt = 1
			step.StartedAt = time.Now().UTC()
			emit(inspection.Event{Kind: inspection.StepProcessing, StepID: step.ID, Attempt: step.Attempt, Input: marshalObservation(step.Input)})
			if err := validateSchema(definition.Descriptor().InputSchema, callInput); err != nil {
				step.Error = &Error{Code: "invalid_input", Class: "validation", Step: instruction.ID, Err: err}
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return result, step.Error
			}
			output, err := definition.Invoke(ctx, callInput)
			step.Executed = true
			if err != nil {
				step.Error = classify(instruction.ID, err)
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return result, step.Error
			}
			if err := validateSchema(definition.Descriptor().OutputSchema, output); err != nil {
				step.Error = &Error{Code: "invalid_output", Class: "validation", Step: instruction.ID, Err: err}
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return result, step.Error
			}
			committed, err := value.Clone(output)
			if err != nil {
				step.Error = &Error{Code: "output_ownership", Class: "validation", Step: instruction.ID, Err: err}
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return result, step.Error
			}
			state[instruction.ID] = committed
			step.Output = committed
		case "output":
			step.StartedAt = time.Now().UTC()
			step.Attempt = 1
			emit(inspection.Event{Kind: inspection.StepProcessing, StepID: step.ID, Attempt: step.Attempt})
			output, err := resolveOutput(state, instruction.References)
			if err != nil {
				step.Error = &Error{Code: "invalid_output_reference", Class: "validation", Step: instruction.ID, Err: err}
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return result, step.Error
			}
			result.Output = output
			step.Output = output
		default:
			step.Error = &Error{Code: "unsupported_instruction", Class: "configuration", Step: instruction.ID}
			step.FinishedAt = time.Now().UTC()
			appendStep(step)
			return result, step.Error
		}
		step.FinishedAt = time.Now().UTC()
		appendStep(step)
	}
	return result, nil
}

func marshalObservation(value any) json.RawMessage {
	if value == nil {
		return nil
	}
	data, _ := json.Marshal(value)
	return data
}

func classify(step string, err error) error {
	var existing *Error
	if errors.As(err, &existing) {
		return existing
	}
	if err == context.Canceled || err == context.DeadlineExceeded {
		return &Error{Code: "canceled", Class: "cancellation", Step: step, Err: err}
	}
	var nodeError *node.Error
	if ok := errorAs(err, &nodeError); ok && nodeError.Code == "node_panic" {
		return &Error{Code: "node_panic", Class: "panic", Step: step, Err: err}
	}
	var domain *node.DomainError
	domain, ok := node.AsDomainError(err)
	if ok {
		return &Error{Code: domain.Code, Class: domain.Class, Step: step, Err: err}
	}
	return &Error{Code: "node_error", Class: "failure", Step: step, Err: err}
}

func resolveCallInput(state map[string]any, instruction contract.InternalInstruction, input any) (any, error) {
	if len(instruction.References) == 0 {
		return input, nil
	}
	return resolveReference(state, instruction.References[0])
}

func resolveOutput(state map[string]any, references []contract.Reference) (any, error) {
	if len(references) == 0 {
		return nil, fmt.Errorf("output requires a reference")
	}
	return resolveReference(state, references[0])
}

func resolveReference(state map[string]any, reference contract.Reference) (any, error) {
	current, ok := state[reference.Step]
	if !ok {
		return nil, fmt.Errorf("step %q has no committed output", reference.Step)
	}
	for _, path := range reference.Path {
		var err error
		current, err = field(current, path)
		if err != nil {
			return nil, err
		}
	}
	return current, nil
}

func field(input any, name string) (any, error) {
	if input == nil {
		return nil, fmt.Errorf("cannot read %q from null", name)
	}
	value := reflect.ValueOf(input)
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return nil, fmt.Errorf("cannot read %q from null", name)
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Map {
		key := reflect.ValueOf(name)
		found := value.MapIndex(key)
		if found.IsValid() {
			return found.Interface(), nil
		}
		return nil, fmt.Errorf("field %q is missing", name)
	}
	if value.Kind() != reflect.Struct {
		return nil, fmt.Errorf("cannot read %q from %s", name, value.Type())
	}
	typ := value.Type()
	for index := 0; index < typ.NumField(); index++ {
		fieldType := typ.Field(index)
		jsonName := strings.Split(fieldType.Tag.Get("json"), ",")[0]
		if jsonName == "" {
			jsonName = fieldType.Name
		}
		if jsonName == name || fieldType.Name == name {
			return value.Field(index).Interface(), nil
		}
	}
	return nil, fmt.Errorf("field %q is missing", name)
}

func validateSchema(raw []byte, value any) error {
	parsed, err := schema.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Type == "object" && len(parsed.Properties) == 0 && len(parsed.Required) == 0 {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = parsed.Normalize(data)
	return err
}

func errorAs(err error, target **node.Error) bool {
	if err == nil {
		return false
	}
	value, ok := err.(*node.Error)
	if ok {
		*target = value
		return true
	}
	return false
}
