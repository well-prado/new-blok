// Package engine executes compiled native programs in bounded memory mode.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/capacity"
	"github.com/well-prado/new-blok/contract/schema"
	"github.com/well-prado/new-blok/internal/value"
	"github.com/well-prado/new-blok/node"
)

type Error struct {
	Code      string
	Class     string
	Step      string
	Err       error
	Uncertain bool
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
func (e *Error) IsUncertain() bool  { return e != nil && e.Uncertain }

type Result struct {
	Output any
	State  map[string]any
	Steps  []StepResult
}

type StepResult struct {
	ID       string
	Executed bool
	Input    any
	Attempt  int
	Output   any
	Error    error
}

type Engine struct {
	nodes    map[string]node.Any
	maxSteps int
}

// StepJournal is the execution boundary used by durable runtimes. Load returns
// a committed step output when one exists. Begin must durably record dispatch
// before an effectful node is invoked; Complete must commit the output before
// the next instruction runs. Fail must preserve uncertainty for effectful
// calls whose external outcome cannot be established. Implementations live
// outside the engine, so the engine has no storage or coordination dependency.
type StepJournal interface {
	VerifyRun(context.Context, string, string, string) error
	Load(context.Context, StepIdentity) (json.RawMessage, bool, error)
	Begin(context.Context, StepIdentity, json.RawMessage, []string) (StepAttempt, error)
	Complete(context.Context, StepAttempt, json.RawMessage) error
	Fail(context.Context, StepAttempt, []string, error) error
}

// StepIdentity binds a checkpoint to the exact run artifact and resolved
// input. OperationKey is stable across retries; each Begin returns a distinct
// AttemptID so a stale result cannot overwrite a later attempt.
type StepIdentity struct {
	RunID          string
	ArtifactDigest string
	StepID         string
	InputDigest    string
	OperationKey   string
}

type StepAttempt struct {
	Identity  StepIdentity
	AttemptID string
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

func (e *Engine) Run(ctx context.Context, program contract.InternalProgram, input any) (Result, error) {
	return e.RunJournaled(ctx, program, input, "", nil)
}

// RunJournaled executes a program through a caller-owned durable step journal.
// A blank runID is valid only when journal is nil. Completed step outputs are
// restored and never invoked again; a journal error fails the run closed.
func (e *Engine) RunJournaled(ctx context.Context, program contract.InternalProgram, input any, runID string, journal StepJournal) (Result, error) {
	if e == nil {
		return Result{}, &Error{Code: "nil_engine", Class: "configuration"}
	}
	if journal != nil && (runID == "" || program.Digest == "") {
		return Result{}, &Error{Code: "missing_run_identity", Class: "configuration"}
	}
	if len(program.Instructions) > e.maxSteps {
		return Result{}, &Error{Code: "step_budget_exceeded", Class: "admission"}
	}
	if journal != nil {
		encodedInput, err := json.Marshal(input)
		if err != nil {
			return Result{}, &Error{Code: "journal_input_encode", Class: "persistence", Err: err}
		}
		inputHash := sha256.Sum256(encodedInput)
		inputDigest := "sha256:" + hex.EncodeToString(inputHash[:])
		if err := journal.VerifyRun(ctx, runID, program.Digest, inputDigest); err != nil {
			return Result{}, &Error{Code: "journal_run_mismatch", Class: "persistence", Err: err}
		}
	}
	state := make(map[string]any)
	result := Result{State: state}
	// effected is the last completed step that declared effects: once it has
	// run, a later failure is no longer safe to retry (see afterEffect).
	effected := ""
	for _, instruction := range program.Instructions {
		if err := ctx.Err(); err != nil {
			return result, &Error{Code: "canceled", Class: "cancellation", Step: instruction.ID, Err: err}
		}
		step := StepResult{ID: instruction.ID}
		switch instruction.Kind {
		case "call":
			var stepAttempt StepAttempt
			definition, ok := e.nodes[instruction.Node]
			if !ok {
				step.Error = &Error{Code: "unknown_node", Class: "configuration", Step: instruction.ID}
				result.Steps = append(result.Steps, step)
				return result, step.Error
			}
			callInput, err := resolveCallInput(state, instruction, input)
			if err != nil {
				step.Error = &Error{Code: "invalid_input_reference", Class: "validation", Step: instruction.ID, Err: err}
				result.Steps = append(result.Steps, step)
				return result, step.Error
			}
			step.Input = callInput
			step.Attempt = 1
			if err := validateSchema(definition.Descriptor().InputSchema, callInput); err != nil {
				step.Error = &Error{Code: "invalid_input", Class: "validation", Step: instruction.ID, Err: err}
				result.Steps = append(result.Steps, step)
				return result, step.Error
			}
			var persistedInput json.RawMessage
			var identity StepIdentity
			if journal != nil {
				persistedInput, err = json.Marshal(callInput)
				if err != nil {
					step.Error = &Error{Code: "journal_input_encode", Class: "persistence", Step: instruction.ID, Err: err}
					result.Steps = append(result.Steps, step)
					return result, step.Error
				}
				identity = stepIdentity(runID, program.Digest, instruction.ID, persistedInput)
				persistedOutput, completed, loadErr := journal.Load(ctx, identity)
				if loadErr != nil {
					step.Error = journalFailure("journal_step_load", instruction.ID, loadErr, definition.Descriptor().Effects)
					result.Steps = append(result.Steps, step)
					return result, step.Error
				}
				if completed {
					if schemaErr := validateRawSchema(definition.Descriptor().OutputSchema, persistedOutput); schemaErr != nil {
						step.Error = &Error{Code: "journal_output_invalid", Class: "persistence", Step: instruction.ID, Err: schemaErr}
						result.Steps = append(result.Steps, step)
						return result, step.Error
					}
					output, decodeErr := definition.DecodeOutput(persistedOutput)
					if decodeErr == nil {
						decodeErr = validateSchema(definition.Descriptor().OutputSchema, output)
					}
					if decodeErr != nil {
						step.Error = &Error{Code: "journal_output_invalid", Class: "persistence", Step: instruction.ID, Err: decodeErr}
						result.Steps = append(result.Steps, step)
						return result, step.Error
					}
					committed, cloneErr := value.Clone(output)
					if cloneErr != nil {
						step.Error = &Error{Code: "output_ownership", Class: "validation", Step: instruction.ID, Err: cloneErr}
						result.Steps = append(result.Steps, step)
						return result, step.Error
					}
					state[instruction.ID] = committed
					step.Output = committed
					result.Steps = append(result.Steps, step)
					if len(definition.Descriptor().Effects) > 0 {
						effected = instruction.ID
					}
					continue
				}
				attempt, beginErr := journal.Begin(ctx, identity, persistedInput, definition.Descriptor().Effects)
				if beginErr != nil {
					step.Error = journalFailure("journal_step_begin", instruction.ID, beginErr, definition.Descriptor().Effects)
					result.Steps = append(result.Steps, step)
					return result, step.Error
				}
				stepAttempt = attempt
			}
			output, err := definition.Invoke(ctx, callInput)
			step.Executed = true
			if err != nil {
				if journal != nil {
					hookCtx, cancel := journalContext(ctx)
					journalErr := journal.Fail(hookCtx, stepAttempt, definition.Descriptor().Effects, err)
					cancel()
					if journalErr != nil {
						err = fmt.Errorf("%w (effect reconciliation unconfirmed: %v)", err, journalErr)
					}
				}
				step.Error = classifyJournaledFailure("node_error", instruction.ID, err, definition.Descriptor().Effects, effected, journal != nil)
				result.Steps = append(result.Steps, step)
				return result, step.Error
			}
			if err := validateSchema(definition.Descriptor().OutputSchema, output); err != nil {
				if journal != nil {
					hookCtx, cancel := journalContext(ctx)
					journalErr := journal.Fail(hookCtx, stepAttempt, definition.Descriptor().Effects, err)
					cancel()
					if journalErr != nil {
						err = fmt.Errorf("%w (effect reconciliation unconfirmed: %v)", err, journalErr)
					}
				}
				step.Error = classifyJournaledFailure("invalid_output", instruction.ID, err, definition.Descriptor().Effects, effected, journal != nil)
				result.Steps = append(result.Steps, step)
				return result, step.Error
			}
			committed, err := value.Clone(output)
			if err != nil {
				if journal != nil {
					hookCtx, cancel := journalContext(ctx)
					journalErr := journal.Fail(hookCtx, stepAttempt, definition.Descriptor().Effects, err)
					cancel()
					if journalErr != nil {
						err = fmt.Errorf("%w (effect reconciliation unconfirmed: %v)", err, journalErr)
					}
				}
				step.Error = classifyJournaledFailure("output_ownership", instruction.ID, err, definition.Descriptor().Effects, effected, journal != nil)
				result.Steps = append(result.Steps, step)
				return result, step.Error
			}
			if journal != nil {
				persistedOutput, encodeErr := json.Marshal(committed)
				if encodeErr != nil {
					hookCtx, cancel := journalContext(ctx)
					journalErr := journal.Fail(hookCtx, stepAttempt, definition.Descriptor().Effects, encodeErr)
					cancel()
					if journalErr != nil {
						encodeErr = fmt.Errorf("%w (effect reconciliation unconfirmed: %v)", encodeErr, journalErr)
					}
				} else {
					hookCtx, cancel := journalContext(ctx)
					encodeErr = journal.Complete(hookCtx, stepAttempt, persistedOutput)
					cancel()
					if encodeErr != nil {
						hookCtx, cancel = journalContext(ctx)
						journalErr := journal.Fail(hookCtx, stepAttempt, definition.Descriptor().Effects, encodeErr)
						cancel()
						if journalErr != nil {
							encodeErr = fmt.Errorf("%w (effect reconciliation unconfirmed: %v)", encodeErr, journalErr)
						}
					}
				}
				if encodeErr != nil {
					step.Error = classifyJournaledFailure("journal_step_complete", instruction.ID, encodeErr, definition.Descriptor().Effects, effected, true)
					result.Steps = append(result.Steps, step)
					return result, step.Error
				}
			}
			state[instruction.ID] = committed
			step.Output = committed
			if len(definition.Descriptor().Effects) > 0 {
				effected = instruction.ID
			}
		case "output":
			output, err := resolveOutput(state, instruction.References)
			if err != nil {
				step.Error = &Error{Code: "invalid_output_reference", Class: "validation", Step: instruction.ID, Err: err}
				result.Steps = append(result.Steps, step)
				return result, step.Error
			}
			result.Output = output
			step.Output = output
		default:
			step.Error = &Error{Code: "unsupported_instruction", Class: "configuration", Step: instruction.ID}
			result.Steps = append(result.Steps, step)
			return result, step.Error
		}
		result.Steps = append(result.Steps, step)
	}
	return result, nil
}

func classify(step string, err error) error {
	var existing *Error
	if errors.As(err, &existing) {
		if existing.Class == "uncertain" && !existing.Uncertain {
			copy := *existing
			copy.Uncertain = true
			return &copy
		}
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
		return &Error{Code: domain.Code, Class: domain.Class, Step: step, Err: err, Uncertain: domain.Uncertain || domain.Class == "uncertain"}
	}
	var uncertainty interface{ IsUncertain() bool }
	if errors.As(err, &uncertainty) && uncertainty.IsUncertain() {
		return &Error{Code: "effect_outcome_uncertain", Class: "uncertain", Step: step, Err: err, Uncertain: true}
	}
	return &Error{Code: "node_error", Class: "failure", Step: step, Err: err}
}

// afterEffect hides saturation from a step's failure once an earlier step
// that declared effects has completed. Saturation tells the caller to
// retry the whole workflow, and the retry would repeat that earlier effect
// (#190). The failure keeps its code, class and text; it just no longer
// matches capacity.ErrSaturated, so triggers answer it as a failure.
func afterEffect(failure error, effected string) error {
	var classified *Error
	if effected == "" || !errors.Is(failure, capacity.ErrSaturated) || !errors.As(failure, &classified) {
		return failure
	}
	return &Error{Code: classified.Code, Class: classified.Class, Step: classified.Step, Err: effectCommitted{err: classified.Err, step: effected}}
}

// effectCommitted is a step failure that follows a committed effect. It
// matches whatever the failure matches except saturation and any error that
// is itself saturation, such as store.ErrBusy; it has no Unwrap, which
// would expose them further down the chain.
type effectCommitted struct {
	err  error
	step string
}

func (e effectCommitted) Error() string {
	return fmt.Sprintf("%v (after step %q committed its effects)", e.err, e.step)
}

func (e effectCommitted) Is(target error) bool {
	if errors.Is(target, capacity.ErrSaturated) {
		return false
	}
	return errors.Is(e.err, target)
}

func (e effectCommitted) As(target any) bool { return errors.As(e.err, target) }

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

func validateRawSchema(raw, data []byte) error {
	parsed, err := schema.Parse(raw)
	if err != nil {
		return err
	}
	return parsed.ValidateValue(data)
}

func stepIdentity(runID, artifactDigest, stepID string, input []byte) StepIdentity {
	inputHash := sha256.Sum256(input)
	identity := StepIdentity{RunID: runID, ArtifactDigest: artifactDigest, StepID: stepID, InputDigest: "sha256:" + hex.EncodeToString(inputHash[:])}
	encoded, _ := json.Marshal(identity)
	operationHash := sha256.Sum256(encoded)
	identity.OperationKey = "op:" + hex.EncodeToString(operationHash[:])
	return identity
}

func journalContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx.Err() == nil {
		return ctx, func() {}
	}
	return context.WithTimeout(context.WithoutCancel(ctx), 2_000_000_000)
}

func classifyJournaledFailure(code, step string, err error, effects []string, effected string, journaled bool) error {
	if journaled && len(effects) > 0 {
		return &Error{Code: "effect_outcome_uncertain", Class: "uncertain", Step: step, Err: err, Uncertain: true}
	}
	if code == "invalid_output" {
		return &Error{Code: code, Class: "validation", Step: step, Err: err}
	}
	return afterEffect(classify(step, err), effected)
}

func journalFailure(code, step string, err error, effects []string) error {
	var existing *Error
	if errors.As(err, &existing) && existing.Class == "uncertain" {
		copy := *existing
		copy.Uncertain = true
		return &copy
	}
	var uncertain interface{ IsUncertain() bool }
	if len(effects) > 0 && errors.As(err, &uncertain) && uncertain.IsUncertain() {
		return &Error{Code: "effect_outcome_uncertain", Class: "uncertain", Step: step, Err: err, Uncertain: true}
	}
	return &Error{Code: code, Class: "persistence", Step: step, Err: err}
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
