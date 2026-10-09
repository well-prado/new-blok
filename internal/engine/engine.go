// Package engine executes compiled native programs in bounded memory mode.
package engine

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/capacity"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/observe"
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
	Suspended bool
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
func (e *Error) IsSuspended() bool  { return e != nil && e.Suspended }

type Result struct {
	Output any
	State  map[string]any
	Steps  []StepResult
	// Trace is the run span of an observed, traced run (zero otherwise). The
	// application boundary that owns the terminal event passes it back
	// through EmitRunTerminal.
	Trace observe.Span
}

type StepResult struct {
	ID string
	// InvocationPath and IterationPath identify this execution of the step
	// (ADR 0028): its id at the root or "<construct path>/<arm>/<id>"
	// inside an arm, and "root" outside every each.
	InvocationPath string
	IterationPath  string
	// Executed reports that this step invoked a node. A control
	// instruction never does itself (its arms' steps report their own), so
	// its step is not Executed even when it ran.
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
	// payloads is false when every selected observer declared, through
	// observe.PayloadObserver, that it never reads Input or Output.
	payloads bool
	tracing  observe.TracePolicy
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

// WaitJournal persists a wait transition and returns its committed outcome.
// ready=false means the run must suspend without occupying an execution slot.
// The journal is separate from step attempts because waiting is not an
// external effect dispatch.
type WaitJournal interface {
	Await(context.Context, WaitIdentity) (WaitResult, bool, error)
}

type WaitIdentity struct {
	Step          StepIdentity
	Name          string
	TimeoutMillis int64
}

type WaitResult struct {
	SignalID string          `json:"signalId,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	TimedOut bool            `json:"timedOut,omitempty"`
}

// StepIdentity binds a checkpoint to the exact run artifact and resolved
// input. OperationKey is stable across retries; each Begin returns a distinct
// AttemptID so a stale result cannot overwrite a later attempt.
//
// Its JSON form is stored (internal/cluster persists it in every step
// record), and OperationKey is a digest of an explicit encoding of it
// (identity.go), so neither moves when a Go field is renamed or added.
// The tags spell the names json.Marshal used before they existed (#382):
// never change one. A field added later must be omitempty and its zero
// value must mean what every record written before it meant.
type StepIdentity struct {
	RunID          string `json:"RunID"`
	ArtifactDigest string `json:"ArtifactDigest"`
	StepID         string `json:"StepID"`
	InputDigest    string `json:"InputDigest"`
	OperationKey   string `json:"OperationKey"`
	// IterationPath is the loop iteration the step runs in (ADR 0028);
	// empty, or RootIteration, outside every loop.
	IterationPath string `json:"IterationPath,omitempty"`
	// InvocationPath is the step's invocation path inside an arm of a
	// control construct ("<construct path>/<arm>/<id>", ADR 0028); empty,
	// or StepID, at the top level. It is not an input to OperationKey: a
	// step id is unique across a program's tree, so with the artifact
	// digest the id already determines the path (#333 slice 2).
	InvocationPath string `json:"InvocationPath,omitempty"`
}

// ScopeJournal journals the scope of each control construct with arms
// (if, choose, try-finally; ADR 0028, #333). A durable runner whose
// journal does not implement it refuses a control program
// (durable_control_unsupported) before touching the journal.
//
// EnterScope records the construct's decision (the arm it selected, empty
// for a try-finally) the first time the run reaches it, and from then on
// returns the recorded decision, which the engine follows: like a
// committed step result, a recorded decision is never re-evaluated. A
// completed scope comes back with Completed set; the engine replays its
// arm from the journal and does not exit it again. ExitScope commits the
// construct's result. A construct that fails leaves its scope running;
// the run's failure ends it.
type ScopeJournal interface {
	EnterScope(context.Context, ScopeIdentity, json.RawMessage) (ScopeEntry, error)
	ExitScope(context.Context, ScopeEntry, json.RawMessage) error
}

// LoopJournal journals each and parallel (ADR 0028, #333 slice 3) on top
// of their scopes: every item of an each and every arm of a parallel is a
// slot, a scope recorded completed with its result wrapped as
// {"output": <result>}, in one transaction, once the item's or arm's last
// step has committed. A recorded slot never changes: RecordSlot with other
// bytes is refused, with the same bytes is a no-op. Slots returns the
// slots recorded under a loop's scope whose paths begin with prefix, by
// path. FailScope replaces a running loop scope's decision with one that
// records the loop's failure (fail-fast), so a replay reports it without
// starting an item. A durable runner whose journal does not implement it
// refuses each and parallel.
type LoopJournal interface {
	ScopeJournal
	Slots(ctx context.Context, loop ScopeEntry, prefix string) (map[string]json.RawMessage, error)
	RecordSlot(ctx context.Context, loop ScopeEntry, slot ScopeIdentity, output json.RawMessage) error
	FailScope(ctx context.Context, loop ScopeEntry, decision json.RawMessage) error
}

// ScopeIdentity names one execution of a construct: Path is
// "<invocation path>@<iteration path>", ParentPath the path of the
// construct whose arm it runs in (empty at the top level), Kind the
// instruction kind (ADR 0028, "Joins and scopes").
type ScopeIdentity struct {
	RunID          string
	ArtifactDigest string
	Path           string
	ParentPath     string
	Kind           string
}

// ScopeEntry is an entered scope: the attempt that may exit it, and the
// decision recorded for it.
type ScopeEntry struct {
	Scope     ScopeIdentity
	AttemptID string
	Decision  json.RawMessage
	Completed bool
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

func (e *Engine) WithObserver(observer inspection.Observer) *Engine {
	copy := *e
	copy.observer = observer
	copy.payloads = observer != nil
	if declared, ok := observer.(observe.PayloadObserver); ok {
		copy.payloads = declared.ObservesPayloads()
	}
	return &copy
}

// WithTracing selects the head-sampling policy for observed runs. Tracing
// happens only while an observer is selected; the zero policy disables it.
// An invalid policy is ignored (app.New rejects it before it gets here).
func (e *Engine) WithTracing(policy observe.TracePolicy) *Engine {
	copy := *e
	copy.tracing = policy
	return &copy
}

func (e *Engine) Run(ctx context.Context, program contract.InternalProgram, input any) (Result, error) {
	return e.RunJournaled(ctx, program, input, "", nil)
}

// RunJournaled executes a program through a caller-owned durable step journal.
// A blank runID is valid only when journal is nil. Completed step outputs are
// restored and never invoked again; a journal error fails the run closed.
//
// Journaled execution emits no inspection events, even when an observer is
// attached. One durable run spans several RunJournaled calls (suspension at a
// wait, replay of committed steps after takeover), and the step event kinds
// describe a single attempt: a suspension would read as a failed step and a
// restored output as a fresh completion. Durable runs are inspected through
// their committed journal, not through per-attempt engine events.
func (e *Engine) RunJournaled(ctx context.Context, program contract.InternalProgram, input any, runID string, journal StepJournal) (Result, error) {
	return e.run(ctx, program, input, runID, journal, inspection.Invocation{}, false, true)
}

// RunObserved executes the same production interpreter as Run and emits
// read-only events tied to trusted invocation metadata.
func (e *Engine) RunObserved(ctx context.Context, program contract.InternalProgram, input any, invocation inspection.Invocation) (result Result, runErr error) {
	return e.run(ctx, program, input, "", nil, invocation, true, true)
}

// RunObservedPending emits start and step evidence but leaves the run terminal
// event to the application boundary that owns durable outcome persistence.
func (e *Engine) RunObservedPending(ctx context.Context, program contract.InternalProgram, input any, invocation inspection.Invocation) (result Result, runErr error) {
	return e.run(ctx, program, input, "", nil, invocation, true, false)
}

// EmitRunTerminal publishes exactly one application-owned terminal projection.
// Call it only after the corresponding durable outcome attempt has resolved.
func (e *Engine) EmitRunTerminal(invocation inspection.Invocation, workflow string, result Result, kind inspection.Kind, code, class string) {
	if e == nil || e.observer == nil || invocation.RunID == "" || invocation.Principal == "" {
		return
	}
	switch kind {
	case inspection.RunCompleted, inspection.RunFailed, inspection.RunCanceled, inspection.RunSuspended, inspection.RunUncertain:
	default:
		return
	}
	var output json.RawMessage
	if kind == inspection.RunCompleted && e.payloads {
		output = marshalObservation(result.Output)
	}
	e.observer.Observe(inspection.Event{
		Kind: kind, RunID: invocation.RunID, Principal: invocation.Principal,
		Workflow: workflow, ParentRun: invocation.ParentRun, ParentStep: invocation.ParentStep,
		Output:    output,
		ErrorCode: code, ErrorClass: class, At: time.Now().UTC(),
		Tenant: invocation.Tenant, Trace: result.Trace,
	})
}

// InputDigest is a run's engine-input identity: the digest of the input's
// json.Marshal encoding, as a journaled run verifies it on every execution.
// An admitter fixing the identity up front uses it too, so the two cannot
// drift.
func InputDigest(input any) (string, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return "", err
	}
	hash := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(hash[:]), nil
}

func (e *Engine) run(ctx context.Context, program contract.InternalProgram, input any, runID string, journal StepJournal, invocation inspection.Invocation, requested, terminalOwned bool) (result Result, runErr error) {
	if e == nil {
		return Result{}, &Error{Code: "nil_engine", Class: "configuration"}
	}
	if journal != nil && (runID == "" || program.Digest == "") {
		return Result{}, &Error{Code: "missing_run_identity", Class: "configuration"}
	}
	if len(program.Instructions) > e.maxSteps {
		return Result{}, &Error{Code: "step_budget_exceeded", Class: "admission"}
	}
	if err := checkProgram(program, e.maxSteps); err != nil {
		return Result{}, err
	}
	controls := controlIDs(program.Instructions, nil)
	var scopes ScopeJournal
	var loops LoopJournal
	if journal != nil && program.Format == contract.ControlFormat {
		// A durable runner journals if, choose and try-finally through
		// the journal's scopes (#333 slice 2), each and parallel through
		// its slots (slice 3); a journal without scopes (the cluster's)
		// runs none of them.
		var ok bool
		if scopes, ok = journal.(ScopeJournal); !ok {
			return Result{}, &Error{Code: "durable_control_unsupported", Class: "configuration", Err: fmt.Errorf("this durable runner's journal does not journal control constructs")}
		}
		loops, _ = journal.(LoopJournal)
		if kind := unjournaledControl(program.Instructions); kind != "" && loops == nil {
			return Result{}, &Error{Code: "durable_control_unsupported", Class: "configuration", Err: fmt.Errorf("this durable runner's journal does not journal the slots %s needs", kind)}
		}
	}
	if journal != nil {
		inputDigest, err := InputDigest(input)
		if err != nil {
			return Result{}, &Error{Code: "journal_input_encode", Class: "persistence", Err: err}
		}
		if err := journal.VerifyRun(ctx, runID, program.Digest, inputDigest); err != nil {
			return Result{}, &Error{Code: "journal_run_mismatch", Class: "persistence", Err: err}
		}
	}
	observing := requested && e.observer != nil
	if observing && (invocation.RunID == "" || invocation.Principal == "") {
		return Result{}, &Error{Code: "inspection_identity_required", Class: "configuration"}
	}
	if observing && invocation.AttemptID == "" {
		var id [16]byte
		if _, err := rand.Read(id[:]); err != nil {
			return Result{}, &Error{Code: "inspection_attempt_id_failed", Class: "configuration", Err: err}
		}
		invocation.AttemptID = "attempt:" + hex.EncodeToString(id[:])
	}
	payloads := observing && e.payloads
	// A traced run joins a trusted parent supplied with the invocation or,
	// failing that, the trace of the step that started it (a child run).
	// Spans are allocated here, not by an exporter, so the context handed to
	// nodes, workers and child runs exists before any exporter sees it.
	tracing := observing && e.tracing.Enabled()
	var runSpan observe.Span
	if tracing {
		parent := invocation.Trace
		if !parent.Valid() {
			parent, _ = observe.TraceFrom(ctx)
		}
		runSpan = e.tracing.Root(parent)
	}
	// Steps in concurrent arms (each, parallel) report at once: events,
	// the step list and effected are shared, so each is serialised.
	var emitMu, stepsMu, effectedMu sync.Mutex
	// emitIn publishes one event of a step running in iteration. A step
	// inside an each runs once per item, so its attempt id names the
	// iteration as well; outside every each it is unchanged.
	emitIn := func(event inspection.Event, iteration string) {
		if !observing {
			return
		}
		event.RunID, event.Principal, event.Workflow = invocation.RunID, invocation.Principal, program.WorkflowID
		event.ParentRun, event.ParentStep = invocation.ParentRun, invocation.ParentStep
		event.Tenant = invocation.Tenant
		if tracing && !event.Trace.SpanID.IsValid() {
			event.Trace = runSpan
		}
		if event.At.IsZero() {
			event.At = time.Now().UTC()
		}
		if event.Attempt > 0 && event.StepID != "" {
			step := event.StepID
			if iteration != "" && iteration != rootIteration {
				step += "@" + boundedIteration(iteration)
			}
			event.AttemptID = invocation.AttemptID + "/" + step + "/" + fmt.Sprint(event.Attempt)
		}
		emitMu.Lock()
		defer emitMu.Unlock()
		e.observer.Observe(event)
	}
	emit := func(event inspection.Event) { emitIn(event, rootIteration) }
	var inputJSON json.RawMessage
	if payloads {
		inputJSON = marshalObservation(input)
	}
	emit(inspection.Event{Kind: inspection.RunStarted, Input: inputJSON})
	defer func() {
		if !terminalOwned {
			return
		}
		terminal := inspection.Event{Kind: inspection.RunCompleted}
		if runErr != nil {
			terminal.Kind = inspection.RunFailed
			var classified *Error
			if errors.As(runErr, &classified) {
				terminal.ErrorCode, terminal.ErrorClass = classified.Code, classified.Class
				if classified.Uncertain {
					terminal.Kind = inspection.RunUncertain
				}
			}
			if terminal.Kind != inspection.RunUncertain && (errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded)) {
				terminal.Kind = inspection.RunCanceled
			}
		} else if payloads {
			terminal.Output = marshalObservation(result.Output)
		}
		emit(terminal)
	}()
	// The run's own context lets a construct tell the caller's cancellation
	// from its own fail-fast cancellation of siblings (runTry).
	ctx = context.WithValue(ctx, runContextKey{}, ctx)
	state := make(map[string]any)
	result = Result{State: state, Trace: runSpan}
	// effected is the last completed step that declared effects: once it has
	// run, a later failure is no longer safe to retry (see afterEffect).
	effected := ""
	lastEffected := func() string {
		effectedMu.Lock()
		defer effectedMu.Unlock()
		return effected
	}
	setEffected := func(step string) {
		effectedMu.Lock()
		effected = step
		effectedMu.Unlock()
	}
	// recordStep appends a finished step and reports it. external marks an
	// external call for observers: its node declares effects (ADR 0022).
	recordStep := func(step StepResult, stepSpan observe.Span, external bool) {
		stepsMu.Lock()
		result.Steps = append(result.Steps, step)
		stepsMu.Unlock()
		event := inspection.Event{Kind: inspection.StepCompleted, StepID: step.ID, Attempt: step.Attempt, Trace: stepSpan, External: external}
		if payloads {
			event.Input = marshalObservation(step.Input)
			event.Output = marshalObservation(step.Output)
		}
		if step.Error != nil {
			event.Kind = inspection.StepFailed
			var classified *Error
			if errors.As(step.Error, &classified) {
				event.ErrorCode, event.ErrorClass = classified.Code, classified.Class
				if classified.Uncertain {
					event.Kind = inspection.StepUncertain
				}
			}
			if event.Kind != inspection.StepUncertain && (errors.Is(step.Error, context.Canceled) || errors.Is(step.Error, context.DeadlineExceeded)) {
				event.Kind = inspection.StepCanceled
			}
		}
		event.At = step.FinishedAt
		emitIn(event, step.IterationPath)
	}
	// runBlock runs one block of instructions in its frame: the program at
	// the root, or one arm of a control instruction (ADR 0028).
	var runBlock func(context.Context, *frame, []contract.InternalInstruction) error
	// runArm runs one arm in its frame and returns the arm's result.
	runArm := func(ctx context.Context, f *frame, owner string, arm contract.Arm) (any, error) {
		if err := runBlock(ctx, f, arm.Instructions); err != nil {
			return nil, err
		}
		if arm.Output == nil {
			return nil, nil
		}
		output, err := f.operand(*arm.Output)
		if err != nil {
			return nil, &Error{Code: "invalid_output_reference", Class: "validation", Step: owner, Err: fmt.Errorf("arm %s: %w", arm.Name, err)}
		}
		return output, nil
	}
	runBlock = func(ctx context.Context, f *frame, instructions []contract.InternalInstruction) error {
		for _, instruction := range instructions {
			if err := ctx.Err(); err != nil {
				return &Error{Code: "canceled", Class: "cancellation", Step: instruction.ID, Err: err}
			}
			step := StepResult{ID: instruction.ID, InvocationPath: f.invocation(instruction.ID), IterationPath: f.iteration}
			// Per instruction, so steps in concurrent arms never share them.
			external := false
			var stepSpan observe.Span
			if tracing {
				stepSpan = f.span.Child()
			}
			appendStep := func(step StepResult) { recordStep(step, stepSpan, external) }
			emit := func(event inspection.Event) { emitIn(event, f.iteration) }
			switch instruction.Kind {
			case "wait":
				if journal == nil || instruction.Wait == nil {
					step.Error = &Error{Code: "wait_requires_durable_runner", Class: "configuration", Step: instruction.ID}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				waitJournal, ok := journal.(WaitJournal)
				if !ok {
					step.Error = &Error{Code: "wait_journal_unavailable", Class: "configuration", Step: instruction.ID}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				plan, err := json.Marshal(instruction.Wait)
				if err != nil {
					step.Error = &Error{Code: "wait_identity_encode", Class: "persistence", Step: instruction.ID, Err: err}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				identity := f.stepIdentity(runID, program.Digest, instruction.ID, plan)
				waitResult, ready, waitErr := waitJournal.Await(ctx, WaitIdentity{Step: identity, Name: instruction.Wait.Name, TimeoutMillis: instruction.Wait.TimeoutMillis})
				if waitErr != nil {
					step.Error = journalFailure("journal_wait", instruction.ID, waitErr, nil)
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				if !ready {
					step.Error = &Error{Code: "run_suspended", Class: "waiting", Step: instruction.ID, Suspended: true}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				f.values[instruction.ID] = waitResult
				step.Output = waitResult
			case "call":
				var stepAttempt StepAttempt
				definition, ok := e.nodes[instruction.Node]
				if !ok {
					step.Error = &Error{Code: "unknown_node", Class: "configuration", Step: instruction.ID}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				external = len(definition.Descriptor().Effects) > 0
				callInput, err := resolveCallInput(f, instruction, input)
				if err != nil {
					step.Error = &Error{Code: "invalid_input_reference", Class: "validation", Step: instruction.ID, Err: err}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				step.Input = callInput
				step.Attempt = 1
				step.StartedAt = time.Now().UTC()
				if observing {
					processing := inspection.Event{Kind: inspection.StepProcessing, StepID: step.ID, Attempt: step.Attempt, AttemptID: invocation.AttemptID, Trace: stepSpan, External: external}
					if payloads {
						processing.Input = marshalObservation(step.Input)
					}
					emit(processing)
				}
				if err := validateSchema(definition.Descriptor().InputSchema, callInput); err != nil {
					step.Error = &Error{Code: "invalid_input", Class: "validation", Step: instruction.ID, Err: err}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				var persistedInput json.RawMessage
				var identity StepIdentity
				if journal != nil {
					persistedInput, err = json.Marshal(callInput)
					if err != nil {
						step.Error = &Error{Code: "journal_input_encode", Class: "persistence", Step: instruction.ID, Err: err}
						step.FinishedAt = time.Now().UTC()
						appendStep(step)
						return step.Error
					}
					identity = f.stepIdentity(runID, program.Digest, instruction.ID, persistedInput)
					persistedOutput, completed, loadErr := journal.Load(ctx, identity)
					if loadErr != nil {
						step.Error = journalFailure("journal_step_load", instruction.ID, loadErr, definition.Descriptor().Effects)
						step.FinishedAt = time.Now().UTC()
						appendStep(step)
						return step.Error
					}
					if completed {
						if schemaErr := validateRawSchema(definition.Descriptor().OutputSchema, persistedOutput); schemaErr != nil {
							step.Error = &Error{Code: "journal_output_invalid", Class: "persistence", Step: instruction.ID, Err: schemaErr}
							step.FinishedAt = time.Now().UTC()
							appendStep(step)
							return step.Error
						}
						output, decodeErr := definition.DecodeOutput(persistedOutput)
						if decodeErr == nil {
							decodeErr = validateSchema(definition.Descriptor().OutputSchema, output)
						}
						if decodeErr != nil {
							step.Error = &Error{Code: "journal_output_invalid", Class: "persistence", Step: instruction.ID, Err: decodeErr}
							step.FinishedAt = time.Now().UTC()
							appendStep(step)
							return step.Error
						}
						committed, cloneErr := value.Clone(output)
						if cloneErr != nil {
							step.Error = &Error{Code: "output_ownership", Class: "validation", Step: instruction.ID, Err: cloneErr}
							step.FinishedAt = time.Now().UTC()
							appendStep(step)
							return step.Error
						}
						f.values[instruction.ID] = committed
						step.Output = committed
						step.FinishedAt = time.Now().UTC()
						appendStep(step)
						if len(definition.Descriptor().Effects) > 0 {
							setEffected(instruction.ID)
						}
						continue
					}
					attempt, beginErr := journal.Begin(ctx, identity, persistedInput, definition.Descriptor().Effects)
					if beginErr != nil {
						step.Error = journalFailure("journal_step_begin", instruction.ID, beginErr, definition.Descriptor().Effects)
						step.FinishedAt = time.Now().UTC()
						appendStep(step)
						return step.Error
					}
					stepAttempt = attempt
				}
				invokeCtx := ctx
				if observing {
					invokeCtx = node.WithLogger(ctx, slog.New(&inspectionLogHandler{emit: emit, step: instruction.ID, trace: stepSpan}))
				}
				if tracing {
					// The node, a worker it calls and any child run it starts
					// see this step attempt as their parent.
					invokeCtx = observe.WithTrace(invokeCtx, stepSpan.TraceContext)
				}
				invokeInput := callInput
				if program.Format == contract.ControlFormat && len(instruction.References) > 0 && controls[instruction.References[0].Step] {
					// A value a control construct produced (a JSON literal,
					// an each's results, a *T it handed on) reaches the node
					// as its input type when it reads exactly as one; the
					// schema above checked the value as resolved. Every
					// other input is passed as is, as in a call-only
					// program (ADR 0028).
					invokeInput = definition.ConvertInput(callInput)
				}
				output, err := definition.Invoke(invokeCtx, invokeInput)
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
					step.Error = classifyJournaledFailure("node_error", instruction.ID, err, definition.Descriptor().Effects, lastEffected(), journal != nil)
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
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
					step.Error = classifyJournaledFailure("invalid_output", instruction.ID, err, definition.Descriptor().Effects, lastEffected(), journal != nil)
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
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
					step.Error = classifyJournaledFailure("output_ownership", instruction.ID, err, definition.Descriptor().Effects, lastEffected(), journal != nil)
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
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
						if len(definition.Descriptor().Effects) > 0 {
							step.Error = classifyJournaledFailure("journal_step_complete", instruction.ID, encodeErr, definition.Descriptor().Effects, lastEffected(), true)
						} else {
							step.Error = &Error{Code: "journal_step_complete", Class: "persistence", Step: instruction.ID, Err: encodeErr}
						}
						step.FinishedAt = time.Now().UTC()
						appendStep(step)
						return step.Error
					}
				}
				f.values[instruction.ID] = committed
				step.Output = committed
				if len(definition.Descriptor().Effects) > 0 {
					setEffected(instruction.ID)
				}
			case "output":
				step.StartedAt = time.Now().UTC()
				step.Attempt = 1
				if observing {
					emit(inspection.Event{Kind: inspection.StepProcessing, StepID: step.ID, Attempt: step.Attempt, AttemptID: invocation.AttemptID, Trace: stepSpan})
				}
				output, err := resolveOutput(f, instruction.References)
				if err != nil {
					step.Error = &Error{Code: "invalid_output_reference", Class: "validation", Step: instruction.ID, Err: err}
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				result.Output = output
				step.Output = output
			case "compare", "default", "if", "choose", "try-finally", "each", "parallel":
				step.StartedAt = time.Now().UTC()
				step.Attempt = 1
				if observing {
					emit(inspection.Event{Kind: inspection.StepProcessing, StepID: step.ID, Attempt: step.Attempt, AttemptID: invocation.AttemptID, Trace: stepSpan})
				}
				// enter records the construct's decision in its scope and
				// returns the decision to follow: the recorded one once
				// there is one (ScopeJournal). In memory it is the one
				// just made.
				var entered *ScopeEntry
				enter := func(decision string) (string, error) {
					if scopes == nil {
						return decision, nil
					}
					entry, arm, err := enterScope(ctx, scopes, ScopeIdentity{RunID: runID, ArtifactDigest: program.Digest, Path: f.scopePath(instruction.ID), ParentPath: f.scope, Kind: instruction.Kind}, instruction, decision)
					if err != nil {
						return "", err
					}
					entered = &entry
					return arm, nil
				}
				// An each or a parallel journals its scope and slots itself
				// (loopScope).
				var loop *loopScope
				if loops != nil && (instruction.Kind == "each" || instruction.Kind == "parallel") {
					loop = &loopScope{journal: loops, identity: ScopeIdentity{RunID: runID, ArtifactDigest: program.Digest, Path: f.scopePath(instruction.ID), ParentPath: f.scope, Kind: instruction.Kind}}
				}
				output, err := runControl(ctx, f, instruction, e.maxSteps, func(ctx context.Context, armFrame *frame, arm contract.Arm) (any, error) {
					// The arm's steps are children of the construct's span.
					armFrame.span = stepSpan
					return runArm(ctx, armFrame, instruction.ID, arm)
				}, enter, loop)
				if err == nil && entered != nil && !entered.Completed {
					err = exitScope(ctx, scopes, *entered, instruction.ID, output)
				}
				if err != nil {
					// An arm's failure was classified when it failed, but a
					// concurrent sibling may have committed an effect since,
					// before the construct joined: decide retry safety
					// again now (#190).
					step.Error = afterEffect(err, lastEffected())
					step.FinishedAt = time.Now().UTC()
					appendStep(step)
					return step.Error
				}
				f.values[instruction.ID] = output
				step.Output = output
			default:
				step.Error = &Error{Code: "unsupported_instruction", Class: "configuration", Step: instruction.ID}
				step.FinishedAt = time.Now().UTC()
				appendStep(step)
				return step.Error
			}
			step.FinishedAt = time.Now().UTC()
			appendStep(step)
		}
		return nil
	}
	if err := runBlock(ctx, &frame{values: state, iteration: rootIteration, input: input, span: runSpan}, program.Instructions); err != nil {
		return result, err
	}
	return result, nil
}

func marshalObservation(value any) json.RawMessage {
	if value == nil {
		return nil
	}
	const truncated = `{"$truncated":true}`
	budget := observationBudget{remaining: maxObservedPayloadBytes, nodes: maxObservationNodes}
	safe, ok := budget.capture(reflect.ValueOf(value), 0)
	if !ok {
		return json.RawMessage(truncated)
	}
	data, err := json.Marshal(safe)
	if err != nil || len(data) > maxObservedPayloadBytes {
		return json.RawMessage(truncated)
	}
	return data
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
		return &Error{Code: boundedLabel(domain.Code, "node_error"), Class: boundedLabel(domain.Class, "failure"), Step: step, Err: err, Uncertain: domain.Uncertain || domain.Class == "uncertain" || uncertaintyMarked(err)}
	}
	if uncertaintyMarked(err) {
		return &Error{Code: "external_outcome_uncertain", Class: "effect", Step: step, Uncertain: true, Err: err}
	}
	return &Error{Code: "node_error", Class: "failure", Step: step, Err: err}
}

func uncertaintyMarked(err error) bool {
	var marker interface{ IsUncertain() bool }
	return errors.As(err, &marker) && marker.IsUncertain()
}
func boundedLabel(value, fallback string) string {
	if value == "" {
		return fallback
	}
	if len(value) > 64 {
		value = value[:64]
	}
	for _, r := range value {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '_' || r == '-' || r == '.') {
			return fallback
		}
	}
	return value
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
	return &Error{Code: classified.Code, Class: classified.Class, Step: classified.Step, Uncertain: classified.Uncertain, Err: effectCommitted{err: classified.Err, step: effected}}
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

func resolveCallInput(f *frame, instruction contract.InternalInstruction, input any) (any, error) {
	if len(instruction.References) == 0 {
		return input, nil
	}
	return f.resolve(instruction.References[0])
}

func resolveOutput(f *frame, references []contract.Reference) (any, error) {
	if len(references) == 0 {
		return nil, fmt.Errorf("output requires a reference")
	}
	return f.resolve(references[0])
}

func resolveReference(state map[string]any, reference contract.Reference) (any, error) {
	current, ok := state[reference.Step]
	if !ok {
		return nil, fmt.Errorf("step %q has no committed output", reference.Step)
	}
	if len(reference.Path) == 0 {
		return current, nil
	}
	// The walk keeps reflect values between segments: encoding/json picks
	// a pointer-receiver marshaler only for addressable values, so losing
	// addressability midway would change what a key resolves to.
	resolved := reflect.ValueOf(&current).Elem()
	for _, name := range reference.Path {
		var err error
		resolved, err = field(resolved, name)
		if err != nil {
			return nil, err
		}
	}
	return resolved.Interface(), nil
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
