package execution

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/internal/program"
	"github.com/well-prado/new-blok/internal/resumer"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// DefaultJournalPath is where a durable runtime keeps its journal when its
// configuration names none: under the process's working directory.
const DefaultJournalPath = "data/journal.db"

var (
	// ErrNotDurable: a workflow registered as durable whose Spec.Durability
	// is not flow.Durable.
	ErrNotDurable = errors.New("execution: workflow is not durable")
	// ErrUnknownWorkflow: Run names a workflow the runtime has not
	// registered.
	ErrUnknownWorkflow = errors.New("execution: durable workflow is not registered")
	// ErrDurableNotStarted: the runtime's application dependency has not
	// started, or has stopped.
	ErrDurableNotStarted = errors.New("execution: durable runtime is not started")
)

// DurableWorkflow is a workflow registered for durable execution: its
// lowered program, identified by digest, and its typed input decoder.
type DurableWorkflow struct {
	name   string
	work   resumer.Workflow
	digest string
}

// Name is the name the workflow's runs are admitted under, its
// Spec.Name; a flow.Child names it.
func (w DurableWorkflow) Name() string { return w.name }

// Digest identifies the workflow's program: a run admitted under one
// digest only ever runs that program (a build whose program changed
// leaves the run alone).
func (w DurableWorkflow) Digest() string { return w.digest }

// Durable registers definition for durable execution. Its
// Spec.Durability must be flow.Durable. A run's input is decoded into I
// as its admitted JSON every time it executes.
func Durable[I, O any](definition flow.Definition[I, O]) (DurableWorkflow, error) {
	spec := definition.Program().Spec
	if spec.Durability != flow.Durable {
		return DurableWorkflow{}, fmt.Errorf("%w: %s has durability %q", ErrNotDurable, spec.Name, spec.Durability)
	}
	lowered, err := definition.Lower()
	if err != nil {
		return DurableWorkflow{}, err
	}
	if lowered.Digest, err = programDigest(lowered); err != nil {
		return DurableWorkflow{}, err
	}
	decode := func(raw json.RawMessage) (any, error) {
		var input I
		err := json.Unmarshal(raw, &input)
		return input, err
	}
	return DurableWorkflow{name: spec.Name, digest: lowered.Digest, work: resumer.Workflow{Program: lowered, DecodeInput: decode}}, nil
}

// programDigest identifies a lowered program: a call-only program by its
// version-1 artifact digest (internal/program), any other by the SHA-256
// of a versioned encoding of its workflow id, format and instructions
// (ADR 0028, slice 5).
func programDigest(lowered contract.InternalProgram) (string, error) {
	if lowered.Format == 0 {
		built, err := program.Build(lowered, program.Limits{})
		return built.Digest(), err
	}
	encoded, err := json.Marshal(struct {
		Artifact     string                         `json:"artifact"`
		WorkflowID   string                         `json:"workflowId"`
		Format       int                            `json:"format"`
		Instructions []contract.InternalInstruction `json:"instructions"`
	}{"blok-program/v2", lowered.WorkflowID, lowered.Format, lowered.Instructions})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// DurableConfig configures a durable runtime.
type DurableConfig struct {
	// Path is the SQLite journal; empty means DefaultJournalPath. Its
	// directory is created if missing.
	Path      string
	Nodes     map[string]node.Any
	Workflows []DurableWorkflow
	// Workers bounds concurrent executions; zero means 4.
	Workers int
	// WakeupLease is how long a run stays leased to an execution before
	// another may take it over (a crashed process's runs resume after it);
	// zero means 30 seconds.
	WakeupLease time.Duration
	// Interval between sweeps for woken, due and interrupted runs; zero
	// means one second.
	Interval time.Duration
	// MaxChildDepth bounds how deep child runs nest; zero means 8.
	MaxChildDepth int
	// OnError, if set, receives the runtime's sweep and settlement errors.
	OnError func(error)
}

// DurableRuntime runs durable workflows through a single-host journal: a
// run is admitted, executed by a resumer, suspended at waits and child
// runs holding nothing, and resumed after a crash or restart (ADR 0027,
// 0028). Register it with the application through Dependency: it opens
// the journal and starts resuming runs when the application starts, and
// drains them when it shuts down.
type DurableRuntime struct {
	config    DurableConfig
	workflows map[string]DurableWorkflow

	mu       sync.Mutex
	database store.Database
	journal  *journal.Journal
	resumer  *resumer.Resumer
	stop     context.CancelFunc
	stopped  chan struct{}
	waiters  map[string][]chan resumer.Outcome
}

// NewDurable checks config; nothing is opened until the runtime's
// dependency starts.
func NewDurable(config DurableConfig) (*DurableRuntime, error) {
	if config.Path == "" {
		config.Path = DefaultJournalPath
	}
	if len(config.Workflows) == 0 {
		return nil, errors.New("execution: a durable runtime needs at least one workflow")
	}
	workflows := map[string]DurableWorkflow{}
	for _, workflow := range config.Workflows {
		if workflow.name == "" || workflow.digest == "" {
			return nil, errors.New("execution: register durable workflows with execution.Durable")
		}
		if _, seen := workflows[workflow.name]; seen {
			return nil, fmt.Errorf("execution: durable workflow %s is registered twice", workflow.name)
		}
		workflows[workflow.name] = workflow
	}
	return &DurableRuntime{config: config, workflows: workflows, waiters: map[string][]chan resumer.Outcome{}}, nil
}

// Dependency is the runtime's place in the application lifecycle: Start
// opens the journal and starts resuming runs (every accepted run that is
// woken, due or interrupted, including those a crashed process left);
// Close stops sweeping, lets running executions finish until its context
// ends, then cancels them and closes the journal.
func (d *DurableRuntime) Dependency() app.Dependency {
	return app.Dependency{Name: "durable-runs", Start: d.start, Close: d.close}
}

func (d *DurableRuntime) start(ctx context.Context) error {
	if err := os.MkdirAll(filepath.Dir(d.config.Path), 0o700); err != nil {
		return err
	}
	database, err := (sqlite.Backend{}).Open(ctx, d.config.Path)
	if err != nil {
		return err
	}
	j, err := journal.New(ctx, database, journal.Config{WakeupLease: d.config.WakeupLease, MaxChildDepth: d.config.MaxChildDepth})
	if err != nil {
		_ = database.Close()
		return err
	}
	workflows := make(map[string]resumer.Workflow, len(d.workflows))
	for name, workflow := range d.workflows {
		workflows[name] = workflow.work
	}
	r, err := resumer.New(resumer.Config{Journal: j, Engine: engine.New(d.config.Nodes), Workflows: workflows, Workers: d.config.Workers, Interval: d.config.Interval, OnError: d.config.OnError, Settled: d.settled})
	if err != nil {
		_ = database.Close()
		return err
	}
	running, stop := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	d.mu.Lock()
	d.database, d.journal, d.resumer, d.stop, d.stopped = database, j, r, stop, stopped
	d.mu.Unlock()
	go func() {
		defer close(stopped)
		r.Run(running)
	}()
	return nil
}

func (d *DurableRuntime) close(ctx context.Context) error {
	d.mu.Lock()
	database, r, stop, stopped := d.database, d.resumer, d.stop, d.stopped
	d.database, d.journal, d.resumer = nil, nil, nil
	d.mu.Unlock()
	if r == nil {
		return nil
	}
	stop()
	err := r.Close(ctx)
	<-stopped
	return errors.Join(err, database.Close())
}

// settled hands an execution's outcome to the callers waiting for it.
func (d *DurableRuntime) settled(runID string, outcome resumer.Outcome, _ error) {
	d.mu.Lock()
	waiting := d.waiters[runID]
	delete(d.waiters, runID)
	d.mu.Unlock()
	for _, waiter := range waiting {
		waiter <- outcome
	}
}

func (d *DurableRuntime) running() (*journal.Journal, *resumer.Resumer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.journal == nil {
		return nil, nil, ErrDurableNotStarted
	}
	return d.journal, d.resumer, nil
}

// RunOptions qualify one durable run.
type RunOptions struct {
	// RequestKey identifies the request: a second Run with the same key
	// and input is the same run (empty draws a fresh one). The same key
	// with another input is refused.
	RequestKey string
	// Principal owns the run (and its child runs); empty is the system.
	Principal string
}

// DurableResult is a durable run as Run or Status sees it: State is
// completed (with Output), failed or uncertain (with ErrorCode and
// ErrorClass), canceled, suspended (waiting at a wait or a child run), or
// accepted (still running, or about to be resumed).
type DurableResult struct {
	RunID      string          `json:"runId"`
	State      string          `json:"state"`
	Output     json.RawMessage `json:"output,omitempty"`
	ErrorCode  string          `json:"errorCode,omitempty"`
	ErrorClass string          `json:"errorClass,omitempty"`
}

// Err is the run's failure as an error a trigger can classify (its
// ErrorCode and ErrorClass): nil unless the run failed, ended uncertain or
// was canceled.
func (r DurableResult) Err() error {
	switch r.State {
	case "failed", "uncertain", "canceled":
		return &DurableRunError{RunID: r.RunID, State: r.State, Code: r.ErrorCode, Class: r.ErrorClass}
	}
	return nil
}

// DurableRunError is a durable run that did not complete.
type DurableRunError struct {
	RunID, State, Code, Class string
}

func (e *DurableRunError) Error() string {
	return fmt.Sprintf("durable run %s %s: %s (%s)", e.RunID, e.State, e.Code, e.Class)
}
func (e *DurableRunError) ErrorCode() string  { return e.Code }
func (e *DurableRunError) ErrorClass() string { return e.Class }
func (e *DurableRunError) IsUncertain() bool  { return e.State == "uncertain" }

// Run admits a run of workflow with input and starts it, then waits until
// its first execution ends or ctx does: a run that completes without
// suspending returns its output; one that suspends returns suspended; and
// when ctx ends first, the run goes on and Run returns it accepted. A
// repeated request key returns the run it admitted, as it stands.
func (d *DurableRuntime) Run(ctx context.Context, workflow string, input any, options RunOptions) (DurableResult, error) {
	registered, ok := d.workflows[workflow]
	if !ok {
		return DurableResult{}, fmt.Errorf("%w: %s", ErrUnknownWorkflow, workflow)
	}
	j, r, err := d.running()
	if err != nil {
		return DurableResult{}, err
	}
	encoded, err := json.Marshal(input)
	if err != nil {
		return DurableResult{}, err
	}
	decoded, err := registered.work.DecodeInput(encoded)
	if err != nil {
		return DurableResult{}, fmt.Errorf("execution: %s input: %w", workflow, err)
	}
	key := options.RequestKey
	if key == "" {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return DurableResult{}, err
		}
		key = "request:" + hex.EncodeToString(random[:])
	}
	admission, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: key, Principal: options.Principal, Workflow: workflow, ArtifactDigest: registered.digest, Input: encoded, EngineInput: decoded})
	if err != nil {
		return DurableResult{}, err
	}
	if !admission.Accepted {
		return d.Status(ctx, admission.RunID)
	}
	waiter := make(chan resumer.Outcome, 1)
	d.mu.Lock()
	d.waiters[admission.RunID] = append(d.waiters[admission.RunID], waiter)
	d.mu.Unlock()
	if err := r.Start(ctx, admission.RunID); err != nil && !errors.Is(err, journal.ErrLeaseLost) {
		d.forget(admission.RunID, waiter)
		return DurableResult{RunID: admission.RunID, State: "accepted"}, nil
	}
	select {
	case outcome := <-waiter:
		if outcome == resumer.Suspended {
			return DurableResult{RunID: admission.RunID, State: "suspended"}, nil
		}
		return d.Status(context.WithoutCancel(ctx), admission.RunID)
	case <-ctx.Done():
		d.forget(admission.RunID, waiter)
		return DurableResult{RunID: admission.RunID, State: "accepted"}, nil
	}
}

func (d *DurableRuntime) forget(runID string, waiter chan resumer.Outcome) {
	d.mu.Lock()
	defer d.mu.Unlock()
	waiting := d.waiters[runID]
	for index, candidate := range waiting {
		if candidate == waiter {
			waiting = append(waiting[:index], waiting[index+1:]...)
			break
		}
	}
	if len(waiting) == 0 {
		delete(d.waiters, runID)
	} else {
		d.waiters[runID] = waiting
	}
}

// Status reads a durable run as it stands. A live run is accepted, or
// suspended while it has an open wait (at a wait or a child run).
func (d *DurableRuntime) Status(ctx context.Context, runID string) (DurableResult, error) {
	j, _, err := d.running()
	if err != nil {
		return DurableResult{}, err
	}
	run, err := j.Run(ctx, runID)
	if err != nil {
		return DurableResult{}, err
	}
	result := DurableResult{RunID: run.RunID, State: run.State, ErrorCode: run.ErrorCode, ErrorClass: run.ErrorClass}
	switch run.State {
	case "completed":
		result.Output = append(json.RawMessage(nil), run.Output...)
	case "accepted":
		waiting, err := j.Waiting(ctx, runID)
		if err != nil {
			return DurableResult{}, err
		}
		if waiting {
			result.State = "suspended"
		}
	}
	return result, nil
}

// SignalResult is how a signal was taken: delivered to an open wait
// (Resumed), kept for the run's next wait of that name (Accepted only), a
// repeat of one already taken (Duplicate), or Late (the run has ended).
type SignalResult struct {
	Accepted  bool `json:"accepted"`
	Resumed   bool `json:"resumed"`
	Duplicate bool `json:"duplicate"`
	Late      bool `json:"late"`
}

// Signal delivers a signal named name, with signalID identifying it, to
// the run's oldest open wait of that name (flow.Wait), or keeps it for the
// run's next one; the run resumes at once. The caller has authorized
// principal to signal the run: Signal does not decide that.
func (d *DurableRuntime) Signal(ctx context.Context, runID, name, signalID, principal string, payload json.RawMessage) (SignalResult, error) {
	j, r, err := d.running()
	if err != nil {
		return SignalResult{}, err
	}
	if len(payload) == 0 {
		payload = json.RawMessage(`null`)
	}
	taken, err := j.Signal(ctx, signal.Envelope{RunID: runID, SignalID: signalID, Name: name, Principal: principal, Payload: payload}, true)
	if err != nil {
		return SignalResult{}, err
	}
	if taken.Resumed {
		r.Wake()
	}
	return SignalResult{Accepted: taken.Accepted, Resumed: taken.Resumed, Duplicate: taken.Duplicate, Late: taken.Late}, nil
}
