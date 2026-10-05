package cluster

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/store/distributed"
)

// MaxInspectionTenants bounds the tenants one inspection read may search for
// a run: one linearizable run read each.
const MaxInspectionTenants = 16

// errInspectionStop ends an inspection replay at the first step the step
// journal has no committed result for. It never reaches a caller.
var errInspectionStop = errors.New("cluster: inspection replay reached the end of the committed journal")

// InspectionSource reads durable cluster runs for inspection (ADR 0016,
// #263). It implements inspection.Source and inspect.RunOwnerSource over
// store/distributed, so the live event endpoint can reconstruct a cluster run
// and follow it by polling.
//
// A run is owned by its tenant, as its durable run record names it; the
// reader never becomes the owner. Tenants, when set, names the tenants whose
// runs reader may look up (at most MaxInspectionTenants); nil allows only
// reader's own runs, reader being the tenant. A run is found only in the
// partition of one of those tenants and only when its record names that
// tenant, so another tenant's run is not found, even in the same partition.
//
// Steps are the step journal's committed facts, in program order. Their
// operation keys digest each step's resolved input, so the source replays
// the run through the engine's journaled interpreter with a read-only
// journal: a committed step restores its output, as on takeover, and the
// replay stops at the first step without one. The read-only journal refuses
// every dispatch, so inspection never invokes a node and never writes.
type InspectionSource struct {
	runtime *Runtime
	tenants func(reader string) []string
}

// InspectionSource returns the durable inspection source of this runtime.
func (r *Runtime) InspectionSource(tenants func(reader string) []string) *InspectionSource {
	return &InspectionSource{runtime: r, tenants: tenants}
}

// RunOwner names the durable owner of runID, the tenant its run record
// names, for a reader allowed to see it.
func (s *InspectionSource) RunOwner(ctx context.Context, reader, runID string) (string, error) {
	record, _, err := s.locate(ctx, reader, runID)
	if err != nil {
		return "", err
	}
	return record.Tenant, nil
}

// ReadInspection reads one bounded page of runID for reader. The run record
// is read first and authorized by tenant; payloads are projected only for
// the selected fields and capped at maxPayload. The replay reads at most one
// step record per program instruction, and stops after offset+limit+1
// matching steps, so a later page is announced without reading it.
func (s *InspectionSource) ReadInspection(ctx context.Context, reader, runID, stepID string, offset, limit int, fields map[inspection.Field]bool, maxPayload int) (inspection.Run, []inspection.Step, int, error) {
	if reader == "" || runID == "" || offset < 0 || limit < 1 || limit > 200 || maxPayload < inspection.MinPayloadBytes {
		return inspection.Run{}, nil, 0, ErrNotFound
	}
	record, partition, err := s.locate(ctx, reader, runID)
	if err != nil {
		return inspection.Run{}, nil, 0, err
	}
	run := inspection.Run{ID: record.RunID, Workflow: record.Workflow, Status: runStatus(record.State), ErrorCode: record.ErrorCode}
	if fields[inspection.FieldInput] {
		run.Input = boundedRaw(record.Input, maxPayload)
	}
	if fields[inspection.FieldOutput] {
		run.Output = boundedRaw(record.Output, maxPayload)
	}
	steps := s.replay(ctx, partition, record, stepID, offset+limit+1, fields, maxPayload)
	total := len(steps)
	if offset >= total {
		return run, []inspection.Step{}, total, nil
	}
	return run, steps[offset:min(offset+limit, total)], total, nil
}

// locate finds runID among the tenants reader may look up. The owner is the
// record's tenant, checked against the tenant whose partition it was read
// from; nothing about it comes from the reader.
func (s *InspectionSource) locate(ctx context.Context, reader, runID string) (RunRecord, string, error) {
	if s == nil || s.runtime == nil || reader == "" || runID == "" {
		return RunRecord{}, "", ErrNotFound
	}
	tenants := []string{reader}
	if s.tenants != nil {
		tenants = s.tenants(reader)
	}
	if len(tenants) > MaxInspectionTenants {
		return RunRecord{}, "", ErrNotFound
	}
	for _, tenant := range tenants {
		if tenant == "" {
			continue
		}
		partition := s.runtime.Partition(tenant)
		record, _, err := s.runtime.readRun(ctx, partition, runID)
		if err != nil {
			return RunRecord{}, "", err
		}
		if record.RunID == runID && record.Tenant == tenant {
			return record, partition, nil
		}
	}
	return RunRecord{}, "", ErrNotFound
}

func (s *InspectionSource) replay(ctx context.Context, partition string, record RunRecord, stepID string, budget int, fields map[inspection.Field]bool, maxPayload int) []inspection.Step {
	workflow, ok := s.runtime.workflows[record.Workflow]
	if !ok || workflow.Program.Digest != record.ArtifactDigest {
		// Without the exact registered artifact the operation keys cannot
		// be derived; the run is reported without steps.
		return nil
	}
	input, err := workflow.DecodeInput(record.Input)
	if err != nil {
		// Includes a terminal record whose input was dropped to fit (#265).
		return nil
	}
	journal := &inspectionJournal{store: s.runtime.store, partition: partition, record: record, stepID: stepID, budget: budget, fields: fields, maxPayload: maxPayload}
	_, _ = s.runtime.engine.RunJournaled(ctx, workflow.Program, input, record.RunID, journal)
	return journal.steps
}

// inspectionJournal is a read-only engine.StepJournal and WaitJournal. It
// returns committed step outputs and resolved waits, exactly as the runtime's
// journal does on takeover, and records each fact it reads. Every other
// transition, including dispatch, stops the replay.
type inspectionJournal struct {
	store      *distributed.Store
	partition  string
	record     RunRecord
	stepID     string
	budget     int
	fields     map[inspection.Field]bool
	maxPayload int
	steps      []inspection.Step
	matched    int
}

func (j *inspectionJournal) VerifyRun(_ context.Context, runID, artifact, inputDigest string) error {
	if runID != j.record.RunID || artifact != j.record.ArtifactDigest || inputDigest != j.record.InputDigest {
		return errInspectionStop
	}
	return nil
}

func (j *inspectionJournal) Load(ctx context.Context, identity engine.StepIdentity) (json.RawMessage, bool, error) {
	if j.matched >= j.budget || !sameStepIdentity(identity, j.record, identity.StepID) {
		return nil, false, errInspectionStop
	}
	data, revision, err := j.store.ReadState(ctx, j.partition, stepStateID(identity.OperationKey))
	if err != nil || revision == 0 {
		return nil, false, errInspectionStop
	}
	var persisted stepRecord
	if json.Unmarshal(data, &persisted) != nil || persisted.Identity != identity {
		return nil, false, errInspectionStop
	}
	step := inspection.Step{ID: identity.StepID, Status: j.stepStatus(persisted), Attempt: persisted.AttemptNumber}
	attempt := inspection.Attempt{ID: persisted.CurrentAttempt, Number: persisted.AttemptNumber, Status: step.Status}
	if persisted.State == "committed" && j.fields[inspection.FieldOutput] {
		step.Output = boundedRaw(persisted.Output, j.maxPayload)
		attempt.Output = step.Output
	}
	step.Attempts = []inspection.Attempt{attempt}
	j.add(step)
	if persisted.State != "committed" {
		return nil, false, errInspectionStop
	}
	return append(json.RawMessage(nil), persisted.Output...), true, nil
}

// stepStatus maps a step record to a status. A dispatched step is in flight
// while its run is; once the run is terminal it can never complete, and an
// effectful one is uncertain, as takeover would have marked it.
func (j *inspectionJournal) stepStatus(persisted stepRecord) inspection.Status {
	switch persisted.State {
	case "committed":
		return inspection.StatusCompleted
	case "uncertain":
		return inspection.StatusUncertain
	case "retryable":
		return inspection.StatusFailed
	}
	if isTerminal(j.record.State) {
		if len(persisted.Effects) > 0 {
			return inspection.StatusUncertain
		}
		return inspection.StatusFailed
	}
	return inspection.StatusRunning
}

func (j *inspectionJournal) Await(ctx context.Context, identity engine.WaitIdentity) (engine.WaitResult, bool, error) {
	if j.matched >= j.budget || !sameStepIdentity(identity.Step, j.record, identity.Step.StepID) {
		return engine.WaitResult{}, false, errInspectionStop
	}
	data, revision, err := j.store.ReadState(ctx, j.partition, waitStateID(j.record.Tenant, WaitIDFor(j.record.RunID, identity.Step.StepID)))
	if err != nil || revision == 0 {
		return engine.WaitResult{}, false, errInspectionStop
	}
	var wait WaitRecord
	if json.Unmarshal(data, &wait) != nil || wait.RunID != j.record.RunID || wait.Tenant != j.record.Tenant || wait.OperationKey != identity.Step.OperationKey {
		return engine.WaitResult{}, false, errInspectionStop
	}
	step := inspection.Step{ID: identity.Step.StepID, Attempt: 1}
	var result engine.WaitResult
	switch wait.State {
	case "waiting":
		step.Status = inspection.StatusSuspended
	case "signaled":
		step.Status = inspection.StatusCompleted
		result = engine.WaitResult{SignalID: wait.SignalID, Payload: append(json.RawMessage(nil), wait.Payload...)}
	case "timed_out":
		step.Status = inspection.StatusCompleted
		result = engine.WaitResult{TimedOut: true}
	default:
		return engine.WaitResult{}, false, errInspectionStop
	}
	if step.Status == inspection.StatusCompleted && j.fields[inspection.FieldOutput] {
		encoded, _ := json.Marshal(result)
		step.Output = boundedRaw(encoded, j.maxPayload)
	}
	j.add(step)
	if step.Status != inspection.StatusCompleted {
		return engine.WaitResult{}, false, errInspectionStop
	}
	return result, true, nil
}

// Begin, Complete and Fail are the transitions of a live attempt. A replay
// for inspection never makes one: refusing Begin is what keeps the engine
// from invoking a node.
func (j *inspectionJournal) Begin(context.Context, engine.StepIdentity, json.RawMessage, []string) (engine.StepAttempt, error) {
	return engine.StepAttempt{}, errInspectionStop
}

func (j *inspectionJournal) Complete(context.Context, engine.StepAttempt, json.RawMessage) error {
	return errInspectionStop
}

func (j *inspectionJournal) Fail(context.Context, engine.StepAttempt, []string, error) error {
	return errInspectionStop
}

func (j *inspectionJournal) add(step inspection.Step) {
	if j.stepID != "" && step.ID != j.stepID {
		return
	}
	j.steps = append(j.steps, step)
	j.matched++
}

func runStatus(state string) inspection.Status {
	switch state {
	case "completed":
		return inspection.StatusCompleted
	case "failed":
		return inspection.StatusFailed
	case "uncertain":
		return inspection.StatusUncertain
	case "waiting":
		return inspection.StatusSuspended
	}
	// accepted (queued, or re-admitted by a signal or timer) and running.
	return inspection.StatusRunning
}

// boundedRaw copies a stored JSON value, replacing one over limit with the
// truncation marker. A missing or null value is omitted.
func boundedRaw(raw json.RawMessage, limit int) json.RawMessage {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	if len(raw) > limit {
		return json.RawMessage(`{"$truncated":true}`)
	}
	return append(json.RawMessage(nil), raw...)
}
