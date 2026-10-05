// Package cluster composes partition ownership, durable admission and the
// engine's backend-neutral step journal into an application execution path.
package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/store/distributed"
)

var (
	ErrInvalid               = errors.New("cluster: invalid runtime request")
	ErrUnavailable           = errors.New("cluster: durable admission unavailable; retry without acknowledging")
	ErrRequestConflict       = errors.New("cluster: request key conflicts with a prior accepted input")
	ErrEffectUncertain error = uncertainEffectError{}
	ErrNoWork                = errors.New("cluster: no accepted work is available")
	ErrNotFound              = errors.New("cluster: run not found")
)

type uncertainEffectError struct{}

func (uncertainEffectError) Error() string {
	return "cluster: external effect outcome is uncertain and requires reconciliation"
}
func (uncertainEffectError) IsUncertain() bool { return true }

type Limits struct {
	Partitions          int
	PartitionAdmissions int
	TenantAdmissions    int
	OwnerTTL            time.Duration
}

const MaxInputBytes = distributed.MaxPayloadBytes - 4096

// acquireRetryInterval bounds how often a worker retries acquiring a partition
// that another owner holds or that storage could not grant.
const acquireRetryInterval = 250 * time.Millisecond

const (
	maxPartitions          = 256
	maxPartitionAdmissions = 4096
	maxTenantAdmissions    = 256
)

type Workflow struct {
	Program     contract.InternalProgram
	DecodeInput func(json.RawMessage) (any, error)
}

type Runtime struct {
	store     *distributed.Store
	engine    *engine.Engine
	workflows map[string]Workflow
	limits    Limits
}

type Submission struct {
	Tenant     string
	RequestKey string
	Workflow   string
	Input      json.RawMessage
}

type Admission struct {
	RunID    string `json:"runId"`
	Accepted bool   `json:"accepted"`
	State    string `json:"state"`
}

type RunRecord struct {
	RunID          string          `json:"runId"`
	Tenant         string          `json:"tenant"`
	RequestKey     string          `json:"requestKey"`
	Workflow       string          `json:"workflow"`
	ArtifactDigest string          `json:"artifactDigest"`
	InputDigest    string          `json:"inputDigest"`
	Input          json.RawMessage `json:"input"`
	State          string          `json:"state"`
	Output         json.RawMessage `json:"output,omitempty"`
	ErrorCode      string          `json:"errorCode,omitempty"`
	OwnerID        string          `json:"ownerId,omitempty"`
	Fence          int64           `json:"fence,omitempty"`
	GlobalSlot     string          `json:"globalSlot"`
	TenantSlot     string          `json:"tenantSlot"`
}

func New(store *distributed.Store, runner *engine.Engine, workflows map[string]Workflow, limits Limits) (*Runtime, error) {
	if store == nil || runner == nil || limits.Partitions < 1 || limits.Partitions > maxPartitions || limits.PartitionAdmissions < 1 || limits.PartitionAdmissions > maxPartitionAdmissions || limits.TenantAdmissions < 1 || limits.TenantAdmissions > maxTenantAdmissions || limits.TenantAdmissions > limits.PartitionAdmissions || limits.OwnerTTL < time.Second {
		return nil, ErrInvalid
	}
	registered := make(map[string]Workflow, len(workflows))
	for name, workflow := range workflows {
		if name == "" || workflow.Program.WorkflowID == "" || workflow.Program.Digest == "" || workflow.DecodeInput == nil {
			return nil, fmt.Errorf("%w: workflow %q needs a program digest and typed input decoder", ErrInvalid, name)
		}
		registered[name] = workflow
	}
	if len(registered) == 0 {
		return nil, fmt.Errorf("%w: at least one workflow is required", ErrInvalid)
	}
	return &Runtime{store: store, engine: runner, workflows: registered, limits: limits}, nil
}

// Partition deterministically routes a tenant to a stable partition. The
// partition count is immutable here; online movement belongs to E18-T03.
func (r *Runtime) Partition(tenant string) string {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(tenant))
	return fmt.Sprintf("p-%04d", hash.Sum64()%uint64(r.limits.Partitions))
}

// Check proves the current incarnation can be read linearly; applications
// should refuse readiness if it fails because admission requires quorum.
func (r *Runtime) Check(ctx context.Context) error {
	_, _, err := r.store.ReadState(ctx, r.Partition("__readiness__"), "readiness")
	if err != nil {
		return err
	}
	settings, err := json.Marshal(struct {
		Partitions          int `json:"partitions"`
		PartitionAdmissions int `json:"partitionAdmissions"`
		TenantAdmissions    int `json:"tenantAdmissions"`
	}{r.limits.Partitions, r.limits.PartitionAdmissions, r.limits.TenantAdmissions})
	if err != nil {
		return err
	}
	return r.store.EnsureSetting(ctx, "cluster-runtime-v1", settings)
}

// Admit acknowledges only a quorum-committed run record. Storage failures are
// fail-closed; callers must not acknowledge and may retry the same key.
func (r *Runtime) Admit(ctx context.Context, request Submission) (Admission, error) {
	if request.Tenant == "" || request.RequestKey == "" || len(request.RequestKey) > 180 || request.Workflow == "" || !json.Valid(request.Input) || len(request.Input) > MaxInputBytes {
		return Admission{}, ErrInvalid
	}
	if err := r.Check(ctx); err != nil {
		return Admission{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	workflow, exists := r.workflows[request.Workflow]
	if !exists {
		return Admission{}, fmt.Errorf("%w: workflow is not registered", ErrInvalid)
	}
	partition := r.Partition(request.Tenant)
	identity := sha256.Sum256([]byte(request.Tenant + "\x00" + request.RequestKey))
	runID := "run-" + hex.EncodeToString(identity[:16])
	decodedInput, err := workflow.DecodeInput(request.Input)
	if err != nil {
		return Admission{}, fmt.Errorf("%w: workflow input is invalid: %v", ErrInvalid, err)
	}
	canonicalInput, err := json.Marshal(decodedInput)
	if err != nil || len(canonicalInput) > distributed.MaxPayloadBytes {
		return Admission{}, fmt.Errorf("%w: workflow input cannot be persisted", ErrInvalid)
	}
	inputDigest := digest(canonicalInput)
	if record, _, err := r.readRun(ctx, partition, runID); err != nil {
		return Admission{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	} else if record.RunID != "" {
		if !sameRequest(record, request, workflow.Program.Digest, inputDigest) {
			return Admission{}, ErrRequestConflict
		}
		return Admission{RunID: runID, Accepted: false, State: record.State}, nil
	}
	for attempt := 0; attempt < 4; attempt++ {
		globalSlots, tenantSlots, err := r.store.FreeAdmissionSlots(ctx, partition, request.Tenant, r.limits.PartitionAdmissions, r.limits.TenantAdmissions)
		if err != nil {
			return Admission{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		if len(globalSlots) == 0 || len(tenantSlots) == 0 {
			existing, _, readErr := r.readRun(ctx, partition, runID)
			if readErr != nil {
				return Admission{}, fmt.Errorf("%w: reconcile full-capacity duplicate: %v", ErrUnavailable, readErr)
			}
			if existing.RunID != "" {
				if !sameRequest(existing, request, workflow.Program.Digest, inputDigest) {
					return Admission{}, ErrRequestConflict
				}
				return Admission{RunID: runID, Accepted: false, State: existing.State}, nil
			}
			return Admission{}, distributed.ErrAdmissionFull
		}
		run := RunRecord{RunID: runID, Tenant: request.Tenant, RequestKey: request.RequestKey, Workflow: request.Workflow, ArtifactDigest: workflow.Program.Digest, InputDigest: inputDigest, Input: append(json.RawMessage(nil), canonicalInput...), State: "accepted", GlobalSlot: globalSlots[0], TenantSlot: tenantSlots[0]}
		encoded, _ := json.Marshal(run)
		// Capacity slot choices are transaction-local allocation details. Keep
		// them out of the stable admission event identity so concurrent ingress
		// retries for the same request reconcile to the winning projection.
		admissionIdentity, _ := json.Marshal(struct {
			RunID          string          `json:"runId"`
			Tenant         string          `json:"tenant"`
			RequestKey     string          `json:"requestKey"`
			Workflow       string          `json:"workflow"`
			ArtifactDigest string          `json:"artifactDigest"`
			InputDigest    string          `json:"inputDigest"`
			Input          json.RawMessage `json:"input"`
		}{run.RunID, run.Tenant, run.RequestKey, run.Workflow, run.ArtifactDigest, run.InputDigest, run.Input})
		err = r.store.CommitAdmission(ctx, partition, request.Tenant, run.GlobalSlot, run.TenantSlot, runID, encoded, admissionIdentity)
		if err == nil {
			return Admission{RunID: runID, Accepted: true, State: run.State}, nil
		}
		if errors.Is(err, distributed.ErrAlreadyWritten) || errors.Is(err, distributed.ErrAdmissionConflict) {
			// A concurrent ingress committed this request identity first.
			// Reconcile against its committed record: identical input is a
			// duplicate, any other input is a definite conflict.
			existing, _, readErr := r.readRun(ctx, partition, runID)
			if readErr != nil {
				return Admission{}, fmt.Errorf("%w: reconcile duplicate admission: %v", ErrUnavailable, readErr)
			}
			if !sameRequest(existing, request, workflow.Program.Digest, inputDigest) {
				return Admission{}, ErrRequestConflict
			}
			return Admission{RunID: runID, Accepted: false, State: existing.State}, nil
		}
		if !errors.Is(err, distributed.ErrAdmissionFull) {
			return Admission{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
	}
	if existing, _, readErr := r.readRun(ctx, partition, runID); readErr != nil {
		return Admission{}, fmt.Errorf("%w: reconcile admission contention: %v", ErrUnavailable, readErr)
	} else if existing.RunID != "" {
		if !sameRequest(existing, request, workflow.Program.Digest, inputDigest) {
			return Admission{}, ErrRequestConflict
		}
		return Admission{RunID: runID, Accepted: false, State: existing.State}, nil
	}
	return Admission{}, distributed.ErrAdmissionFull
}

func (r *Runtime) readRun(ctx context.Context, partition, runID string) (RunRecord, int64, error) {
	data, revision, err := r.store.ReadState(ctx, partition, runID)
	if err != nil {
		return RunRecord{}, 0, err
	}
	if revision == 0 {
		return RunRecord{}, 0, nil
	}
	var record RunRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return RunRecord{}, 0, fmt.Errorf("cluster: decode run state: %w", err)
	}
	return record, revision, nil
}

// GetRun reads a tenant-scoped persisted execution record.
func (r *Runtime) GetRun(ctx context.Context, tenant, runID string) (RunRecord, error) {
	if tenant == "" || runID == "" {
		return RunRecord{}, ErrInvalid
	}
	record, _, err := r.readRun(ctx, r.Partition(tenant), runID)
	if err != nil {
		return RunRecord{}, err
	}
	if record.RunID == "" || record.Tenant != tenant {
		return RunRecord{}, ErrNotFound
	}
	return record, nil
}

func sameRequest(record RunRecord, request Submission, artifact, inputDigest string) bool {
	return record.RunID != "" && record.Tenant == request.Tenant && record.RequestKey == request.RequestKey && record.Workflow == request.Workflow && record.ArtifactDigest == artifact && record.InputDigest == inputDigest
}

// processOne claims and executes one persisted run under a current owner.
// Result publication and slot release are a single fenced transaction.
func (r *Runtime) processOne(ctx context.Context, owner distributed.Owner) (RunRecord, error) {
	if _, err := r.FireDueWaits(ctx, owner, time.Now().UTC(), 64); err != nil {
		return RunRecord{}, fmt.Errorf("cluster: fire due timers: %w", err)
	}
	runIDs, err := r.store.ListActiveRunIDs(ctx, owner.Partition, r.limits.PartitionAdmissions)
	if err != nil {
		return RunRecord{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	candidates := make([]RunRecord, 0)
	for _, runID := range runIDs {
		record, _, err := r.readRun(ctx, owner.Partition, runID)
		if err != nil {
			return RunRecord{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		if record.RunID == "" || record.RunID != runID {
			return RunRecord{}, fmt.Errorf("cluster: active admission slot references missing run %q", runID)
		}
		if record.State == "accepted" || record.State == "running" {
			candidates = append(candidates, record)
		}
	}
	if len(candidates) == 0 {
		return RunRecord{}, ErrNoWork
	}
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].RunID < candidates[j].RunID })
	selected, err := r.fairCandidate(ctx, owner, candidates)
	if err != nil {
		return RunRecord{}, err
	}
	workflow, ok := r.workflows[selected.Workflow]
	if !ok || workflow.Program.Digest != selected.ArtifactDigest {
		return RunRecord{}, fmt.Errorf("cluster: registered artifact mismatch for run %s workflow=%q persisted=%q registered=%q", selected.RunID, selected.Workflow, selected.ArtifactDigest, workflow.Program.Digest)
	}
	current, revision, err := r.store.ReadState(ctx, owner.Partition, selected.RunID)
	if err != nil || revision == 0 {
		return RunRecord{}, fmt.Errorf("%w: read selected run: %v", ErrUnavailable, err)
	}
	if err := json.Unmarshal(current, &selected); err != nil {
		return RunRecord{}, err
	}
	selected.State, selected.OwnerID, selected.Fence = "running", owner.ID, owner.Token
	state, _ := json.Marshal(selected)
	claimID := transitionID("claim", selected.RunID+"\x00revision="+strconv.FormatInt(revision, 10), owner.Token)
	if _, err := r.store.CommitFencedState(ctx, owner, selected.RunID, revision, claimID, "run.claimed", state, state); err != nil && !errors.Is(err, distributed.ErrAlreadyWritten) {
		return RunRecord{}, err
	}
	input, err := workflow.DecodeInput(selected.Input)
	if err != nil {
		return r.finish(ctx, owner, selected, "failed", "input_decode", nil)
	}
	journal := &runStepJournal{runtime: r, owner: owner, record: selected}
	result, runErr := r.engine.RunJournaled(ctx, workflow.Program, input, selected.RunID, journal)
	if runErr != nil {
		var suspended interface{ IsSuspended() bool }
		if errors.As(runErr, &suspended) && suspended.IsSuspended() {
			var engineErr *engine.Error
			if !errors.As(runErr, &engineErr) || engineErr.Step == "" {
				return RunRecord{}, fmt.Errorf("cluster: suspended execution did not identify its wait step")
			}
			if err := r.suspend(ctx, owner, selected, engineErr.Step); err != nil {
				return RunRecord{}, err
			}
			return RunRecord{}, ErrNoWork
		}
		// Accepted work is durable. A transient journal/quorum failure or
		// cooperative cancellation must leave its admission slots intact so a
		// later owner can replay the committed prefix; it is not a business
		// failure and must not be acknowledged as terminal.
		var engineErr *engine.Error
		if errors.As(runErr, &engineErr) && engineErr.Class == "persistence" {
			return RunRecord{}, runErr
		}
		if errors.Is(runErr, context.Canceled) || errors.Is(runErr, context.DeadlineExceeded) {
			return RunRecord{}, runErr
		}
		terminal, code := "failed", "execution_failed"
		if errors.As(runErr, &engineErr) {
			code = engineErr.Code
		}
		var uncertainty interface{ IsUncertain() bool }
		if errors.As(runErr, &uncertainty) && uncertainty.IsUncertain() {
			terminal = "uncertain"
			if engineErr == nil {
				code = "effect_outcome_uncertain"
			}
		}
		return r.finish(ctx, owner, selected, terminal, code, nil)
	}
	output, err := json.Marshal(result.Output)
	if err != nil {
		return r.finish(ctx, owner, selected, "failed", "output_encode", nil)
	}
	return r.finish(ctx, owner, selected, "completed", "", output)
}

func (r *Runtime) suspend(ctx context.Context, owner distributed.Owner, run RunRecord, stepID string) error {
	latest, revision, err := r.readRun(ctx, owner.Partition, run.RunID)
	if err != nil || revision == 0 {
		return fmt.Errorf("%w: read run before suspension: %v", ErrUnavailable, err)
	}
	if latest.OwnerID != owner.ID || latest.Fence != owner.Token {
		return distributed.ErrOwnershipLost
	}
	if latest.State == "waiting" {
		return nil
	}
	if latest.State == "accepted" {
		// A signal or timer may win after the wait record is committed but
		// before this worker publishes its suspension transition.
		return nil
	}
	if latest.State != "running" {
		return fmt.Errorf("cluster: cannot suspend run in state %q", latest.State)
	}
	latest.State = "waiting"
	state, _ := json.Marshal(latest)
	transition := transitionID("suspend", run.RunID+"\x00"+stepID, owner.Token)
	if _, err := r.store.CommitFencedState(ctx, owner, run.RunID, revision, transition, "run.waiting", state, state); err != nil {
		if errors.Is(err, distributed.ErrAlreadyWritten) {
			current, _, readErr := r.readRun(ctx, owner.Partition, run.RunID)
			if readErr == nil && current.State == "waiting" && current.OwnerID == owner.ID && current.Fence == owner.Token {
				return nil
			}
		}
		return err
	}
	return nil
}

// Run owns every configured stable partition with one worker each. Thus the
// maximum concurrent engine runs is bounded by Partitions and each tenant is
// serialized within its partition. Lease loss cancels the local execution;
// every subsequent durable transition is still checked by etcd fencing.
func (r *Runtime) Run(ctx context.Context, ownerID string) error {
	if ownerID == "" {
		return ErrInvalid
	}
	if err := r.Check(ctx); err != nil {
		return fmt.Errorf("cluster: runtime configuration unavailable: %w", err)
	}
	var workers sync.WaitGroup
	errorsSeen := make(chan error, r.limits.Partitions)
	for partition := 0; partition < r.limits.Partitions; partition++ {
		name := fmt.Sprintf("p-%04d", partition)
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := r.runPartition(ctx, name, ownerID); err != nil && ctx.Err() == nil {
				errorsSeen <- err
			}
		}()
	}
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		return err
	}
	return ctx.Err()
}

func (r *Runtime) runPartition(ctx context.Context, partition, ownerID string) error {
	for ctx.Err() == nil {
		owner, err := r.store.Acquire(ctx, partition, ownerID, r.limits.OwnerTTL)
		if err != nil {
			// Whether another owner holds the partition or storage refused the
			// grant, retry at a bounded rate: a standby worker must not spin
			// lease grants against etcd while it waits for a takeover.
			if waitErr := waitContext(ctx, acquireRetryInterval); waitErr != nil {
				return waitErr
			}
			continue
		}
		ownerCtx, cancelOwner := context.WithCancel(ctx)
		renewDone := make(chan struct{})
		go func() {
			defer close(renewDone)
			ticker := time.NewTicker(r.limits.OwnerTTL / 3)
			defer ticker.Stop()
			for {
				select {
				case <-ownerCtx.Done():
					return
				case <-ticker.C:
					renewCtx, cancel := context.WithTimeout(ownerCtx, r.limits.OwnerTTL/4)
					if renewErr := r.store.Renew(renewCtx, owner); renewErr != nil {
						cancel()
						cancelOwner()
						return
					}
					cancel()
				}
			}
		}()
		for ownerCtx.Err() == nil {
			_, err := r.processOne(ownerCtx, owner)
			if errors.Is(err, ErrNoWork) {
				if waitErr := waitContext(ownerCtx, 200*time.Millisecond); waitErr != nil {
					break
				}
				continue
			}
			if err != nil {
				cancelOwner()
				break
			}
		}
		cancelOwner()
		<-renewDone
		if ctx.Err() != nil {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
			_ = r.store.Release(releaseCtx, owner)
			cancel()
			return ctx.Err()
		}
		if err := waitContext(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
	return ctx.Err()
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *Runtime) finish(ctx context.Context, owner distributed.Owner, run RunRecord, terminal, code string, output json.RawMessage) (RunRecord, error) {
	latest, revision, err := r.readRun(ctx, owner.Partition, run.RunID)
	if err != nil || revision == 0 {
		return RunRecord{}, fmt.Errorf("%w: read terminal state: %v", ErrUnavailable, err)
	}
	if latest.State == "completed" || latest.State == "failed" || latest.State == "uncertain" {
		if latest.OwnerID != owner.ID || latest.Fence != owner.Token {
			return RunRecord{}, distributed.ErrOwnershipLost
		}
		return latest, nil
	}
	run.State, run.ErrorCode, run.Output = terminal, code, append(json.RawMessage(nil), output...)
	run.OwnerID, run.Fence = owner.ID, owner.Token
	state, _ := json.Marshal(run)
	finishID := transitionID("finish", run.RunID, owner.Token)
	if _, err := r.store.ReleaseAdmissionSlots(ctx, owner, run.RunID, run.Tenant, run.GlobalSlot, run.TenantSlot, revision, finishID, "run."+terminal, state, state); err != nil && !errors.Is(err, distributed.ErrAlreadyWritten) {
		return RunRecord{}, err
	}
	return run, nil
}

func (r *Runtime) fairCandidate(ctx context.Context, owner distributed.Owner, records []RunRecord) (RunRecord, error) {
	cursorData, revision, err := r.store.ReadState(ctx, owner.Partition, "scheduler-cursor")
	if err != nil {
		return RunRecord{}, fmt.Errorf("%w: read durable tenant scheduler cursor: %v", ErrUnavailable, err)
	}
	var cursor struct {
		LastTenant string `json:"lastTenant"`
	}
	if revision > 0 {
		if err := json.Unmarshal(cursorData, &cursor); err != nil {
			return RunRecord{}, fmt.Errorf("cluster: decode durable tenant scheduler cursor: %w", err)
		}
	}
	selected := nextFairCandidate(cursor.LastTenant, records)
	cursor.LastTenant = selected.Tenant
	encoded, _ := json.Marshal(cursor)
	eventID := transitionID("scheduler-cursor", owner.Partition+"\x00"+strconv.FormatInt(revision, 10)+"\x00"+selected.Tenant, owner.Token)
	if _, err := r.store.CommitFencedState(ctx, owner, "scheduler-cursor", revision, eventID, "scheduler.cursor", encoded, encoded); err != nil {
		if errors.Is(err, distributed.ErrAlreadyWritten) {
			currentData, currentRevision, readErr := r.store.ReadState(ctx, owner.Partition, "scheduler-cursor")
			var current struct {
				LastTenant string `json:"lastTenant"`
			}
			if readErr == nil && currentRevision > revision && json.Unmarshal(currentData, &current) == nil && current.LastTenant == selected.Tenant {
				return selected, nil
			}
		}
		return RunRecord{}, fmt.Errorf("cluster: persist durable tenant scheduler cursor: %w", err)
	}
	return selected, nil
}

func nextFairCandidate(lastTenant string, records []RunRecord) RunRecord {
	byTenant := make(map[string]RunRecord, len(records))
	for _, record := range records {
		if _, exists := byTenant[record.Tenant]; !exists {
			byTenant[record.Tenant] = record
		}
	}
	tenants := make([]string, 0, len(byTenant))
	for tenant := range byTenant {
		tenants = append(tenants, tenant)
	}
	sort.Strings(tenants)
	selected := tenants[0]
	for _, tenant := range tenants {
		if tenant > lastTenant {
			selected = tenant
			break
		}
	}
	return byTenant[selected]
}

func digest(data []byte) string {
	hash := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(hash[:])
}

func transitionID(kind, runID string, fence int64) string {
	hash := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", kind, runID, fence)))
	return kind + "-" + hex.EncodeToString(hash[:16])
}
