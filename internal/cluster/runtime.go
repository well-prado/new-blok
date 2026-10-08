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
	"math/rand/v2"
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

// maxAdmissionAttempts bounds how often one admission re-reads capacity after
// losing its chosen slot to a concurrent admission.
const maxAdmissionAttempts = 32

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
	runID := admissionRunID(request.Tenant, request.RequestKey)
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
	// Each attempt starts from one linearizable read of both the partition's
	// and the tenant's free slots. Only that read can establish exhaustion;
	// losing a slot to a concurrent admission is contention and is retried.
	for attempt := 0; attempt < maxAdmissionAttempts; attempt++ {
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
			scope := "partition"
			if len(globalSlots) > 0 {
				scope = "tenant"
			}
			return Admission{}, &CapacityError{Partition: partition, Tenant: request.Tenant, Scope: scope, PartitionLimit: r.limits.PartitionAdmissions, TenantLimit: r.limits.TenantAdmissions, FreePartitionSlots: len(globalSlots), FreeTenantSlots: len(tenantSlots)}
		}
		// Concurrent ingress nodes see the same free set; a random choice
		// spreads them over it instead of all racing for the lowest slot.
		globalSlot := globalSlots[rand.IntN(len(globalSlots))]
		tenantSlot := tenantSlots[rand.IntN(len(tenantSlots))]
		run := RunRecord{RunID: runID, Tenant: request.Tenant, RequestKey: request.RequestKey, Workflow: request.Workflow, ArtifactDigest: workflow.Program.Digest, InputDigest: inputDigest, Input: append(json.RawMessage(nil), canonicalInput...), State: "accepted", GlobalSlot: globalSlot, TenantSlot: tenantSlot}
		// The accepted record must leave room for everything the runtime
		// adds before the run finishes, or it is admitted but can never be
		// claimed (#254).
		if err := checkEncoded(maxNonTerminalRun(run)); err != nil {
			return Admission{}, fmt.Errorf("%w: workflow input leaves no room for run metadata: %w", ErrInvalid, err)
		}
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
		if tooLargeErr := classifyTooLarge(err, callerOwnsAll); tooLargeErr != nil {
			return Admission{}, tooLargeErr
		}
		if !errors.Is(err, distributed.ErrAdmissionSlotTaken) {
			return Admission{}, fmt.Errorf("%w: %v", ErrUnavailable, err)
		}
		if waitErr := waitContext(ctx, time.Duration(rand.IntN(4*(attempt+1)))*time.Millisecond); waitErr != nil {
			return Admission{}, fmt.Errorf("%w: admission contention: %v", ErrUnavailable, waitErr)
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
	// Free slots were observed on every attempt, so capacity is not
	// exhausted: report bounded contention as retryable unavailability.
	return Admission{}, fmt.Errorf("%w: admission slot contention exceeded %d attempts", ErrUnavailable, maxAdmissionAttempts)
}

// CapacityError is a definite admission rejection. It carries the single
// linearizable capacity read that showed no free partition or tenant slot;
// Admit never reports exhausted capacity without one.
type CapacityError struct {
	Partition          string
	Tenant             string
	Scope              string // "partition" or "tenant": which bound was exhausted
	PartitionLimit     int
	TenantLimit        int
	FreePartitionSlots int
	FreeTenantSlots    int
}

func (e *CapacityError) Error() string {
	return fmt.Sprintf("%v: %s %s has no free slot (free partition=%d/%d tenant=%d/%d)", distributed.ErrAdmissionFull, e.Scope, e.Partition, e.FreePartitionSlots, e.PartitionLimit, e.FreeTenantSlots, e.TenantLimit)
}

func (e *CapacityError) Unwrap() error { return distributed.ErrAdmissionFull }

// admissionRunID is the cluster-wide run identity of a tenant's request key.
func admissionRunID(tenant, requestKey string) string {
	identity := sha256.Sum256([]byte(tenant + "\x00" + requestKey))
	return "run-" + hex.EncodeToString(identity[:16])
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
		if errors.Is(err, distributed.ErrRecordTooLarge) {
			// A run stored without #254's metadata headroom (for example by
			// an older replica) that this owner's ID pushes over the bound.
			// This owner can never claim it, so it fails instead of blocking
			// the partition; finish drops what the terminal record cannot hold.
			return r.finish(ctx, owner, selected, "failed", recordTooLargeCode, nil)
		}
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
		// failure and must not be acknowledged as terminal. A step record the
		// journal could not write because it exceeds the store bound is the
		// exception: replaying it can never fit, so it is a terminal failure
		// (#265), and the engine's persistence class does not make it
		// retryable.
		var engineErr *engine.Error
		overflow := errors.Is(runErr, ErrRecordOverflow)
		if errors.As(runErr, &engineErr) && engineErr.Class == "persistence" && !overflow {
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
		if overflow {
			// An effectful step whose result cannot be written stays
			// uncertain: its effect ran. The code names why.
			code = recordTooLargeCode
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
	consecutiveFailures := 0
	for ctx.Err() == nil {
		// The lease's TTL starts when etcd processes the grant, which is after
		// this instant: it is the earliest the lease could have started.
		acquireSent := time.Now()
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
			r.keepOwnership(ownerCtx, cancelOwner, owner, acquireSent)
		}()
		var processErr error
		for ownerCtx.Err() == nil {
			_, err := r.processOne(ownerCtx, owner)
			if errors.Is(err, ErrNoWork) {
				consecutiveFailures = 0
				if waitErr := waitContext(ownerCtx, 200*time.Millisecond); waitErr != nil {
					break
				}
				continue
			}
			if err != nil {
				processErr = err
				cancelOwner()
				break
			}
			consecutiveFailures = 0
		}
		cancelOwner()
		<-renewDone
		// Relinquish the exact fence explicitly, so another worker can take
		// the partition at once instead of after lease expiry. Release is
		// fenced: it is a no-op error if ownership was already lost.
		releaseCtx, cancelRelease := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
		_ = r.store.Release(releaseCtx, owner)
		cancelRelease()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		backoff := 250 * time.Millisecond
		if processErr != nil {
			// A processing error may be deterministic (for example a
			// registered artifact that does not match an accepted run). Back
			// off exponentially, bounded by the owner TTL, so a faulty worker
			// does not flap on the partition while healthy workers take it.
			consecutiveFailures++
			backoff = acquireRetryInterval << min(consecutiveFailures, 6)
			if backoff > r.limits.OwnerTTL {
				backoff = r.limits.OwnerTTL
			}
		}
		if err := waitContext(ctx, backoff); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// leaseSafetyMargin is how long before its lease could expire a worker stops
// acting on its partition. It covers the time the worker takes to notice the
// cancellation and any drift between the local clock and etcd's.
func leaseSafetyMargin(ttl time.Duration) time.Duration { return ttl / 4 }

// renewRetryInterval is the pause between renewal attempts after a transient
// failure. Each attempt is still bounded by how long the lease is provably
// alive.
func renewRetryInterval(ttl time.Duration) time.Duration { return ttl / 10 }

// keepOwnership renews owner's lease OwnerTTL/3 after each request that
// proved it alive until ownerCtx ends, and cancels the worker once the lease
// can no longer be proven alive (#405).
//
// etcd restarts a lease's TTL when it processes a grant or keepalive, which is
// after the client sent it, and a leader change only extends a lease. A lease
// last proven alive by a request sent at S therefore cannot expire before
// S + OwnerTTL (etcd grants at least the requested TTL, rounded up to whole
// seconds). The worker treats it as valid until S + OwnerTTL - margin. A slow
// or failed renewal is retried within that window, each attempt bounded by
// it; a timer cancels the worker at the deadline even when a renewal call is
// stuck. Anchoring the schedule to the proof, not to a free-running ticker,
// renews at once after a slow acquisition instead of letting the deadline
// pass before the first renewal is due. A renewal that proves ownership is
// gone (lease expired, owner key deleted or fenced by a successor) ends
// ownership at once. Durable writes stay fenced by etcd regardless; this
// bound only stops the worker from dispatching more work on a partition whose
// lease could have expired.
func (r *Runtime) keepOwnership(ownerCtx context.Context, cancelOwner context.CancelFunc, owner distributed.Owner, proven time.Time) {
	ttl := r.limits.OwnerTTL
	validFor := ttl - leaseSafetyMargin(ttl)
	validUntil := proven.Add(validFor)
	guard := time.AfterFunc(time.Until(validUntil), cancelOwner)
	defer guard.Stop()
	interval := ttl / 3
	next := time.NewTimer(time.Until(proven.Add(interval)))
	defer next.Stop()
	for {
		select {
		case <-ownerCtx.Done():
			return
		case <-next.C:
		}
		for {
			sent := time.Now()
			if !sent.Before(validUntil) {
				cancelOwner()
				return
			}
			renewCtx, cancel := context.WithDeadline(ownerCtx, validUntil)
			err := r.store.Renew(renewCtx, owner)
			cancel()
			if err == nil {
				if !guard.Stop() {
					// The deadline passed while this renewal was in flight and
					// the worker is already canceled.
					return
				}
				validUntil = sent.Add(validFor)
				guard.Reset(time.Until(validUntil))
				next.Reset(time.Until(sent.Add(interval)))
				break
			}
			if errors.Is(err, distributed.ErrOwnershipLost) || ownerCtx.Err() != nil {
				cancelOwner()
				return
			}
			if waitErr := waitContext(ownerCtx, min(renewRetryInterval(ttl), time.Until(validUntil))); waitErr != nil {
				return
			}
		}
	}
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
	if isTerminal(latest.State) {
		if latest.OwnerID != owner.ID || latest.Fence != owner.Token {
			return RunRecord{}, distributed.ErrOwnershipLost
		}
		return latest, nil
	}
	run.State, run.ErrorCode, run.Output = terminal, code, append(json.RawMessage(nil), output...)
	run.OwnerID, run.Fence = owner.ID, owner.Token
	finishID := transitionID("finish", run.RunID, owner.Token)
	// The terminal transition must commit, or the run keeps its slots and
	// is retried forever (#265). A record over the bound is rejected before
	// anything is sent, so the next, smaller record is tried in the same
	// transition; any other error leaves the run for a later owner.
	var lastErr error
	for _, record := range terminalRecords(run) {
		state, _ := json.Marshal(record)
		_, err := r.store.ReleaseAdmissionSlots(ctx, owner, record.RunID, record.Tenant, record.GlobalSlot, record.TenantSlot, revision, finishID, "run."+record.State, state, state)
		if errors.Is(err, distributed.ErrRecordTooLarge) {
			lastErr = err
			continue
		}
		if err != nil && !errors.Is(err, distributed.ErrAlreadyWritten) {
			return RunRecord{}, err
		}
		return record, nil
	}
	return RunRecord{}, fmt.Errorf("%w: run %s has no terminal record that fits: %w", ErrRecordOverflow, run.RunID, lastErr)
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
