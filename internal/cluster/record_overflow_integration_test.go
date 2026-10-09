package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/clustertest"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/distributed"
)

// The tests in this file cover #265 against the real three-voter etcd
// cluster: a record the runtime itself grows past distributed.MaxPayloadBytes
// (a step output, a step dispatch, a terminal run record, or the run record of
// a run stored without #254's metadata headroom) must end its run as a
// classified terminal failure within bounded time. It must not leave the run
// running, re-execute its node, or hold its tenant's and partition's place.

type overflowInput struct {
	Tag  string `json:"tag"`
	Size int    `json:"size"`
	Text string `json:"text"`
}

type overflowOutput struct {
	Text string `json:"text"`
}

const (
	overflowInputSchema  = `{"type":"object","properties":{"tag":{"type":"string"},"size":{"type":"integer"},"text":{"type":"string"}},"required":["tag","size","text"],"additionalProperties":false}`
	overflowOutputSchema = `{"type":"object","properties":{"text":{"type":"string"}},"required":["text"],"additionalProperties":false}`
	// headroomlessOwner is the longest owner ID the store accepts at its
	// worst encoding: the owner whose claim grows a run record the most.
	headroomlessOwnerBytes = 180
)

type overflowFixture struct {
	FixtureVersion int    `json:"fixtureVersion"`
	Synthetic      bool   `json:"synthetic"`
	Name           string `json:"name"`
	Limits         struct {
		Partitions          int `json:"partitions"`
		PartitionAdmissions int `json:"partitionAdmissions"`
		TenantAdmissions    int `json:"tenantAdmissions"`
		OwnerTTLMillis      int `json:"ownerTTLMillis"`
	} `json:"limits"`
	OversizedOutputBytes int `json:"oversizedOutputBytes"`
	FinishInputBytes     int `json:"finishInputBytes"`
	TimerTimeoutMillis   int `json:"timerTimeoutMillis"`
	MaxTerminalMillis    int `json:"maxTerminalMillis"`
	Expected             struct {
		State                   string `json:"state"`
		EffectfulState          string `json:"effectfulState"`
		ErrorCode               string `json:"errorCode"`
		OversizedInvocations    int64  `json:"oversizedInvocations"`
		DispatchInvocations     int64  `json:"dispatchInvocations"`
		HeadroomlessInvocations int64  `json:"headroomlessInvocations"`
		FollowUpState           string `json:"followUpState"`
		FollowUpOutput          string `json:"followUpOutput"`
		FollowUpInvocations     int64  `json:"followUpInvocations"`
		ActiveRunsAfter         int    `json:"activeRunsAfter"`
		DueTimersAfter          int    `json:"dueTimersAfter"`
		WaitStateAfterTimer     string `json:"waitStateAfterTimer"`
	} `json:"expected"`
	Outage struct {
		FailureClass          string `json:"failureClass"`
		StateAfterOutage      string `json:"stateAfterOutage"`
		ActiveRunsAfterOutage int    `json:"activeRunsAfterOutage"`
		FinalState            string `json:"finalState"`
		FinalOutput           string `json:"finalOutput"`
		MinInvocations        int64  `json:"minInvocations"`
		MaxInvocations        int64  `json:"maxInvocations"`
	} `json:"outage"`
}

type overflowHarness struct {
	t         *testing.T
	ctx       context.Context
	fixture   overflowFixture
	store     *distributed.Store
	runtime   *Runtime
	tenant    string
	partition string
	calls     sync.Map // tag -> *atomic.Int64
}

func newOverflowHarness(t *testing.T) *overflowHarness {
	t.Helper()
	h := &overflowHarness{t: t}
	readDistributedFixture(t, "record-overflow-fixtures.json", &h.fixture)
	if !h.fixture.Synthetic || h.fixture.OversizedOutputBytes <= distributed.MaxPayloadBytes || 2*h.fixture.FinishInputBytes <= distributed.MaxPayloadBytes || h.fixture.FinishInputBytes >= distributed.MaxPayloadBytes-4096 {
		t.Fatalf("fixture %+v does not span the store bound %d", h.fixture, distributed.MaxPayloadBytes)
	}
	h.store = integrationDistributedStore(t)
	count := func(tag string) {
		counter, _ := h.calls.LoadOrStore(tag, new(atomic.Int64))
		counter.(*atomic.Int64).Add(1)
	}
	// produce is pure: output = Size 'a's followed by Text.
	produce := node.MustDefine("fixture/overflow-produce", "1.0.0", func(_ context.Context, input overflowInput) (overflowOutput, error) {
		count(input.Tag)
		return overflowOutput{Text: strings.Repeat("a", input.Size) + input.Text}, nil
	}, node.Description("pure node whose output size the caller chooses"), node.Schemas([]byte(overflowInputSchema), []byte(overflowOutputSchema)), node.Pure()).Any()
	// dispatch declares an effect whose name alone makes its dispatch record
	// exceed the bound. Effect names are trusted application metadata and
	// the descriptor does not bound them.
	dispatch := node.MustDefine("fixture/overflow-dispatch", "1.0.0", func(_ context.Context, input overflowInput) (overflowOutput, error) {
		count(input.Tag)
		return overflowOutput{Text: input.Text}, nil
	}, node.Description("effectful node with an oversized declared effect"), node.Schemas([]byte(overflowInputSchema), []byte(overflowOutputSchema)), node.Effects("fixture:"+strings.Repeat("e", distributed.MaxPayloadBytes))).Any()
	effectOutput := node.MustDefine("fixture/overflow-effect-output", "1.0.0", func(_ context.Context, input overflowInput) (overflowOutput, error) {
		count(input.Tag)
		return overflowOutput{Text: strings.Repeat("a", input.Size)}, nil
	}, node.Description("effectful node whose output the caller sizes"), node.Schemas([]byte(overflowInputSchema), []byte(overflowOutputSchema)), node.Effects("fixture:overflow-effect-output")).Any()
	afterWait := node.MustDefine("fixture/overflow-after-wait", "1.0.0", func(_ context.Context, _ engine.WaitResult) (overflowOutput, error) {
		count("after-wait")
		return overflowOutput{Text: "resumed"}, nil
	}, node.Description("pure node after a durable timer"), node.Schemas([]byte(waitConsumerSchema), []byte(overflowOutputSchema)), node.Pure()).Any()
	program := func(id, digit string, instructions ...contract.InternalInstruction) Workflow {
		return Workflow{Program: contract.InternalProgram{WorkflowID: id, Digest: "sha256:" + strings.Repeat(digit, 64), Instructions: instructions}, DecodeInput: decodeTyped[overflowInput]}
	}
	workflows := map[string]Workflow{
		"overflow-output": program("overflow-output", "a",
			contract.InternalInstruction{Index: 0, ID: "produce", Kind: "call", Node: "fixture/overflow-produce"},
			contract.InternalInstruction{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "produce"}}}),
		"overflow-dispatch": program("overflow-dispatch", "b",
			contract.InternalInstruction{Index: 0, ID: "effect", Kind: "call", Node: "fixture/overflow-dispatch"},
			contract.InternalInstruction{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "effect"}}}),
		"overflow-effect-output": program("overflow-effect-output", "d",
			contract.InternalInstruction{Index: 0, ID: "effect", Kind: "call", Node: "fixture/overflow-effect-output"},
			contract.InternalInstruction{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "effect"}}}),
		"overflow-timer": program("overflow-timer", "c",
			contract.InternalInstruction{Index: 0, ID: "delay", Kind: "wait", Wait: &contract.WaitInstruction{Name: "delay", TimeoutMillis: int64(h.fixture.TimerTimeoutMillis)}},
			contract.InternalInstruction{Index: 1, ID: "after", Kind: "call", Node: "fixture/overflow-after-wait", References: []contract.Reference{{Step: "delay"}}},
			contract.InternalInstruction{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "after"}}}),
	}
	limits := h.fixture.Limits
	runtime, err := New(h.store, engine.New(map[string]node.Any{"fixture/overflow-produce": produce, "fixture/overflow-dispatch": dispatch, "fixture/overflow-effect-output": effectOutput, "fixture/overflow-after-wait": afterWait}), workflows, Limits{Partitions: limits.Partitions, PartitionAdmissions: limits.PartitionAdmissions, TenantAdmissions: limits.TenantAdmissions, OwnerTTL: time.Duration(limits.OwnerTTLMillis) * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	h.runtime = runtime
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	h.ctx = ctx
	if err := runtime.Check(ctx); err != nil {
		t.Fatal(err)
	}
	h.partition = "p-0000"
	h.tenant = tenantsInPartition(runtime, h.partition, "overflow-tenant", 1)[0]
	return h
}

func (h *overflowHarness) invocations(tag string) int64 {
	counter, ok := h.calls.Load(tag)
	if !ok {
		return 0
	}
	return counter.(*atomic.Int64).Load()
}

func (h *overflowHarness) admit(workflow, key string, input overflowInput) (string, json.RawMessage) {
	h.t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		h.t.Fatal(err)
	}
	admission, err := h.runtime.Admit(h.ctx, Submission{Tenant: h.tenant, RequestKey: key, Workflow: workflow, Input: raw})
	if err != nil || !admission.Accepted {
		h.t.Fatalf("admit %s: admission=%+v err=%v", key, admission, err)
	}
	return admission.RunID, raw
}

// startWorker runs the application worker loop (claim, execute, finish, back
// off) for every partition under ownerID until the returned stop is called.
func (h *overflowHarness) startWorker(ownerID string) func() {
	workerCtx, cancel := context.WithCancel(h.ctx)
	done := make(chan error, 1)
	go func() { done <- h.runtime.Run(workerCtx, ownerID) }()
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			<-done
		})
	}
	h.t.Cleanup(stop)
	return stop
}

func overflowTerminal(state string) bool {
	return state == "completed" || state == "failed" || state == "uncertain"
}

// awaitTerminal polls the committed run record until it is terminal or the
// fixture's bound passes.
func (h *overflowHarness) awaitTerminal(runID string, extra time.Duration) RunRecord {
	h.t.Helper()
	within := time.Duration(h.fixture.MaxTerminalMillis)*time.Millisecond + extra
	started := time.Now()
	var run RunRecord
	var err error
	for time.Since(started) < within {
		if run, err = h.runtime.GetRun(h.ctx, h.tenant, runID); err == nil && overflowTerminal(run.State) {
			h.t.Logf("run %s terminal after %s: state=%s errorCode=%q input=%d bytes output=%d bytes", runID, time.Since(started).Round(time.Millisecond), run.State, run.ErrorCode, len(run.Input), len(run.Output))
			return run
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.t.Fatalf("run %s not terminal within %s: state=%q errorCode=%q owner=%.16q err=%v; oversized=%d headroomless=%d invocations", runID, within, run.State, run.ErrorCode, run.OwnerID, err, h.invocations("oversized"), h.invocations("headroomless"))
	return RunRecord{}
}

func (h *overflowHarness) awaitState(runID, state string) {
	h.t.Helper()
	deadline := time.Now().Add(time.Duration(h.fixture.MaxTerminalMillis) * time.Millisecond)
	for time.Now().Before(deadline) {
		if run, err := h.runtime.GetRun(h.ctx, h.tenant, runID); err == nil && run.State == state {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	h.t.Fatalf("run %s never reached state %q", runID, state)
}

// assertClassifiedFailure checks the terminal record of an overflowed run.
func (h *overflowHarness) assertClassifiedFailure(run RunRecord) {
	h.t.Helper()
	if run.State != h.fixture.Expected.State || run.ErrorCode != h.fixture.Expected.ErrorCode || len(run.Output) != 0 {
		h.t.Fatalf("overflowed run state=%q errorCode=%q output=%d bytes; want %q / %q with no output", run.State, run.ErrorCode, len(run.Output), h.fixture.Expected.State, h.fixture.Expected.ErrorCode)
	}
}

// assertFollowUpAndRelease checks that the tenant's next run completed and
// that no run still holds an admission slot.
func (h *overflowHarness) assertFollowUpAndRelease(followUp string) {
	h.t.Helper()
	run := h.awaitTerminal(followUp, 0)
	if run.State != h.fixture.Expected.FollowUpState || string(run.Output) != h.fixture.Expected.FollowUpOutput || h.invocations("follow-up") != h.fixture.Expected.FollowUpInvocations {
		h.t.Fatalf("follow-up run state=%q output=%s invocations=%d; want %q / %s / %d", run.State, run.Output, h.invocations("follow-up"), h.fixture.Expected.FollowUpState, h.fixture.Expected.FollowUpOutput, h.fixture.Expected.FollowUpInvocations)
	}
	active, err := h.store.ListActiveRunIDs(h.ctx, h.partition, h.fixture.Limits.PartitionAdmissions)
	if err != nil || len(active) != h.fixture.Expected.ActiveRunsAfter {
		h.t.Fatalf("active runs after both runs ended=%v err=%v; want %d (the overflowed run must release its slots)", active, err, h.fixture.Expected.ActiveRunsAfter)
	}
}

func followUpInput() overflowInput { return overflowInput{Tag: "follow-up", Size: 2, Text: "ok"} }

// inflateRun rewrites a stored run the way a replica without #254's metadata
// headroom could have stored it: the record fits the bound with a one-byte
// owner, and only a claim or re-admission by the longest owner ID pushes it
// over. The input stays a valid workflow input.
func (h *overflowHarness) inflateRun(runID, state string) RunRecord {
	h.t.Helper()
	short := acquireWhenFree(h.t, h.ctx, h.store, h.partition, "s", time.Duration(h.fixture.Limits.OwnerTTLMillis)*time.Millisecond)
	data, revision, err := h.store.ReadState(h.ctx, h.partition, runID)
	if err != nil || revision == 0 {
		h.t.Fatalf("read run %s: revision=%d err=%v", runID, revision, err)
	}
	var run RunRecord
	if err := json.Unmarshal(data, &run); err != nil {
		h.t.Fatal(err)
	}
	input := overflowInput{Tag: "headroomless"}
	run.State, run.OwnerID, run.Fence = state, "s", short.Token
	run.Input, _ = json.Marshal(input)
	base, _ := json.Marshal(run)
	input.Text = strings.Repeat("a", distributed.MaxPayloadBytes-len(base)-64)
	run.Input, _ = json.Marshal(input)
	run.InputDigest = digest(run.Input)
	inflated, _ := json.Marshal(run)
	claimed := run
	claimed.State, claimed.OwnerID = "accepted", strings.Repeat("<", headroomlessOwnerBytes)
	grown, _ := json.Marshal(claimed)
	if len(inflated) > distributed.MaxPayloadBytes || len(grown) <= distributed.MaxPayloadBytes {
		h.t.Fatalf("inflated run is %d bytes and %d once claimed by the longest owner; want under, then over, the bound %d", len(inflated), len(grown), distributed.MaxPayloadBytes)
	}
	if _, err := h.store.CommitFencedState(h.ctx, short, runID, revision, "test-inflate-"+runID, "test.inflate", inflated, inflated); err != nil {
		h.t.Fatal(err)
	}
	if err := h.store.Release(h.ctx, short); err != nil {
		h.t.Fatal(err)
	}
	return run
}

// TestOversizedPureStepOutputFailsTheRunOnce: a pure node returning an output
// over the bound ends its run failed with record_too_large, runs exactly once,
// and the tenant's next run (admitted behind it) completes.
func TestOversizedPureStepOutputFailsTheRunOnce(t *testing.T) {
	h := newOverflowHarness(t)
	oversized, raw := h.admit("overflow-output", "oversized-output", overflowInput{Tag: "oversized", Size: h.fixture.OversizedOutputBytes})
	followUp, _ := h.admit("overflow-output", "oversized-output-follow-up", followUpInput())
	h.startWorker("overflow-worker")
	run := h.awaitTerminal(oversized, 0)
	h.assertClassifiedFailure(run)
	if string(run.Input) != string(raw) {
		t.Fatalf("failed run input=%.64q, want the admitted input kept (it fits the terminal record)", run.Input)
	}
	h.assertFollowUpAndRelease(followUp)
	if got := h.invocations("oversized"); got != h.fixture.Expected.OversizedInvocations {
		t.Fatalf("oversized pure node ran %d times, want exactly %d", got, h.fixture.Expected.OversizedInvocations)
	}
}

// TestRunRecordOverflowAtFinishFailsTheRun: input and output each fit, and
// the step output commits, but together they overflow the terminal run
// record. The run must end failed with record_too_large instead of retrying
// its finish forever; its node is not re-run.
func TestRunRecordOverflowAtFinishFailsTheRun(t *testing.T) {
	h := newOverflowHarness(t)
	finishing, raw := h.admit("overflow-output", "finish-overflow", overflowInput{Tag: "oversized", Text: strings.Repeat("a", h.fixture.FinishInputBytes)})
	followUp, _ := h.admit("overflow-output", "finish-overflow-follow-up", followUpInput())
	h.startWorker("overflow-worker")
	run := h.awaitTerminal(finishing, 0)
	h.assertClassifiedFailure(run)
	if string(run.Input) != string(raw) {
		t.Fatalf("failed run input=%.64q, want the admitted input kept (it fits without the output)", run.Input)
	}
	h.assertFollowUpAndRelease(followUp)
	if got := h.invocations("oversized"); got != h.fixture.Expected.OversizedInvocations {
		t.Fatalf("finishing node ran %d times, want exactly %d", got, h.fixture.Expected.OversizedInvocations)
	}
}

// TestOversizedStepDispatchRecordFailsBeforeInvoking: the dispatch record
// carries the node's declared effects. One that cannot be written fails the
// run before the effect is invoked, so the effect never runs.
func TestOversizedStepDispatchRecordFailsBeforeInvoking(t *testing.T) {
	h := newOverflowHarness(t)
	dispatching, _ := h.admit("overflow-dispatch", "dispatch-overflow", overflowInput{Tag: "dispatch", Text: "x"})
	followUp, _ := h.admit("overflow-output", "dispatch-overflow-follow-up", followUpInput())
	h.startWorker("overflow-worker")
	h.assertClassifiedFailure(h.awaitTerminal(dispatching, 0))
	h.assertFollowUpAndRelease(followUp)
	if got := h.invocations("dispatch"); got != h.fixture.Expected.DispatchInvocations {
		t.Fatalf("effect with an unwritable dispatch record ran %d times, want %d", got, h.fixture.Expected.DispatchInvocations)
	}
}

// TestRunStoredWithoutHeadroomFailsAtClaim: a stored run that fits only until
// the claiming owner's ID is written can never be claimed by that owner. It
// must end as a classified failure whose terminal record fits (its input is
// dropped and its input digest kept), not block the partition.
func TestRunStoredWithoutHeadroomFailsAtClaim(t *testing.T) {
	h := newOverflowHarness(t)
	headroomless, _ := h.admit("overflow-output", "headroomless-claim", overflowInput{Tag: "headroomless", Text: "small"})
	followUp, _ := h.admit("overflow-output", "headroomless-claim-follow-up", followUpInput())
	stored := h.inflateRun(headroomless, "accepted")
	h.startWorker(strings.Repeat("<", headroomlessOwnerBytes))
	run := h.awaitTerminal(headroomless, 0)
	h.assertClassifiedFailure(run)
	if (len(run.Input) != 0 && string(run.Input) != "null") || run.InputDigest != stored.InputDigest {
		t.Fatalf("failed run input=%.32q digest=%s; want the input dropped and digest %s kept", run.Input, run.InputDigest, stored.InputDigest)
	}
	h.assertFollowUpAndRelease(followUp)
	if got := h.invocations("headroomless"); got != h.fixture.Expected.HeadroomlessInvocations {
		t.Fatalf("unclaimable run's node ran %d times, want %d", got, h.fixture.Expected.HeadroomlessInvocations)
	}
}

// TestRunStoredWithoutHeadroomFailsAtTimerReadmission: a suspended run stored
// without headroom overflows when its due timer re-admits it under the
// longest owner ID. The run must fail with record_too_large, its wait must
// close and leave the due-timer index, and the partition must keep working.
func TestRunStoredWithoutHeadroomFailsAtTimerReadmission(t *testing.T) {
	h := newOverflowHarness(t)
	headroomless, _ := h.admit("overflow-timer", "headroomless-timer", overflowInput{Tag: "headroomless", Text: "small"})
	stopShort := h.startWorker("w1")
	h.awaitState(headroomless, "waiting")
	stopShort()
	stored := h.inflateRun(headroomless, "waiting")
	followUp, _ := h.admit("overflow-output", "headroomless-timer-follow-up", followUpInput())
	h.startWorker(strings.Repeat("<", headroomlessOwnerBytes))
	run := h.awaitTerminal(headroomless, time.Duration(h.fixture.TimerTimeoutMillis)*time.Millisecond)
	h.assertClassifiedFailure(run)
	if (len(run.Input) != 0 && string(run.Input) != "null") || run.InputDigest != stored.InputDigest {
		t.Fatalf("failed run input=%.32q digest=%s; want the input dropped and digest %s kept", run.Input, run.InputDigest, stored.InputDigest)
	}
	h.assertFollowUpAndRelease(followUp)
	wait, err := h.runtime.GetWait(h.ctx, h.tenant, WaitIDFor(headroomless, "delay", ""))
	if err != nil || wait.State != h.fixture.Expected.WaitStateAfterTimer {
		t.Fatalf("wait of the failed run=%+v err=%v; want %q", wait, err, h.fixture.Expected.WaitStateAfterTimer)
	}
	due, err := h.store.ListDueTimers(h.ctx, h.partition, time.Now().UTC().Add(time.Hour), 16)
	if err != nil || len(due) != h.fixture.Expected.DueTimersAfter {
		t.Fatalf("due timers after the failed re-admission=%v err=%v; want %d", due, err, h.fixture.Expected.DueTimersAfter)
	}
	if got := h.invocations("after-wait"); got != h.fixture.Expected.HeadroomlessInvocations {
		t.Fatalf("post-timer node of the failed run ran %d times, want %d", got, h.fixture.Expected.HeadroomlessInvocations)
	}
}

// TestOversizedEffectfulOutputEndsUncertain: an effectful node already ran
// when its output turns out to be unwritable, so its run ends uncertain (it
// is not retried, and not reported as a plain failure) with the
// record_too_large code, and the effect ran exactly once.
func TestOversizedEffectfulOutputEndsUncertain(t *testing.T) {
	h := newOverflowHarness(t)
	effectful, _ := h.admit("overflow-effect-output", "effectful-output", overflowInput{Tag: "oversized", Size: h.fixture.OversizedOutputBytes})
	followUp, _ := h.admit("overflow-output", "effectful-output-follow-up", followUpInput())
	h.startWorker("overflow-worker")
	run := h.awaitTerminal(effectful, 0)
	if run.State != h.fixture.Expected.EffectfulState || run.ErrorCode != h.fixture.Expected.ErrorCode || len(run.Output) != 0 {
		t.Fatalf("effectful overflow state=%q errorCode=%q output=%d bytes; want %q / %q", run.State, run.ErrorCode, len(run.Output), h.fixture.Expected.EffectfulState, h.fixture.Expected.ErrorCode)
	}
	h.assertFollowUpAndRelease(followUp)
	if got := h.invocations("oversized"); got != h.fixture.Expected.OversizedInvocations {
		t.Fatalf("effect ran %d times, want exactly %d", got, h.fixture.Expected.OversizedInvocations)
	}
}

// TestDueTimerOfATerminalRunClosesItsWait covers the window between the two
// transitions of a failed timer re-admission: the run already failed (and
// released its slots) but the owner stopped before closing its wait. The next
// poll must close the wait and drop its due-timer entry, not skip it forever.
func TestDueTimerOfATerminalRunClosesItsWait(t *testing.T) {
	h := newOverflowHarness(t)
	runID, _ := h.admit("overflow-timer", "terminal-run-timer", overflowInput{Tag: "headroomless", Text: "small"})
	stop := h.startWorker("w1")
	h.awaitState(runID, "waiting")
	stop()
	ownerTTL := time.Duration(h.fixture.Limits.OwnerTTLMillis) * time.Millisecond
	owner := acquireWhenFree(t, h.ctx, h.store, h.partition, "w2", ownerTTL)
	holdOwner(t, h.store, owner, ownerTTL)
	run, err := h.runtime.GetRun(h.ctx, h.tenant, runID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.runtime.finish(h.ctx, owner, run, h.fixture.Expected.State, h.fixture.Expected.ErrorCode, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := h.runtime.FireDueWaits(h.ctx, owner, time.Now().UTC().Add(time.Duration(h.fixture.TimerTimeoutMillis)*time.Millisecond+time.Second), 8); err != nil {
		t.Fatal(err)
	}
	wait, err := h.runtime.GetWait(h.ctx, h.tenant, WaitIDFor(runID, "delay", ""))
	if err != nil || wait.State != h.fixture.Expected.WaitStateAfterTimer {
		t.Fatalf("wait of a terminal run after its timer was due=%+v err=%v; want %q", wait, err, h.fixture.Expected.WaitStateAfterTimer)
	}
	due, err := h.store.ListDueTimers(h.ctx, h.partition, time.Now().UTC().Add(time.Hour), 16)
	if err != nil || len(due) != h.fixture.Expected.DueTimersAfter {
		t.Fatalf("due timers=%v err=%v; want %d", due, err, h.fixture.Expected.DueTimersAfter)
	}
	if final, err := h.runtime.GetRun(h.ctx, h.tenant, runID); err != nil || final.State != h.fixture.Expected.State {
		t.Fatalf("closing the wait changed the terminal run: %+v err=%v", final, err)
	}
}

// TestRealOutageAtStepCompletionStaysRetryable is the other side of the
// classification: a real quorum loss while a step result commits is a
// storage outage, not an oversized record. Two voters are paused just before
// the committed-step transaction, under a context long enough that etcd
// itself answers (etcdserver: request timed out) instead of the context
// expiring. The run must stay running with its slots held, and a later owner
// must complete it. etcd's timeout does not say whether the result proposal
// committed once quorum returned, so the pure step runs once (it did) or
// twice (the successor re-dispatched it), never more.
func TestRealOutageAtStepCompletionStaysRetryable(t *testing.T) {
	// Voters are paused inside transaction hooks: hold the exclusive
	// cluster lock before the test starts, not mid-transaction.
	clustertest.Disrupt(t)
	h := newOverflowHarness(t)
	client := &hookedClient{Client: integrationClient(t)}
	outageRuntime := *h.runtime
	outageRuntime.store = integrationStoreFor(t, client)
	runID, _ := h.admit("overflow-output", "outage-at-complete", followUpInput())
	owner := acquireWhenFree(t, h.ctx, outageRuntime.store, h.partition, "outage-owner", 30*time.Second)
	restore := func() {}
	// The first step transaction is the dispatch, the second the result.
	hook := client.arm("/step-", 1, false, func() { restore = clustertest.PauseQuorum(t) })
	started := time.Now()
	_, err := outageRuntime.processOne(h.ctx, owner)
	elapsed := time.Since(started)
	restore()
	var engineErr *engine.Error
	if !hook.fired.Load() || !errors.As(err, &engineErr) || engineErr.Class != h.fixture.Outage.FailureClass || errors.Is(err, ErrRecordOverflow) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		t.Fatalf("outage at step completion after %s (hook fired=%v): err=%v; want a %q engine error from etcd itself, neither an overflow nor a context error", elapsed, hook.fired.Load(), err, h.fixture.Outage.FailureClass)
	}
	t.Logf("outage at step completion surfaced after %s: %v", elapsed.Round(time.Millisecond), err)
	run, err := h.runtime.GetRun(h.ctx, h.tenant, runID)
	active, activeErr := h.store.ListActiveRunIDs(h.ctx, h.partition, h.fixture.Limits.PartitionAdmissions)
	if err != nil || activeErr != nil || run.State != h.fixture.Outage.StateAfterOutage || len(active) != h.fixture.Outage.ActiveRunsAfterOutage {
		t.Fatalf("after the outage run=%q err=%v active=%v (%v); want %q with %d active run (retryable, slots held)", run.State, err, active, activeErr, h.fixture.Outage.StateAfterOutage, h.fixture.Outage.ActiveRunsAfterOutage)
	}
	if err := outageRuntime.store.Release(h.ctx, owner); err != nil && !errors.Is(err, distributed.ErrOwnershipLost) {
		t.Fatal(err)
	}
	h.startWorker("recovery-worker")
	final := h.awaitTerminal(runID, 0)
	invocations := h.invocations("follow-up")
	if final.State != h.fixture.Outage.FinalState || string(final.Output) != h.fixture.Outage.FinalOutput || invocations < h.fixture.Outage.MinInvocations || invocations > h.fixture.Outage.MaxInvocations {
		t.Fatalf("recovered run=%q output=%s invocations=%d; want %q / %s / %d..%d", final.State, final.Output, invocations, h.fixture.Outage.FinalState, h.fixture.Outage.FinalOutput, h.fixture.Outage.MinInvocations, h.fixture.Outage.MaxInvocations)
	}
	t.Logf("recovered after the outage: pure step ran %d time(s)", invocations)
}
