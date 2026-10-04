package engine_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type fixtureUncertainError struct{}

func (fixtureUncertainError) Error() string     { return "effect outcome uncertain" }
func (fixtureUncertainError) IsUncertain() bool { return true }

var errEffectUncertain error = fixtureUncertainError{}

type stepJournalFixture struct {
	mu          sync.Mutex
	outputs     map[string]json.RawMessage
	started     map[string]bool
	uncertain   map[string]bool
	attempts    map[string]int
	current     map[string]string
	completeErr error
	failErr     error
	beginErr    error
	waitReady   bool
	waitResult  engine.WaitResult
	runs        map[string]struct{ artifact, input string }
}

func newStepJournalFixture() *stepJournalFixture {
	return &stepJournalFixture{outputs: map[string]json.RawMessage{}, started: map[string]bool{}, uncertain: map[string]bool{}, attempts: map[string]int{}, current: map[string]string{}, runs: map[string]struct{ artifact, input string }{}}
}

func (j *stepJournalFixture) expectRun(runID, artifact string, input any) {
	encoded, err := json.Marshal(input)
	if err != nil {
		panic(err)
	}
	digest := sha256.Sum256(encoded)
	j.runs[runID] = struct{ artifact, input string }{artifact: artifact, input: "sha256:" + hex.EncodeToString(digest[:])}
}

func (j *stepJournalFixture) VerifyRun(_ context.Context, runID, artifact, input string) error {
	if j.runs[runID] != (struct{ artifact, input string }{artifact: artifact, input: input}) {
		return errors.New("run artifact or input identity mismatch")
	}
	return nil
}

func (j *stepJournalFixture) Load(_ context.Context, identity engine.StepIdentity) (json.RawMessage, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	key := identity.OperationKey
	if result, ok := j.outputs[key]; ok {
		return append(json.RawMessage(nil), result...), true, nil
	}
	if j.uncertain[key] {
		return nil, false, errEffectUncertain
	}
	return nil, false, nil
}

func (j *stepJournalFixture) Begin(_ context.Context, identity engine.StepIdentity, _ json.RawMessage, _ []string) (engine.StepAttempt, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.beginErr != nil {
		return engine.StepAttempt{}, j.beginErr
	}
	key := identity.OperationKey
	if j.started[key] {
		j.uncertain[key] = true
		return engine.StepAttempt{}, errEffectUncertain
	}
	j.started[key] = true
	j.attempts[key]++
	attemptID := fmt.Sprintf("attempt-%d", j.attempts[key])
	j.current[key] = attemptID
	return engine.StepAttempt{Identity: identity, AttemptID: attemptID}, nil
}

func (j *stepJournalFixture) Complete(_ context.Context, attempt engine.StepAttempt, output json.RawMessage) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.current[attempt.Identity.OperationKey] != attempt.AttemptID {
		return errors.New("stale attempt")
	}
	if j.completeErr != nil {
		return j.completeErr
	}
	j.outputs[attempt.Identity.OperationKey] = append(json.RawMessage(nil), output...)
	return nil
}

func (j *stepJournalFixture) Fail(_ context.Context, attempt engine.StepAttempt, effects []string, _ error) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.current[attempt.Identity.OperationKey] != attempt.AttemptID {
		return errors.New("stale attempt")
	}
	if len(effects) != 0 {
		j.uncertain[attempt.Identity.OperationKey] = true
	}
	return j.failErr
}

func (j *stepJournalFixture) Await(_ context.Context, _ engine.WaitIdentity) (engine.WaitResult, bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.waitResult, j.waitReady, nil
}

type journalInput struct {
	Value int `json:"value"`
}

func TestRunJournaledResumesCommittedEffectWithoutReinvokingIt(t *testing.T) {
	const testSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`
	calls := 0
	write := node.MustDefine("test/write", "1.0.0", func(_ context.Context, in journalInput) (journalInput, error) {
		calls++
		return journalInput{Value: in.Value + 1}, nil
	}, node.Description("journal fixture"), node.Schemas([]byte(testSchema), []byte(testSchema)), node.Effects("fixture:write")).Any()
	runner := engine.New(map[string]node.Any{"test/write": write})
	program := contract.InternalProgram{WorkflowID: "journal", Digest: "sha256:artifact-v1", Instructions: []contract.InternalInstruction{{Index: 0, ID: "write", Kind: "call", Node: "test/write"}}}
	journal := newStepJournalFixture()
	journal.expectRun("run-1", program.Digest, journalInput{Value: 4})

	for attempt := 0; attempt < 2; attempt++ {
		result, err := runner.RunJournaled(context.Background(), program, journalInput{Value: 4}, "run-1", journal)
		if err != nil || result.State["write"].(journalInput).Value != 5 {
			t.Fatalf("attempt %d: result=%+v err=%v", attempt+1, result, err)
		}
	}
	if calls != 1 {
		t.Fatalf("effect invocation count=%d, want 1 after resume", calls)
	}
}

func TestRunJournaledSuspendsAndResumesFromCommittedSteps(t *testing.T) {
	const testSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`
	calls := 0
	write := node.MustDefine("test/write", "1.0.0", func(_ context.Context, in journalInput) (journalInput, error) {
		calls++
		return journalInput{Value: in.Value + 1}, nil
	}, node.Description("journal fixture"), node.Schemas([]byte(testSchema), []byte(testSchema)), node.Effects("fixture:write")).Any()
	runner := engine.New(map[string]node.Any{"test/write": write})
	program := contract.InternalProgram{WorkflowID: "journal-wait", Digest: "sha256:artifact-wait", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "write", Kind: "call", Node: "test/write"},
		{Index: 1, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval", TimeoutMillis: 60000}},
		{Index: 2, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
	}}
	journal := newStepJournalFixture()
	journal.expectRun("run-wait", program.Digest, journalInput{Value: 4})
	journal.waitResult = engine.WaitResult{SignalID: "signal-1", Payload: json.RawMessage(`{"approved":true}`)}
	_, err := runner.RunJournaled(context.Background(), program, journalInput{Value: 4}, "run-wait", journal)
	var suspended interface{ IsSuspended() bool }
	if !errors.As(err, &suspended) || !suspended.IsSuspended() {
		t.Fatalf("first execution error=%v, want durable suspension", err)
	}
	if calls != 1 {
		t.Fatalf("effect calls after suspension=%d, want 1", calls)
	}
	journal.mu.Lock()
	journal.waitReady = true
	journal.mu.Unlock()
	result, err := runner.RunJournaled(context.Background(), program, journalInput{Value: 4}, "run-wait", journal)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	resumed, ok := result.Output.(engine.WaitResult)
	if !ok || resumed.SignalID != "signal-1" || string(resumed.Payload) != `{"approved":true}` || calls != 1 {
		t.Fatalf("output=%#v effect calls=%d, want committed wait outcome and one effect", result.Output, calls)
	}
}

func TestRunJournaledDoesNotReplayEffectWithUncommittedResult(t *testing.T) {
	const testSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`
	calls := 0
	write := node.MustDefine("test/write", "1.0.0", func(_ context.Context, in journalInput) (journalInput, error) {
		calls++
		return journalInput{Value: in.Value + 1}, nil
	}, node.Description("journal fixture"), node.Schemas([]byte(testSchema), []byte(testSchema)), node.Effects("fixture:write")).Any()
	runner := engine.New(map[string]node.Any{"test/write": write})
	program := contract.InternalProgram{WorkflowID: "journal", Digest: "sha256:artifact-v1", Instructions: []contract.InternalInstruction{{Index: 0, ID: "write", Kind: "call", Node: "test/write"}}}
	journal := newStepJournalFixture()
	journal.expectRun("run-2", program.Digest, journalInput{Value: 4})
	journal.completeErr = errors.New("fenced result commit rejected")
	if _, mismatchErr := runner.RunJournaled(context.Background(), program, journalInput{Value: 99}, "run-2", journal); mismatchErr == nil || !strings.Contains(mismatchErr.Error(), "journal_run_mismatch") {
		t.Fatalf("wrong input error=%v, want journal_run_mismatch before node dispatch", mismatchErr)
	}

	_, err := runner.RunJournaled(context.Background(), program, journalInput{Value: 4}, "run-2", journal)
	if err == nil {
		t.Fatal("first run unexpectedly succeeded despite result-commit failure")
	}
	var uncertainty interface{ IsUncertain() bool }
	if !errors.As(err, &uncertainty) || !uncertainty.IsUncertain() {
		t.Fatalf("first error=%v, want public uncertain-effect marker", err)
	}
	_, retryErr := runner.RunJournaled(context.Background(), program, journalInput{Value: 4}, "run-2", journal)
	uncertainty = nil
	if !errors.Is(retryErr, errEffectUncertain) || !errors.As(retryErr, &uncertainty) || !uncertainty.IsUncertain() {
		t.Fatalf("retry error=%v, want explicit uncertain-effect error", retryErr)
	}
	if calls != 1 {
		t.Fatalf("effect invocation count=%d, want exactly 1 across uncommitted result and retry", calls)
	}
}

func TestRunJournaledExposesNeutralMarkerWhenFailureReconciliationIsUncertain(t *testing.T) {
	const testSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`
	write := node.MustDefine("test/write", "1.0.0", func(_ context.Context, in journalInput) (journalInput, error) {
		return journalInput{}, errors.New("handler completion outage")
	}, node.Description("journal fixture"), node.Schemas([]byte(testSchema), []byte(testSchema)), node.Effects("fixture:write")).Any()
	program := contract.InternalProgram{WorkflowID: "journal", Digest: "sha256:artifact-fail", Instructions: []contract.InternalInstruction{{Index: 0, ID: "write", Kind: "call", Node: "test/write"}}}
	journal := newStepJournalFixture()
	journal.expectRun("run-fail", program.Digest, journalInput{Value: 4})
	journal.failErr = fmt.Errorf("failure reconciliation unavailable: %w", fixtureUncertainError{})

	_, got := engine.New(map[string]node.Any{"test/write": write}).RunJournaled(context.Background(), program, journalInput{Value: 4}, "run-fail", journal)
	var uncertainty interface{ IsUncertain() bool }
	if !errors.As(got, &uncertainty) || !uncertainty.IsUncertain() {
		t.Fatalf("journal failure=%v, want public IsUncertain marker", got)
	}
	var classified *engine.Error
	if !errors.As(got, &classified) || classified.Class != "uncertain" || !classified.Uncertain {
		t.Fatalf("journal failure=%#v, want uncertain class and explicit marker", classified)
	}
}

func TestRunJournaledPreservesNeutralMarkerFromBeginJournalFailure(t *testing.T) {
	const testSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`
	write := node.MustDefine("test/write", "1.0.0", func(_ context.Context, in journalInput) (journalInput, error) {
		return journalInput{Value: in.Value + 1}, nil
	}, node.Description("journal fixture"), node.Schemas([]byte(testSchema), []byte(testSchema)), node.Effects("fixture:write")).Any()
	program := contract.InternalProgram{WorkflowID: "journal", Digest: "sha256:artifact-begin", Instructions: []contract.InternalInstruction{{Index: 0, ID: "write", Kind: "call", Node: "test/write"}}}
	journal := newStepJournalFixture()
	journal.expectRun("run-begin", program.Digest, journalInput{Value: 4})
	journal.beginErr = fmt.Errorf("fenced attempt already uncertain: %w", fixtureUncertainError{})

	_, got := engine.New(map[string]node.Any{"test/write": write}).RunJournaled(context.Background(), program, journalInput{Value: 4}, "run-begin", journal)
	var uncertainty interface{ IsUncertain() bool }
	if !errors.As(got, &uncertainty) || !uncertainty.IsUncertain() {
		t.Fatalf("begin journal error=%v, want public IsUncertain marker", got)
	}
	var classified *engine.Error
	if !errors.As(got, &classified) || classified.Class != "uncertain" || !classified.Uncertain {
		t.Fatalf("begin journal error=%#v, want uncertain class and explicit marker", classified)
	}
}

func TestRunNormalizesUncertainClassToExplicitMarker(t *testing.T) {
	const testSchema = `{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`
	write := node.MustDefine("test/write", "1.0.0", func(_ context.Context, _ journalInput) (journalInput, error) {
		return journalInput{}, &engine.Error{Code: "effect_outcome_uncertain", Class: "uncertain"}
	}, node.Description("uncertain marker compatibility fixture"), node.Schemas([]byte(testSchema), []byte(testSchema))).Any()
	program := contract.InternalProgram{WorkflowID: "journal", Digest: "sha256:artifact-class", Instructions: []contract.InternalInstruction{{Index: 0, ID: "write", Kind: "call", Node: "test/write"}}}
	_, got := engine.New(map[string]node.Any{"test/write": write}).Run(context.Background(), program, journalInput{Value: 1})
	var uncertainty interface{ IsUncertain() bool }
	if !errors.As(got, &uncertainty) || !uncertainty.IsUncertain() {
		t.Fatalf("engine error=%v, want explicit IsUncertain marker", got)
	}
	var classified *engine.Error
	if !errors.As(got, &classified) || !classified.Uncertain {
		t.Fatalf("engine error=%#v, want explicit Uncertain field", classified)
	}
}
