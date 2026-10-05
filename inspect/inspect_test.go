package inspect_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	runtimecontract "github.com/well-prado/new-blok/contract/runtime"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/internal/journal"
	internaltime "github.com/well-prado/new-blok/internal/runtime"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/store/sqlite"
)

type catalog struct {
	err   error
	calls atomic.Int64
}

type shutdownCatalog struct {
	started chan struct{}
	stopped chan bool
	events  chan string
}

func (c *shutdownCatalog) PriceCents(ctx context.Context, _ string) (int64, error) {
	close(c.started)
	<-ctx.Done()
	c.stopped <- app.Aborted(ctx)
	c.events <- "node stopped"
	return 0, ctx.Err()
}

func (c *catalog) PriceCents(context.Context, string) (int64, error) {
	c.calls.Add(1)
	return 1500, c.err
}

func observedQuote(t *testing.T, recorder *inspect.Recorder, invocation inspection.Invocation, input any) (engine.Result, error, int64) {
	t.Helper()
	fixtureCatalog := &catalog{}
	if item, ok := input.(quote.Input); ok && item.SKU == "failure" {
		fixtureCatalog.err = errors.New("synthetic provider failure")
	} else if item, ok := input.(quote.Input); ok && item.SKU == "uncertain" {
		fixtureCatalog.err = &node.DomainError{Code: "provider_outcome_unknown", Class: "transport", Uncertain: true}
	}
	definition, err := quote.NewNode(fixtureCatalog)
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
	result, runErr := engine.New(map[string]node.Any{"shop/calculate-quote": definition.Any()}).WithObserver(recorder).RunObserved(context.Background(), program, input, invocation)
	return result, runErr, fixtureCatalog.calls.Load()
}

func TestRealEngineLogsAndUncertainCancellationRemainEvidence(t *testing.T) {
	recorder := inspect.NewRecorder()
	definition := node.MustDefine("test/effect", "1.0.0", func(ctx context.Context, _ quote.Input) (map[string]any, error) {
		node.Logger(ctx).Warn("dispatch ended ambiguously", "api_token", "must-redact")
		return nil, &node.DomainError{Code: "provider_unknown", Class: "transport", Uncertain: true, Err: context.Canceled}
	}, node.Description("real inspected failure"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)), node.Effects("http:provider"))
	program := contract.InternalProgram{WorkflowID: "payment", Instructions: []contract.InternalInstruction{{Index: 0, ID: "charge", Kind: "call", Node: "test/effect"}}}
	appInstance, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "payment"}}, Inspection: recorder})
	if err != nil {
		t.Fatal(err)
	}
	if err = appInstance.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(appInstance, map[string]node.Any{"test/effect": definition.Any()})
	_, err = runner.Run(context.Background(), program, quote.Input{}, inspection.Invocation{RunID: "uncertain-canceled", Principal: "alice", AttemptID: "dispatch-actual-1"})
	if err == nil {
		t.Fatal("expected uncertain failure")
	}
	page, err := recorder.Inspect("alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldError: true, inspection.FieldLogs: true}, MaxPayloadBytes: 1024}, inspection.Query{Version: inspection.Version, RunID: "uncertain-canceled"})
	if err != nil || page.Run.Status != inspection.StatusUncertain || len(page.Steps) != 1 || page.Steps[0].Status != inspection.StatusUncertain || len(page.Steps[0].Attempts) != 1 || page.Steps[0].Attempts[0].ID != "dispatch-actual-1/charge/1" || len(page.Steps[0].Logs) != 1 || strings.Contains(string(page.Steps[0].Logs[0].Attrs), "must-redact") {
		t.Fatalf("actual uncertain/log projection=%+v err=%v", page, err)
	}
}

func TestApplicationRunnerPersistsTerminalJournalOutcome(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "execution-outcomes.db")
	database, err := (sqlite.Backend{}).Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	journalStore, err := journal.New(ctx, database, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	failedRun, err := journalStore.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "runner-failure", Workflow: "quote", ArtifactDigest: "sha256:runner-failure", Input: []byte(`{"sku":"failure","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := journalStore.TerminalOutcomes().FailRun(ctx, inspection.Invocation{RunID: failedRun.RunID, Principal: "mallory"}, "forged_failure", "workflow"); !errors.Is(err, journal.ErrNotFound) {
		t.Fatalf("terminal port allowed a principal to mutate another run: %v", err)
	}
	appInstance, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "quote"}}, RunOutcomes: journalStore.TerminalOutcomes()})
	if err != nil {
		t.Fatal(err)
	}
	if err := appInstance.Start(ctx); err != nil {
		t.Fatal(err)
	}
	failingNode, err := quote.NewNode(&catalog{err: errors.New("synthetic provider failure")})
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
	runner := execution.NewRunner(appInstance, map[string]node.Any{"shop/calculate-quote": failingNode.Any()})
	if _, err := runner.Run(ctx, program, quote.Input{SKU: "failure", Quantity: 1}, inspection.Invocation{RunID: failedRun.RunID, Principal: "alice"}); err == nil {
		t.Fatal("real failing execution unexpectedly succeeded")
	}
	if run, err := journalStore.Run(ctx, failedRun.RunID); err != nil || run.State != "failed" || run.ErrorCode != "node_error" || run.ErrorClass != "failure" {
		t.Fatalf("application runner did not persist classified terminal failure: run=%+v err=%v", run, err)
	}
	completedRun, err := journalStore.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "runner-complete", Workflow: "quote", ArtifactDigest: "sha256:runner-complete", Input: []byte(`{"sku":"coffee","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	completedNode, err := quote.NewNode(&catalog{})
	if err != nil {
		t.Fatal(err)
	}
	completedRunner := execution.NewRunner(appInstance, map[string]node.Any{"shop/calculate-quote": completedNode.Any()})
	completedResult, err := completedRunner.Run(ctx, program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: completedRun.RunID, Principal: "alice"})
	if err != nil || completedResult.Output != int64(1500) {
		t.Fatalf("real successful execution result=%+v err=%v", completedResult, err)
	}
	if run, err := journalStore.Run(ctx, completedRun.RunID); err != nil || run.State != "completed" {
		t.Fatalf("application runner did not persist completion: run=%+v err=%v", run, err)
	}

	retryingRun, err := journalStore.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "runner-retry", Workflow: "quote", ArtifactDigest: "sha256:runner-retry", Input: []byte(`{"sku":"failure","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	operation, err := journalStore.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: retryingRun.RunID, ArtifactDigest: "sha256:runner-retry", InvocationPath: "calculate", IterationPath: "root"}, Input: []byte(`{"sku":"failure"}`)})
	if err != nil {
		t.Fatal(err)
	}
	first, err := journalStore.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := journalStore.FailAttempt(ctx, operation.Key, first.ID, true, "retryable transport failure"); err != nil {
		t.Fatal(err)
	}
	second, err := journalStore.StartAttempt(ctx, operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.Run(ctx, program, quote.Input{SKU: "failure", Quantity: 1}, inspection.Invocation{RunID: retryingRun.RunID, Principal: "alice"}); !errors.Is(err, journal.ErrRunActiveWork) {
		t.Fatalf("terminal failure hid a live retry attempt: %v", err)
	}
	if run, err := journalStore.Run(ctx, retryingRun.RunID); err != nil || run.State != "accepted" {
		t.Fatalf("retrying run was incorrectly marked terminal: run=%+v err=%v", run, err)
	}
	uncertainRun, err := journalStore.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "runner-uncertain", Workflow: "quote", ArtifactDigest: "sha256:runner-uncertain", Input: []byte(`{"sku":"uncertain","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	uncertainNode, err := quote.NewNode(&catalog{err: &node.DomainError{Code: "provider_unknown", Class: "transport", Uncertain: true}})
	if err != nil {
		t.Fatal(err)
	}
	uncertainRunner := execution.NewRunner(appInstance, map[string]node.Any{"shop/calculate-quote": uncertainNode.Any()})
	if _, err := uncertainRunner.Run(ctx, program, quote.Input{SKU: "uncertain", Quantity: 1}, inspection.Invocation{RunID: uncertainRun.RunID, Principal: "alice"}); err == nil {
		t.Fatal("uncertain execution unexpectedly succeeded")
	}
	if run, err := journalStore.Run(ctx, uncertainRun.RunID); err != nil || run.State != "uncertain" {
		t.Fatalf("unknown provider outcome was not durably conservative: run=%+v err=%v", run, err)
	}
	if _, err := journalStore.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: uncertainRun.RunID, ArtifactDigest: "sha256:runner-uncertain", InvocationPath: "later", IterationPath: "root"}}); !errors.Is(err, journal.ErrRunNotActive) {
		t.Fatalf("new effect was allowed after uncertain terminal outcome: %v", err)
	}
	canceledRun, err := journalStore.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "runner-canceled", Workflow: "quote", ArtifactDigest: "sha256:runner-canceled", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	canceledContext, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := runner.Run(canceledContext, program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: canceledRun.RunID, Principal: "alice"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("actual canceled execution error=%v", err)
	}
	if run, err := journalStore.Run(ctx, canceledRun.RunID); err != nil || run.State != "canceled" {
		t.Fatalf("canceled execution outcome did not survive canceled request context: run=%+v err=%v", run, err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, err = (sqlite.Backend{}).Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	journalStore, err = journal.New(ctx, database, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	policy := inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldError: true}, MaxPageSize: 10}
	failedPage, err := inspect.InspectSource(ctx, journalStore, "alice", policy, inspection.Query{Version: inspection.Version, RunID: failedRun.RunID})
	if err != nil || failedPage.Run.Status != inspection.StatusFailed || failedPage.Run.ErrorCode != "node_error" || failedPage.Run.ErrorClass != "failure" {
		t.Fatalf("restart lost actual failed execution projection: page=%+v err=%v", failedPage, err)
	}
	completedPage, err := inspect.InspectSource(ctx, journalStore, "alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldOutput: true}, MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: completedRun.RunID})
	if err != nil || completedPage.Run.Status != inspection.StatusCompleted || string(completedPage.Run.Output) != "1500" {
		t.Fatalf("restart lost actual successful execution projection: page=%+v err=%v", completedPage, err)
	}
	retryingPage, err := inspect.InspectSource(ctx, journalStore, "alice", policy, inspection.Query{Version: inspection.Version, RunID: retryingRun.RunID})
	if err != nil || retryingPage.Run.Status != inspection.StatusRunning || len(retryingPage.Steps) != 1 || len(retryingPage.Steps[0].Attempts) != 2 || retryingPage.Steps[0].Attempts[1].ID != second.ID || retryingPage.Steps[0].Attempts[1].Status != inspection.StatusRunning {
		t.Fatalf("restart lost live retry state: page=%+v err=%v", retryingPage, err)
	}
	uncertainPage, err := inspect.InspectSource(ctx, journalStore, "alice", policy, inspection.Query{Version: inspection.Version, RunID: uncertainRun.RunID})
	if err != nil || uncertainPage.Run.Status != inspection.StatusUncertain || uncertainPage.Run.ErrorCode != "provider_unknown" {
		t.Fatalf("restart lost conservative uncertain outcome: page=%+v err=%v", uncertainPage, err)
	}
	canceledPage, err := inspect.InspectSource(ctx, journalStore, "alice", policy, inspection.Query{Version: inspection.Version, RunID: canceledRun.RunID})
	if err != nil || canceledPage.Run.Status != inspection.StatusCanceled {
		t.Fatalf("restart lost canceled execution outcome: page=%+v err=%v", canceledPage, err)
	}
}

type failingCompleteOutcome struct {
	app.RunOutcomePort
	err error
}

func (f failingCompleteOutcome) CompleteRun(context.Context, inspection.Invocation, json.RawMessage) error {
	return f.err
}

type blockingCompleteOutcome struct {
	app.RunOutcomePort
	started chan struct{}
}

func (b blockingCompleteOutcome) CompleteRun(ctx context.Context, _ inspection.Invocation, _ json.RawMessage) error {
	close(b.started)
	<-ctx.Done()
	return ctx.Err()
}

type gatedCompleteOutcome struct {
	app.RunOutcomePort
	started chan struct{}
	release chan struct{}
}

func (g gatedCompleteOutcome) CompleteRun(ctx context.Context, _ inspection.Invocation, _ json.RawMessage) error {
	close(g.started)
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestApplicationRunnerReturnsReconciliationIdentityWhenTerminalWriteFails(t *testing.T) {
	ctx := context.Background()
	database, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "outcome-write-failure.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	journalStore, err := journal.New(ctx, database, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	admission, err := journalStore.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "outcome-write-failure", Workflow: "quote", ArtifactDigest: "sha256:outcome-write-failure", Input: []byte(`{"sku":"coffee","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	writeErr := errors.New("synthetic terminal store outage")
	recorder := inspect.NewRecorder()
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "quote"}}, Inspection: recorder, RunOutcomes: failingCompleteOutcome{RunOutcomePort: journalStore.TerminalOutcomes(), err: writeErr}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	fixtureCatalog := &catalog{}
	definition, err := quote.NewNode(fixtureCatalog)
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
	runner := execution.NewRunner(application, map[string]node.Any{"shop/calculate-quote": definition.Any()})
	result, runErr := runner.Run(ctx, program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: admission.RunID, Principal: "alice"})
	var uncertain *execution.OutcomeUncertainError
	if !errors.As(runErr, &uncertain) || !errors.Is(runErr, writeErr) || uncertain.RunID != admission.RunID || !uncertain.IsUncertain() {
		t.Fatalf("terminal write failure was not an explicit uncertain/reconcile result: result=%+v err=%#v", result, runErr)
	}
	if fixtureCatalog.calls.Load() != 1 || result.Output != int64(1500) {
		t.Fatalf("real node effect did not succeed exactly once before write failure: result=%+v effects=%d", result, fixtureCatalog.calls.Load())
	}
	if run, err := journalStore.Run(ctx, admission.RunID); err != nil || run.State != "accepted" {
		t.Fatalf("failed terminal write falsely committed a run state: run=%+v err=%v", run, err)
	}
	page, err := recorder.Inspect("alice", inspection.Policy{MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: admission.RunID})
	if err != nil || page.Run.Status != inspection.StatusUncertain {
		t.Fatalf("failed durable terminal write projected a committed completion: page=%+v err=%v", page, err)
	}
}

func TestApplicationRunnerDoesNotProjectCompletionBeforeDurableOutcome(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	recorder := inspect.NewRecorder()
	application, err := app.New(app.Config{
		Workflows: []app.Workflow{{Name: "quote"}}, Inspection: recorder,
		RunOutcomes: gatedCompleteOutcome{started: started, release: release},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	definition, err := quote.NewNode(&catalog{})
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
	runner := execution.NewRunner(application, map[string]node.Any{"shop/calculate-quote": definition.Any()})
	finished := make(chan error, 1)
	go func() {
		_, runErr := runner.Run(context.Background(), program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: "outcome-pending", Principal: "alice"})
		finished <- runErr
	}()
	<-started
	pending, err := recorder.Inspect("alice", inspection.Policy{MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: "outcome-pending"})
	if err != nil || pending.Run.Status != inspection.StatusRunning {
		t.Fatalf("uncommitted effect was projected terminal before durable outcome: page=%+v err=%v", pending, err)
	}
	close(release)
	if err := <-finished; err != nil {
		t.Fatalf("durable completion failed: %v", err)
	}
	completed, err := recorder.Inspect("alice", inspection.Policy{MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: "outcome-pending"})
	if err != nil || completed.Run.Status != inspection.StatusCompleted {
		t.Fatalf("committed effect was not projected completed: page=%+v err=%v", completed, err)
	}
}

func TestApplicationRunnerBindsExecutionToShutdownLease(t *testing.T) {
	started := make(chan struct{}, 1)
	stopped := make(chan bool, 1)
	events := make(chan string, 2)
	application, err := app.New(app.Config{
		Workflows:    []app.Workflow{{Name: "quote"}},
		DrainTimeout: 25 * time.Millisecond,
		AbortGrace:   500 * time.Millisecond,
		Dependencies: []app.Dependency{{
			Name:  "provider",
			Start: func(context.Context) error { return nil },
			Close: func(context.Context) error {
				events <- "dependency closed"
				return nil
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	definition, err := quote.NewNode(&shutdownCatalog{started: started, stopped: stopped, events: events})
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"}}}
	runner := execution.NewRunner(application, map[string]node.Any{"shop/calculate-quote": definition.Any()})
	runDone := make(chan error, 1)
	go func() {
		_, err := runner.Run(context.Background(), program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{})
		runDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("runner did not enter the blocking node")
	}
	if err := application.Shutdown(context.Background()); !errors.Is(err, app.ErrDrainTimeout) {
		t.Fatalf("shutdown error=%v, want drain timeout after aborting the active runner", err)
	}
	select {
	case aborted := <-stopped:
		if !aborted {
			t.Fatal("node context ended without the application abort cause")
		}
	case <-time.After(time.Second):
		t.Fatal("runner node did not stop after the application aborted its lease")
	}
	if err := <-runDone; err == nil {
		t.Fatal("canceled runner unexpectedly succeeded")
	}
	if first, second := <-events, <-events; first != "node stopped" || second != "dependency closed" {
		t.Fatalf("dependency shutdown raced active runner: events=%q,%q", first, second)
	}
}

func TestApplicationAbortBoundsTerminalWriteAfterEffectAndReturnsUncertain(t *testing.T) {
	started := make(chan struct{}, 1)
	writeStarted := make(chan struct{})
	application, err := app.New(app.Config{
		Workflows:    []app.Workflow{{Name: "quote"}},
		DrainTimeout: 25 * time.Millisecond,
		AbortGrace:   250 * time.Millisecond,
		RunOutcomes:  blockingCompleteOutcome{started: writeStarted},
		Dependencies: []app.Dependency{{
			Name:  "store",
			Start: func(context.Context) error { return nil },
			Close: func(context.Context) error {
				started <- struct{}{}
				return nil
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	fixtureCatalog := &catalog{}
	definition, err := quote.NewNode(fixtureCatalog)
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "calculate", Path: []string{"totalCents"}}}},
	}}
	runner := execution.NewRunner(application, map[string]node.Any{"shop/calculate-quote": definition.Any()})
	runDone := make(chan error, 1)
	go func() {
		_, err := runner.Run(context.Background(), program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: "shutdown-outcome-run", Principal: "alice"})
		runDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("runner did not enter terminal outcome persistence")
	}
	if fixtureCatalog.calls.Load() != 1 {
		t.Fatalf("effect count before terminal write=%d, want one", fixtureCatalog.calls.Load())
	}
	if err := application.Shutdown(context.Background()); !errors.Is(err, app.ErrDrainTimeout) {
		t.Fatalf("shutdown error=%v, want drain timeout", err)
	}
	var uncertain *execution.OutcomeUncertainError
	if err := <-runDone; !errors.As(err, &uncertain) || uncertain.RunID != "shutdown-outcome-run" {
		t.Fatalf("aborted terminal write after a real effect did not return reconciliation identity: %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("dependency close did not complete after runner released its lease")
	}
}

func TestRecorderHardBoundsAndSaturationAreExplicit(t *testing.T) {
	r := inspect.NewRecorderWithLimits(inspect.RecorderLimits{MaxRuns: 1, MaxEvents: 3, MaxRetainedBytes: 4096, MaxStepsPerRun: 1, MaxEventBytes: 512})
	at := time.Unix(1, 0).UTC()
	r.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "r1", Principal: "p", At: at})
	r.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: "r1", Principal: "p", StepID: "a", Attempt: 1, At: at})
	r.Observe(inspection.Event{Kind: inspection.StepCompleted, RunID: "r1", Principal: "p", StepID: "a", Attempt: 1, At: at})
	r.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: "r1", Principal: "p", StepID: "b", Attempt: 1, At: at})
	page, err := r.Inspect("p", fullPolicy(), inspection.Query{Version: inspection.Version, RunID: "r1"})
	stats := r.Stats()
	if err != nil || !page.Truncated || !stats.Saturated || stats.DroppedEvents == 0 || stats.Runs != 1 || len(page.Steps) != 1 {
		t.Fatalf("saturation page=%+v stats=%+v err=%v", page, stats, err)
	}
}

type sessionBlobReader struct {
	session *internaltime.AuthenticatedSession
	store   *internaltime.BlobStore
}

func (r sessionBlobReader) Principal() string { return r.session.Principal().Name }
func (r sessionBlobReader) ReadBlob(ref runtimecontract.BlobRef, max int) ([]byte, error) {
	if ref.Size > max {
		return nil, errors.New("blob exceeds bound")
	}
	return r.store.GetAuthorized(r.session, ref)
}

func TestAuthorizedBlobRetrievalUsesRealSessionAndHardBound(t *testing.T) {
	auth, err := internaltime.NewTokenAuthenticator([]internaltime.Credential{{Token: "blob-reader-token-alice", Principal: "alice", Capabilities: []runtimecontract.Capability{"blob:read", "blob:write"}}, {Token: "blob-reader-token-bob", Principal: "bob", Capabilities: []runtimecontract.Capability{"blob:read"}}})
	if err != nil {
		t.Fatal(err)
	}
	alice, err := auth.NewSession("blob-reader-token-alice", "alice", []runtimecontract.Capability{"blob:read", "blob:write"})
	if err != nil {
		t.Fatal(err)
	}
	bob, err := auth.NewSession("blob-reader-token-bob", "bob", []runtimecontract.Capability{"blob:read"})
	if err != nil {
		t.Fatal(err)
	}
	store, err := internaltime.NewBlobStore(1024)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("authorized body")
	ref := runtimecontract.BlobRef{Digest: runtimecontract.CanonicalDigest(data), Size: len(data)}
	if err = store.PutAuthorized(alice, ref, data); err != nil {
		t.Fatal(err)
	}
	policy := inspection.Policy{AllowBlobRead: true, MaxBlobBytes: 64}
	got, err := inspect.RetrieveBlob(sessionBlobReader{alice, store}, "alice", policy, ref, 64)
	if err != nil || string(got) != string(data) {
		t.Fatalf("blob=%q err=%v", got, err)
	}
	if _, err = inspect.RetrieveBlob(sessionBlobReader{alice, store}, "bob", policy, ref, 64); !errors.Is(err, inspect.ErrNotFound) {
		t.Fatalf("principal mismatch=%v", err)
	}
	if _, err = inspect.RetrieveBlob(sessionBlobReader{bob, store}, "bob", policy, ref, 64); !errors.Is(err, inspect.ErrNotFound) {
		t.Fatalf("cross-session blob=%v", err)
	}
	if _, err = inspect.RetrieveBlob(sessionBlobReader{alice, store}, "alice", policy, ref, 4); !errors.Is(err, inspect.ErrNotFound) {
		t.Fatalf("oversize blob read=%v", err)
	}
}

func fullPolicy() inspection.Policy {
	return inspection.Policy{Fields: map[inspection.Field]bool{
		inspection.FieldInput: true, inspection.FieldOutput: true, inspection.FieldError: true,
	}, MaxPageSize: 10, MaxPayloadBytes: 1024}
}

func TestProjectionUsesRealEngineEventsAndIsPrincipalIsolated(t *testing.T) {
	recorder := inspect.NewRecorder()
	result, err, effects := observedQuote(t, recorder, inspection.Invocation{RunID: "run-alice", Principal: "alice", AttemptID: "golden-attempt"}, quote.Input{SKU: "coffee", Quantity: 2})
	if err != nil || result.Output != int64(3000) || effects != 1 {
		t.Fatalf("result=%+v effects=%d err=%v", result, effects, err)
	}
	query := inspection.Query{Version: inspection.Version, RunID: "run-alice"}
	page, err := recorder.Inspect("alice", fullPolicy(), query)
	if err != nil || page.Run.Status != inspection.StatusCompleted || len(page.Steps) != 2 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	if page.Steps[0].Status != inspection.StatusCompleted || page.Steps[0].Attempt != 1 || string(page.Steps[0].Input) == "" || string(page.Steps[0].Output) == "" {
		t.Fatalf("step projection=%+v", page.Steps[0])
	}
	normalized := page
	normalized.Run.StartedAt = time.Time{}
	normalized.Run.FinishedAt = time.Time{}
	for i := range normalized.Steps {
		normalized.Steps[i].StartedAt = time.Time{}
		normalized.Steps[i].FinishedAt = time.Time{}
		for j := range normalized.Steps[i].Attempts {
			normalized.Steps[i].Attempts[j].StartedAt = time.Time{}
			normalized.Steps[i].Attempts[j].FinishedAt = time.Time{}
		}
	}
	actual, _ := json.MarshalIndent(normalized, "", "  ")
	actual = append(actual, '\n')
	goldenPath := filepath.Join("..", "testdata", "inspection", "golden", "native-success.json")
	golden, goldenErr := os.ReadFile(goldenPath)
	if goldenErr != nil || string(actual) != string(golden) {
		t.Fatalf("actual-run golden mismatch (path=%s, read=%v):\n%s", goldenPath, goldenErr, actual)
	}
	if _, err := recorder.Inspect("bob", fullPolicy(), query); !errors.Is(err, inspect.ErrNotFound) {
		t.Fatalf("cross-principal read err=%v", err)
	}
	page.Steps[0].Input[0] = 'x'
	page.Run.Output[0] = 'x'
	again, err := recorder.Inspect("alice", fullPolicy(), query)
	if err != nil || !json.Valid(again.Steps[0].Input) || !json.Valid(again.Run.Output) {
		t.Fatalf("projection mutation escaped: page=%+v err=%v", again, err)
	}
}

func TestPaginationVersionRedactionAndPayloadBounds(t *testing.T) {
	recorder := inspect.NewRecorder()
	at := time.Unix(10, 0).UTC()
	recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "run-1", Principal: "alice", At: at, Input: json.RawMessage(`{"password":"secret-value","ok":true}`)})
	for _, id := range []string{"one", "two", "three"} {
		recorder.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: "run-1", Principal: "alice", StepID: id, Attempt: 1, At: at.Add(time.Second), Input: json.RawMessage(`{"api_token":"private","large":"` + strings.Repeat("x", 128) + `"}`)})
		recorder.Observe(inspection.Event{Kind: inspection.StepCompleted, RunID: "run-1", Principal: "alice", StepID: id, Attempt: 1, At: at.Add(2 * time.Second), Output: json.RawMessage(`{"password":"out-secret","result":true}`)})
	}
	recorder.Observe(inspection.Event{Kind: inspection.RunCompleted, RunID: "run-1", Principal: "alice", At: at.Add(3 * time.Second)})
	policy := fullPolicy()
	policy.MaxPageSize, policy.MaxPayloadBytes = 1, inspection.MinPayloadBytes
	query := inspection.Query{Version: inspection.Version, RunID: "run-1", Limit: 1}
	first, err := recorder.Inspect("alice", policy, query)
	if err != nil || len(first.Steps) != 1 || first.Next == "" || !strings.Contains(string(first.Steps[0].Input), `"$truncated":true`) {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	if strings.Contains(string(first.Run.Input), "secret-value") || strings.Contains(string(first.Steps[0].Output), "out-secret") {
		t.Fatalf("secret leaked: %+v", first)
	}
	restricted, err := recorder.Inspect("alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldOutput: true}}, inspection.Query{Version: inspection.Version, RunID: "run-1"})
	if err != nil || len(restricted.Steps) != 3 || restricted.Run.Input != nil || restricted.Steps[0].Input != nil || restricted.Steps[0].Output == nil {
		t.Fatalf("field projection=%+v err=%v", restricted, err)
	}
	encoded, err := json.Marshal(first)
	if err != nil || len(encoded) > 256<<10 {
		t.Fatalf("response bound bytes=%d err=%v", len(encoded), err)
	}
	query.Cursor = first.Next
	second, err := recorder.Inspect("alice", policy, query)
	if err != nil || len(second.Steps) != 1 || second.Steps[0].ID != "two" {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
	query.Version = "inspection/v99"
	if _, err := recorder.Inspect("alice", policy, query); !errors.Is(err, inspection.ErrUnsupportedVersion) {
		t.Fatalf("unsupported version err=%v", err)
	}
}

func TestOversizedCursorIsRejectedBeforeBase64Decode(t *testing.T) {
	recorder := inspect.NewRecorder()
	recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "cursor-bound", Principal: "alice", At: time.Unix(10, 0).UTC()})
	_, err := recorder.Inspect("alice", fullPolicy(), inspection.Query{
		Version: inspection.Version, RunID: "cursor-bound", Cursor: strings.Repeat("A", 1<<20),
	})
	if err == nil || !strings.Contains(err.Error(), "invalid cursor") {
		t.Fatalf("oversized cursor was not rejected: %v", err)
	}
}

func TestExplicitPayloadCeilingBelowEnvelopeIsRejected(t *testing.T) {
	recorder := inspect.NewRecorder()
	at := time.Unix(20, 0).UTC()
	recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "small-limit", Principal: "alice", At: at, Input: json.RawMessage(`{"value":"payload-that-must-not-exceed-the-caller-limit"}`)})
	policy := fullPolicy()
	policy.MaxPayloadBytes = 40
	if page, err := recorder.Inspect("alice", policy, inspection.Query{Version: inspection.Version, RunID: "small-limit"}); !errors.Is(err, inspection.ErrPayloadLimit) || len(page.Run.Input) != 0 {
		t.Fatalf("small explicit recorder limit silently changed: page=%+v err=%v", page, err)
	}
}

func TestProjectionCapsAggregateAttemptAndLogHistories(t *testing.T) {
	recorder := inspect.NewRecorder()
	at := time.Unix(30, 0).UTC()
	recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "history", Principal: "alice", At: at})
	for i := 1; i <= inspection.MaxProjectedAttempts+2; i++ {
		attemptID := fmt.Sprintf("run/step/%d", i)
		recorder.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: "history", Principal: "alice", StepID: "step", Attempt: i, AttemptID: attemptID, At: at.Add(time.Duration(i) * time.Second)})
		recorder.Observe(inspection.Event{Kind: inspection.StepLog, RunID: "history", Principal: "alice", StepID: "step", Attempt: i, At: at.Add(time.Duration(i) * time.Second), LogLevel: "INFO", LogMessage: fmt.Sprintf("log-%d", i)})
		recorder.Observe(inspection.Event{Kind: inspection.StepCompleted, RunID: "history", Principal: "alice", StepID: "step", Attempt: i, AttemptID: attemptID, At: at.Add(time.Duration(i+1) * time.Second)})
	}
	recorder.Observe(inspection.Event{Kind: inspection.RunCompleted, RunID: "history", Principal: "alice", At: at.Add(time.Minute)})
	page, err := recorder.Inspect("alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldLogs: true}, MaxPayloadBytes: 1024}, inspection.Query{Version: inspection.Version, RunID: "history"})
	if err != nil || len(page.Steps) != 1 {
		t.Fatalf("history projection=%+v err=%v", page, err)
	}
	step := page.Steps[0]
	if len(step.Attempts) != inspection.MaxProjectedAttempts || !step.AttemptsTruncated || len(step.Logs) != inspection.MaxProjectedLogs || !step.LogsTruncated || step.Attempts[0].Number != 3 || step.Logs[0].Message != "log-4" {
		t.Fatalf("aggregate history not bounded/latest: %+v", step)
	}
}

func TestDefaultMetadataPolicyInspectsTwentySteps(t *testing.T) {
	recorder := inspect.NewRecorder()
	at := time.Unix(40, 0).UTC()
	recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "metadata", Principal: "alice", At: at})
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("step-%02d", i)
		recorder.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: "metadata", Principal: "alice", StepID: id, Attempt: 1, At: at.Add(time.Duration(i+1) * time.Second)})
		recorder.Observe(inspection.Event{Kind: inspection.StepCompleted, RunID: "metadata", Principal: "alice", StepID: id, Attempt: 1, At: at.Add(time.Duration(i+2) * time.Second)})
	}
	recorder.Observe(inspection.Event{Kind: inspection.RunCompleted, RunID: "metadata", Principal: "alice", At: at.Add(time.Minute)})
	page, err := recorder.Inspect("alice", inspection.Policy{}, inspection.Query{Version: inspection.Version, RunID: "metadata"})
	if err != nil || len(page.Steps) != 20 {
		t.Fatalf("default metadata inspection=%+v err=%v", page, err)
	}
}

func TestRealEngineFailureAndCancellationRemainDistinct(t *testing.T) {
	recorder := inspect.NewRecorder()
	_, err, effects := observedQuote(t, recorder, inspection.Invocation{RunID: "failed", Principal: "alice"}, quote.Input{SKU: "failure", Quantity: 1})
	if err == nil || effects != 1 {
		t.Fatalf("provider failure err=%v effects=%d", err, effects)
	}
	failed, err := recorder.Inspect("alice", fullPolicy(), inspection.Query{Version: inspection.Version, RunID: "failed"})
	if err != nil || failed.Run.Status != inspection.StatusFailed {
		t.Fatalf("failed projection=%+v err=%v", failed, err)
	}

	definition, err := quote.NewNode(&catalog{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"}}}
	_, err = engine.New(map[string]node.Any{"shop/calculate-quote": definition.Any()}).WithObserver(recorder).RunObserved(ctx, program, quote.Input{}, inspection.Invocation{RunID: "canceled", Principal: "alice"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation err=%v", err)
	}
	canceled, err := recorder.Inspect("alice", fullPolicy(), inspection.Query{Version: inspection.Version, RunID: "canceled"})
	if err != nil || canceled.Run.Status != inspection.StatusCanceled {
		t.Fatalf("canceled projection=%+v err=%v", canceled, err)
	}
}

func TestRealEngineUncertainEffectHasDistinctProjection(t *testing.T) {
	recorder := inspect.NewRecorder()
	_, err, effects := observedQuote(t, recorder, inspection.Invocation{RunID: "uncertain", Principal: "alice"}, quote.Input{SKU: "uncertain", Quantity: 1})
	if err == nil || effects != 1 {
		t.Fatalf("uncertain provider outcome err=%v effects=%d", err, effects)
	}
	page, err := recorder.Inspect("alice", fullPolicy(), inspection.Query{Version: inspection.Version, RunID: "uncertain"})
	if err != nil || page.Run.Status != inspection.StatusUncertain || len(page.Steps) == 0 || page.Steps[0].Status != inspection.StatusUncertain {
		t.Fatalf("uncertain projection=%+v err=%v", page, err)
	}
}

func TestInspectionIssueFixtureHasPositiveNegativeAndLimitedEvidenceCases(t *testing.T) {
	path := filepath.Join("..", "testdata", "inspection", "cases.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	type expected struct {
		RunStatus    string   `json:"runStatus"`
		StepStatuses []string `json:"stepStatuses"`
		Effects      int64    `json:"effects"`
		Error        string   `json:"error"`
		Secret       string   `json:"secret"`
		Truncated    bool     `json:"truncated"`
	}
	var fixture struct {
		SchemaVersion int    `json:"schemaVersion"`
		Owner         string `json:"owner"`
		Contract      string `json:"contract"`
		Cases         []struct {
			ID       string   `json:"id"`
			Kind     string   `json:"kind"`
			Input    string   `json:"input"`
			Expected expected `json:"expected"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.SchemaVersion != 1 || fixture.Owner != "E15-T01" || fixture.Contract != inspection.Version || len(fixture.Cases) != 8 {
		t.Fatalf("inspection fixture contract=%+v", fixture)
	}
	seen := map[string]bool{}
	for _, item := range fixture.Cases {
		t.Run(item.ID, func(t *testing.T) {
			if item.ID == "" || seen[item.ID] || item.Input == "" {
				t.Fatalf("invalid/duplicate fixture case: %+v", item)
			}
			seen[item.ID] = true
			var page inspection.Page
			var gotErr error
			var effects int64
			switch item.ID {
			case "engine-completed-run", "engine-failed-run", "engine-uncertain-run":
				sku := map[string]string{"engine-completed-run": "coffee", "engine-failed-run": "failure", "engine-uncertain-run": "uncertain"}[item.ID]
				recorder := inspect.NewRecorder()
				_, _, effects = observedQuote(t, recorder, inspection.Invocation{RunID: item.ID, Principal: "alice"}, quote.Input{SKU: sku, Quantity: 2})
				page, gotErr = recorder.Inspect("alice", fullPolicy(), inspection.Query{Version: inspection.Version, RunID: item.ID})
			case "engine-canceled-run":
				recorder := inspect.NewRecorder()
				definition, err := quote.NewNode(&catalog{})
				if err != nil {
					t.Fatal(err)
				}
				program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{{Index: 0, ID: "calculate", Kind: "call", Node: "shop/calculate-quote"}}}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				_, runErr := engine.New(map[string]node.Any{"shop/calculate-quote": definition.Any()}).WithObserver(recorder).RunObserved(ctx, program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: item.ID, Principal: "alice"})
				page, err = recorder.Inspect("alice", fullPolicy(), inspection.Query{Version: inspection.Version, RunID: item.ID})
				if !errors.Is(runErr, context.Canceled) || err != nil {
					t.Fatalf("run cancellation=%v, inspect=%v", runErr, err)
				}
			case "second-principal-hidden":
				recorder := inspect.NewRecorder()
				_, _, _ = observedQuote(t, recorder, inspection.Invocation{RunID: item.ID, Principal: "alice"}, quote.Input{SKU: "coffee", Quantity: 1})
				before := int64(0)
				_, gotErr = recorder.Inspect("bob", fullPolicy(), inspection.Query{Version: inspection.Version, RunID: item.ID})
				effects -= before
			case "future-version-rejected":
				recorder := inspect.NewRecorder()
				_, gotErr = recorder.Inspect("alice", fullPolicy(), inspection.Query{Version: "inspection/v99", RunID: "not-run"})
			case "secret-redacted-and-large-payload-bounded":
				recorder := inspect.NewRecorder()
				at := time.Unix(1, 0).UTC()
				recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: item.ID, Principal: "alice", At: at, Input: json.RawMessage(`{"api_token":"synthetic-secret"}`)})
				recorder.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: item.ID, Principal: "alice", StepID: "large", Attempt: 1, At: at.Add(time.Second)})
				recorder.Observe(inspection.Event{Kind: inspection.StepCompleted, RunID: item.ID, Principal: "alice", StepID: "large", Attempt: 1, At: at.Add(2 * time.Second), Output: json.RawMessage(`{"padding":"` + strings.Repeat("x", 1024) + `"}`)})
				recorder.Observe(inspection.Event{Kind: inspection.RunCompleted, RunID: item.ID, Principal: "alice", At: at.Add(3 * time.Second)})
				policy := inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true, inspection.FieldOutput: true}, MaxPageSize: 10, MaxPayloadBytes: 128}
				page, gotErr = recorder.Inspect("alice", policy, inspection.Query{Version: inspection.Version, RunID: item.ID})
			case "suspended-node-worker-and-durable-history":
				// This case explicitly records a gap rather than simulating success.
				effects = 0
			default:
				t.Fatalf("fixture case is not executable or explicitly classified: %+v", item)
			}
			if item.Kind == "accepted" {
				if gotErr != nil {
					t.Fatalf("accepted case returned error: %v", gotErr)
				}
				if item.Expected.RunStatus != "" && string(page.Run.Status) != item.Expected.RunStatus {
					t.Errorf("run status=%q, want %q", page.Run.Status, item.Expected.RunStatus)
				}
				if item.Expected.StepStatuses != nil {
					statuses := make([]string, len(page.Steps))
					for i := range page.Steps {
						statuses[i] = string(page.Steps[i].Status)
					}
					if fmt.Sprint(statuses) != fmt.Sprint(item.Expected.StepStatuses) {
						t.Errorf("step statuses=%v, want %v", statuses, item.Expected.StepStatuses)
					}
				}
				if item.ID == "secret-redacted-and-large-payload-bounded" {
					if len(page.Steps) != 1 || string(page.Run.Input) == "" || !strings.Contains(string(page.Run.Input), item.Expected.Secret) || !item.Expected.Truncated || !strings.Contains(string(page.Steps[0].Output), `"$truncated":true`) {
						t.Errorf("secret/payload expectation not met: page=%+v expected=%+v", page, item.Expected)
					}
				}
			} else if item.Kind == "rejected" {
				if gotErr == nil || fixtureErrorName(gotErr) != item.Expected.Error {
					t.Errorf("error=%v (%s), want %q", gotErr, fixtureErrorName(gotErr), item.Expected.Error)
				}
				if effects != item.Expected.Effects {
					t.Errorf("effects=%d, want %d", effects, item.Expected.Effects)
				}
			} else if item.Kind == "insufficient-evidence" {
				if item.Expected.Error != "not_implemented_or_not_verified" || effects != item.Expected.Effects || item.Expected.Effects != 0 {
					t.Errorf("insufficient-evidence classification/effects disagree: %+v effects=%d", item.Expected, effects)
				}
			} else {
				t.Fatalf("unknown fixture kind %q", item.Kind)
			}
			if item.Kind == "accepted" && effects != item.Expected.Effects {
				t.Errorf("effects=%d, want %d", effects, item.Expected.Effects)
			}
		})
	}
}

func fixtureErrorName(err error) string {
	switch {
	case errors.Is(err, inspect.ErrNotFound):
		return "run_not_found"
	case errors.Is(err, inspection.ErrUnsupportedVersion):
		return "unsupported_version"
	default:
		return "unexpected_error"
	}
}

// TestLogArrivingAfterItsStepCompletedIsRecorded pins existing behaviour that
// #226 relies on: a worker's log reaches inspection asynchronously
// (contract/runtime Call.OnLog), so it can arrive after its step, and its
// run, completed. It is still recorded with that step, not dropped.
func TestLogArrivingAfterItsStepCompletedIsRecorded(t *testing.T) {
	recorder := inspect.NewRecorder()
	at := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	recorder.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "run-1", Principal: "alice", At: at, Input: json.RawMessage(`{}`)})
	recorder.Observe(inspection.Event{Kind: inspection.StepProcessing, RunID: "run-1", Principal: "alice", StepID: "quote", Attempt: 1, At: at})
	recorder.Observe(inspection.Event{Kind: inspection.StepCompleted, RunID: "run-1", Principal: "alice", StepID: "quote", Attempt: 1, At: at.Add(time.Second), Output: json.RawMessage(`{}`)})
	recorder.Observe(inspection.Event{Kind: inspection.RunCompleted, RunID: "run-1", Principal: "alice", At: at.Add(time.Second), Output: json.RawMessage(`{}`)})
	recorder.Observe(inspection.Event{Kind: inspection.StepLog, RunID: "run-1", Principal: "alice", StepID: "quote", At: at.Add(500 * time.Millisecond), LogLevel: "INFO", LogMessage: "quote calculated", LogAttrs: json.RawMessage(`{}`)})
	policy := fullPolicy()
	policy.Fields[inspection.FieldLogs] = true
	page, err := recorder.Inspect("alice", policy, inspection.Query{Version: inspection.Version, RunID: "run-1"})
	if err != nil || page.Run.Status != inspection.StatusCompleted || len(page.Steps) != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	if logs := page.Steps[0].Logs; len(logs) != 1 || logs[0].Message != "quote calculated" || page.Steps[0].Status != inspection.StatusCompleted {
		t.Fatalf("step=%+v; want the late log recorded on the completed step", page.Steps[0])
	}
}
