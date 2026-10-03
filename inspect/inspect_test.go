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
	internaltime "github.com/well-prado/new-blok/internal/runtime"
	"github.com/well-prado/new-blok/node"
)

type catalog struct {
	err   error
	calls atomic.Int64
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
	var fixture struct {
		Owner    string `json:"owner"`
		Contract string `json:"contract"`
		Cases    []struct {
			Kind string `json:"kind"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, item := range fixture.Cases {
		counts[item.Kind]++
	}
	if fixture.Owner != "E15-T01" || fixture.Contract != inspection.Version || counts["accepted"] < 1 || counts["rejected"] < 1 || counts["insufficient-evidence"] < 1 {
		t.Fatalf("inspection fixture inventory=%+v counts=%v", fixture, counts)
	}
}
