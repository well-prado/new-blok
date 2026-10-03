package inspect_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/engine"
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
		fixtureCatalog.err = &node.DomainError{Code: "provider_outcome_unknown", Class: "uncertain", Uncertain: true}
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

func fullPolicy() inspection.Policy {
	return inspection.Policy{Fields: map[inspection.Field]bool{
		inspection.FieldInput: true, inspection.FieldOutput: true, inspection.FieldError: true,
	}, MaxPageSize: 10, MaxPayloadBytes: 1024}
}

func TestProjectionUsesRealEngineEventsAndIsPrincipalIsolated(t *testing.T) {
	recorder := inspect.NewRecorder()
	result, err, effects := observedQuote(t, recorder, inspection.Invocation{RunID: "run-alice", Principal: "alice"}, quote.Input{SKU: "coffee", Quantity: 2})
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
	policy.MaxPageSize, policy.MaxPayloadBytes = 1, 40
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
