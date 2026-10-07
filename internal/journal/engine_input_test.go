package journal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

// echoRun admits a run with raw input, leases it, and returns a runner and
// program that echo a typed input of type T through one pure step.
func echoRun[T any](t *testing.T, key, raw, schema string) (*Journal, string, int64, *engine.Engine, contract.InternalProgram) {
	t.Helper()
	return echoRunFixed[T](t, key, raw, schema, nil)
}

// echoRunFixed is echoRun with the run's engine input fixed at admission
// (AdmissionRequest.EngineInput) when engineInput is not nil.
func echoRunFixed[T any](t *testing.T, key, raw, schema string, engineInput any) (*Journal, string, int64, *engine.Engine, contract.InternalProgram) {
	t.Helper()
	database, j := newJournal(t, key+".db", Config{Holder: "a"})
	t.Cleanup(func() { database.Close() })
	admitted, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: key, Workflow: "echo", ArtifactDigest: engineArtifact, Input: []byte(raw), EngineInput: engineInput})
	if err != nil {
		t.Fatal(err)
	}
	token, err := j.TakeRunLease(context.Background(), admitted.RunID, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	echo := node.MustDefine("test/echo", "1.0.0", func(_ context.Context, in T) (T, error) { return in, nil }, node.Description("echo"), node.Schemas([]byte(schema), []byte(schema))).Any()
	program := contract.InternalProgram{WorkflowID: "echo", Digest: engineArtifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "echo", Kind: "call", Node: "test/echo"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "echo"}}},
	}}
	return j, admitted.RunID, token, engine.New(map[string]node.Any{"test/echo": echo}), program
}

func encode(t *testing.T, v any) json.RawMessage {
	t.Helper()
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// refused reports VerifyRun's refusal: journal_run_mismatch, a conflict.
func refused(err error) bool {
	return errors.Is(err, ErrRequestConflict) && strings.Contains(err.Error(), "journal_run_mismatch")
}

// decoded decodes admitted JSON into T as a runner's typed decoder does.
func decoded[T any](t *testing.T, raw string) T {
	t.Helper()
	var value T
	if err := json.Unmarshal([]byte(raw), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

type reversed struct {
	B int `json:"b"`
	A int `json:"a"`
}

type big64 struct {
	N uint64 `json:"n"`
}

type order struct {
	Value int    `json:"value"`
	Kind  string `json:"kind"`
}

// TestEveryValidInputRuns is Review R's input reproductions (rounds 2 on
// #380 and 1 on #384): a run admitted with valid input runs, whatever its
// typed decode re-encodes to: fields out of key order, an integer beyond
// float64's exact range, an unknown field the decoder drops, an optional
// field it fills with its zero value. Each runs twice (the second time
// replaying). On 003655d each was journal_run_mismatch on every execution.
func TestEveryValidInputRuns(t *testing.T) {
	ctx := context.Background()
	check := func(t *testing.T, j *Journal, run string, token int64, runner *engine.Engine, program contract.InternalProgram, input any) {
		t.Helper()
		for range 2 {
			if _, err := runner.RunJournaled(ctx, program, input, run, j.ForRun(run, token)); err != nil && !suspended(err) {
				t.Fatalf("input %#v: %v", input, err)
			}
		}
	}
	t.Run("fields out of key order", func(t *testing.T) {
		const raw = `{"a":1,"b":2}`
		j, run, token, runner, program := echoRun[reversed](t, "typed", raw, `{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]}`)
		check(t, j, run, token, runner, program, decoded[reversed](t, raw))
	})
	t.Run("an integer beyond float64", func(t *testing.T) {
		const raw = `{"n":12345678901234567891}`
		j, run, token, _, _ := echoRun[big64](t, "large", raw, `{"type":"object"}`)
		// A wait, so only VerifyRun reads the input (the schema validator
		// bounds integers to int64).
		runner, program := engine.New(nil), contract.InternalProgram{WorkflowID: "echo", Digest: engineArtifact, Instructions: []contract.InternalInstruction{
			{Index: 0, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
			{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "approval"}}},
		}}
		check(t, j, run, token, runner, program, decoded[big64](t, raw))
	})
	const orderSchema = `{"type":"object","properties":{"value":{"type":"integer"},"kind":{"type":"string"}},"required":["value"]}`
	t.Run("an unknown field the decoder drops", func(t *testing.T) {
		const raw = `{"value":1,"kind":"x","extra":true}`
		j, run, token, runner, program := echoRun[order](t, "unknown", raw, orderSchema)
		check(t, j, run, token, runner, program, decoded[order](t, raw))
	})
	t.Run("an optional field left out", func(t *testing.T) {
		const raw = `{"value":1}`
		j, run, token, runner, program := echoRun[order](t, "optional", raw, orderSchema)
		check(t, j, run, token, runner, program, decoded[order](t, raw))
	})
}

// TestEngineInputIsFixedOnce: a run's engine input is fixed by its first
// execution, or at admission (AdmissionRequest.EngineInput); a later
// execution handing the engine another input is refused, permanently.
func TestEngineInputIsFixedOnce(t *testing.T) {
	ctx := context.Background()
	const schema = `{"type":"object","properties":{"value":{"type":"integer"},"kind":{"type":"string"}},"required":["value"]}`
	j, run, token, runner, program := echoRun[order](t, "fixed", `{"value":1}`, schema)
	if _, err := runner.RunJournaled(ctx, program, order{Value: 1}, run, j.ForRun(run, token)); err != nil {
		t.Fatal(err)
	}
	_, err := runner.RunJournaled(ctx, program, order{Value: 2}, run, j.ForRun(run, token))
	if !refused(err) || !Permanent(err) {
		t.Fatalf("another input on a later execution: err=%v permanent=%v; want refused, permanently", err, Permanent(err))
	}

	admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: "canonical", Workflow: "echo", ArtifactDigest: engineArtifact, Input: []byte(`{"value":1}`), EngineInput: order{Value: 1}})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := j.TakeRunLease(ctx, admitted.RunID, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunJournaled(ctx, program, order{Value: 9}, admitted.RunID, j.ForRun(admitted.RunID, lease)); !refused(err) {
		t.Fatalf("an input other than the one fixed at admission: err=%v; want refused", err)
	}
	if _, err := runner.RunJournaled(ctx, program, order{Value: 1}, admitted.RunID, j.ForRun(admitted.RunID, lease)); err != nil {
		t.Fatalf("the input fixed at admission: %v", err)
	}
}

// TestEngineInputFixedAtAdmissionRuns is Review R round 3's S1 probes on
// #380: a run whose engine input is fixed at admission runs, whatever the
// admitted bytes look like next to the engine's encoding: whitespace,
// HTML characters json.Marshal escapes, keys in another order than the
// typed struct, an optional field zero-filled, a \u escape. On aac2338
// (EngineInput as the caller's bytes) each failed journal_run_mismatch on
// every execution.
func TestEngineInputFixedAtAdmissionRuns(t *testing.T) {
	ctx := context.Background()
	const orderSchema = `{"type":"object","properties":{"value":{"type":"integer"},"kind":{"type":"string"}},"required":["value"]}`
	check := func(t *testing.T, j *Journal, run string, token int64, runner *engine.Engine, program contract.InternalProgram, input any) {
		t.Helper()
		for range 2 {
			if _, err := runner.RunJournaled(ctx, program, input, run, j.ForRun(run, token)); err != nil {
				t.Fatalf("input %#v: %v", input, err)
			}
		}
	}
	for _, c := range []struct{ name, raw string }{
		{"whitespace and an optional field left out", `{"value": 1, "kind": ""}`},
		{"HTML characters", `{"value":1,"kind":"<a>&"}`},
		{"keys in another order", `{"kind":"x","value":1}`},
		{"a unicode escape", `{"value":1,"kind":"\u00e9"}`},
	} {
		t.Run(c.name, func(t *testing.T) {
			input := decoded[order](t, c.raw)
			j, run, token, runner, program := echoRunFixed[order](t, "fixed", c.raw, orderSchema, input)
			check(t, j, run, token, runner, program, input)
		})
	}
	t.Run("a struct whose fields reverse the input's key order", func(t *testing.T) {
		const raw = `{"a":1,"b":2}`
		input := decoded[reversed](t, raw)
		j, run, token, runner, program := echoRunFixed[reversed](t, "reversed", raw, `{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]}`, input)
		check(t, j, run, token, runner, program, input)
	})
}

// TestEngineInputMustEncode: an engine input json.Marshal refuses is
// refused at admission, and no run is admitted for it.
func TestEngineInputMustEncode(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "encode.db", Config{Holder: "a"})
	defer database.Close()
	for _, c := range []struct {
		name  string
		input any
	}{
		{"invalid JSON bytes", json.RawMessage(`{"value":`)},
		{"an unencodable value", make(chan int)},
	} {
		key := "encode-" + c.name
		if _, err := j.Admit(ctx, AdmissionRequest{RequestKey: key, Workflow: "echo", ArtifactDigest: engineArtifact, Input: []byte(`{"value":1}`), EngineInput: c.input}); err == nil {
			t.Fatalf("%s: admitted; want refused", c.name)
		}
		admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: key, Workflow: "echo", ArtifactDigest: engineArtifact, Input: []byte(`{"value":1}`)})
		if err != nil || !admitted.Accepted {
			t.Fatalf("%s: the refused admission left a run behind: accepted=%v err=%v", c.name, admitted.Accepted, err)
		}
	}
}

// TestRepeatAdmissionComparesEngineInput: a repeat admission of a request
// key whose run's engine input is fixed (at admission or by its first
// execution) and that names another engine input is a conflict; the same
// engine input, or none, is the same run.
func TestRepeatAdmissionComparesEngineInput(t *testing.T) {
	ctx := context.Background()
	request := func(key string, engineInput any) AdmissionRequest {
		return AdmissionRequest{RequestKey: key, Workflow: "echo", ArtifactDigest: engineArtifact, Input: []byte(`{"value":1}`), EngineInput: engineInput}
	}
	same := func(t *testing.T, j *Journal, run string, r AdmissionRequest) {
		t.Helper()
		admitted, err := j.Admit(ctx, r)
		if err != nil || admitted.Accepted || admitted.RunID != run {
			t.Fatalf("repeat admission with engine input %#v: %+v err=%v; want run %s", r.EngineInput, admitted, err, run)
		}
	}
	t.Run("fixed at admission", func(t *testing.T) {
		database, j := newJournal(t, "repeat.db", Config{Holder: "a"})
		defer database.Close()
		first, err := j.Admit(ctx, request("repeat", order{Value: 1}))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := j.Admit(ctx, request("repeat", order{Value: 2})); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("another engine input: %v; want ErrRequestConflict", err)
		}
		same(t, j, first.RunID, request("repeat", order{Value: 1}))
		same(t, j, first.RunID, request("repeat", nil))
	})
	t.Run("fixed by the first execution", func(t *testing.T) {
		j, run, token, runner, program := echoRun[order](t, "executed", `{"value":1}`, `{"type":"object"}`)
		same(t, j, run, request("executed", order{Value: 2}))
		if _, err := runner.RunJournaled(ctx, program, order{Value: 1}, run, j.ForRun(run, token)); err != nil {
			t.Fatal(err)
		}
		if _, err := j.Admit(ctx, request("executed", order{Value: 2})); !errors.Is(err, ErrRequestConflict) {
			t.Fatalf("another engine input than the first execution's: %v; want ErrRequestConflict", err)
		}
		same(t, j, run, request("executed", order{Value: 1}))
	})
}

// TestPermanentErrors: the conflicts no retry can fix are permanent; a
// lost lease (another holder runs it) and a fault are not.
func TestPermanentErrors(t *testing.T) {
	for _, c := range []struct {
		err       error
		permanent bool
	}{
		{ErrRequestConflict, true},
		{fmt.Errorf("journal_run_mismatch: %w", ErrRequestConflict), true},
		{ErrWaitCanceled, true},
		{ErrStepResultLimit, true},
		{ErrLeaseLost, false},
		{ErrRunNotActive, false},
		{errors.New("database is locked"), false},
		{nil, false},
	} {
		if got := Permanent(c.err); got != c.permanent {
			t.Errorf("Permanent(%v)=%v; want %v", c.err, got, c.permanent)
		}
	}
}

// TestWritesAfterTheRunEndedAreRefused: once a run has ended, a RunJournal
// that still holds its lease cannot commit a step or schedule a wait
// (ErrRunNotActive): the fence checks the run is live, not only the lease.
func TestWritesAfterTheRunEndedAreRefused(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "ended.db", Config{Holder: "a"})
	defer database.Close()
	run := admitApproval(t, j, "ended")
	token, err := j.TakeRunLease(ctx, run, fixtureBase)
	if err != nil {
		t.Fatal(err)
	}
	rj := verified(t, j, run, token)
	if err := rj.CompleteRun(ctx, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if err := rj.Complete(ctx, engine.StepAttempt{Identity: stepIdentity(run, "notify", `{"value":4}`), AttemptID: pureAttempt}, json.RawMessage(`{}`)); !errors.Is(err, ErrRunNotActive) {
		t.Fatalf("a step after the run ended: err=%v; want ErrRunNotActive", err)
	}
	if _, _, err := rj.Await(ctx, engine.WaitIdentity{Step: stepIdentity(run, "late", `{"name":"late"}`), Name: "late"}); !errors.Is(err, ErrRunNotActive) {
		t.Fatalf("a wait after the run ended: err=%v; want ErrRunNotActive", err)
	}
}

// TestWaitIDFormatIsPinned: a suspended run finds its wait again by this
// ID after an upgrade, so its derivation must not move. The engine's
// digest of the wait plan is of contract.WaitInstruction's JSON encoding:
// a new field without omitempty would change it, and every suspended
// run's wait ID with it.
func TestWaitIDFormatIsPinned(t *testing.T) {
	plan, err := json.Marshal(contract.WaitInstruction{Name: "approval", TimeoutMillis: 60000})
	if err != nil || string(plan) != `{"name":"approval","timeoutMillis":60000}` {
		t.Fatalf("wait plan encoding %s err=%v; changing it moves every suspended run's wait ID", plan, err)
	}
	operation := OperationIdentity{RunID: "run:00000000000000000000000000000332", ArtifactDigest: engineArtifact, InvocationPath: "approval", IterationPath: rootIteration}
	got := waitID(operation, engine.StepIdentity{InputDigest: "sha256:" + strings.Repeat("ab", 32)})
	if want := "wait:bd2915d8a536eaad10768d3cd6c0f90f491b6eeea818ed875fb3f5f8116fdcb1"; got != want {
		t.Fatalf("waitID=%s; want %s (format wait/v1)", got, want)
	}
}

// TestOversizeStepResultIsRecognizable: a node whose output schema is an
// open object is not size-checked by the engine; a result over
// MaxStepResultBytes then fails the run with an error a runner can
// recognize (errors.Is ErrStepResultLimit) and settle as final, rather
// than the engine's generic journal_step_complete persistence fault.
func TestOversizeStepResultIsRecognizable(t *testing.T) {
	const schema = `{"type":"object"}`
	j, run, token, _, _ := echoRun[map[string]any](t, "oversize", `{"a":1}`, schema)
	huge := node.MustDefine("test/huge", "1.0.0", func(context.Context, map[string]any) (map[string]any, error) {
		return map[string]any{"x": strings.Repeat("x", MaxStepResultBytes)}, nil
	}, node.Description("huge"), node.Schemas([]byte(schema), []byte(schema))).Any()
	program := contract.InternalProgram{WorkflowID: "echo", Digest: engineArtifact, Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "huge", Kind: "call", Node: "test/huge"},
		{Index: 1, ID: "output", Kind: "output", References: []contract.Reference{{Step: "huge"}}},
	}}
	_, err := engine.New(map[string]node.Any{"test/huge": huge}).RunJournaled(context.Background(), program, map[string]any{"a": 1}, run, j.ForRun(run, token))
	if !errors.Is(err, ErrStepResultLimit) || !Permanent(err) {
		t.Fatalf("err=%v; want ErrStepResultLimit, permanent", err)
	}
}
