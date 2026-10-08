package engine_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/capacity"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
)

type effectIn struct {
	Item any `json:"item,omitempty"`
}

var effectSchema = node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`))

func control(id, kind string, body *contract.Control) contract.InternalInstruction {
	return contract.InternalInstruction{ID: id, Kind: kind, Control: body}
}

func call(index int, id, nodeName string, references ...contract.Reference) contract.InternalInstruction {
	return contract.InternalInstruction{Index: index, ID: id, Kind: "call", Node: nodeName, References: references}
}

func format2(instructions ...contract.InternalInstruction) contract.InternalProgram {
	for index := range instructions {
		instructions[index].Index = index
	}
	return contract.InternalProgram{WorkflowID: "w", Version: "1.0.0", Format: contract.ControlFormat, Instructions: instructions}
}

// #190 across concurrent arms: a parallel arm fails saturated while its
// sibling, which declares effects and ignores cancellation, commits its
// write before the parallel joins. Retrying the run would write twice, so
// the run's failure must no longer read as saturation (Review R round 1 on
// #383).
//
// The order is forced, not timed (#400): busy fails only once write has
// started, and write commits only after busy's failure has canceled it
// (fail-fast). With sleeps alone, a loaded machine could fail busy before
// arm 0 reached its node; the run was then canceled before anything was
// written, and a retryable saturated failure was the right answer.
func TestSaturationAfterAConcurrentEffectIsNotRetryable(t *testing.T) {
	for attempt := 0; attempt < 3; attempt++ {
		var wrote atomic.Int32
		var canceledFirst atomic.Bool
		started := make(chan struct{})
		write := node.MustDefine("write", "1.0.0", func(ctx context.Context, _ effectIn) (effectIn, error) {
			close(started)
			select {
			case <-ctx.Done():
				canceledFirst.Store(true)
			case <-time.After(10 * time.Second):
			}
			// The write is still in flight when the run fails: the join
			// must wait for it.
			time.Sleep(20 * time.Millisecond)
			wrote.Add(1)
			return effectIn{}, nil
		}, node.Description("writes"), effectSchema, node.Effects("store:write"))
		busy := node.MustDefine("busy", "1.0.0", func(context.Context, effectIn) (effectIn, error) {
			select {
			case <-started:
				return effectIn{}, capacity.ErrSaturated
			case <-time.After(10 * time.Second):
				return effectIn{}, errors.New("write never started")
			}
		}, node.Description("saturated"), effectSchema)
		program := format2(
			control("fan", "parallel", &contract.Control{Arms: []contract.Arm{
				{Name: "0", Instructions: []contract.InternalInstruction{call(0, "store", "write")}},
				{Name: "1", Instructions: []contract.InternalInstruction{call(0, "check", "busy")}},
			}}),
			contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: "store"}}},
		)
		_, err := engine.New(map[string]node.Any{"write": write.Any(), "busy": busy.Any()}).Run(context.Background(), program, effectIn{})
		if err == nil || wrote.Load() != 1 {
			t.Fatalf("err=%v wrote=%d", err, wrote.Load())
		}
		if !canceledFirst.Load() {
			t.Fatalf("attempt %d: busy's failure never canceled write, so write did not commit after it", attempt)
		}
		if errors.Is(err, capacity.ErrSaturated) {
			t.Fatalf("attempt %d: %v still reads as saturated after the write committed", attempt, err)
		}
		if !strings.Contains(err.Error(), `after step "store" committed its effects`) {
			t.Fatalf("attempt %d: err=%v; want it to name the committed step", attempt, err)
		}
	}
}

// Many iterations commit effects at once; the last effectful step is
// shared bookkeeping, and the race detector checks it is serialised. A
// saturated failure after them is not retryable.
func TestConcurrentEffectsAreRecordedOnce(t *testing.T) {
	var wrote atomic.Int32
	write := node.MustDefine("write", "1.0.0", func(context.Context, effectIn) (effectIn, error) {
		wrote.Add(1)
		return effectIn{}, nil
	}, node.Description("writes"), effectSchema, node.Effects("store:write"))
	busy := node.MustDefine("busy", "1.0.0", func(context.Context, effectIn) (effectIn, error) {
		return effectIn{}, capacity.ErrSaturated
	}, node.Description("saturated"), effectSchema)
	items := make([]string, 64)
	for index := range items {
		items[index] = "1"
	}
	program := format2(
		control("loop", "each", &contract.Control{Concurrency: 16, Operands: []contract.Operand{{Literal: []byte("[" + strings.Join(items, ",") + "]")}}, Arms: []contract.Arm{
			{Name: "body", Instructions: []contract.InternalInstruction{call(0, "store", "write")}, Output: &contract.Operand{Reference: &contract.Reference{Step: "store"}}},
		}}),
		call(0, "check", "busy"),
		contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: "check"}}},
	)
	_, err := engine.New(map[string]node.Any{"write": write.Any(), "busy": busy.Any()}).Run(context.Background(), program, effectIn{})
	if wrote.Load() != 64 || err == nil || errors.Is(err, capacity.ErrSaturated) {
		t.Fatalf("wrote=%d err=%v; want 64 writes and a failure that is not retryable", wrote.Load(), err)
	}
}

// Long each and step ids: every iteration's attempt id stays within what
// inspection records, stays distinct, and completes there (Review R round 1
// on #383: truncated ids collided and stayed running).
func TestLongIdsKeepIterationAttemptsDistinctInInspection(t *testing.T) {
	loopID, stepID := "l"+strings.Repeat("o", 59), "s"+strings.Repeat("t", 59)
	echo := node.MustDefine("echo", "1.0.0", func(_ context.Context, in effectIn) (effectIn, error) { return in, nil },
		node.Description("echo"), effectSchema)
	program := format2(
		control(loopID, "each", &contract.Control{Concurrency: 2, Operands: []contract.Operand{{Literal: []byte(`[{"item":1},{"item":2},{"item":3}]`)}}, Arms: []contract.Arm{
			{Name: "body", Instructions: []contract.InternalInstruction{call(0, stepID, "echo", contract.Reference{Step: loopID})}, Output: &contract.Operand{Reference: &contract.Reference{Step: stepID}}},
		}}),
		contract.InternalInstruction{ID: "output", Kind: "output", References: []contract.Reference{{Step: loopID}}},
	)
	recorder := inspect.NewRecorder()
	invocation := inspection.Invocation{RunID: "run-long", Principal: "p", AttemptID: "attempt:" + strings.Repeat("a", 32)}
	if _, err := engine.New(map[string]node.Any{"echo": echo.Any()}).WithObserver(recorder).RunObserved(context.Background(), program, effectIn{}, invocation); err != nil {
		t.Fatal(err)
	}
	page, err := recorder.Inspect("p", inspection.Policy{}, inspection.Query{Version: inspection.Version, RunID: "run-long", StepID: stepID})
	if err != nil || len(page.Steps) != 1 {
		t.Fatalf("page=%+v err=%v", page, err)
	}
	seen := map[string]bool{}
	for _, attempt := range page.Steps[0].Attempts {
		if attempt.Status != inspection.StatusCompleted || len(attempt.ID) > 160 || seen[attempt.ID] {
			t.Fatalf("attempt %+v: want completed, at most 160 characters, distinct", attempt)
		}
		seen[attempt.ID] = true
	}
	if len(seen) != 3 {
		t.Fatalf("attempts=%d, want one per iteration", len(seen))
	}
}
