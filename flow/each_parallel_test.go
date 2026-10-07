package flow_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/node"
)

// #333 slice 1b: Each and Parallel lower and run through the public runner,
// concurrently, with the bounds they declare.

type loopNodes struct {
	line, stock, price, verdict node.Definition[object, object]
	// inFlight and maxInFlight measure line's concurrent invocations.
	inFlight, maxInFlight atomic.Int32
	// canceled counts invocations that stopped because their context was
	// canceled; arrived and both are where the parallel arms meet.
	canceled atomic.Int32
	arrived  atomic.Int32
	both     chan struct{}
	// extra registers a test's own nodes too.
	extra map[string]node.Any
}

const notCanceled = "sibling was not canceled"

func newLoopNodes() *loopNodes {
	n := &loopNodes{both: make(chan struct{})}
	define := func(name string, run func(context.Context, object) (object, error)) node.Definition[object, object] {
		return node.MustDefine(name, "1.0.0", run, node.Description(name), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)))
	}
	// line prices one order line. "boom" fails; "block" waits for its
	// context to be canceled, failing loudly if it never is.
	n.line = define("line", func(ctx context.Context, in object) (object, error) {
		current := n.inFlight.Add(1)
		defer n.inFlight.Add(-1)
		for {
			seen := n.maxInFlight.Load()
			if current <= seen || n.maxInFlight.CompareAndSwap(seen, current) {
				break
			}
		}
		switch in["sku"] {
		case "boom":
			// Fail once the three blocked items are in flight, so failing
			// fast must cancel them.
			for deadline := time.Now().Add(5 * time.Second); n.inFlight.Load() < 4 && time.Now().Before(deadline); {
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("boom failed")
		case "block":
			select {
			case <-ctx.Done():
				n.canceled.Add(1)
				return nil, ctx.Err()
			case <-time.After(5 * time.Second):
				return nil, errors.New(notCanceled)
			}
		}
		time.Sleep(10 * time.Millisecond)
		return object{"sku": in["sku"], "total": quantity(in) * 3}, nil
	})
	// price and stock meet at the rendezvous: each returns only once both
	// have started, so they can only finish if they run concurrently.
	meet := func(ctx context.Context) error {
		if n.arrived.Add(1) == 2 {
			close(n.both)
		}
		select {
		case <-n.both:
			return nil
		case <-ctx.Done():
			n.canceled.Add(1)
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("parallel arms did not run concurrently")
		}
	}
	n.price = define("price", func(ctx context.Context, in object) (object, error) {
		if err := meet(ctx); err != nil {
			return nil, err
		}
		return object{"total": quantity(in) * 3}, nil
	})
	n.stock = define("stock", func(ctx context.Context, in object) (object, error) {
		if in["sku"] == "boom" {
			// Fail once price is waiting at the rendezvous, so failing fast
			// must cancel it.
			for deadline := time.Now().Add(5 * time.Second); n.arrived.Load() == 0 && time.Now().Before(deadline); {
				time.Sleep(time.Millisecond)
			}
			return nil, errors.New("stock lookup failed")
		}
		if err := meet(ctx); err != nil {
			return nil, err
		}
		return object{"available": in["sku"] != "sold-out"}, nil
	})
	n.verdict = define("verdict", func(_ context.Context, in object) (object, error) {
		return object{"verdict": in}, nil
	})
	return n
}

func (n *loopNodes) registry() map[string]node.Any {
	registry := map[string]node.Any{"line": n.line.Any(), "stock": n.stock.Any(), "price": n.price.Any(), "verdict": n.verdict.Any()}
	for name, extra := range n.extra {
		registry[name] = extra
	}
	return registry
}

func runLoop[O any](t *testing.T, n *loopNodes, definition flow.Definition[object, O], input object) (execution.Result, error) {
	t.Helper()
	program, err := definition.Lower()
	if err != nil {
		t.Fatalf("Lower: %v", err)
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: controlSpec.Name}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return execution.NewRunner(application, n.registry()).Run(context.Background(), program, input, inspection.Invocation{})
}

func lines(skus ...string) []any {
	out := make([]any, len(skus))
	for index, sku := range skus {
		out[index] = object{"sku": sku, "quantity": index + 1}
	}
	return out
}

func eachLines(n *loopNodes, concurrency int) flow.Definition[object, []object] {
	return flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[[]object] {
		return flow.Each(b, "lines", flow.Select[object, []object](in, "lines"), concurrency, func(arm *flow.ArmBuilder, line flow.Ref[object]) flow.Ref[object] {
			return flow.ArmCall(arm, "line", n.line, line)
		})
	})
}

func TestEachRunsEveryItemInOrderWithinItsConcurrency(t *testing.T) {
	for _, concurrency := range []int{1, 3} {
		n := newLoopNodes()
		skus := make([]string, 12)
		for index := range skus {
			skus[index] = fmt.Sprintf("sku-%d", index)
		}
		result, err := runLoop(t, n, eachLines(n, concurrency), object{"lines": lines(skus...)})
		if err != nil {
			t.Fatalf("concurrency %d: %v", concurrency, err)
		}
		totals, ok := result.State["lines"].([]any)
		if !ok || len(totals) != 12 {
			t.Fatalf("concurrency %d: each result=%#v", concurrency, result.State["lines"])
		}
		for index, total := range totals {
			if want := (object{"sku": skus[index], "total": (index + 1) * 3}); !reflect.DeepEqual(total, want) {
				t.Fatalf("concurrency %d: item %d=%#v want %#v (results out of item order)", concurrency, index, total, want)
			}
		}
		if got := n.maxInFlight.Load(); got > int32(concurrency) || concurrency > 1 && got < 2 {
			t.Fatalf("concurrency %d: at most %d lines ran at once", concurrency, got)
		}
		runs := 0
		for _, step := range result.Steps {
			if step.ID == "line" {
				runs++
			}
		}
		if runs != 12 {
			t.Fatalf("concurrency %d: line ran %d times, want 12", concurrency, runs)
		}
		if _, leaked := result.State["line"]; leaked {
			t.Fatalf("concurrency %d: a body step leaked into the workflow scope", concurrency)
		}
	}
}

func TestEachFailsFastAndCancelsItemsInFlight(t *testing.T) {
	n := newLoopNodes()
	started := time.Now()
	_, err := runLoop(t, n, eachLines(n, 4), object{"lines": lines("block", "block", "block", "boom", "a", "b", "c", "d")})
	var classified interface{ ErrorCode() string }
	if err == nil || !errors.As(err, &classified) || classified.ErrorCode() != "node_error" || !strings.Contains(err.Error(), "boom failed") {
		t.Fatalf("err=%v; want the failing item's node_error", err)
	}
	if strings.Contains(fmt.Sprint(err), notCanceled) || n.canceled.Load() != 3 {
		t.Fatalf("canceled=%d err=%v; want the 3 blocked items canceled", n.canceled.Load(), err)
	}
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("fail-fast took %s", elapsed)
	}
}

// Parallel arms run concurrently (they meet at a rendezvous neither can pass
// alone), and their steps are readable once every arm has completed.
func TestParallelArmsRunConcurrentlyAndJoinBeforeTheirResultsAreRead(t *testing.T) {
	define := func(n *loopNodes) flow.Definition[object, object] {
		return flow.MustDefine(controlSpec, func(b *flow.Builder, in flow.Ref[object]) flow.Ref[object] {
			var price, stock flow.Ref[object]
			flow.Parallel(b, "fan",
				func(arm *flow.ArmBuilder) { price = flow.ArmCall(arm, "price", n.price, in) },
				func(arm *flow.ArmBuilder) { stock = flow.ArmCall(arm, "stock", n.stock, in) })
			available := flow.Compare(b, "available", "eq", flow.Select[object, bool](stock, "available"), flow.Lit(true))
			return flow.If(b, "decide", available,
				func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "accept", n.verdict, price) },
				func(arm *flow.ArmBuilder) flow.Ref[object] { return flow.ArmCall(arm, "reject", n.verdict, stock) })
		})
	}
	for sku, want := range map[string]object{
		"tea":      {"verdict": map[string]any{"total": float64(6)}},
		"sold-out": {"verdict": map[string]any{"available": false}},
	} {
		n := newLoopNodes()
		result, err := runLoop(t, n, define(n), object{"sku": sku, "quantity": 2})
		if err != nil {
			t.Fatalf("%s: %v", sku, err)
		}
		if canonical(t, result.Output) != canonical(t, want) {
			t.Fatalf("%s: output=%s want %s", sku, canonical(t, result.Output), canonical(t, want))
		}
	}

	n := newLoopNodes()
	_, err := runLoop(t, n, define(n), object{"sku": "boom", "quantity": 2})
	if err == nil || !strings.Contains(err.Error(), "stock lookup failed") || n.canceled.Load() != 1 {
		t.Fatalf("fail-fast: err=%v canceled=%d; want stock's failure and price canceled", err, n.canceled.Load())
	}
}
