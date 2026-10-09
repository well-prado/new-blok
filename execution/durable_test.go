package execution_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/flow"
	"github.com/well-prado/new-blok/internal/program"
	"github.com/well-prado/new-blok/node"
)

type durableItem struct {
	Value int `json:"value"`
}

var durableItemSchema = []byte(`{"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}`)

// TestDurableRefusesAMemoryWorkflow: only a workflow declared durable is
// registered for durable execution; the default (Memory) is refused.
func TestDurableRefusesAMemoryWorkflow(t *testing.T) {
	for _, durability := range []flow.Durability{"", flow.Memory} {
		definition := flow.MustDefine(flow.Spec{Name: "memory", Version: "1.0.0", Durability: durability}, func(b *flow.Builder, in flow.Ref[durableItem]) flow.Ref[durableItem] {
			return flow.Call(b, "same", sameNode(), in)
		})
		if _, err := execution.Durable(definition); !errors.Is(err, execution.ErrNotDurable) {
			t.Fatalf("durability %q: err=%v; want ErrNotDurable", durability, err)
		}
	}
}

func sameNode() node.Definition[durableItem, durableItem] {
	return node.MustDefine("test/same", "1.0.0", func(_ context.Context, in durableItem) (durableItem, error) { return in, nil },
		node.Description("same"), node.Schemas(durableItemSchema, durableItemSchema))
}

// TestDurableProgramDigestsArePinned: a run admitted under a digest runs
// only that program, so the digest must not move unless the program does.
// A call-only workflow keeps its version-1 artifact digest; a control
// program's is the SHA-256 of a versioned encoding of it.
func TestDurableProgramDigestsArePinned(t *testing.T) {
	callOnly, err := execution.Durable(flow.MustDefine(flow.Spec{Name: "pinned", Version: "1.0.0", Durability: flow.Durable}, func(b *flow.Builder, in flow.Ref[durableItem]) flow.Ref[durableItem] {
		return flow.Call(b, "same", sameNode(), in)
	}))
	if err != nil {
		t.Fatal(err)
	}
	waits, err := execution.Durable(flow.MustDefine(flow.Spec{Name: "pinned", Version: "1.0.0", Durability: flow.Durable}, func(b *flow.Builder, in flow.Ref[durableItem]) flow.Ref[durableItem] {
		flow.Wait(b, "approval", "approval", time.Minute)
		return flow.Call(b, "same", sameNode(), in)
	}))
	if err != nil {
		t.Fatal(err)
	}
	lowered, err := flow.MustDefine(flow.Spec{Name: "pinned", Version: "1.0.0", Durability: flow.Durable}, func(b *flow.Builder, in flow.Ref[durableItem]) flow.Ref[durableItem] {
		return flow.Call(b, "same", sameNode(), in)
	}).Lower()
	if err != nil {
		t.Fatal(err)
	}
	if built, err := program.Build(lowered, program.Limits{}); err != nil || built.Digest() != callOnly.Digest() {
		t.Fatalf("call-only digest %s; want its version-1 artifact digest (%v)", callOnly.Digest(), err)
	}
	for name, got := range map[string]string{"call-only": callOnly.Digest(), "with a wait": waits.Digest()} {
		want := map[string]string{
			"call-only":   "sha256:8b60feaec05fe3ad2b7104d527bf92e6f6c722b8008f19d109e9bede20123ca5",
			"with a wait": "sha256:759a4008e5312756f6679fd7c4599a79dfc12ed97fee6db492fa815a833e81fb",
		}[name]
		if got != want {
			t.Errorf("%s digest %s; want %s", name, got, want)
		}
	}
}

// signalNode reads the signal a wait resumed with as a typed flow.Signal.
func signalNode() node.Definition[flow.Signal, durableItem] {
	return node.MustDefine("test/signal", "1.0.0", func(_ context.Context, in flow.Signal) (durableItem, error) {
		var item durableItem
		err := json.Unmarshal(in.Payload, &item)
		return item, err
	}, node.Description("signal"), node.Schemas([]byte(`{"type":"object"}`), durableItemSchema))
}

func durableRuntime(t *testing.T, path string, nodes map[string]node.Any, definitions ...flow.Definition[durableItem, durableItem]) (*execution.DurableRuntime, *app.Application) {
	t.Helper()
	var workflows []execution.DurableWorkflow
	for _, definition := range definitions {
		workflow, err := execution.Durable(definition)
		if err != nil {
			t.Fatal(err)
		}
		workflows = append(workflows, workflow)
	}
	runtime, err := execution.NewDurable(execution.DurableConfig{Path: path, Nodes: nodes, Workflows: workflows, WakeupLease: time.Second, Interval: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{Dependencies: []app.Dependency{runtime.Dependency()}})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return runtime, application
}

// TestDurableWaitHandsTheSignalToTypedNodes: a flow.Wait suspends the run;
// a signal resumes it, and the next node reads it as a typed flow.Signal.
func TestDurableWaitHandsTheSignalToTypedNodes(t *testing.T) {
	ctx := context.Background()
	definition := flow.MustDefine(flow.Spec{Name: "approve", Version: "1.0.0", Durability: flow.Durable}, func(b *flow.Builder, in flow.Ref[durableItem]) flow.Ref[durableItem] {
		approval := flow.Wait(b, "approval", "approval", 0)
		return flow.Call(b, "read", signalNode(), approval)
	})
	runtime, application := durableRuntime(t, filepath.Join(t.TempDir(), "journal.db"), map[string]node.Any{"test/signal": signalNode().Any()}, definition)
	defer application.Shutdown(ctx)
	run, err := runtime.Run(ctx, "approve", durableItem{Value: 1}, execution.RunOptions{})
	if err != nil || run.State != "suspended" {
		t.Fatalf("run %+v err=%v; want suspended", run, err)
	}
	if taken, err := runtime.Signal(ctx, run.RunID, "approval", "s1", "operator", json.RawMessage(`{"value":7}`)); err != nil || !taken.Resumed {
		t.Fatalf("signal %+v err=%v", taken, err)
	}
	var result execution.DurableResult
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if result, err = runtime.Status(ctx, run.RunID); err != nil || result.State == "completed" || result.State == "failed" {
			break
		}
	}
	if result.State != "completed" || string(result.Output) != `{"value":7}` {
		t.Fatalf("run %+v (%s) err=%v; want completed with the signal's payload", result, result.Output, err)
	}
}

// TestDurableShutdownHonoursItsDeadline: shutting the application down
// while a run blocks returns by its deadline and leaves the run accepted,
// not failed; a runtime started again on the same journal completes it.
func TestDurableShutdownHonoursItsDeadline(t *testing.T) {
	ctx := context.Background()
	release := make(chan struct{})
	hold := node.MustDefine("test/hold", "1.0.0", func(ctx context.Context, in durableItem) (durableItem, error) {
		select {
		case <-release:
			return in, nil
		case <-ctx.Done():
			return durableItem{}, ctx.Err()
		}
	}, node.Description("hold"), node.Schemas(durableItemSchema, durableItemSchema))
	definition := flow.MustDefine(flow.Spec{Name: "held", Version: "1.0.0", Durability: flow.Durable}, func(b *flow.Builder, in flow.Ref[durableItem]) flow.Ref[durableItem] {
		return flow.Call(b, "hold", hold, in)
	})
	path := filepath.Join(t.TempDir(), "journal.db")
	runtime, application := durableRuntime(t, path, map[string]node.Any{"test/hold": hold.Any()}, definition)
	waited, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	run, err := runtime.Run(waited, "held", durableItem{Value: 3}, execution.RunOptions{})
	cancel()
	if err != nil || run.State != "accepted" {
		t.Fatalf("run %+v err=%v; want accepted while it blocks", run, err)
	}
	deadline, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	started := time.Now()
	_ = application.Shutdown(deadline)
	stop()
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("shutdown took %v", elapsed)
	}
	close(release)
	again, restarted := durableRuntime(t, path, map[string]node.Any{"test/hold": hold.Any()}, definition)
	defer restarted.Shutdown(ctx)
	var result execution.DurableResult
	for until := time.Now().Add(10 * time.Second); time.Now().Before(until); time.Sleep(20 * time.Millisecond) {
		if result, err = again.Status(ctx, run.RunID); err != nil || result.State == "completed" {
			break
		}
	}
	if result.State != "completed" || string(result.Output) != `{"value":3}` {
		t.Fatalf("after a restart: %+v (%s) err=%v; want completed", result, result.Output, err)
	}
}
