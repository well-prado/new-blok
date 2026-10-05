package otel_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/observe/otel"
)

// stallingSpans stalls every export until release. With honour it returns
// when its context ends; without it ignores the context entirely, as a
// badly behaved exporter would. Shutdown behaves the same way.
type stallingSpans struct {
	honour    bool
	release   chan struct{}
	exports   atomic.Int64
	shutdowns atomic.Int64
}

func (s *stallingSpans) wait(ctx context.Context) error {
	if !s.honour {
		<-s.release
		return nil
	}
	select {
	case <-s.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *stallingSpans) ExportSpans(ctx context.Context, _ []sdktrace.ReadOnlySpan) error {
	s.exports.Add(1)
	return s.wait(ctx)
}

func (s *stallingSpans) Shutdown(ctx context.Context) error {
	s.shutdowns.Add(1)
	return s.wait(ctx)
}

// pipelineGoroutines counts live goroutines running observe/otel code, the
// way goleak does, without a dependency: the export loop and any export or
// shutdown call it started. loops counts only the export loop.
func pipelineGoroutines() (all, loops int) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	for _, stack := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(stack, "new-blok/observe/otel.(*Exporter)") {
			all++
			if strings.Contains(stack, "new-blok/observe/otel.(*Exporter).loop(") {
				loops++
			}
		}
	}
	return all, loops
}

func waitGoroutines(t *testing.T, want int) (int, int) {
	t.Helper()
	var all, loops int
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		all, loops = pipelineGoroutines()
		if all <= want || time.Now().After(deadline) {
			return all, loops
		}
	}
}

// TestShutdownIsBoundedByItsContext reproduces the review case: a stalled
// trace exporter, BatchSize 8, ExportTimeout 2s, 32 runs, then Shutdown with
// a 100ms context. Shutdown must return by its deadline with the export loop
// already gone, the providers and the exporter shut down, every accepted
// event either processed or counted as discarded, and no pipeline goroutine
// left behind — except, for an exporter that ignores its context, the calls
// still inside it, which end when it returns.
func TestShutdownIsBoundedByItsContext(t *testing.T) {
	for _, honour := range []bool{true, false} {
		name := "exporter honours ctx"
		if !honour {
			name = "exporter ignores ctx"
		}
		t.Run(name, func(t *testing.T) {
			baseline, _ := waitGoroutines(t, 0)
			stall := &stallingSpans{honour: honour, release: make(chan struct{})}
			released := false
			defer func() {
				if !released {
					close(stall.release)
				}
			}()
			exporter, err := otel.New(otel.Config{Traces: stall, BatchSize: 8, ExportTimeout: 2 * time.Second, FlushInterval: 10 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			counter := &countingObserver{inner: exporter}
			h := &harness{exporter: exporter}
			runner, program := orderRunnerWith(t, h, counter)
			for i := 0; i < 32; i++ {
				if _, err := runner.Run(context.Background(), program, orderInput{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: "run-shutdown-" + string(rune('a'+i)), Principal: principalSentinel}); err != nil {
					t.Fatal(err)
				}
			}
			for deadline := time.Now().Add(2 * time.Second); stall.exports.Load() == 0 && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			}
			if stall.exports.Load() == 0 {
				t.Fatal("no export reached the stalled exporter; the case is vacuous")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			start := time.Now()
			err = exporter.Shutdown(ctx)
			elapsed := time.Since(start)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("Shutdown err=%v, want the context's deadline", err)
			}
			if elapsed > 600*time.Millisecond {
				t.Fatalf("Shutdown took %v with a 100ms context", elapsed)
			}
			if _, loops := pipelineGoroutines(); loops != 0 {
				t.Fatal("the export loop is still running after Shutdown returned")
			}
			// With an expired ctx the shutdown call is dispatched and not
			// waited for; it must still reach the exporter, exactly once.
			for deadline := time.Now().Add(time.Second); stall.shutdowns.Load() == 0 && time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			}
			if stall.shutdowns.Load() != 1 {
				t.Fatalf("exporter Shutdown called %d times, want 1 (providers must always be shut down)", stall.shutdowns.Load())
			}
			stats := exporter.Stats()
			if stats.Queued != 0 || stats.Accepted != stats.Processed+stats.DroppedShutdown {
				t.Fatalf("accepted %d != processed %d + discarded %d (queued %d)", stats.Accepted, stats.Processed, stats.DroppedShutdown, stats.Queued)
			}
			if offered := uint64(counter.offered.Load()); stats.Accepted+stats.Dropped+stats.DroppedClosed != offered {
				t.Fatalf("offered %d != accepted %d + dropped %d + closed %d", offered, stats.Accepted, stats.Dropped, stats.DroppedClosed)
			}
			if again := exporter.Shutdown(context.Background()); again != nil && !errors.Is(again, context.DeadlineExceeded) {
				t.Fatalf("second Shutdown: %v", again)
			}

			if honour {
				if all, _ := waitGoroutines(t, baseline); all > baseline {
					t.Fatalf("%d pipeline goroutines left (baseline %d)", all, baseline)
				}
				return
			}
			// The calls stuck inside the exporter are all that may remain:
			// at most one export (one slot per signal) and its Shutdown.
			if all, _ := pipelineGoroutines(); all > baseline+2 {
				t.Fatalf("%d pipeline goroutines while the exporter ignores ctx (baseline %d, at most 2 abandoned calls)", all, baseline)
			}
			close(stall.release)
			released = true
			if all, _ := waitGoroutines(t, baseline); all > baseline {
				t.Fatalf("%d pipeline goroutines left after the exporter returned (baseline %d)", all, baseline)
			}
		})
	}
}

// TestIgnoringExporterNeverAccumulatesGoroutines: while a context-ignoring
// exporter is stuck, later batches fail fast instead of each leaving a
// goroutine behind.
func TestIgnoringExporterNeverAccumulatesGoroutines(t *testing.T) {
	baseline, _ := waitGoroutines(t, 0)
	stall := &stallingSpans{release: make(chan struct{})}
	exporter, err := otel.New(otel.Config{Traces: stall, BatchSize: 1, ExportTimeout: 20 * time.Millisecond, FlushInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	h := &harness{exporter: exporter}
	runner, program := orderRunnerWith(t, h, exporter)
	for i := 0; i < 20; i++ {
		if _, err := runner.Run(context.Background(), program, orderInput{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: "run-ignore-" + string(rune('a'+i)), Principal: principalSentinel}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if all, _ := pipelineGoroutines(); all > baseline+2 {
		t.Fatalf("%d pipeline goroutines with a stuck exporter (baseline %d): abandoned calls accumulate", all, baseline)
	}
	if stall.exports.Load() != 1 || exporter.Stats().SpansFailed < 20 {
		t.Fatalf("exports=%d stats=%+v: later batches must fail fast while the first call is stuck", stall.exports.Load(), exporter.Stats())
	}
	close(stall.release)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := exporter.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if all, _ := waitGoroutines(t, baseline); all > baseline {
		t.Fatalf("%d pipeline goroutines left (baseline %d)", all, baseline)
	}
}
