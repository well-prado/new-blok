package otel_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/observe/otel"
)

type nopSpans struct{}

func (nopSpans) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error { return nil }
func (nopSpans) Shutdown(context.Context) error                             { return nil }

// TestShutdownRacingObserveKeepsTheAccounting races four producers calling
// Observe against Shutdown with varied deadlines (already expired, 1 ms,
// 10 ms, unbounded), many times. After Shutdown returns nothing more may be
// accepted, nothing may be left queued, every accepted event must be either
// processed or counted as discarded, and every offered event must be
// accepted or counted as dropped. Without the Observe/Shutdown producer
// handshake a send that passed the closed check lands after the final drain.
func TestShutdownRacingObserveKeepsTheAccounting(t *testing.T) {
	iterations := 300
	if testing.Short() {
		iterations = 50
	}
	deadlines := []time.Duration{-1, time.Millisecond, 10 * time.Millisecond, 0}
	for i := 0; i < iterations; i++ {
		exporter, err := otel.New(otel.Config{Traces: nopSpans{}, QueueSize: 64, FlushInterval: time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		var offered atomic.Uint64
		stop := make(chan struct{})
		var producers sync.WaitGroup
		for p := 0; p < 4; p++ {
			producers.Add(1)
			go func() {
				defer producers.Done()
				for n := 0; ; n++ {
					select {
					case <-stop:
						return
					default:
					}
					offered.Add(1)
					exporter.Observe(inspection.Event{Kind: inspection.RunStarted, RunID: "run-" + strconv.Itoa(p) + "-" + strconv.Itoa(n), At: time.Now()})
				}
			}()
		}
		time.Sleep(time.Duration(i%3) * 100 * time.Microsecond)
		ctx := context.Background()
		cancel := func() {}
		switch d := deadlines[i%len(deadlines)]; {
		case d < 0:
			ctx, cancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
		case d > 0:
			ctx, cancel = context.WithTimeout(ctx, d)
		}
		_ = exporter.Shutdown(ctx)
		cancel()
		after := exporter.Stats()
		time.Sleep(200 * time.Microsecond) // producers keep offering after Shutdown returned
		close(stop)
		producers.Wait()
		final := exporter.Stats()
		if final.Accepted != after.Accepted || final.Processed != after.Processed {
			t.Fatalf("iteration %d: accepted %d → %d, processed %d → %d after Shutdown returned", i, after.Accepted, final.Accepted, after.Processed, final.Processed)
		}
		if final.Queued != 0 || final.Accepted != final.Processed+final.DroppedShutdown {
			t.Fatalf("iteration %d: accepted %d != processed %d + discarded %d (queued %d)", i, final.Accepted, final.Processed, final.DroppedShutdown, final.Queued)
		}
		if got := offered.Load(); final.Accepted+final.Dropped+final.DroppedClosed != got {
			t.Fatalf("iteration %d: offered %d != accepted %d + dropped %d + closed %d", i, got, final.Accepted, final.Dropped, final.DroppedClosed)
		}
	}
}
