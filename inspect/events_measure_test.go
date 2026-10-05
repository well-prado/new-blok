package inspect_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
)

// streamRunner composes an application whose only observer is a stream
// built from config (none when config is nil).
func streamRunner(t *testing.T, config *inspect.EventStreamConfig) (*execution.Runner, *inspect.EventStream) {
	t.Helper()
	var observer inspection.Observer
	var stream *inspect.EventStream
	if config != nil {
		var err error
		if stream, err = inspect.NewEventStream(*config); err != nil {
			t.Fatal(err)
		}
		observer = stream
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "quote"}}, Inspection: observer})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	quoteNode, err := quote.NewNode(&catalog{})
	if err != nil {
		t.Fatal(err)
	}
	return execution.NewRunner(application, map[string]node.Any{"test/gate": (&gateNode{}).node(t), "shop/calculate-quote": quoteNode.Any()}), stream
}

func percentiles(samples []time.Duration) string {
	sorted := slices.Clone(samples)
	slices.Sort(sorted)
	at := func(p float64) time.Duration { return sorted[min(len(sorted)-1, int(p*float64(len(sorted))))] }
	return fmt.Sprintf("n=%d p50=%s p95=%s p99=%s max=%s", len(sorted), at(0.50), at(0.95), at(0.99), sorted[len(sorted)-1])
}

// TestEventStreamOverheadOnRunLatency measures what the stream adds to a
// run: the same three-step native workflow through execution.Runner with no
// observer, with the stream and no readers (zero and full capture), and with
// eight stalled readers per run that the hub must cut off. Modes are
// interleaved in rounds so drift on a shared machine hits all of them. The
// figures are logged raw; they are evidence, not a performance claim.
func TestEventStreamOverheadOnRunLatency(t *testing.T) {
	// The workflow logs once per run in every mode; without an inspection
	// logger that log goes to the default slog handler, discarded here so the
	// baseline is not charged for terminal output.
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	const rounds, perRound = 10, 100
	type mode struct {
		name    string
		stream  *inspect.EventStream
		stalled int
		runner  *execution.Runner
		samples []time.Duration
	}
	newMode := func(name string, config *inspect.EventStreamConfig, stalled int) *mode {
		runner, stream := streamRunner(t, config)
		return &mode{name: name, stream: stream, stalled: stalled, runner: runner}
	}
	modes := []*mode{
		newMode("no observer", nil, 0),
		newMode("stream, zero capture, no readers", &inspect.EventStreamConfig{}, 0),
		newMode("stream, full capture, no readers", &inspect.EventStreamConfig{Capture: fullCapture}, 0),
		// Each run is pre-attached so its readers exist before it starts. A
		// recovered attach may displace only closed runs, so finished runs
		// close quickly here (LateWindow 1 ms) to keep making room.
		newMode("stream, full capture, 8 stalled readers", &inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{QueueDepth: 2, LateWindow: time.Millisecond}}, 8),
	}
	program := gatedQuoteProgram()
	share := func(string, string) error { return nil }
	for round := 0; round < rounds; round++ {
		for _, current := range modes {
			for i := 0; i < perRound; i++ {
				runID := fmt.Sprintf("measure-%d-%d", round, i)
				var subs []*event.Subscriber
				if current.stalled > 0 {
					hub := current.stream.Hub()
					if err := hub.AttachRecovered(runID, "alice", "alice", false); err != nil {
						t.Fatal(err)
					}
					for reader := 0; reader < current.stalled; reader++ {
						replay, err := hub.Subscribe(runID, fmt.Sprintf("reader-%d", reader), "", share)
						if err != nil {
							t.Fatal(err)
						}
						subs = append(subs, replay.Subscriber)
					}
				}
				started := time.Now()
				result, err := current.runner.Run(context.Background(), program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: runID, Principal: "alice"})
				current.samples = append(current.samples, time.Since(started))
				if err != nil || result.Output != int64(1500) {
					t.Fatalf("%s run=%+v err=%v", current.name, result, err)
				}
				for _, sub := range subs {
					select {
					case <-sub.Done():
					default:
						t.Fatalf("%s: a stalled reader was not cut off", current.name)
					}
					// The reader's connection ends, releasing its admission.
					current.stream.Hub().Unsubscribe(sub, "test")
				}
			}
		}
	}
	for _, current := range modes {
		t.Logf("%-42s %s", current.name, percentiles(current.samples))
		if current.stalled > 0 {
			stats := current.stream.Hub().Stats()
			t.Logf("%-42s slowSubscribers=%d subscribers=%d", "", stats.SlowSubscribers, stats.Subscribers)
			if stats.SlowSubscribers != uint64(rounds*perRound*current.stalled) || stats.Subscribers != 0 {
				t.Fatalf("stats=%+v", stats)
			}
		}
	}
}

// TestEventStreamDeliveryLatencyLeaksAndRetainedMemory measures delivery
// latency to an actual HTTP subscriber, checks that subscribe/disconnect
// cycles leave no goroutine behind, and that the retained heap after many
// runs stays within the hub's configured retention bound.
func TestEventStreamDeliveryLatencyLeaksAndRetainedMemory(t *testing.T) {
	gate := &gateNode{hold: "held", release: make(chan struct{}), entered: make(chan struct{}, 1)}
	hubConfig := event.Config{MaxRuns: 64, LateWindow: 10 * time.Millisecond}
	live := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: fullCapture, Hub: hubConfig}, nodes: map[string]node.Any{"test/gate": gate.node(t)}})
	effective := live.stream.Hub().Config()

	// Delivery latency: observation time (the log record) to receipt by
	// an HTTP subscriber, for a run that logs while the subscriber follows.
	pacedLogs := make(chan struct{})
	paced, err := node.Define("test/paced", "1.0.0", func(ctx context.Context, input quote.Input) (quote.Input, error) {
		<-pacedLogs
		for i := 0; i < 500; i++ {
			node.Logger(ctx).Info("tick", "n", i)
			time.Sleep(200 * time.Microsecond)
		}
		return input, nil
	}, node.Description("Synthetic paced logger"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)))
	if err != nil {
		t.Fatal(err)
	}
	pacedApp := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{QueueDepth: 512, MaxSubscribers: 8, SubscribersPerRun: 8, SubscribersPerPrincipal: 8, LateWindow: 10 * time.Millisecond}}, nodes: map[string]node.Any{"test/gate": paced.Any()}})
	result := pacedApp.start("latency-run", "alice", quote.Input{SKU: "coffee", Quantity: 1})
	stream := awaitStream(t, context.Background(), pacedApp.eventsURL("latency-run"), "alice")
	close(pacedLogs)
	frames := stream.rest(t, 30*time.Second)
	stream.close()
	awaitResult(t, result)
	var latencies []time.Duration
	for _, frame := range frames {
		if frame.Event == "step.log" {
			latencies = append(latencies, frame.At.Sub(frame.decode(t).At))
		}
	}
	if len(latencies) != 500 {
		t.Fatalf("delivered %d of 500 logs; frames=%d", len(latencies), len(frames))
	}
	t.Logf("delivery latency, observation to HTTP receipt: %s", percentiles(latencies))

	// Leaks: abandoned and completed subscriptions leave nothing behind.
	waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 0 })
	http.DefaultClient.CloseIdleConnections()
	runtime.GC()
	baseline := runtime.NumGoroutine()
	held := live.start("held-run", "alice", quote.Input{SKU: "held", Quantity: 1})
	<-gate.entered
	const cycles = 100
	for i := 0; i < cycles; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		abandoned := awaitStream(t, ctx, live.eventsURL("held-run"), "alice")
		abandoned.until(t, "step.processing", 5*time.Second)
		cancel()
		abandoned.close()
	}
	close(gate.release)
	awaitResult(t, held)
	for i := 0; i < cycles; i++ {
		_, _, completed := openStream(t, context.Background(), live.eventsURL("held-run"), "alice", "")
		completed.rest(t, 5*time.Second)
	}
	waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 0 })
	http.DefaultClient.CloseIdleConnections()
	var after int
	deadline := time.Now().Add(10 * time.Second)
	for {
		runtime.GC()
		after = runtime.NumGoroutine()
		if after <= baseline+2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Logf("goroutines: baseline=%d after %d abandoned + %d completed subscriptions=%d", baseline, cycles, cycles, after)
	if after > baseline+2 {
		buffer := make([]byte, 1<<20)
		t.Fatalf("goroutines leaked: baseline=%d after=%d\n%s", baseline, after, buffer[:runtime.Stack(buffer, true)])
	}

	// Retained memory: thousands of runs keep at most the retention bound.
	// This application's only observer is the stream, so heap growth is
	// attributable to it.
	memoryRunner, memoryStream := streamRunner(t, &inspect.EventStreamConfig{Capture: fullCapture, Hub: hubConfig})
	var before, end runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	program := gatedQuoteProgram()
	const runs = 3000
	for i := 0; i < runs; i++ {
		if _, err := memoryRunner.Run(context.Background(), program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: fmt.Sprintf("memory-%d", i), Principal: "alice"}); err != nil {
			t.Fatal(err)
		}
	}
	runtime.GC()
	runtime.ReadMemStats(&end)
	stats := memoryStream.Hub().Stats()
	bound := effective.MaxRuns * effective.RunBytes
	growth := int64(end.HeapAlloc) - int64(before.HeapAlloc)
	t.Logf("after %d runs: hub runs=%d frames=%d retainedBytes=%d (bound %d) evictedRuns=%d; heap %d -> %d (growth %d bytes)", runs, stats.Runs, stats.Frames, stats.RetainedBytes, bound, stats.EvictedRuns, before.HeapAlloc, end.HeapAlloc, growth)
	if stats.Runs > effective.MaxRuns || stats.RetainedBytes > bound || growth > int64(bound)+8<<20 {
		t.Fatalf("stream exceeded its bound: %+v heap growth %d", stats, growth)
	}
}
