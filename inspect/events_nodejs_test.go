package inspect_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
	"github.com/well-prado/new-blok/runtime/worker"
)

// TestActualNodeWorkerRunsStreamWorkerLogsIncludingLateOnes streams real
// HTTP-triggered runs of the actual Node worker. The worker's log travels
// off the call's result path (Call.OnLog), so it lands before or after the
// run's terminal transition depending on a race (#226). Whichever way it
// lands, the stream delivers it before its end frame, attributed to its
// step and with its credential attribute redacted.
func TestActualNodeWorkerRunsStreamWorkerLogsIncludingLateOnes(t *testing.T) {
	supervisor, descriptors := startNodeWorker(t)
	descriptor := descriptors["fixture/quote"]
	type output struct {
		Total int64 `json:"totalCents"`
	}
	definition, err := worker.Define[quote.Input, output](supervisor, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	program := contract.InternalProgram{WorkflowID: "quote", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "quote", Kind: "call", Node: descriptor.Name},
		{Index: 1, ID: "respond", Kind: "output", References: []contract.Reference{{Step: "quote", Path: []string{"totalCents"}}}},
	}}
	live := newLiveApp(t, liveConfig{
		stream:  inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{LateWindow: 300 * time.Millisecond}},
		nodes:   map[string]node.Any{descriptor.Name: definition.Any()},
		program: program,
	})
	const runs = 20
	lateCount := 0
	for i := 0; i < runs; i++ {
		runID := fmt.Sprintf("node-stream-%d", i)
		if got := awaitResult(t, live.start(runID, "alice", quote.Input{SKU: "coffee", Quantity: 2})); got[0] != "200 OK" || got[1] != `{"totalCents":3000}` {
			t.Fatalf("Node run %d=%v", i, got)
		}
		_, _, stream := openStream(t, context.Background(), live.eventsURL(runID), "alice", "")
		frames := stream.rest(t, 10*time.Second)
		names := stepNames(t, frames)
		if names[len(names)-1] != "end" {
			t.Fatalf("run %d frames=%v", i, names)
		}
		terminal, logged := -1, -1
		for index, frame := range frames {
			switch frame.Event {
			case "run.completed":
				terminal = index
			case "step.log":
				value := frame.decode(t)
				if value.StepID != "quote" || value.LogMessage != "quote calculated" || strings.Contains(frame.Data, "synthetic-token-value") || !strings.Contains(string(value.LogAttrs), "[redacted]") {
					t.Fatalf("run %d log frame=%s", i, frame.Data)
				}
				logged = index
			}
		}
		if terminal < 0 || logged < 0 {
			t.Fatalf("run %d is missing its terminal or its worker log: %v", i, names)
		}
		if logged > terminal {
			lateCount++
		}
	}
	t.Logf("actual Node worker: %d of %d runs delivered the worker log after run.completed; all %d were streamed", lateCount, runs, runs)
	// A failing Node run streams its classified failure.
	if got := awaitResult(t, live.start("node-stream-failed", "alice", quote.Input{SKU: "bad-sku", Quantity: 1})); got[0] == "200 OK" {
		t.Fatalf("failing Node run=%v", got)
	}
	_, _, failed := openStream(t, context.Background(), live.eventsURL("node-stream-failed"), "alice", "")
	frames := failed.rest(t, 10*time.Second)
	var failure *inspection.Event
	for _, frame := range frames {
		if frame.Event == "run.failed" {
			value := frame.decode(t)
			failure = &value
		}
	}
	if failure == nil || failure.ErrorCode == "" || failure.ErrorClass == "" {
		t.Fatalf("Node failure frames=%v failure=%+v", frameNames(frames), failure)
	}
}
