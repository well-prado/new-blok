package inspect_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
)

var fullCapture = inspect.Capture{Inputs: true, Outputs: true, Logs: true}

// TestLiveHTTPTriggerRunStreamsTransitionsAndLogs follows an actual
// trigger/http request while it runs: the subscriber attaches mid-run, sees
// the running step and its log, then the rest of the run, the terminal
// transition and an explicit end. Reconnecting from the end is answered 204,
// which stops EventSource.
func TestLiveHTTPTriggerRunStreamsTransitionsAndLogs(t *testing.T) {
	gate := &gateNode{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	live := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{LateWindow: 100 * time.Millisecond}}, nodes: map[string]node.Any{"test/gate": gate.node(t)}})
	result := live.start("http-run-1", "alice", quote.Input{SKU: "coffee", Quantity: 2})
	<-gate.entered
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := awaitStream(t, ctx, live.eventsURL("http-run-1"), "alice")
	defer stream.close()
	before := stream.until(t, "step.log", 5*time.Second)
	if got := strings.Join(stepNames(t, before), ","); got != "run.started,step.processing:gate,step.log:gate" {
		t.Fatalf("frames before release=%s", got)
	}
	close(gate.release)
	after := stream.rest(t, 10*time.Second)
	if got := awaitResult(t, result); got[0] != "200 OK" || got[1] != `{"totalCents":3000}` {
		t.Fatalf("HTTP result=%v", got)
	}
	all := append(before, after...)
	want := "run.started,step.processing:gate,step.log:gate,step.completed:gate,step.processing:calculate,step.completed:calculate,step.processing:respond,step.completed:respond,run.completed,end"
	if got := strings.Join(stepNames(t, all), ","); got != want {
		t.Fatalf("frames=%s\nwant  =%s", got, want)
	}
	seen := map[string]bool{}
	for _, frame := range all[:len(all)-1] {
		if frame.ID == "" || seen[frame.ID] {
			t.Fatalf("frame %s has missing or repeated id %q", frame.Event, frame.ID)
		}
		seen[frame.ID] = true
		if strings.Contains(frame.Data, "alice") || strings.Contains(frame.Data, "synthetic-token-value") {
			t.Fatalf("frame leaks the owner or a credential: %s", frame.Data)
		}
	}
	logged := before[2].decode(t)
	if logged.LogMessage != "gate opened" || !strings.Contains(string(logged.LogAttrs), `"api_token":"[redacted]"`) {
		t.Fatalf("log frame=%+v", logged)
	}
	calculated := all[5].decode(t)
	if string(calculated.Input) != `{"quantity":2,"sku":"coffee"}` || !strings.Contains(string(calculated.Output), `"totalCents":3000`) {
		t.Fatalf("captured payloads=%s / %s", calculated.Input, calculated.Output)
	}
	end := all[len(all)-1]
	if end.ID != all[len(all)-2].ID {
		t.Fatalf("end id=%q want the terminal id %q", end.ID, all[len(all)-2].ID)
	}
	response, _, _ := openStream(t, ctx, live.eventsURL("http-run-1"), "alice", end.ID)
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("reconnect at end status=%d", response.StatusCode)
	}
	if live.catalog.calls.Load() != 1 {
		t.Fatalf("effects=%d, want exactly one", live.catalog.calls.Load())
	}
}

// TestDisconnectAndReconnectResumeFromCursorWithoutLossOrDuplicates drops
// a subscriber mid-run and resumes it from its Last-Event-ID. What it
// receives across both connections equals what an uninterrupted subscriber
// received.
func TestDisconnectAndReconnectResumeFromCursorWithoutLossOrDuplicates(t *testing.T) {
	gate := &gateNode{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	live := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{LateWindow: 50 * time.Millisecond}}, nodes: map[string]node.Any{"test/gate": gate.node(t)}})
	result := live.start("http-run-2", "alice", quote.Input{SKU: "coffee", Quantity: 1})
	<-gate.entered
	steady := awaitStream(t, context.Background(), live.eventsURL("http-run-2"), "alice")
	defer steady.close()
	firstCtx, disconnect := context.WithCancel(context.Background())
	first := awaitStream(t, firstCtx, live.eventsURL("http-run-2"), "alice")
	received := first.until(t, "step.processing", 5*time.Second)
	disconnect()
	first.close()
	// The client is gone; the hub must release its subscription.
	waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 1 })
	close(gate.release)
	awaitResult(t, result)
	_, _, resumed := openStream(t, context.Background(), live.eventsURL("http-run-2"), "alice", received[len(received)-1].ID)
	defer resumed.close()
	received = append(received, resumed.rest(t, 10*time.Second)...)
	reference := steady.rest(t, 10*time.Second)
	if strings.Join(stepNames(t, received), ",") != strings.Join(stepNames(t, reference), ",") {
		t.Fatalf("resumed=%v\nsteady =%v\nresumed comments=%v", stepNames(t, received), stepNames(t, reference), resumed.Comments())
	}
	for index := range reference[:len(reference)-1] {
		if received[index].ID != reference[index].ID || received[index].Data != reference[index].Data {
			t.Fatalf("frame %d differs: %+v vs %+v", index, received[index], reference[index])
		}
	}
	for _, frame := range received {
		if frame.Event == event.GapName {
			t.Fatalf("a resume within retention reported a gap: %+v", frame)
		}
	}
}

// TestCursorGapsAreVisibleAndForeignCursorsRejected uses a tiny retention so
// an early cursor has been evicted, and checks every malformed or foreign
// cursor is refused before any stream bytes.
func TestCursorGapsAreVisibleAndForeignCursorsRejected(t *testing.T) {
	gate := &gateNode{logs: 40}
	live := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{EventsPerRun: 8, LateWindow: 20 * time.Millisecond}}, nodes: map[string]node.Any{"test/gate": gate.node(t)}})
	ctx := context.Background()
	if got := awaitResult(t, live.start("gap-run", "alice", quote.Input{SKU: "coffee", Quantity: 1})); got[0] != "200 OK" {
		t.Fatalf("run=%v", got)
	}
	// A fresh reader of a run whose start was evicted sees a counted gap.
	_, _, stream := openStream(t, ctx, live.eventsURL("gap-run"), "alice", "")
	frames := stream.rest(t, 5*time.Second)
	if frames[0].Event != event.GapName || !strings.Contains(frames[0].Data, `"reason":"retention"`) || !strings.Contains(frames[0].Data, `"missed":`) {
		t.Fatalf("first frame=%+v", frames[0])
	}
	if got := frameNames(frames); len(got) != 10 || got[len(got)-2] != "run.completed" || got[len(got)-1] != "end" {
		t.Fatalf("retained frames=%v", got)
	}
	// Reconnecting from the gap's id continues without another gap.
	_, _, again := openStream(t, ctx, live.eventsURL("gap-run"), "alice", frames[0].ID)
	if more := again.rest(t, 5*time.Second); more[0].Event == event.GapName || len(more) != 9 {
		t.Fatalf("after gap cursor=%v", frameNames(more))
	}
	if got := awaitResult(t, live.start("other-run", "alice", quote.Input{SKU: "coffee", Quantity: 1})); got[0] != "200 OK" {
		t.Fatalf("run=%v", got)
	}
	_, _, other := openStream(t, ctx, live.eventsURL("other-run"), "alice", "")
	otherFrames := other.rest(t, 5*time.Second)
	head, _, _ := strings.Cut(frames[1].ID, ":")
	for name, cursor := range map[string]string{
		"malformed": "not-a-cursor",
		"ahead":     head + ":9999",
		"other run": otherFrames[0].ID,
		"oversized": strings.Repeat("a", event.MaxCursorBytes+1),
	} {
		response, body, _ := openStream(t, ctx, live.eventsURL("gap-run"), "alice", cursor)
		if response.StatusCode != http.StatusBadRequest || !strings.Contains(body, "invalid_cursor") {
			t.Errorf("%s: status=%d body=%s", name, response.StatusCode, body)
		}
	}
	// A cursor from another process epoch is reported as a restart.
	restarted := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: fullCapture}, nodes: map[string]node.Any{"test/gate": (&gateNode{}).node(t)}})
	awaitResult(t, restarted.start("gap-run", "alice", quote.Input{SKU: "coffee", Quantity: 1}))
	_, _, fromOld := openStream(t, ctx, restarted.eventsURL("gap-run"), "alice", frames[3].ID)
	restartFrames := fromOld.rest(t, 5*time.Second)
	if restartFrames[0].Event != event.GapName || !strings.Contains(restartFrames[0].Data, `"reason":"restart"`) || restartFrames[1].Event != "run.started" {
		t.Fatalf("restart frames=%v %s", frameNames(restartFrames), restartFrames[0].Data)
	}
}

// TestAuthorizationAndCaptureArePolicyChecked covers the access rules:
// unauthenticated 401; another principal and an unknown run are the same
// 404; the zero capture configuration keeps no inputs, outputs or logs even
// for a reader whose policy allows them; a policy that withholds a field
// withholds it even when it was captured.
func TestAuthorizationAndCaptureArePolicyChecked(t *testing.T) {
	ctx := context.Background()
	defaults := newLiveApp(t, liveConfig{nodes: map[string]node.Any{"test/gate": (&gateNode{}).node(t)}})
	awaitResult(t, defaults.start("cap-run", "alice", quote.Input{SKU: "coffee", Quantity: 1}))
	if response, body, _ := openStream(t, ctx, defaults.eventsURL("cap-run"), "", ""); response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d body=%s", response.StatusCode, body)
	}
	foreign, foreignBody, _ := openStream(t, ctx, defaults.eventsURL("cap-run"), "mallory", "")
	unknown, unknownBody, _ := openStream(t, ctx, defaults.eventsURL("no-such-run"), "mallory", "")
	if foreign.StatusCode != http.StatusNotFound || unknown.StatusCode != http.StatusNotFound || foreignBody != unknownBody {
		t.Fatalf("foreign=%d %q unknown=%d %q", foreign.StatusCode, foreignBody, unknown.StatusCode, unknownBody)
	}
	if response, _, _ := openStream(t, ctx, defaults.server.URL+"/inspect/runs/a%2Fb/events", "alice", ""); response.StatusCode != http.StatusNotFound {
		t.Fatalf("bad path status=%d", response.StatusCode)
	}
	request, _ := http.NewRequest(http.MethodPost, defaults.eventsURL("cap-run"), nil)
	request.Header.Set("X-Principal", "alice")
	if response, err := http.DefaultClient.Do(request); err != nil || response.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("POST status=%v err=%v", response, err)
	}
	_, _, stream := openStream(t, ctx, defaults.eventsURL("cap-run"), "alice", "")
	for _, frame := range stream.rest(t, 5*time.Second) {
		value := frame.decode(t)
		if frame.Event == "step.log" || value.Input != nil || value.Output != nil || value.LogAttrs != nil || strings.Contains(frame.Data, "coffee") {
			t.Fatalf("zero capture exposed content: %s %s", frame.Event, frame.Data)
		}
	}
	// Not merely withheld on the wire: never retained. Read the hub's own
	// frames, beneath any reader projection.
	retained, err := defaults.stream.Hub().Subscribe("cap-run", "alice", "", nil)
	if err != nil || len(retained.Frames) == 0 {
		t.Fatalf("retained=%+v err=%v", retained, err)
	}
	for _, frame := range retained.Frames {
		if frame.Name() == "step.log" || strings.Contains(string(frame.Data()), "coffee") || strings.Contains(string(frame.Data()), `"input"`) || strings.Contains(string(frame.Data()), `"output"`) {
			t.Fatalf("zero capture retained content: %s %s", frame.Name(), frame.Data())
		}
	}
	if retained.Subscriber != nil {
		defaults.stream.Hub().Unsubscribe(retained.Subscriber, "test")
	}

	readers := map[string]inspection.Policy{
		"alice":    allFields(),
		"reviewer": {Fields: map[inspection.Field]bool{inspection.FieldError: true}},
	}
	captured := newLiveApp(t, liveConfig{
		stream: inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{LateWindow: 20 * time.Millisecond}},
		handler: inspect.EventHandlerConfig{Policy: func(principal string) inspection.Policy { return readers[principal] }, Authorize: func(reader, owner string) error {
			return map[bool]error{true: nil, false: event.ErrUnauthorized}[reader == owner || reader == "reviewer"]
		}},
		nodes: map[string]node.Any{"test/gate": (&gateNode{}).node(t)},
	})
	awaitResult(t, captured.start("cap-run", "alice", quote.Input{SKU: "coffee", Quantity: 1}))
	_, _, owner := openStream(t, ctx, captured.eventsURL("cap-run"), "alice", "")
	ownerFrames := owner.rest(t, 5*time.Second)
	if !strings.Contains(strings.Join(frameNames(ownerFrames), ","), "step.log") || !strings.Contains(ownerFrames[len(ownerFrames)-2].Data, `"output":1500`) {
		t.Fatalf("owner frames=%v last=%+v", frameNames(ownerFrames), ownerFrames[len(ownerFrames)-2])
	}
	_, _, reviewer := openStream(t, ctx, captured.eventsURL("cap-run"), "reviewer", "")
	reviewerFrames := reviewer.rest(t, 5*time.Second)
	for _, frame := range reviewerFrames {
		value := frame.decode(t)
		if frame.Event == "step.log" || value.Input != nil || value.Output != nil {
			t.Fatalf("withheld field reached the reviewer: %s %s", frame.Event, frame.Data)
		}
	}
	if len(reviewerFrames) != len(ownerFrames)-1 {
		t.Fatalf("reviewer frames=%v owner=%v", frameNames(reviewerFrames), frameNames(ownerFrames))
	}
}

// TestSaturatedSubscribersAreTransientlyRefusedAndRunsUnaffected fills the
// subscriber and run capacity. A refused subscriber gets an empty stream
// with a retry hint (EventSource retries); a run admitted while the hub is
// full still completes, and its lost observations are counted.
func TestSaturatedSubscribersAreTransientlyRefusedAndRunsUnaffected(t *testing.T) {
	gate := &gateNode{hold: "held", release: make(chan struct{}), entered: make(chan struct{}, 1)}
	live := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Hub: event.Config{MaxRuns: 1, SubscribersPerRun: 1}}, nodes: map[string]node.Any{"test/gate": gate.node(t)}})
	result := live.start("sat-run", "alice", quote.Input{SKU: "held", Quantity: 1})
	<-gate.entered
	ctx := context.Background()
	held := awaitStream(t, ctx, live.eventsURL("sat-run"), "alice")
	defer held.close()
	held.until(t, "step.processing", 5*time.Second)
	_, _, refused := openStream(t, ctx, live.eventsURL("sat-run"), "alice", "")
	if frames := refused.rest(t, 5*time.Second); len(frames) != 0 || strings.Join(refused.Comments(), ",") != "saturated" || refused.retry == "" {
		t.Fatalf("refusal frames=%v comments=%v retry=%q", frameNames(frames), refused.Comments(), refused.retry)
	}
	// The only run slot is followed, so a second run cannot be retained.
	if got := awaitResult(t, live.start("unretained-run", "alice", quote.Input{SKU: "coffee", Quantity: 1})); got[0] != "200 OK" || got[1] != `{"totalCents":1500}` {
		t.Fatalf("run under a saturated hub=%v", got)
	}
	close(gate.release)
	awaitResult(t, result)
	stats := live.stream.Hub().Stats()
	if stats.RejectedSubscribers != 1 || stats.Dropped == 0 {
		t.Fatalf("stats=%+v", stats)
	}
	if page, err := live.recorder.Inspect("alice", allFields(), inspection.Query{Version: inspection.Version, RunID: "unretained-run"}); err != nil || page.Run.Status != inspection.StatusCompleted {
		t.Fatalf("the unretained run's other observer=%+v err=%v", page.Run, err)
	}
}

// lateLogger is a native node that keeps its step logger and logs once the
// test says so, after the run has completed. It exercises the same engine
// path a worker log delivered by Call.OnLog takes (#226): the engine's
// inspection logger attributes it to its step whenever it arrives.
type lateLogger struct {
	logger chan func(string)
}

func (l *lateLogger) node(t *testing.T) node.Any {
	definition, err := node.Define("test/gate", "1.0.0", func(ctx context.Context, input quote.Input) (quote.Input, error) {
		logger := node.Logger(ctx)
		l.logger <- func(message string) { logger.Info(message, "late", true) }
		return input, nil
	}, node.Description("Synthetic stream test node"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)))
	if err != nil {
		t.Fatal(err)
	}
	return definition.Any()
}

// TestLateStepLogAfterRunCompletedIsDeliveredWithinTheLateWindow is the ADR
// 0016 / #226 rule: a log can reach inspection after its step and its run
// completed. The stream keeps following for the late window after the
// terminal frame, so such a log is delivered, and it ends with an explicit
// end frame. A log after the window is dropped and marked, never silent.
func TestLateStepLogAfterRunCompletedIsDeliveredWithinTheLateWindow(t *testing.T) {
	late := &lateLogger{logger: make(chan func(string), 1)}
	const window = 700 * time.Millisecond
	live := newLiveApp(t, liveConfig{stream: inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{LateWindow: window}}, nodes: map[string]node.Any{"test/gate": late.node(t)}})
	result := live.start("late-run", "alice", quote.Input{SKU: "coffee", Quantity: 1})
	ctx := context.Background()
	stream := awaitStream(t, ctx, live.eventsURL("late-run"), "alice")
	defer stream.close()
	completed := stream.until(t, "run.completed", 5*time.Second)
	awaitResult(t, result)
	logAfterCompletion := <-late.logger
	terminalAt := completed[len(completed)-1].At
	logAfterCompletion("logged after the run completed")
	tail := stream.rest(t, 5*time.Second)
	if got := strings.Join(stepNames(t, tail), ","); got != "step.log:gate,end" {
		t.Fatalf("frames after run.completed=%s", got)
	}
	if message := tail[0].decode(t).LogMessage; message != "logged after the run completed" {
		t.Fatalf("late log=%q", message)
	}
	if held := tail[1].At.Sub(terminalAt); held < window-100*time.Millisecond || held > window+2*time.Second {
		t.Fatalf("stream ended %s after the terminal frame; late window is %s", held, window)
	}
	if !strings.Contains(tail[1].Data, `"lateWindowMs":700`) || tail[1].ID != tail[0].ID {
		t.Fatalf("end frame=%+v", tail[1])
	}
	// After the window a late log is dropped and marked for later readers.
	time.Sleep(window)
	logAfterCompletion("too late")
	_, _, again := openStream(t, ctx, live.eventsURL("late-run"), "alice", tail[1].ID)
	if frames := again.rest(t, 5*time.Second); strings.Join(frameNames(frames), ",") != "gap,end" || !strings.Contains(frames[0].Data, event.GapLateDropped) {
		t.Fatalf("after window frames=%v", frameNames(frames))
	}
	if stats := live.stream.Hub().Stats(); stats.LateDelivered != 1 || stats.LateDropped != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

// TestReaderWithoutLogAccessIsNotHeldForTheLateWindow: waiting for late
// logs is only worth it for a reader who may see them.
func TestReaderWithoutLogAccessIsNotHeldForTheLateWindow(t *testing.T) {
	live := newLiveApp(t, liveConfig{
		stream:  inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{LateWindow: 20 * time.Second}},
		handler: inspect.EventHandlerConfig{Policy: func(string) inspection.Policy { return inspection.Policy{} }},
		nodes:   map[string]node.Any{"test/gate": (&gateNode{}).node(t)},
	})
	awaitResult(t, live.start("nolog-run", "alice", quote.Input{SKU: "coffee", Quantity: 1}))
	started := time.Now()
	_, _, stream := openStream(t, context.Background(), live.eventsURL("nolog-run"), "alice", "")
	frames := stream.rest(t, 5*time.Second)
	if elapsed := time.Since(started); elapsed > 3*time.Second || frames[len(frames)-1].Event != "end" {
		t.Fatalf("frames=%v elapsed=%s", frameNames(frames), elapsed)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestSubscriptionLifetimeIsBoundedAndResumable: an idle subscription gets
// heartbeats, ends at MaxDuration however long the run takes, and resumes
// from its cursor without loss.
func TestSubscriptionLifetimeIsBoundedAndResumable(t *testing.T) {
	gate := &gateNode{release: make(chan struct{}), entered: make(chan struct{}, 1)}
	live := newLiveApp(t, liveConfig{
		stream:  inspect.EventStreamConfig{Capture: fullCapture, Hub: event.Config{LateWindow: 20 * time.Millisecond}},
		handler: inspect.EventHandlerConfig{MaxDuration: 400 * time.Millisecond, Heartbeat: 50 * time.Millisecond},
		nodes:   map[string]node.Any{"test/gate": gate.node(t)},
	})
	result := live.start("lifetime-run", "alice", quote.Input{SKU: "coffee", Quantity: 1})
	<-gate.entered
	started := time.Now()
	stream := awaitStream(t, context.Background(), live.eventsURL("lifetime-run"), "alice")
	first := stream.rest(t, 5*time.Second)
	lived := time.Since(started)
	if lived < 350*time.Millisecond || lived > 3*time.Second || first[len(first)-1].Event != "step.log" {
		t.Fatalf("subscription lived %s; frames=%v", lived, frameNames(first))
	}
	if beats := len(stream.Comments()); beats < 3 {
		t.Fatalf("heartbeats=%d", beats)
	}
	waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 0 && live.active.Load() == 0 })
	close(gate.release)
	awaitResult(t, result)
	_, _, resumed := openStream(t, context.Background(), live.eventsURL("lifetime-run"), "alice", first[len(first)-1].ID)
	rest := resumed.rest(t, 5*time.Second)
	if got := strings.Join(stepNames(t, append(first, rest...)), ","); got != "run.started,step.processing:gate,step.log:gate,step.completed:gate,step.processing:calculate,step.completed:calculate,step.processing:respond,step.completed:respond,run.completed,end" {
		t.Fatalf("frames across the lifetime boundary=%s", got)
	}
}
