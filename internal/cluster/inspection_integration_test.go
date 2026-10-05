package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/engine"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
)

// inspectionFrame is one parsed Server-Sent Event of the inspection stream.
type inspectionFrame struct{ ID, Event, Data string }

type inspectionStream struct {
	frames chan inspectionFrame
	body   io.Closer
}

func (s *inspectionStream) next(t *testing.T, timeout time.Duration) (inspectionFrame, bool) {
	t.Helper()
	select {
	case frame, ok := <-s.frames:
		return frame, ok
	case <-time.After(timeout):
		t.Fatalf("no inspection frame within %s", timeout)
		return inspectionFrame{}, false
	}
}

// rest reads frames until the server ends the stream.
func (s *inspectionStream) rest(t *testing.T, timeout time.Duration) []inspectionFrame {
	t.Helper()
	var out []inspectionFrame
	deadline := time.Now().Add(timeout)
	for {
		frame, ok := s.next(t, time.Until(deadline))
		if !ok {
			return out
		}
		out = append(out, frame)
	}
}

func inspectionNames(frames []inspectionFrame) string {
	names := make([]string, len(frames))
	for i, frame := range frames {
		names[i] = frame.Event
	}
	return strings.Join(names, ",")
}

// openInspection issues GET as principal. A non-200 status is returned with
// no stream.
func openInspection(t *testing.T, ctx context.Context, url, principal string) (int, *inspectionStream) {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if principal != "" {
		request.Header.Set("X-Principal", principal)
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
		return response.StatusCode, nil
	}
	stream := &inspectionStream{frames: make(chan inspectionFrame, 1024), body: response.Body}
	go func() {
		defer close(stream.frames)
		scanner := bufio.NewScanner(response.Body)
		scanner.Buffer(make([]byte, 64<<10), 4<<20)
		var current inspectionFrame
		for scanner.Scan() {
			line := scanner.Text()
			switch {
			case line == "":
				if current.Event != "" {
					stream.frames <- current
				}
				current = inspectionFrame{}
			case strings.HasPrefix(line, "id: "):
				current.ID = strings.TrimPrefix(line, "id: ")
			case strings.HasPrefix(line, "event: "):
				current.Event = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				current.Data = strings.TrimPrefix(line, "data: ")
			}
		}
	}()
	t.Cleanup(func() { _ = response.Body.Close() })
	return http.StatusOK, stream
}

func inspectionPage(t *testing.T, frame inspectionFrame) inspection.Page {
	t.Helper()
	if frame.Event != "snapshot" {
		t.Fatalf("frame %q is not a snapshot: %s", frame.Event, frame.Data)
	}
	var value struct {
		Page inspection.Page `json:"page"`
	}
	if err := json.Unmarshal([]byte(frame.Data), &value); err != nil {
		t.Fatalf("snapshot %s: %v", frame.Data, err)
	}
	return value.Page
}

// stepStates renders a page's steps as id=status[/attempt] in order.
func stepStates(page inspection.Page) string {
	parts := make([]string, len(page.Steps))
	for i, step := range page.Steps {
		parts[i] = step.ID + "=" + string(step.Status)
	}
	return strings.Join(parts, ",")
}

// awaitSnapshot reads frames until a snapshot whose run has status.
func awaitSnapshot(t *testing.T, stream *inspectionStream, status inspection.Status, timeout time.Duration) inspection.Page {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		frame, ok := stream.next(t, time.Until(deadline))
		if !ok {
			t.Fatalf("stream ended before a %s snapshot", status)
		}
		if frame.Event != "snapshot" {
			t.Fatalf("unexpected frame %s %s before a %s snapshot", frame.Event, frame.Data, status)
		}
		if page := inspectionPage(t, frame); page.Run.Status == status {
			return page
		}
	}
}

func inspectionHandler(t *testing.T, config inspect.EventHandlerConfig, capture inspect.Capture) (*inspect.EventStream, string) {
	t.Helper()
	stream, err := inspect.NewEventStream(inspect.EventStreamConfig{Capture: capture})
	if err != nil {
		t.Fatal(err)
	}
	config.Authenticate = func(request *http.Request) (string, error) {
		if principal := request.Header.Get("X-Principal"); principal != "" {
			return principal, nil
		}
		return "", errors.New("unauthenticated")
	}
	if config.Policy == nil {
		config.Policy = func(string) inspection.Policy {
			return inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true, inspection.FieldOutput: true, inspection.FieldError: true, inspection.FieldLogs: true}}
		}
	}
	handler, err := inspect.NewEventHandler(stream, config)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(func() {
		stream.Hub().Close()
		server.Close()
	})
	return stream, server.URL
}

// TestClusterRunIsReconstructedThroughTheInspectionStream is #263: a run
// executed by the durable cluster runtime (RunJournaled over
// store/distributed) used to be invisible to the inspection stream (404).
// Through the store/distributed source its owner, the tenant its run record
// names, follows it: the step journal's transitions appear as
// reconstructions while the run is in flight, suspended at a wait and
// resumed by a signal, and the stream ends once the durable run completes,
// long before MaxDuration. Another tenant, even in the same partition, gets
// 404. Inspection never invokes a node.
func TestClusterRunIsReconstructedThroughTheInspectionStream(t *testing.T) {
	store := integrationDistributedStore(t)
	var firstCalls, secondCalls atomic.Int64
	release, entered := make(chan struct{}), make(chan struct{}, 1)
	first := node.MustDefine("fixture/inspect-first", "1.0.0", func(ctx context.Context, input waitInput) (integrationOutput, error) {
		firstCalls.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
			return integrationOutput{}, ctx.Err()
		}
		return integrationOutput{Value: input.Value + 1}, nil
	}, node.Description("held pure step"), node.Schemas([]byte(waitInputSchema), []byte(outputSchema))).Any()
	second := node.MustDefine("fixture/inspect-second", "1.0.0", func(_ context.Context, input engine.WaitResult) (integrationOutput, error) {
		secondCalls.Add(1)
		return integrationOutput{Value: 99}, nil
	}, node.Description("counted effect after the wait"), node.Schemas([]byte(waitConsumerSchema), []byte(outputSchema)), node.Effects("fixture:inspect")).Any()
	runtime := newWaitIntegrationRuntime(t, store, "inspected", []contract.InternalInstruction{
		{Index: 0, ID: "first", Kind: "call", Node: "fixture/inspect-first"},
		{Index: 1, ID: "approval", Kind: "wait", Wait: &contract.WaitInstruction{Name: "approval"}},
		{Index: 2, ID: "second", Kind: "call", Node: "fixture/inspect-second", References: []contract.Reference{{Step: "approval"}}},
		{Index: 3, ID: "output", Kind: "output", References: []contract.Reference{{Step: "second"}}},
	}, map[string]node.Any{"fixture/inspect-first": first, "fixture/inspect-second": second})
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tenant, partition := tenantForEmptyPartition(t, ctx, store, runtime, "inspect-tenant")
	neighbour := tenantsInPartition(runtime, partition, tenant+"-neighbour", 1)[0]
	admission, err := runtime.Admit(ctx, Submission{Tenant: tenant, RequestKey: "inspected", Workflow: "inspected", Input: json.RawMessage(`{"value":40}`)})
	if err != nil {
		t.Fatal(err)
	}
	owner := acquireWhenFree(t, ctx, store, partition, "inspection-owner", 5*time.Second)
	processed := make(chan error, 1)
	go func() { _, err := runtime.processOne(ctx, owner); processed <- err }()
	select {
	case <-entered:
	case err := <-processed:
		t.Fatalf("run ended before its held step: %v", err)
	case <-ctx.Done():
		t.Fatal("run did not reach its held step")
	}

	hub, base := inspectionHandler(t, inspect.EventHandlerConfig{Source: runtime.InspectionSource(nil), RecoveredPoll: 100 * time.Millisecond}, inspect.Capture{Outputs: true})
	url := base + "/runs/" + admission.RunID + "/events"
	for _, reader := range []string{neighbour, "inspect-stranger"} {
		if status, _ := openInspection(t, ctx, url, reader); status != http.StatusNotFound {
			t.Fatalf("%s got %d for another tenant's run", reader, status)
		}
	}
	// The source itself refuses them, before any hub authorization.
	source := runtime.InspectionSource(nil)
	for _, reader := range []string{neighbour, "inspect-stranger"} {
		if _, _, _, err := source.ReadInspection(ctx, reader, admission.RunID, "", 0, 20, nil, 1024); !errors.Is(err, ErrNotFound) {
			t.Fatalf("source read for %s: %v", reader, err)
		}
		if owner, err := source.RunOwner(ctx, reader, admission.RunID); !errors.Is(err, ErrNotFound) {
			t.Fatalf("source owner for %s: %q %v", reader, owner, err)
		}
	}
	if status, _ := openInspection(t, ctx, url, ""); status != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d", status)
	}
	status, stream := openInspection(t, ctx, url, tenant)
	if status != http.StatusOK {
		t.Fatalf("the owner got %d for its cluster run", status)
	}
	if gap, _ := stream.next(t, 5*time.Second); gap.Event != event.GapName || !strings.Contains(gap.Data, event.GapUnavailable) {
		t.Fatalf("first frame=%+v", gap)
	}
	// In flight: the step journal holds the dispatch of "first".
	frame, _ := stream.next(t, 5*time.Second)
	inFlight := inspectionPage(t, frame)
	if inFlight.Run.Status != inspection.StatusRunning || stepStates(inFlight) != "first=running" || inFlight.Steps[0].Attempt != 1 || len(inFlight.Steps[0].Attempts) != 1 || inFlight.Steps[0].Attempts[0].ID == "" {
		t.Fatalf("in-flight reconstruction=%+v", inFlight)
	}
	close(release)
	if err := <-processed; !errors.Is(err, ErrNoWork) {
		t.Fatalf("process to the wait: %v", err)
	}
	suspended := awaitSnapshot(t, stream, inspection.StatusSuspended, 10*time.Second)
	if stepStates(suspended) != "first=completed,approval=suspended" || string(suspended.Steps[0].Output) != `{"value":41}` {
		t.Fatalf("suspended reconstruction=%+v", suspended)
	}
	waitID := WaitIDFor(admission.RunID, "approval")
	if result, err := runtime.DeliverSignal(ctx, tenant, waitID, "inspect-signal", "inspect-approver", json.RawMessage(`{"ok":true}`), true); err != nil || !result.Accepted {
		t.Fatalf("signal=%+v err=%v", result, err)
	}
	finished, err := runtime.processOne(ctx, owner)
	if err != nil || finished.State != "completed" {
		t.Fatalf("resumed run=%+v err=%v", finished, err)
	}
	completedAt := time.Now()
	completed := awaitSnapshot(t, stream, inspection.StatusCompleted, 10*time.Second)
	if stepStates(completed) != "first=completed,approval=completed,second=completed" || string(completed.Run.Output) != `{"value":99}` || string(completed.Steps[2].Output) != `{"value":99}` {
		t.Fatalf("completed reconstruction=%+v", completed)
	}
	if rest := stream.rest(t, 10*time.Second); inspectionNames(rest) != "end" {
		t.Fatalf("frames after completion=%s", inspectionNames(rest))
	}
	t.Logf("end %s after the cluster run completed (MaxDuration %s)", time.Since(completedAt), inspect.DefaultEventMaxDuration)
	deadline := time.Now().Add(5 * time.Second)
	for stats := hub.Hub().Stats(); stats.Readers != 0 || stats.Subscribers != 0; stats = hub.Hub().Stats() {
		if time.Now().After(deadline) {
			t.Fatalf("admission not released: %+v", stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("inspection invoked nodes: first=%d second=%d", firstCalls.Load(), secondCalls.Load())
	}

	// A reader the application lets see the tenant's runs is not their
	// owner: the run is attached under the tenant, whom it still admits.
	// Capture is off here, so no payload reaches either reader.
	allowOps := func(reader string) []string {
		if reader == "inspect-ops" {
			return []string{tenant}
		}
		return []string{reader}
	}
	authorize := func(reader, runOwner string) error {
		if reader == runOwner || reader == "inspect-ops" && runOwner == tenant {
			return nil
		}
		return event.ErrUnauthorized
	}
	_, opsBase := inspectionHandler(t, inspect.EventHandlerConfig{Source: runtime.InspectionSource(allowOps), Authorize: authorize}, inspect.Capture{})
	opsURL := opsBase + "/runs/" + admission.RunID + "/events"
	for _, reader := range []string{"inspect-ops", tenant} {
		status, stream := openInspection(t, ctx, opsURL, reader)
		if status != http.StatusOK {
			t.Fatalf("%s got %d after inspect-ops attached the run", reader, status)
		}
		frames := stream.rest(t, 10*time.Second)
		if inspectionNames(frames) != "gap,snapshot,end" {
			t.Fatalf("%s frames=%s", reader, inspectionNames(frames))
		}
		page := inspectionPage(t, frames[1])
		if stepStates(page) != "first=completed,approval=completed,second=completed" || len(page.Run.Output) != 0 || strings.Contains(frames[1].Data, `"output"`) || !strings.Contains(frames[1].Data, unavailableTimestamps) {
			t.Fatalf("%s reconstruction=%s", reader, frames[1].Data)
		}
	}
	if status, _ := openInspection(t, ctx, opsURL, neighbour); status != http.StatusNotFound {
		t.Fatalf("neighbour got %d through the ops source", status)
	}
}
