package inspect_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/app"
	"github.com/well-prado/new-blok/contract"
	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/examples/quote"
	"github.com/well-prado/new-blok/execution"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/node"
	"github.com/well-prado/new-blok/observe/event"
	"github.com/well-prado/new-blok/store/sqlite"
)

// journaledNode is a synthetic native node whose effect is recorded in the
// real journal: intent, attempt, then commit. With block set it stops after
// starting its attempt, the way a process dies mid-effect.
func journaledNode(t testing.TB, store *journal.Journal, name, step, digest string, runID func() string, before <-chan struct{}, logs int, pace time.Duration, block bool) node.Any {
	definition, err := node.Define(name, "1.0.0", func(ctx context.Context, input quote.Input) (quote.Input, error) {
		if before != nil {
			select {
			case <-before:
			case <-ctx.Done():
				return quote.Input{}, ctx.Err()
			}
		}
		operation, err := store.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: runID(), ArtifactDigest: digest, InvocationPath: step, IterationPath: "root"}, Input: []byte(`{"sku":"` + input.SKU + `"}`)})
		if err != nil {
			return quote.Input{}, err
		}
		attempt, err := store.StartAttempt(ctx, operation.Key)
		if err != nil {
			return quote.Input{}, err
		}
		logger := node.Logger(ctx)
		for i := 0; i < logs; i++ {
			logger.Info("flood", "n", i, "a", strings.Repeat("a", 250), "b", strings.Repeat("b", 250), "c", strings.Repeat("c", 250), "d", strings.Repeat("d", 250))
			if pace > 0 {
				time.Sleep(pace)
			}
		}
		if block {
			<-ctx.Done()
			select {}
		}
		if err := store.CommitEffect(ctx, journal.EffectCommit{OperationKey: operation.Key, AttemptID: attempt.ID, Result: []byte(`{"ok":true}`)}); err != nil {
			return quote.Input{}, err
		}
		return input, nil
	}, node.Description("Synthetic journaled effect"), node.Schemas([]byte(`{"type":"object"}`), []byte(`{"type":"object"}`)))
	if err != nil {
		t.Fatal(err)
	}
	return definition.Any()
}

func openJournal(t testing.TB, path string) (*journal.Journal, func()) {
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	store, err := journal.New(context.Background(), database, journal.Config{})
	if err != nil {
		t.Fatal(err)
	}
	return store, func() { _ = database.Close() }
}

// stalledClient opens a subscription and never reads its response, with a
// tiny receive buffer, like a frozen browser tab on a slow link.
func stalledClient(t *testing.T, address, path, principal string) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", address)
	if err != nil {
		t.Fatal(err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetReadBuffer(1024)
	}
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: %s\r\nX-Principal: %s\r\nAccept: text/event-stream\r\n\r\n", path, address, principal); err != nil {
		t.Fatal(err)
	}
	return conn
}

// TestStalledSubscribersCannotBlockRunOrJournalTransitions attaches readers
// that never read to a real HTTP run whose node floods logs while it moves
// a journaled effect through intent, attempt and commit, and whose terminal
// outcome is persisted through the journal port. The run and every journal
// transition complete in about the time they take without readers; the
// readers are disconnected as slow, and their handlers exit.
func TestStalledSubscribersCannotBlockRunOrJournalTransitions(t *testing.T) {
	store, closeJournal := openJournal(t, filepath.Join(t.TempDir(), "stalled.db"))
	defer closeJournal()
	const logs, stalled = 2000, 4
	measure := func(name string, readers int) time.Duration {
		admission, err := store.Admit(context.Background(), journal.AdmissionRequest{Principal: "alice", RequestKey: name, Workflow: "quote", ArtifactDigest: "sha256:" + name, Input: []byte(`{"sku":"coffee","quantity":1}`)})
		if err != nil {
			t.Fatal(err)
		}
		begin := make(chan struct{})
		// Paced so a reader that is draining keeps up: the stalled
		// readers' handlers then fill their sockets and block in a write
		// before the hub cuts them off.
		flood := journaledNode(t, store, "test/gate", "flood", "sha256:"+name, func() string { return admission.RunID }, begin, logs, 100*time.Microsecond, false)
		live := newLiveApp(t, liveConfig{
			stream:   inspect.EventStreamConfig{Capture: inspect.Capture{Logs: true}, Hub: event.Config{QueueDepth: 4, LateWindow: 10 * time.Millisecond}},
			handler:  inspect.EventHandlerConfig{WriteTimeout: time.Minute},
			outcomes: store.TerminalOutcomes(),
			nodes:    map[string]node.Any{"test/gate": flood},
		})
		result := live.start(admission.RunID, "alice", quote.Input{SKU: "coffee", Quantity: 1})
		probe := awaitStream(t, context.Background(), live.eventsURL(admission.RunID), "alice")
		probe.close()
		waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 0 })
		address := strings.TrimPrefix(live.server.URL, "http://")
		conns := make([]net.Conn, 0, readers)
		for i := 0; i < readers; i++ {
			conns = append(conns, stalledClient(t, address, "/inspect/runs/"+admission.RunID+"/events", "alice"))
		}
		waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == readers })
		started := time.Now()
		close(begin)
		got := awaitResult(t, result)
		elapsed := time.Since(started)
		if got[0] != "200 OK" {
			t.Fatalf("%s run=%v", name, got)
		}
		run, err := store.Run(context.Background(), admission.RunID)
		if err != nil || run.State != "completed" {
			t.Fatalf("%s journal run=%+v err=%v", name, run, err)
		}
		operation, err := store.Operation(context.Background(), journal.OperationIdentity{RunID: admission.RunID, ArtifactDigest: "sha256:" + name, InvocationPath: "flood", IterationPath: "root"}.Key())
		if err != nil || operation.State != "committed" {
			t.Fatalf("%s journal operation=%+v err=%v", name, operation, err)
		}
		if readers > 0 {
			// Every stalled reader was cut off; none was waited for. Its
			// handler, blocked writing to a socket nobody drains, is
			// interrupted at once rather than at its one-minute write
			// timeout, although the connections are still open.
			cut := time.Now()
			waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 0 && live.active.Load() == 0 })
			if stats := live.stream.Hub().Stats(); stats.SlowSubscribers != uint64(readers) {
				t.Fatalf("%s stats=%+v", name, stats)
			}
			t.Logf("%s: stalled handlers exited %s after the run", name, time.Since(cut))
		}
		for _, conn := range conns {
			_ = conn.Close()
		}
		t.Logf("%s: %d stalled readers, %d logs: run %s", name, readers, logs, elapsed)
		return elapsed
	}
	baseline := measure("no-readers", 0)
	withStalled := measure("stalled-readers", stalled)
	// The bound is generous on purpose: this asserts "not blocked" (a
	// blocked publisher would wait for the minute-long write timeout), not a
	// performance figure. Raw durations are logged above.
	if withStalled > baseline+5*time.Second {
		t.Fatalf("stalled readers slowed the run: baseline %s, with %d stalled readers %s", baseline, stalled, withStalled)
	}
}

// TestCrashRecoveryReconstructsRunFromJournal kills a real process in the
// middle of an effect while a reader follows it, then serves the same
// journal from a fresh process. The reader's old cursor is reported as a
// restart gap followed by a snapshot reconstructed from committed journal
// facts; the reconstruction matches what the reader saw before the crash.
func TestCrashRecoveryReconstructsRunFromJournal(t *testing.T) {
	if os.Getenv("NEWBLOK_EVENTS_CRASH_CHILD") == "1" {
		runEventsCrashChild(t)
		return
	}
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "crash.db")
	markerPath := filepath.Join(directory, "marker")
	command := exec.Command(os.Args[0], "-test.run=^TestCrashRecoveryReconstructsRunFromJournal$", "-test.v")
	command.Env = append(os.Environ(), "NEWBLOK_EVENTS_CRASH_CHILD=1", "NEWBLOK_EVENTS_CRASH_DB="+databasePath, "NEWBLOK_EVENTS_CRASH_MARKER="+markerPath)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	killed := false
	defer func() {
		if !killed {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	var marker []string
	waitFor(t, func() bool {
		data, err := os.ReadFile(markerPath)
		marker = strings.Fields(string(data))
		return err == nil && len(marker) == 2
	})
	address, runID := marker[0], marker[1]
	ctx, cancel := context.WithCancel(context.Background())
	stream := awaitStream(t, ctx, "http://"+address+"/inspect/runs/"+runID+"/events", "alice")
	var before []sseFrame
	for {
		frame, ok := stream.next(t, 10*time.Second)
		if !ok {
			t.Fatalf("child stream ended: %v", frameNames(before))
		}
		before = append(before, frame)
		if frame.Event == "step.processing" && frame.decode(t).StepID == "charge" {
			break
		}
	}
	if got := strings.Join(stepNames(t, before), ","); got != "run.started,step.processing:reserve,step.completed:reserve,step.processing:charge" {
		t.Fatalf("pre-crash frames=%s", got)
	}
	cursor := before[len(before)-1].ID
	// The crash: no shutdown, no flush, no terminal outcome.
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	killed = true
	cancel()
	stream.close()

	store, closeJournal := openJournal(t, databasePath)
	defer closeJournal()
	recovered, err := inspect.NewEventStream(inspect.EventStreamConfig{Capture: fullCapture})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := inspect.NewEventHandler(recovered, inspect.EventHandlerConfig{Authenticate: principalFromHeader, Policy: func(string) inspection.Policy { return allFields() }, Source: store})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.StripPrefix("/inspect", handler))
	defer server.Close()
	defer recovered.Hub().Close()
	url := server.URL + "/inspect/runs/" + runID + "/events"
	if response, body, _ := openStream(t, context.Background(), url, "mallory", cursor); response.StatusCode != http.StatusNotFound {
		t.Fatalf("another principal reached the recovered run: %d %s", response.StatusCode, body)
	}
	for _, item := range []struct{ cursor, reason string }{{cursor, event.GapRestart}, {"", event.GapUnavailable}} {
		readCtx, stop := context.WithCancel(context.Background())
		_, _, resumed := openStream(t, readCtx, url, "alice", item.cursor)
		gap, _ := resumed.next(t, 5*time.Second)
		snapshotFrame, _ := resumed.next(t, 5*time.Second)
		stop()
		resumed.close()
		if gap.Event != event.GapName || !strings.Contains(gap.Data, `"reason":"`+item.reason+`"`) {
			t.Fatalf("cursor %q: first frame=%+v", item.cursor, gap)
		}
		var snapshot struct {
			Source        string          `json:"source"`
			Reconstructed bool            `json:"reconstructed"`
			Page          inspection.Page `json:"page"`
			Unavailable   []string        `json:"unavailable"`
		}
		if snapshotFrame.Event != "snapshot" || json.Unmarshal([]byte(snapshotFrame.Data), &snapshot) != nil || snapshot.Source != "journal" || !snapshot.Reconstructed || len(snapshot.Unavailable) == 0 {
			t.Fatalf("snapshot frame=%+v", snapshotFrame)
		}
		page := snapshot.Page
		statuses := map[string]inspection.Status{}
		for _, step := range page.Steps {
			statuses[strings.TrimSuffix(step.ID, "[root]")] = step.Status
		}
		// The reconstruction agrees with the reader's pre-crash view: the
		// run is running, reserve committed, charge was in flight.
		if page.Run.ID != runID || page.Run.Status != inspection.StatusRunning || statuses["reserve"] != inspection.StatusCompleted || statuses["charge"] != inspection.StatusRunning || len(page.Steps) != 2 {
			t.Fatalf("reconstructed page=%+v", page)
		}
		if snapshotFrame.ID != gap.ID {
			t.Fatalf("snapshot id=%q gap id=%q", snapshotFrame.ID, gap.ID)
		}
	}
	waitFor(t, func() bool { return recovered.Hub().Stats().Subscribers == 0 })
}

func runEventsCrashChild(t *testing.T) {
	ctx := context.Background()
	store, _ := openJournal(t, os.Getenv("NEWBLOK_EVENTS_CRASH_DB"))
	admission, err := store.Admit(ctx, journal.AdmissionRequest{Principal: "alice", RequestKey: "crash", Workflow: "checkout", ArtifactDigest: "sha256:crash", Input: []byte(`{"sku":"coffee","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	runID := func() string { return admission.RunID }
	stream, err := inspect.NewEventStream(inspect.EventStreamConfig{Capture: fullCapture})
	if err != nil {
		t.Fatal(err)
	}
	application, err := app.New(app.Config{Workflows: []app.Workflow{{Name: "checkout"}}, Inspection: stream, RunOutcomes: store.TerminalOutcomes()})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	runner := execution.NewRunner(application, map[string]node.Any{
		"test/reserve": journaledNode(t, store, "test/reserve", "reserve", "sha256:crash", runID, nil, 0, 0, false),
		"test/charge":  journaledNode(t, store, "test/charge", "charge", "sha256:crash", runID, nil, 0, 0, true),
	})
	program := contract.InternalProgram{WorkflowID: "checkout", Instructions: []contract.InternalInstruction{
		{Index: 0, ID: "reserve", Kind: "call", Node: "test/reserve"},
		{Index: 1, ID: "charge", Kind: "call", Node: "test/charge"},
	}}
	handler, err := inspect.NewEventHandler(stream, inspect.EventHandlerConfig{Authenticate: principalFromHeader, Policy: func(string) inspection.Policy { return allFields() }, Source: store})
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = http.Serve(listener, http.StripPrefix("/inspect", handler)) }()
	go func() {
		_, _ = runner.Run(ctx, program, quote.Input{SKU: "coffee", Quantity: 1}, inspection.Invocation{RunID: admission.RunID, Principal: "alice"})
	}()
	marker := os.Getenv("NEWBLOK_EVENTS_CRASH_MARKER")
	if err := os.WriteFile(marker+".tmp", []byte(listener.Addr().String()+" "+admission.RunID), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(marker+".tmp", marker); err != nil {
		t.Fatal(err)
	}
	select {}
}
