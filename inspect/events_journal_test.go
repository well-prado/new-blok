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
// real journal: intent, attempt, then commit. With block set it reports the
// committed attempt to block and stops there, the way a process dies
// mid-effect.
func journaledNode(t testing.TB, store *journal.Journal, name, step, digest string, runID func() string, before <-chan struct{}, logs int, pace time.Duration, block func()) node.Any {
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
		if block != nil {
			block()
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
// transition complete although the readers never drain; the readers are
// disconnected as slow, and their handlers exit.
//
// What it guards, and what it does not (#310): a publisher that blocks on a
// stalled reader for up to the reader's write timeout (MaxEventWriteTimeout,
// one minute), or for ever. The run must end before any stalled write could
// have timed out, and every reader must have been cut off by the hub as
// slow, not released by its write timeout. A brief, bounded wait per publish
// is not detected: by time it cannot be told apart from load (the run's own
// duration varied from 3 s to 25 s under load), so the test no longer times
// the run against a baseline run. One non-timing check remains: an
// in-process reader that never takes a frame is attached too, and the run
// must publish past its whole queue while it has consumed nothing.
func TestStalledSubscribersCannotBlockRunOrJournalTransitions(t *testing.T) {
	store, closeJournal := openJournal(t, filepath.Join(t.TempDir(), "stalled.db"))
	defer closeJournal()
	const logs, readers, queueDepth = 2000, 4, 4
	const writeTimeout = inspect.MaxEventWriteTimeout
	const name = "stalled-readers"
	admission, err := store.Admit(context.Background(), journal.AdmissionRequest{Principal: "alice", RequestKey: name, Workflow: "quote", ArtifactDigest: "sha256:" + name, Input: []byte(`{"sku":"coffee","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	begin := make(chan struct{})
	// Paced so a reader that is draining keeps up: the stalled readers'
	// handlers then fill their sockets and block in a write before the hub
	// cuts them off.
	flood := journaledNode(t, store, "test/gate", "flood", "sha256:"+name, func() string { return admission.RunID }, begin, logs, 100*time.Microsecond, nil)
	live := newLiveApp(t, liveConfig{
		stream:   inspect.EventStreamConfig{Capture: inspect.Capture{Logs: true}, Hub: event.Config{QueueDepth: queueDepth, LateWindow: 10 * time.Millisecond}},
		handler:  inspect.EventHandlerConfig{WriteTimeout: writeTimeout},
		outcomes: store.TerminalOutcomes(),
		nodes:    map[string]node.Any{"test/gate": flood},
	})
	result := live.start(admission.RunID, "alice", quote.Input{SKU: "coffee", Quantity: 1})
	probe := awaitStream(t, context.Background(), live.eventsURL(admission.RunID), "alice")
	probe.close()
	waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 0 })
	address := strings.TrimPrefix(live.server.URL, "http://")
	for i := 0; i < readers; i++ {
		conn := stalledClient(t, address, "/inspect/runs/"+admission.RunID+"/events", "alice")
		defer conn.Close()
	}
	quiet, err := live.stream.Hub().Subscribe(admission.RunID, "alice", "", nil)
	if err != nil || quiet.Subscriber == nil {
		t.Fatalf("quiet reader=%+v err=%v", quiet, err)
	}
	defer live.stream.Hub().Unsubscribe(quiet.Subscriber, "test_done")
	waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == readers+1 })
	before := live.stream.Hub().Stats().Published
	started := time.Now()
	close(begin)
	var got [2]string
	select {
	case got = <-result:
	case <-time.After(2 * writeTimeout):
		t.Fatalf("the run did not finish while %d readers stalled: it waited for them", readers)
	}
	elapsed := time.Since(started)
	t.Logf("%d stalled readers, %d logs: run %s", readers, logs, elapsed)
	if elapsed >= writeTimeout {
		t.Fatalf("the run took %s, as long as a stalled reader's write timeout %s: it waited for one", elapsed, writeTimeout)
	}
	if got[0] != "200 OK" {
		t.Fatalf("run=%v", got)
	}
	run, err := store.Run(context.Background(), admission.RunID)
	if err != nil || run.State != "completed" {
		t.Fatalf("journal run=%+v err=%v", run, err)
	}
	operation, err := store.Operation(context.Background(), journal.OperationIdentity{RunID: admission.RunID, ArtifactDigest: "sha256:" + name, InvocationPath: "flood", IterationPath: "root"}.Key())
	if err != nil || operation.State != "committed" {
		t.Fatalf("journal operation=%+v err=%v", operation, err)
	}
	// The quiet reader consumed nothing, yet the run published past its
	// whole queue: those publications did not wait for it. It was cut off
	// as slow, not left buffering.
	select {
	case <-quiet.Subscriber.Done():
	default:
		t.Fatal("the quiet reader, which consumed nothing, was neither waited for nor cut off")
	}
	if reason := quiet.Subscriber.Reason(); reason != "slow_subscriber" {
		t.Fatalf("quiet reader reason=%q", reason)
	}
	if published := live.stream.Hub().Stats().Published - before; published <= queueDepth {
		t.Fatalf("the run published %d frames after the quiet reader attached, not past its queue of %d", published, queueDepth)
	}
	// Every stalled reader was cut off; none was waited for. Its handler,
	// blocked writing to a socket nobody drains, is interrupted at once
	// rather than at its write timeout, although the connections
	// are still open.
	cut := time.Now()
	waitFor(t, func() bool { return live.stream.Hub().Stats().Subscribers == 0 && live.active.Load() == 0 })
	if stats := live.stream.Hub().Stats(); stats.SlowSubscribers != readers+1 {
		t.Fatalf("stats=%+v", stats)
	}
	t.Logf("stalled handlers exited %s after the run", time.Since(cut))
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
	// The stream's step.processing precedes the node's own journal write;
	// crash only once the charge attempt is committed, so the journal holds
	// the in-flight effect.
	waitFor(t, func() bool { _, err := os.Stat(markerPath + ".attempt"); return err == nil })
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
		"test/reserve": journaledNode(t, store, "test/reserve", "reserve", "sha256:crash", runID, nil, 0, 0, nil),
		"test/charge": journaledNode(t, store, "test/charge", "charge", "sha256:crash", runID, nil, 0, 0, func() {
			// The charge attempt is committed; the process may now die.
			_ = os.WriteFile(os.Getenv("NEWBLOK_EVENTS_CRASH_MARKER")+".attempt", nil, 0o600)
		}),
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
