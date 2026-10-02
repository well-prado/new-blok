package journal

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

func TestConcurrentAdmissionDeduplicatesAndRejectsConflicts(t *testing.T) {
	database, journal := newJournal(t, "journal.db", Config{})
	defer database.Close()
	request := AdmissionRequest{RequestKey: "order-1", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":2}`)}
	const callers = 32
	results := make(chan Admission, callers)
	errorsSeen := make(chan error, callers)
	start := make(chan struct{})
	var group sync.WaitGroup
	for index := 0; index < callers; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			<-start
			admission, err := journal.Admit(context.Background(), request)
			if err != nil {
				errorsSeen <- err
				return
			}
			results <- admission
		}()
	}
	close(start)
	group.Wait()
	close(results)
	close(errorsSeen)
	var accepted int
	var runID string
	for admission := range results {
		if admission.Accepted {
			accepted++
		}
		if runID == "" {
			runID = admission.RunID
		}
		if admission.RunID != runID {
			t.Fatalf("duplicate admission run IDs: %q and %q", runID, admission.RunID)
		}
	}
	for err := range errorsSeen {
		t.Fatal(err)
	}
	if accepted != 1 {
		t.Fatalf("accepted=%d, want one durable admission", accepted)
	}
	conflict := request
	conflict.Input = []byte(`{"sku":"tea","quantity":2}`)
	if _, err := journal.Admit(context.Background(), conflict); !errors.Is(err, ErrRequestConflict) {
		t.Fatalf("conflict err=%v", err)
	}
}

func TestCommittedEffectReplaysWithoutRedispatchAndRetriesKeepProviderKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	database, journal := newJournalAtPath(t, path, Config{})
	defer database.Close()
	admission, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "order-2", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":2}`)})
	if err != nil {
		t.Fatal(err)
	}
	identity := OperationIdentity{RunID: admission.RunID, ArtifactDigest: "sha256:artifact", InvocationPath: "charge", IterationPath: "root"}
	operation, err := journal.BeginEffect(context.Background(), EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	first, err := journal.StartAttempt(context.Background(), operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.FailAttempt(context.Background(), operation.Key, first.ID, true, "temporary provider timeout"); err != nil {
		t.Fatal(err)
	}
	second, err := journal.StartAttempt(context.Background(), operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID || first.ProviderOperationKey != second.ProviderOperationKey {
		t.Fatalf("attempt identity/provider key mismatch: first=%+v second=%+v", first, second)
	}
	if err := journal.CommitEffect(context.Background(), EffectCommit{OperationKey: operation.Key, AttemptID: first.ID, Result: []byte(`{"charged":true}`)}); !errors.Is(err, ErrStaleAttempt) {
		t.Fatalf("stale commit err=%v", err)
	}
	if err := journal.CommitEffect(context.Background(), EffectCommit{OperationKey: operation.Key, AttemptID: second.ID, Result: []byte(`{"charged":true}`)}); err != nil {
		t.Fatal(err)
	}
	if err := journal.CompleteRun(context.Background(), admission.RunID, []byte(`{"totalCents":3000}`)); err != nil {
		t.Fatal(err)
	}
	database.Close()
	database, journal = newJournalAtPath(t, path, Config{})
	defer database.Close()
	replayed, err := journal.BeginEffect(context.Background(), EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	if replayed.State != operationCommitted || string(replayed.Result) != `{"charged":true}` {
		t.Fatalf("replayed operation=%+v", replayed)
	}
	if _, err := journal.StartAttempt(context.Background(), operation.Key); !errors.Is(err, ErrNotDispatchable) {
		t.Fatalf("committed operation was dispatchable: %v", err)
	}
}

func TestUncertainOutcomeFailsClosedAndReplayGetsFreshLineage(t *testing.T) {
	database, journal := newJournal(t, "journal.db", Config{})
	defer database.Close()
	admission, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "order-3", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	identity := OperationIdentity{RunID: admission.RunID, ArtifactDigest: "sha256:artifact", InvocationPath: "charge", IterationPath: "root"}
	operation, err := journal.BeginEffect(context.Background(), EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	attempt, err := journal.StartAttempt(context.Background(), operation.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := journal.MarkUncertain(context.Background(), operation.Key, attempt.ID, "provider succeeded; local commit outcome unknown"); err != nil {
		t.Fatal(err)
	}
	uncertain, err := journal.BeginEffect(context.Background(), EffectIntent{Identity: identity})
	if err != nil {
		t.Fatal(err)
	}
	if uncertain.State != operationUncertain {
		t.Fatalf("uncertain state=%q", uncertain.State)
	}
	if _, err := journal.StartAttempt(context.Background(), operation.Key); !errors.Is(err, ErrUncertain) {
		t.Fatalf("uncertain effect was redispatched: %v", err)
	}
	replay, err := journal.Replay(context.Background(), admission.RunID, "order-3-replay")
	if err != nil {
		t.Fatal(err)
	}
	if replay.RunID == admission.RunID || replay.ReplayOf != admission.RunID {
		t.Fatalf("replay lineage=%+v original=%+v", replay, admission)
	}
	replayIdentity := identity
	replayIdentity.RunID = replay.RunID
	if replayIdentity.Key() == identity.Key() {
		t.Fatal("replay reused operation identity")
	}
}

func TestAdmissionCommitBarrierSurvivesOrRollsBackProcessKill(t *testing.T) {
	if os.Getenv("NEWBLOK_JOURNAL_CHILD") == "1" {
		runAdmissionChild()
		return
	}
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "journal.db")
			markerPath := filepath.Join(directory, "marker")
			command := exec.Command(os.Args[0], "-test.run=TestAdmissionCommitBarrierSurvivesOrRollsBackProcessKill", "-test.v")
			command.Env = append(os.Environ(), "NEWBLOK_JOURNAL_CHILD=1", "NEWBLOK_JOURNAL_PHASE="+phase, "NEWBLOK_JOURNAL_PATH="+databasePath, "NEWBLOK_JOURNAL_MARKER="+markerPath)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForJournalMarker(t, markerPath)
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()
			database, journal := newJournalAtPath(t, databasePath, Config{})
			defer database.Close()
			admission, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "barrier-order", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":1}`)})
			if err != nil {
				t.Fatal(err)
			}
			if phase == "before" && !admission.Accepted {
				t.Fatal("pre-commit kill left an acknowledged admission")
			}
			if phase == "after" && admission.Accepted {
				t.Fatal("post-commit kill lost durable admission")
			}
		})
	}
}

func TestIntentDispatchResultAndCompletionBarriers(t *testing.T) {
	if os.Getenv("NEWBLOK_JOURNAL_TRANSITION_CHILD") == "1" {
		runTransitionChild()
		return
	}
	for _, transition := range []string{"intent", "dispatch", "result", "ack"} {
		for _, phase := range []string{"before", "after"} {
			t.Run(transition+"/"+phase, func(t *testing.T) {
				directory := t.TempDir()
				databasePath := filepath.Join(directory, "journal.db")
				markerPath := filepath.Join(directory, "marker")
				command := exec.Command(os.Args[0], "-test.run=TestIntentDispatchResultAndCompletionBarriers", "-test.v")
				command.Env = append(os.Environ(), "NEWBLOK_JOURNAL_TRANSITION_CHILD=1", "NEWBLOK_JOURNAL_TRANSITION="+transition, "NEWBLOK_JOURNAL_PHASE="+phase, "NEWBLOK_JOURNAL_PATH="+databasePath, "NEWBLOK_JOURNAL_MARKER="+markerPath)
				if err := command.Start(); err != nil {
					t.Fatal(err)
				}
				waitForJournalMarker(t, markerPath)
				if err := command.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				_ = command.Wait()

				database, journal := newJournalAtPath(t, databasePath, Config{})
				defer database.Close()
				request := AdmissionRequest{RequestKey: "barrier-" + transition, Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":1}`)}
				admission, err := journal.Admit(context.Background(), request)
				if err != nil {
					t.Fatal(err)
				}
				identity := OperationIdentity{RunID: admission.RunID, ArtifactDigest: request.ArtifactDigest, InvocationPath: "charge", IterationPath: "root"}
				operationKey := identity.Key()
				switch transition {
				case "intent":
					operation, err := journal.Operation(context.Background(), operationKey)
					if phase == "before" {
						if !errors.Is(err, ErrNotFound) {
							t.Fatalf("pre-intent operation=%+v err=%v", operation, err)
						}
					} else if err != nil || operation.State != operationIntent {
						t.Fatalf("post-intent operation=%+v err=%v", operation, err)
					}
				case "dispatch":
					operation, err := journal.Operation(context.Background(), operationKey)
					want := operationIntent
					if phase == "after" {
						want = operationDispatched
					}
					if err != nil || operation.State != want {
						t.Fatalf("%s dispatch operation=%+v err=%v", phase, operation, err)
					}
				case "result":
					operation, err := journal.Operation(context.Background(), operationKey)
					want := operationDispatched
					if phase == "after" {
						want = operationCommitted
					}
					if err != nil || operation.State != want {
						t.Fatalf("%s result operation=%+v err=%v", phase, operation, err)
					}
				case "ack":
					run, err := journal.Run(context.Background(), admission.RunID)
					want := runAccepted
					if phase == "after" {
						want = runCompleted
					}
					if err != nil || run.State != want {
						t.Fatalf("%s ack run=%+v err=%v", phase, run, err)
					}
				}
			})
		}
	}
}

func runTransitionChild() {
	transition := os.Getenv("NEWBLOK_JOURNAL_TRANSITION")
	phase := os.Getenv("NEWBLOK_JOURNAL_PHASE")
	hooks := Hooks{}
	barrier := func(name string) {
		if name == "effect-intent" && transition == "intent" || name == "attempt-start" && transition == "dispatch" || name == "effect-commit" && transition == "result" || name == "run-complete" && transition == "ack" {
			if phase == "before" {
				journalMarkerAndWait()
			}
		}
	}
	hooks.BeforeCommit = barrier
	hooks.AfterCommit = func(name string) {
		if name == "effect-intent" && transition == "intent" || name == "attempt-start" && transition == "dispatch" || name == "effect-commit" && transition == "result" || name == "run-complete" && transition == "ack" {
			if phase == "after" {
				journalMarkerAndWait()
			}
		}
	}
	database, journal := newJournalAtPathForChild(os.Getenv("NEWBLOK_JOURNAL_PATH"), Config{Hooks: hooks})
	defer database.Close()
	request := AdmissionRequest{RequestKey: "barrier-" + transition, Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":1}`)}
	admission, err := journal.Admit(context.Background(), request)
	if err != nil {
		panic(err)
	}
	identity := OperationIdentity{RunID: admission.RunID, ArtifactDigest: request.ArtifactDigest, InvocationPath: "charge", IterationPath: "root"}
	operation, err := journal.BeginEffect(context.Background(), EffectIntent{Identity: identity})
	if err != nil && transition != "intent" {
		panic(err)
	}
	if transition == "intent" {
		return
	}
	attempt, err := journal.StartAttempt(context.Background(), operation.Key)
	if err != nil {
		panic(err)
	}
	if transition == "dispatch" {
		return
	}
	if transition == "result" {
		if err := journal.CommitEffect(context.Background(), EffectCommit{OperationKey: operation.Key, AttemptID: attempt.ID, Result: []byte(`{"ok":true}`)}); err != nil {
			panic(err)
		}
		return
	}
	if err := journal.CompleteRun(context.Background(), admission.RunID, []byte(`{"ok":true}`)); err != nil {
		panic(err)
	}
}

func runAdmissionChild() {
	database, journal := newJournalAtPathForChild(os.Getenv("NEWBLOK_JOURNAL_PATH"), Config{Hooks: Hooks{
		BeforeCommit: func(name string) {
			if name == "admission" && os.Getenv("NEWBLOK_JOURNAL_PHASE") == "before" {
				journalMarkerAndWait()
			}
		},
		AfterCommit: func(name string) {
			if name == "admission" && os.Getenv("NEWBLOK_JOURNAL_PHASE") == "after" {
				journalMarkerAndWait()
			}
		},
	}})
	defer database.Close()
	if _, err := journal.Admit(context.Background(), AdmissionRequest{RequestKey: "barrier-order", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":1}`)}); err != nil {
		panic(err)
	}
}

func TestIntegrityFailureFailsClosed(t *testing.T) {
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "journal.db")
	database, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, journalErr := New(context.Background(), database, Config{}); journalErr != nil {
		t.Fatal(journalErr)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(databasePath, contents[:len(contents)/2], 0o600); err != nil {
		t.Fatal(err)
	}
	corrupted, err := (sqlite.Backend{}).Open(context.Background(), databasePath)
	if err != nil {
		return
	}
	defer corrupted.Close()
	if _, err := New(context.Background(), corrupted, Config{}); err == nil {
		t.Fatal("corrupted journal opened successfully")
	}
}

func TestWriteFailureDoesNotAcknowledgeAdmission(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	j, err := New(context.Background(), database, Config{})
	if err != nil {
		t.Fatal(err)
	}
	failing := &failingDatabase{Database: database}
	failingJournal, err := New(context.Background(), failing, Config{})
	if err != nil {
		t.Fatal(err)
	}
	failing.fail = true
	request := AdmissionRequest{RequestKey: "write-failure", Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(`{"sku":"coffee","quantity":1}`)}
	if _, err := failingJournal.Admit(context.Background(), request); err == nil {
		t.Fatal("write failure was acknowledged")
	}
	failing.fail = false
	admission, err := j.Admit(context.Background(), request)
	if err != nil || !admission.Accepted {
		t.Fatalf("admission after failed write=%+v err=%v", admission, err)
	}
}

type failingDatabase struct {
	store.Database
	fail bool
}

func (d *failingDatabase) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if d.fail {
		return errors.New("simulated disk-full write failure")
	}
	return d.Database.WithTx(ctx, fn)
}

func newJournal(t *testing.T, name string, config Config) (store.Database, *Journal) {
	t.Helper()
	return newJournalAtPath(t, filepath.Join(t.TempDir(), name), config)
}

func newJournalAtPath(t *testing.T, path string, config Config) (store.Database, *Journal) {
	t.Helper()
	database, journal, err := openJournal(path, config)
	if err != nil {
		t.Fatal(err)
	}
	return database, journal
}

func newJournalAtPathForChild(path string, config Config) (store.Database, *Journal) {
	database, journal, err := openJournal(path, config)
	if err != nil {
		panic(err)
	}
	return database, journal
}

func openJournal(path string, config Config) (store.Database, *Journal, error) {
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		return nil, nil, err
	}
	journal, err := New(context.Background(), database, config)
	if err != nil {
		_ = database.Close()
		return nil, nil, err
	}
	return database, journal, nil
}

func journalMarkerAndWait() {
	if err := os.WriteFile(os.Getenv("NEWBLOK_JOURNAL_MARKER"), []byte("ready"), 0o600); err != nil {
		panic(err)
	}
	time.Sleep(10 * time.Second)
}

func waitForJournalMarker(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("journal child did not reach barrier: %s", path)
}
