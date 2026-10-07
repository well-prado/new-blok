package journal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/inspect"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

func TestJournalInspectionInputMigrationPreservesLegacyUnknowns(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-journal.db")
	database, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	legacyStatements := make([]string, len(schemaStatements))
	for index, statement := range schemaStatements {
		statement = strings.ReplaceAll(statement, "\n\t\terror_code TEXT NOT NULL DEFAULT ''", "")
		statement = strings.ReplaceAll(statement, "\n\t\terror_class TEXT NOT NULL DEFAULT ''", "")
		statement = strings.ReplaceAll(statement, "\n\t\tinput_json BLOB,", "")
		statement = strings.ReplaceAll(statement, "principal TEXT NOT NULL DEFAULT '',", "principal TEXT NOT NULL DEFAULT ''")
		statement = strings.ReplaceAll(statement, ",,\n", ",\n")
		statement = strings.ReplaceAll(statement, ",\n\t)", "\n\t)")
		legacyStatements[index] = statement
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		for index, statement := range legacyStatements {
			if _, err := tx.ExecContext(ctx, statement); err != nil {
				return fmt.Errorf("legacy schema statement %d: %w (%s)", index, err, statement)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_runs
			(run_id,request_key,workflow,artifact_digest,input_json,input_digest,state,replay_of,created_at,principal)
			VALUES ('run:legacy','legacy','legacy','sha256:legacy',?,'sha256:input','accepted','',1,'alice')`, []byte(`{"legacy":true}`)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_operations
			(operation_key,run_id,artifact_digest,invocation_path,iteration_path,provider_operation_key,state,current_attempt_id,created_at,updated_at)
			VALUES ('op:legacy','run:legacy','sha256:legacy','charge','root','provider:legacy','intent','',1,1)`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO journal_attempts
			(attempt_id,operation_key,attempt_number,provider_operation_key,state,error_text,started_at)
			VALUES ('attempt:legacy','op:legacy',1,'provider:legacy','failed','legacy failure',1)`); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `INSERT INTO journal_scopes(run_id,path,kind,parent_path,state,updated_at)
			VALUES ('run:legacy','legacy-step','node','','running',1)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database, journal := newJournalAtPath(t, path, Config{})
	defer database.Close()
	if run, err := journal.Run(ctx, "run:legacy"); err != nil || run.Principal != "alice" || string(run.Input) != `{"legacy":true}` || run.ErrorCode != "" {
		t.Fatalf("legacy run migration=%+v err=%v", run, err)
	}
	identity := OperationIdentity{RunID: "run:legacy", ArtifactDigest: "sha256:legacy", InvocationPath: "charge", IterationPath: "root"}
	if _, err := journal.BeginEffect(ctx, EffectIntent{Identity: identity, Input: []byte(`{"guessed":true}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := journal.StartScope(ctx, ScopeRecord{RunID: "run:legacy", Path: "legacy-step", Kind: "node", Input: []byte(`{"guessed":true}`)}); err != nil {
		t.Fatal(err)
	}
	page, err := inspect.InspectSource(ctx, journal, "alice", inspection.Policy{Fields: map[inspection.Field]bool{inspection.FieldInput: true}, MaxPageSize: 10}, inspection.Query{Version: inspection.Version, RunID: "run:legacy"})
	if err != nil || len(page.Steps) != 2 {
		t.Fatalf("legacy migrated page=%+v err=%v", page, err)
	}
	for _, step := range page.Steps {
		if len(step.Input) != 0 {
			t.Fatalf("invented legacy step input for %q: %s", step.ID, step.Input)
		}
		for _, attempt := range step.Attempts {
			if len(attempt.Input) != 0 {
				t.Fatalf("invented legacy attempt input for %q: %s", step.ID, attempt.Input)
			}
		}
	}
}

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
	// The lineage is read back from the store, not only from Replay's
	// return value (#340, mutation M48d).
	stored, err := journal.Run(context.Background(), replay.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ReplayOf != admission.RunID || stored.State != runAccepted || stored.ArtifactDigest != "sha256:artifact" {
		t.Fatalf("stored replay=%+v, want replay_of %q on the source artifact", stored, admission.RunID)
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
	if err := journal.CommitEffect(context.Background(), EffectCommit{OperationKey: operation.Key, AttemptID: attempt.ID, Result: []byte(`{"ok":true}`)}); err != nil {
		panic(err)
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

// TestWriteFailureDoesNotAcknowledgeAdmission covers a store that refuses
// before any transaction starts; TestRealDiskFullFailsClosedAndReopensIntact
// covers a real ENOSPC inside one, on Linux.
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

// smallFilesystemEnv names a directory on a small, size-limited filesystem
// that TestRealDiskFullFailsClosedAndReopensIntact may fill. CI and local
// runs leave it unset, and the test skips; it runs in Docker on Linux:
//
//	docker run --rm --tmpfs /mnt/small:size=4m -e BLOK_TEST_SMALL_FS=/mnt/small \
//	  -v "$PWD":/src:ro -w /src golang:1.27.1 \
//	  go test -count=1 -run TestRealDiskFullFailsClosedAndReopensIntact ./internal/journal
const smallFilesystemEnv = "BLOK_TEST_SMALL_FS"

const (
	// diskFullPad is the size of each admission's input, so the room
	// left to the journal fills within a few admissions.
	diskFullPad = 32 << 10
	// diskFullRoom is the space left for the journal: a ballast file
	// fills the filesystem, then gives back this much.
	diskFullRoom = 512 << 10
	// diskFullLimit bounds the ballast before the test decides the
	// directory is not on a small filesystem.
	diskFullLimit = 64 << 20
)

// TestRealDiskFullFailsClosedAndReopensIntact (#44, #337) fills a real
// filesystem with journal admissions until SQLite meets ENOSPC. The
// admission that does not fit must fail, leave nothing of itself behind,
// and leave every earlier admission whole; once space is freed the file
// must reopen, pass PRAGMA integrity_check, and accept the refused
// admission. TestWriteFailureDoesNotAcknowledgeAdmission covers the other
// failure point, a store that refuses before any transaction starts.
func TestRealDiskFullFailsClosedAndReopensIntact(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skipf("needs a size-limited Linux filesystem (tmpfs) in %s; GOOS is %s", smallFilesystemEnv, runtime.GOOS)
	}
	small := os.Getenv(smallFilesystemEnv)
	if small == "" {
		t.Skipf("%s is unset; set it to a directory on a small tmpfs (e.g. docker run --tmpfs /mnt/small:size=4m)", smallFilesystemEnv)
	}
	ctx := context.Background()
	directory, err := os.MkdirTemp(small, "disk-full-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	path := filepath.Join(directory, "journal.db")
	database, journal := newJournalAtPath(t, path, Config{})
	closed := false
	defer func() {
		if !closed {
			_ = database.Close()
		}
	}()
	// A ballast file takes all the free space and gives diskFullRoom back,
	// so the journal fills only that room; removing the ballast afterwards
	// frees far more than the journal wrote, enough for SQLite to
	// checkpoint the whole log into the file on reopen.
	ballast := filepath.Join(directory, "ballast")
	if err := fillFilesystem(ballast); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(ballast)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() < 2*diskFullRoom {
		t.Fatalf("%s had %d KiB free; the test needs at least %d KiB (e.g. a 4m tmpfs)", smallFilesystemEnv, info.Size()>>10, 2*diskFullRoom>>10)
	}
	if err := os.Truncate(ballast, info.Size()-diskFullRoom); err != nil {
		t.Fatal(err)
	}

	type admitted struct{ runID, input string }
	acknowledged := map[string]admitted{}
	request := func(index int) AdmissionRequest {
		input := fmt.Sprintf(`{"index":%d,"pad":%q}`, index, strings.Repeat(string(rune('a'+index%26)), diskFullPad))
		return AdmissionRequest{RequestKey: fmt.Sprintf("disk-full-%04d", index), Workflow: "orders/quote", ArtifactDigest: "sha256:artifact", Input: []byte(input)}
	}
	var refused AdmissionRequest
	var admitErr error
	for index := 0; index*diskFullPad < 4*diskFullRoom; index++ {
		next := request(index)
		admission, err := journal.Admit(ctx, next)
		if err != nil {
			refused, admitErr = next, err
			break
		}
		if !admission.Accepted || admission.RunID == "" {
			t.Fatalf("admission %d=%+v", index, admission)
		}
		// An acknowledged admission must be in the journal: a store that
		// lost a failed commit's error would acknowledge one that is not.
		if run, err := journal.Run(ctx, admission.RunID); err != nil || run.RequestKey != next.RequestKey {
			t.Fatalf("admission %d was acknowledged but is not in the journal: run=%q err=%v", index, run.RunID, err)
		}
		acknowledged[next.RequestKey] = admitted{runID: admission.RunID, input: string(next.Input)}
	}
	if admitErr == nil {
		t.Fatalf("%d admissions fit in %d KiB of free space without filling it", len(acknowledged), diskFullRoom>>10)
	}
	t.Logf("admission %d (%d KiB input) failed: %v", len(acknowledged), diskFullPad>>10, admitErr)
	if !strings.Contains(admitErr.Error(), "disk is full") {
		t.Fatalf("the failing admission's error is not SQLite's disk-full error: %v", admitErr)
	}
	if len(acknowledged) == 0 {
		t.Fatal("the filesystem was full before the first admission; it must hold some committed admissions to show they survive")
	}
	// The filesystem itself must be out of space, so the failure above was
	// a real ENOSPC and not some other limit.
	if err := os.WriteFile(filepath.Join(directory, "probe"), make([]byte, diskFullPad), 0o600); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("writing beside the journal after the failure: err=%v, want ENOSPC", err)
	}
	_ = os.Remove(filepath.Join(directory, "probe"))

	// Exactly the acknowledged admissions are visible, each whole, and
	// nothing of the refused one: through the same handle while the disk
	// is still full, and through a fresh handle after space is freed.
	requireExactly := func(stage string, database store.Database) {
		t.Helper()
		seen := map[string]admitted{}
		if err := database.WithTx(ctx, func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, `SELECT request_key, run_id, input_json, input_digest FROM journal_runs`)
			if err != nil {
				return err
			}
			defer rows.Close()
			for rows.Next() {
				var key, runID, digest string
				var input []byte
				if err := rows.Scan(&key, &runID, &input, &digest); err != nil {
					return err
				}
				if digest != digestBytes(input) {
					return fmt.Errorf("%s: input of %s does not match its digest (%d bytes)", key, runID, len(input))
				}
				seen[key] = admitted{runID: runID, input: string(input)}
			}
			return rows.Err()
		}); err != nil {
			t.Fatalf("%s: read admissions: %v", stage, err)
		}
		if _, ok := seen[refused.RequestKey]; ok {
			t.Fatalf("%s: the refused admission %s is visible", stage, refused.RequestKey)
		}
		if len(seen) != len(acknowledged) {
			t.Fatalf("%s: %d admissions visible, want the %d acknowledged", stage, len(seen), len(acknowledged))
		}
		for key, want := range acknowledged {
			if got, ok := seen[key]; !ok || got != want {
				t.Fatalf("%s: acknowledged admission %s missing or changed (found=%v)", stage, key, ok)
			}
		}
	}
	requireExactly("disk full", database)

	// The process gives up with the disk still full. Closing may fail to
	// checkpoint; what matters is the file it leaves behind.
	if err := database.Close(); err != nil {
		t.Logf("close with the disk full: %v", err)
	}
	closed = true
	if err := os.Remove(ballast); err != nil {
		t.Fatal(err)
	}
	reopened, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatalf("reopen after freeing space: %v", err)
	}
	defer reopened.Close()
	if err := reopened.Integrity(ctx); err != nil {
		t.Fatalf("PRAGMA integrity_check after freeing space: %v", err)
	}
	requireExactly("reopened", reopened)
	journal, err = New(ctx, reopened, Config{})
	if err != nil {
		t.Fatalf("journal over the reopened store: %v", err)
	}
	admission, err := journal.Admit(ctx, refused)
	if err != nil || !admission.Accepted {
		t.Fatalf("the refused admission after freeing space=%+v err=%v", admission, err)
	}
}

// fillFilesystem writes path until its filesystem has no space left, and
// fails unless that happens, with ENOSPC, within diskFullLimit.
func fillFilesystem(path string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	chunk := make([]byte, 64<<10)
	for written := 0; written < diskFullLimit; {
		n, err := file.Write(chunk)
		written += n
		if errors.Is(err, syscall.ENOSPC) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("fill %s: %w", path, err)
		}
	}
	return fmt.Errorf("%s is not on a small filesystem: %d MiB written without ENOSPC", path, diskFullLimit>>20)
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
