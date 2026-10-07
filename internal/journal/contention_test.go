package journal

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// Each case executes a real transition while a different connection owns the
// WAL writer lock. A read-first transition fails its upgrade immediately;
// write-first waits for the writer without taking a stale read snapshot.
func TestJournalTransitionsWaitForConcurrentWriter(t *testing.T) {
	for _, name := range []string{"replay", "attempt-start", "effect-commit", "signal", "wait-claim", "checkpoint", "scope-start", "artifact-register", "reconcile", "compact"} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			j, err := New(ctx, db, Config{Audit: newTestAudit(t, db)})
			if err != nil {
				t.Fatal(err)
			}
			run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "request", Workflow: "synthetic", ArtifactDigest: "artifact", Input: []byte(`{}`)})
			if err != nil {
				t.Fatal(err)
			}
			op, err := j.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: run.RunID, ArtifactDigest: "artifact", InvocationPath: "effect", IterationPath: "root"}})
			if err != nil {
				t.Fatal(err)
			}
			var attempt Attempt
			if name == "effect-commit" || name == "reconcile" {
				attempt, err = j.StartAttempt(ctx, op.Key)
				if err != nil {
					t.Fatal(err)
				}
			}
			if name == "reconcile" {
				if err := j.MarkUncertain(ctx, op.Key, attempt.ID, "synthetic timeout"); err != nil {
					t.Fatal(err)
				}
			}
			if name == "wait-claim" {
				if _, err := j.ScheduleWait(ctx, WaitRequest{RunID: run.RunID, WaitID: "wait", Name: "ready", InvocationPath: "ready", IterationPath: "root", DueAt: time.Unix(1, 0)}); err != nil {
					t.Fatal(err)
				}
			}
			if name == "compact" {
				if err := j.CompleteRun(ctx, run.RunID, []byte(`{}`)); err != nil {
					t.Fatal(err)
				}
			}
			transition := func() error {
				switch name {
				case "replay":
					_, err := j.Replay(ctx, run.RunID, "replay")
					return err
				case "attempt-start":
					_, err := j.StartAttempt(ctx, op.Key)
					return err
				case "effect-commit":
					return j.CommitEffect(ctx, EffectCommit{OperationKey: op.Key, AttemptID: attempt.ID, Result: []byte(`{}`)})
				case "signal":
					_, err := j.Signal(ctx, signal.Envelope{RunID: run.RunID, SignalID: "signal", Name: "ready", Principal: "synthetic", Payload: []byte(`{}`)}, true)
					return err
				case "wait-claim":
					records, err := j.ClaimDueWaits(ctx, time.Now(), 1)
					if err == nil && len(records) != 1 {
						t.Errorf("claimed %d waits", len(records))
					}
					return err
				case "checkpoint":
					return j.SaveCheckpoint(ctx, Checkpoint{RunID: run.RunID, ArtifactDigest: "artifact", CheckpointDigest: "checkpoint", State: []byte(`{}`)})
				case "scope-start":
					_, err := j.StartScope(ctx, ScopeRecord{RunID: run.RunID, Path: "scope", Kind: "branch"})
					return err
				case "artifact-register":
					return j.RegisterArtifact(ctx, ArtifactRecord{Digest: "artifact", Version: "1.0.0", ManifestJSON: []byte(`{}`)})
				case "reconcile":
					_, _, err := j.reconcileOnce(ctx, op.Key, "operator", "synthetic receipt", []byte(`{}`))
					return err
				case "compact":
					report, err := j.Compact(ctx, time.Now().Add(time.Hour))
					if err == nil && report.RemovedRuns != 1 {
						t.Errorf("removed %d runs", report.RemovedRuns)
					}
					return err
				default:
					panic("unknown transition")
				}
			}
			reader := *j
			started := make(chan struct{})
			j.database = &startedTransaction{Database: db, started: started}
			contendJournal(t, ctx, db, started, transition, func() error { _, err := reader.Run(ctx, run.RunID); return err })
		})
	}
}

// Signal only after BeginTx has entered the actual mutation callback, not
// merely when the test's contender goroutine has been scheduled.
type startedTransaction struct {
	store.Database
	started chan struct{}
	once    sync.Once
}

func (d *startedTransaction) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return d.Database.WithTx(ctx, func(tx *sql.Tx) error {
		d.once.Do(func() { close(d.started) })
		return fn(tx)
	})
}

func contendJournal(t *testing.T, ctx context.Context, db store.Database, started <-chan struct{}, transition, read func() error) {
	t.Helper()
	locked, release, writerDone := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		writerDone <- db.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, `UPDATE journal_runs SET run_id=run_id WHERE 0`); err != nil {
				return err
			}
			close(locked)
			select {
			case <-release:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	select {
	case <-locked:
	case err := <-writerDone:
		t.Fatalf("writer lock: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer close(release)
	readDone := make(chan error, 1)
	go func() { readDone <- read() }()
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("read blocked behind writer")
	}
	done := make(chan error, 1)
	go func() { done <- transition() }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("transition did not enter transaction")
	}
	select {
	case err := <-done:
		t.Fatalf("transition returned while writer held lock: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	// Release exactly once, including failure paths.
	release <- struct{}{}
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
