package journal

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// compactionCrashRuns is how many completed runs the crash test compacts
// next to its one active run: one leaf run every other run names as its
// child, and the rest seeded with a row in every table a run owns.
const compactionCrashRuns = 50

// TestCompactionSurvivesAKillAtEitherCommitBarrier kills a real child
// process while it compacts 50 completed runs next to an active one, once
// just before the compaction transaction commits and once just after
// (#342, #49 AC5, probe P8). After each kill the store reopens and passes
// its integrity and foreign-key checks, the active run is whole row for
// row, and the completed runs are either all untouched (before) or all
// compacted with their purge still owed (after), never a mix. Compacting
// again then finishes the job and pays the purge.
func TestCompactionSurvivesAKillAtEitherCommitBarrier(t *testing.T) {
	if os.Getenv("NEWBLOK_COMPACT_CHILD") == "1" {
		runCompactionChild(t)
		return
	}
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "journal.db")
			markerPath := filepath.Join(directory, "marker")

			database, j := openRetentionJournal(t, databasePath, Hooks{})
			leaf := admitCompletedLeaf(t, j, "leaf")
			completed := []recoveryRun{{runID: leaf}}
			for i := 1; i < compactionCrashRuns; i++ {
				run := seedRecoveryRun(t, j, fmt.Sprintf("completed-%02d", i), leaf)
				completeRun(t, j, run.runID)
				completed = append(completed, run)
			}
			active := seedRecoveryRun(t, j, "active", leaf)
			before := map[string]runRows{active.runID: snapshotRun(t, j, active)}
			requireEveryTable(t, active.runID, before[active.runID])
			for _, run := range completed {
				before[run.runID] = snapshotRun(t, j, run)
			}
			requireEveryTable(t, completed[1].runID, before[completed[1].runID])
			if err := database.Close(); err != nil {
				t.Fatal(err)
			}

			command := exec.Command(os.Args[0], "-test.run=^TestCompactionSurvivesAKillAtEitherCommitBarrier$", "-test.v")
			command.Env = append(os.Environ(), "NEWBLOK_COMPACT_CHILD=1", "NEWBLOK_JOURNAL_PHASE="+phase, "NEWBLOK_JOURNAL_PATH="+databasePath, "NEWBLOK_JOURNAL_MARKER="+markerPath)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = command.Process.Kill() })
			started := time.Now()
			waitForCompactionBarrier(t, markerPath)
			t.Logf("child reached the %s-commit barrier after %v", phase, time.Since(started).Round(time.Millisecond))
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()
			if command.ProcessState.Success() {
				t.Fatal("compaction child exited on its own; it was not killed at the barrier")
			}

			database, j = reopenAfterKill(t, databasePath)
			defer database.Close()
			assertWhole(t, j, active, before[active.runID])
			tombstones, err := j.TombstoneCount(ctx)
			if err != nil {
				t.Fatal(err)
			}
			erased, purged := erasureGenerations(t, j)
			switch phase {
			case "before":
				// The kill rolled the whole compaction back.
				for _, run := range completed {
					assertWhole(t, j, run, before[run.runID])
				}
				if tombstones != 0 || erased != 0 || purged != 0 {
					t.Fatalf("rolled-back compaction left tombstones=%d erasure generation=%d purged=%d", tombstones, erased, purged)
				}
			case "after":
				// The compaction committed whole, and so did the purge it owes.
				for _, run := range completed {
					assertCompacted(t, j, run, before[run.runID])
				}
				if tombstones != compactionCrashRuns || erased != 1 || purged != 0 {
					t.Fatalf("committed compaction left tombstones=%d erasure generation=%d purged=%d, want %d, 1, 0", tombstones, erased, purged, compactionCrashRuns)
				}
			}

			report, err := j.Compact(ctx, time.Now().Add(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			wantRemoved := compactionCrashRuns
			if phase == "after" {
				wantRemoved = 0
			}
			if report.RemovedRuns != wantRemoved || report.Tombstones != compactionCrashRuns || !report.LogPurged || report.PurgePending {
				t.Fatalf("compaction after the kill=%+v, want %d removed, %d tombstones and the log purged", report, wantRemoved, compactionCrashRuns)
			}
			for _, run := range completed {
				assertCompacted(t, j, run, before[run.runID])
			}
			assertWhole(t, j, active, before[active.runID])
			if erased, purged := erasureGenerations(t, j); erased != 1 || purged != 1 {
				t.Fatalf("erasure generation=%d purged=%d after the purge, want 1, 1", erased, purged)
			}
			if result := integrityCheck(t, database); result != "ok" {
				t.Fatalf("integrity after finishing the compaction: %s", result)
			}
		})
	}
}

// runCompactionChild compacts the parent's journal and parks at the
// requested side of the compaction commit until it is killed.
func runCompactionChild(t *testing.T) {
	phase := os.Getenv("NEWBLOK_JOURNAL_PHASE")
	park := func(side string) func(string) {
		return func(name string) {
			if name != "compact" || phase != side {
				return
			}
			if err := os.WriteFile(os.Getenv("NEWBLOK_JOURNAL_MARKER"), []byte(side), 0o600); err != nil {
				panic(err)
			}
			// Park until the parent kills this process.
			time.Sleep(time.Hour)
		}
	}
	database, j := openRetentionJournal(t, os.Getenv("NEWBLOK_JOURNAL_PATH"), Hooks{BeforeCommit: park("before"), AfterCommit: park("after")})
	defer database.Close()
	if _, err := j.Compact(context.Background(), time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
}

// waitForCompactionBarrier waits for the child to park at its barrier. Its
// deadline is generous because the child parks until killed, so a loaded
// machine only slows the test down.
func waitForCompactionBarrier(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("compaction child did not reach its barrier: %s", path)
}

// reopenAfterKill opens the killed child's store, checks its integrity and
// foreign keys before the journal touches it, and opens the journal.
func reopenAfterKill(t *testing.T, path string) (store.Database, *Journal) {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if result := integrityCheck(t, database); result != "ok" {
		t.Fatalf("integrity after the kill: %s", result)
	}
	var violations int
	err = database.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(context.Background(), `PRAGMA foreign_key_check`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			violations++
		}
		return rows.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if violations != 0 {
		t.Fatalf("%d foreign-key violations after the kill", violations)
	}
	j, err := New(context.Background(), database, Config{Audit: newTestAudit(t, database)})
	if err != nil {
		t.Fatal(err)
	}
	return database, j
}

// erasureGenerations reads the erasure and purge counters Compact keeps in
// journal_meta.
func erasureGenerations(t *testing.T, j *Journal) (erased, purged int64) {
	t.Helper()
	err := j.withRead(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COALESCE((SELECT value FROM journal_meta WHERE name = ?), 0), COALESCE((SELECT value FROM journal_meta WHERE name = ?), 0)`, metaErasureGeneration, metaPurgedGeneration).Scan(&erased, &purged)
	})
	if err != nil {
		t.Fatal(err)
	}
	return erased, purged
}
