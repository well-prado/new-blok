package sqlite

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestProcessKillBeforeDuringAndAfterCommit(t *testing.T) {
	if os.Getenv("NEWBLOK_SQLITE_CHILD") == "1" {
		runCrashChild()
		return
	}
	for _, phase := range []string{"before", "during", "after"} {
		t.Run(phase, func(t *testing.T) {
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "journal.db")
			markerPath := filepath.Join(directory, "marker")
			command := exec.Command(os.Args[0], "-test.run=TestProcessKillBeforeDuringAndAfterCommit", "-test.v")
			command.Env = append(os.Environ(),
				"NEWBLOK_SQLITE_CHILD=1",
				"NEWBLOK_SQLITE_PHASE="+phase,
				"NEWBLOK_SQLITE_PATH="+databasePath,
				"NEWBLOK_SQLITE_MARKER="+markerPath,
			)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForMarker(t, markerPath)
			// The "during" child finishes its commit and exits on its own, so
			// it may be gone before the kill. POSIX still signals an unreaped
			// child; Windows refuses to terminate an exited process.
			killErr := command.Process.Kill()
			_ = command.Wait()
			if killErr != nil && (command.ProcessState == nil || !command.ProcessState.Exited()) {
				t.Fatal(killErr)
			}

			database, err := (Backend{}).Open(context.Background(), databasePath)
			if err != nil {
				t.Fatal(err)
			}
			defer database.Close()
			if err := database.Integrity(context.Background()); err != nil {
				t.Fatal(err)
			}
			count := 0
			if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
				return tx.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM entries").Scan(&count)
			}); err != nil {
				t.Fatal(err)
			}
			switch phase {
			case "before":
				if count != 0 {
					t.Fatalf("uncommitted row survived process kill: count=%d", count)
				}
			case "after":
				if count != 1 {
					t.Fatalf("committed row was lost after process kill: count=%d", count)
				}
			case "during":
				if count != 0 && count != 1 {
					t.Fatalf("unexpected recovered row count=%d", count)
				}
			}
		})
	}
}

func runCrashChild() {
	ctx := context.Background()
	databasePath := os.Getenv("NEWBLOK_SQLITE_PATH")
	markerPath := os.Getenv("NEWBLOK_SQLITE_MARKER")
	phase := os.Getenv("NEWBLOK_SQLITE_PHASE")
	database, err := (Backend{}).Open(ctx, databasePath)
	if err != nil {
		panic(err)
	}
	defer database.Close()
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
		return err
	}); err != nil {
		panic(err)
	}
	connection := database.(*connection)
	wait := func() {
		if err := os.WriteFile(markerPath, []byte("ready"), 0o600); err != nil {
			panic(err)
		}
		time.Sleep(10 * time.Second)
	}
	switch phase {
	case "before":
		database.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, "INSERT INTO entries (value) VALUES ('accepted')"); err != nil {
				return err
			}
			wait()
			return nil
		})
	case "during":
		connection.beforeCommit = func() {
			if err := os.WriteFile(markerPath, []byte("ready"), 0o600); err != nil {
				panic(err)
			}
		}
		if err := database.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entries (value) VALUES ('accepted')")
			return err
		}); err != nil {
			panic(err)
		}
	case "after":
		connection.afterCommit = wait
		if err := database.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "INSERT INTO entries (value) VALUES ('accepted')")
			return err
		}); err != nil {
			panic(err)
		}
	}
}

func waitForMarker(t *testing.T, path string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("child did not reach crash barrier: %s", path)
}

func TestCorruptedDatabaseFailsIntegrityCheck(t *testing.T) {
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "journal.db")
	database, err := (Backend{}).Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)")
		return err
	}); err != nil {
		t.Fatal(err)
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
	corrupted, err := (Backend{}).Open(ctx, databasePath)
	if err != nil {
		return
	}
	defer corrupted.Close()
	if err := corrupted.Integrity(ctx); err == nil {
		t.Fatal("truncated database passed integrity check")
	}
}
