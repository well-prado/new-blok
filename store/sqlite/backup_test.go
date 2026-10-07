package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// seedDatabase creates a database at path holding rows rows of pad bytes
// each, with an index, so it spans many pages, and returns it open.
func seedDatabase(t *testing.T, path string, rows, pad int) *connection {
	t.Helper()
	database, err := seedDatabaseAt(path, rows, pad)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	return database
}

func seedDatabaseAt(path string, rows, pad int) (*connection, error) {
	ctx := context.Background()
	database, err := (Backend{}).Open(ctx, path)
	if err != nil {
		return nil, err
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `CREATE TABLE entries (id INTEGER PRIMARY KEY, value BLOB NOT NULL)`); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `CREATE INDEX entries_prefix ON entries (substr(value, 1, 16))`); err != nil {
			return err
		}
		for index := range rows {
			if _, err := tx.ExecContext(ctx, `INSERT INTO entries (id, value) VALUES (?, ?)`, index, entryValue(index, pad)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = database.Close()
		return nil, err
	}
	return database.(*connection), nil
}

func entryValue(index, pad int) []byte {
	return append([]byte(fmt.Sprintf("%08d:", index)), bytes.Repeat([]byte{byte('a' + index%26)}, pad)...)
}

// requireEntries opens path through the backend and fails unless it passes
// its integrity check and holds exactly the rows seedDatabase wrote.
func requireEntries(t *testing.T, path string, rows, pad int) {
	t.Helper()
	ctx := context.Background()
	database, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer database.Close()
	if err := database.Integrity(ctx); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	count := 0
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.QueryContext(ctx, `SELECT id, value FROM entries ORDER BY id`)
		if err != nil {
			return err
		}
		defer result.Close()
		for result.Next() {
			var id int
			var value []byte
			if err := result.Scan(&id, &value); err != nil {
				return err
			}
			if id != count || !bytes.Equal(value, entryValue(id, pad)) {
				return fmt.Errorf("row %d is id %d with %d bytes", count, id, len(value))
			}
			count++
		}
		return result.Err()
	}); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	if count != rows {
		t.Fatalf("%s holds %d rows; want %d", path, count, rows)
	}
}

// corruptPage overwrites the second-to-last page of a database file with
// garbage. Page 1 is untouched, so the file still opens (and switches to
// WAL); only PRAGMA integrity_check finds the damage.
func corruptPage(t *testing.T, path string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	const pageSize = 4096
	pages := len(contents) / pageSize
	if pages < 4 {
		t.Fatalf("%s has %d pages; the corruption needs at least 4", path, pages)
	}
	for index := (pages - 2) * pageSize; index < (pages-1)*pageSize; index++ {
		contents[index] = 0xAA
	}
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
}

func fileDigest(t *testing.T, path string) [sha256.Size]byte {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return sha256.Sum256(contents)
}

// entries lists a directory's names; a missing directory lists nothing.
func entries(t *testing.T, directory string) []string {
	t.Helper()
	listed, err := os.ReadDir(directory)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(listed))
	for _, entry := range listed {
		names = append(names, entry.Name())
	}
	return names
}

func requireAbsentFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("%s exists (err=%v); want it absent", path, err)
	}
}

// TestRestoreRefusesACorruptBackupBeforeCopyingIt (#343, mutation M49b): a
// backup that fails PRAGMA integrity_check is refused before Restore writes
// anything beside the destination. Without the source check the copy is
// installed and only the restored check would catch it, after a corrupt
// database has sat at the destination.
func TestRestoreRefusesACorruptBackupBeforeCopyingIt(t *testing.T) {
	ctx := context.Background()
	const rows, pad = 400, 200
	database := seedDatabase(t, filepath.Join(t.TempDir(), "journal.db"), rows, pad)
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := database.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	corruptPage(t, backup)
	before := fileDigest(t, backup)
	var reached []string
	restoreCopied = func(string) { reached = append(reached, "copied") }
	restoreInstalled = func(string) { reached = append(reached, "installed") }
	t.Cleanup(func() { restoreCopied, restoreInstalled = nil, nil })

	parent := filepath.Join(t.TempDir(), "restore")
	destination := filepath.Join(parent, "restored.db")
	err := (Backend{}).Restore(ctx, backup, destination)
	if err == nil || !strings.Contains(err.Error(), "source integrity") {
		t.Fatalf("Restore of a corrupt backup: err=%v; want the source integrity check to refuse it", err)
	}
	if len(reached) != 0 {
		t.Fatalf("Restore reached %v with a corrupt backup; it must refuse before copying", reached)
	}
	requireAbsentFile(t, destination)
	if left := entries(t, parent); len(left) != 0 {
		t.Fatalf("Restore left %v beside the destination", left)
	}
	if fileDigest(t, backup) != before {
		t.Fatal("Restore changed the corrupt backup")
	}
}

// TestRestoreRemovesACopyThatFailsItsCheck (#343, mutation M49b2): when the
// installed copy fails its integrity check, here because the test damages
// it between the rename and the check, Restore fails, removes it with any
// -wal or -shm the check opened, and a retry installs a good copy.
func TestRestoreRemovesACopyThatFailsItsCheck(t *testing.T) {
	ctx := context.Background()
	const rows, pad = 400, 200
	database := seedDatabase(t, filepath.Join(t.TempDir(), "journal.db"), rows, pad)
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := database.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	destination := filepath.Join(parent, "restored.db")
	restoreInstalled = func(path string) { corruptPage(t, path) }
	t.Cleanup(func() { restoreInstalled = nil })
	err := (Backend{}).Restore(ctx, backup, destination)
	if err == nil || !strings.Contains(err.Error(), "restored integrity") {
		t.Fatalf("Restore of a copy damaged after the rename: err=%v; want the restored integrity check to fail it", err)
	}
	requireAbsentFile(t, destination)
	if left := entries(t, parent); len(left) != 0 {
		t.Fatalf("a failed Restore left %v beside the destination", left)
	}
	restoreInstalled = nil
	if err := (Backend{}).Restore(ctx, backup, destination); err != nil {
		t.Fatalf("retry after the failed check: %v", err)
	}
	requireEntries(t, destination, rows, pad)
}

// TestRestoreLeavesTheBackupUntouched (#343, D9a): Restore only reads its
// source. Before #343 it opened the backup read-write, which switched the
// file to WAL (header bytes 18–19 went from 1 1 to 2 2) and could not
// restore from a read-only backup at all. A backup Backup wrote is in
// rollback mode; a closed database copied as a backup is in WAL mode, which
// a read-only (but not immutable) connection cannot open in a read-only
// directory, since it must create the -shm file.
func TestRestoreLeavesTheBackupUntouched(t *testing.T) {
	if os.Geteuid() == 0 {
		// Root writes through read-only permissions, so as root this test
		// runs again in a child process as an unprivileged user.
		if os.Getenv(unprivilegedChildEnv) == "1" {
			t.Fatal("the unprivileged child still runs as root")
		}
		rerunUnprivileged(t, "TestRestoreLeavesTheBackupUntouched")
		return
	}
	for _, test := range []struct {
		name              string
		walFile, readOnly bool
		wantHeader1819    byte
	}{
		{name: "Backup's file, writable", wantHeader1819: 1},
		{name: "Backup's file, read-only file and directory", readOnly: true, wantHeader1819: 1},
		{name: "closed WAL-mode database, read-only file and directory", walFile: true, readOnly: true, wantHeader1819: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.readOnly && runtime.GOOS == "windows" {
				t.Skip("POSIX permissions")
			}
			ctx := context.Background()
			const rows, pad = 300, 100
			directory := t.TempDir()
			backup := filepath.Join(directory, "backup.db")
			if test.walFile {
				database, err := seedDatabaseAt(backup, rows, pad)
				if err != nil {
					t.Fatal(err)
				}
				if err := database.Close(); err != nil {
					t.Fatal(err)
				}
			} else {
				database := seedDatabase(t, filepath.Join(t.TempDir(), "journal.db"), rows, pad)
				if err := database.Backup(ctx, backup); err != nil {
					t.Fatal(err)
				}
			}
			header, err := os.ReadFile(backup)
			if err != nil {
				t.Fatal(err)
			}
			if header[18] != test.wantHeader1819 || header[19] != test.wantHeader1819 {
				t.Fatalf("the backup's header bytes 18-19 are %v; the case needs %d", header[18:20], test.wantHeader1819)
			}
			before := sha256.Sum256(header)
			info, err := os.Stat(backup)
			if err != nil {
				t.Fatal(err)
			}
			if test.readOnly {
				if err := os.Chmod(backup, 0o444); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(directory, 0o555); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Chmod(directory, 0o755) })
			}
			destination := filepath.Join(t.TempDir(), "restored.db")
			if err := (Backend{}).Restore(ctx, backup, destination); err != nil {
				t.Fatalf("Restore: %v", err)
			}
			if fileDigest(t, backup) != before {
				header, _ := os.ReadFile(backup)
				t.Fatalf("Restore changed the backup (header bytes 18-19 now %v)", header[18:20])
			}
			after, err := os.Stat(backup)
			if err != nil {
				t.Fatal(err)
			}
			if !after.ModTime().Equal(info.ModTime()) {
				t.Fatalf("Restore touched the backup: modified %v, was %v", after.ModTime(), info.ModTime())
			}
			if left := entries(t, directory); !slices.Equal(left, []string{"backup.db"}) {
				t.Fatalf("Restore left %v beside the backup; want only backup.db", left)
			}
			requireEntries(t, destination, rows, pad)
		})
	}
}

// TestRestoreRefusesASourceWithALiveLog (#343): Restore checks and copies
// the database file alone, so a source whose write-ahead log still holds
// committed rows is refused instead of restored without them.
func TestRestoreRefusesASourceWithALiveLog(t *testing.T) {
	ctx := context.Background()
	source := filepath.Join(t.TempDir(), "live.db")
	// Held open, the database keeps its committed rows in its log.
	seedDatabase(t, source, 50, 100)
	if info, err := os.Stat(source + "-wal"); err != nil || info.Size() == 0 {
		t.Fatalf("the open source has no log to test with: info=%v err=%v", info, err)
	}
	destination := filepath.Join(t.TempDir(), "restored.db")
	err := (Backend{}).Restore(ctx, source, destination)
	if err == nil || !strings.Contains(err.Error(), "not self-contained") {
		t.Fatalf("Restore of a source with a live log: err=%v; want it refused", err)
	}
	requireAbsentFile(t, destination)
}

// staleLog leaves at path+"-wal" the write-ahead log of another database
// at path, holding rows rows the database file never received, and removes
// that database: what a crash, or an operator deleting only app.db, leaves
// behind.
func staleLog(t *testing.T, path string, rows int) []byte {
	t.Helper()
	database := seedDatabase(t, path, rows, 100)
	log, err := os.ReadFile(path + "-wal")
	if err != nil || len(log) == 0 {
		t.Fatalf("the old database has no log to leave behind: %d bytes, err=%v", len(log), err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(path+"-wal", log, 0o600); err != nil {
		t.Fatal(err)
	}
	return log
}

// TestRestoreRefusesALeftoverBesideTheDestination (#343, Review R): SQLite
// reads a -wal, -shm or -journal file beside a database as that database's,
// so a log an old database at the destination left behind was replayed into
// the restored copy: from a 400-row backup Restore returned nil and the
// destination held the old database's 7 rows, passing integrity_check.
// Restore refuses, and leaves the leftover for the operator.
func TestRestoreRefusesALeftoverBesideTheDestination(t *testing.T) {
	ctx := context.Background()
	const rows, pad = 400, 200
	database := seedDatabase(t, filepath.Join(t.TempDir(), "journal.db"), rows, pad)
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := database.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		t.Run(suffix, func(t *testing.T) {
			destination := filepath.Join(t.TempDir(), "app.db")
			var leftover []byte
			if suffix == "-wal" {
				leftover = staleLog(t, destination, 7)
			} else {
				leftover = bytes.Repeat([]byte{0x5A}, 4096)
				if err := os.WriteFile(destination+suffix, leftover, 0o600); err != nil {
					t.Fatal(err)
				}
			}
			err := (Backend{}).Restore(ctx, backup, destination)
			if err == nil {
				restored := -1
				if db, openErr := (Backend{}).Open(ctx, destination); openErr == nil {
					_ = db.WithTx(ctx, func(tx *sql.Tx) error {
						return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries`).Scan(&restored)
					})
					_ = db.Close()
				}
				t.Fatalf("Restore beside a leftover %s returned nil; the restored database holds %d rows of the backup's %d", suffix, restored, rows)
			}
			if !strings.Contains(err.Error(), suffix) {
				t.Fatalf("Restore beside a leftover %s: err=%v; want it to name the leftover", suffix, err)
			}
			requireAbsentFile(t, destination)
			if got, err := os.ReadFile(destination + suffix); err != nil || !bytes.Equal(got, leftover) {
				t.Fatalf("Restore changed the leftover %s (err=%v)", suffix, err)
			}
		})
	}
}

// TestRestoreRechecksForALeftoverBeforeInstalling (#343, Review R): a log
// that appears beside the destination while Restore copies is caught by the
// check just before the rename, not only by the one at the start.
func TestRestoreRechecksForALeftoverBeforeInstalling(t *testing.T) {
	ctx := context.Background()
	const rows, pad = 400, 200
	database := seedDatabase(t, filepath.Join(t.TempDir(), "journal.db"), rows, pad)
	backup := filepath.Join(t.TempDir(), "backup.db")
	if err := database.Backup(ctx, backup); err != nil {
		t.Fatal(err)
	}
	parent := t.TempDir()
	destination := filepath.Join(parent, "app.db")
	restoreCopied = func(string) { staleLog(t, destination, 7) }
	t.Cleanup(func() { restoreCopied = nil })
	err := (Backend{}).Restore(ctx, backup, destination)
	if err == nil || !strings.Contains(err.Error(), "-wal") {
		t.Fatalf("Restore with a log left beside the destination during the copy: err=%v; want it refused", err)
	}
	requireAbsentFile(t, destination)
	if left := entries(t, parent); !slices.Equal(left, []string{"app.db-wal"}) {
		t.Fatalf("the refused Restore left %v; want only the leftover app.db-wal", left)
	}
}

// Child processes for TestProcessKillDuringRestore.
const (
	crashChildEnv  = "NEWBLOK_SQLITE_CRASH_POINT"
	crashSourceEnv = "NEWBLOK_SQLITE_CRASH_SOURCE"
	crashTargetEnv = "NEWBLOK_SQLITE_CRASH_TARGET"
	crashMarkerEnv = "NEWBLOK_SQLITE_CRASH_MARKER"
	// crashDoneSuffix marks a child whose Backup or Restore returned nil.
	crashDoneSuffix = ".done"
)

// TestProcessKillDuringRestore (#343) kills a real child process with
// SIGKILL inside Restore. Killed before the rename, it leaves nothing at the
// destination and a retry succeeds; killed after it, the destination holds
// the whole, checked backup, which a second Restore never overwrites.
func TestProcessKillDuringRestore(t *testing.T) {
	if phase := os.Getenv(crashChildEnv); phase != "" {
		runCrashPointChild(phase)
		return
	}
	const rows, pad = 400, 200
	for _, phase := range []string{"restore-before-rename", "restore-after-rename"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			database := seedDatabase(t, filepath.Join(t.TempDir(), "journal.db"), rows, pad)
			backup := filepath.Join(t.TempDir(), "backup.db")
			if err := database.Backup(ctx, backup); err != nil {
				t.Fatal(err)
			}
			before := fileDigest(t, backup)
			parent := t.TempDir()
			destination := filepath.Join(parent, "restored.db")
			killAtCrashPoint(t, "TestProcessKillDuringRestore", phase, backup, destination)
			if fileDigest(t, backup) != before {
				t.Fatal("the killed Restore changed the backup")
			}
			if phase == "restore-after-rename" {
				requireEntries(t, destination, rows, pad)
				if err := (Backend{}).Restore(ctx, backup, destination); err == nil || !strings.Contains(err.Error(), "already exists") {
					t.Fatalf("a second Restore over the installed one: err=%v; want it refused", err)
				}
				return
			}
			requireAbsentFile(t, destination)
			if err := (Backend{}).Restore(ctx, backup, destination); err != nil {
				t.Fatalf("retry after the kill: %v (left %v)", err, entries(t, parent))
			}
			requireEntries(t, destination, rows, pad)
		})
	}
}

// killAtCrashPoint runs test's child at phase's crash point and kills it
// there with SIGKILL.
func killAtCrashPoint(t *testing.T, test, phase, source, destination string) {
	t.Helper()
	command, marker := startCrashPointChild(t, test, phase, source, destination)
	waitForMarker(t, marker)
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
}

func startCrashPointChild(t *testing.T, test, phase, source, destination string) (*exec.Cmd, string) {
	t.Helper()
	marker := filepath.Join(t.TempDir(), "marker")
	command := exec.Command(os.Args[0], "-test.run=^"+test+"$", "-test.count=1")
	// Kept for diagnosing a child that fails instead of parking.
	command.Stdout, command.Stderr = &bytes.Buffer{}, &bytes.Buffer{}
	command.Env = append(os.Environ(),
		crashChildEnv+"="+phase,
		crashSourceEnv+"="+source,
		crashTargetEnv+"="+destination,
		crashMarkerEnv+"="+marker,
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	return command, marker
}

// runCrashPointChild runs one Restore, writes the marker at the
// phase's crash point and waits there for the parent to kill it.
func runCrashPointChild(phase string) {
	ctx := context.Background()
	source, destination, marker := os.Getenv(crashSourceEnv), os.Getenv(crashTargetEnv), os.Getenv(crashMarkerEnv)
	park := func(string) {
		if err := os.WriteFile(marker, []byte(phase), 0o600); err != nil {
			panic(err)
		}
		time.Sleep(time.Minute)
		panic("the parent did not kill the child")
	}
	var err error
	switch phase {
	case "restore-before-rename":
		restoreCopied = park
		err = (Backend{}).Restore(ctx, source, destination)
	case "restore-after-rename":
		restoreInstalled = park
		err = (Backend{}).Restore(ctx, source, destination)
	default:
		panic("unknown phase " + phase)
	}
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile(marker+crashDoneSuffix, nil, 0o600); err != nil {
		panic(err)
	}
	panic("the child passed its crash point")
}
