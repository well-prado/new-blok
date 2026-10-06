package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
)

// firstOpeners is how many handles or processes open one brand-new file at
// once. Each in-process round costs milliseconds, so that test runs many
// more of them: the losing interleaving is rarer between goroutines.
const (
	firstOpeners          = 8
	inProcessFirstOpens   = 100
	acrossProcessesRounds = 10
	// childOpenLimit bounds how long the first-open processes of one round
	// may take, from the go file to their exit. Open waits at most the
	// default five-second busy timeout.
	childOpenLimit = time.Minute
)

// openAndCheckWAL opens path and confirms the handle is usable and in WAL
// mode, the way a replica's first boot would see it.
func openAndCheckWAL(ctx context.Context, path string) error {
	database, err := (Backend{}).Open(ctx, path)
	if err != nil {
		return err
	}
	defer database.Close()
	var mode string
	if err := database.(*connection).database.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		return fmt.Errorf("read journal mode: %w", err)
	}
	if !strings.EqualFold(mode, journalMode) {
		return fmt.Errorf("journal mode is %q, want %q", mode, journalMode)
	}
	return nil
}

// TestConcurrentFirstOpensInProcess: eight handles in one process open the
// same brand-new file at the same moment; every one must open it, in WAL
// mode (#320). Before the fix one of them could fail its first
// PRAGMA journal_mode=WAL with "database is locked".
func TestConcurrentFirstOpensInProcess(t *testing.T) {
	ctx := context.Background()
	var failures []string
	for round := range inProcessFirstOpens {
		path := filepath.Join(t.TempDir(), "first.db")
		start := make(chan struct{})
		errs := make(chan error, firstOpeners)
		var ready, done sync.WaitGroup
		for range firstOpeners {
			ready.Add(1)
			done.Add(1)
			go func() {
				defer done.Done()
				ready.Done()
				<-start
				errs <- openAndCheckWAL(ctx, path)
			}()
		}
		ready.Wait()
		close(start)
		done.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				failures = append(failures, fmt.Sprintf("round %d: %v", round, err))
			}
		}
	}
	if len(failures) > 0 {
		t.Fatalf("%d of %d concurrent first opens failed (#320):\n%s", len(failures), firstOpeners*inProcessFirstOpens, strings.Join(failures, "\n"))
	}
}

// TestConcurrentFirstOpensAcrossProcesses: eight processes open the same
// brand-new file at the same moment; every one must open it, in WAL mode
// (#320). The in-process handles share nothing but the file either, but
// only separate processes rule out any in-process serialization.
func TestConcurrentFirstOpensAcrossProcesses(t *testing.T) {
	if os.Getenv("NEWBLOK_SQLITE_FIRST_OPEN_CHILD") == "1" {
		runFirstOpenChild()
		return
	}
	if testing.Short() {
		t.Skip("spawns processes")
	}
	var failures []string
	for round := range acrossProcessesRounds {
		directory := t.TempDir()
		path := filepath.Join(directory, "first.db")
		goFile := filepath.Join(directory, "go")
		commands := make([]*exec.Cmd, firstOpeners)
		outputs := make([]*strings.Builder, firstOpeners)
		for i := range commands {
			outputs[i] = &strings.Builder{}
			command := exec.Command(os.Args[0], "-test.run=^TestConcurrentFirstOpensAcrossProcesses$")
			command.Env = append(os.Environ(),
				"NEWBLOK_SQLITE_FIRST_OPEN_CHILD=1",
				"NEWBLOK_SQLITE_FIRST_OPEN_PATH="+path,
				"NEWBLOK_SQLITE_FIRST_OPEN_GO="+goFile,
				"NEWBLOK_SQLITE_FIRST_OPEN_READY="+filepath.Join(directory, "ready-"+strconv.Itoa(i)),
			)
			command.Stdout, command.Stderr = outputs[i], outputs[i]
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			commands[i] = command
			// A round that fails before its children exit must not leave
			// them polling for the go file.
			t.Cleanup(func() { _ = command.Process.Kill() })
		}
		// Release the openers together, once every process is up and
		// polling for the go file.
		deadline := time.Now().Add(30 * time.Second)
		for i := range commands {
			for {
				if _, err := os.Stat(filepath.Join(directory, "ready-"+strconv.Itoa(i))); err == nil {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("round %d: opener %d never became ready", round, i)
				}
				time.Sleep(time.Millisecond)
			}
		}
		if err := os.WriteFile(goFile, nil, 0o644); err != nil {
			t.Fatal(err)
		}
		// A child that never returns must fail this round, naming it, rather
		// than hang the test until go test's own timeout.
		exited := make(chan int, len(commands))
		waits := make([]error, len(commands))
		for i, command := range commands {
			go func() {
				waits[i] = command.Wait()
				exited <- i
			}()
		}
		hung := make(map[int]bool, len(commands))
		for i := range commands {
			hung[i] = true
		}
		limit := time.NewTimer(childOpenLimit)
	wait:
		for range commands {
			select {
			case i := <-exited:
				delete(hung, i)
			case <-limit.C:
				break wait
			}
		}
		limit.Stop()
		if len(hung) > 0 {
			for i := range hung {
				_ = commands[i].Process.Kill()
			}
			for range hung {
				<-exited
			}
			var stuck []string
			for i := range commands {
				if hung[i] {
					stuck = append(stuck, fmt.Sprintf("opener %d (killed; output %q)", i, strings.TrimSpace(outputs[i].String())))
				}
			}
			t.Fatalf("round %d: %d of %d first-open processes had not exited %v after the go file: %s", round, len(hung), len(commands), childOpenLimit, strings.Join(stuck, ", "))
		}
		for i, err := range waits {
			if err != nil {
				failures = append(failures, fmt.Sprintf("round %d opener %d: %v: %s", round, i, err, strings.TrimSpace(outputs[i].String())))
			}
		}
	}
	if len(failures) > 0 {
		t.Fatalf("%d of %d concurrent first opens across processes failed (#320):\n%s", len(failures), firstOpeners*acrossProcessesRounds, strings.Join(failures, "\n"))
	}
}

func runFirstOpenChild() {
	if err := os.WriteFile(os.Getenv("NEWBLOK_SQLITE_FIRST_OPEN_READY"), nil, 0o644); err != nil {
		fmt.Println(err)
		os.Exit(2)
	}
	goFile := os.Getenv("NEWBLOK_SQLITE_FIRST_OPEN_GO")
	for {
		if _, err := os.Stat(goFile); err == nil {
			break
		}
		time.Sleep(50 * time.Microsecond)
	}
	if err := openAndCheckWAL(context.Background(), os.Getenv("NEWBLOK_SQLITE_FIRST_OPEN_PATH")); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	os.Exit(0)
}

// TestUnwritablePathFailsFast: a path SQLite can never write fails Open at
// once with an error naming the cause, not after a busy retry loop, and not
// as store.ErrBusy (#320).
func TestUnwritablePathFailsFast(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permissions")
	}
	if os.Geteuid() == 0 {
		// Root writes through read-only permissions, so as root this test
		// runs again in a child process as an unprivileged user.
		if os.Getenv(unprivilegedChildEnv) == "1" {
			t.Fatal("the unprivileged child still runs as root")
		}
		rerunUnprivileged(t, "TestUnwritablePathFailsFast")
		return
	}
	readOnlyDirectory := func(t *testing.T) string {
		directory := t.TempDir()
		if err := os.Chmod(directory, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(directory, 0o755) })
		return directory
	}
	cases := []struct {
		name string
		path func(t *testing.T) string
		want string
	}{
		{"read-only file", func(t *testing.T) string {
			directory := t.TempDir()
			path := filepath.Join(directory, "read-only.db")
			if err := os.WriteFile(path, nil, 0o444); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(directory, 0o555); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(directory, 0o755) })
			return path
		}, "readonly"},
		{"read-only directory", func(t *testing.T) string {
			return filepath.Join(readOnlyDirectory(t), "new.db")
		}, "unable to open"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := tc.path(t)
			begin := time.Now()
			database, err := (Backend{}).Open(context.Background(), path)
			elapsed := time.Since(begin)
			if err == nil {
				_ = database.Close()
				t.Fatalf("opened the unwritable path %s", path)
			}
			if errors.Is(err, store.ErrBusy) {
				t.Fatalf("an unwritable path failed busy: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("the error %q does not say %q", err, tc.want)
			}
			// The default busy timeout is five seconds; a retry loop would
			// spend it.
			if elapsed > time.Second {
				t.Fatalf("an unwritable path took %v to fail: %v", elapsed, err)
			}
		})
	}
}

// TestWALSwitchGivesUpBusyWithinTheBusyTimeout: while another connection
// holds the new file, Open waits for it only until the busy timeout is
// spent, then fails with store.ErrBusy (#320). A reserved lock lets Open
// read the file but never switch it, so Open retries the switch for the
// whole timeout; an exclusive lock already stops it connecting.
func TestWALSwitchGivesUpBusyWithinTheBusyTimeout(t *testing.T) {
	for _, lock := range []string{"IMMEDIATE", "EXCLUSIVE"} {
		t.Run(lock, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "held.db")
			release := holdNewFile(t, path, lock)
			const timeout = 300 * time.Millisecond
			opened := make(chan openResult, 1)
			begin := time.Now()
			go func() {
				database, err := (Backend{BusyTimeout: timeout}).Open(context.Background(), path)
				opened <- openResult{database, err}
			}()
			var got openResult
			select {
			case got = <-opened:
			case <-time.After(10 * time.Second):
				// Let a retry loop that ignores the timeout finish, then fail.
				release()
				got = <-opened
				got.close()
				t.Fatalf("Open kept retrying for 10s with a %v busy timeout; it finally returned %v", timeout, got.err)
			}
			elapsed := time.Since(begin)
			if got.err == nil {
				got.close()
				t.Fatal("Open switched a file another connection holds")
			}
			if !errors.Is(got.err, store.ErrBusy) {
				t.Fatalf("Open of a held file returned %v; want store.ErrBusy", got.err)
			}
			if elapsed < timeout || elapsed > timeout+2*time.Second {
				t.Fatalf("Open gave up after %v (%v); want about the %v busy timeout", elapsed, got.err, timeout)
			}
		})
	}
}

// TestWALSwitchPausesBetweenAttempts: while another connection holds the new
// file, Open pauses before each new attempt to switch it to WAL, so a held
// lock costs a handful of attempts over the busy timeout, not a busy loop
// (#320). The pauses start at 1ms and double up to maxWALSwitchPause, so a
// 300ms timeout leaves room for about a dozen; a loop that never pauses
// makes thousands.
func TestWALSwitchPausesBetweenAttempts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "held.db")
	holdNewFile(t, path, "IMMEDIATE")
	const most = 25
	// Past the bound the test has failed; cancelling ends a loop that would
	// otherwise retry for as long as it ignores the busy timeout.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var failed int
	walSwitchRetrying = func(n int) {
		failed = n
		if n > most {
			cancel()
		}
	}
	defer func() { walSwitchRetrying = nil }()
	const timeout = 300 * time.Millisecond
	database, err := (Backend{BusyTimeout: timeout}).Open(ctx, path)
	if err == nil {
		_ = database.Close()
		t.Fatal("Open switched a file another connection holds")
	}
	if failed > most {
		t.Fatalf("Open made more than %d busy attempts in a %v busy timeout (cancelled then: %v); want a pause before each retry", most, timeout, err)
	}
	if !errors.Is(err, store.ErrBusy) {
		t.Fatalf("Open of a held file returned %v; want store.ErrBusy", err)
	}
	if failed < 2 {
		t.Fatalf("Open made %d busy attempts in a %v busy timeout; want it to retry", failed, timeout)
	}
}

// TestCancelledOpenStopsRetrying: cancelling Open while it waits to retry the
// WAL switch ends it at once, with no further attempt, and the error carries
// both the cancellation and the busy failure it was waiting out (#320).
func TestCancelledOpenStopsRetrying(t *testing.T) {
	path := filepath.Join(t.TempDir(), "held.db")
	holdNewFile(t, path, "IMMEDIATE")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const cancelAfter = 3
	var failed int
	walSwitchRetrying = func(n int) {
		failed = n
		if n == cancelAfter {
			cancel()
		}
	}
	defer func() { walSwitchRetrying = nil }()
	begin := time.Now()
	database, err := (Backend{BusyTimeout: time.Minute}).Open(ctx, path)
	elapsed := time.Since(begin)
	if err == nil {
		_ = database.Close()
		t.Fatal("Open switched a file another connection holds")
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, store.ErrBusy) {
		t.Fatalf("a cancelled Open returned %v; want context.Canceled joined with store.ErrBusy", err)
	}
	if failed != cancelAfter {
		t.Fatalf("Open was cancelled during its pause after %d busy attempts, but %d attempts failed busy: it tried again after the cancellation", cancelAfter, failed)
	}
	if elapsed > 10*time.Second {
		t.Fatalf("a cancelled Open took %v to return", elapsed)
	}
}

// TestWALSwitchFailsUnlessTheFileEndsInWAL: SQLite answers a journal-mode
// switch it cannot make with the mode the database keeps, not an error.
// enableWAL fails then instead of returning a handle that is not in WAL
// (#320). Open never reaches this with a file path, so the switch is run
// directly against an in-memory database, which cannot be in WAL.
func TestWALSwitchFailsUnlessTheFileEndsInWAL(t *testing.T) {
	database, err := sql.Open("sqlite", "file:/new-blok-not-wal?vfs=memdb")
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	err = enableWAL(context.Background(), database, time.Second)
	if err == nil {
		t.Fatal("enableWAL succeeded on a database that cannot be in WAL")
	}
	if errors.Is(err, store.ErrBusy) || !strings.Contains(err.Error(), "stayed in journal mode") {
		t.Fatalf("enableWAL returned %v; want it to name the journal mode the database stayed in", err)
	}
}

type openResult struct {
	database store.Database
	err      error
}

func (r openResult) close() {
	if r.database != nil {
		_ = r.database.Close()
	}
}

// holdNewFile creates the database at path and holds it in a transaction
// begun with lock (IMMEDIATE or EXCLUSIVE), until the returned release or
// the end of the test.
func holdNewFile(t *testing.T, path, lock string) (release func()) {
	t.Helper()
	ctx := context.Background()
	holder, err := sql.Open("sqlite", "file:"+uriPath(path))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = holder.Close() })
	held, err := holder.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = held.Close() })
	if _, err := held.ExecContext(ctx, "BEGIN "+lock); err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	release = func() { once.Do(func() { _, _ = held.ExecContext(ctx, "ROLLBACK") }) }
	t.Cleanup(release)
	return release
}
