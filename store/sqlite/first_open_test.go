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
		for i, command := range commands {
			if err := command.Wait(); err != nil {
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
		t.Skip("POSIX permissions and /dev/null")
	}
	if os.Geteuid() == 0 {
		t.Skip("root writes through read-only permissions")
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
		{"under /dev/null", func(*testing.T) string { return "/dev/null/x.db" }, "create parent"},
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
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "held.db")
			holder, err := sql.Open("sqlite", "file:"+uriPath(path))
			if err != nil {
				t.Fatal(err)
			}
			defer holder.Close()
			held, err := holder.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer held.Close()
			if _, err := held.ExecContext(ctx, "BEGIN "+lock); err != nil {
				t.Fatal(err)
			}
			const timeout = 300 * time.Millisecond
			type result struct {
				database store.Database
				err      error
			}
			opened := make(chan result, 1)
			begin := time.Now()
			go func() {
				database, err := (Backend{BusyTimeout: timeout}).Open(ctx, path)
				opened <- result{database, err}
			}()
			var got result
			select {
			case got = <-opened:
			case <-time.After(10 * time.Second):
				// Let a retry loop that ignores the timeout finish, then fail.
				_, _ = held.ExecContext(ctx, "ROLLBACK")
				got = <-opened
				if got.database != nil {
					_ = got.database.Close()
				}
				t.Fatalf("Open kept retrying for 10s with a %v busy timeout; it finally returned %v", timeout, got.err)
			}
			elapsed := time.Since(begin)
			if got.err == nil {
				_ = got.database.Close()
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
