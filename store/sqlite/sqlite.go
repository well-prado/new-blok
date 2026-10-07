// Package sqlite is the selected embedded durable backend.
package sqlite

import (
	"context"
	"crypto/rand"
	"database/sql"
	sqldriver "database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"time"

	"github.com/well-prado/new-blok/store"
	driver "modernc.org/sqlite"
)

// sqliteBusy is SQLITE_BUSY; extended codes carry it in their low byte.
const sqliteBusy = 5

// busy marks an error caused by SQLITE_BUSY as store.ErrBusy, so callers can
// tell a saturated store from a failure, and names the write domain whose
// lock was not available. An error that already is store.ErrBusy, such as
// another store's busy error returned through a callback, keeps its domain.
func busy(err error, domain *store.WriteDomain) error {
	var failure *driver.Error
	if err != nil && errors.As(err, &failure) && failure.Code()&0xff == sqliteBusy && !errors.Is(err, store.ErrBusy) {
		return store.WithWriteDomain(fmt.Errorf("%w: %w", store.ErrBusy, err), domain)
	}
	return err
}

const (
	journalMode = "WAL"
	synchronous = "FULL"
	// secureDelete is set on every pooled connection: SQLite then overwrites
	// deleted content with zeros, in the page and on freed pages, instead of
	// leaving it readable in the file until the space is reused (#281).
	secureDelete = "secure_delete(ON)"
	// maxLogPurgeWait bounds how long PurgeLog waits for readers to leave
	// the log, since the checkpoint holds writers meanwhile (#281).
	maxLogPurgeWait = 100 * time.Millisecond
	// defaultBusyTimeout bounds how long a writer waits for the write lock,
	// in the writer queue and in SQLite's busy handler, before ErrBusy.
	defaultBusyTimeout = 5 * time.Second
)

// sharedMemoryWriteDomain is the one writer domain of every :memory: handle,
// and sharedMemoryWriters its one writer queue.
var (
	sharedMemoryWriteDomain = store.NewWriteDomain()
	sharedMemoryWriters     = make(chan struct{}, 1)
)

// Backend opens SQLite databases with the durability settings required by the
// journal. It is the only package that imports the SQLite driver.
type Backend struct {
	// BusyTimeout bounds how long a writer waits for the write lock before
	// the transaction fails with store.ErrBusy. Zero means five seconds.
	BusyTimeout time.Duration
}

var (
	_ store.Backend = Backend{}
	_ store.Purger  = (*connection)(nil)
)

func (b Backend) busyTimeout() time.Duration {
	if b.BusyTimeout > 0 {
		return b.BusyTimeout
	}
	return defaultBusyTimeout
}

func (b Backend) Open(ctx context.Context, path string) (store.Database, error) {
	if path == "" {
		return nil, errors.New("sqlite: database path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("sqlite: create parent: %w", err)
		}
	}
	timeout := b.busyTimeout()
	database, err := sql.Open("sqlite", dsn(path, timeout))
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(8)
	if err := configure(ctx, database, path, timeout); err != nil {
		_ = database.Close()
		return nil, err
	}
	writeDomain, writers := sharedMemoryWriteDomain, sharedMemoryWriters
	if path != ":memory:" {
		file, err := os.Stat(path)
		if err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("sqlite: identify write domain: %w", err)
		}
		writeDomain, writers = store.NewFileWriteDomain(file), make(chan struct{}, 1)
	}
	return &connection{database: database, writeDomain: writeDomain, writers: writers, busyTimeout: timeout}, nil
}

func configure(ctx context.Context, database *sql.DB, path string, timeout time.Duration) error {
	if path != ":memory:" {
		if err := enableWAL(ctx, database, timeout); err != nil {
			return err
		}
	}
	settings := []string{
		"PRAGMA synchronous=" + synchronous,
		fmt.Sprintf("PRAGMA busy_timeout=%d", timeout.Milliseconds()),
		"PRAGMA foreign_keys=ON",
	}
	for _, statement := range settings {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("sqlite: configure %q: %w", statement, err)
		}
	}
	return nil
}

// maxWALSwitchPause caps the pause between two attempts to switch a file
// to WAL.
const maxWALSwitchPause = 50 * time.Millisecond

// walSwitchRetrying, test-only, is called each time enableWAL is about to
// pause before another attempt, with the number of attempts that failed busy
// so far.
var walSwitchRetrying func(failed int)

// enableWAL switches the file to WAL, and fails unless it is in WAL
// afterwards. WAL is a property of the file, so every later connection to it
// opens in WAL.
//
// SQLite switches a file to WAL in a transaction that reads page 1 and then
// writes it, and it never waits for the write lock when a read transaction
// asks for it: the busy handler is skipped. So when several openers switch
// the same brand-new file at once, the losers fail SQLITE_BUSY at once,
// without their busy timeout (#320). The switch is therefore retried while
// it fails busy, until the busy timeout is spent, and each attempt's own
// busy wait is cut to what remains of it. Any other failure, such as an
// unwritable path, returns at once. The switch runs on a connection of its
// own that is discarded afterwards, so no pooled connection keeps the
// shortened wait.
func enableWAL(ctx context.Context, database *sql.DB, timeout time.Duration) error {
	statement := "PRAGMA journal_mode=" + journalMode
	deadline := time.Now().Add(timeout)
	// Connecting applies the DSN's pragmas, which wait out the busy timeout
	// on a file another connection holds.
	conn, err := database.Conn(ctx)
	if err != nil {
		return busy(fmt.Errorf("sqlite: open: %w", err), nil)
	}
	defer conn.Close()
	defer discard(conn)
	pause := time.Millisecond
	for failed := 1; ; failed++ {
		var mode string
		_, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", max(time.Until(deadline), 0).Milliseconds()))
		if err == nil {
			err = conn.QueryRowContext(ctx, statement).Scan(&mode)
		}
		if err == nil {
			if !strings.EqualFold(mode, journalMode) {
				return fmt.Errorf("sqlite: configure %q: the database stayed in journal mode %q", statement, mode)
			}
			return nil
		}
		err = busy(fmt.Errorf("sqlite: configure %q: %w", statement, err), nil)
		remaining := time.Until(deadline)
		if !errors.Is(err, store.ErrBusy) || remaining <= 0 {
			return err
		}
		if walSwitchRetrying != nil {
			walSwitchRetrying(failed)
		}
		timer := time.NewTimer(min(pause, remaining))
		select {
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		case <-timer.C:
		}
		pause = min(2*pause, maxWALSwitchPause)
	}
}

// dsn opens every :memory: path as one named database on SQLite's memdb VFS,
// so each handle shares the database and participates in one writer domain.
// A file's DSN does not ask for WAL: the driver would switch on connect,
// where a concurrent opener's busy failure cannot be retried; enableWAL
// switches it instead (#320).
func dsn(path string, timeout time.Duration) string {
	if path == ":memory:" {
		return fmt.Sprintf("file:/new-blok-memory?vfs=memdb&_busy_timeout=%d&_journal_mode=MEMORY&_synchronous=FULL&_foreign_keys=ON&_pragma=%s", timeout.Milliseconds(), secureDelete)
	}
	return (&url.URL{
		Scheme:   "file",
		Path:     uriPath(path),
		RawQuery: fmt.Sprintf("_busy_timeout=%d&_synchronous=FULL&_foreign_keys=ON&_pragma=%s", timeout.Milliseconds(), secureDelete),
	}).String()
}

// uriPath turns a filesystem path into the path of a SQLite file: URI. It
// must be absolute with forward slashes; a Windows drive path also gains a
// leading slash (file:///C:/data/app.db). Otherwise "C:\data" or a relative
// "app.db" lands in the URI's authority and SQLite refuses to open it.
func uriPath(path string) string {
	if goruntime.GOOS == "windows" {
		// An extended-length prefix would turn into a literal "/?/" path
		// segment; SQLite applies long-path handling itself.
		if unc, ok := strings.CutPrefix(path, `\\?\UNC\`); ok {
			path = `\\` + unc
		} else {
			path = strings.TrimPrefix(path, `\\?\`)
		}
	}
	if absolute, err := filepath.Abs(path); err == nil {
		path = absolute
	}
	path = filepath.ToSlash(path)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return path
}

type connection struct {
	database    *sql.DB
	writeDomain *store.WriteDomain
	// writers queues this handle's marked writers (store.Writer), one at a
	// time, in arrival order.
	writers      chan struct{}
	busyTimeout  time.Duration
	beforeCommit func()
	afterCommit  func()
	// restoreHook, test-only, replaces the busy-timeout reset after a log
	// purge.
	restoreHook func(context.Context, *sql.Conn) error
}

func (c *connection) WriteDomain() *store.WriteDomain { return c.writeDomain }

// BusyTimeout reports how long a writer waits for the write lock, in the
// writer queue and in SQLite's busy handler, before store.ErrBusy.
func (c *connection) BusyTimeout() time.Duration { return c.busyTimeout }

func (c *connection) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if fn == nil {
		return errors.New("sqlite: transaction callback is required")
	}
	if store.IsWriter(ctx) {
		release, err := c.queueWriter(ctx)
		if err != nil {
			return err
		}
		// Deferred before the rollback below, so it runs after it: the next
		// marked writer gets its turn only once this transaction has let go
		// of the write lock, on every exit.
		defer release()
	}
	tx, err := c.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return busy(fmt.Errorf("sqlite: begin: %w", err), c.writeDomain)
	}
	// Roll back on every exit that does not reach COMMIT, including a
	// callback (or the commit hook) that panics or calls runtime.Goexit
	// (#267). database/sql never rolls such a transaction back on its own
	// unless its context is canceled, and the worker's is deliberately not
	// cancelable: the transaction kept the write lock and its pooled
	// connection, so every later writer on the file failed busy until the
	// process exited. Nothing is recovered: a panic continues to the caller
	// with its original value and stack once the rollback has run.
	committing := false
	defer func() {
		if !committing {
			_ = tx.Rollback()
		}
	}()
	if err := fn(tx); err != nil {
		return busy(err, c.writeDomain)
	}
	if c.beforeCommit != nil {
		c.beforeCommit()
	}
	// From here database/sql ends the transaction itself, committed or not:
	// a failed COMMIT is rolled back by the driver.
	committing = true
	if err := tx.Commit(); err != nil {
		return busy(fmt.Errorf("sqlite: commit: %w", err), c.writeDomain)
	}
	if c.afterCommit != nil {
		c.afterCommit()
	}
	return nil
}

// queueWriter waits for this handle's earlier marked writers (store.Writer),
// first come first served, and returns the release for the writer's turn
// (#214). Left to
// SQLite, contending writers poll the lock with ever longer sleeps, so the
// writers that have waited longest poll least often and newcomers keep
// overtaking them: a writer can wait out the whole busy timeout behind
// transactions that each hold the lock for microseconds. Queued here, a
// writer waits only for those ahead of it. The wait is bounded by the busy
// timeout and fails as SQLite's would, with ErrBusy naming the write domain,
// so a handler that submits to the store its own claim holds still fails
// after one busy wait (#207). SQLite's busy handler still governs unmarked
// writers and writers on other handles and in other processes; a marked
// writer that meets one waits up to the busy timeout again there.
func (c *connection) queueWriter(ctx context.Context) (func(), error) {
	release := func() { <-c.writers }
	select {
	case c.writers <- struct{}{}:
		return release, nil
	default:
	}
	timer := time.NewTimer(c.busyTimeout)
	defer timer.Stop()
	select {
	case c.writers <- struct{}{}:
		return release, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("sqlite: begin: %w", ctx.Err())
	case <-timer.C:
		return nil, store.WithWriteDomain(fmt.Errorf("sqlite: begin: %w: no write turn within %v", store.ErrBusy, c.busyTimeout), c.writeDomain)
	}
}

// Backup writes a transaction-consistent copy of the database to
// destination, which must not exist. VACUUM INTO writes the copy to a
// temporary file beside destination; the file is synced, renamed into place
// and its directory synced, so destination never names a partial copy: a
// crash or a failure (a full disk, a canceled ctx) leaves destination absent
// and Backup can be retried (#343). A crash can leave the temporary file
// (.backup-*, with its -journal) behind; it never blocks a retry.
func (c *connection) Backup(ctx context.Context, destination string) error {
	if destination == "" || destination == ":memory:" {
		return errors.New("sqlite: backup destination is required")
	}
	directory := filepath.Dir(destination)
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return fmt.Errorf("sqlite: create backup parent: %w", err)
	}
	if err := requireAbsent(destination, "backup destination"); err != nil {
		return err
	}
	// VACUUM INTO creates the file itself, with SQLite's usual permissions,
	// and refuses one that exists and is not empty.
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return fmt.Errorf("sqlite: name backup temporary: %w", err)
	}
	temporary := filepath.Join(directory, ".backup-"+hex.EncodeToString(suffix))
	installed := false
	defer func() {
		if !installed {
			// SQLite deletes VACUUM INTO's rollback journal itself when
			// the statement fails; removeFiles also takes one it left.
			_ = removeFiles(temporary)
		}
	}()
	if _, err := c.database.ExecContext(ctx, "VACUUM INTO ?", temporary); err != nil {
		return fmt.Errorf("sqlite: backup: %w", err)
	}
	if err := syncFile(temporary); err != nil {
		return fmt.Errorf("sqlite: sync backup: %w", err)
	}
	if backupWritten != nil {
		backupWritten(temporary)
	}
	if err := install(temporary, destination, "backup"); err != nil {
		return err
	}
	installed = true
	return nil
}

// PurgeLog checkpoints the write-ahead log into the database and truncates
// it (#281). It takes this handle's writer turn, so marked writers on the
// handle are not starved by it; it fails with store.ErrBusy when a reader
// on any handle still needs the log after maxLogPurgeWait (or the busy
// timeout, if shorter), so writers wait at most that long behind it.
func (c *connection) PurgeLog(ctx context.Context) error {
	release, err := c.queueWriter(ctx)
	if err != nil {
		return err
	}
	defer release()
	return c.purgeLog(ctx)
}

// purgeLog runs the truncating checkpoint on a connection of its own whose
// busy timeout is maxLogPurgeWait, not the store's. A truncating checkpoint
// holds the write lock while it waits for readers, so waiting the full busy
// timeout for a long reader would hold every writer that long (#281).
func (c *connection) purgeLog(ctx context.Context) (err error) {
	conn, err := c.database.Conn(ctx)
	if err != nil {
		return busy(fmt.Errorf("sqlite: purge log: %w", err), c.writeDomain)
	}
	defer conn.Close()
	// The connection goes back to the pool, so it must leave with the
	// store's busy timeout, even when ctx is done; one left at the short
	// wait would fail its later writers busy early. If the timeout cannot
	// be restored (or was never confirmed lowered), the connection is
	// discarded instead of returned.
	defer func() {
		if restoreErr := c.restoreBusyTimeout(context.WithoutCancel(ctx), conn); restoreErr != nil {
			discard(conn)
			if err == nil {
				err = fmt.Errorf("sqlite: purge log: restore busy timeout: %w", restoreErr)
			}
		}
	}()
	wait := min(c.busyTimeout, maxLogPurgeWait)
	if _, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", wait.Milliseconds())); err != nil {
		return fmt.Errorf("sqlite: purge log: %w", err)
	}
	var blocked, frames, copied int
	if err := conn.QueryRowContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)").Scan(&blocked, &frames, &copied); err != nil {
		return busy(fmt.Errorf("sqlite: purge log: %w", err), c.writeDomain)
	}
	if blocked != 0 {
		return store.WithWriteDomain(fmt.Errorf("sqlite: purge log: %w: a reader still uses the log", store.ErrBusy), c.writeDomain)
	}
	return nil
}

// restoreBusyTimeout sets conn back to the store's busy timeout. Tests
// replace it through restoreHook to make the reset fail.
func (c *connection) restoreBusyTimeout(ctx context.Context, conn *sql.Conn) error {
	if c.restoreHook != nil {
		return c.restoreHook(ctx, conn)
	}
	_, err := conn.ExecContext(ctx, fmt.Sprintf("PRAGMA busy_timeout=%d", c.busyTimeout.Milliseconds()))
	return err
}

// discard closes conn's driver connection instead of returning it to the
// pool: database/sql drops a connection whose Raw callback reports
// sqldriver.ErrBadConn.
func discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return sqldriver.ErrBadConn })
}

// PurgeFree rebuilds the database with VACUUM, which leaves no free page and
// no free space holding deleted content, then purges the log (#281).
func (c *connection) PurgeFree(ctx context.Context) error {
	release, err := c.queueWriter(ctx)
	if err != nil {
		return err
	}
	defer release()
	if _, err := c.database.ExecContext(ctx, "VACUUM"); err != nil {
		return busy(fmt.Errorf("sqlite: purge free space: %w", err), c.writeDomain)
	}
	return c.purgeLog(ctx)
}

func (c *connection) Integrity(ctx context.Context) error {
	var result string
	if err := c.database.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("sqlite: integrity check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("sqlite: integrity check failed: %s", result)
	}
	return nil
}

func (c *connection) Close() error { return c.database.Close() }

// Test-only crash points of Backup and Restore, called when set.
var (
	// backupWritten runs once Backup's temporary file is written and
	// synced, before it is renamed to the destination.
	backupWritten func(temporary string)
	// restoreCopied runs once Restore's temporary copy is written and
	// synced, before it is renamed to the destination.
	restoreCopied func(temporary string)
	// restoreInstalled runs once the copy is renamed to the destination and
	// the directory synced, before the restored database is checked.
	restoreInstalled func(destination string)
)

// Restore copies an integrity-checked SQLite backup into a new destination.
// It refuses a destination that exists or has a -wal, -shm or -journal file
// beside it, checking again just before it installs the copy; a file another
// process creates there in that moment is replaced (see install). The source
// must not be written while Restore runs. The backup is only read: it is
// checked through a read-only, immutable connection and copied through a
// read-only file, so its bytes are unchanged and it may be read-only, on a
// read-only directory (#343). The copy is written to a temporary file beside
// the destination, synced, renamed into place and its directory synced, then
// opened and checked again; if that check fails the destination is removed.
// So when Restore returns an error the destination does not exist, and an
// interrupted Restore leaves at most its temporary file (.restore-*), which
// never blocks a retry. A crash after the rename leaves the whole, synced
// copy of the checked backup at the destination.
func (Backend) Restore(ctx context.Context, source, destination string) error {
	if source == "" || destination == "" || source == ":memory:" || destination == ":memory:" || source == destination {
		return errors.New("sqlite: distinct source and destination paths are required")
	}
	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("sqlite: inspect source: %w", err)
	}
	if err := requireAbsent(destination, "restore destination"); err != nil {
		return err
	}
	if err := requireSelfContained(source); err != nil {
		return err
	}
	if err := checkSource(ctx, source); err != nil {
		return fmt.Errorf("sqlite: source integrity: %w", err)
	}
	directory := filepath.Dir(destination)
	if err := createDirectory(directory); err != nil {
		return fmt.Errorf("sqlite: create destination parent: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".restore-*")
	if err != nil {
		return fmt.Errorf("sqlite: create restore temporary: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	input, err := os.Open(source)
	if err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sqlite: reopen source: %w", err)
	}
	if _, err := io.Copy(temporary, input); err != nil {
		_ = input.Close()
		_ = temporary.Close()
		return fmt.Errorf("sqlite: copy backup: %w", err)
	}
	if err := input.Close(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sqlite: close backup: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sqlite: sync restore: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("sqlite: close restore: %w", err)
	}
	if restoreCopied != nil {
		restoreCopied(temporaryPath)
	}
	if err := install(temporaryPath, destination, "restore"); err != nil {
		return err
	}
	if restoreInstalled != nil {
		restoreInstalled(destination)
	}
	if err := checkRestored(ctx, destination); err != nil {
		return errors.Join(err, removeDatabase(destination))
	}
	return nil
}

// requireSelfContained refuses a source with a non-empty write-ahead log or
// rollback journal beside it. Restore reads and copies the database file
// alone, so changes held in either would be silently left out; Restore will
// not fold them in, since that writes to the backup. Backup writes a
// self-contained file.
func requireSelfContained(source string) error {
	for _, suffix := range []string{"-wal", "-journal"} {
		info, err := os.Stat(source + suffix)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("sqlite: inspect source %s: %w", suffix, err)
		}
		if info.Size() > 0 {
			return fmt.Errorf("sqlite: restore source is not self-contained: %s holds changes the database file does not; restore a backup written by Backup, or checkpoint the source first", source+suffix)
		}
	}
	return nil
}

// checkSource runs PRAGMA integrity_check on a backup without writing to
// it. The connection is read-only and immutable: SQLite takes no lock and
// creates no -wal, -shm or -journal file, so it works on a read-only file in
// a read-only directory, whatever journal mode the file's header names; it
// reads the database file alone, exactly the bytes Restore copies
// (requireSelfContained refuses a source whose log holds more).
func checkSource(ctx context.Context, source string) error {
	database, err := sql.Open("sqlite", (&url.URL{Scheme: "file", Path: uriPath(source), RawQuery: "mode=ro&immutable=1"}).String())
	if err != nil {
		return fmt.Errorf("sqlite: open: %w", err)
	}
	defer database.Close()
	database.SetMaxOpenConns(1)
	var result string
	if err := database.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result); err != nil {
		return fmt.Errorf("sqlite: integrity check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("sqlite: integrity check failed: %s", result)
	}
	return nil
}

// checkRestored opens the installed copy through the backend and checks its
// integrity, closing it before it returns so a failed copy can be removed.
func checkRestored(ctx context.Context, destination string) error {
	restored, err := (Backend{}).Open(ctx, destination)
	if err != nil {
		return fmt.Errorf("sqlite: open restored database: %w", err)
	}
	if err := restored.Integrity(ctx); err != nil {
		_ = restored.Close()
		return fmt.Errorf("sqlite: restored integrity: %w", err)
	}
	if err := restored.Close(); err != nil {
		return fmt.Errorf("sqlite: close restored database: %w", err)
	}
	return nil
}

// requireAbsent fails unless nothing exists at path, nor a -wal, -shm or
// -journal file beside it. SQLite reads such a file as the database's own:
// a log an old database at path left behind would be replayed into the new
// one (#343). A leftover is refused, not removed: it may belong to a
// database still in use.
func requireAbsent(path, what string) error {
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(path + suffix); err == nil {
			if suffix == "" {
				return fmt.Errorf("sqlite: %s already exists: %s", what, path)
			}
			return fmt.Errorf("sqlite: %s already exists beside the %s: %s; SQLite would read it into the new database, so remove it if no database uses it", suffix, what, path+suffix)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("sqlite: inspect %s: %w", what, err)
		}
	}
	return nil
}

// install renames a synced temporary file to destination, which must not
// exist nor have a -wal, -shm or -journal file beside it, and syncs the
// directory so the rename survives a power loss. If the directory cannot be
// synced, the installed file is removed again. Destination is checked just
// before the rename; a file created there in between by another process
// would be replaced.
func install(temporary, destination, what string) error {
	if err := requireAbsent(destination, what+" destination"); err != nil {
		return err
	}
	if err := os.Rename(temporary, destination); err != nil {
		return fmt.Errorf("sqlite: install %s: %w", what, err)
	}
	if err := syncDirectory(filepath.Dir(destination)); err != nil {
		return errors.Join(fmt.Errorf("sqlite: sync %s directory: %w", what, err), removeDatabase(destination))
	}
	return nil
}

// removeDatabase removes a database Backup or Restore installed, with its
// side files, and syncs its directory.
func removeDatabase(path string) error {
	err := removeFiles(path)
	if syncErr := syncDirectory(filepath.Dir(path)); syncErr != nil {
		err = errors.Join(err, fmt.Errorf("sqlite: sync directory after removing %s: %w", path, syncErr))
	}
	return err
}

// removeFiles removes a database file with any -wal, -shm or -journal file
// SQLite left beside it.
func removeFiles(path string) error {
	var failures []error
	for _, suffix := range []string{"", "-wal", "-shm", "-journal"} {
		if err := os.Remove(path + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			failures = append(failures, fmt.Errorf("sqlite: remove %s: %w", path+suffix, err))
		}
	}
	return errors.Join(failures...)
}

// syncFile flushes path's contents to stable storage.
func syncFile(path string) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// createDirectory creates directory and any missing parents, and syncs the
// parent of each directory it created, so a file installed in it survives a
// power loss with the directories that hold it.
func createDirectory(directory string) error {
	var created []string
	for path := directory; ; path = filepath.Dir(path) {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			break
		}
		created = append(created, path)
		if filepath.Dir(path) == path {
			break
		}
	}
	if err := os.MkdirAll(directory, 0o755); err != nil {
		return err
	}
	for _, path := range created {
		if err := syncDirectory(filepath.Dir(path)); err != nil {
			return err
		}
	}
	return nil
}

// syncDirectory flushes a directory's entries, so a file renamed into or
// removed from it stays so after a power loss. Not done on Windows
// (unsupported, #297).
func syncDirectory(path string) error {
	if goruntime.GOOS == "windows" {
		return nil
	}
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}
