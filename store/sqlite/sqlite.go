// Package sqlite is the selected embedded durable backend.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"

	"github.com/well-prado/new-blok/store"
	driver "modernc.org/sqlite"
)

// sqliteBusy is SQLITE_BUSY; extended codes carry it in their low byte.
const sqliteBusy = 5

// busy marks an error caused by SQLITE_BUSY as store.ErrBusy, so callers can
// tell a saturated store from a failure.
func busy(err error) error {
	var failure *driver.Error
	if err != nil && errors.As(err, &failure) && failure.Code()&0xff == sqliteBusy && !errors.Is(err, store.ErrBusy) {
		return fmt.Errorf("%w: %w", store.ErrBusy, err)
	}
	return err
}

const (
	journalMode = "WAL"
	synchronous = "FULL"
	busyTimeout = 5000
)

// dsn uses one named shared-cache URI for every :memory: open, so each handle
// participates in the same SQLite writer domain.
var sharedMemoryWriteDomain = store.NewWriteDomain()

// Backend opens SQLite databases with the durability settings required by the
// journal. It is the only package that imports the SQLite driver.
type Backend struct{}

var _ store.Backend = Backend{}

func (Backend) Open(ctx context.Context, path string) (store.Database, error) {
	if path == "" {
		return nil, errors.New("sqlite: database path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("sqlite: create parent: %w", err)
		}
	}
	database, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	database.SetMaxOpenConns(8)
	database.SetMaxIdleConns(8)
	if err := configure(ctx, database); err != nil {
		_ = database.Close()
		return nil, err
	}
	writeDomain := sharedMemoryWriteDomain
	if path != ":memory:" {
		file, err := os.Stat(path)
		if err != nil {
			_ = database.Close()
			return nil, fmt.Errorf("sqlite: identify write domain: %w", err)
		}
		writeDomain = store.NewFileWriteDomain(file)
	}
	return &connection{database: database, writeDomain: writeDomain}, nil
}

func configure(ctx context.Context, database *sql.DB) error {
	settings := []string{
		"PRAGMA journal_mode=" + journalMode,
		"PRAGMA synchronous=" + synchronous,
		fmt.Sprintf("PRAGMA busy_timeout=%d", busyTimeout),
		"PRAGMA foreign_keys=ON",
	}
	for _, statement := range settings {
		if _, err := database.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("sqlite: configure %q: %w", statement, err)
		}
	}
	return nil
}

func dsn(path string) string {
	if path == ":memory:" {
		return "file::memory:?cache=shared&_busy_timeout=5000&_journal_mode=MEMORY&_synchronous=FULL&_foreign_keys=ON"
	}
	return (&url.URL{
		Scheme:   "file",
		Path:     uriPath(path),
		RawQuery: "_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL&_foreign_keys=ON",
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
	database     *sql.DB
	writeDomain  *store.WriteDomain
	beforeCommit func()
	afterCommit  func()
}

func (c *connection) WriteDomain() *store.WriteDomain { return c.writeDomain }

func (c *connection) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if fn == nil {
		return errors.New("sqlite: transaction callback is required")
	}
	tx, err := c.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return busy(fmt.Errorf("sqlite: begin: %w", err))
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return busy(err)
	}
	if c.beforeCommit != nil {
		c.beforeCommit()
	}
	if err := tx.Commit(); err != nil {
		return busy(fmt.Errorf("sqlite: commit: %w", err))
	}
	if c.afterCommit != nil {
		c.afterCommit()
	}
	return nil
}

func (c *connection) Backup(ctx context.Context, destination string) error {
	if destination == "" || destination == ":memory:" {
		return errors.New("sqlite: backup destination is required")
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return fmt.Errorf("sqlite: create backup parent: %w", err)
	}
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("sqlite: backup destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("sqlite: inspect backup destination: %w", err)
	}
	if _, err := c.database.ExecContext(ctx, "VACUUM INTO ?", destination); err != nil {
		return fmt.Errorf("sqlite: backup: %w", err)
	}
	return nil
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

// Restore copies an integrity-checked SQLite backup into a new destination.
// The destination is never overwritten; an interrupted copy leaves only its
// temporary file and cannot replace a usable database.
func (Backend) Restore(ctx context.Context, source, destination string) error {
	if source == "" || destination == "" || source == ":memory:" || destination == ":memory:" || source == destination {
		return errors.New("sqlite: distinct source and destination paths are required")
	}
	if _, err := os.Stat(source); err != nil {
		return fmt.Errorf("sqlite: inspect source: %w", err)
	}
	if _, err := os.Stat(destination); err == nil {
		return fmt.Errorf("sqlite: restore destination already exists: %s", destination)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("sqlite: inspect destination: %w", err)
	}
	sourceDB, err := (Backend{}).Open(ctx, source)
	if err != nil {
		return fmt.Errorf("sqlite: open source: %w", err)
	}
	if err := sourceDB.Integrity(ctx); err != nil {
		_ = sourceDB.Close()
		return fmt.Errorf("sqlite: source integrity: %w", err)
	}
	if err := sourceDB.Close(); err != nil {
		return fmt.Errorf("sqlite: close source: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return fmt.Errorf("sqlite: create destination parent: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(destination), ".restore-*")
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
	if err := os.Rename(temporaryPath, destination); err != nil {
		return fmt.Errorf("sqlite: install restore: %w", err)
	}
	restored, err := (Backend{}).Open(ctx, destination)
	if err != nil {
		return fmt.Errorf("sqlite: open restored database: %w", err)
	}
	defer restored.Close()
	if err := restored.Integrity(ctx); err != nil {
		return fmt.Errorf("sqlite: restored integrity: %w", err)
	}
	return nil
}
