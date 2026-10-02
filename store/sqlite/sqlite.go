// Package sqlite is the selected embedded durable backend.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/well-prado/new-blok/store"
	_ "modernc.org/sqlite"
)

const (
	journalMode = "WAL"
	synchronous = "FULL"
	busyTimeout = 5000
)

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
	return &connection{database: database}, nil
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
		Path:     path,
		RawQuery: "_busy_timeout=5000&_journal_mode=WAL&_synchronous=FULL&_foreign_keys=ON",
	}).String()
}

type connection struct {
	database     *sql.DB
	beforeCommit func()
	afterCommit  func()
}

func (c *connection) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	if fn == nil {
		return errors.New("sqlite: transaction callback is required")
	}
	tx, err := c.database.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("sqlite: begin: %w", err)
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	if c.beforeCommit != nil {
		c.beforeCommit()
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit: %w", err)
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
