package migration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/well-prado/new-blok/store"
)

// VersionTable stamps each component's schema version (#291). One database
// file holds several components (the journal, audit, the worker queue,
// approvals, cron, provider records, and the application's own tables),
// each opened on its own by whichever packages a binary composes, so one
// number such as PRAGMA user_version cannot describe it: the journal and
// the queue migrate independently, and an application may use user_version
// itself. Each component owns one row.
//
// upgraded_from is the version the database had before the binary that
// wrote version migrated it: 0 when that binary created the component's
// tables, or the version inferred from their shape when they predate the
// stamp.
const VersionTable = "blok_schema_versions"

// Schema describes one component's stamped schema.
type Schema struct {
	// Component names the row, and the refusal.
	Component string
	// Supported is the highest version this binary understands, and the
	// version Apply leaves stamped.
	Supported int
	// Infer classifies a database whose component has no stamp yet: 0 when
	// none of the component's tables exist, otherwise the version their
	// shape shows. It runs inside the schema transaction, before migrate,
	// and must only read.
	Infer func(context.Context, *sql.Tx) (int, error)
}

// Apply runs one component's schema migration inside tx, the component's
// schema transaction, under its version stamp:
//
//  1. The stamp is read, or inferred from the tables' shape when the
//     component has none (a database written before #291).
//  2. A stamp newer than s.Supported is refused with a
//     *store.NewerSchemaError before anything else runs: this binary does
//     not know that shape.
//  3. migrate runs with the version found. Migrations added from now on run
//     only when from is older than their version; the shape-guarded ones
//     from before the stamp run on every open, because a binary from before
//     #291 can still bring back an older shape in a stamped database.
//  4. The stamp is raised to s.Supported in the same transaction, so a
//     crash leaves neither the migration nor the stamp, and the next open
//     does both. A database already at s.Supported is not written.
//
// Apply is idempotent and, like the migrations it runs, retried by Retry
// while concurrent openers race for the write lock.
func Apply(ctx context.Context, tx *sql.Tx, s Schema, migrate func(from int) error) error {
	if s.Component == "" || s.Supported <= 0 || s.Infer == nil || migrate == nil {
		return errors.New("migration: component, supported version, inference and migration are required")
	}
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+VersionTable+` (
		component TEXT PRIMARY KEY,
		version INTEGER NOT NULL CHECK (version > 0),
		upgraded_from INTEGER NOT NULL CHECK (upgraded_from >= 0)
	)`); err != nil {
		return fmt.Errorf("%s schema version: %w", s.Component, err)
	}
	from, stamped, err := Stamped(ctx, tx, s.Component)
	if err != nil {
		return err
	}
	if !stamped {
		if from, err = s.Infer(ctx, tx); err != nil {
			return fmt.Errorf("%s schema version: infer: %w", s.Component, err)
		}
	}
	if from > s.Supported {
		return &store.NewerSchemaError{Component: s.Component, Version: from, Supported: s.Supported}
	}
	if err := migrate(from); err != nil {
		return err
	}
	if stamped && from == s.Supported {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+VersionTable+` (component, version, upgraded_from) VALUES (?, ?, ?)
		ON CONFLICT(component) DO UPDATE SET version = excluded.version, upgraded_from = excluded.upgraded_from`,
		s.Component, s.Supported, from); err != nil {
		return fmt.Errorf("%s schema version: stamp: %w", s.Component, err)
	}
	return nil
}

// Stamped reads a component's stamp. A database without the table, or
// without the component's row, is not stamped.
func Stamped(ctx context.Context, tx *sql.Tx, component string) (version int, stamped bool, err error) {
	exists, err := TableExists(ctx, tx, VersionTable)
	if err != nil {
		return 0, false, fmt.Errorf("%s schema version: %w", component, err)
	}
	if !exists {
		return 0, false, nil
	}
	err = tx.QueryRowContext(ctx, `SELECT version FROM `+VersionTable+` WHERE component = ?`, component).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("%s schema version: %w", component, err)
	}
	return version, true, nil
}

// TableExists reports whether a table exists, for Infer functions.
func TableExists(ctx context.Context, tx *sql.Tx, name string) (bool, error) {
	var found string
	err := tx.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&found)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// ColumnExists reports whether table has column, for Infer functions. A
// missing table has no columns.
func ColumnExists(ctx context.Context, tx *sql.Tx, table, column string) (bool, error) {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, table, column).Scan(&count)
	return count > 0, err
}

// Present infers version 1 when any of tables exists and 0 otherwise, for a
// component whose shape has not changed since it was introduced.
func Present(tables ...string) func(context.Context, *sql.Tx) (int, error) {
	return func(ctx context.Context, tx *sql.Tx) (int, error) {
		for _, table := range tables {
			found, err := TableExists(ctx, tx, table)
			if err != nil {
				return 0, err
			}
			if found {
				return 1, nil
			}
		}
		return 0, nil
	}
}
