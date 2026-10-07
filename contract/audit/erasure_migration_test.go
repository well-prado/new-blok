package audit_test

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/internal/migration"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// legacyFixture is a journal written by origin/main at ea3eec6, before
// #281 (testdata/restore/erasure-281/generate.go): one run compacted there,
// whose tombstone kept its output and request key, and one reconciled run
// main could not compact.
const legacyFixture = "../../testdata/restore/erasure-281/legacy-main-ea3eec6.db.gz"

var (
	legacyCompacted  = []string{"SYNTHETIC-281-LEGACY-OUTPUT", "SYNTHETIC-281-LEGACY-REQUEST"}
	legacyReconciled = []string{"SYNTHETIC-281-LEGACY-EVIDENCE", "SYNTHETIC-281-LEGACY-RESULT", "SYNTHETIC-281-LEGACY-RECONCILED-OUTPUT"}
	legacyClock      = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
)

func legacyDatabase(t *testing.T) string {
	t.Helper()
	compressed, err := os.Open(legacyFixture)
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	reader, err := gzip.NewReader(compressed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy.db")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(file, reader); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

// snapshot digests every table's schema and rows, for idempotence checks.
func (r *rig) snapshot() string {
	r.t.Helper()
	hash := sha256.New()
	err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(r.ctx, `SELECT name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name`)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var name string
			var statement sql.NullString
			if err := rows.Scan(&name, &statement); err != nil {
				rows.Close()
				return err
			}
			io.WriteString(hash, name+"\x00"+statement.String+"\x00")
			if strings.HasPrefix(statement.String, "CREATE TABLE") {
				tables = append(tables, name)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, table := range tables {
			rows, err := tx.QueryContext(r.ctx, `SELECT * FROM "`+table+`" ORDER BY 1`)
			if err != nil {
				return err
			}
			columns, _ := rows.Columns()
			for rows.Next() {
				values := make([]any, len(columns))
				pointers := make([]any, len(columns))
				for i := range values {
					pointers[i] = &values[i]
				}
				if err := rows.Scan(pointers...); err != nil {
					rows.Close()
					return err
				}
				for _, value := range values {
					io.WriteString(hash, table+"\x00")
					switch v := value.(type) {
					case []byte:
						hash.Write(v)
					case nil:
						io.WriteString(hash, "\x01null")
					default:
						io.WriteString(hash, strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(sqlText(v), "\n", " "), "\t", " ")))
					}
				}
			}
			if err := rows.Close(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func sqlText(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case int64:
		return time.Unix(0, v).UTC().Format(time.RFC3339Nano)
	default:
		return ""
	}
}

func rowsHold(found []string, wanted string) bool {
	for _, f := range found {
		if strings.HasPrefix(f, wanted) {
			return true
		}
	}
	return false
}

// TestMigrationScrubsLegacyTombstones opens a journal origin/main wrote.
// The legacy tombstone's output and request key are scrubbed on open; the
// reconciled run, still within its retention, keeps its evidence until it is
// compacted, which no longer fails; and the audit cross-check passes before
// and after.
func TestMigrationScrubsLegacyTombstones(t *testing.T) {
	path := legacyDatabase(t)
	if found := fileMarkers(t, path, append(legacyCompacted, legacyReconciled...)); len(found) != len(legacyCompacted)+len(legacyReconciled) {
		t.Fatalf("fixture: legacy markers found=%v", found)
	}
	r := openRig(t, path, rigOptions{})
	r.clock = legacyClock
	found := r.rowMarkers()
	for _, m := range legacyCompacted {
		if strings.Contains(strings.Join(r.rowValues(), "\n"), m) {
			t.Fatalf("legacy tombstone content %s survived the migration: %v", m, found)
		}
	}
	if n := r.count(`SELECT COUNT(*) FROM sqlite_master WHERE name = 'journal_audit'`); n != 0 {
		t.Fatal("legacy tombstone table survived the migration")
	}
	if !rowsHold(found, "journal_reconciliations.evidence") {
		t.Fatalf("the reconciled run's evidence was erased before its retention: %v", found)
	}
	r.mustVerify(2)
	r.clock = legacyClock.Add(48 * time.Hour)
	if report, err := r.journal.Compact(r.ctx, legacyClock.Add(24*time.Hour)); err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compact the legacy reconciled run=%+v err=%v", report, err)
	}
	if found := r.rowMarkers(); len(found) != 0 {
		t.Fatalf("content in rows after migration and compaction: %v", found)
	}
	r.mustVerify(2)
}

// rowValues returns every text and blob value in the database.
func (r *rig) rowValues() []string {
	r.t.Helper()
	var values []string
	err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(r.ctx, `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				rows.Close()
				return err
			}
			tables = append(tables, name)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, table := range tables {
			rows, err := tx.QueryContext(r.ctx, `SELECT * FROM "`+table+`"`)
			if err != nil {
				return err
			}
			columns, _ := rows.Columns()
			for rows.Next() {
				cells := make([]any, len(columns))
				pointers := make([]any, len(columns))
				for i := range cells {
					pointers[i] = &cells[i]
				}
				if err := rows.Scan(pointers...); err != nil {
					rows.Close()
					return err
				}
				for _, cell := range cells {
					switch v := cell.(type) {
					case string:
						values = append(values, v)
					case []byte:
						values = append(values, string(v))
					}
				}
			}
			if err := rows.Close(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		r.t.Fatal(err)
	}
	return values
}

// TestMigrationIsIdempotent: reopening a migrated journal changes nothing.
func TestMigrationIsIdempotent(t *testing.T) {
	path := legacyDatabase(t)
	first := openRig(t, path, rigOptions{})
	migrated := first.snapshot()
	second := openRig(t, path, rigOptions{})
	if again := second.snapshot(); again != migrated {
		t.Fatal("reopening a migrated journal changed it")
	}
	second.mustVerify(2)
}

// TestMigrationSurvivesACrash kills a process inside the migration's
// transaction, just before it commits. The journal reopens with its legacy
// rows intact and passes the integrity check, then migrates on the next open.
func TestMigrationSurvivesACrash(t *testing.T) {
	if path := os.Getenv("NEWBLOK_281_MIGRATION_CHILD"); path != "" {
		ctx := context.Background()
		db, err := (sqlite.Backend{}).Open(ctx, path)
		if err != nil {
			os.Exit(2)
		}
		log, err := audit.NewJournal(ctx, db, audit.Config{MaxRecords: 1000, Readers: tenantReaders{}})
		if err != nil {
			os.Exit(2)
		}
		_, _ = journal.New(ctx, db, journal.Config{Audit: log, Hooks: journal.Hooks{BeforeCommit: func(name string) {
			if name == "schema" {
				os.Exit(7)
			}
		}}})
		os.Exit(3)
	}
	path := legacyDatabase(t)
	command := exec.Command(os.Args[0], "-test.run=^TestMigrationSurvivesACrash$")
	command.Env = append(os.Environ(), "NEWBLOK_281_MIGRATION_CHILD="+path)
	err := command.Run()
	if exitErr, ok := err.(*exec.ExitError); !ok || exitErr.ExitCode() != 7 {
		t.Fatalf("child did not die inside the migration transaction: %v", err)
	}
	db, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Integrity(context.Background()); err != nil {
		t.Fatal(err)
	}
	var legacy int
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM journal_audit WHERE output_json IS NOT NULL`).Scan(&legacy)
	}); err != nil || legacy != 1 {
		t.Fatalf("crashed migration was not rolled back: legacy tombstones=%d err=%v", legacy, err)
	}
	// The stamp is written in the migration's transaction (#291), so the
	// crash left none.
	if version, stamped := journalStamp(t, db); stamped {
		t.Fatalf("the crashed migration left journal stamp %d", version)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	r := openRig(t, path, rigOptions{})
	if n := r.count(`SELECT COUNT(*) FROM sqlite_master WHERE name = 'journal_audit'`); n != 0 {
		t.Fatal("the reopened journal did not migrate")
	}
	if version, stamped := journalStamp(t, r.db); !stamped || version != 6 {
		t.Fatalf("the reopened journal is stamped %d (%v); want 6", version, stamped)
	}
	r.mustVerify(2)
}

func journalStamp(t *testing.T, db store.Database) (version int, stamped bool) {
	t.Helper()
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		version, stamped, err = migration.Stamped(context.Background(), tx, "journal")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return version, stamped
}
