package migration_test

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/internal/migration"
	"github.com/well-prado/new-blok/provider"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	"github.com/well-prado/new-blok/trigger/worker"
)

// The databases origin/main wrote before #291, none of them stamped.
const (
	// Every component, at ef330a3: journal 3, queue 2 with a tombstone.
	mainFixture = "../../testdata/restore/schema-version-291/legacy-main-ef330a3.db.gz"
	// A queue at 8633027, before #290: queue 1.
	queueFixture = "../../testdata/restore/schema-version-291/legacy-queue-8633027.db.gz"
	// A journal at ea3eec6, before #281: journal 1.
	erasureFixture = "../../testdata/restore/erasure-281/legacy-main-ea3eec6.db.gz"
	// A journal at 99a9228, after #281 and before #286: journal 2.
	tenantFixture = "../../testdata/restore/reconcile-tenant-286/legacy-main-99a9228.db.gz"
)

type readers struct{}

func (readers) AuthorizeAuditRead(context.Context, string) error { return nil }

type reviewer struct{}

func (reviewer) AuthorizeReview(context.Context, approval.Proposal, []string) (string, error) {
	return "reviewer:carol", nil
}

type submitter struct{}

func (submitter) Submit(context.Context, trigger.Submission) (bool, error) { return true, nil }

type component struct {
	name    string
	version int
	open    func(context.Context, store.Database) error
}

// components is every component the framework migrates in place, with the
// schema version this binary supports, opened as an application composes
// it. Approval needs audit on the same database, so it opens audit first.
var components = []component{
	{"journal", 5, func(ctx context.Context, db store.Database) error {
		_, err := journal.New(ctx, db, journal.Config{})
		return err
	}},
	{"audit", 1, func(ctx context.Context, db store.Database) error {
		_, err := openAudit(ctx, db)
		return err
	}},
	{"worker", 2, func(ctx context.Context, db store.Database) error {
		_, err := worker.New(ctx, db, nil)
		return err
	}},
	{"approval", 1, func(ctx context.Context, db store.Database) error {
		log, err := openAudit(ctx, db)
		if err != nil {
			return err
		}
		_, err = approval.NewJournalStore(ctx, db, approval.Config{Authorizer: reviewer{}, Audit: log, MaxDecisions: 100})
		return err
	}},
	{"cron", 1, func(ctx context.Context, db store.Database) error {
		_, err := cron.New(ctx, db, submitter{}, nil, nil)
		return err
	}},
	{"provider", 1, func(ctx context.Context, db store.Database) error {
		_, err := provider.NewRecords(ctx, db)
		return err
	}},
}

func openAudit(ctx context.Context, db store.Database) (*audit.Journal, error) {
	return audit.NewJournal(ctx, db, audit.Config{MaxRecords: 1000, Readers: readers{}})
}

func open(t *testing.T, path string) store.Database {
	t.Helper()
	db, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func fixture(t *testing.T, compressed string) string {
	t.Helper()
	in, err := os.Open(compressed)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	reader, err := gzip.NewReader(in)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy.db")
	out, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, reader); err != nil {
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

type stamp struct{ version, from int }

func stampOf(t *testing.T, db store.Database, name string) (stamp, bool) {
	t.Helper()
	var s stamp
	found := false
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		var err error
		if s.version, found, err = migration.Stamped(context.Background(), tx, name); err != nil || !found {
			return err
		}
		return tx.QueryRow(`SELECT upgraded_from FROM `+migration.VersionTable+` WHERE component = ?`, name).Scan(&s.from)
	}); err != nil {
		t.Fatal(err)
	}
	return s, found
}

func setStamp(t *testing.T, db store.Database, name string, version int) {
	t.Helper()
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE `+migration.VersionTable+` SET version = ? WHERE component = ?`, version, name)
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

// snapshot digests every schema object and row, so a refused or repeated
// open can be shown to have written nothing.
func snapshot(t *testing.T, db store.Database) string {
	t.Helper()
	hash := sha256.New()
	if err := db.WithTx(context.Background(), func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT type, name, COALESCE(sql, '') FROM sqlite_master ORDER BY name`)
		if err != nil {
			return err
		}
		var tables []string
		for rows.Next() {
			var kind, name, statement string
			if err := rows.Scan(&kind, &name, &statement); err != nil {
				rows.Close()
				return err
			}
			fmt.Fprintf(hash, "%s\x00%s\x00%s\x00", kind, name, statement)
			if kind == "table" {
				tables = append(tables, name)
			}
		}
		if err := rows.Close(); err != nil {
			return err
		}
		for _, table := range tables {
			rows, err := tx.Query(`SELECT * FROM "` + table + `" ORDER BY 1`)
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
				fmt.Fprintf(hash, "%s\x00%v\x00", table, cells)
			}
			if err := rows.Close(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

// TestEveryComponentStampsItsSchema: opening a fresh database stamps each
// component with the version this binary supports, as created (upgraded
// from 0), and reopening writes nothing.
func TestEveryComponentStampsItsSchema(t *testing.T) {
	ctx := context.Background()
	for _, c := range components {
		t.Run(c.name, func(t *testing.T) {
			db := open(t, filepath.Join(t.TempDir(), "fresh.db"))
			if err := c.open(ctx, db); err != nil {
				t.Fatal(err)
			}
			if s, found := stampOf(t, db, c.name); !found || s != (stamp{version: c.version, from: 0}) {
				t.Fatalf("stamp=%+v found=%v; want version %d created fresh", s, found, c.version)
			}
			before := snapshot(t, db)
			if err := c.open(ctx, db); err != nil {
				t.Fatalf("the same binary reopening: %v", err)
			}
			if snapshot(t, db) != before {
				t.Fatal("reopening changed the database")
			}
		})
	}
}

// TestEveryComponentRefusesANewerSchema: a database a newer binary migrated
// (stamped one past this binary's version) is refused at open, naming the
// component and both versions, and the refused open writes nothing. On
// origin/main nothing is refused.
func TestEveryComponentRefusesANewerSchema(t *testing.T) {
	ctx := context.Background()
	for _, c := range components {
		t.Run(c.name, func(t *testing.T) {
			db := open(t, filepath.Join(t.TempDir(), "newer.db"))
			if err := c.open(ctx, db); err != nil {
				t.Fatal(err)
			}
			setStamp(t, db, c.name, c.version+1)
			before := snapshot(t, db)
			for range 2 {
				err := c.open(ctx, db)
				var newer *store.NewerSchemaError
				if !errors.Is(err, store.ErrNewerSchema) || !errors.As(err, &newer) {
					t.Fatalf("err=%v; want a refusal", err)
				}
				if want := (store.NewerSchemaError{Component: c.name, Version: c.version + 1, Supported: c.version}); *newer != want {
					t.Fatalf("refusal=%+v; want %+v", *newer, want)
				}
				for _, part := range []string{c.name, fmt.Sprintf("version %d", c.version+1), fmt.Sprintf("version %d", c.version)} {
					if !strings.Contains(err.Error(), part) {
						t.Fatalf("refusal %q does not name %q", err, part)
					}
				}
			}
			if snapshot(t, db) != before {
				t.Fatal("a refused open changed the database")
			}
		})
	}
}

// TestEveryComponentMigratesAnOlderStampForward: a database stamped by an
// older binary is migrated in place and its stamp raised; the binary that
// is newer than the database opens it. Components still at version 1 have
// no older stamp; their unstamped databases are migrated below.
func TestEveryComponentMigratesAnOlderStampForward(t *testing.T) {
	ctx := context.Background()
	for _, c := range components {
		if c.version == 1 {
			continue
		}
		t.Run(c.name, func(t *testing.T) {
			db := open(t, filepath.Join(t.TempDir(), "older.db"))
			if err := c.open(ctx, db); err != nil {
				t.Fatal(err)
			}
			setStamp(t, db, c.name, c.version-1)
			if err := c.open(ctx, db); err != nil {
				t.Fatal(err)
			}
			if s, _ := stampOf(t, db, c.name); s != (stamp{version: c.version, from: c.version - 1}) {
				t.Fatalf("stamp=%+v; want %d upgraded from %d", s, c.version, c.version-1)
			}
		})
	}
}

// TestOriginMainDatabaseIsClassifiedAndStamped opens the database
// origin/main wrote at ef330a3, where nothing is stamped. Each component
// is classified by its tables' shape, migrated, and stamped in the same
// transaction; its data still works; and reopening changes nothing. The
// journal there predates #332's wait identity, so it is classified 3.
func TestOriginMainDatabaseIsClassifiedAndStamped(t *testing.T) {
	ctx := context.Background()
	db := open(t, fixture(t, mainFixture))
	for _, c := range components {
		if _, found := stampOf(t, db, c.name); found {
			t.Fatalf("fixture: %s is already stamped", c.name)
		}
		if err := c.open(ctx, db); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		// ef330a3 wrote every other component at the version this binary
		// supports, so each is classified as that version.
		from := c.version
		if c.name == "journal" {
			from = 3
		}
		if s, found := stampOf(t, db, c.name); !found || s != (stamp{version: c.version, from: from}) {
			t.Fatalf("%s stamp=%+v found=%v; want %d upgraded from %d, classified from its shape", c.name, s, found, c.version, from)
		}
	}
	// The queue's tombstone still deduplicates the compacted job (#290).
	queue, err := worker.New(ctx, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "job-compacted", Kind: "report", Payload: json.RawMessage(`{"marker":"SYNTHETIC-291-COMPACTED"}`), MaxAttempts: 3})
	if err != nil || duplicate.Accepted || !duplicate.Job.Compacted {
		t.Fatalf("duplicate of the compacted job=%+v err=%v; want refused by its tombstone", duplicate, err)
	}
	// The reconciliation keeps the tenant that decided it (#286).
	var tenant sql.NullString
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRow(`SELECT tenant FROM journal_reconciliations`).Scan(&tenant)
	}); err != nil || tenant.String != "tenant-a" {
		t.Fatalf("reconciliation tenant=%v err=%v", tenant, err)
	}
	before := snapshot(t, db)
	for _, c := range components {
		if err := c.open(ctx, db); err != nil {
			t.Fatal(err)
		}
	}
	if snapshot(t, db) != before {
		t.Fatal("reopening the migrated database changed it")
	}
}

// TestOlderUnstampedShapesAreClassified: databases origin/main wrote
// before #281, before #286 and before #290 are classified by their shape,
// migrated forward, and stamped as upgraded from that version.
func TestOlderUnstampedShapesAreClassified(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		fixture string
		open    component
		from    int
	}{
		{erasureFixture, components[0], 1},
		{tenantFixture, components[0], 2},
		{queueFixture, components[2], 1},
	} {
		t.Run(filepath.Base(c.fixture), func(t *testing.T) {
			db := open(t, fixture(t, c.fixture))
			if err := c.open.open(ctx, db); err != nil {
				t.Fatal(err)
			}
			if s, _ := stampOf(t, db, c.open.name); s != (stamp{version: c.open.version, from: c.from}) {
				t.Fatalf("%s stamp=%+v; want %d upgraded from %d", c.open.name, s, c.open.version, c.from)
			}
		})
	}
}

// TestConcurrentOpensOfAnUnstampedDatabase: the first open of an existing
// database reads before it writes its stamp, so several processes doing it
// at once race for the write lock; each component's schema transaction is
// retried while it loses, and every open starts (#235).
func TestConcurrentOpensOfAnUnstampedDatabase(t *testing.T) {
	ctx := context.Background()
	const handles, rounds = 8, 10
	for _, c := range components {
		t.Run(c.name, func(t *testing.T) {
			for range rounds {
				path := fixture(t, mainFixture)
				databases := make([]store.Database, handles)
				for i := range databases {
					databases[i] = open(t, path)
				}
				start := make(chan struct{})
				errs := make(chan error, handles)
				var group sync.WaitGroup
				for _, db := range databases {
					group.Go(func() {
						<-start
						errs <- c.open(ctx, db)
					})
				}
				close(start)
				group.Wait()
				close(errs)
				for err := range errs {
					if err != nil {
						t.Fatalf("a concurrent first open failed: %v", err)
					}
				}
			}
		})
	}
}
