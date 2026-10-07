package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// storeFixtures is testdata/store/fixtures.json (#43, #339): the settings,
// acknowledgment barrier, crash cases, corruption case and effect counts
// the selected backend must show. Every value is checked against a real
// SQLite file by the tests below.
type storeFixtures struct {
	SchemaVersion  string                `json:"schemaVersion"`
	Backend        string                `json:"backend"`
	Durability     fixtureDurability     `json:"durability"`
	Acknowledgment fixtureAcknowledgment `json:"acknowledgment"`
	CrashCases     []fixtureCrashCase    `json:"crashCases"`
	CorruptionCase fixtureCorruption     `json:"corruptionCase"`
	Effects        fixtureEffects        `json:"effects"`
	Limits         []string              `json:"limits"`
}

type fixtureDurability struct {
	PooledConnections int    `json:"pooledConnections"`
	JournalMode       string `json:"journalMode"`
	Synchronous       string `json:"synchronous"`
	BusyTimeoutMillis int64  `json:"busyTimeoutMillis"`
	ForeignKeys       bool   `json:"foreignKeys"`
	SecureDelete      bool   `json:"secureDelete"`
}

type fixtureAcknowledgment struct {
	RowsVisibleBeforeCommit            int    `json:"rowsVisibleBeforeCommit"`
	RowsVisibleAfterCommitBeforeReturn int    `json:"rowsVisibleAfterCommitBeforeReturn"`
	FailedCommitAcknowledged           bool   `json:"failedCommitAcknowledged"`
	RowsAfterFailedCommit              int    `json:"rowsAfterFailedCommit"`
	WriteAfterFailedCommit             string `json:"writeAfterFailedCommit"`
}

type fixtureCrashCase struct {
	Phase             string `json:"phase"`
	KillPoint         string `json:"killPoint"`
	ExpectedRows      []int  `json:"expectedRows"`
	ExpectedIntegrity string `json:"expectedIntegrity"`
}

type fixtureCorruption struct {
	Action   string `json:"action"`
	Expected string `json:"expected"`
}

type fixtureEffects struct {
	SuccessfulEffects        int `json:"successfulEffects"`
	FailedEffects            int `json:"failedEffects"`
	DuplicateAcknowledgments int `json:"duplicateAcknowledgments"`
}

// storeFixtureCrashPhases are the crash cases the file must declare, in
// order: a case dropped from the file, or one added without a child phase
// to run it, fails instead of going unrun.
var storeFixtureCrashPhases = []string{"before", "during", "after"}

const storeFixtureSchema = "store/v2"

func loadStoreFixtures(t *testing.T) storeFixtures {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "store", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	fixtures, err := decodeStoreFixtures(raw)
	if err != nil {
		t.Fatalf("store fixtures: %v", err)
	}
	return fixtures
}

// decodeStoreFixtures reads the fixture strictly: no unknown field, no
// missing one (the decoded value must encode back to the same document, so
// an expectation dropped from the file is not read as its zero value), no
// content after it, the known schema, and exactly the declared crash cases.
func decodeStoreFixtures(raw []byte) (storeFixtures, error) {
	var fixtures storeFixtures
	if err := strictDecode(raw, &fixtures); err != nil {
		return storeFixtures{}, err
	}
	if fixtures.SchemaVersion != storeFixtureSchema {
		return storeFixtures{}, fmt.Errorf("schema %q, want %s", fixtures.SchemaVersion, storeFixtureSchema)
	}
	if fixtures.Backend != "sqlite" {
		return storeFixtures{}, fmt.Errorf("backend %q; these tests run the sqlite backend", fixtures.Backend)
	}
	var phases []string
	for _, crash := range fixtures.CrashCases {
		phases = append(phases, crash.Phase)
	}
	if !slices.Equal(phases, storeFixtureCrashPhases) {
		return storeFixtures{}, fmt.Errorf("crash cases %q, want %q", phases, storeFixtureCrashPhases)
	}
	return fixtures, nil
}

// strictDecode decodes raw into value, refusing unknown fields and content
// after the value, then requires value to encode back to raw's document,
// which fails for a field raw leaves out.
func strictDecode(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return fmt.Errorf("content after the fixture object (%v)", err)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	var want, got any
	if err := json.Unmarshal(raw, &want); err != nil {
		return err
	}
	if err := json.Unmarshal(encoded, &got); err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("the file and the fields the tests read differ (a field is missing or null):\nfile:  %s\nread:  %s", raw, encoded)
	}
	return nil
}

// TestStoreFixtureDecodingIsStrict proves the loader's guards can fail:
// each mutation of today's fixture must be refused.
func TestStoreFixtureDecodingIsStrict(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "store", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeStoreFixtures(raw); err != nil {
		t.Fatalf("today's fixture: %v", err)
	}
	for _, mutation := range []struct{ name, old, new, want string }{
		{"trailing content", "\n}\n", "\n}\n{}\n", "content after"},
		{"unknown field", `"backend": "sqlite",`, `"backend": "sqlite", "extra": 1,`, "unknown field"},
		{"missing expectation", `"failedEffects": 0,`, ``, "a field is missing"},
		{"dropped crash case", `{"phase": "during", "killPoint": "commit-boundary race after marker and before process exit", "expectedRows": [0, 1], "expectedIntegrity": "ok"},`, ``, "crash cases"},
		{"unrun crash case", `"phase": "after"`, `"phase": "afterwards"`, "crash cases"},
		{"schema", `"store/v2"`, `"store/v1"`, "schema"},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			mutated := strings.Replace(string(raw), mutation.old, mutation.new, 1)
			if mutated == string(raw) {
				t.Fatalf("mutation %q did not apply to the fixture", mutation.old)
			}
			if _, err := decodeStoreFixtures([]byte(mutated)); err == nil || !strings.Contains(err.Error(), mutation.want) {
				t.Fatalf("mutated fixture: err=%v, want one containing %q", err, mutation.want)
			}
		})
	}
}

// TestStoreFixtureDurability reads the fixture's settings back from every
// pooled connection of a store opened with the default Backend, held at
// once.
func TestStoreFixtureDurability(t *testing.T) {
	want := loadStoreFixtures(t).Durability
	ctx := context.Background()
	database, err := (Backend{}).Open(ctx, filepath.Join(t.TempDir(), "durability.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pool := database.(*connection).database
	held := make([]*sql.Conn, 0, pool.Stats().MaxOpenConnections)
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	for range pool.Stats().MaxOpenConnections {
		conn, err := pool.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	if inUse := pool.Stats().InUse; inUse != len(held) {
		t.Fatalf("%d connections in use; want %d distinct ones held at once", inUse, len(held))
	}
	synchronousNames := map[string]string{"0": "OFF", "1": "NORMAL", "2": "FULL", "3": "EXTRA"}
	for index, conn := range held {
		read := func(pragma string) string {
			t.Helper()
			var value string
			if err := conn.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&value); err != nil {
				t.Fatalf("connection %d: PRAGMA %s: %v", index, pragma, err)
			}
			return value
		}
		var timeout int64
		if _, err := fmt.Sscan(read("busy_timeout"), &timeout); err != nil {
			t.Fatal(err)
		}
		got := fixtureDurability{
			PooledConnections: len(held),
			JournalMode:       strings.ToUpper(read("journal_mode")),
			Synchronous:       synchronousNames[read("synchronous")],
			BusyTimeoutMillis: timeout,
			ForeignKeys:       read("foreign_keys") == "1",
			SecureDelete:      read("secure_delete") == "1",
		}
		if got != want {
			t.Fatalf("connection %d: %+v, fixture expects %+v", index, got, want)
		}
	}
}

// TestStoreFixtureAcknowledgment checks the barrier: WithTx returns nil
// only once COMMIT succeeded. Another handle sees nothing at the commit
// hook before COMMIT and the row at the hook after it, before WithTx
// returns; a COMMIT that fails (a deferred foreign key) is not
// acknowledged, leaves nothing, and leaves the store writable.
func TestStoreFixtureAcknowledgment(t *testing.T) {
	want := loadStoreFixtures(t).Acknowledgment
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "acknowledgment.db")
	database, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	reader, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE parents (id INTEGER PRIMARY KEY);
			CREATE TABLE entries (id INTEGER PRIMARY KEY, parent INTEGER REFERENCES parents(id) DEFERRABLE INITIALLY DEFERRED, value TEXT NOT NULL)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	count := func(value string) int {
		t.Helper()
		n := -1
		if err := reader.WithTx(ctx, func(tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM entries WHERE value = ?`, value).Scan(&n)
		}); err != nil {
			t.Fatal(err)
		}
		return n
	}
	var got fixtureAcknowledgment
	conn := database.(*connection)
	conn.beforeCommit = func() { got.RowsVisibleBeforeCommit = count("acknowledged") }
	conn.afterCommit = func() { got.RowsVisibleAfterCommitBeforeReturn = count("acknowledged") }
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO entries (value) VALUES ('acknowledged')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	conn.beforeCommit, conn.afterCommit = nil, nil

	failed := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO entries (parent, value) VALUES (404, 'refused')`)
		return err
	})
	t.Logf("commit with a deferred foreign key violation: %v", failed)
	if failed != nil && !strings.Contains(failed.Error(), "sqlite: commit") {
		t.Fatalf("the violation failed before COMMIT, so the test does not reach the barrier: %v", failed)
	}
	got.FailedCommitAcknowledged = failed == nil
	got.RowsAfterFailedCommit = count("refused")
	got.WriteAfterFailedCommit = "ok"
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO entries (value) VALUES ('next')`)
		return err
	}); err != nil {
		got.WriteAfterFailedCommit = err.Error()
	}
	if got != want {
		t.Fatalf("observed %+v, fixture expects %+v", got, want)
	}
}

// TestStoreFixtureCrashCases kills a real child process at each declared
// phase (runCrashChild) and checks the recovered row count and integrity.
func TestStoreFixtureCrashCases(t *testing.T) {
	for _, crash := range loadStoreFixtures(t).CrashCases {
		t.Run(crash.Phase, func(t *testing.T) {
			directory := t.TempDir()
			databasePath := filepath.Join(directory, "journal.db")
			markerPath := filepath.Join(directory, "marker")
			command := exec.Command(os.Args[0], "-test.run=^TestProcessKillBeforeDuringAndAfterCommit$")
			command.Env = append(os.Environ(),
				"NEWBLOK_SQLITE_CHILD=1",
				"NEWBLOK_SQLITE_PHASE="+crash.Phase,
				"NEWBLOK_SQLITE_PATH="+databasePath,
				"NEWBLOK_SQLITE_MARKER="+markerPath,
			)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForMarker(t, markerPath)
			// The "during" child commits and exits on its own, so it may be
			// gone before the kill.
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
			integrity := "ok"
			if err := database.Integrity(context.Background()); err != nil {
				integrity = err.Error()
			}
			rows := -1
			if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
				return tx.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM entries").Scan(&rows)
			}); err != nil {
				t.Fatal(err)
			}
			t.Logf("phase %s: %d rows, integrity %s", crash.Phase, rows, integrity)
			if !slices.Contains(crash.ExpectedRows, rows) || integrity != crash.ExpectedIntegrity {
				t.Fatalf("recovered %d rows with integrity %q; fixture expects rows in %v and integrity %q", rows, integrity, crash.ExpectedRows, crash.ExpectedIntegrity)
			}
		})
	}
}

// TestStoreFixtureCorruption damages a closed database as the fixture's
// action names, and requires opening it or checking it to fail.
func TestStoreFixtureCorruption(t *testing.T) {
	corruption := loadStoreFixtures(t).CorruptionCase
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "corrupt.db")
	database, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT NOT NULL)"); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, "INSERT INTO entries (value) VALUES (?)", strings.Repeat("x", 8<<10))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	switch corruption.Action {
	case "truncate-half":
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, contents[:len(contents)/2], 0o600); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("corruption action %q has no implementation here", corruption.Action)
	}
	outcome := "opened_and_passed_integrity"
	corrupted, err := (Backend{}).Open(ctx, path)
	if err != nil {
		outcome = "fails_closed"
		t.Logf("open refused the damaged file: %v", err)
	} else {
		defer corrupted.Close()
		if err := corrupted.Integrity(ctx); err != nil {
			outcome = "fails_closed"
			t.Logf("integrity check refused the damaged file: %v", err)
		}
	}
	if outcome != corruption.Expected {
		t.Fatalf("damaged database: %s, fixture expects %s", outcome, corruption.Expected)
	}
}

// TestStoreFixtureEffects counts the effects of acknowledged and refused
// writes after the store is closed and reopened: an acknowledged write
// leaves one row, a write whose callback fails leaves none, and writing the
// acknowledged row again under its unique key is refused, not acknowledged
// a second time.
func TestStoreFixtureEffects(t *testing.T) {
	want := loadStoreFixtures(t).Effects
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "effects.db")
	database, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "CREATE TABLE effects (effect_key TEXT PRIMARY KEY, value TEXT NOT NULL)")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	write := func(key string, fail bool) error {
		return database.WithTx(ctx, func(tx *sql.Tx) error {
			if _, err := tx.ExecContext(ctx, "INSERT INTO effects (effect_key, value) VALUES (?, 'applied')", key); err != nil {
				return err
			}
			if fail {
				return errors.New("the effect's callback failed after its write")
			}
			return nil
		})
	}
	acknowledgments := 0
	for range 2 {
		if err := write("succeeds", false); err == nil {
			acknowledgments++
		} else {
			t.Logf("second write of an acknowledged effect: %v", err)
		}
	}
	if err := write("fails", true); err == nil {
		t.Fatal("a failing callback was acknowledged")
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := (Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	var got fixtureEffects
	if err := reopened.WithTx(ctx, func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM effects WHERE effect_key = 'succeeds'").Scan(&got.SuccessfulEffects); err != nil {
			return err
		}
		return tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM effects WHERE effect_key = 'fails'").Scan(&got.FailedEffects)
	}); err != nil {
		t.Fatal(err)
	}
	got.DuplicateAcknowledgments = acknowledgments - 1
	if got != want {
		t.Fatalf("observed %+v, fixture expects %+v", got, want)
	}
}
