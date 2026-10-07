package journal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// TestCompactionRetainsAuditAndActiveRuns compacts a completed run next to
// an active one. Both hold a row in every table a run owns, so the test
// fails when compaction removes or rewrites any row of the active run, not
// only its journal_runs row (#342, mutation M49a2).
func TestCompactionRetainsAuditAndActiveRuns(t *testing.T) {
	ctx := context.Background()
	database, j := openRetentionJournal(t, filepath.Join(t.TempDir(), "journal.db"), Hooks{})
	defer database.Close()
	leaf := admitCompletedLeaf(t, j, "leaf")
	completed := seedRecoveryRun(t, j, "completed", leaf)
	completeRun(t, j, completed.runID)
	active := seedRecoveryRun(t, j, "active", leaf)
	before := map[string]runRows{
		completed.runID: snapshotRun(t, j, completed),
		active.runID:    snapshotRun(t, j, active),
	}
	requireEveryTable(t, active.runID, before[active.runID])
	requireEveryTable(t, completed.runID, before[completed.runID])

	report, err := j.Compact(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if report.RemovedRuns != 2 || report.Tombstones != 2 {
		t.Fatalf("report=%+v, want the completed run and its child leaf removed", report)
	}
	assertCompacted(t, j, completed, before[completed.runID])
	assertWhole(t, j, active, before[active.runID])
	if _, err := j.Run(ctx, active.runID); err != nil {
		t.Fatalf("active run was compacted: %v", err)
	}

	again, err := j.Compact(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if again.RemovedRuns != 0 || again.Tombstones != 2 {
		t.Fatalf("second pass=%+v, want no removal and no duplicate tombstone", again)
	}
	assertWhole(t, j, active, before[active.runID])
}

// TestRetentionBackupFixture runs the scenario testdata/restore/
// retention-backup.json describes and checks every expectation it declares
// against what the journal and store actually do (#49 V5). The test
// predeclares the expectations too, so an edited fixture fails here instead
// of silently changing what is proven.
func TestRetentionBackupFixture(t *testing.T) {
	type expectations struct {
		CompletedRunCompacted          bool `json:"completedRunCompacted"`
		ActiveRunRetained              bool `json:"activeRunRetained"`
		AuditRecordRetained            bool `json:"auditRecordRetained"`
		BackupIntegrityChecked         bool `json:"backupIntegrityChecked"`
		RestoreIntegrityChecked        bool `json:"restoreIntegrityChecked"`
		ExistingDestinationOverwritten bool `json:"existingDestinationOverwritten"`
	}
	var fixture struct {
		Name      string       `json:"name"`
		Synthetic bool         `json:"synthetic"`
		Expected  expectations `json:"expected"`
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "testdata", "restore", "retention-backup.json"))
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatalf("fixture: %v", err)
	}
	predeclared := expectations{
		CompletedRunCompacted:          true,
		ActiveRunRetained:              true,
		AuditRecordRetained:            true,
		BackupIntegrityChecked:         true,
		RestoreIntegrityChecked:        true,
		ExistingDestinationOverwritten: false,
	}
	if fixture.Name != "retention-backup-restore" || !fixture.Synthetic || fixture.Expected != predeclared {
		t.Fatalf("fixture %q synthetic=%v expected=%+v, predeclared %+v", fixture.Name, fixture.Synthetic, fixture.Expected, predeclared)
	}

	ctx := context.Background()
	directory := t.TempDir()
	sourcePath := filepath.Join(directory, "source.db")
	backupPath := filepath.Join(directory, "backup.db")
	restoredPath := filepath.Join(directory, "restored.db")
	database, j := openRetentionJournal(t, sourcePath, Hooks{})
	leaf := admitCompletedLeaf(t, j, "leaf")
	completed := seedRecoveryRun(t, j, "completed", leaf)
	completeRun(t, j, completed.runID)
	active := seedRecoveryRun(t, j, "active", leaf)
	completedBefore, activeBefore := snapshotRun(t, j, completed), snapshotRun(t, j, active)
	requireEveryTable(t, active.runID, activeBefore)
	if _, err := j.Compact(ctx, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := j.Backup(ctx, backupPath); err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	var observed expectations
	// backupIntegrityChecked: Restore refuses a backup that fails its
	// integrity check and installs nothing. The corruption leaves page 1
	// intact, so the backup still opens and only the check can catch it.
	corruptPath := filepath.Join(directory, "corrupt.db")
	corruptIndexPage(t, backupPath, corruptPath, "journal_runs", "sqlite_autoindex_journal_runs_1")
	refusedPath := filepath.Join(directory, "refused.db")
	refused := (sqlite.Backend{}).Restore(ctx, corruptPath, refusedPath)
	_, statErr := os.Stat(refusedPath)
	observed.BackupIntegrityChecked = refused != nil && strings.Contains(refused.Error(), "source integrity") && errors.Is(statErr, os.ErrNotExist)
	t.Logf("restore of a corrupt backup: %v", refused)

	// restoreIntegrityChecked: the restored database passes PRAGMA
	// integrity_check. Whether Restore re-checks it itself (mutation M49b2)
	// is #343's test.
	if err := (sqlite.Backend{}).Restore(ctx, backupPath, restoredPath); err != nil {
		t.Fatal(err)
	}
	restored, err := (sqlite.Backend{}).Open(ctx, restoredPath)
	if err != nil {
		t.Fatal(err)
	}
	observed.RestoreIntegrityChecked = integrityCheck(t, restored) == "ok"
	restoredJournal, err := New(ctx, restored, Config{Audit: newTestAudit(t, restored)})
	if err != nil {
		t.Fatal(err)
	}
	observed.CompletedRunCompacted = compactedRows(t, restoredJournal, completed, completedBefore) == ""
	observed.ActiveRunRetained = reflect.DeepEqual(snapshotRun(t, restoredJournal, active), activeBefore)
	// auditRecordRetained: the run's audit records and its digest-only
	// tombstone both survive compaction, backup and restore.
	observed.AuditRecordRetained = tombstoneMatches(t, restoredJournal, "completed", completed.runID) &&
		reflect.DeepEqual(snapshotRun(t, restoredJournal, completed)["audit_records_v1"], completedBefore["audit_records_v1"])
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}

	// existingDestinationOverwritten: Restore onto a file that already
	// exists is refused and leaves its bytes unchanged.
	existingPath, existing := filepath.Join(directory, "existing.db"), []byte("an existing destination")
	if err := os.WriteFile(existingPath, existing, 0o600); err != nil {
		t.Fatal(err)
	}
	again := (sqlite.Backend{}).Restore(ctx, backupPath, existingPath)
	t.Logf("restore onto an existing destination: %v", again)
	after, err := os.ReadFile(existingPath)
	if err != nil {
		t.Fatal(err)
	}
	observed.ExistingDestinationOverwritten = again == nil || !bytes.Equal(after, existing)

	if observed != fixture.Expected {
		t.Fatalf("observed %+v, fixture expects %+v", observed, fixture.Expected)
	}
}

// recoveryRun names a run seeded by seedRecoveryRun and the operation
// whose attempts it owns.
type recoveryRun struct {
	runID, operationKey string
}

// runRows is every row a run has in each table that holds run rows, keyed
// by table, each row rendered in column order and sorted.
type runRows map[string][]string

const retentionArtifact = "sha256:retention"

// Fates of the tables holding a run's rows when Compact removes the run
// (ADR 0021, #281).
const (
	fateErased    = "erased"    // every row of the run is deleted
	fateScrubbed  = "scrubbed"  // identity and digests stay, content goes
	fateTombstone = "tombstone" // one digest-only row is written
	fateKept      = "kept"      // untouched: audit has its own retention
)

// compactionFates lists every table that holds a run's rows. The test
// discovers those tables from the schema and fails when one is missing
// here, so a new run-owned table cannot escape these checks.
var compactionFates = map[string]string{
	"journal_runs":            fateErased,
	"journal_operations":      fateErased,
	"journal_attempts":        fateErased,
	"journal_waits":           fateErased,
	"journal_signals":         fateErased,
	"journal_checkpoints":     fateErased,
	"journal_scopes":          fateErased,
	"journal_children":        fateErased,
	"journal_joins":           fateErased,
	"journal_reconciliations": fateScrubbed,
	"journal_compacted":       fateTombstone,
	"audit_records_v1":        fateKept,
}

// openRetentionJournal opens a journal with the audit journal composed, so
// seeded runs can carry reconciliations.
func openRetentionJournal(tb testing.TB, path string, hooks Hooks) (store.Database, *Journal) {
	tb.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		tb.Fatal(err)
	}
	j, err := New(context.Background(), database, Config{Audit: newTestAudit(tb, database), Hooks: hooks})
	if err != nil {
		_ = database.Close()
		tb.Fatal(err)
	}
	return database, j
}

// admitCompletedLeaf admits and completes a run with no other rows, the
// child seeded runs point at.
func admitCompletedLeaf(tb testing.TB, j *Journal, key string) string {
	tb.Helper()
	admission, err := j.Admit(context.Background(), AdmissionRequest{RequestKey: key, Workflow: "orders", ArtifactDigest: retentionArtifact, Input: []byte(`{}`)})
	if err != nil {
		tb.Fatal(err)
	}
	completeRun(tb, j, admission.RunID)
	return admission.RunID
}

func completeRun(tb testing.TB, j *Journal, runID string) {
	tb.Helper()
	if err := j.CompleteRun(context.Background(), runID, []byte(`{"ok":true}`)); err != nil {
		tb.Fatal(err)
	}
}

// seedRecoveryRun admits a run and gives it a row in every table a run
// owns: an operation with an attempt settled by a reconciliation, a wait
// resumed by a signal, a pending signal, a checkpoint, a completed scope,
// a child and a completed join. It leaves the run active and quiescent, so
// a caller may complete it.
func seedRecoveryRun(tb testing.TB, j *Journal, key, childRunID string) recoveryRun {
	tb.Helper()
	ctx := context.Background()
	admission, err := j.Admit(ctx, AdmissionRequest{RequestKey: key, Workflow: "orders", ArtifactDigest: retentionArtifact, Input: []byte(`{"marker":"` + key + `"}`)})
	if err != nil {
		tb.Fatal(err)
	}
	runID := admission.RunID
	operation, err := j.BeginEffect(ctx, EffectIntent{Identity: OperationIdentity{RunID: runID, ArtifactDigest: retentionArtifact, InvocationPath: "charge", IterationPath: "root"}, Input: []byte(`{"amount":1}`)})
	if err != nil {
		tb.Fatal(err)
	}
	attempt, err := j.StartAttempt(ctx, operation.Key)
	if err != nil {
		tb.Fatal(err)
	}
	if err := j.MarkUncertain(ctx, operation.Key, attempt.ID, "timeout "+key); err != nil {
		tb.Fatal(err)
	}
	if _, err := j.Reconcile(ctx, operation.Key, "operator", "receipt "+key, []byte(`{"charged":true}`), true); err != nil {
		tb.Fatal(err)
	}
	scheduleRetentionWait(tb, j, runID, "wait:"+key, "approval")
	if result, err := j.Signal(ctx, signal.Envelope{RunID: runID, SignalID: "approve:" + key, Name: "approval", Payload: []byte(`{"approved":true}`), Principal: "ops"}, true); err != nil || !result.Resumed {
		tb.Fatalf("signal=%+v err=%v", result, err)
	}
	if result, err := j.Signal(ctx, signal.Envelope{RunID: runID, SignalID: "later:" + key, Name: "later", Payload: []byte(`{"early":true}`), Principal: "ops"}, true); err != nil || !result.Accepted {
		tb.Fatalf("pending signal=%+v err=%v", result, err)
	}
	if err := j.SaveCheckpoint(ctx, Checkpoint{RunID: runID, ArtifactDigest: retentionArtifact, CheckpointDigest: "sha256:checkpoint", State: []byte(`{"step":2}`)}); err != nil {
		tb.Fatal(err)
	}
	seedCompletedScope(tb, j, runID, "root/each")
	if err := j.RecordChild(ctx, ChildRecord{RunID: runID, Path: "root/child", ChildRunID: childRunID, State: childCompleted, Result: []byte(`{"child":true}`)}); err != nil {
		tb.Fatal(err)
	}
	if err := j.RecordJoin(ctx, JoinRecord{RunID: runID, Path: "root/join", Expected: 2, Completed: 2, Results: []json.RawMessage{[]byte(`1`), []byte(`2`)}}); err != nil {
		tb.Fatal(err)
	}
	return recoveryRun{runID: runID, operationKey: operation.Key}
}

// scheduleRetentionWait is the only ScheduleWait call in these tests: its
// request gains the wait's step identity in #350.
func scheduleRetentionWait(tb testing.TB, j *Journal, runID, waitID, name string) {
	tb.Helper()
	if _, err := j.ScheduleWait(context.Background(), WaitRequest{RunID: runID, WaitID: waitID, Name: name, DueAt: time.Now().Add(time.Hour)}); err != nil {
		tb.Fatal(err)
	}
}

// seedCompletedScope is the only StartScope/CompleteScope pair in these
// tests: scope completion gains attempt fencing in #351.
func seedCompletedScope(tb testing.TB, j *Journal, runID, path string) {
	tb.Helper()
	if _, err := j.StartScope(context.Background(), ScopeRecord{RunID: runID, Path: path, Kind: "each", Input: []byte(`{"items":2}`)}); err != nil {
		tb.Fatal(err)
	}
	if err := j.CompleteScope(context.Background(), runID, path, []byte(`{"done":true}`)); err != nil {
		tb.Fatal(err)
	}
}

// snapshotRun reads every row the run has in every table that holds run
// rows. Attempts are read by the run's operation key, not through
// journal_operations, so attempts left behind by a deleted operation still
// show.
func snapshotRun(tb testing.TB, j *Journal, run recoveryRun) runRows {
	tb.Helper()
	ctx := context.Background()
	rows := runRows{}
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		tables, err := runKeyedTables(ctx, tx)
		if err != nil {
			return err
		}
		for _, table := range tables {
			query, argument := `SELECT * FROM `+table+` WHERE run_id = ?`, run.runID
			if table == "journal_attempts" {
				query, argument = `SELECT * FROM journal_attempts WHERE operation_key = ?`, run.operationKey
			}
			rendered, err := renderRows(ctx, tx, query, argument)
			if err != nil {
				return fmt.Errorf("%s: %w", table, err)
			}
			rows[table] = rendered
		}
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return rows
}

// runKeyedTables lists every table with a run_id column, plus
// journal_attempts, which is keyed by operation, and fails when one has no
// declared compaction fate.
func runKeyedTables(ctx context.Context, tx *sql.Tx) ([]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	var all []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, err
		}
		all = append(all, name)
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	tables := []string{"journal_attempts"}
	for _, name := range all {
		owned, err := hasColumn(ctx, tx, name, "run_id")
		if err != nil {
			return nil, err
		}
		if owned {
			tables = append(tables, name)
		}
	}
	sort.Strings(tables)
	for _, table := range tables {
		if _, ok := compactionFates[table]; !ok {
			return nil, fmt.Errorf("table %s holds run rows but has no compaction fate in this test", table)
		}
	}
	if len(tables) != len(compactionFates) {
		return nil, fmt.Errorf("tables holding run rows %v, compaction fates declared for %d", tables, len(compactionFates))
	}
	for _, table := range runOwnedTables {
		if compactionFates[table] != fateErased {
			return nil, fmt.Errorf("runOwnedTables lists %s, whose declared fate is %q", table, compactionFates[table])
		}
	}
	return tables, nil
}

func renderRows(ctx context.Context, tx *sql.Tx, query string, arguments ...any) ([]string, error) {
	rows, err := tx.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var rendered []string
	for rows.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := rows.Scan(pointers...); err != nil {
			return nil, err
		}
		fields := make([]string, len(columns))
		for i, value := range values {
			if data, ok := value.([]byte); ok {
				value = string(data)
			}
			fields[i] = fmt.Sprintf("%s=%#v", columns[i], value)
		}
		rendered = append(rendered, strings.Join(fields, " "))
	}
	sort.Strings(rendered)
	return rendered, rows.Err()
}

// requireEveryTable fails unless the seeded run has a row in every table
// compaction erases or scrubs, so the checks below cannot pass vacuously.
func requireEveryTable(tb testing.TB, runID string, rows runRows) {
	tb.Helper()
	for _, table := range sortedTables() {
		if compactionFates[table] == fateTombstone {
			continue
		}
		if len(rows[table]) == 0 {
			tb.Fatalf("seeded run %s has no row in %s", runID, table)
		}
	}
}

// assertWhole fails unless every row the run had is still there, unchanged.
func assertWhole(tb testing.TB, j *Journal, run recoveryRun, before runRows) {
	tb.Helper()
	after := snapshotRun(tb, j, run)
	var changed []string
	for _, table := range sortedTables() {
		if !reflect.DeepEqual(after[table], before[table]) {
			changed = append(changed, fmt.Sprintf("%s: %d rows -> %d rows\n  before %q\n  after  %q", table, len(before[table]), len(after[table]), before[table], after[table]))
		}
	}
	if len(changed) > 0 {
		tb.Fatalf("run %s changed in %d tables:\n%s", run.runID, len(changed), strings.Join(changed, "\n"))
	}
}

func sortedTables() []string {
	tables := make([]string, 0, len(compactionFates))
	for table := range compactionFates {
		tables = append(tables, table)
	}
	sort.Strings(tables)
	return tables
}

// assertCompacted fails unless the run is compacted exactly per ADR 0021.
func assertCompacted(tb testing.TB, j *Journal, run recoveryRun, before runRows) {
	tb.Helper()
	if problem := compactedRows(tb, j, run, before); problem != "" {
		tb.Fatal(problem)
	}
}

// compactedRows describes how the run's rows differ from a compacted run's,
// or returns "": no row left in an erased table, its reconciliations kept
// with actor, evidence and result erased, one tombstone, and its audit
// records untouched.
func compactedRows(tb testing.TB, j *Journal, run recoveryRun, before runRows) string {
	tb.Helper()
	after := snapshotRun(tb, j, run)
	for _, table := range sortedTables() {
		switch fate := compactionFates[table]; fate {
		case fateErased:
			if len(after[table]) != 0 {
				return fmt.Sprintf("compacted run %s keeps %s rows %q", run.runID, table, after[table])
			}
		case fateTombstone:
			if len(after[table]) != 1 {
				return fmt.Sprintf("compacted run %s has %d tombstones", run.runID, len(after[table]))
			}
		case fateKept:
			if !reflect.DeepEqual(after[table], before[table]) {
				return fmt.Sprintf("compaction changed %s of run %s:\nbefore %q\nafter  %q", table, run.runID, before[table], after[table])
			}
		case fateScrubbed:
			if len(after[table]) != len(before[table]) {
				return fmt.Sprintf("compacted run %s has %d reconciliations, had %d", run.runID, len(after[table]), len(before[table]))
			}
		}
	}
	var unerased int
	err := j.withRead(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM journal_reconciliations WHERE run_id = ? AND (actor != '' OR evidence IS NOT NULL OR result_json IS NOT NULL OR erased_at IS NULL)`, run.runID).Scan(&unerased)
	})
	if err != nil {
		tb.Fatal(err)
	}
	if unerased != 0 {
		return fmt.Sprintf("compacted run %s keeps %d reconciliations with content", run.runID, unerased)
	}
	return ""
}

// tombstoneMatches reports whether the run's tombstone carries the digest
// of its request key and no plain content.
func tombstoneMatches(tb testing.TB, j *Journal, requestKey, runID string) bool {
	tb.Helper()
	var request, state string
	err := j.withRead(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `SELECT request_digest, state FROM journal_compacted WHERE run_id = ?`, runID).Scan(&request, &state)
	})
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if err != nil {
		tb.Fatal(err)
	}
	return request == digestBytes([]byte(requestKey)) && state == runCompleted
}

// integrityCheck returns the first row of PRAGMA integrity_check.
func integrityCheck(tb testing.TB, database store.Database) string {
	tb.Helper()
	var result string
	err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		return tx.QueryRowContext(context.Background(), `PRAGMA integrity_check`).Scan(&result)
	})
	if err != nil {
		tb.Fatal(err)
	}
	return result
}

// corruptIndexPage copies source to destination and overwrites the cell
// content at the end of the root page of an index on table in the copy,
// leaving page 1 and every table page intact. The source is never opened.
// The table must have a row, so the overwritten bytes hold an index entry.
func corruptIndexPage(tb testing.TB, source, destination, table, index string) {
	tb.Helper()
	contents, err := os.ReadFile(source)
	if err != nil {
		tb.Fatal(err)
	}
	if err := os.WriteFile(destination, contents, 0o600); err != nil {
		tb.Fatal(err)
	}
	database, err := (sqlite.Backend{}).Open(context.Background(), destination)
	if err != nil {
		tb.Fatal(err)
	}
	var page, size, entries int64
	err = database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if err := tx.QueryRowContext(context.Background(), `SELECT rootpage FROM sqlite_master WHERE name = ? AND tbl_name = ?`, index, table).Scan(&page); err != nil {
			return err
		}
		if err := tx.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM `+table).Scan(&entries); err != nil {
			return err
		}
		return tx.QueryRowContext(context.Background(), `PRAGMA page_size`).Scan(&size)
	})
	if closeErr := database.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		tb.Fatal(err)
	}
	if entries == 0 {
		tb.Fatalf("%s is empty, so corrupting %s would change no entry", table, index)
	}
	// Opening switched the copy to WAL, which rewrites page 1 only.
	contents, err = os.ReadFile(destination)
	if err != nil {
		tb.Fatal(err)
	}
	if page < 2 || page*size > int64(len(contents)) {
		tb.Fatalf("index %s root page %d outside a %d-byte file", index, page, len(contents))
	}
	end := page * size
	for i := end - 64; i < end; i++ {
		contents[i] ^= 0xa5
	}
	if err := os.WriteFile(destination, contents, 0o600); err != nil {
		tb.Fatal(err)
	}
}
