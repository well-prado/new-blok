package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
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
	// The leaf is the completed child of both runs, the active one
	// included: today it is compacted like any completed run, and its
	// result stays in the active parent's journal_children row. Slice B
	// (#289) revisits child results.
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
	restoredCompleted := snapshotRun(t, restoredJournal, completed)
	observed.AuditRecordRetained = tombstoneProblem(completedBefore["journal_runs"], restoredCompleted["journal_compacted"]) == "" &&
		len(completedBefore["audit_records_v1"]) > 0 && reflect.DeepEqual(restoredCompleted["audit_records_v1"], completedBefore["audit_records_v1"])
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

// TestRetentionTableDeclarationsCoverEveryTable proves the schema guard the
// tests above rely on can fail: a new table the tests do not know about,
// keyed by anything, stops every snapshot until it is declared.
func TestRetentionTableDeclarationsCoverEveryTable(t *testing.T) {
	ctx := context.Background()
	database, j := openRetentionJournal(t, filepath.Join(t.TempDir(), "journal.db"), Hooks{})
	defer database.Close()
	if err := j.withRead(ctx, func(tx *sql.Tx) error {
		_, err := runKeyedTables(ctx, tx)
		return err
	}); err != nil {
		t.Fatalf("today's schema: %v", err)
	}
	if err := database.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE journal_scope_items (scope_rowid INTEGER NOT NULL, item_json BLOB)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	err := j.withRead(ctx, func(tx *sql.Tx) error {
		_, err := runKeyedTables(ctx, tx)
		return err
	})
	if err == nil || !strings.Contains(err.Error(), "table journal_scope_items is declared neither") {
		t.Fatalf("undeclared table without run_id: err=%v", err)
	}
}

// recoveryRun names a run seeded by seedRecoveryRun and the operation
// whose attempts it owns.
type recoveryRun struct {
	runID, operationKey string
}

// runRows is every row a run has in each table that holds run rows, keyed
// by table, sorted.
type runRows map[string][]row

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

// scheduleRetentionWait is the only ScheduleWait call in these tests.
func scheduleRetentionWait(tb testing.TB, j *Journal, runID, waitID, name string) {
	tb.Helper()
	if _, err := j.ScheduleWait(context.Background(), WaitRequest{RunID: runID, WaitID: waitID, Name: name, InvocationPath: name, IterationPath: "root", DueAt: time.Now().Add(time.Hour)}); err != nil {
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
			read, err := readRows(ctx, tx, query, argument)
			if err != nil {
				return fmt.Errorf("%s: %w", table, err)
			}
			rows[table] = read
		}
		return nil
	})
	if err != nil {
		tb.Fatal(err)
	}
	return rows
}

// notRunOwnedTables are the tables that hold no row of any one run. Every
// table in the store must be either here or in compactionFates.
var notRunOwnedTables = map[string]bool{
	"audit_meta_v1":        true,
	"audit_pruned_v1":      true,
	"blok_schema_versions": true,
	"journal_artifacts":    true,
	"journal_meta":         true,
	"sqlite_sequence":      true,
}

// runKeyedTables returns the tables holding run rows. It fails unless every
// table in the store is declared, either with a compaction fate or as not
// run-owned, so a new table keyed in any way cannot escape these checks. A
// table with a fate must be readable by run (a run_id column, or
// journal_attempts by operation key), and one declared not run-owned must
// have no run_id column.
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
	var tables []string
	present := map[string]bool{}
	for _, name := range all {
		present[name] = true
		_, fated := compactionFates[name]
		keyed, err := hasColumn(ctx, tx, name, "run_id")
		if err != nil {
			return nil, err
		}
		switch {
		case fated && !keyed && name != "journal_attempts":
			return nil, fmt.Errorf("table %s has a compaction fate but no run_id column to read it by", name)
		case fated:
			tables = append(tables, name)
		case notRunOwnedTables[name] && keyed:
			return nil, fmt.Errorf("table %s is declared not run-owned but has a run_id column", name)
		case !notRunOwnedTables[name]:
			return nil, fmt.Errorf("table %s is declared neither with a compaction fate nor as not run-owned", name)
		}
	}
	for _, table := range append(sortedTables(), sortedKeys(notRunOwnedTables)...) {
		if !present[table] {
			return nil, fmt.Errorf("declared table %s does not exist", table)
		}
	}
	for _, table := range runOwnedTables {
		if compactionFates[table] != fateErased {
			return nil, fmt.Errorf("runOwnedTables lists %s, whose declared fate is %q", table, compactionFates[table])
		}
	}
	return tables, nil
}

// field is one column of a stored row; a []byte value is kept as a string.
type field struct {
	column string
	value  any
}

// row is a stored row, its fields in column order.
type row []field

func (r row) String() string {
	parts := make([]string, len(r))
	for i, f := range r {
		parts[i] = fmt.Sprintf("%s=%#v", f.column, f.value)
	}
	return strings.Join(parts, " ")
}

func (r row) value(column string) any {
	for _, f := range r {
		if f.column == column {
			return f.value
		}
	}
	return nil
}

// without returns the row without the named columns.
func (r row) without(columns ...string) row {
	var kept row
	for _, f := range r {
		if !slices.Contains(columns, f.column) {
			kept = append(kept, f)
		}
	}
	return kept
}

func rendered(rows []row) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.String()
	}
	return out
}

func readRows(ctx context.Context, tx *sql.Tx, query string, arguments ...any) ([]row, error) {
	result, err := tx.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer result.Close()
	columns, err := result.Columns()
	if err != nil {
		return nil, err
	}
	var rows []row
	for result.Next() {
		values := make([]any, len(columns))
		pointers := make([]any, len(columns))
		for i := range values {
			pointers[i] = &values[i]
		}
		if err := result.Scan(pointers...); err != nil {
			return nil, err
		}
		r := make(row, len(columns))
		for i, value := range values {
			if data, ok := value.([]byte); ok {
				value = string(data)
			}
			r[i] = field{column: columns[i], value: value}
		}
		rows = append(rows, r)
	}
	sort.Slice(rows, func(a, b int) bool { return rows[a].String() < rows[b].String() })
	return rows, result.Err()
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
			changed = append(changed, fmt.Sprintf("%s: %d rows -> %d rows\n  before %q\n  after  %q", table, len(before[table]), len(after[table]), rendered(before[table]), rendered(after[table])))
		}
	}
	if len(changed) > 0 {
		tb.Fatalf("run %s changed in %d tables:\n%s", run.runID, len(changed), strings.Join(changed, "\n"))
	}
}

func sortedTables() []string { return sortedKeys(compactionFates) }

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// assertCompacted fails unless the run is compacted exactly per ADR 0021.
func assertCompacted(tb testing.TB, j *Journal, run recoveryRun, before runRows) {
	tb.Helper()
	if problem := compactedRows(tb, j, run, before); problem != "" {
		tb.Fatal(problem)
	}
}

// erasedReconciliationColumns are the reconciliation columns compaction
// erases (ADR 0021 §7.2); every other column must keep its value.
var erasedReconciliationColumns = []string{"actor", "evidence", "result_json", "erased_at"}

// compactedRows describes how the run's rows differ from a compacted run's
// (ADR 0021 §7), or returns "": no row left in an erased table; each
// reconciliation kept with every column but actor, evidence and result
// unchanged, those three erased and erased_at set; one tombstone carrying
// exactly the run's digests and timestamps; and its audit records
// untouched.
func compactedRows(tb testing.TB, j *Journal, run recoveryRun, before runRows) string {
	tb.Helper()
	after := snapshotRun(tb, j, run)
	for _, table := range sortedTables() {
		switch fate := compactionFates[table]; fate {
		case fateErased:
			if len(after[table]) != 0 {
				return fmt.Sprintf("compacted run %s keeps %s rows %q", run.runID, table, rendered(after[table]))
			}
		case fateTombstone:
			if problem := tombstoneProblem(before["journal_runs"], after[table]); problem != "" {
				return fmt.Sprintf("compacted run %s: %s", run.runID, problem)
			}
		case fateKept:
			if !reflect.DeepEqual(after[table], before[table]) {
				return fmt.Sprintf("compaction changed %s of run %s:\nbefore %q\nafter  %q", table, run.runID, rendered(before[table]), rendered(after[table]))
			}
		case fateScrubbed:
			if problem := scrubProblem(before[table], after[table]); problem != "" {
				return fmt.Sprintf("compacted run %s: %s", run.runID, problem)
			}
		}
	}
	return ""
}

// scrubProblem compares a run's reconciliations before and after
// compaction, or returns "".
func scrubProblem(before, after []row) string {
	if len(after) != len(before) {
		return fmt.Sprintf("%d reconciliations, had %d", len(after), len(before))
	}
	for i := range after {
		if kept, was := after[i].without(erasedReconciliationColumns...), before[i].without(erasedReconciliationColumns...); !reflect.DeepEqual(kept, was) {
			return fmt.Sprintf("reconciliation changed beyond its erased content:\nbefore %s\nafter  %s", was, kept)
		}
		if after[i].value("actor") != "" || after[i].value("evidence") != nil || after[i].value("result_json") != nil || after[i].value("erased_at") == nil {
			return fmt.Sprintf("reconciliation keeps content: %s", after[i])
		}
	}
	return ""
}

// tombstoneProblem checks the one tombstone left for a run against its row
// before compaction (ADR 0021 §7.4), or returns "". Each digest is computed
// here, not by the journal's own helpers.
func tombstoneProblem(runRows, tombstones []row) string {
	if len(runRows) != 1 || len(tombstones) != 1 {
		return fmt.Sprintf("%d run rows before compaction and %d tombstones after, want 1 and 1", len(runRows), len(tombstones))
	}
	run, tombstone := runRows[0], tombstones[0]
	sum := func(value any) string {
		data, _ := value.(string)
		if data == "" {
			return ""
		}
		digest := sha256.Sum256([]byte(data))
		return "sha256:" + hex.EncodeToString(digest[:])
	}
	want := row{
		{"run_id", run.value("run_id")},
		{"request_digest", sum(run.value("request_key"))},
		{"artifact_digest", run.value("artifact_digest")},
		{"input_digest", run.value("input_digest")},
		{"output_digest", sum(run.value("output_json"))},
		{"state", run.value("state")},
		{"completed_at", run.value("completed_at")},
	}
	if got := tombstone.without("compacted_at"); !reflect.DeepEqual(got, want) {
		return fmt.Sprintf("tombstone\n  got  %s\n  want %s", got, want)
	}
	compactedAt, ok := tombstone.value("compacted_at").(int64)
	completedAt, _ := run.value("completed_at").(int64)
	if !ok || compactedAt < completedAt {
		return fmt.Sprintf("tombstone compacted_at %v before the run completed at %d", tombstone.value("compacted_at"), completedAt)
	}
	return ""
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
