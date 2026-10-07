package journal

import (
	"compress/gzip"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/inspection"
	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// waitFixture is a database origin/main wrote at aaf633c, before #332: its
// journal_waits is keyed UNIQUE (run_id, name) and its journal is stamped
// version 3. testdata/restore/wait-identity-332/generate.go wrote it and
// lists its runs, waits and signals.
const waitFixture = "../../testdata/restore/wait-identity-332/legacy-main-aaf633c.db.gz"

// fixtureBase is the generator's clock origin.
var fixtureBase = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

// ticking returns a clock that advances one second per read, so waits
// created one after another have distinct, ordered creation times.
func ticking(start time.Time) func() time.Time {
	now := start
	return func() time.Time { now = now.Add(time.Second); return now }
}

// TestRunWaitsOnOneNameInSuccessiveIterationsAndSteps (#332, D2): a run
// waits on "approval" in two iterations of one step, then in two other
// steps at once, then on a timer in a third iteration. Every wait is
// stored, every signal goes to the open wait of its name (the oldest when
// two are open), and none is classified late while the run is live. On
// origin/main the second wait fails with a raw UNIQUE error and its signal
// is late.
func TestRunWaitsOnOneNameInSuccessiveIterationsAndSteps(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "identity.db", Config{Clock: ticking(fixtureBase)})
	defer database.Close()
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "loop", Principal: "alice", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	later := fixtureBase.Add(time.Hour)
	schedule := func(waitID, step, iteration string, due time.Time) WaitRecord {
		t.Helper()
		record, err := j.ScheduleWait(ctx, WaitRequest{RunID: run.RunID, WaitID: waitID, Name: "approval", InvocationPath: step, IterationPath: iteration, DueAt: due})
		if err != nil || record.State != waitWaiting || record.InvocationPath != step || record.IterationPath != iteration {
			t.Fatalf("schedule %s=%+v err=%v", waitID, record, err)
		}
		return record
	}
	send := func(signalID string) SignalResult {
		t.Helper()
		result, err := j.Signal(ctx, signal.Envelope{RunID: run.RunID, SignalID: signalID, Name: "approval", Principal: "operator", Payload: []byte(`{"signal":"` + signalID + `"}`)}, true)
		if err != nil {
			t.Fatalf("signal %s: %v", signalID, err)
		}
		return result
	}
	delivered := SignalResult{Accepted: true, Resumed: true}

	schedule("loop[0]", "approve", "0", later)
	if result := send("s0"); result != delivered {
		t.Fatalf("s0=%+v", result)
	}
	schedule("loop[1]", "approve", "1", later)
	// The step shows its latest wait: the open one.
	if _, steps, _, err := j.ReadInspection(ctx, "alice", run.RunID, "", 0, 10, nil, inspection.MinPayloadBytes); err != nil || len(steps) != 1 || steps[0].ID != "wait:approval" || steps[0].Status != inspection.StatusSuspended {
		t.Fatalf("inspection while loop[1] is open: steps=%+v err=%v", steps, err)
	}
	if result := send("s1"); result != delivered {
		t.Fatalf("s1, for the second wait of the name=%+v; want delivered", result)
	}
	schedule("confirm", "confirm", "root", later)
	schedule("audit", "audit", "root", later)
	if result := send("s2"); result != delivered {
		t.Fatalf("s2=%+v", result)
	}
	if result := send("s3"); result != delivered {
		t.Fatalf("s3=%+v", result)
	}
	schedule("loop[2]", "approve", "2", fixtureBase)
	claimed, err := j.ClaimDueWaits(ctx, fixtureBase.Add(time.Minute), 10)
	if err != nil || len(claimed) != 1 || claimed[0].WaitID != "loop[2]" || claimed[0].IterationPath != "2" {
		t.Fatalf("claimed=%+v err=%v", claimed, err)
	}
	// Every wait of the name has closed, so a new signal waits for the next.
	if result := send("s4"); result != (SignalResult{Accepted: true}) {
		t.Fatalf("s4 after every wait closed=%+v; want pending", result)
	}
	wantWaits := []string{
		"audit|approval|audit|root|resumed|s3",
		"confirm|approval|confirm|root|resumed|s2",
		"loop[0]|approval|approve|0|resumed|s0",
		"loop[1]|approval|approve|1|resumed|s1",
		"loop[2]|approval|approve|2|resumed|",
	}
	if got := waitRows(t, database, `SELECT wait_id || '|' || name || '|' || invocation_path || '|' || iteration_path || '|' || state || '|' || signal_id FROM journal_waits ORDER BY wait_id`); !reflect.DeepEqual(got, wantWaits) {
		t.Fatalf("waits:\n got %q\nwant %q", got, wantWaits)
	}
	wantSignals := []string{"s0|stored", "s1|stored", "s2|stored", "s3|stored", "s4|pending"}
	if got := waitRows(t, database, `SELECT signal_id || '|' || state FROM journal_signals ORDER BY signal_id`); !reflect.DeepEqual(got, wantSignals) {
		t.Fatalf("signals:\n got %q\nwant %q", got, wantSignals)
	}
}

// TestDuplicateWaitIsErrWaitExists (#332, D2): a wait that reuses a wait ID,
// or a step and iteration the run already waited at, open or closed, is
// ErrWaitExists, never a raw SQLite error, and writes nothing. Another run
// may use the same step and iteration.
func TestDuplicateWaitIsErrWaitExists(t *testing.T) {
	ctx := context.Background()
	database, j := newJournal(t, "duplicate.db", Config{Clock: ticking(fixtureBase)})
	defer database.Close()
	var runs [2]string
	for i := range runs {
		admitted, err := j.Admit(ctx, AdmissionRequest{RequestKey: fmt.Sprintf("run-%d", i), Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		runs[i] = admitted.RunID
	}
	request := WaitRequest{RunID: runs[0], WaitID: "first", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: fixtureBase.Add(time.Hour)}
	if _, err := j.ScheduleWait(ctx, request); err != nil {
		t.Fatal(err)
	}
	duplicate := func(label string, request WaitRequest) {
		t.Helper()
		if _, err := j.ScheduleWait(ctx, request); !errors.Is(err, ErrWaitExists) || strings.Contains(err.Error(), "constraint") {
			t.Fatalf("%s: err=%v; want ErrWaitExists", label, err)
		}
	}
	sameStep := request
	sameStep.WaitID, sameStep.Name = "second", "other-name"
	duplicate("same step and iteration under a new wait ID and name", sameStep)
	sameID := request
	sameID.IterationPath = "1"
	duplicate("same wait ID at another iteration", sameID)
	if result, err := j.Signal(ctx, signal.Envelope{RunID: runs[0], SignalID: "s1", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, true); err != nil || !result.Resumed {
		t.Fatalf("signal=%+v err=%v", result, err)
	}
	sameStep.WaitID = "third"
	duplicate("same step and iteration after its wait resumed", sameStep)
	missing := request
	missing.WaitID, missing.IterationPath = "no-iteration", ""
	if _, err := j.ScheduleWait(ctx, missing); err == nil || errors.Is(err, ErrWaitExists) {
		t.Fatalf("a wait without an iteration path: err=%v; want refused as invalid", err)
	}
	other := request
	other.RunID, other.WaitID = runs[1], "other-run"
	if record, err := j.ScheduleWait(ctx, other); err != nil || record.State != waitWaiting {
		t.Fatalf("another run at the same step and iteration=%+v err=%v", record, err)
	}
	want := []string{"first|approve|root|resumed", "other-run|approve|root|waiting"}
	if got := waitRows(t, database, `SELECT wait_id || '|' || invocation_path || '|' || iteration_path || '|' || state FROM journal_waits ORDER BY wait_id`); !reflect.DeepEqual(got, want) {
		t.Fatalf("waits:\n got %q\nwant %q", got, want)
	}
}

// TestOriginMainWaitsMigrateToStepIdentity opens the database origin/main
// wrote at aaf633c. The journal is migrated from 3 to 4: journal_waits is
// rebuilt keyed by step and iteration, every wait keeps every column it
// had (with no step identity), signals are untouched, and each legacy wait
// still behaves as it did: an open one takes its signal, a resumed one
// answers its signal's retry as a duplicate, a pending signal goes to the
// next wait of its name, a canceled one makes a signal addressed to it
// late, and a due timer is claimed. The run that waited on "approval" can then wait on it
// again. Reopening changes nothing.
func TestOriginMainWaitsMigrateToStepIdentity(t *testing.T) {
	ctx := context.Background()
	path := decompress(t, waitFixture)
	legacy := legacyWaitDump(t, path)
	if legacy.stamp != "3|0" || !strings.Contains(legacy.schema, "UNIQUE (run_id, name)") || len(legacy.waits) != 6 || len(legacy.signals) != 3 {
		t.Fatalf("fixture is not origin/main's pre-#332 journal: %+v", legacy)
	}
	database, j := newJournalAtPath(t, path, Config{Clock: ticking(fixtureBase.Add(48 * time.Hour))})
	defer database.Close()
	if got := oneRow(t, database, `SELECT version || '|' || upgraded_from FROM blok_schema_versions WHERE component = 'journal'`); got != "5|3" {
		t.Fatalf("journal stamp=%s; want 5 upgraded from 3", got)
	}
	unique := waitRows(t, database, `SELECT (SELECT group_concat(name, ',') FROM pragma_index_info(l.name)) FROM pragma_index_list('journal_waits') l WHERE l."unique" = 1 AND l.origin = 'u'`)
	if !reflect.DeepEqual(unique, []string{"run_id,invocation_path,iteration_path"}) {
		t.Fatalf("journal_waits unique constraints=%q; want only (run_id, invocation_path, iteration_path)", unique)
	}
	if got := waitRows(t, database, legacyWaitsQuery+` WHERE invocation_path IS NULL AND iteration_path IS NULL ORDER BY wait_id`); !reflect.DeepEqual(got, legacy.waits) {
		t.Fatalf("migrated waits:\n got %q\nwant %q", got, legacy.waits)
	}
	if got := waitRows(t, database, legacySignalsQuery); !reflect.DeepEqual(got, legacy.signals) {
		t.Fatalf("migrated signals:\n got %q\nwant %q", got, legacy.signals)
	}
	// The same open also brings the journal to version 5 (#334): a scope on
	// the migrated database starts and completes with its attempt.
	scopeRun, err := j.Admit(ctx, AdmissionRequest{RequestKey: "scope-after-migration", Workflow: "orders", ArtifactDigest: "sha256:artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	started, err := j.StartScope(ctx, ScopeRecord{RunID: scopeRun.RunID, Path: "each/0", Kind: "each"})
	if err != nil || started.AttemptID == "" {
		t.Fatalf("scope on the migrated journal: start=%+v err=%v", started, err)
	}
	if err := j.CompleteScope(ctx, scopeRun.RunID, "each/0", started.AttemptID, []byte(`{"v":1}`)); err != nil {
		t.Fatalf("scope on the migrated journal: complete: %v", err)
	}

	runs := map[string]string{}
	for _, key := range []string{"open", "signaled", "early", "canceled", "due", "claimed"} {
		runs[key] = oneRow(t, database, `SELECT run_id FROM journal_runs WHERE request_key = '`+key+`'`)
	}
	send := func(run, signalID, payload string) SignalResult {
		t.Helper()
		result, err := j.Signal(ctx, signal.Envelope{RunID: runs[run], SignalID: signalID, Name: "approval", Principal: "operator", Payload: []byte(payload)}, true)
		if err != nil {
			t.Fatalf("signal %s: %v", signalID, err)
		}
		return result
	}
	if result := send("open", "after-1", `{}`); result != (SignalResult{Accepted: true, Resumed: true}) {
		t.Fatalf("signal to the open legacy wait=%+v", result)
	}
	if _, err := j.ScheduleWait(ctx, WaitRequest{RunID: runs["open"], WaitID: "open-approval-2", Name: "approval", InvocationPath: "approve", IterationPath: "1", DueAt: fixtureBase.Add(72 * time.Hour)}); err != nil {
		t.Fatalf("the legacy run waiting on approval again: %v", err)
	}
	if result := send("open", "after-2", `{}`); result != (SignalResult{Accepted: true, Resumed: true}) {
		t.Fatalf("signal to the second approval wait=%+v; want delivered", result)
	}
	if result := send("signaled", "signal-1", `{"marker":"SYNTHETIC-332-signal-1"}`); result != (SignalResult{Accepted: true, Duplicate: true, Resumed: true}) {
		t.Fatalf("retry of the legacy delivered signal=%+v", result)
	}
	early, err := j.ScheduleWait(ctx, WaitRequest{RunID: runs["early"], WaitID: "early-approval", Name: "approval", InvocationPath: "approve", IterationPath: "root", DueAt: fixtureBase.Add(72 * time.Hour)})
	if err != nil || early.State != waitResumed || early.SignalID != "early-1" {
		t.Fatalf("wait after the legacy pending signal=%+v err=%v", early, err)
	}
	if result := send("canceled", "after-cancel", `{}`); result != (SignalResult{Accepted: true}) {
		t.Fatalf("signal by name after the legacy canceled wait=%+v; want pending for the run's next wait", result)
	}
	if result, err := j.SignalWait(ctx, signal.Envelope{RunID: runs["canceled"], SignalID: "late-2", Name: "approval", Principal: "operator", Payload: []byte(`{}`)}, WaitTarget{WaitID: "canceled-approval"}, true); err != nil || result != (SignalResult{Late: true}) {
		t.Fatalf("signal to the legacy canceled wait=%+v err=%v; want late", result, err)
	}
	claimed, err := j.ClaimDueWaits(ctx, fixtureBase.Add(time.Hour), 10)
	if err != nil || len(claimed) != 1 || claimed[0].WaitID != "due-timer" || claimed[0].InvocationPath != "" {
		t.Fatalf("claimed=%+v err=%v; want only the legacy due timer", claimed, err)
	}
	wantWaits := []string{
		"canceled-approval|approval|||canceled|",
		"claimed-timer|timer|||resumed|",
		"due-timer|timer|||resumed|",
		"early-approval|approval|approve|root|resumed|early-1",
		"open-approval|approval|||resumed|after-1",
		"open-approval-2|approval|approve|1|resumed|after-2",
		"open-review|review|||waiting|",
		"signaled-approval|approval|||resumed|signal-1",
	}
	if got := waitRows(t, database, `SELECT wait_id || '|' || name || '|' || COALESCE(invocation_path, '') || '|' || COALESCE(iteration_path, '') || '|' || state || '|' || signal_id FROM journal_waits ORDER BY wait_id`); !reflect.DeepEqual(got, wantWaits) {
		t.Fatalf("waits after use:\n got %q\nwant %q", got, wantWaits)
	}
	wantSignals := []string{"after-1|stored", "after-2|stored", "after-cancel|pending", "early-1|stored", "late-1|late", "late-2|late", "signal-1|stored"}
	if got := waitRows(t, database, `SELECT signal_id || '|' || state FROM journal_signals ORDER BY signal_id`); !reflect.DeepEqual(got, wantSignals) {
		t.Fatalf("signals after use:\n got %q\nwant %q", got, wantSignals)
	}

	before := waitSchemaDump(t, database)
	if _, err := New(ctx, database, Config{}); err != nil {
		t.Fatal(err)
	}
	if after := waitSchemaDump(t, database); after != before {
		t.Fatalf("reopening changed the migrated journal:\n%s\n%s", before, after)
	}
}

// TestWaitIdentityMigrationSurvivesAKill kills a process (SIGKILL) parked
// inside the journal's schema transaction migrating the aaf633c database,
// just before it commits and just after. Before, the database is exactly
// origin/main's: the old shape, every row, stamp 3. After, it is migrated
// and stamped 5 with every row. Either way the next open leaves it
// migrated with every row.
func TestWaitIdentityMigrationSurvivesAKill(t *testing.T) {
	if os.Getenv("NEWBLOK_332_MIGRATION_CHILD") == "1" {
		hook := func(name string) {
			if name == "schema" {
				journalMarkerAndWait()
			}
		}
		hooks := Hooks{BeforeCommit: hook}
		if os.Getenv("NEWBLOK_JOURNAL_PHASE") == "after" {
			hooks = Hooks{AfterCommit: hook}
		}
		database, _ := newJournalAtPathForChild(os.Getenv("NEWBLOK_JOURNAL_PATH"), Config{Hooks: hooks})
		database.Close()
		return
	}
	for _, phase := range []string{"before", "after"} {
		t.Run(phase, func(t *testing.T) {
			path := decompress(t, waitFixture)
			legacy := legacyWaitDump(t, path)
			marker := filepath.Join(t.TempDir(), "marker")
			command := exec.Command(os.Args[0], "-test.run=^TestWaitIdentityMigrationSurvivesAKill$")
			command.Env = append(os.Environ(), "NEWBLOK_332_MIGRATION_CHILD=1", "NEWBLOK_JOURNAL_PHASE="+phase, "NEWBLOK_JOURNAL_PATH="+path, "NEWBLOK_JOURNAL_MARKER="+marker)
			if err := command.Start(); err != nil {
				t.Fatal(err)
			}
			waitForJournalMarker(t, marker)
			if err := command.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = command.Wait()

			killed := legacyWaitDump(t, path)
			if phase == "before" && !reflect.DeepEqual(killed, legacy) {
				t.Fatalf("a migration killed before commit changed the database:\n got %+v\nwant %+v", killed, legacy)
			}
			if phase == "after" && (killed.stamp != "5|3" || !strings.Contains(killed.scopes, "attempt_id") || strings.Contains(killed.schema, "UNIQUE (run_id, name)") || !reflect.DeepEqual(killed.waits, legacy.waits) || !reflect.DeepEqual(killed.signals, legacy.signals)) {
				t.Fatalf("a migration killed after commit:\n got %+v\nwant migrated with %+v", killed, legacy)
			}
			database, _ := newJournalAtPath(t, path, Config{})
			database.Close()
			reopened := legacyWaitDump(t, path)
			if reopened.stamp != "5|3" || !strings.Contains(reopened.scopes, "attempt_id") || !strings.Contains(reopened.schema, "iteration_path") || !reflect.DeepEqual(reopened.waits, legacy.waits) || !reflect.DeepEqual(reopened.signals, legacy.signals) {
				t.Fatalf("reopened after the kill:\n got %+v\nwant migrated with %+v", reopened, legacy)
			}
		})
	}
}

// TestUnstampedStepIdentityShapeIsClassified4: a journal with step-keyed
// waits and no stamp is classified by that shape as version 4, not
// migrated again, and then upgraded to 5 (#334's scope attempt).
func TestUnstampedStepIdentityShapeIsClassified4(t *testing.T) {
	path := filepath.Join(t.TempDir(), "unstamped.db")
	database, _ := newJournalAtPath(t, path, Config{})
	execAll(t, database, `DELETE FROM blok_schema_versions WHERE component = 'journal'`)
	if _, err := New(context.Background(), database, Config{}); err != nil {
		t.Fatal(err)
	}
	if got := oneRow(t, database, `SELECT version || '|' || upgraded_from FROM blok_schema_versions WHERE component = 'journal'`); got != "5|4" {
		t.Fatalf("journal stamp=%s; want 5 upgraded from 4, classified from its shape", got)
	}
	database.Close()
}

// The columns every journal_waits row had before #332.
const (
	legacyWaitsQuery   = `SELECT wait_id || '|' || run_id || '|' || name || '|' || due_at || '|' || state || '|' || signal_id || '|' || COALESCE(hex(payload_json), 'null') || '|' || created_at || '|' || updated_at FROM journal_waits`
	legacySignalsQuery = `SELECT run_id || '|' || signal_id || '|' || name || '|' || principal || '|' || hex(payload_json) || '|' || state || '|' || created_at FROM journal_signals ORDER BY run_id, signal_id`
)

type waitDump struct {
	stamp, schema, scopes string
	waits, signals        []string
}

// legacyWaitDump reads a database file without opening a journal on it.
func legacyWaitDump(t *testing.T, path string) waitDump {
	t.Helper()
	database, err := (sqlite.Backend{}).Open(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	return waitDump{
		stamp:   oneRow(t, database, `SELECT version || '|' || upgraded_from FROM blok_schema_versions WHERE component = 'journal'`),
		schema:  oneRow(t, database, `SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'journal_waits'`),
		scopes:  oneRow(t, database, `SELECT group_concat(name, ',') FROM pragma_table_info('journal_scopes')`),
		waits:   waitRows(t, database, legacyWaitsQuery+` ORDER BY wait_id`),
		signals: waitRows(t, database, legacySignalsQuery),
	}
}

// waitSchemaDump is the journal's stamp, the wait tables' schema objects
// and every wait and signal row.
func waitSchemaDump(t *testing.T, database store.Database) string {
	t.Helper()
	parts := waitRows(t, database, `SELECT type || ' ' || name || ' ' || COALESCE(sql, '') FROM sqlite_master WHERE tbl_name IN ('journal_waits', 'journal_signals', 'blok_schema_versions') ORDER BY name`)
	parts = append(parts, waitRows(t, database, `SELECT component || '|' || version || '|' || upgraded_from FROM blok_schema_versions ORDER BY component`)...)
	parts = append(parts, waitRows(t, database, legacyWaitsQuery+` ORDER BY wait_id`)...)
	parts = append(parts, waitRows(t, database, `SELECT wait_id || '|' || COALESCE(invocation_path, 'null') || '|' || COALESCE(iteration_path, 'null') FROM journal_waits ORDER BY wait_id`)...)
	return strings.Join(append(parts, waitRows(t, database, legacySignalsQuery)...), "\n")
}

func waitRows(t *testing.T, database store.Database, query string) []string {
	t.Helper()
	var rows []string
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		result, err := tx.Query(query)
		if err != nil {
			return err
		}
		defer result.Close()
		for result.Next() {
			var row string
			if err := result.Scan(&row); err != nil {
				return err
			}
			rows = append(rows, row)
		}
		return result.Err()
	}); err != nil {
		t.Fatal(err)
	}
	return rows
}

func oneRow(t *testing.T, database store.Database, query string) string {
	t.Helper()
	rows := waitRows(t, database, query)
	if len(rows) != 1 {
		t.Fatalf("%s: %d rows; want 1", query, len(rows))
	}
	return rows[0]
}

func decompress(t *testing.T, compressed string) string {
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
