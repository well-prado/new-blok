package audit_test

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/signal"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store/sqlite"
)

// Erasure (#281): once a run is compacted past its retention, no byte of its
// content may remain anywhere in the database's files, while the audit and
// its cross-check still prove the run's decisions existed. Every marker is
// synthetic and distinct per run, so a leak names the run and the field.

const erasureMarker = "SYNTHETIC-281-"

// marker names one content field of one run.
func marker(run, field string) string { return erasureMarker + run + "-" + field }

// contentRun is a completed run whose every content field carries a marker
// for label: request key, principal, input, effect input and result,
// uncertainty evidence, scope input and output, checkpoint state, signal
// payload, child result, join results and run output. reconciled also
// leaves its effect uncertain and reconciles it with marked evidence and a
// marked provider result.
type contentRun struct {
	RunID     string
	Operation journal.Operation
	Markers   []string
}

func (r *rig) contentRun(label string, reconciled bool) contentRun {
	r.t.Helper()
	ctx := r.ctx
	m := func(field string) string { return marker(label, field) }
	quoted := func(field string) json.RawMessage {
		data, _ := json.Marshal(map[string]string{"note": m(field)})
		return data
	}
	fields := []string{"REQUEST", "PRINCIPAL", "INPUT", "EFFECT-INPUT", "SCOPE-INPUT", "SCOPE-OUTPUT", "CHECKPOINT", "SIGNAL", "CHILD", "JOIN", "OUTPUT"}
	run, err := r.journal.Admit(ctx, journal.AdmissionRequest{RequestKey: "request-" + m("REQUEST"), Principal: "principal-" + m("PRINCIPAL"), Workflow: "orders", ArtifactDigest: digest("artifact"), Input: quoted("INPUT")})
	if err != nil {
		r.t.Fatal(err)
	}
	op, err := r.journal.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: run.RunID, ArtifactDigest: digest("artifact"), InvocationPath: "charge", IterationPath: "root"}, Input: quoted("EFFECT-INPUT")})
	if err != nil {
		r.t.Fatal(err)
	}
	attempt, err := r.journal.StartAttempt(ctx, op.Key)
	if err != nil {
		r.t.Fatal(err)
	}
	if reconciled {
		fields = append(fields, "UNCERTAIN", "EVIDENCE", "RESULT")
		if err := r.journal.MarkUncertain(ctx, op.Key, attempt.ID, "timeout "+m("UNCERTAIN")); err != nil {
			r.t.Fatal(err)
		}
		if _, err := r.journal.Reconcile(ctx, op.Key, "operator:bob", "provider lookup "+m("EVIDENCE"), quoted("RESULT"), true); err != nil {
			r.t.Fatal(err)
		}
	} else {
		fields = append(fields, "RESULT")
		if err := r.journal.CommitEffect(ctx, journal.EffectCommit{OperationKey: op.Key, AttemptID: attempt.ID, Result: quoted("RESULT")}); err != nil {
			r.t.Fatal(err)
		}
	}
	scope, err := r.journal.StartScope(ctx, journal.ScopeRecord{RunID: run.RunID, Path: "step", Kind: "step", Input: quoted("SCOPE-INPUT")})
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.CompleteScope(ctx, run.RunID, "step", scope.AttemptID, quoted("SCOPE-OUTPUT")); err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.SaveCheckpoint(ctx, journal.Checkpoint{RunID: run.RunID, ArtifactDigest: digest("artifact"), CheckpointDigest: digest("checkpoint"), State: quoted("CHECKPOINT")}); err != nil {
		r.t.Fatal(err)
	}
	if _, err := r.journal.ScheduleWait(ctx, journal.WaitRequest{RunID: run.RunID, WaitID: "wait-" + label, Name: "approved", InvocationPath: "approve", IterationPath: "root", DueAt: r.clock.Add(time.Minute)}); err != nil {
		r.t.Fatal(err)
	}
	if _, err := r.journal.Signal(ctx, signal.Envelope{RunID: run.RunID, SignalID: "signal-" + label, Name: "approved", Payload: quoted("SIGNAL"), Principal: "alice"}, true); err != nil {
		r.t.Fatal(err)
	}
	// The resumed run consumes its wakeup before it completes (#332).
	token, err := r.journal.TakeRunLease(ctx, run.RunID, r.clock)
	if err == nil {
		err = r.journal.AcknowledgeWait(ctx, "wait-"+label, token)
	}
	if err == nil {
		err = r.journal.ReleaseRunLease(ctx, run.RunID, token)
	}
	if err != nil {
		r.t.Fatal(err)
	}
	// The child record must name a run the journal holds (#334). The child
	// carries no marker and stays accepted, so compaction leaves it alone.
	child, err := r.journal.Admit(ctx, journal.AdmissionRequest{RequestKey: "child-of-" + label, Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.RecordChild(ctx, journal.ChildRecord{RunID: run.RunID, Path: "child", ChildRunID: child.RunID, State: "completed", Result: quoted("CHILD")}); err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.RecordJoin(ctx, journal.JoinRecord{RunID: run.RunID, Path: "join", Expected: 1, Completed: 1, Results: []json.RawMessage{quoted("JOIN")}}); err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.CompleteRun(ctx, run.RunID, quoted("OUTPUT")); err != nil {
		r.t.Fatal(err)
	}
	markers := make([]string, len(fields))
	for i, field := range fields {
		markers[i] = m(field)
	}
	return contentRun{RunID: run.RunID, Operation: op, Markers: markers}
}

// effectRun is a completed run with a committed effect and nothing else,
// the shape origin/main could already compact.
func (r *rig) effectRun(label string) contentRun {
	r.t.Helper()
	m := func(field string) string { return marker(label, field) }
	quoted := func(field string) json.RawMessage {
		data, _ := json.Marshal(map[string]string{"note": m(field)})
		return data
	}
	run, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "request-" + m("REQUEST"), Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: quoted("INPUT")})
	if err != nil {
		r.t.Fatal(err)
	}
	op, err := r.journal.BeginEffect(r.ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: run.RunID, ArtifactDigest: digest("artifact"), InvocationPath: "charge", IterationPath: "root"}, Input: quoted("EFFECT-INPUT")})
	if err != nil {
		r.t.Fatal(err)
	}
	attempt, err := r.journal.StartAttempt(r.ctx, op.Key)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.CommitEffect(r.ctx, journal.EffectCommit{OperationKey: op.Key, AttemptID: attempt.ID, Result: quoted("RESULT")}); err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.CompleteRun(r.ctx, run.RunID, quoted("OUTPUT")); err != nil {
		r.t.Fatal(err)
	}
	var markers []string
	for _, field := range []string{"REQUEST", "INPUT", "EFFECT-INPUT", "RESULT", "OUTPUT"} {
		markers = append(markers, m(field))
	}
	return contentRun{RunID: run.RunID, Operation: op, Markers: markers}
}

// fileMarkers byte-searches the database file and, when present, its
// write-ahead log and shared-memory index, and returns every marker found
// with the file that holds it.
func fileMarkers(t *testing.T, path string, markers []string) []string {
	t.Helper()
	var found []string
	for _, file := range []string{path, path + "-wal", path + "-shm"} {
		data, err := os.ReadFile(file)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range markers {
			if bytes.Contains(data, []byte(m)) {
				found = append(found, filepath.Base(file)+":"+m)
			}
		}
	}
	return found
}

// rowMarkers searches every text and blob column of every table, through
// SQL, for erasureMarker. It sees live rows only, not freed pages.
func (r *rig) rowMarkers() []string {
	r.t.Helper()
	var found []string
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
				values := make([]any, len(columns))
				pointers := make([]any, len(columns))
				for i := range values {
					pointers[i] = &values[i]
				}
				if err := rows.Scan(pointers...); err != nil {
					rows.Close()
					return err
				}
				for i, value := range values {
					var text string
					switch v := value.(type) {
					case string:
						text = v
					case []byte:
						text = string(v)
					}
					if strings.Contains(text, erasureMarker) {
						found = append(found, table+"."+columns[i])
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
	return found
}

func (r *rig) mustVerify(want int) {
	r.t.Helper()
	if n, err := r.audit.Verify(r.ctx, r.approval, r.journal); err != nil || (want >= 0 && n != want) {
		r.t.Fatalf("verify=%d err=%v, want %d records", n, err, want)
	}
}

// TestCompactedRunLeavesNoContentInTheDatabaseFiles: the run shape
// origin/main could already compact. Its tombstone kept the output, and
// deleted rows stayed readable in freed space and in the log.
func TestCompactedRunLeavesNoContentInTheDatabaseFiles(t *testing.T) {
	r := newRig(t, rigOptions{})
	run := r.effectRun("EFFECT")
	if _, err := r.approve("decision-effect", run.RunID, true); err != nil {
		t.Fatal(err)
	}
	if found := fileMarkers(t, r.path, run.Markers); len(found) == 0 {
		t.Fatal("fixture: the run's content must be in the database files before compaction")
	}
	r.clock = r.clock.Add(48 * time.Hour)
	report, err := r.journal.Compact(r.ctx, r.clock.Add(-24*time.Hour))
	if err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	if found := r.rowMarkers(); len(found) != 0 {
		t.Fatalf("compacted content in rows: %v", found)
	}
	if found := fileMarkers(t, r.path, run.Markers); len(found) != 0 {
		t.Fatalf("compacted content in the database files: %v", found)
	}
	if _, err := r.journal.Run(r.ctx, run.RunID); !errors.Is(err, journal.ErrNotFound) {
		t.Fatalf("compacted run still readable: %v", err)
	}
	r.mustVerify(1)
	// The tombstone still proves the run ended, so its approval is prunable.
	if report, err := r.audit.Prune(r.ctx, r.clock, r.journal); err != nil || report.Removed != 1 {
		t.Fatalf("prune after compaction=%+v err=%v", report, err)
	}
	r.mustVerify(0)
}

// TestReconciledRunPastRetentionCompacts: origin/main failed every
// compaction that reached a reconciled run, on a foreign key, so such a run
// and its evidence were kept forever.
func TestReconciledRunPastRetentionCompacts(t *testing.T) {
	r := newRig(t, rigOptions{})
	run := r.contentRun("RECONCILED", true)
	r.mustVerify(1)
	r.clock = r.clock.Add(48 * time.Hour)
	report, err := r.journal.Compact(r.ctx, r.clock.Add(-24*time.Hour))
	if err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	if found := r.rowMarkers(); len(found) != 0 {
		t.Fatalf("compacted content in rows: %v", found)
	}
	if found := fileMarkers(t, r.path, run.Markers); len(found) != 0 {
		t.Fatalf("compacted content in the database files: %v", found)
	}
	// Verify still knows the reconciliation existed, and its record stays.
	r.mustVerify(1)
	if n := r.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE operation_key = ?`, run.Operation.Key); n != 1 {
		t.Fatalf("reconciliation identity rows=%d, want 1", n)
	}
	// A re-delivery is a duplicate and returns no erased content.
	again, err := r.journal.Reconcile(r.ctx, run.Operation.Key, "operator:bob", "provider lookup "+marker("RECONCILED", "EVIDENCE"), []byte(`{}`), true)
	if err != nil || !again.Duplicate || again.Evidence != "" || len(again.Result) != 0 || again.Actor != "" {
		t.Fatalf("re-delivery after erasure=%+v err=%v", again, err)
	}
	if found := r.rowMarkers(); len(found) != 0 {
		t.Fatalf("re-delivery stored content again: %v", found)
	}
	r.mustVerify(1)
}

// TestLegalHoldKeepsRunContent: a held run keeps all of its content,
// evidence included, while an unheld one beside it is erased.
func TestLegalHoldKeepsRunContent(t *testing.T) {
	r := newRig(t, rigOptions{runHold: func(run journal.RetainedRun) bool { return strings.Contains(run.Principal, "HELD") }})
	held := r.contentRun("HELD", true)
	erased := r.contentRun("ERASED", true)
	r.clock = r.clock.Add(48 * time.Hour)
	report, err := r.journal.Compact(r.ctx, r.clock.Add(-24*time.Hour))
	if err != nil || report.RemovedRuns != 1 || report.HeldRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	if found := fileMarkers(t, r.path, erased.Markers); len(found) != 0 {
		t.Fatalf("erased run's content in the database files: %v", found)
	}
	kept, err := r.journal.Run(r.ctx, held.RunID)
	if err != nil || !strings.Contains(string(kept.Output), marker("HELD", "OUTPUT")) {
		t.Fatalf("held run=%+v err=%v", kept, err)
	}
	var evidence string
	var result []byte
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(r.ctx, `SELECT evidence, result_json FROM journal_reconciliations WHERE operation_key = ?`, held.Operation.Key).Scan(&evidence, &result)
	}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(evidence, marker("HELD", "EVIDENCE")) || !strings.Contains(string(result), marker("HELD", "RESULT")) {
		t.Fatalf("held reconciliation lost content: evidence=%q result=%s", evidence, result)
	}
	again, err := r.journal.Reconcile(r.ctx, held.Operation.Key, "operator:bob", "x", []byte(`{}`), true)
	if err != nil || !again.Duplicate || !strings.Contains(again.Evidence, marker("HELD", "EVIDENCE")) {
		t.Fatalf("held re-delivery=%+v err=%v", again, err)
	}
	r.mustVerify(2)
}

// TestRestoreAfterErasure: a backup taken after compaction holds no erased
// content and verifies. A backup taken before it is a copy that still holds
// the content: restoring it brings the content back (the guarantee is
// stated in ADR 0021 §7), and the next compaction under the same policy
// erases it again, with the audit consistent throughout.
func TestRestoreAfterErasure(t *testing.T) {
	directory := t.TempDir()
	r := openRig(t, filepath.Join(directory, "source.db"), rigOptions{})
	run := r.contentRun("RESTORE", true)
	before := filepath.Join(directory, "before.db")
	if err := r.journal.Backup(r.ctx, before); err != nil {
		t.Fatal(err)
	}
	r.clock = r.clock.Add(48 * time.Hour)
	cutoff := r.clock.Add(-24 * time.Hour)
	if report, err := r.journal.Compact(r.ctx, cutoff); err != nil || report.RemovedRuns != 1 {
		t.Fatalf("compact=%+v err=%v", report, err)
	}
	after := filepath.Join(directory, "after.db")
	if err := r.journal.Backup(r.ctx, after); err != nil {
		t.Fatal(err)
	}
	if found := fileMarkers(t, after, run.Markers); len(found) != 0 {
		t.Fatalf("backup after erasure holds content: %v", found)
	}
	for name, backup := range map[string]string{"after": after, "before": before} {
		restoredPath := filepath.Join(directory, "restored-"+name+".db")
		if err := (sqlite.Backend{}).Restore(r.ctx, backup, restoredPath); err != nil {
			t.Fatal(err)
		}
		restored := openRig(t, restoredPath, rigOptions{})
		restored.clock = r.clock
		restored.mustVerify(1)
		if name == "after" {
			if found := fileMarkers(t, restoredPath, run.Markers); len(found) != 0 {
				t.Fatalf("restore of the erased backup holds content: %v", found)
			}
			continue
		}
		// The pre-erasure copy re-imports the content: this is the honest
		// limit, pinned so a change to it is noticed.
		if found := restored.rowMarkers(); len(found) == 0 {
			t.Fatal("restore of a pre-erasure backup was expected to hold the content")
		}
		if report, err := restored.journal.Compact(restored.ctx, cutoff); err != nil || report.RemovedRuns != 1 {
			t.Fatalf("re-compact after restore=%+v err=%v", report, err)
		}
		if found := fileMarkers(t, restoredPath, run.Markers); len(found) != 0 {
			t.Fatalf("re-compaction after restore left content: %v", found)
		}
		restored.mustVerify(1)
	}
	if n, err := r.audit.Verify(r.ctx, r.approval, r.journal); err != nil || n != 1 {
		t.Fatalf("source verify=%d err=%v", n, err)
	}
}
