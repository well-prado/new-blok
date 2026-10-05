package audit_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// secretMarker appears in every synthetic secret-shaped fixture string. No
// audit row may ever contain it.
const secretMarker = "SYNTHETIC"

type readerKey struct{}

// tenantReaders authenticates the reader from ctx (the trusted channel the
// application's boundary would set) and grants it the listed tenants.
type tenantReaders map[string][]string

func (r tenantReaders) AuthorizeAuditRead(ctx context.Context, tenant string) error {
	reader, _ := ctx.Value(readerKey{}).(string)
	for _, granted := range r[reader] {
		if granted == tenant {
			return nil
		}
	}
	return errors.New("synthetic: reader not authorized")
}

func asReader(ctx context.Context, reader string) context.Context {
	return context.WithValue(ctx, readerKey{}, reader)
}

// boundedMirror is an optional log sink with a fixed capacity: it accepts
// until full, then drops, like a telemetry queue under pressure.
type boundedMirror struct {
	capacity int64
	accepted atomic.Int64
	panics   bool
}

func (m *boundedMirror) Offer(audit.Record) bool {
	if m.panics {
		panic("synthetic mirror failure")
	}
	if m.accepted.Load() >= m.capacity {
		return false
	}
	m.accepted.Add(1)
	return true
}

type reviewer struct{ name string }

func (r reviewer) AuthorizeReview(context.Context, approval.Proposal, []string) (string, error) {
	return r.name, nil
}

type rig struct {
	t        *testing.T
	ctx      context.Context
	path     string
	db       store.Database
	audit    *audit.Journal
	approval *approval.JournalStore
	journal  *journal.Journal
	mirror   *boundedMirror
	clock    time.Time
}

type rigOptions struct {
	maxRecords   int
	reviewer     string
	noAudit      bool
	mirror       *boundedMirror
	minRetention time.Duration
	hold         func(audit.Record) bool
	runHold      func(journal.RetainedRun) bool
	readers      tenantReaders
}

func openRig(t *testing.T, path string, options rigOptions) *rig {
	t.Helper()
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	r := &rig{t: t, ctx: ctx, path: path, db: db, clock: time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)}
	if options.maxRecords == 0 {
		options.maxRecords = 1000
	}
	if options.reviewer == "" {
		options.reviewer = "reviewer:alice"
	}
	if options.readers == nil {
		options.readers = tenantReaders{"auditor:all": {"", "tenant-a", "tenant-b"}}
	}
	if options.mirror == nil {
		options.mirror = &boundedMirror{capacity: 1 << 30}
	}
	r.mirror = options.mirror
	clock := func() time.Time { return r.clock }
	if !options.noAudit {
		r.audit, err = audit.NewJournal(ctx, db, audit.Config{Clock: clock, MaxRecords: options.maxRecords, Readers: options.readers, Mirror: options.mirror, MinRetention: options.minRetention, Hold: options.hold})
		if err != nil {
			t.Fatal(err)
		}
	}
	r.journal, err = journal.New(ctx, db, journal.Config{Clock: clock, Audit: r.audit, Hold: options.runHold})
	if err != nil {
		t.Fatal(err)
	}
	r.approval, err = approval.NewJournalStore(ctx, db, approval.Config{Audit: r.audit, Authorizer: reviewer{options.reviewer}, Clock: clock, MaxDecisions: 1000})
	if err != nil && !(options.noAudit && errors.Is(err, audit.ErrRequired)) {
		t.Fatal(err)
	}
	return r
}

func newRig(t *testing.T, options rigOptions) *rig {
	t.Helper()
	return openRig(t, filepath.Join(t.TempDir(), "journal.db"), options)
}

func (r *rig) count(query string, args ...any) int {
	r.t.Helper()
	var n int
	err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error { return tx.QueryRowContext(r.ctx, query, args...).Scan(&n) })
	if err != nil {
		if strings.Contains(err.Error(), "no such table") {
			return 0
		}
		r.t.Fatal(err)
	}
	return n
}

// auditBytes returns every stored audit row, raw, for leak checks.
func (r *rig) auditBytes() string {
	r.t.Helper()
	var all strings.Builder
	err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(r.ctx, `SELECT id, tenant, run_id, record FROM audit_records_v1`)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, tenant, runID string
			var record []byte
			if err := rows.Scan(&id, &tenant, &runID, &record); err != nil {
				return err
			}
			all.WriteString(id + tenant + runID + string(record))
		}
		return rows.Err()
	})
	if err != nil && !strings.Contains(err.Error(), "no such table") {
		r.t.Fatal(err)
	}
	return all.String()
}

// breakAudit makes the audit store unavailable: every insert aborts inside
// SQLite, exactly as a failing audit write would.
func (r *rig) breakAudit() {
	r.t.Helper()
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.ctx, `CREATE TRIGGER audit_outage BEFORE INSERT ON audit_records_v1 BEGIN SELECT RAISE(ABORT, 'synthetic audit outage'); END`)
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
}

func (r *rig) repairAudit() {
	r.t.Helper()
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(r.ctx, `DROP TRIGGER audit_outage`)
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
}

// seedAudit appends one record directly, for capacity and retention cases.
func (r *rig) seedAudit(record audit.Record) {
	r.t.Helper()
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		_, _, err := r.audit.Append(r.ctx, tx, record)
		return err
	}); err != nil {
		r.t.Fatal(err)
	}
}

func digest(text string) string { return audit.Digest([]byte(text)) }

func proposal(runID string) approval.Proposal {
	return approval.Proposal{Action: "payments/charge@1.0.0", Workflow: "orders@1", RunID: runID, InvocationPath: "charge", IterationPath: "root",
		InputDigest: digest(`{"amountCents":100}`), ArtifactDigest: digest("artifact"), Effects: []string{"payment:charge"}, Scope: []string{"payment:write"}}
}

func (r *rig) approve(id, runID string, approved bool) (approval.Decision, error) {
	if r.approval == nil {
		return approval.Decision{}, audit.ErrRequired
	}
	return r.approval.Record(r.ctx, id, proposal(runID), []string{"payment:write"}, r.clock.Add(time.Hour), approved)
}

func (r *rig) authorizes(id, runID string) bool {
	if r.approval == nil {
		return false
	}
	return approval.Authorize(r.ctx, r.approval, r.clock, approval.Request{Proposal: proposal(runID), ApprovalID: id}) == nil
}

// uncertainEffect admits a run and leaves one effect uncertain: the state a
// reconciliation decides.
func (r *rig) uncertainEffect(key string) (string, journal.Operation) {
	r.t.Helper()
	run, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: key, Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		r.t.Fatal(err)
	}
	op, err := r.journal.BeginEffect(r.ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: run.RunID, ArtifactDigest: digest("artifact"), InvocationPath: "charge-" + key, IterationPath: "root"}})
	if err != nil {
		r.t.Fatal(err)
	}
	attempt, err := r.journal.StartAttempt(r.ctx, op.Key)
	if err != nil {
		r.t.Fatal(err)
	}
	if err := r.journal.MarkUncertain(r.ctx, op.Key, attempt.ID, "synthetic timeout after dispatch"); err != nil {
		r.t.Fatal(err)
	}
	return run.RunID, op
}

func (r *rig) reconcile(op journal.Operation, actor string) error {
	_, err := r.journal.Reconcile(r.ctx, op.Key, actor, "provider lookup "+secretMarker+"-evidence-0001", []byte(`{"charged":true,"receipt":"`+secretMarker+`-receipt"}`), true)
	return err
}

func (r *rig) artifacts() (string, string) {
	r.t.Helper()
	from, to := digest("artifact-v1"), digest("artifact-v2")
	for version, d := range map[string]string{"1.0.0": from, "2.0.0": to} {
		if err := r.journal.RegisterArtifact(r.ctx, journal.ArtifactRecord{Digest: d, Version: version, ManifestJSON: []byte(`{}`)}); err != nil {
			r.t.Fatal(err)
		}
	}
	if _, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "in-flight", Principal: "alice", Workflow: "orders", ArtifactDigest: from, Input: []byte(`{}`)}); err != nil {
		r.t.Fatal(err)
	}
	return from, to
}

func errorName(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, audit.ErrSensitive):
		return "audit_sensitive"
	case errors.Is(err, audit.ErrCapacity):
		return "audit_capacity"
	case errors.Is(err, audit.ErrUnavailable):
		return "audit_unavailable"
	case errors.Is(err, audit.ErrRequired):
		return "audit_required"
	case errors.Is(err, journal.ErrUpgradeWouldDiscard):
		return "upgrade_would_discard"
	default:
		return "unexpected: " + err.Error()
	}
}

type expected struct {
	Error               string `json:"error"`
	Decisions           int    `json:"decisions"`
	AuditRecords        int    `json:"auditRecords"`
	Authorized          int    `json:"authorized"`
	Reconciliations     int    `json:"reconciliations"`
	CommittedOperations int    `json:"committedOperations"`
	UncertainOperations int    `json:"uncertainOperations"`
	Plans               int    `json:"plans"`
	Mirrored            int    `json:"mirrored"`
}

// TestAuditDecisionFixtures runs each predeclared case: one real decision
// against a fresh SQLite store under one audit condition. Every refusal
// leaves nothing applied: no decision row, no reconciliation, the effect
// still uncertain, no plan.
func TestAuditDecisionFixtures(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Cases []struct {
			Name, Operation, Condition string
			Expected                   expected
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &file); err != nil {
		t.Fatal(err)
	}
	failures := 0
	for _, item := range file.Cases {
		if item.Expected.Error != "" {
			failures++
		}
		t.Run(item.Name, func(t *testing.T) {
			options := rigOptions{}
			switch item.Condition {
			case "capacity":
				options.maxRecords = 1
			case "not-composed":
				options.noAudit = true
			case "sensitive-actor":
				options.reviewer = "Bearer " + secretMarker + "-reviewer-0001"
			}
			r := newRig(t, options)
			if item.Condition == "capacity" {
				r.seedAudit(audit.Record{ID: "seed", Kind: audit.KindDeployment, Actor: "operator:seed", Subject: "seed", Outcome: audit.OutcomeAccepted})
				r.mirror.accepted.Store(0)
			}
			actor := "operator:bob"
			if item.Condition == "sensitive-actor" {
				actor = base64.StdEncoding.EncodeToString([]byte("token=" + secretMarker + "-actor-0001"))
			}
			var opKey string
			var from, to string
			switch item.Operation {
			case "reconcile":
				_, op := r.uncertainEffect("reconcile")
				opKey = op.Key
			case "upgrade-retain", "upgrade-discard":
				from, to = r.artifacts()
			}
			if item.Condition == "unavailable" {
				r.breakAudit()
			}
			var got expected
			switch item.Operation {
			case "approve", "reject":
				_, err = r.approve("decision-1", "run-1", item.Operation == "approve")
				if r.authorizes("decision-1", "run-1") {
					got.Authorized = 1
				}
			case "reconcile":
				err = r.reconcile(journal.Operation{Key: opKey}, actor)
			case "upgrade-retain", "upgrade-discard":
				var plan journal.UpgradePlan
				plan, err = r.journal.DecideUpgrade(r.ctx, actor, from, to, item.Operation == "upgrade-retain")
				if plan.ToDigest != "" {
					got.Plans = 1
				}
			default:
				t.Fatalf("unknown operation %q", item.Operation)
			}
			got.Error = errorName(err)
			got.Decisions = r.count(`SELECT COUNT(*) FROM approval_decisions_v1`)
			got.AuditRecords = r.count(`SELECT COUNT(*) FROM audit_records_v1`)
			got.Reconciliations = r.count(`SELECT COUNT(*) FROM journal_reconciliations`)
			got.CommittedOperations = r.count(`SELECT COUNT(*) FROM journal_operations WHERE state = 'committed'`)
			got.UncertainOperations = r.count(`SELECT COUNT(*) FROM journal_operations WHERE state = 'uncertain'`)
			got.Mirrored = int(r.mirror.accepted.Load())
			if got != item.Expected {
				t.Fatalf("got %+v\nwant %+v", got, item.Expected)
			}
			if leaked := r.auditBytes(); strings.Contains(leaked, secretMarker) {
				t.Fatalf("audit stored a secret-shaped or raw value: %s", leaked)
			}
			if r.audit != nil {
				if _, err := r.audit.Verify(r.ctx, r.approval, r.journal); err != nil {
					t.Fatalf("verify: %v", err)
				}
			}
		})
	}
	if failures < 9 || len(file.Cases)-failures < 4 {
		t.Fatalf("fixture balance: %d failure cases, %d success cases", failures, len(file.Cases)-failures)
	}
}

// TestAuditRecoversAfterOutage shows the refusal is not a lost decision: the
// same approval and reconciliation retried after the store recovers are
// recorded once each, with their audit.
func TestAuditRecoversAfterOutage(t *testing.T) {
	r := newRig(t, rigOptions{})
	_, op := r.uncertainEffect("retry")
	r.breakAudit()
	if _, err := r.approve("decision-retry", "run-1", true); !errors.Is(err, audit.ErrUnavailable) {
		t.Fatalf("approval during outage: %v", err)
	}
	if err := r.reconcile(op, "operator:bob"); !errors.Is(err, audit.ErrUnavailable) {
		t.Fatalf("reconcile during outage: %v", err)
	}
	if r.authorizes("decision-retry", "run-1") {
		t.Fatal("an unaudited decision authorized a dispatch")
	}
	r.repairAudit()
	if _, err := r.approve("decision-retry", "run-1", true); err != nil {
		t.Fatal(err)
	}
	if _, err := r.approve("decision-retry", "run-1", true); err != nil {
		t.Fatalf("idempotent retry: %v", err)
	}
	if err := r.reconcile(op, "operator:bob"); err != nil {
		t.Fatal(err)
	}
	if !r.authorizes("decision-retry", "run-1") {
		t.Fatal("recorded decision does not authorize")
	}
	if n := r.count(`SELECT COUNT(*) FROM audit_records_v1`); n != 2 {
		t.Fatalf("audit records=%d want 2", n)
	}
	if n := r.count(`SELECT COUNT(*) FROM journal_operations WHERE state = 'committed'`); n != 1 {
		t.Fatalf("committed operations=%d", n)
	}
}

// TestOptionalMirrorDropCannotRemoveAudit drives decisions through a mirror
// (the optional log path) that holds one record and then drops, and through
// one that panics. Every decision keeps its durable record; only the
// optional copies are lost, and they are counted.
func TestOptionalMirrorDropCannotRemoveAudit(t *testing.T) {
	for _, panics := range []bool{false, true} {
		mirror := &boundedMirror{capacity: 1, panics: panics}
		r := newRig(t, rigOptions{mirror: mirror})
		const approvals, reconciliations = 10, 5
		for i := range approvals {
			if _, err := r.approve("decision-"+string(rune('a'+i)), "run-1", true); err != nil {
				t.Fatal(err)
			}
		}
		for i := range reconciliations {
			_, op := r.uncertainEffect("mirror-" + string(rune('a'+i)))
			if err := r.reconcile(op, "operator:bob"); err != nil {
				t.Fatal(err)
			}
		}
		durable := r.count(`SELECT COUNT(*) FROM audit_records_v1`)
		verified, err := r.audit.Verify(r.ctx, r.approval, r.journal)
		stats := r.audit.Stats()
		wantMirrored := uint64(1)
		if panics {
			wantMirrored = 0
		}
		if durable != approvals+reconciliations || verified != durable || err != nil {
			t.Fatalf("panics=%v durable=%d verified=%d err=%v; want %d", panics, durable, verified, err, approvals+reconciliations)
		}
		if stats.Mirrored != wantMirrored || stats.MirrorDropped != uint64(approvals+reconciliations)-wantMirrored {
			t.Fatalf("panics=%v mirror stats=%+v", panics, stats)
		}
	}
}

// TestAuditRetentionRespectsActiveRunsAndLegalPolicy prunes a store holding
// records for a still-active run, a completed run, a legally held record and
// one younger than the legal minimum. Only the completed, unheld, old record
// goes; the active run's record goes only after the run completes.
func TestAuditRetentionRespectsActiveRunsAndLegalPolicy(t *testing.T) {
	held := func(r audit.Record) bool { return r.Reason == "legal_hold" }
	r := newRig(t, rigOptions{minRetention: 24 * time.Hour, hold: held})
	activeRun, _ := r.uncertainEffect("active")
	completed, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: "done", Principal: "alice", Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.journal.CompleteRun(r.ctx, completed.RunID, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	old := r.clock.Add(-30 * 24 * time.Hour)
	record := func(id, runID, reason string, at time.Time) audit.Record {
		return audit.Record{ID: id, Kind: audit.KindApproval, Actor: "reviewer:alice", Subject: id, RunID: runID, Outcome: audit.OutcomeApproved, Reason: reason, At: at}
	}
	r.seedAudit(record("active-run", activeRun, "", old))
	r.seedAudit(record("completed-run", completed.RunID, "", old))
	r.seedAudit(record("held", completed.RunID, "legal_hold", old))
	r.seedAudit(record("young", completed.RunID, "", r.clock.Add(-time.Hour)))
	r.seedAudit(record("no-run", "", "", old))

	if _, err := r.audit.Prune(r.ctx, r.clock, nil); !errors.Is(err, audit.ErrRequired) {
		t.Fatalf("prune without run activity: %v", err)
	}
	report, err := r.audit.Prune(r.ctx, r.clock, r.journal)
	if err != nil {
		t.Fatal(err)
	}
	if report.Removed != 2 || report.KeptActive != 1 || report.KeptHeld != 1 {
		t.Fatalf("report=%+v; want removed 2 (completed-run, no-run), kept active 1, held 1", report)
	}
	remaining := map[string]bool{}
	records, _, err := r.audit.List(asReader(r.ctx, "auditor:all"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range records {
		remaining[item.ID] = true
	}
	if len(remaining) != 3 || !remaining["active-run"] || !remaining["held"] || !remaining["young"] {
		t.Fatalf("remaining=%v", remaining)
	}
	// The run is reconciled and completed: its record becomes deletable.
	if err := r.reconcile(journal.Operation{Key: r.operationOf(activeRun)}, "operator:bob"); err != nil {
		t.Fatal(err)
	}
	if err := r.journal.CompleteRun(r.ctx, activeRun, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	report, err = r.audit.Prune(r.ctx, r.clock, r.journal)
	if err != nil || report.Removed != 1 || report.KeptActive != 0 || report.KeptHeld != 1 {
		t.Fatalf("second prune report=%+v err=%v", report, err)
	}
	if _, err := r.audit.Verify(r.ctx); err != nil {
		t.Fatalf("verify after prune: %v", err)
	}
}

func (r *rig) operationOf(runID string) string {
	r.t.Helper()
	var key string
	if err := r.db.WithTx(r.ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(r.ctx, `SELECT operation_key FROM journal_operations WHERE run_id = ?`, runID).Scan(&key)
	}); err != nil {
		r.t.Fatal(err)
	}
	return key
}

// TestRunCompactionHonoursLegalHold: journal compaction (#49) deletes only
// completed runs, and never one the application's legal hold keeps. Audit
// records are never touched by compaction.
func TestRunCompactionHonoursLegalHold(t *testing.T) {
	r := newRig(t, rigOptions{runHold: func(run journal.RetainedRun) bool { return run.Principal == "litigation-hold" }})
	var ids []string
	for _, principal := range []string{"alice", "litigation-hold"} {
		run, err := r.journal.Admit(r.ctx, journal.AdmissionRequest{RequestKey: principal, Principal: principal, Workflow: "orders", ArtifactDigest: digest("artifact"), Input: []byte(`{}`)})
		if err != nil {
			t.Fatal(err)
		}
		if err := r.journal.CompleteRun(r.ctx, run.RunID, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, run.RunID)
	}
	if _, err := r.approve("decision-held", ids[1], true); err != nil {
		t.Fatal(err)
	}
	report, err := r.journal.Compact(r.ctx, r.clock.Add(time.Hour))
	if err != nil || report.RemovedRuns != 1 || report.HeldRuns != 1 {
		t.Fatalf("report=%+v err=%v", report, err)
	}
	if _, err := r.journal.Run(r.ctx, ids[0]); err == nil {
		t.Fatal("unheld completed run was kept")
	}
	if _, err := r.journal.Run(r.ctx, ids[1]); err != nil {
		t.Fatalf("held run was compacted: %v", err)
	}
	if n := r.count(`SELECT COUNT(*) FROM audit_records_v1`); n != 1 {
		t.Fatalf("compaction touched audit: %d records", n)
	}
}

// TestRestoreAuditWithDurableState backs up a store holding decisions, then
// makes more decisions, then restores the backup. The restored store has
// exactly the pre-backup audit, verified, and it agrees with the restored
// durable state: every reconciled effect has its record and the later
// decisions are absent from both.
func TestRestoreAuditWithDurableState(t *testing.T) {
	directory := t.TempDir()
	r := openRig(t, filepath.Join(directory, "source.db"), rigOptions{})
	if _, err := r.approve("before-backup", "run-1", true); err != nil {
		t.Fatal(err)
	}
	_, reconciled := r.uncertainEffect("before")
	if err := r.reconcile(reconciled, "operator:bob"); err != nil {
		t.Fatal(err)
	}
	from, to := r.artifacts()
	if _, err := r.journal.DecideUpgrade(r.ctx, "operator:carol", from, to, true); err != nil {
		t.Fatal(err)
	}
	backup := filepath.Join(directory, "backup.db")
	if err := r.journal.Backup(r.ctx, backup); err != nil {
		t.Fatal(err)
	}
	if _, err := r.approve("after-backup", "run-1", true); err != nil {
		t.Fatal(err)
	}
	_, later := r.uncertainEffect("after")
	if err := r.reconcile(later, "operator:bob"); err != nil {
		t.Fatal(err)
	}
	if n := r.count(`SELECT COUNT(*) FROM audit_records_v1`); n != 5 {
		t.Fatalf("source audit records=%d want 5", n)
	}
	restoredPath := filepath.Join(directory, "restored.db")
	if err := (sqlite.Backend{}).Restore(r.ctx, backup, restoredPath); err != nil {
		t.Fatal(err)
	}
	restored := openRig(t, restoredPath, rigOptions{})
	verified, err := restored.audit.Verify(restored.ctx, restored.approval, restored.journal)
	if err != nil || verified != 3 {
		t.Fatalf("restored audit verified=%d err=%v; want 3", verified, err)
	}
	records, _, err := restored.audit.List(asReader(restored.ctx, "auditor:all"), "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[audit.Kind]int{}
	for _, record := range records {
		kinds[record.Kind]++
	}
	if len(records) != 3 || kinds[audit.KindApproval] != 1 || kinds[audit.KindReconciliation] != 1 || kinds[audit.KindDeployment] != 1 {
		t.Fatalf("restored records=%+v", records)
	}
	if !restored.authorizes("before-backup", "run-1") || restored.authorizes("after-backup", "run-1") {
		t.Fatal("restored decisions disagree with the backup point")
	}
	// Durable state and audit agree: each committed reconciliation has its
	// record, and no record names a reconciliation the store does not have.
	orphans := restored.count(`SELECT COUNT(*) FROM journal_reconciliations WHERE 'reconcile:' || operation_key NOT IN (SELECT id FROM audit_records_v1)`)
	ghosts := restored.count(`SELECT COUNT(*) FROM audit_records_v1 WHERE kind = 'reconciliation.decision' AND substr(id, 11) NOT IN (SELECT operation_key FROM journal_reconciliations)`)
	if orphans != 0 || ghosts != 0 || restored.count(`SELECT COUNT(*) FROM journal_reconciliations`) != 1 {
		t.Fatalf("restored state/audit disagree: orphans=%d ghosts=%d", orphans, ghosts)
	}
	if restored.count(`SELECT COUNT(*) FROM journal_operations WHERE operation_key = ? AND state = 'uncertain'`, later.Key) != 0 {
		t.Fatal("an operation from after the backup exists in the restored store")
	}
	// A restored store whose audit lost a record is detected, not trusted.
	if err := restored.db.WithTx(restored.ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(restored.ctx, `DELETE FROM audit_records_v1 WHERE kind = 'approval.decision'`); err != nil {
			return err
		}
		_, err := tx.ExecContext(restored.ctx, `UPDATE audit_meta_v1 SET value = value - 1 WHERE name = 'records'`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.audit.Verify(restored.ctx, restored.approval, restored.journal); !errors.Is(err, audit.ErrMismatch) {
		t.Fatalf("restored audit missing a decision's record verified: %v", err)
	}
	// A damaged restored audit table is detected, not trusted.
	if err := restored.db.WithTx(restored.ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(restored.ctx, `UPDATE audit_records_v1 SET record = replace(record, 'operator:bob', 'operator:eve')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := restored.audit.Verify(restored.ctx); !errors.Is(err, audit.ErrCorrupt) {
		t.Fatalf("tampered audit verified: %v", err)
	}
}

// TestAuditReadIsTenantAuthorized: two tenants' decisions in one store. A
// reader sees only tenants it is granted; another tenant, an unknown reader
// and an unauthenticated context get ErrDenied and no records.
func TestAuditReadIsTenantAuthorized(t *testing.T) {
	readers := tenantReaders{"auditor:a": {"tenant-a"}, "auditor:b": {"tenant-b"}}
	r := newRig(t, rigOptions{readers: readers})
	for _, tenant := range []string{"tenant-a", "tenant-b"} {
		ctx := audit.WithTenant(r.ctx, tenant)
		if _, err := r.approval.Record(ctx, "decision-"+tenant, proposal("run-"+tenant), []string{"payment:write"}, r.clock.Add(time.Hour), true); err != nil {
			t.Fatal(err)
		}
	}
	own, _, err := r.audit.List(asReader(r.ctx, "auditor:a"), "tenant-a", "", 0)
	if err != nil || len(own) != 1 || own[0].Tenant != "tenant-a" || own[0].Subject != "decision-tenant-a" {
		t.Fatalf("own tenant records=%+v err=%v", own, err)
	}
	for name, ctx := range map[string]context.Context{
		"other tenant":    asReader(r.ctx, "auditor:a"),
		"unknown reader":  asReader(r.ctx, "auditor:z"),
		"unauthenticated": r.ctx,
	} {
		records, next, err := r.audit.List(ctx, "tenant-b", "", 0)
		if !errors.Is(err, audit.ErrDenied) || records != nil || next != "" {
			t.Fatalf("%s: records=%+v next=%q err=%v", name, records, next, err)
		}
	}
	if records, _, err := r.audit.List(asReader(r.ctx, "auditor:b"), "tenant-b", "", 0); err != nil || len(records) != 1 || records[0].Tenant != "tenant-b" {
		t.Fatalf("tenant-b reader records=%+v err=%v", records, err)
	}
}

// TestAuditPagination pins cursor paging within a tenant.
func TestAuditPagination(t *testing.T) {
	r := newRig(t, rigOptions{})
	for i := range 5 {
		if _, err := r.approve("page-"+string(rune('a'+i)), "run-1", true); err != nil {
			t.Fatal(err)
		}
	}
	ctx := asReader(r.ctx, "auditor:all")
	var seen []string
	cursor := ""
	for range 10 {
		records, next, err := r.audit.List(ctx, "", cursor, 2)
		if err != nil {
			t.Fatal(err)
		}
		for _, record := range records {
			seen = append(seen, record.Subject)
		}
		if next == "" {
			break
		}
		cursor = next
	}
	if strings.Join(seen, ",") != "page-a,page-b,page-c,page-d,page-e" {
		t.Fatalf("paged=%v", seen)
	}
}
