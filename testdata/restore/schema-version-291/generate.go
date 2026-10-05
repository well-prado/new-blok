//go:build ignore

// generate writes legacy-main-ef330a3.db: a database written by origin/main
// at ef330a3, the last commit before #291, for the schema version tests. It
// holds every component the framework migrates in place, none of them
// stamped: the journal (schema 3, after #286), audit, the worker queue
// (schema 2, after #290, with one compacted job's tombstone and one pending
// job), approvals, cron and provider records. It must be run from a
// checkout of that commit, never of a later one:
//
//	git worktree add /tmp/main-ef330a3 ef330a3
//	cp testdata/restore/schema-version-291/generate.go /tmp/main-ef330a3/testdata/restore/schema-version-291/
//	(cd /tmp/main-ef330a3 && go run ./testdata/restore/schema-version-291/generate.go /tmp/legacy.db)
//	gzip -9 -n -c /tmp/legacy.db > testdata/restore/schema-version-291/legacy-main-ef330a3.db.gz
//
// The committed database decompresses to 278,528 bytes with sha256
// 06fcd5729e8ad910a0de76654678aeef540fdcd5fb452a838b7f8ef144260636.
//
// Every marker is synthetic. The fixture is a frozen historical snapshot,
// not a generated artifact checked for drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/provider"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
	"github.com/well-prado/new-blok/trigger/cron"
	"github.com/well-prado/new-blok/trigger/worker"
)

type readers struct{}

func (readers) AuthorizeAuditRead(context.Context, string) error { return nil }

type reviewer struct{}

func (reviewer) AuthorizeReview(context.Context, approval.Proposal, []string) (string, error) {
	return "reviewer:carol", nil
}

type clock struct{ now *time.Time }

func (c clock) Now() time.Time                         { return *c.now }
func (c clock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func main() {
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path string) error {
	ctx := context.Background()
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return at }
	db, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		return err
	}
	defer db.Close()
	log, err := audit.NewJournal(ctx, db, audit.Config{Clock: now, MaxRecords: 1000, Readers: readers{}})
	if err != nil {
		return err
	}

	// Journal: one reconciled, completed run decided under tenant-a.
	j, err := journal.New(ctx, db, journal.Config{Clock: now, Audit: log})
	if err != nil {
		return err
	}
	artifact := audit.Digest([]byte("artifact"))
	admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "reconciled-tenant-a", Principal: "alice", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{}`)})
	if err != nil {
		return err
	}
	op, err := j.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: admitted.RunID, ArtifactDigest: artifact, InvocationPath: "charge", IterationPath: "root"}})
	if err != nil {
		return err
	}
	attempt, err := j.StartAttempt(ctx, op.Key)
	if err != nil {
		return err
	}
	if err := j.MarkUncertain(ctx, op.Key, attempt.ID, "synthetic timeout after dispatch"); err != nil {
		return err
	}
	if _, err := j.Reconcile(audit.WithTenant(ctx, "tenant-a"), op.Key, "operator:bob", "provider lookup SYNTHETIC-291-EVIDENCE", []byte(`{"receipt":"SYNTHETIC-291-RESULT"}`), true); err != nil {
		return err
	}
	if err := j.CompleteRun(ctx, admitted.RunID, []byte(`{}`)); err != nil {
		return err
	}

	// Worker queue: one job completed and compacted (its tombstone keeps
	// the dedupe identity), one still pending.
	queue, err := worker.New(ctx, db, now)
	if err != nil {
		return err
	}
	if _, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "job-compacted", Kind: "report", Payload: json.RawMessage(`{"marker":"SYNTHETIC-291-COMPACTED"}`), MaxAttempts: 3}); err != nil {
		return err
	}
	if ran, err := queue.ProcessOnce(ctx, func(context.Context, worker.Tx, worker.Job) error { return nil }); err != nil || !ran {
		return fmt.Errorf("process: ran=%v err=%w", ran, err)
	}
	at = at.Add(time.Hour)
	if report, err := queue.Compact(ctx, worker.Retention{Completed: at}); err != nil || report.Compacted != 1 {
		return fmt.Errorf("compact: %+v err=%w", report, err)
	}
	if _, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "job-pending", Kind: "report", Payload: json.RawMessage(`{"marker":"SYNTHETIC-291-PENDING"}`), MaxAttempts: 3}); err != nil {
		return err
	}

	// Approval: one recorded decision.
	approvals, err := approval.NewJournalStore(ctx, db, approval.Config{Authorizer: reviewer{}, Audit: log, Clock: now, MaxDecisions: 100})
	if err != nil {
		return err
	}
	proposal := approval.Proposal{Action: "charge@1", Workflow: "orders@1", RunID: "run-1", InvocationPath: "charge", IterationPath: "root", InputDigest: approval.BytesDigest([]byte(`{"amount":1}`)), ArtifactDigest: approval.BytesDigest([]byte("artifact")), Effects: []string{"payment:charge"}, Scope: []string{"payment:write"}}
	if _, err := approvals.Record(ctx, "approval-1", proposal, proposal.Scope, at.Add(time.Hour), true); err != nil {
		return err
	}

	// Cron: one schedule's cursor.
	scheduler, err := cron.New(ctx, db, queue, nil, clock{now: &at})
	if err != nil {
		return err
	}
	if _, err := scheduler.Add(ctx, cron.Schedule{Name: "nightly", Spec: "0 3 * * *", TimeZone: "UTC", Kind: "report", Payload: json.RawMessage(`{}`), InputSchema: []byte(`{"type":"object"}`), Principal: trigger.Principal{ID: "system:cron"}}); err != nil {
		return err
	}

	// Provider records: one business row and its outbox event.
	records, err := provider.NewRecords(ctx, db)
	if err != nil {
		return err
	}
	if _, err := records.Execute(ctx, provider.DatabaseInput{Key: "provider-op-1", RecordID: "record-1", Value: "SYNTHETIC-291-PROVIDER"}); err != nil {
		return err
	}
	fmt.Printf("run=%s operation=%s\n", admitted.RunID, op.Key)
	return nil
}
