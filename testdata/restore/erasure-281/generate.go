//go:build ignore

// generate writes legacy-main-ea3eec6.db: a journal written by origin/main
// at ea3eec6, before #281, for the erasure migration test. It must be run
// from a checkout of that commit, never of a later one:
//
//	git worktree add /tmp/main-ea3eec6 ea3eec6
//	cp testdata/restore/erasure-281/generate.go /tmp/main-ea3eec6/testdata/restore/erasure-281/
//	(cd /tmp/main-ea3eec6 && go run ./testdata/restore/erasure-281/generate.go /tmp/legacy.db)
//	gzip -9 -n -c /tmp/legacy.db > testdata/restore/erasure-281/legacy-main-ea3eec6.db.gz
//
// The committed database decompresses to 184,320 bytes with sha256
// 6c718946b11ce53a6839794d360d4181131b5d2583e37e2cdb3809eac25e04b3.
//
// Every marker is synthetic. The fixture is a frozen historical snapshot,
// not a generated artifact checked for drift: regenerating it with later
// code would no longer produce a legacy database.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store/sqlite"
)

type readers struct{}

func (readers) AuthorizeAuditRead(context.Context, string) error { return nil }

type reviewer struct{}

func (reviewer) AuthorizeReview(context.Context, approval.Proposal, []string) (string, error) {
	return "reviewer:alice", nil
}

func main() {
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path string) error {
	ctx := context.Background()
	clock := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return clock }
	db, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		return err
	}
	defer db.Close()
	log, err := audit.NewJournal(ctx, db, audit.Config{Clock: now, MaxRecords: 1000, Readers: readers{}})
	if err != nil {
		return err
	}
	j, err := journal.New(ctx, db, journal.Config{Clock: now, Audit: log})
	if err != nil {
		return err
	}
	approvals, err := approval.NewJournalStore(ctx, db, approval.Config{Audit: log, Authorizer: reviewer{}, Clock: now, MaxDecisions: 1000})
	if err != nil {
		return err
	}
	artifact := audit.Digest([]byte("artifact"))

	// A completed run main compacts: its tombstone keeps the output.
	compacted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "request-SYNTHETIC-281-LEGACY-REQUEST", Principal: "alice", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{"note":"SYNTHETIC-281-LEGACY-INPUT"}`)})
	if err != nil {
		return err
	}
	if err := j.CompleteRun(ctx, compacted.RunID, []byte(`{"note":"SYNTHETIC-281-LEGACY-OUTPUT"}`)); err != nil {
		return err
	}
	if _, err := approvals.Record(ctx, "decision-legacy", approval.Proposal{Action: "payments/charge@1.0.0", Workflow: "orders@1", RunID: compacted.RunID, InvocationPath: "charge", IterationPath: "root", InputDigest: audit.Digest([]byte("{}")), ArtifactDigest: artifact, Effects: []string{"payment:charge"}, Scope: []string{"payment:write"}}, []string{"payment:write"}, clock.Add(time.Hour), true); err != nil {
		return err
	}
	if report, err := j.Compact(ctx, clock.Add(time.Hour)); err != nil || report.RemovedRuns != 1 {
		return fmt.Errorf("compact: %+v %v", report, err)
	}

	// A reconciled, completed run main cannot compact (foreign key).
	reconciled, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "reconciled", Principal: "alice", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{}`)})
	if err != nil {
		return err
	}
	op, err := j.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: reconciled.RunID, ArtifactDigest: artifact, InvocationPath: "charge", IterationPath: "root"}})
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
	if _, err := j.Reconcile(ctx, op.Key, "operator:bob", "provider lookup SYNTHETIC-281-LEGACY-EVIDENCE", []byte(`{"receipt":"SYNTHETIC-281-LEGACY-RESULT"}`), true); err != nil {
		return err
	}
	if err := j.CompleteRun(ctx, reconciled.RunID, []byte(`{"note":"SYNTHETIC-281-LEGACY-RECONCILED-OUTPUT"}`)); err != nil {
		return err
	}
	if _, err := j.Compact(ctx, clock.Add(time.Hour)); err == nil {
		return fmt.Errorf("main compacted a reconciled run; this is not the legacy code")
	} else {
		fmt.Println("expected legacy compaction failure:", err)
	}
	return nil
}
