//go:build ignore

// generate_alpha writes alpha-79ee0a7.db: legacy-main-71b8351.db (see
// generate.go) upgraded and used by v0.1.0-alpha (79ee0a7), the release
// that shipped mandatory audit without a start marker (#284). Opening it
// there created the audit tables (audit schema 1) and migrated the journal;
// the alpha binary then recorded one approval decision and one
// reconciliation, each with its audit record. The five pre-audit decisions
// still have none. It must be run from a checkout of v0.1.0-alpha, never of
// a later commit:
//
//	git worktree add /tmp/alpha v0.1.0-alpha
//	mkdir -p /tmp/alpha/testdata/restore/audit-start-284
//	cp testdata/restore/audit-start-284/generate_alpha.go /tmp/alpha/testdata/restore/audit-start-284/
//	gunzip -c testdata/restore/audit-start-284/legacy-main-71b8351.db.gz > /tmp/alpha.db
//	(cd /tmp/alpha && go run ./testdata/restore/audit-start-284/generate_alpha.go /tmp/alpha.db)
//	gzip -9 -n -c /tmp/alpha.db > testdata/restore/audit-start-284/alpha-79ee0a7.db.gz
//
// Every marker is synthetic. The fixture is a frozen historical snapshot,
// not a generated artifact checked for drift.
package main

import (
	"context"
	"errors"
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
	return "reviewer:carol", nil
}

func main() {
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path string) error {
	ctx := context.Background()
	at := time.Date(2026, 10, 6, 12, 0, 0, 0, time.UTC)
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
	j, err := journal.New(ctx, db, journal.Config{Clock: now, Audit: log})
	if err != nil {
		return err
	}
	approvals, err := approval.NewJournalStore(ctx, db, approval.Config{Authorizer: reviewer{}, Audit: log, Clock: now, MaxDecisions: 100})
	if err != nil {
		return err
	}
	// What #284 reports: the alpha binary fails Verify on the upgraded
	// database before it records anything.
	if _, err := log.Verify(ctx, approvals, j); !errors.Is(err, audit.ErrMismatch) {
		return fmt.Errorf("alpha verify of the upgraded database: %v, want ErrMismatch", err)
	}

	artifact := approval.BytesDigest([]byte("artifact"))
	admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "alpha-reconciled", Principal: "alice", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{}`)})
	if err != nil {
		return err
	}
	op, err := j.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: admitted.RunID, ArtifactDigest: artifact, InvocationPath: "charge-alpha", IterationPath: "root"}})
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
	if _, err := j.Reconcile(ctx, op.Key, "operator:bob", "provider lookup SYNTHETIC-284-EVIDENCE-alpha", []byte(`{"receipt":"SYNTHETIC-284-RESULT-alpha"}`), true); err != nil {
		return err
	}
	proposal := approval.Proposal{Action: "charge@1", Workflow: "orders@1", RunID: "alpha-run", InvocationPath: "charge", IterationPath: "root", InputDigest: approval.BytesDigest([]byte(`{"amount":9}`)), ArtifactDigest: artifact, Effects: []string{"payment:charge"}, Scope: []string{"payment:write"}}
	if _, err := approvals.Record(ctx, "alpha-approval-1", proposal, proposal.Scope, at.Add(time.Hour), true); err != nil {
		return err
	}
	n, err := log.Verify(ctx)
	if err != nil || n != 2 {
		return fmt.Errorf("alpha records=%d err=%v, want 2", n, err)
	}
	fmt.Printf("run=%s operation=%s approval=alpha-approval-1 records=%d\n", admitted.RunID, op.Key, n)
	return nil
}
