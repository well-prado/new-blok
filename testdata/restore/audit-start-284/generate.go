//go:build ignore

// generate writes legacy-main-71b8351.db: a database written by origin/main
// at 71b8351, the last commit before #280 introduced mandatory audit, for
// the audit start marker tests (#284). It holds three approval decisions
// (two approved, one rejected) and two reconciliations of uncertain
// effects (one on a completed run, one on a run still accepted), all
// written by the pre-audit code path, so none has an audit record and the
// database has no audit tables. It must be run from a checkout of that
// commit, never of a later one:
//
//	git worktree add /tmp/main-71b8351 71b8351
//	mkdir -p /tmp/main-71b8351/testdata/restore/audit-start-284
//	cp testdata/restore/audit-start-284/generate.go /tmp/main-71b8351/testdata/restore/audit-start-284/
//	(cd /tmp/main-71b8351 && go run ./testdata/restore/audit-start-284/generate.go /tmp/legacy.db)
//	gzip -9 -n -c /tmp/legacy.db > testdata/restore/audit-start-284/legacy-main-71b8351.db.gz
//
// Every marker is synthetic. The fixture is a frozen historical snapshot,
// not a generated artifact checked for drift.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/well-prado/new-blok/contract/approval"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store/sqlite"
)

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
	at := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	now := func() time.Time { return at }
	db, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		return err
	}
	defer db.Close()

	j, err := journal.New(ctx, db, journal.Config{Clock: now})
	if err != nil {
		return err
	}
	artifact := approval.BytesDigest([]byte("artifact"))
	for _, label := range []string{"completed", "accepted"} {
		admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: "legacy-" + label, Principal: "alice", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{}`)})
		if err != nil {
			return err
		}
		op, err := j.BeginEffect(ctx, journal.EffectIntent{Identity: journal.OperationIdentity{RunID: admitted.RunID, ArtifactDigest: artifact, InvocationPath: "charge-" + label, IterationPath: "root"}})
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
		if _, err := j.Reconcile(ctx, op.Key, "operator:bob", "provider lookup SYNTHETIC-284-EVIDENCE-"+label, []byte(`{"receipt":"SYNTHETIC-284-RESULT-`+label+`"}`), true); err != nil {
			return err
		}
		if label == "completed" {
			if err := j.CompleteRun(ctx, admitted.RunID, []byte(`{}`)); err != nil {
				return err
			}
		}
		fmt.Printf("run=%s operation=%s\n", admitted.RunID, op.Key)
	}

	approvals, err := approval.NewJournalStore(ctx, db, approval.Config{Authorizer: reviewer{}, Clock: now, MaxDecisions: 100})
	if err != nil {
		return err
	}
	for i, approved := range []bool{true, true, false} {
		id := fmt.Sprintf("legacy-approval-%d", i+1)
		proposal := approval.Proposal{Action: "charge@1", Workflow: "orders@1", RunID: "legacy-run", InvocationPath: "charge", IterationPath: "root", InputDigest: approval.BytesDigest([]byte(fmt.Sprintf(`{"amount":%d}`, i+1))), ArtifactDigest: artifact, Effects: []string{"payment:charge"}, Scope: []string{"payment:write"}}
		if _, err := approvals.Record(ctx, id, proposal, proposal.Scope, at.Add(time.Hour), approved); err != nil {
			return err
		}
		fmt.Printf("approval=%s\n", id)
	}
	return nil
}
