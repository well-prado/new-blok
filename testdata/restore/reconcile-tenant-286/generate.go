//go:build ignore

// generate writes legacy-main-99a9228.db: a journal written by origin/main
// at 99a9228, after #281 and before #286, for the reconciliation tenant
// migration test. Its journal_reconciliations table has no tenant column.
// It must be run from a checkout of that commit, never of a later one:
//
//	git worktree add /tmp/main-99a9228 99a9228
//	cp testdata/restore/reconcile-tenant-286/generate.go /tmp/main-99a9228/testdata/restore/reconcile-tenant-286/
//	(cd /tmp/main-99a9228 && go run ./testdata/restore/reconcile-tenant-286/generate.go /tmp/legacy.db)
//	gzip -9 -n -c /tmp/legacy.db > testdata/restore/reconcile-tenant-286/legacy-main-99a9228.db.gz
//
// The committed database decompresses to 188,416 bytes with sha256
// 2387002781401df5395f1a6340dcd7cd64102858de83742763d8bf1d1bc91ae9.
//
// It holds two reconciled, completed runs: one decided under tenant
// "tenant-a" and one under the system tenant "". Every marker is synthetic.
// The fixture is a frozen historical snapshot, not a generated artifact
// checked for drift.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/internal/journal"
	"github.com/well-prado/new-blok/store/sqlite"
)

type readers struct{}

func (readers) AuthorizeAuditRead(context.Context, string) error { return nil }

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
	artifact := audit.Digest([]byte("artifact"))
	for _, c := range []struct{ request, tenant, marker string }{
		{"reconciled-tenant-a", "tenant-a", "A"},
		{"reconciled-system", "", "SYSTEM"},
	} {
		admitted, err := j.Admit(ctx, journal.AdmissionRequest{RequestKey: c.request, Principal: "alice", Workflow: "orders", ArtifactDigest: artifact, Input: []byte(`{}`)})
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
		decided := audit.WithTenant(ctx, c.tenant)
		if _, err := j.Reconcile(decided, op.Key, "operator:bob", "provider lookup SYNTHETIC-286-"+c.marker+"-EVIDENCE", []byte(`{"receipt":"SYNTHETIC-286-`+c.marker+`-RESULT"}`), true); err != nil {
			return err
		}
		if err := j.CompleteRun(ctx, admitted.RunID, []byte(`{}`)); err != nil {
			return err
		}
		fmt.Printf("%s run=%s operation=%s tenant=%q\n", c.request, admitted.RunID, op.Key, c.tenant)
	}
	return nil
}
