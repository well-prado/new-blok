package journal

import (
	"context"
	"testing"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/store"
)

type allowAuditReads struct{}

func (allowAuditReads) AuthorizeAuditRead(context.Context, string) error { return nil }

// newTestAudit composes the mandatory audit journal Reconcile and
// DecideUpgrade require, on the journal's own database.
func newTestAudit(t testing.TB, database store.Database) *audit.Journal {
	t.Helper()
	journal, err := audit.NewJournal(context.Background(), database, audit.Config{MaxRecords: 1000, Readers: allowAuditReads{}})
	if err != nil {
		t.Fatal(err)
	}
	return journal
}
