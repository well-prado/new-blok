package policy

import (
	"context"
	"testing"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/store"
)

type allowAuditReads struct{}

func (allowAuditReads) AuthorizeAuditRead(context.Context, string) error { return nil }

// testAudit composes the mandatory audit journal approvals require, on the
// approval store's own database (ADR 0021).
func testAudit(t testing.TB, database store.Database) *audit.Journal {
	t.Helper()
	journal, err := audit.NewJournal(context.Background(), database, audit.Config{MaxRecords: 10000, Readers: allowAuditReads{}})
	if err != nil {
		t.Fatal(err)
	}
	return journal
}
