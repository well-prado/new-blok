package approval

import (
	"context"
	"testing"

	"github.com/well-prado/new-blok/contract/audit"
	"github.com/well-prado/new-blok/store"
)

type allowAuditReads struct{}

func (allowAuditReads) AuthorizeAuditRead(context.Context, string) error { return nil }

// withAudit composes the mandatory audit journal on the store's database.
func withAudit(t testing.TB, database store.Database, cfg Config) Config {
	t.Helper()
	journal, err := audit.NewJournal(context.Background(), database, audit.Config{MaxRecords: 1000, Readers: allowAuditReads{}})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Audit = journal
	return cfg
}
