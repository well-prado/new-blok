package approval

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

type markRecorder struct {
	store.Database
	marks []bool
}

func (d *markRecorder) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	d.marks = append(d.marks, store.IsWriter(ctx))
	return d.Database.WithTx(ctx, fn)
}

// TestRecordIsAMarkedWriterAndGetIsNot: Record reserves the writer first and
// takes its turn in the store's writer queue; Get reads beside it (#214).
func TestRecordIsAMarkedWriterAndGetIsNot(t *testing.T) {
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	marks := &markRecorder{Database: db}
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	s, err := NewJournalStore(ctx, marks, withAudit(t, marks, Config{Authorizer: reviewer{true}, Clock: func() time.Time { return now }, MaxDecisions: 2}))
	if err != nil {
		t.Fatal(err)
	}
	marks.marks = nil
	p := proposal()
	if _, err := s.Record(ctx, "a", p, p.Scope, now.Add(time.Hour), true); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if len(marks.marks) != 2 || !marks.marks[0] || marks.marks[1] {
		t.Fatalf("Record/Get marks=%v; want [true false]", marks.marks)
	}
}
