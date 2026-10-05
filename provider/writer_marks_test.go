package provider

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

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

// TestRecordsExecuteIsAMarkedWriter: Execute reserves the writer first and
// takes its turn in the store's writer queue (#214).
func TestRecordsExecuteIsAMarkedWriter(t *testing.T) {
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "records.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	marks := &markRecorder{Database: db}
	records, err := NewRecords(ctx, marks)
	if err != nil {
		t.Fatal(err)
	}
	marks.marks = nil
	if _, err := records.Execute(ctx, DatabaseInput{Key: "op", RecordID: "record", Value: "value"}); err != nil {
		t.Fatal(err)
	}
	if len(marks.marks) != 1 || !marks.marks[0] {
		t.Fatalf("Execute marks=%v; want one marked writer", marks.marks)
	}
}
