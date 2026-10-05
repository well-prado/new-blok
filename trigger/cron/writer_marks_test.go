package cron_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/cron"
)

type markRecorder struct {
	store.Database
	marks []bool
}

func (d *markRecorder) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	d.marks = append(d.marks, store.IsWriter(ctx))
	return d.Database.WithTx(ctx, fn)
}

// TestCursorWritesAreMarkedWriters: adding a schedule and recording a tick's
// cursors write first and take turns in the store's writer queue (#214).
func TestCursorWritesAreMarkedWriters(t *testing.T) {
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "cron.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	marks := &markRecorder{Database: db}
	clock := newClock(at(10, 30))
	s, err := cron.New(ctx, marks, &memorySubmitter{keys: map[string]bool{}}, nil, clock)
	if err != nil {
		t.Fatal(err)
	}
	marks.marks = nil
	if _, err := s.Add(ctx, schedule("minutely", "* * * * *")); err != nil {
		t.Fatal(err)
	}
	clock.Set(at(10, 31))
	if _, err := s.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(marks.marks) < 2 {
		t.Fatalf("Add and Tick made %d transactions; want at least 2", len(marks.marks))
	}
	for i, marked := range marks.marks {
		if !marked {
			t.Fatalf("transaction %d of %v was not a marked writer", i, marks.marks)
		}
	}
}
