package journal

import (
	"context"
	"database/sql"
	"path/filepath"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

type markRecorder struct {
	store.Database
	mu    sync.Mutex
	marks []bool
}

func (d *markRecorder) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	d.mu.Lock()
	d.marks = append(d.marks, store.IsWriter(ctx))
	d.mu.Unlock()
	return d.Database.WithTx(ctx, fn)
}

func (d *markRecorder) take() []bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	marks := d.marks
	d.marks = nil
	return marks
}

// TestTransitionsAreMarkedWritersAndReadsAreNot: journal transitions write
// first and take turns in the store's writer queue; its reads and schema
// creation do not (#214).
func TestTransitionsAreMarkedWritersAndReadsAreNot(t *testing.T) {
	ctx := context.Background()
	db, err := (sqlite.Backend{}).Open(ctx, filepath.Join(t.TempDir(), "journal.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	marks := &markRecorder{Database: db}
	j, err := New(ctx, marks, Config{})
	if err != nil {
		t.Fatal(err)
	}
	// Schema creation only reads when the tables exist, so it is not
	// queued: a journal opened inside a held turn must not wait for it.
	if got := marks.take(); len(got) != 1 || got[0] {
		t.Fatalf("schema marks=%v; want one unmarked transaction", got)
	}
	run, err := j.Admit(ctx, AdmissionRequest{RequestKey: "request", Workflow: "synthetic", ArtifactDigest: "artifact", Input: []byte(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if got := marks.take(); len(got) != 1 || !got[0] {
		t.Fatalf("Admit marks=%v; want one marked writer", got)
	}
	if _, err := j.Run(ctx, run.RunID); err != nil {
		t.Fatal(err)
	}
	if got := marks.take(); len(got) != 1 || got[0] {
		t.Fatalf("Run marks=%v; want one unmarked read", got)
	}
}
