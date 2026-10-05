package journal

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/well-prado/new-blok/store"
	"github.com/well-prado/new-blok/store/sqlite"
)

// TestConcurrentOpensMigrateWithoutFailing: several processes opening a
// journal that still needs a column added must all start. Adding a column
// reads the schema before it writes, and SQLite cannot make such a
// transaction wait for another writer, so without a retry all but one
// opener fail busy at once (#235, as #233 for the worker queue).
func TestConcurrentOpensMigrateWithoutFailing(t *testing.T) {
	ctx := context.Background()
	const handles, rounds = 6, 10
	failures := 0
	var first error
	for round := range rounds {
		path := filepath.Join(t.TempDir(), fmt.Sprintf("journal-%d.db", round))
		seed, err := (sqlite.Backend{}).Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := New(ctx, seed, Config{}); err != nil {
			t.Fatal(err)
		}
		// Make the journal predate its newest column.
		if err := seed.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `ALTER TABLE journal_scopes DROP COLUMN input_json`)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		_ = seed.Close()
		databases := make([]store.Database, handles)
		for i := range databases {
			if databases[i], err = (sqlite.Backend{}).Open(ctx, path); err != nil {
				t.Fatal(err)
			}
		}
		start := make(chan struct{})
		errs := make(chan error, handles)
		var group sync.WaitGroup
		for _, database := range databases {
			group.Add(1)
			go func() {
				defer group.Done()
				<-start
				_, err := New(ctx, database, Config{})
				errs <- err
			}()
		}
		close(start)
		group.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				failures++
				if first == nil {
					first = err
				}
			}
		}
		for _, database := range databases {
			_ = database.Close()
		}
	}
	if failures != 0 {
		t.Fatalf("%d of %d concurrent opens failed; first: %v", failures, handles*rounds, first)
	}
}
