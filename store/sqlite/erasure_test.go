package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
)

// The marker is synthetic. It is long enough to need its own cell and is
// repeated past a page, so it also lands on an overflow page.
const deletedMarker = "SYNTHETIC-281-DELETED-CONTENT"

func fileHolds(t *testing.T, path, marker string) []string {
	t.Helper()
	var found []string
	for _, file := range []string{path, path + "-wal", path + "-shm"} {
		data, err := os.ReadFile(file)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(data, []byte(marker)) {
			found = append(found, filepath.Base(file))
		}
	}
	return found
}

// writeAndDelete inserts marked rows, updates one (which frees its old
// cell), then deletes them all, each in its own committed transaction.
func writeAndDelete(t *testing.T, ctx context.Context, exec func(string, ...any) error) {
	t.Helper()
	long := strings.Repeat(deletedMarker+" ", 400)
	for _, statement := range []struct {
		query string
		args  []any
	}{
		{"CREATE TABLE content (id INTEGER PRIMARY KEY, value TEXT NOT NULL)", nil},
		{"INSERT INTO content (id, value) VALUES (1, ?), (2, ?), (3, 'kept')", []any{deletedMarker, long}},
		{"UPDATE content SET value = ? WHERE id = 1", []any{deletedMarker + " updated and longer than before"}},
		{"DELETE FROM content WHERE id IN (1, 2)", nil},
	} {
		if err := exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
}

// TestDeletedContentLeavesTheFiles: with the backend's secure deletion and
// one log purge, deleted content is in neither the database file nor its
// log. The control, the same writes without secure_delete, still holds it
// after the same checkpoint: deletion alone does not erase.
func TestDeletedContentLeavesTheFiles(t *testing.T) {
	ctx := context.Background()
	t.Run("backend", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "erased.db")
		database, err := (Backend{}).Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		defer database.Close()
		writeAndDelete(t, ctx, func(query string, args ...any) error {
			return database.WithTx(ctx, func(tx *sql.Tx) error {
				_, err := tx.ExecContext(ctx, query, args...)
				return err
			})
		})
		if found := fileHolds(t, path, deletedMarker); len(found) == 0 {
			t.Fatal("fixture: before the purge the log must still hold the content")
		}
		purger, ok := store.PurgerOf(database)
		if !ok {
			t.Fatal("the SQLite store has no purge capability")
		}
		if err := purger.PurgeLog(ctx); err != nil {
			t.Fatal(err)
		}
		if found := fileHolds(t, path, deletedMarker); len(found) != 0 {
			t.Fatalf("deleted content still in %v", found)
		}
	})
	t.Run("control without secure_delete", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "plain.db")
		plain, err := sql.Open("sqlite", strings.Replace(dsn(path, time.Second), "&_pragma="+secureDelete, "", 1))
		if err != nil {
			t.Fatal(err)
		}
		defer plain.Close()
		plain.SetMaxOpenConns(1)
		writeAndDelete(t, ctx, func(query string, args ...any) error {
			_, err := plain.ExecContext(ctx, query, args...)
			return err
		})
		if _, err := plain.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
			t.Fatal(err)
		}
		if found := fileHolds(t, path, deletedMarker); len(found) == 0 {
			t.Fatal("control: without secure_delete the deleted content was expected in the file")
		}
	})
}

// TestSecureDeleteOnEveryConnection: the setting is per connection, so it
// must come from the DSN, not from one configuring statement.
func TestSecureDeleteOnEveryConnection(t *testing.T) {
	ctx := context.Background()
	for _, path := range []string{filepath.Join(t.TempDir(), "pool.db"), ":memory:"} {
		database, err := (Backend{}).Open(ctx, path)
		if err != nil {
			t.Fatal(err)
		}
		pool := database.(*connection).database
		var conns []*sql.Conn
		for range 8 {
			conn, err := pool.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			conns = append(conns, conn)
			var on int
			if err := conn.QueryRowContext(ctx, "PRAGMA secure_delete").Scan(&on); err != nil || on != 1 {
				t.Fatalf("%s connection %d: secure_delete=%d err=%v", path, len(conns), on, err)
			}
		}
		for _, conn := range conns {
			_ = conn.Close()
		}
		if purger, ok := store.PurgerOf(database); !ok || purger.PurgeLog(ctx) != nil || purger.PurgeFree(ctx) != nil {
			t.Fatalf("%s: purge unavailable or failing", path)
		}
		_ = database.Close()
	}
}

// TestPurgeLogRefusesWhileAReaderNeedsIt: an open read snapshot keeps the
// log; the purge reports ErrBusy instead of claiming it truncated.
func TestPurgeLogRefusesWhileAReaderNeedsIt(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "reader.db")
	database, err := (Backend{BusyTimeout: 50 * time.Millisecond}).Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	writeAndDelete(t, ctx, func(query string, args ...any) error {
		return database.WithTx(ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, query, args...)
			return err
		})
	})
	held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- database.WithTx(ctx, func(tx *sql.Tx) error {
			var n int
			if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM content").Scan(&n); err != nil {
				return err
			}
			close(held)
			<-release
			return nil
		})
	}()
	<-held
	purger, _ := store.PurgerOf(database)
	busyErr := purger.PurgeLog(ctx)
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !errors.Is(busyErr, store.ErrBusy) {
		t.Fatalf("purge beside a reader: %v, want store.ErrBusy", busyErr)
	}
	if err := purger.PurgeLog(ctx); err != nil {
		t.Fatal(err)
	}
}
