package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// pooledSettingsHeld is how many pooled connections the settings test holds
// at once: every connection the pool may open (SetMaxOpenConns), so the one
// configure ran its PRAGMAs on and the ones the pool opened later are all
// checked.
const pooledSettingsHeld = 8

// TestEveryPooledConnectionKeepsTheDurabilitySettings (#337): the journal's
// ack-after-commit guarantee rests on synchronous=FULL, and a killed child
// process cannot tell a flushed commit from one still in the OS cache, so
// the crash tests stay green with synchronous=OFF. This test reads the
// settings back instead, on several pooled connections held at once, since
// each one applies them for itself when it connects: one connection with
// the setting proves nothing about the others.
func TestEveryPooledConnectionKeepsTheDurabilitySettings(t *testing.T) {
	ctx := context.Background()
	const busyTimeout = 1234 * time.Millisecond
	database, err := (Backend{BusyTimeout: busyTimeout}).Open(ctx, filepath.Join(t.TempDir(), "settings.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	pool := database.(*connection).database
	if open := pool.Stats().MaxOpenConnections; open != pooledSettingsHeld {
		t.Fatalf("the pool opens up to %d connections; the test holds %d", open, pooledSettingsHeld)
	}
	held := make([]*sql.Conn, 0, pooledSettingsHeld)
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	for range pooledSettingsHeld {
		conn, err := pool.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, conn)
	}
	if inUse := pool.Stats().InUse; inUse != pooledSettingsHeld {
		t.Fatalf("%d connections in use; want %d distinct ones held at once", inUse, pooledSettingsHeld)
	}
	for index, conn := range held {
		read := func(pragma string) string {
			t.Helper()
			var value string
			if err := conn.QueryRowContext(ctx, "PRAGMA "+pragma).Scan(&value); err != nil {
				t.Fatalf("connection %d: PRAGMA %s: %v", index, pragma, err)
			}
			return value
		}
		// synchronous=FULL reads back as 2; OFF is 0, NORMAL 1, EXTRA 3.
		if got := read("synchronous"); got != "2" {
			t.Errorf("connection %d: synchronous=%s, want 2 (FULL)", index, got)
		}
		// A literal, not the journalMode constant, so a changed constant
		// fails here.
		if got := read("journal_mode"); got != "wal" {
			t.Errorf("connection %d: journal_mode=%s, want wal", index, got)
		}
		if got := read("foreign_keys"); got != "1" {
			t.Errorf("connection %d: foreign_keys=%s, want 1", index, got)
		}
		if got, want := read("busy_timeout"), "1234"; got != want {
			t.Errorf("connection %d: busy_timeout=%s, want %s (the configured %v)", index, got, want, busyTimeout)
		}
		if got := read("secure_delete"); got != "1" {
			t.Errorf("connection %d: secure_delete=%s, want 1 (#281)", index, got)
		}
	}
}
