package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
)

// panicBusyTimeout is the busy timeout of the stores in this file. A write
// that has to wait for a transaction leaked by a panic waits it out and
// fails with store.ErrBusy; one that does not finishes in well under it.
const panicBusyTimeout = 2 * time.Second

// promptly is how long a write after the exit may take: a quarter of the
// busy timeout, so a write that waited for the lock cannot pass.
const promptly = panicBusyTimeout / 4

type panicValue struct{ name string }

var errPanicValue = errors.New("panic value that is an error")

// exit is how a transaction callback leaves.
type exit struct {
	name string
	// leave runs inside the callback, after its write.
	leave func()
	// hook, when set, is installed as the commit hook named by beforeCommit
	// (true) or afterCommit (false), and the callback returns nil.
	hook         func()
	beforeCommit bool
	// recovered is what the caller must recover: the very value the exit
	// panicked with. Nil means runtime.Goexit, which nothing recovers.
	recovered any
	// committed reports whether the callback's write must survive.
	committed bool
}

func exits() []exit {
	pointer := &panicValue{name: "callback bug"}
	before := &panicValue{name: "before-commit bug"}
	after := &panicValue{name: "after-commit bug"}
	return []exit{
		{name: "panic", leave: func() { panic(pointer) }, recovered: pointer},
		// An error-valued panic reaches the caller as that error, not
		// wrapped or annotated as store.ErrBusy.
		{name: "panic with an error", leave: func() { panic(errPanicValue) }, recovered: errPanicValue},
		{name: "runtime.Goexit", leave: runtime.Goexit},
		// The commit hook runs after the callback returned nil and before
		// COMMIT: the only panic during commit this backend can reach.
		{name: "panic before commit", hook: func() { panic(before) }, beforeCommit: true, recovered: before},
		// After COMMIT the write is durable; a panic there must not undo it.
		{name: "panic after commit", hook: func() { panic(after) }, recovered: after, committed: true},
	}
}

// leaveTx runs WithTx on its own goroutine, so a runtime.Goexit ends only
// that goroutine, and reports what the caller recovered and whether WithTx
// returned at all.
func leaveTx(ctx context.Context, db store.Database, fn func(*sql.Tx) error) (recovered any, returned bool) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { recovered = recover() }()
		_ = db.WithTx(ctx, fn)
		returned = true
	}()
	<-done
	return recovered, returned
}

// TestWithTxRollsBackWhenItsCallbackDoesNotReturn: a transaction callback
// that panics, or leaves through runtime.Goexit, after writing must not keep
// its transaction open. Before #267 the transaction stayed open with the
// write lock and its pooled connection, so every later writer on the
// database file, on any handle, waited out the busy timeout and failed with
// store.ErrBusy until the process exited; on :memory: no other connection
// could even read. The panic itself must still reach the caller with its
// original value. Contexts are not cancelable: database/sql rolls a
// transaction back when its context is canceled, which would hide the leak.
func TestWithTxRollsBackWhenItsCallbackDoesNotReturn(t *testing.T) {
	for _, path := range []string{"file", ":memory:"} {
		for _, marked := range []bool{false, true} {
			for _, exit := range exits() {
				name := path + "/unmarked/" + exit.name
				if marked {
					name = path + "/store.Writer/" + exit.name
				}
				t.Run(name, func(t *testing.T) {
					testWithTxExit(t, path, marked, exit)
				})
			}
		}
	}
}

func testWithTxExit(t *testing.T, path string, marked bool, exit exit) {
	ctx := context.Background()
	if path == "file" {
		path = filepath.Join(t.TempDir(), "panic.db")
	}
	backend := Backend{BusyTimeout: panicBusyTimeout}
	db, err := backend.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// A table per subtest: every :memory: handle shares one database.
	table := "rows_" + sanitize(t.Name())
	if err := db.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (writer TEXT NOT NULL)`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	conn := db.(*connection)
	if exit.hook != nil {
		if exit.beforeCommit {
			conn.beforeCommit = exit.hook
		} else {
			conn.afterCommit = exit.hook
		}
	}
	txCtx := ctx
	if marked {
		txCtx = store.Writer(ctx)
	}
	recovered, returned := leaveTx(txCtx, db, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, `INSERT INTO `+table+` VALUES ('left')`); err != nil {
			return err
		}
		if exit.leave != nil {
			exit.leave()
		}
		return nil
	})
	conn.beforeCommit, conn.afterCommit = nil, nil
	if returned {
		t.Fatal("WithTx returned; want the exit to propagate to its caller")
	}
	if recovered != exit.recovered {
		t.Fatalf("the caller recovered %#v; want the original panic value %#v", recovered, exit.recovered)
	}
	// The writer queue's one slot: had it leaked, every later marked writer
	// on this handle would wait out the busy timeout for a turn nobody holds
	// (#214).
	if held := len(conn.writers); held != 0 {
		t.Fatalf("%d writer turn(s) still held after the exit; want the turn released", held)
	}
	// Another handle on the same database: its own pool, its own writer
	// queue, the same write lock.
	other, err := backend.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	for _, probe := range []struct {
		name string
		db   store.Database
		ctx  context.Context
	}{
		{"another handle", other, ctx},
		{"a marked writer on the same handle", db, store.Writer(ctx)},
	} {
		begin := time.Now()
		err := probe.db.WithTx(probe.ctx, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `INSERT INTO `+table+` VALUES ('next')`)
			return err
		})
		if elapsed := time.Since(begin); err != nil || elapsed > promptly {
			t.Fatalf("a write from %s after the exit took %v (err %v); want it to commit within %v (busy timeout %v)", probe.name, elapsed, err, promptly, panicBusyTimeout)
		}
	}
	var left int
	if err := other.WithTx(ctx, func(tx *sql.Tx) error {
		return tx.QueryRowContext(ctx, `SELECT count(*) FROM `+table+` WHERE writer = 'left'`).Scan(&left)
	}); err != nil {
		t.Fatal(err)
	}
	if want := map[bool]int{false: 0, true: 1}[exit.committed]; left != want {
		t.Fatalf("%d row(s) from the exited transaction committed; want %d", left, want)
	}
}

func sanitize(name string) string {
	out := make([]byte, 0, len(name))
	for i := range len(name) {
		c := name[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' {
			out = append(out, c)
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}
