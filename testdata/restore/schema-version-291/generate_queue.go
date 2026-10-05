//go:build ignore

// generate_queue writes legacy-queue-8633027.db: a worker queue written by
// origin/main at 8633027, before #290, for the schema version tests. It has
// no tombstone tables (queue schema 1) and holds one completed job. It must
// be run from a checkout of that commit, never of a later one:
//
//	git worktree add /tmp/main-8633027 8633027
//	cp testdata/restore/schema-version-291/generate_queue.go /tmp/main-8633027/testdata/restore/schema-version-291/
//	(cd /tmp/main-8633027 && go run ./testdata/restore/schema-version-291/generate_queue.go /tmp/queue.db)
//	gzip -9 -n -c /tmp/queue.db > testdata/restore/schema-version-291/legacy-queue-8633027.db.gz
//
// The committed database decompresses to 24,576 bytes with sha256
// 7b4e61a64f0271f24f5207a0a58d91863e31fed3a3d37c2409ec12f75b7c86fe.
//
// Every marker is synthetic. The fixture is a frozen historical snapshot,
// not a generated artifact checked for drift.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger/worker"
)

func main() {
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(path string) error {
	ctx := context.Background()
	at := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	db, err := (sqlite.Backend{}).Open(ctx, path)
	if err != nil {
		return err
	}
	defer db.Close()
	queue, err := worker.New(ctx, db, func() time.Time { return at })
	if err != nil {
		return err
	}
	if _, err := queue.Enqueue(ctx, worker.EnqueueRequest{RequestKey: "job-completed", Kind: "report", Payload: json.RawMessage(`{"marker":"SYNTHETIC-291-QUEUE"}`), MaxAttempts: 3}); err != nil {
		return err
	}
	if ran, err := queue.ProcessOnce(ctx, func(context.Context, worker.Tx, worker.Job) error { return nil }); err != nil || !ran {
		return fmt.Errorf("process: ran=%v err=%w", ran, err)
	}
	return nil
}
