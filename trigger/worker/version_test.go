package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/well-prado/new-blok/store"
)

// asBinary stands in for a binary that supports queue schema version.
func asBinary(t *testing.T, version int) {
	t.Helper()
	previous := schemaVersion
	schemaVersion = version
	t.Cleanup(func() { schemaVersion = previous })
}

// TestOlderWorkerRefusesACompactedQueue: a queue this binary migrated has
// tombstones (#290). A worker that predates them would not read them, so a
// duplicate of a compacted job submitted through it would be accepted and
// run again. A binary built with schema version 1 now refuses the queue at
// open, naming both versions, so it never gets to enqueue; this binary
// still answers the duplicate from the tombstone.
func TestOlderWorkerRefusesACompactedQueue(t *testing.T) {
	r := newRetentionRig(t)
	done := marked("done")
	r.finish(t, done, StateCompleted)
	r.clock.Set(retentionStart.Add(48 * time.Hour))
	if report, err := r.queue.Compact(context.Background(), Retention{Completed: r.clock.Now()}); err != nil || report.Compacted != 1 {
		t.Fatalf("report %+v err=%v", report, err)
	}

	func() {
		asBinary(t, 1)
		older, err := New(context.Background(), r.database, r.clock.Now)
		var newer *store.NewerSchemaError
		if older != nil || !errors.As(err, &newer) || *newer != (store.NewerSchemaError{Component: "worker", Version: 2, Supported: 1}) {
			t.Fatalf("a queue-1 binary opened a queue-2 database: queue=%v err=%v", older, err)
		}
	}()

	// The same binary, and a newer one, open it and keep deduplicating.
	for _, version := range []int{2, 3} {
		asBinary(t, version)
		queue, err := New(context.Background(), r.database, r.clock.Now)
		if err != nil {
			t.Fatalf("queue-%d binary: %v", version, err)
		}
		result, err := queue.Enqueue(context.Background(), done.request(t))
		if err != nil || result.Accepted || !result.Job.Compacted {
			t.Fatalf("queue-%d binary: duplicate of a compacted job %+v err=%v", version, result, err)
		}
	}
	if rows := tableCount(t, r.database, "worker_jobs"); rows != 0 {
		t.Fatalf("the duplicate created %d rows", rows)
	}
}
