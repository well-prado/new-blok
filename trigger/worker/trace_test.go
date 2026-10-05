package worker

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"github.com/well-prado/new-blok/contract/observe"
	"github.com/well-prado/new-blok/store/sqlite"
	"github.com/well-prado/new-blok/trigger"
)

// A job carries its run's parent trace context in the job record (#276).

func parent(t *testing.T, traceparent, state string) observe.TraceContext {
	t.Helper()
	parsed, err := observe.ParseTraceparent(traceparent)
	if err != nil {
		t.Fatal(err)
	}
	parsed.State = state
	return parsed
}

const (
	traceA = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	traceB = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"
)

// handled processes one job and returns it with the trace its handler's
// context carried.
func handled(t *testing.T, queue *Queue) (Job, observe.TraceContext, bool) {
	t.Helper()
	var job Job
	var seen observe.TraceContext
	var traced bool
	processed, err := queue.ProcessOnce(context.Background(), func(ctx context.Context, _ Tx, claimed Job) error {
		job = claimed
		seen, traced = observe.TraceFrom(ctx)
		return nil
	})
	if err != nil || !processed {
		t.Fatalf("processed=%v err=%v", processed, err)
	}
	return job, seen, traced
}

func TestTraceTravelsWithTheJobRecordToTheHandler(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker.db")
	database := openSQLite(t, path)
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := parent(t, traceA, "vendor=1")
	result, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: "traced", Kind: "test", Payload: []byte(`{}`), Trace: want})
	if err != nil || !result.Accepted || result.Job.Trace != want {
		t.Fatalf("enqueue %+v err=%v", result, err)
	}
	if _, err := queue.Submit(context.Background(), trigger.Submission{Key: "plain", Kind: "test", Payload: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	// A restarted queue reads the trace back from the record.
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	database = openSQLite(t, path)
	if queue, err = New(context.Background(), database, nil); err != nil {
		t.Fatal(err)
	}
	job, seen, traced := handled(t, queue)
	if job.RequestKey != "traced" || job.Trace != want || !traced || seen != want {
		t.Fatalf("job %+v handler saw %+v traced=%v", job, seen, traced)
	}
	job, seen, traced = handled(t, queue)
	if job.RequestKey != "plain" || job.Trace != (observe.TraceContext{}) || traced {
		t.Fatalf("an untraced job reached its handler with %+v (%v)", seen, traced)
	}
}

// TestTraceIsNotPartOfRequestIdentity: the payload digest and the duplicate
// comparison exclude the trace, so a redelivery with another trace context
// (or none) is a duplicate that keeps the first committed one.
func TestTraceIsNotPartOfRequestIdentity(t *testing.T) {
	queue, err := New(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "worker.db")), nil)
	if err != nil {
		t.Fatal(err)
	}
	first := parent(t, traceA, "vendor=1")
	submit := func(key string, trace observe.TraceContext) (bool, error) {
		return queue.Submit(context.Background(), trigger.Submission{Key: key, Kind: "test", Payload: []byte(`{"n":1}`), Principal: trigger.Principal{ID: "p"}, Trace: trace})
	}
	for key, sequence := range map[string][]observe.TraceContext{
		"traced-first": {first, {}, parent(t, traceB, ""), first},
		"plain-first":  {{}, first, parent(t, traceB, "x=y")},
	} {
		for index, trace := range sequence {
			accepted, err := submit(key, trace)
			if err != nil || accepted != (index == 0) {
				t.Fatalf("%s[%d]: accepted=%v err=%v, want a duplicate after the first", key, index, accepted, err)
			}
		}
		job, err := queue.Get(context.Background(), key)
		if err != nil || job.Trace != sequence[0] {
			t.Fatalf("%s: stored trace %+v, want the first committed %+v (%v)", key, job.Trace, sequence[0], err)
		}
		var digestStored string
		if err := queue.database.WithTx(context.Background(), func(tx *sql.Tx) error {
			return tx.QueryRowContext(context.Background(), `SELECT payload_digest FROM worker_jobs WHERE request_key = ?`, key).Scan(&digestStored)
		}); err != nil || digestStored != digest([]byte(`{"n":1}`)) {
			t.Fatalf("%s: payload digest %s depends on more than the payload (%v)", key, digestStored, err)
		}
	}
	// Identity still conflicts on what it always did.
	if _, err := queue.Submit(context.Background(), trigger.Submission{Key: "traced-first", Kind: "test", Payload: []byte(`{"n":2}`), Principal: trigger.Principal{ID: "p"}, Trace: first}); !errors.Is(err, trigger.ErrConflict) {
		t.Fatalf("another payload with the same trace err=%v, want a conflict", err)
	}
}

// TestMalformedTraceIsNeitherStoredNorAnError: an invalid context is not
// stored, an invalid tracestate is dropped, and a stored value that does not
// parse (a row another writer produced) is ignored when the job runs.
func TestMalformedTraceIsNeitherStoredNorAnError(t *testing.T) {
	queue, err := New(context.Background(), openSQLite(t, filepath.Join(t.TempDir(), "worker.db")), nil)
	if err != nil {
		t.Fatal(err)
	}
	valid := parent(t, traceA, "")
	zeroSpan := valid
	zeroSpan.SpanID = observe.SpanID{}
	badState := valid
	badState.State = "vendor=é"
	credential := valid
	credential.State = "pw=password:hunter2,vendor=1"
	cleanState := valid
	cleanState.State = "vendor=1"
	cases := []struct {
		key        string
		trace      observe.TraceContext
		overwrite  []string
		want       observe.TraceContext
		wantColumn [2]string
	}{
		{key: "zero-span", trace: zeroSpan, wantColumn: [2]string{"", ""}},
		{key: "bad-state", trace: badState, want: valid, wantColumn: [2]string{traceA, ""}},
		{key: "stored-version-ff", trace: valid, overwrite: []string{"ff" + traceA[2:], ""}},
		{key: "stored-garbage", trace: valid, overwrite: []string{"not a trace", "vendor=1"}},
		{key: "stored-bad-state", trace: valid, overwrite: []string{traceA, "vendor=\x01"}, want: valid},
		{key: "stored-credential", trace: valid, overwrite: []string{traceA, "token=SYNTHETIC-ts-0001,vendor=1"}, want: cleanState},
		{key: "enqueued-credential", trace: credential, want: cleanState, wantColumn: [2]string{traceA, "vendor=1"}},
	}
	for _, tc := range cases {
		if _, err := queue.Enqueue(context.Background(), EnqueueRequest{RequestKey: tc.key, Kind: "test", Payload: []byte(`{}`), Trace: tc.trace}); err != nil {
			t.Fatalf("%s: %v", tc.key, err)
		}
		if err := queue.database.WithTx(context.Background(), func(tx *sql.Tx) error {
			if tc.overwrite != nil {
				_, err := tx.ExecContext(context.Background(), `UPDATE worker_jobs SET traceparent = ?, tracestate = ? WHERE request_key = ?`, tc.overwrite[0], tc.overwrite[1], tc.key)
				return err
			}
			var traceparent, tracestate string
			if err := tx.QueryRowContext(context.Background(), `SELECT traceparent, tracestate FROM worker_jobs WHERE request_key = ?`, tc.key).Scan(&traceparent, &tracestate); err != nil {
				return err
			}
			if [2]string{traceparent, tracestate} != tc.wantColumn {
				t.Errorf("%s: stored %q %q, want %q", tc.key, traceparent, tracestate, tc.wantColumn)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	for range cases {
		job, seen, traced := handled(t, queue)
		var want observe.TraceContext
		for _, tc := range cases {
			if tc.key == job.RequestKey {
				want = tc.want
			}
		}
		if job.Trace != want || traced != want.Valid() || (traced && seen != want) {
			t.Fatalf("%s: job trace %+v, handler saw %+v (%v), want %+v", job.RequestKey, job.Trace, seen, traced, want)
		}
		if job.State != StateProcessing {
			t.Fatalf("%s: state %s", job.RequestKey, job.State)
		}
	}
}

// TestQueueMigratesPreTraceSchema opens a queue created before the trace
// columns existed: they are added in place, existing jobs carry no trace and
// still run, and an old row's identity is unchanged.
func TestQueueMigratesPreTraceSchema(t *testing.T) {
	database, err := (sqlite.Backend{}).Open(context.Background(), filepath.Join(t.TempDir(), "worker.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.WithTx(context.Background(), func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(context.Background(), `CREATE TABLE worker_jobs (
			job_id TEXT PRIMARY KEY, request_key TEXT NOT NULL UNIQUE, kind TEXT NOT NULL, payload_json BLOB NOT NULL,
			payload_digest TEXT NOT NULL, attempt INTEGER NOT NULL DEFAULT 0, deferrals INTEGER NOT NULL DEFAULT 0,
			principal_json TEXT NOT NULL DEFAULT '', max_attempts INTEGER NOT NULL, state TEXT NOT NULL,
			available_at INTEGER NOT NULL, lease_until INTEGER, error_text TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL, enqueue_seq INTEGER NOT NULL DEFAULT 0)`); err != nil {
			return err
		}
		_, err := tx.ExecContext(context.Background(), `INSERT INTO worker_jobs (job_id, request_key, kind, payload_json, payload_digest, max_attempts, state, available_at, created_at, updated_at, enqueue_seq)
			VALUES ('job:legacy', 'legacy', 'test', '{}', ?, 3, 'pending', 0, 0, 0, 1)`, digest([]byte(`{}`)))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	queue, err := New(context.Background(), database, nil)
	if err != nil {
		t.Fatalf("migration: %v", err)
	}
	if _, err := New(context.Background(), database, nil); err != nil {
		t.Fatalf("second open after migration: %v", err)
	}
	if accepted, err := queue.Submit(context.Background(), trigger.Submission{Key: "legacy", Kind: "test", Payload: []byte(`{}`), Trace: parent(t, traceA, "")}); err != nil || accepted {
		t.Fatalf("a traced repeat of a legacy job: accepted=%v err=%v, want a duplicate", accepted, err)
	}
	job, _, traced := handled(t, queue)
	if job.ID != "job:legacy" || job.Trace != (observe.TraceContext{}) || traced {
		t.Fatalf("legacy job %+v traced=%v", job, traced)
	}
}
